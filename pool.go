// Package tpool is a multitenant PostgreSQL connection pool on top of pgx.
//
// Every tenant gets a base pgxpool with a reserved warm-socket floor (Min)
// and, optionally, a short-lived burst pool. All sockets — base or burst —
// draw from one shared global budget, a lock-free semaphore that caps live
// connections across all tenants. Idle tenants effectively lend their
// reserved capacity to a tenant that spikes: the loan is a burst socket that
// dies in about a second of idle and never becomes a permanent allocation.
//
// Routing is context-driven: WithTenant records the tenant ID on the context,
// TenantFrom reads it back, and Pool.Acquire and the query helpers use it to
// pick the tenant's pools. Acquire always prefers the base pool and falls back
// to the burst only when the base is saturated (zero idle and no dial space).
//
// Stat returns a point-in-time snapshot of socket counts, budget pressure, and
// per-tenant base/burst attribution.
package tpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is a multitenant connection pool. Each tenant gets a base pgxpool with
// its own entitlement plus an optional burst pool; both draw tokens from a
// single shared Budget that caps total live sockets below server max_connections.

const (
	// connectTimeout bounds TCP + TLS + auth of every dial. Fixed on purpose
	// (not a Config knob): the number of sockets is what the budget caps, so a
	// dial must never wait forever behind a full budget.
	connectTimeout = 5 * time.Second

	// burstHealthCheckPeriod keeps burst sockets probed aggressively: a burst
	// is a short-lived loan, so any socket it holds must be shown as dead
	// almost immediately, or the loan outlives its usefulness.
	burstHealthCheckPeriod = time.Second
)

type Pool struct {
	budget       *Budget
	dsnFunc      DSNFunc
	cfg          Config
	tenants      map[string]*tenantPool
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	acquireTotal atomic.Int64
	burstSpill   atomic.Int64
}

// Config tunes a Pool. Zero values select defaults.
//
// Acquire always prefers the base pool. It first probes the base with a short
// try (BurstAfter): a free idle connection returns immediately, a pool below
// Max tries a fresh dial. Only when the base is saturated (zero idle and no
// dial space) does it try the burst, also with a short try. If neither yields,
// it falls back to a full wait on the base until the ctx (or AcquireWait).
type Config struct {
	// BurstAfter is how long each probe waits on a pool before giving up on
	// it. It covers the base probe (a quick check) and the burst probe alike:
	// it is the duration of a single attempt, not an alternation cadence.
	// Because the burst is only used as spillover, a small value keeps the
	// wait low when the base dial round-trip is long. Default 10ms.
	BurstAfter time.Duration

	// AcquireWait is the TOTAL wait budget of one Acquire: the sum of all
	// probes (base + burst) plus the final wait on the base must not exceed
	// it. 0 (default) disables the cap — the Acquire ends only when the
	// caller's ctx ends. Set it when a stuck tenant must not hold a request
	// goroutine beyond a budget.
	AcquireWait time.Duration

	// BudgetWait bounds how long a NEW socket waits for a global budget token
	// at birth (inside the dial). When it expires, that dial fails and the
	// request moves on to the queue or an error. Default 30s.
	BudgetWait time.Duration

	// GlobalMaxConns is the shared cap on live sockets (base + burst of all
	// tenants combined). It is the semaphore that controls throughput: keep it
	// below the server's max_connections. Default 500.
	GlobalMaxConns int

	// ReconcileInterval is how often the reconciler runs, re-anchoring budget
	// usage to the physical socket count (repairs failed-dial leaks). Lower
	// reacts faster; each run costs one SELECT. Default 3s.
	ReconcileInterval time.Duration

	// MaxConnIdleTime is the idle timeout of the base pool: an idle socket
	// dies after this. It controls the drain back to the Min floor.
	// Default 3s.
	MaxConnIdleTime time.Duration

	// BurstMaxConnIdleTime is the idle timeout of the burst pool. It must be
	// lower than the base's — a burst is a temporary loan and dies first.
	// Default 1s.
	BurstMaxConnIdleTime time.Duration

	// MaxConnLifetime is how long a base socket may live before it is
	// replaced. The rotation redistributes the budget under saturation and
	// recycles old connections. Default 1h.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter spreads MaxConnLifetime around its value.
	// Default 5m.
	MaxConnLifetimeJitter time.Duration

	// HealthCheckPeriod is how often the base pool health-checks its sockets.
	// Default 15s.
	HealthCheckPeriod time.Duration

	// BurstSuffix is the application_name suffix of burst connections.
	// Default "!b".
	BurstSuffix string
}

// New builds a Pool whose connections are created via dsnFunc(tenant).
func New(dsnFunc DSNFunc, cfg Config) *Pool {
	if dsnFunc == nil {
		panic("pool: nil DSNFunc")
	}
	if cfg.BurstAfter == 0 {
		cfg.BurstAfter = 10 * time.Millisecond
	}
	if cfg.BudgetWait == 0 {
		cfg.BudgetWait = 30 * time.Second
	}
	if cfg.GlobalMaxConns == 0 {
		cfg.GlobalMaxConns = 500
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 3 * time.Second
	}
	if cfg.MaxConnIdleTime == 0 {
		cfg.MaxConnIdleTime = 3 * time.Second
	}
	if cfg.BurstMaxConnIdleTime == 0 {
		cfg.BurstMaxConnIdleTime = 1 * time.Second
	}
	if cfg.MaxConnLifetime == 0 {
		cfg.MaxConnLifetime = time.Hour
	}
	if cfg.MaxConnLifetimeJitter == 0 {
		cfg.MaxConnLifetimeJitter = 5 * time.Minute
	}
	if cfg.HealthCheckPeriod == 0 {
		cfg.HealthCheckPeriod = 15 * time.Second
	}
	if cfg.BurstSuffix == "" {
		cfg.BurstSuffix = "!b"
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		budget:  NewBudget(int64(cfg.GlobalMaxConns)),
		dsnFunc: dsnFunc,
		cfg:     cfg,
		tenants: make(map[string]*tenantPool),
		ctx:     ctx,
		cancel:  cancel,
	}

	go p.reconciler(ctx)

	return p
}

// Close cancels the reconciler and closes every tenant pool. The pools are
// closed in parallel: each pgxpool.Close blocks until its in-flight
// connections are returned, so closing them one by one would serialize the
// whole drain. Close returns once all pools are shut down.
func (p *Pool) Close() {
	p.cancel()

	p.mu.RLock()
	var pools []*pgxpool.Pool
	for _, tp := range p.tenants {
		pools = append(pools, tp.base)
		if tp.burst != nil {
			pools = append(pools, tp.burst)
		}
	}
	p.mu.RUnlock()

	var wg sync.WaitGroup
	for _, pool := range pools {
		wg.Go(func() { pool.Close() })
	}
	wg.Wait()
}

// Register creates the base and (if e.Burst > 0) burst pools for tenantID.
func (p *Pool) Register(tenantID string, e Entitlement) error {
	tp := &tenantPool{}
	base, err := p.poolFor(tenantID, int32(e.Max), int32(e.Min), p.cfg.MaxConnLifetime, p.cfg.MaxConnLifetimeJitter, p.cfg.MaxConnIdleTime, p.cfg.HealthCheckPeriod, tenantID)
	if err != nil {
		return err
	}
	tp.base = base

	if e.Burst > 0 {
		// Burst pool = exception: no reserved floor (Min 0), a hard TTL
		// (MaxConnLifetime = BurstTTL, no jitter) and a short idle timeout.
		// pgxpool enforces the lifetime on Release — a burst connection dies
		// right after its query returns, never mid-query.
		burst, err := p.poolFor(tenantID, int32(e.Burst), 0, e.BurstTTL, 0, p.cfg.BurstMaxConnIdleTime, burstHealthCheckPeriod, tenantID+p.cfg.BurstSuffix)
		if err != nil {
			base.Close()
			return err
		}
		tp.burst = burst
	}

	p.mu.Lock()
	p.tenants[tenantID] = tp
	p.mu.Unlock()

	return nil
}

// poolFor builds one pgxpool with budget hooks wired in.
func (p *Pool) poolFor(tenantID string, max, min int32, lifetime, jitter, idle, health time.Duration, appName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(p.dsnFunc(tenantID))
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = max
	cfg.MinConns = min
	cfg.MaxConnLifetime = lifetime
	cfg.MaxConnLifetimeJitter = jitter
	cfg.MaxConnIdleTime = idle
	cfg.HealthCheckPeriod = health
	cfg.ConnConfig.ConnectTimeout = connectTimeout
	cfg.ConnConfig.RuntimeParams["application_name"] = appName

	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		return p.acquireToken()
	}
	cfg.BeforeClose = func(*pgx.Conn) {
		p.budget.Release(1)
	}

	return pgxpool.NewWithConfig(p.ctx, cfg)
}

// acquireToken waits for one global budget token. The wait is anchored to the
// pool's lifetime ctx (so Close unwedges it) and bounded by BudgetWait. The
// dead server guard: a token is only released by BeforeClose when the socket
// that holds it dies, or by the reconciler when a dial failure leaked it.
func (p *Pool) acquireToken() error {
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.BudgetWait)
	defer cancel()
	return p.budget.Acquire(ctx, 1)
}

// Acquire returns a connection to tenantID (from ctx). It always prefers the
// base pool: a quick try first (BurstAfter), then — only when the base is
// saturated (zero idle and no dial space) — one quick try on the burst. If
// neither yields, it falls back to a full wait on the base. The total wait
// (tries + fallback) is bounded by AcquireWait, or just by the caller's ctx
// when AcquireWait is 0.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	tp, err := p.tenant(ctx)
	if err != nil {
		return nil, err
	}
	p.acquireTotal.Add(1)

	if p.cfg.AcquireWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.AcquireWait)
		defer cancel()
	}

	// No burst pool, no spillover: wait straight on the base until the ctx.
	if tp.burst == nil {
		c, err := tp.base.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		return &Conn{pconn: c}, nil
	}

	// 1. Quick probe of the base: a free idle connection returns immediately; a
	// pool below Max tries a dial for up to BurstAfter; a pool at Max (no idle)
	// yields nil.
	if c := p.probe(ctx, tp.base, p.cfg.BurstAfter); c != nil {
		return &Conn{pconn: c}, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// 2. Base saturated: try the burst. Only when the base has ZERO idle — as
	// soon as it rests and frees a socket, the spillover turns itself off.
	if tp.base.Stat().IdleConns() == 0 {
		if c := p.probe(ctx, tp.burst, p.cfg.BurstAfter); c != nil {
			p.burstSpill.Add(1)
			return &Conn{pconn: c}, nil
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// 3. Neither yielded right now: full wait on the base until the ctx.
	c, err := tp.base.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{pconn: c}, nil
}

// probe waits up to `wait` on a single pool. A ready connection comes back
// immediately (a free idle socket costs no waiting); an exhausted pool just
// yields nil after the window, and Acquire moves on to alternate with the
// other pool.
func (p *Pool) probe(ctx context.Context, pool *pgxpool.Pool, wait time.Duration) *pgxpool.Conn {
	probeCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	c, err := pool.Acquire(probeCtx)
	if err != nil {
		return nil
	}
	return c
}

// Conn is a single acquired connection, released with Release.
type Conn struct {
	pconn *pgxpool.Conn
}

// Release returns the connection to its pool.
func (c *Conn) Release() {
	c.pconn.Release()
}

// Exec via the held connection.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return c.pconn.Exec(ctx, sql, args...)
}

// Query via the held connection; the caller must close the returned Rows.
func (c *Conn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.pconn.Query(ctx, sql, args...)
}

// QueryRow via the held connection.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.pconn.QueryRow(ctx, sql, args...)
}

// Begin starts a transaction on the held connection; the returned Tx must be
// committed or rolled back.
func (c *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return c.pconn.Begin(ctx)
}

// BeginTx starts a transaction with explicit options.
func (c *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return c.pconn.BeginTx(ctx, txOptions)
}

// SendBatch sends a batch of queries on the held connection.
func (c *Conn) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return c.pconn.SendBatch(ctx, b)
}

// CopyFrom copies rows into tableName on the held connection.
func (c *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return c.pconn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Ping verifies the held connection is alive.
func (c *Conn) Ping(ctx context.Context) error {
	return c.pconn.Ping(ctx)
}

// Conn returns the raw [pgx.Conn] for advanced use. Releasing the Conn returns
// this handle to the pool. Hijack is deliberately not exposed: it would strip
// the socket from the pool without running BeforeClose and leak the budget
// token.
func (c *Conn) Conn() *pgx.Conn {
	return c.pconn.Conn()
}

// Exec runs verbatim SQL on the tenant's base pool and returns only when done.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	bp, err := p.basePool(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return bp.Exec(ctx, sql, args...)
}

// Query runs SQL on the tenant's base pool. The returned Rows release their
// connection on Close; the caller must close them.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	bp, err := p.basePool(ctx)
	if err != nil {
		return nil, err
	}
	return bp.Query(ctx, sql, args...)
}

// QueryRow runs one SQL statement on the tenant's base pool.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	bp, err := p.basePool(ctx)
	if err != nil {
		return errRow{err: err}
	}
	return bp.QueryRow(ctx, sql, args...)
}

// Begin starts a transaction on the tenant's base pool. The returned Tx
// releases its connection when closed.
func (p *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	bp, err := p.basePool(ctx)
	if err != nil {
		return nil, err
	}
	return bp.Begin(ctx)
}

// BeginTx starts a transaction with explicit options on the tenant's base
// pool.
func (p *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	bp, err := p.basePool(ctx)
	if err != nil {
		return nil, err
	}
	return bp.BeginTx(ctx, txOptions)
}

// Ping verifies the tenant's idle connections are alive.
func (p *Pool) Ping(ctx context.Context) error {
	bp, err := p.basePool(ctx)
	if err != nil {
		return err
	}
	return bp.Ping(ctx)
}

// SendBatch sends a batch on the tenant's base pool. The returned
// BatchResults release their connection on Close.
func (p *Pool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	bp, err := p.basePool(ctx)
	if err != nil {
		return errBatch{err: err}
	}
	return bp.SendBatch(ctx, b)
}

// CopyFrom copies rows into tableName on the tenant's base pool.
func (p *Pool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	bp, err := p.basePool(ctx)
	if err != nil {
		return 0, err
	}
	return bp.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// AcquireAllIdle claims every currently idle connection of the tenant's base
// pool. Release each returned Conn when done.
func (p *Pool) AcquireAllIdle(ctx context.Context) []*Conn {
	tp, err := p.tenant(ctx)
	if err != nil {
		return nil
	}
	conns := tp.base.AcquireAllIdle(ctx)
	out := make([]*Conn, len(conns))
	for i, c := range conns {
		out[i] = &Conn{pconn: c}
	}
	return out
}

// Stat returns a point-in-time snapshot of pool and budget metrics, with a
// per-tenant base/burst breakdown.
func (p *Pool) Stat() *Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s := &Stats{
		Tenants: make(map[string]TenantStat, len(p.tenants)),
	}
	for id, tp := range p.tenants {
		ts := TenantStat{
			Base:  poolStat(tp.base),
			Burst: poolStat(tp.burst),
		}
		s.Tenants[id] = ts
		s.TotalConns += ts.Base.Total + ts.Burst.Total
		s.IdleConns += ts.Base.Idle + ts.Burst.Idle
		s.AcquiredConns += ts.Base.Acquired + ts.Burst.Acquired
	}
	s.BudgetLimit, s.BudgetUsed = p.budget.snapshot()
	s.BudgetWaiters = int64(p.budget.waitersLen())
	s.AcquireTotal = p.acquireTotal.Load()
	s.BurstSpillCount = p.burstSpill.Load()
	return s
}

func poolStat(pool *pgxpool.Pool) PoolStat {
	if pool == nil {
		return PoolStat{}
	}
	st := pool.Stat()
	return PoolStat{
		Total:    int64(st.TotalConns()),
		Idle:     int64(st.IdleConns()),
		Acquired: int64(st.AcquiredConns()),
	}
}

// Stats is a point-in-time snapshot of pool usage.
type Stats struct {
	// Aggregated across all tenants (base + burst).
	TotalConns    int64
	IdleConns     int64
	AcquiredConns int64

	// Budget.
	BudgetLimit   int64
	BudgetUsed    int64
	BudgetWaiters int64 // Acquire calls blocked on the budget queue

	// Counters since the pool started.
	AcquireTotal    int64 // Acquire calls served
	BurstSpillCount int64 // Acquire calls that got a burst connection

	// Per-tenant breakdown. Preallocated to the number of registered tenants.
	Tenants map[string]TenantStat
}

// TenantStat is one tenant's base/burst split at a point in time.
type TenantStat struct {
	Base  PoolStat
	Burst PoolStat // zero when the tenant has no burst pool
}

// PoolStat is one pool's connection counts.
type PoolStat struct {
	Total    int64
	Idle     int64
	Acquired int64
}

type tenantPool struct {
	base  *pgxpool.Pool
	burst *pgxpool.Pool
}

func (p *Pool) tenant(ctx context.Context) (*tenantPool, error) {
	tenantID, ok := TenantFrom(ctx)
	if !ok {
		return nil, ErrNoTenant
	}
	p.mu.RLock()
	tp, ok := p.tenants[tenantID]
	p.mu.RUnlock()
	if !ok {
		return nil, ErrTenantNotFound
	}
	return tp, nil
}

func (p *Pool) basePool(ctx context.Context) (*pgxpool.Pool, error) {
	tp, err := p.tenant(ctx)
	if err != nil {
		return nil, err
	}
	return tp.base, nil
}

type errRow struct{ err error }
type errBatch struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

func (b errBatch) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, b.err }
func (b errBatch) Query() (pgx.Rows, error)         { return nil, b.err }
func (b errBatch) QueryRow() pgx.Row                { return errRow{err: b.err} }
func (b errBatch) Close() error                     { return b.err }

var (
	ErrNoTenant       = &PoolError{msg: "no tenant in context"}
	ErrTenantNotFound = &PoolError{msg: "tenant not registered"}
)

// PoolError is a tenant-routing failure returned by Pool methods.
type PoolError struct{ msg string }

func (e *PoolError) Error() string { return e.msg }

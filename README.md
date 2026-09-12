# tpool

A multitenant PostgreSQL connection pool for Go with a **global budget**, per-tenant base pools, burst spillover, and live stats.

The pool caps **all tenants combined** below your server's `max_connections` (a lock-free semaphore), hands every tenant a **reserved floor of warm sockets** (`Min=1`), and lets idle tenants **lend** their spare capacity to a tenant that spikes — without the loan ever becoming a permanent allocation.

## Why this exists

The usual multitenant setup is "one big pool + RLS". It breaks in a familiar way:

- the shared pool saturates the server's `max_connections`, and one heavy tenant stamps down every other tenant;
- a per-tenant fix of `Min=N` reserves a floor for everyone but drops the global cap, so a spike still blows up the server.

`pool` is the middle ground:

| Mechanism | What it solves |
|---|---|
| **Global budget** (lock-free semaphore) | caps live sockets for *all* tenants combined, kept below `max_connections` |
| **Per-tenant base pools, `Min=1`** | every tenant keeps a warm socket; 30 SaaS tenants never reconnect from scratch |
| **Burst pools as spillover** | a tenant that outgrows its base cap borrows extra sockets for the spike; the loan dies in ~1s idle |
| **Prefer-base acquire** | the base is always tried first; the burst fires only when it is saturated (zero idle) — no dial churn |
| **Reconciler** | re-anchors the budget to real socket counts every 3s, repairing leaked tokens from failed dials |
| **Attribution** | every socket carries `application_name` (`<tenant>` base vs `<tenant>!b` burst), visible in `pg_stat_activity` |

## Install

Requires Go 1.27+.

```
go get github.com/lrweck/tpool@v0.1.0
```

## Quick start

`go run`-ready:

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lrweck/tpool"
)

func main() {
	ctx := context.Background()

	// One DSN per tenant: same database, different role/password.
	dsn := func(tenantID string) string {
		return fmt.Sprintf("postgres://%s:pass@localhost:5432/app?sslmode=disable", tenantID)
	}

	p := tpool.New(dsn, tpool.Config{
		GlobalMaxConns: 500, // keep this below the server's max_connections
	})

	// Entitlement = this tenant's cap. Min is a warm-socket floor.
	if err := p.Register("acme", tpool.Entitlement{Min: 1, Max: 5}); err != nil {
		panic(err)
	}

	// Tenant routing lives in the context.
	ctx = tpool.WithTenant(ctx, "acme")

	conn, err := p.Acquire(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT count(*) FROM users"); err != nil {
		panic(err)
	}
}
```

## Registering tenants

`Register` builds the tenant's base pool and, when `Burst > 0`, its burst pool. An entitlement is a *cap*, not a throughput allocation — throughput is governed by the global budget, shared by all tenants.

```go
// Explicit, one per tenant.
p.Register("acme", tpool.Entitlement{Min: 1, Max: 5, Burst: 2, BurstTTL: 30 * time.Second})

// Or from a profile table.
for _, c := range tpool.DefaultClasses() { // small, medium, large
	for _, id := range []string{"acme", "globex", "initech"} {
		p.Register(id, tpool.Entitlement{
			Min: c.Min, Max: c.Max, Burst: c.Burst, BurstTTL: c.BurstTTL,
		})
	}
}
```

`DefaultClasses()` ships `small (1/5+2)`, `medium (1/10+4)`, `large (1/15+5)` — caps differ, the floor is always 1.

The `DSNFunc` decides *how* each tenant authenticates: the same role for all (quick start above), **one LOGIN role per tenant** (hard isolation; the tenant shows up as `usename` in `pg_stat_activity`), or even different databases / shard hosts per tenant. tpool always owns `application_name` (for attribution) and forces `connect_timeout=5s`, overriding what the DSN says for the pool knobs. See [Per-tenant DSNs and authentication](docs/parameters.md#per-tenant-dsns-and-authentication).

## Using a connection

**Low-level** — `Acquire` returns a `*Conn` wrapping a pgx connection:

```go
dc, err := p.Acquire(tpool.WithTenant(ctx, "acme"))
if err != nil {
	return err
}
defer dc.Release()

var balance int64
if err := dc.QueryRow(ctx, "SELECT balance FROM accounts WHERE id=$1", id).Scan(&balance); err != nil {
	return err
}
```

`*Conn` exposes `Exec`, `Query`, `QueryRow`, `Begin`, `BeginTx`, `SendBatch`, `CopyFrom`, `Ping`, and `Conn` (raw pgx for the corner cases).

**Convenience** — `p.Exec`, `p.Query`, `p.QueryRow`, `p.Begin`, `p.BeginTx`, `p.SendBatch`, `p.CopyFrom`, and `p.Ping` route by context and skip the acquire/release dance:

```go
tx, err := p.Begin(tpool.WithTenant(ctx, "acme"))
if err != nil {
	return err
}
defer tx.Rollback(ctx)

tag, err := tx.Exec(ctx, "UPDATE orders SET status=$1 WHERE id=$2", "paid", orderID)
// ...
if err := tx.Commit(ctx); err != nil {
	return err
}
```

> All routing is context-driven: `tpool.WithTenant(ctx, id)` records the tenant, `tpool.TenantFrom(ctx)` reads it back. A call without a tenant returns `tpool.ErrNoTenant`; an unregistered tenant returns `tpool.ErrTenantNotFound`.

## Burst = temporary spillover

```go
p.Register("acme", tpool.Entitlement{
	Min:   1,
	Max:   5,
	Burst: 2, // up to 2 extra sockets while the base is saturated
	BurstTTL: 30 * time.Second,
})
```

The burst pool has no reserved floor (`Min=0`), a short idle TTL (1s), and a lifetime of `BurstTTL`. It is a **loan**: it exists only while the base can't absorb the spike, dies before the base, and never becomes part of the tenant's permanent floor.

## Timeouts and errors

```go
p := tpool.New(dsn, tpool.Config{
	GlobalMaxConns: 500,
	AcquireWait:    5 * time.Second, // cap the WHOLE acquire (base probe + burst + wait)
	BudgetWait:     10 * time.Second, // cap waiting for a budget token when a socket is born
})

conn, err := p.Acquire(tpool.WithTenant(ctx, "acme"))
if err != nil {
	if errors.Is(err, context.DeadlineExceeded) {
		log.Printf("tenant saturated past its budget")
	}
	return err // or retry with backoff
}
defer conn.Release()
```

The timeout cascade, shortest first: caller `ctx` → `AcquireWait` (total acquire) → `BurstAfter` (switch pool) → `BudgetWait` (dial token) → `ConnectTimeout` (5s, fixed). See [docs/parameters.md](docs/parameters.md) for the full playbook.

## Stats and observability

`p.Stat()` is a point-in-time snapshot — one `RLock`, no allocation beyond a pre-sized map:

```go
st := p.Stat()

fmt.Printf("sockets: total=%d idle=%d acquired=%d\n",
	st.TotalConns, st.IdleConns, st.AcquiredConns)
fmt.Printf("budget: limit=%d used=%d waiters=%d\n",
	st.BudgetLimit, st.BudgetUsed, st.BudgetWaiters)
fmt.Printf("since start: acquire=%d burst_spill=%d\n",
	st.AcquireTotal, st.BurstSpillCount)

for id, ts := range st.Tenants { // map[string]TenantStat, pre-sized, base/burst split
	fmt.Printf("%s base(total=%d idle=%d) burst(total=%d idle=%d)\n",
		id, ts.Base.Total, ts.Base.Idle, ts.Burst.Total, ts.Burst.Idle)
}
```

What to watch:

- `BudgetUsed` / `BudgetWaiters` — is the *global cap* the bottleneck, and how many acquires are queued for a token.
- `BurstSpillCount` — how often tenants spilled to burst; a leading indicator that `Max` is too small or `GlobalMaxConns` too tight.
- `Tenants[*].Burst.Idle` — burst sockets above zero mean the pool hasn't drained yet (they die in ~1s).
- Counters are cumulative since the pool started; feed `Stat()` to Prometheus at any interval.

Or ask Postgres who owns the sockets (per-tenant attribution via `application_name`):

```sql
SELECT application_name, state, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY 1;

-- acme     idle   1   <- base socket (o tenant's floor is warm)
-- acme!b   idle   2   <- burst loan, dies in ~1s idle
```

## Why it's fast

- The budget's happy path is **one CAS** on a single atomic word: `BenchmarkBudgetAcquireRelease` runs **~16 ns/op, 0 allocs**. A tenant that never hits the cap never touches the mutex.
- A 1000-request spike is absorbed in **~55 ms** — sockets dial in parallel; when the budget is full, waiters queue FIFO (nobody jumps the queue).
- Scaling **down** is slow on purpose (1-3s): sockets idle out instead of thrashing on every request.
- The reconciler only ever *tightens* the budget to real socket counts — it can't overcommit the cap by releasing tokens freed by in-flight destroys.

Full numbers, the acquire flow, lock contention analysis, and throughput tables: [docs/performance.md](docs/performance.md).

## Production tuning

| Knob | Recommendation |
|---|---|
| `GlobalMaxConns` | ~50-60% of the server's `max_connections` |
| `Min` | `1` on every main pool — a warm socket, not a throughput slice |
| `BudgetWait` | 30s default; 5-10s if request p99 matters |
| `AcquireWait` | `0` (let the caller ctx decide) unless each request needs a hard cap |
| `ReconcileInterval` | 1-2s (in-process stats, no DB round-trip) |
| `MaxConnLifetime` | 30s-1m under chronic saturation — rotation redistributes the budget |
| `MaxConnIdleTime` / `BurstMaxConnIdleTime` | 3s base / 1s burst — the pool drains in seconds |

## Docs

- [docs/parameters.md](docs/parameters.md) — full `Config` reference, the Acquire operation, the timeout cascade, production values.
- [docs/performance.md](docs/performance.md) — measurements: cost per step, lock-free `Budget` design, throughput by pool size.

Released under the [MIT License](LICENSE).
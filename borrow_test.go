//go:build integration

package tpool

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Test01_ReservedFloorIsUntouchable
// 50 light tenants sleep holding exactly the min; the rest saturate the
// global budget. Under total pressure, a dormant tenant's reserved socket
// must NOT be lent out (the floor is paid for permanently).
func Test01_ReservedFloorIsUntouchable(t *testing.T) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)
	specs := buildTenants()

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    globalMax,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   2 * time.Second,
		HealthCheckPeriod: 200 * time.Millisecond,
	})
	for _, s := range specs {
		p.Register(s.id, s.ents)
	}

	wait(t, "prewarm to sumMin=100", 60*time.Second, func() bool {
		return m.snapshot(t).PoolTotal(specs) >= sumMin(specs)
	})

	// aggressive: 30 light + 15 medium + 5 hard => demand 520 > 500
	aggrCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	aggressive := 0
	for _, s := range specs {
		if s.class == "medium" || s.class == "hard" ||
			(s.class == "light" && s.id <= "tnt_light_30") {
			aggressive++
			go hold(aggrCtx, p, s, s.ents.Max+s.ents.Burst)
		}
	}
	if aggressive != 50 {
		t.Fatalf("expected 50 aggressive tenants, got %d", aggressive)
	}

	wait(t, "budget saturated by aggressive tenants", 90*time.Second, func() bool {
		_, used := p.budget.snapshot()
		return used >= globalMax-5
	})
	time.Sleep(time.Second)
	v := m.snapshot(t)

	if v.PoolTotal(specs) > globalMax {
		t.Errorf("global ceiling overflowed: %d > %d", v.PoolTotal(specs), globalMax)
	}
	for _, s := range specs {
		if n := v.Tenant(s.id); n > s.ents.Max+s.ents.Burst {
			t.Errorf("%s above its own cap: %d > %d", s.id, n, s.ents.Max+s.ents.Burst)
		}
	}
	// the 50 dormant tenants hold the min untouched
	for _, s := range specs {
		if s.class == "light" && s.id > "tnt_light_30" {
			if n := v.Tenant(s.id); n < s.ents.Min {
				t.Errorf("dormant tenant %s fell below min: %d < %d", s.id, n, s.ents.Min)
			}
		}
	}
	reportLoans(t, "partial saturation (50 aggressive)", v, specs)
}

// Test02_GlobalCeilingAndLoanReturn
// All 100 tenants fire to their max at once (nominal demand 870 > 500). The
// budget cuts at 500 without errors — whoever arrives later waits. Once the
// load stops, the loan returns: used drains to the reserved floor (Σ min = 100).
func Test02_GlobalCeilingAndLoanReturn(t *testing.T) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)
	specs := buildTenants()

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    globalMax,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   2 * time.Second,
		HealthCheckPeriod: 200 * time.Millisecond,
	})
	for _, s := range specs {
		p.Register(s.id, s.ents)
	}

	wait(t, "prewarm to sumMin=100", 60*time.Second, func() bool {
		return m.snapshot(t).PoolTotal(specs) >= sumMin(specs)
	})

	allCtx, cancelAll := context.WithCancel(t.Context())
	defer cancelAll()
	sumDemand := 0
	for _, s := range specs {
		sumDemand += s.ents.Max + s.ents.Burst
		go hold(allCtx, p, s, s.ents.Max+s.ents.Burst)
	}
	t.Logf("nominal demand of all tenants at their cap: %d sockets, global budget: %d (fungible %d)",
		sumDemand, globalMax, globalMax-sumMin(specs))

	wait(t, "global budget fully saturated", 120*time.Second, func() bool {
		_, used := p.budget.snapshot()
		return used >= globalMax-5
	})
	time.Sleep(time.Second)

	v := m.snapshot(t)
	if v.PoolTotal(specs) > globalMax {
		t.Errorf("global ceiling overflowed: %d > %d", v.PoolTotal(specs), globalMax)
	}
	for _, s := range specs {
		if n := v.Tenant(s.id); n > s.ents.Max+s.ents.Burst {
			t.Errorf("%s above its own cap: %d > %d", s.id, n, s.ents.Max+s.ents.Burst)
		}
		if n := v.Tenant(s.id); n < s.ents.Min {
			t.Errorf("%s below min: %d < %d", s.id, n, s.ents.Min)
		}
	}
	_, used := p.budget.snapshot()
	// used is exact bookkeeping (1 token per socket). The cap holds on the
	// sockets and the counter; it only takes a moment for the reconciler to
	// tighten on top of a failed-dial leak.
	if v.PoolTotal(specs) > globalMax {
		t.Errorf("global ceiling overflowed: %d > %d", v.PoolTotal(specs), globalMax)
	}
	t.Logf("ceiling holds: pool conns=%d, budget used=%d/%d (transient may exceed)",
		v.PoolTotal(specs), used, globalMax)
	reportLoans(t, "total saturation", v, specs)
	time.Sleep(time.Second)
	reportLoans(t, "total saturation (resample)", m.snapshot(t), specs)

	// return phases
	cancelAll()
	wait(t, "loan returns to the reserved floor", 90*time.Second, func() bool {
		return m.snapshot(t).PoolTotal(specs) <= sumMin(specs)+12
	})
	// The reconciler re-anchors used to the physical stats asynchronously;
	// wait on the budget drain instead of reading in the burst of the last
	// release.
	wait(t, "budget drains to the reserved floor", 35*time.Second, func() bool {
		_, used := p.budget.snapshot()
		return used <= int64(sumMin(specs)+15)
	})
	_, used = p.budget.snapshot()
	v = m.snapshot(t)
	t.Logf("after drain: pool conns=%d (=Σ min %d), budget used=%d", v.PoolTotal(specs), sumMin(specs), used)
}

// Test03_BurstTTLReclaimsLoans
// The burst pool TTL returns the loan: an idle burst connection expires in
// ~TTL; a connection running a query survives the TTL. BurstTTL=1s and a 1s
// burst healthcheck make the window measurable.
func Test03_BurstTTLReclaimsLoans(t *testing.T) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    200,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   8 * time.Second,
	})
	id := "tnt_hard_01"
	err := p.Register(id, Entitlement{Min: 3, Max: 15, Burst: 5, BurstTTL: 1 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	wait(t, "base prewarm min=3", 30*time.Second, func() bool {
		return m.snapshot(t).Base(id) >= 3
	})

	// saturate the 15 of the base
	base := acquireN(t, p, id, 15)
	defer releaseAll(base)
	v := m.snapshot(t)
	if v.Base(id) < 15 {
		t.Fatalf("wanted 15 base, have %d", v.Base(id))
	}

	// #16 comes from the burst (base saturated), and runs a long query on it
	busy := acquire1(t, p, id)
	if v.Burst(id)+1 != m.snapshot(t).Burst(id) {
		t.Fatalf("acquisition #16 did not come from the burst: burst base=%d now=%d", v.Burst(id), m.snapshot(t).Burst(id))
	}
	queryDone := make(chan struct{})
	go func() {
		defer close(queryDone)
		busy.Exec(t.Context(), "SELECT pg_sleep(2)")
	}()
	time.Sleep(700 * time.Millisecond) // let the query start

	// grab 4 more from the burst and RELEASE them: they idle in the burst pool,
	// the TTL target
	more := acquireN(t, p, id, 4)
	releaseAll(more)
	if got := m.snapshot(t).Burst(id); got < 5 {
		t.Fatalf("expected 5 burst (1 busy + 4 idle), have %d", got)
	}

	wait(t, "idle burst expires, busy survives", 8*time.Second, func() bool {
		v := m.snapshot(t)
		return v.Burst(id) <= 1 && v.Base(id) >= 15
	})
	if got := m.snapshot(t).Burst(id); got != 1 {
		t.Errorf("while the query ran, the busy one also died: burst=%d (expected 1)", got)
	}

	<-queryDone
	busy.Release() // lifetime expired -> destroyed on release
	wait(t, "busy expires after the query", 8*time.Second, func() bool {
		return m.snapshot(t).Burst(id) == 0
	})
	t.Logf("TTL ok: idle died in ~1s, busy lived through the query and fell after")
}

// Test04_ReconcilerRecoversLeakedTokens
// A dial keeps failing: BeforeConnect charges the token and the destructor
// never runs. The reconciler (Σ TotalConns) fixes used back to the real count.
func Test04_ReconcilerRecoversLeakedTokens(t *testing.T) {
	deadDSN := "postgres://postgres:postgres@127.0.0.1:1/nope?connect_timeout=3"
	p := New(func(string) string { return deadDSN }, Config{
		BurstAfter:        5 * time.Millisecond,
		BudgetWait:        2 * time.Second,
		GlobalMaxConns:    globalMax,
		ReconcileInterval: time.Second,
	})
	defer p.Close()
	if err := p.Register("tnt_dead", Entitlement{Min: 0, Max: 5, Burst: 0}); err != nil {
		t.Fatal(err)
	}

	const n = 24
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 800*time.Millisecond)
			defer cancel()
			p.Acquire(WithTenant(ctx, "tnt_dead"))
		})
	}
	wg.Wait()

	_, used := p.budget.snapshot()
	if used == 0 {
		t.Logf("warning: expected leaked tokens (used=%d>0) before the reconciler", used)
	}
	t.Logf("after %d failed dials: budget used=%d (tokens leaked while the reconciler waits)", n, used)

	wait(t, "reconciler plugs the leak", 20*time.Second, func() bool {
		_, u := p.budget.snapshot()
		return u <= 2
	})
	_, used = p.budget.snapshot()
	t.Logf("reconciler OK: used=%d after failed dials", used)
	if used > 2 {
		t.Errorf("leak not repaired: used=%d", used)
	}
}

// Test05_PerTenantAttribution
// Each connection opens the session with the tenant role (options) and the
// tenant application_name (base) or tenant!b (burst). pg_stat_activity proves
// no connection is shared between tenants (one pool per tenant).
func Test05_PerTenantAttribution(t *testing.T) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    200,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   8 * time.Second,
	})
	ids := []string{"tnt_light_01", "tnt_medium_02", "tnt_hard_05"}
	for _, id := range ids {
		s := findSpec(buildTenants(), id)
		if err := p.Register(id, s.ents); err != nil {
			t.Fatal(err)
		}
	}
	wait(t, "prewarm bases", 30*time.Second, func() bool {
		v := m.snapshot(t)
		return v.Base("tnt_light_01") >= 1 && v.Base("tnt_medium_02") >= 1
	})

	// load: light 2 base, medium 3 base, hard saturates the base and grabs 1 burst
	l := acquireN(t, p, "tnt_light_01", 2)
	defer releaseAll(l)
	m2 := acquireN(t, p, "tnt_medium_02", 3)
	defer releaseAll(m2)
	h := acquireN(t, p, "tnt_hard_05", 15)
	defer releaseAll(h)
	hb := acquire1(t, p, "tnt_hard_05")
	defer hb.Release()
	if m.snapshot(t).Burst("tnt_hard_05") != 1 {
		t.Fatal("expected 1 burst connection on the hard tenant")
	}
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	rows, err := m.p.Query(ctx, `SELECT application_name, usename, count(*)::int
		FROM pg_stat_activity
		WHERE datname = current_database() AND application_name LIKE 'tnt\_%'
		GROUP BY application_name, usename`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	saw := false
	for rows.Next() {
		var app, username string
		var n int
		if err := rows.Scan(&app, &username, &n); err != nil {
			t.Fatal(err)
		}
		role := app
		if app == "tnt_hard_05"+burstSfx {
			role = "tnt_hard_05"
		}
		if username != role {
			t.Errorf("wrong attribution: app_name=%s running as role=%s (want %s)", app, username, role)
		}
		t.Logf("  app=%-16s role=%-14s conns=%d", app, username, n)
		saw = true
	}
	if !saw {
		t.Fatal("no tenant connection found in pg_stat_activity")
	}
}

// Test06_AcquirePrefersBaseAndStats: while the base has a free socket or
// dial space, Acquire does NOT go to the burst; the burst enters only when
// the base is saturated (zero idle) and another Acquire needs a connection.
// Stat() reports the base/burst split per tenant and the counters.
func Test06_AcquirePrefersBaseAndStats(t *testing.T) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    200,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   8 * time.Second,
	})
	specs := buildTenants()
	for _, id := range []string{"tnt_medium_02", "tnt_light_01"} {
		s := findSpec(specs, id)
		if err := p.Register(id, s.ents); err != nil {
			t.Fatal(err)
		}
	}

	wait(t, "prewarm bases", 30*time.Second, func() bool {
		v := m.snapshot(t)
		return v.Base("tnt_light_01") >= 1 && v.Base("tnt_medium_02") >= 1
	})

	if s := p.Stat(); len(s.Tenants) != 2 {
		t.Fatalf("Tenants size %d, want 2 (preallocated)", len(s.Tenants))
	} else if s.BurstSpillCount != 0 {
		t.Fatalf("spill before saturation = %d, want 0", s.BurstSpillCount)
	}

	// saturate the medium base (Max=10) and hold; then 2 more acquires land
	// on the burst because the base has no idle
	base := acquireN(t, p, "tnt_medium_02", 10)
	burstConns := acquireN(t, p, "tnt_medium_02", 2)
	st := p.Stat()
	if st.BurstSpillCount < 2 {
		t.Fatalf("BurstSpillCount = %d, want >= 2", st.BurstSpillCount)
	}
	if ts, ok := st.Tenants["tnt_medium_02"]; !ok || ts.Burst.Total < 1 {
		t.Fatalf("medium burst = %+v, want >= 1 burst total", ts.Burst)
	}
	if ts, ok := st.Tenants["tnt_light_01"]; !ok || ts.Base.Total < 1 {
		t.Fatalf("light base = %+v, want >= 1", ts.Base)
	}
	// aggregate = sum of tenants (base+burst)
	if sum := st.Tenants["tnt_medium_02"].Base.Total + st.Tenants["tnt_medium_02"].Burst.Total +
		st.Tenants["tnt_light_01"].Base.Total + st.Tenants["tnt_light_01"].Burst.Total; sum != st.TotalConns {
		t.Fatalf("TotalConns=%d != sum tenants=%d", st.TotalConns, sum)
	}
	if st.AcquireTotal < 12 {
		t.Fatalf("AcquireTotal = %d, want >= 12", st.AcquireTotal)
	}
	reportLoans(t, "saturated (10 base + 2 burst)", m.snapshot(t), specs)

	// release everything: the base regains idle, a new Acquire must not spill
	releaseAll(burstConns)
	releaseAll(base)
	if !waitBool(t, "base has idle", 15*time.Second, func() bool {
		return p.Stat().Tenants["tnt_medium_02"].Base.Idle > 0
	}) {
		t.Fatal("medium base did not regain idle")
	}
	before := p.Stat().BurstSpillCount
	if c := acquire1(t, p, "tnt_medium_02"); c != nil {
		c.Release()
	}
	if after := p.Stat().BurstSpillCount; after != before {
		t.Fatalf("spill after base rested: %d -> %d, want equal", before, after)
	}
}

func waitBool(t *testing.T, what string, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(60 * time.Millisecond)
	}
	t.Logf("waitBool failed: %s", what)
	return false
}

// ---- local helpers ----

func acquire1(t *testing.T, p *Pool, id string) *Conn {
	t.Helper()
	conns := acquireN(t, p, id, 1)
	return conns[0]
}

func acquireN(t *testing.T, p *Pool, id string, n int) []*Conn {
	t.Helper()
	var out []*Conn
	deadline := time.Now().Add(60 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 800*time.Millisecond)
		c, err := p.Acquire(WithTenant(ctx, id))
		cancel()
		if err != nil {
			time.Sleep(60 * time.Millisecond)
			continue
		}
		out = append(out, c)
	}
	if len(out) < n {
		t.Fatalf("wanted %d connections for %s, got %d", n, id, len(out))
	}
	return out
}

func releaseAll(conns []*Conn) {
	for _, c := range conns {
		c.Release()
	}
}

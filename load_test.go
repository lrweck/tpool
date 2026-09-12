//go:build integration

package tpool

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSaturationScenarios simulates a real system with load controlled by
// token semaphores: each tenant gets a semaphore releasing `rate` requests/s
// (guaranteed QPS at the source), and every query pays the token with a
// pg_sleep of 5..100ms. The in-flight concurrency (conns) emerges from rate x
// query duration (Little: ~rate * 52.5ms). The scenarios mix saturated and
// quiet classes:
//
//	small  -> light  (min1/max5/burst2  => cap 7)
//	medium -> medium (min1/max10/burst4 => cap 14)
//	large  -> hard   (min1/max15/burst5 => cap 20)
//
// Scenarios each get a disjoint slice of the shared role pool (slot*8 per
// class) and run in parallel, so their pg_stat_activity assertions don't
// cross-pollute. Min=1 for every main pool (overall goal): throughput is
// controlled by the budget (global semaphore), not by Min. In the non-binding
// scenarios the budget has slack: the saturated class reaches its own cap
// (high rate) while the others carry only background load (low rate, assert
// min+2). In the binding scenarios the global budget IS the bottleneck: two
// classes compete for tokens and the third stays quiet. In all of them: the
// global cap never overflows, every tenant's Min floor is never violated,
// queries never fail, and everything drains back to the reserved floor.
func TestSaturationScenarios(t *testing.T) {
	cases := []scenario{
		{
			name: "smalls_saturating_others_fine", budget: 75, slot: 0,
			smallN: 8, mediumN: 4, largeN: 2,
			smallR: 250, mediumR: 14, largeR: 16,
			fineMedium: true, fineLarge: true,
			smallFloor: 46, // 82% of the class cap (56)
		},
		{
			name: "mediums_saturating_others_fine", budget: 130, slot: 1,
			smallN: 4, mediumN: 8, largeN: 2,
			smallR: 10, mediumR: 400, largeR: 16,
			fineSmall: true, fineLarge: true,
			mediumFloor: 95, // 85% of the class cap (112)
		},
		{
			name: "larges_saturating_others_fine", budget: 145, slot: 2,
			smallN: 3, mediumN: 3, largeN: 6,
			smallR: 10, mediumR: 14, largeR: 500,
			fineSmall: true, fineMedium: true,
			largeFloor: 100, // 83% of the class cap (120)
		},
		{
			name: "small_and_medium_saturating_large_fine", budget: 60, slot: 3,
			smallN: 5, mediumN: 5, largeN: 3,
			smallR: 250, mediumR: 400, largeR: 16,
			fineLarge:   true,
			smallFloor:  12, // class min = 5, real pressure
			mediumFloor: 28, // min = 10, bulk of the demand
		},
		{
			name: "medium_and_large_saturating_small_fine", budget: 60, slot: 4,
			smallN: 4, mediumN: 5, largeN: 3,
			smallR: 10, mediumR: 400, largeR: 500,
			fineSmall:   true,
			mediumFloor: 20,
			largeFloor:  15,
		},
	}

	for _, sc := range cases {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			runScenario(t, sc)
		})
	}
}

type scenario struct {
	name                                string
	budget                              int
	slot                                int
	smallN, mediumN, largeN             int
	smallR, mediumR, largeR             int // requests/s per tenant (token semaphore)
	fineSmall, fineMedium, fineLarge    bool
	smallFloor, mediumFloor, largeFloor int // aggregate floor to prove saturation
}

func runScenario(t *testing.T, sc scenario) {
	dsn := requirePostgres(t)
	setupRoles(t, dsn)
	m := newMonitor(t, dsn)

	p := newPool(t, dsn, Config{
		BurstAfter:        10 * time.Millisecond,
		BudgetWait:        30 * time.Second,
		GlobalMaxConns:    sc.budget,
		ReconcileInterval: time.Second,
		MaxConnIdleTime:   2 * time.Second,
		HealthCheckPeriod: 200 * time.Millisecond,
	})

	specs := scenarioRolePool()
	smalls := classSpecs(specs, "small", sc.smallN, sc.slot*8)
	mediums := classSpecs(specs, "medium", sc.mediumN, sc.slot*8)
	larges := classSpecs(specs, "large", sc.largeN, sc.slot*8)
	all := append(append(append([]tenantSpec{}, smalls...), mediums...), larges...)
	for _, s := range all {
		if err := p.Register(s.id, s.ents); err != nil {
			t.Fatal(err)
		}
	}

	wait(t, "prewarm", 60*time.Second, func() bool {
		return m.snapshot(t).PoolTotal(all) >= sumMin(all)
	})

	loadCtx, stop := context.WithCancel(t.Context())
	defer stop()
	var sems []*rateSemaphore
	res := map[string]*loadResult{}
	mergeLoads(res, startLoads(loadCtx, p, smalls, sc.smallR, 1, &sems))
	mergeLoads(res, startLoads(loadCtx, p, mediums, sc.mediumR, 1000, &sems))
	mergeLoads(res, startLoads(loadCtx, p, larges, sc.largeR, 2000, &sems))

	wait(t, "classes saturadas entram em regime", 30*time.Second, func() bool {
		v := m.snapshot(t)
		return aggConns(v, smalls) >= sc.smallFloor &&
			aggConns(v, mediums) >= sc.mediumFloor &&
			aggConns(v, larges) >= sc.largeFloor
	})
	o := observe(t, m, 4*time.Second, all, smalls, mediums, larges)

	// tolerance 2 = in-flight dial window; a real overflow is >budget+2
	if o.peakTotal > sc.budget+2 {
		t.Errorf("global ceiling overflowed: peak %d > budget %d", o.peakTotal, sc.budget)
	}
	if o.bestSmall < sc.smallFloor {
		t.Errorf("smalls below expected: peak %d < %d", o.bestSmall, sc.smallFloor)
	}
	if o.bestMedium < sc.mediumFloor {
		t.Errorf("mediums below expected: peak %d < %d", o.bestMedium, sc.mediumFloor)
	}
	if o.bestLarge < sc.largeFloor {
		t.Errorf("larges below expected: peak %d < %d", o.bestLarge, sc.largeFloor)
	}
	assertCeilings(t, o, all)
	assertMins(t, o, all)
	if sc.fineSmall {
		assertFine(t, o, smalls)
	}
	if sc.fineMedium {
		assertFine(t, o, mediums)
	}
	if sc.fineLarge {
		assertFine(t, o, larges)
	}
	assertNoErrs(t, res)

	t.Logf("peak total=%d (budget %d)  small best=%d/%d  medium best=%d/%d  large best=%d/%d",
		o.peakTotal, sc.budget,
		o.bestSmall, sc.smallN*7,
		o.bestMedium, sc.mediumN*14,
		o.bestLarge, sc.largeN*20)
	reportClass(t, "small ", o.final, res, smalls)
	reportClass(t, "medium", o.final, res, mediums)
	reportClass(t, "large ", o.final, res, larges)

	stop()
	for _, s := range sems {
		s.release()
	}
	wait(t, "drains to the reserved floor", 30*time.Second, func() bool {
		return m.snapshot(t).PoolTotal(all) <= sumMin(all)+4
	})
	t.Logf("drained: total=%d <= sumMin+4=%d", m.snapshot(t).PoolTotal(all), sumMin(all)+4)
}

// ---- load helpers ----

var classNames = map[string]string{"small": "light", "medium": "medium", "large": "hard"}

// classSpecs returns the next n tenants of a class starting at index start,
// so parallel scenarios pick disjoint role slices (slot*8).
func classSpecs(specs []tenantSpec, class string, n, start int) []tenantSpec {
	name := classNames[class]
	var out []tenantSpec
	i := 0
	for _, s := range specs {
		if s.class != name {
			continue
		}
		if i < start {
			i++
			continue
		}
		out = append(out, s)
		if len(out) == n {
			return out
		}
	}
	return out
}

func aggConns(v activityView, group []tenantSpec) int {
	n := 0
	for _, s := range group {
		n += v.Tenant(s.id)
	}
	return n
}

// loadResult counts completed (ok) and failed (err) queries of a tenant.
type loadResult struct {
	ok  atomic.Int64
	err atomic.Int64
}

// rateSemaphore is a token semaphore releasing `rate` requests/s: take()
// blocks until a token is available, so per-tenant QPS is guaranteed at the
// source. In-flight concurrency emerges as rate x query duration, not by
// configuration. The bucket starts empty (no initial burst): the pace holds
// from the first token on.
type rateSemaphore struct {
	permits chan struct{}
	stop    chan struct{}
}

func newRateSemaphore(rate int, seed int64) *rateSemaphore {
	if rate < 1 {
		rate = 1
	}
	s := &rateSemaphore{
		permits: make(chan struct{}, rate), // burst = one second of tokens
		stop:    make(chan struct{}),
	}
	go s.refill(rate, seed)
	return s
}

func (s *rateSemaphore) refill(rate int, seed int64) {
	interval := time.Second / time.Duration(rate)
	// random offset desynchronizes tenants from each other (avoids aligned bursts)
	offset := time.Duration(rand.New(rand.NewSource(seed)).Float64() * float64(interval))
	time.Sleep(offset)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			select {
			case s.permits <- struct{}{}:
			default: // bucket full
			}
		}
	}
}

func (s *rateSemaphore) release() { close(s.stop) }

func (s *rateSemaphore) take(ctx context.Context) error {
	select {
	case <-s.permits:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type tenantLoad struct {
	res *loadResult
	sem *rateSemaphore
}

// runTenantLoad runs rate/10+8 submitters for one tenant; each takes a token
// from the semaphore before issuing a query (pg_sleep 5..100ms). The
// semaphore is the QPS bottleneck, never the goroutines.
func runTenantLoad(ctx context.Context, p *Pool, id string, rate int, seed int64) tenantLoad {
	tl := tenantLoad{res: &loadResult{}, sem: newRateSemaphore(rate, seed)}
	for u := range rate/10 + 8 {
		go func(u int) {
			rng := rand.New(rand.NewSource(seed*1000 + int64(u)))
			for ctx.Err() == nil {
				if err := tl.sem.take(ctx); err != nil {
					return
				}
				actx, cancel := context.WithTimeout(ctx, 30*time.Second)
				c, err := p.Acquire(WithTenant(actx, id))
				cancel()
				if err != nil {
					tl.res.err.Add(1)
					continue
				}
				ms := 5 + rng.Float64()*95
				qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err = c.Exec(qctx, fmt.Sprintf("SELECT pg_sleep(%.3f)", ms/1000))
				cancel()
				if err != nil {
					tl.res.err.Add(1)
				} else {
					tl.res.ok.Add(1)
				}
				c.Release()
			}
		}(u)
	}
	return tl
}

func startLoads(ctx context.Context, p *Pool, group []tenantSpec, rate int, seed int64, sems *[]*rateSemaphore) map[string]*loadResult {
	out := make(map[string]*loadResult, len(group))
	for i, s := range group {
		tl := runTenantLoad(ctx, p, s.id, rate, seed+int64(i))
		*sems = append(*sems, tl.sem)
		out[s.id] = tl.res
	}
	return out
}

func mergeLoads(dst, src map[string]*loadResult) {
	for k, v := range src {
		dst[k] = v
	}
}

// ---- sampling and assertions ----

type observed struct {
	peakTotal  int
	bestSmall  int
	bestMedium int
	bestLarge  int
	tenantsMax map[string]int
	final      activityView
}

// observe samples pg_stat_activity over `window`, tracking per-tenant and
// per-class peaks (max across samples, immune to dips).
func observe(t *testing.T, m *monitor, window time.Duration, all, smalls, mediums, larges []tenantSpec) observed {
	t.Helper()
	o := observed{tenantsMax: make(map[string]int)}
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		v := m.snapshot(t)
		if n := v.PoolTotal(all); n > o.peakTotal {
			o.peakTotal = n
		}
		if n := aggConns(v, smalls); n > o.bestSmall {
			o.bestSmall = n
		}
		if n := aggConns(v, mediums); n > o.bestMedium {
			o.bestMedium = n
		}
		if n := aggConns(v, larges); n > o.bestLarge {
			o.bestLarge = n
		}
		for _, s := range all {
			if n := v.Tenant(s.id); n > o.tenantsMax[s.id] {
				o.tenantsMax[s.id] = n
			}
		}
		o.final = v
		time.Sleep(250 * time.Millisecond)
	}
	return o
}

func assertMins(t *testing.T, o observed, all []tenantSpec) {
	t.Helper()
	for _, s := range all {
		if n := o.final.Tenant(s.id); n < s.ents.Min {
			t.Errorf("%s fell below min: %d < %d (reserved floor loaned out)", s.id, n, s.ents.Min)
		}
	}
}

func assertCeilings(t *testing.T, o observed, all []tenantSpec) {
	t.Helper()
	for _, s := range all {
		if n := o.tenantsMax[s.id]; n > s.ents.Max+s.ents.Burst {
			t.Errorf("%s above its own cap: %d > max+burst=%d", s.id, n, s.ents.Max+s.ents.Burst)
		}
	}
}

func assertFine(t *testing.T, o observed, group []tenantSpec) {
	t.Helper()
	for _, s := range group {
		if n := o.final.Tenant(s.id); n > s.ents.Min+2 {
			t.Errorf("%s (quiet class) rose too high: %d > min+2=%d", s.id, n, s.ents.Min+2)
		}
	}
}

func assertNoErrs(t *testing.T, res map[string]*loadResult) {
	t.Helper()
	for id, r := range res {
		if r.ok.Load() == 0 {
			t.Errorf("%s: no query executed", id)
		}
		if r.err.Load() != 0 {
			t.Errorf("%s: %d queries failed", id, r.err.Load())
		}
	}
}

func reportClass(t *testing.T, label string, v activityView, res map[string]*loadResult, group []tenantSpec) {
	t.Helper()
	var ok, errs int64
	for _, s := range group {
		if r, found := res[s.id]; found {
			ok += r.ok.Load()
			errs += r.err.Load()
		}
	}
	var parts []string
	for _, s := range group {
		parts = append(parts, fmt.Sprintf("%s=%d", s.id[len("tnt_"):], v.Tenant(s.id)))
	}
	t.Logf("  %s conns=%2d ok=%4d err=%d  [%s]", label, aggConns(v, group), ok, errs, strings.Join(parts, " "))
}

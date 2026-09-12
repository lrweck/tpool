//go:build integration

package tpool

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	globalMax  = 500
	burstSfx   = "!b"
	monitorApp = "pool-monitor"
	tenantPre  = "tnt_"
)

type tenantSpec struct {
	id    string
	class string
	ents  Entitlement
}

func tenantID(class string, idx int) string {
	return fmt.Sprintf("%s%s_%02d", tenantPre, class, idx)
}

func buildTenants() []tenantSpec {
	// Overall goal: Min=1 for every main pool; classes differ only in the cap
	// (Max, Burst). The burst pool (min 0) is the exception.
	profiles := []struct {
		class              string
		n, min, max, burst int
	}{
		{"light", 80, 1, 5, 2},
		{"medium", 15, 1, 10, 4},
		{"hard", 5, 1, 15, 5},
	}
	var out []tenantSpec
	for _, pr := range profiles {
		for i := range pr.n {
			i++
			out = append(out, tenantSpec{
				id:    tenantID(pr.class, i),
				class: pr.class,
				ents: Entitlement{
					Min:      pr.min,
					Max:      pr.max,
					Burst:    pr.burst,
					BurstTTL: 1 * time.Second,
				},
			})
		}
	}
	return out
}

// scenarioRolePool is the role universe for parallel saturation scenarios:
// buildTenants (light_01..80, medium_01..15, hard_01..05) grown so each of the
// 5 slots can slice a disjoint window of 8 per class (see slot + classSpecs).
func scenarioRolePool() []tenantSpec {
	out := append([]tenantSpec{}, buildTenants()...)
	for i := 16; i <= 40; i++ {
		out = append(out, tenantSpec{
			id:    tenantID("medium", i),
			class: "medium",
			ents:  Entitlement{Min: 1, Max: 10, Burst: 4, BurstTTL: 1 * time.Second},
		})
	}
	for i := 6; i <= 40; i++ {
		out = append(out, tenantSpec{
			id:    tenantID("hard", i),
			class: "hard",
			ents:  Entitlement{Min: 1, Max: 15, Burst: 5, BurstTTL: 1 * time.Second},
		})
	}
	return out
}

func sumMin(specs []tenantSpec) int {
	total := 0
	for _, s := range specs {
		total += s.ents.Min
	}
	return total
}

func findSpec(specs []tenantSpec, id string) tenantSpec {
	for _, s := range specs {
		if s.id == id {
			return s
		}
	}
	return tenantSpec{}
}

// tenantDSN builds a keyword/value DSN that authenticates every session AS
// the tenant role so attribution lands in pg_stat_activity.usename. (SET ROLE
// via -c role= changes current_user, but usename stays the login role — hence
// authenticating directly in the tenant role.)
func tenantDSN(base, tenant string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable connect_timeout=5",
		u.Hostname(), u.Port(), tenant, tenant, strings.TrimPrefix(u.Path, "/"))
}

func requirePostgres(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; run `make test-it`")
	}
	return dsn
}

func newPool(t *testing.T, dsn string, cfg Config) *Pool {
	t.Helper()
	p := New(func(tenant string) string { return tenantDSN(dsn, tenant) }, cfg)
	t.Cleanup(p.Close)
	return p
}

type monitor struct {
	p *pgxpool.Pool
}

func newMonitor(t *testing.T, dsn string) *monitor {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 5
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = monitorApp
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &monitor{p: pool}
}

type activityView struct {
	byApp    map[string]int
	totalAll int
}

func (v activityView) Base(id string) int  { return v.byApp[id] }
func (v activityView) Burst(id string) int { return v.byApp[id+burstSfx] }
func (v activityView) Tenant(id string) int {
	return v.Base(id) + v.Burst(id)
}

func (v activityView) PoolTotal(specs []tenantSpec) int {
	sum := 0
	for _, s := range specs {
		sum += v.Tenant(s.id)
	}
	return sum
}

func (m *monitor) snapshot(t *testing.T) activityView {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	rows, err := m.p.Query(ctx, `SELECT application_name, count(*)::int
		FROM pg_stat_activity
		WHERE datname = current_database() AND application_name IS NOT NULL
		GROUP BY application_name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	v := activityView{byApp: make(map[string]int)}
	for rows.Next() {
		var app string
		var n int
		if err := rows.Scan(&app, &n); err != nil {
			t.Fatal(err)
		}
		v.byApp[app] = n
		v.totalAll += n
	}
	return v
}

func wait(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// hold saturates a tenant's pools towards maxHeld connections and keeps them
// held until ctx is cancelled. Releases everything on return.
func hold(ctx context.Context, p *Pool, s tenantSpec, maxHeld int) {
	var held []*Conn
	defer func() {
		for _, c := range held {
			c.Release()
		}
	}()
	for ctx.Err() == nil && len(held) < maxHeld {
		actx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		c, err := p.Acquire(WithTenant(actx, s.id))
		cancel()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		held = append(held, c)
	}
	<-ctx.Done()
}

func reportLoans(t *testing.T, title string, v activityView, specs []tenantSpec) {
	t.Helper()
	type loan struct {
		id  string
		n   int
		max int
	}
	var loans []loan
	loaned := 0
	for _, s := range specs {
		n := v.Tenant(s.id)
		if n > s.ents.Min {
			loans = append(loans, loan{s.id, n, s.ents.Max + s.ents.Burst})
			loaned += n - s.ents.Min
		}
	}
	t.Logf("%s: pool conns=%d (fungible loaned=%d)", title, v.PoolTotal(specs), loaned)
	for i := 0; i < len(loans) && i < 12; i++ {
		t.Logf("   %-16s conns=%-3d (cap %d)", loans[i].id, loans[i].n, loans[i].max)
	}
	if len(loans) > 12 {
		t.Logf("   ... and %d more tenants holding loans", len(loans)-12)
	}
}

var (
	rolesOnce sync.Once
	rolesErr  error
)

// setupRoles creates the LOGIN tenant roles once; every pool session
// authenticates as its tenant. Covers the reserved-floor tests (buildTenants)
// plus the extra windows the parallel saturation scenarios slice.
func setupRoles(t *testing.T, dsn string) {
	t.Helper()
	rolesOnce.Do(func() {
		specs := scenarioRolePool()
		quoted := make([]string, len(specs))
		for i, s := range specs {
			quoted[i] = "'" + s.id + "'"
		}
		sql := fmt.Sprintf(`DO $mon$
		DECLARE n text;
		BEGIN
		  FOREACH n IN ARRAY ARRAY[%s] LOOP
		    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = n) THEN
		      EXECUTE format('CREATE ROLE %%I LOGIN PASSWORD %%L', n, n);
		    ELSE
		      EXECUTE format('ALTER ROLE %%I LOGIN PASSWORD %%L', n, n);
		    END IF;
		  END LOOP;
		END $mon$;`, strings.Join(quoted, ","))

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		conn, err := pgxpool.New(ctx, dsn)
		if err != nil {
			rolesErr = err
			return
		}
		defer conn.Close()
		_, err = conn.Exec(ctx, sql)
		if err != nil {
			rolesErr = err
		}
	})
	if rolesErr != nil {
		t.Fatalf("setupRoles: %v", rolesErr)
	}
}

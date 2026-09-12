# Pool configuration (tpool.Config)

Start with one design rule: **every main pool has `Min=1`**, and throughput
is controlled by the **budget (a global semaphore)**, not by `Min`. The
parameters below only shape the cap, the timeouts, and the acquire cadence.

## Tenant routing (context-driven)

Every `Pool` method reads the tenant from the caller context: attach it with
`tpool.WithTenant(ctx, id)` and read it back with `tpool.TenantFrom(ctx)`. A
call without a tenant returns `tpool.ErrNoTenant`; an unregistered tenant
returns `tpool.ErrTenantNotFound`.

```go
ctx = tpool.WithTenant(ctx, "acme")   // once per request, in your middleware
c, err := p.Acquire(ctx)             // routes to acme's base/burst pools
```

`Register` must run before the first `Acquire` for a tenant; call it at
startup from a config table or `tpool.DefaultClasses()`.

## Per-tenant DSNs and authentication

`New` takes a `DSNFunc` — a callback that maps the tenant ID to a DSN. The
point of the indirection is that tpool never sees your credential storage:
the callback is called once per pool at `Register` (a second time for the
burst pool), and the returned DSN is parsed with pgx `ParseConfig`. The DSN
never leaves your process, so it can come from a map, an env var, a config
table, or a secrets manager.

### Three authentication models

Same database, different data isolation:

| Model | DSN per tenant | Isolation boundary | Attribution |
|---|---|---|---|
| **Shared role + RLS** | `postgres://app:pass@db/app` for all | none at auth; data hidden by `tenant_id` + RLS policies | `application_name` only |
| **One LOGIN role per tenant** | `postgres://acme:secret@db/app` (recommended for hard isolation) | **the database itself**: a tenant's queries can never reach another tenant's data, even if app logic is wrong | `application_name` + `usename` in `pg_stat_activity` |
| **Shared login + `SET ROLE`** | one login, plus `options=-c role=<tenant>` (or `SET ROLE` after connect) | auth role, no re-auth | `application_name` only — **`usename` stays the login role** (fits testing exactly; see below) |

The suite tests use one LOGIN role per tenant on purpose: attribution lands in
`pg_stat_activity.usename` because every session authenticates *as* the
tenant. A `role=`/`SET ROLE` approach changes `current_user` but not the
login (`usename`) — fine for authorization, useless for session attribution.

Beyond auth, the callback is a **shard router**: return a different `dbname`
or host per tenant to spread tenants across databases/machines.

```go
// Same role/password for everyone (quick start).
dsn := func(tenant string) string {
    return "postgres://app:pass@db:5432/app?sslmode=require"
}

// One role per tenant, password read from a secrets vault (never hardcoded).
dsn := func(tenant string) string {
    secret, _ := vault.Get("postgres/" + tenant) // per-tenant password
    return fmt.Sprintf("postgres://%s:%s@db:5432/app?sslmode=require", tenant, url.QueryEscape(secret))
}

// Sharding: spread tenants across databases on the same server.
dsn := func(tenant string) string {
    shard := "shard" + strconv.Itoa(abs(hash(tenant))%4)
    return fmt.Sprintf("postgres://app:pass@db:5432/%s", shard)
}
```

### What tpool overrides in your DSN

The DSN configures the **connection** (host, user, password, sslmode). The
**pool** knobs and attribution are always owned by tpool, set after
`ParseConfig` and overriding whatever the DSN said:

- `pool_max_conns`, `pool_min_conns`, `pool_max_conn_lifetime`,
  `pool_max_conn_idle_time`, `pool_health_check_period` → taken from the
  `Entitlement` and `Config`, not from the DSN. Don't put them in the tenant
  DSN; they'd be silently replaced.
- `connect_timeout` → always the fixed 5s (`connectTimeout`); a dial must
  never hang behind the budget.
- `application_name` → always the tenant ID (base) or `tenantID+BurstSuffix`
  (burst, default `"!b"`). This is the attribution key for `pg_stat_activity`;
  don't set it in the DSN or you'll lose per-tenant visibility.

### Credential hygiene

- The DSNFunc is the only place credentials exist; keep the returned DSN out
  of logs (don't `log.Println(p.Stat())`-adjacent DSNs, and never print them
  in error paths that include the configured DSN).
- Rotate passwords at the vault, not in the code: `Register` re-reads on every
  call, so a restart (or re-`Register`) picks up rotation.
- Different roles per tenant need the roles created beforehand — see the
  integration suite's `setupRoles` for the pattern, run as privileged once at
  deploy time.

| Parameter | Default | Controls | Change it when |
|---|---|---|---|
| `GlobalMaxConns` | 500 | cap on live sockets (all tenants combined) | always set it: keep it below the server's `max_connections` (e.g. 500 with max_conn=1000) |
| `BurstAfter` | 10ms | how long ONE quick probe waits on a pool (base first, burst only when the base is saturated) | want low p99 latency -> lower it; more churn risk -> raise it |
| `AcquireWait` | 0 (no cap) | TOTAL time one `Acquire` may spend (base probe + burst spill + final base wait) | want a guaranteed per-request limit -> set it (e.g. 5s). 0 = only the caller ctx |
| `BudgetWait` | 30s | how long a NEW socket waits for a budget token when it is born | p99 matters -> 5-10s; high sustained load -> keep it high |
| `ReconcileInterval` | 3s | how often the reconciler runs (re-anchors budget to physical stats) | want fast cap reaction -> 1s (reads in-process pool stats, no DB round-trip) |
| `MaxConnIdleTime` | 3s | idle of the base pool -> drains to the floor | want few idle conns in the DB -> keep low (3s is already low) |
| `BurstMaxConnIdleTime` | 1s | idle of the burst pool (dies before the base) | always < `MaxConnIdleTime` |
| `MaxConnLifetime` | 1h (+5m jitter) | base socket rotation; redistributes budget under saturation | chronic saturation -> 30s-1m |
| `HealthCheckPeriod` | 15s | health check of base sockets | flaky DB -> lower it; costs a probe per socket |
| `BurstTTL` (per tenant, from class) | 30s | life of the burst pool itself | short burst -> lower it |
| `BurstSuffix` | `"!b"` | `application_name` of burst conns (attribution in pg_stat_activity) | rarely |

## The Acquire operation

`Acquire` NEVER waits on one pool only. It **prefers the base**:

1. Probe the **base** -- wait up to `BurstAfter` for a ready connection. A
   free connection returns immediately (cost ~ns); a saturated pool returns
   empty after the probe.
2. Only if the base is **saturated** (zero idle AND no dial space): probe
   the **burst** (the `try`) -- same `BurstAfter`. If the burst has no token
   left (all in use), it fails fast.
3. Otherwise fall back to a **full wait on the base** -- no going back and
   forth. If `tp.burst == nil`, the acquire waits straight on the base.

The wait ends when:
- a pool gives a connection -> success; or
- the **caller ctx** stops -> error; or
- `AcquireWait` (TOTAL time, base+burst combined) expires ->
  `context.DeadlineExceeded`.

Why prefer the base over alternating? The base holds the tenant's own cap;
it is the pool the tenant is entitled to. The burst is a loan for spikes --
probed only when the base cannot grow. Alternating (the old behavior)
probed both pools back and forth, which let a saturated base keep generating
dial churn against the burst even when the base would free a connection
first. Preferring the base kills that churn: the burst is touched only in
the spike that the base cannot absorb.

Honest trade-off: when the base is saturated **and** the burst still has
spare dial capacity, the burst spill still fires a dial attempt that lines
up in the budget queue. The queue is FIFO, so nobody starves -- but there is
churn of aborted attempts. If that bothers you, raise `BurstAfter` (20-50ms)
or lower `BudgetWait` so the dial gives up sooner.

## Order of waits (which one cuts first)

```
caller ctx (request deadline)
  - AcquireWait -- cuts the TOTAL base+burst wait (DeadlineExceeded error)
  - per pool: BurstAfter -- switch pool (base -> burst -> base -> ...)
  - new dial: BudgetWait -- wait for a global token; expired -> dial fails
  - ConnectTimeout=5s (fixed) -- stuck TCP+TLS+auth -> dial fails
```

The shortest one wins. Example: caller gives 800ms -> `AcquireWait` never
cuts; a dial hitting a full budget waits at most 30s of `BudgetWait`, but the
10ms `BurstAfter` makes the probe switch pools before that -- the caller's
800ms dominates.

## The semaphores in play (don't mix them up)

1. **Rate token (test/load)** -- `rateSemaphore` in `load_test.go`: caps QPS
   per tenant at the load source. External to the pool.
2. **Budget (library)** -- `GlobalMaxConns`: semaphore of live sockets.
   Arbitrates when the combined QPS exceeds the connection cap. This is the
   real throughput control.
3. **Local pool** -- `Max+Burst` per tenant: its own cap, independent of the
   budget. A tenant never goes above its own cap, even with budget to spare.

## Production recommendations

- `GlobalMaxConns`: ~50-60% of the server's `max_connections`.
- `BudgetWait`: 5-10s if p99 matters; 30s if sustained load has long spikes
  and the request can tolerate it.
- `AcquireWait`: 0 (ctx decides) in most cases; set a value if each request
  has a time budget and you want an early, explicit error.
- `MaxConnLifetime`: 30s-1m under saturation so the budget keeps
  redistributing.
- `ReconcileInterval`: 1-2s (reads in-process stats; no DB round-trip).
- `Min=1` on every pool; `MaxConnIdleTime=3s`;
  `BurstMaxConnIdleTime=1s`.

## Tests

- Integration tests set `BurstAfter=10ms`, `BudgetWait=30s`,
  `ReconcileInterval=1s`, `MaxConnIdleTime=2s`, `HealthCheckPeriod=200ms`,
  and do NOT set `AcquireWait` (stays 0 -> only the test ctx cuts).
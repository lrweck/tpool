package tpool

import "time"

// Entitlement is one tenant's resource profile. Min sockets are reserve that
// survives idle; Max is the base pool ceiling; Burst adds a short-lived extra
// pool. A socket costs one budget token whether it is base or burst.
//
// Min is 1 for every main pool: the floor is only a warm socket, not a
// throughput allocation. Throughput is controlled by the global budget
// (a counting semaphore), never by per-class Min. The burst pool is the
// exception: it is always created with Min 0 because it is a short-lived
// spillover, not a reserved floor.
type Entitlement struct {
	Min      int
	Max      int
	Burst    int
	BurstTTL time.Duration
}

// TenantClass describes a recurring entitlement profile.
type TenantClass struct {
	Name     string
	Min      int
	Max      int
	Burst    int
	BurstTTL time.Duration
}

// DSNFunc maps a tenant ID to the DSN used to build its pools. It is called
// once per pool at Register (a second time for the burst pool) and the result
// is parsed with pgx.ParseConfig. Because it is a callback, it may look up a
// per-tenant password or certificate from a secrets store, or route different
// tenants to different databases or shard hosts — the DSN never leaves the
// process.
//
// tpool overrides whatever the returned DSN says about pool limits
// (pool_max_conns, pool_min_conns, lifetime, idle, health check), the connect
// timeout (fixed 5s) and application_name (set to the tenant ID, or
// tenantID+BurstSuffix for burst sockets, for pg_stat_activity attribution).
type DSNFunc func(tenant string) string

// DefaultClasses returns the shipped small/medium/large profiles. Classes
// only differ in ceiling (Max, Burst): every main pool keeps Min=1.
func DefaultClasses() []TenantClass {
	return []TenantClass{
		{Name: "small", Min: 1, Max: 5, Burst: 2, BurstTTL: 30 * time.Second},
		{Name: "medium", Min: 1, Max: 10, Burst: 4, BurstTTL: 30 * time.Second},
		{Name: "large", Min: 1, Max: 15, Burst: 5, BurstTTL: 30 * time.Second},
	}
}

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

// DSNFunc maps a tenant ID to the DSN used to build its pools.
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

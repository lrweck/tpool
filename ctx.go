package tpool

import "context"

type tenantIDKey struct{}

// TenantFrom returns the tenant ID recorded in ctx by WithTenant.
func TenantFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantIDKey{}).(string)
	return id, ok
}

// WithTenant attaches a tenant ID to ctx. Acquire and the query helpers
// route to that tenant's pools.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantIDKey{}, tenant)
}

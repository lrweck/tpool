package tpool_test

import (
	"context"
	"fmt"

	"github.com/lrweck/tpool"
)

// Example demonstrates tenant routing through the context.
func Example() {
	ctx := tpool.WithTenant(context.Background(), "acme")
	id, ok := tpool.TenantFrom(ctx)
	fmt.Println(id, ok)

	_, ok = tpool.TenantFrom(context.Background())
	fmt.Println("without tenant:", ok)
	// Output:
	// acme true
	// without tenant: false
}

// ExampleNew shows a fresh pool before any tenant is registered: the shared
// budget already exists, the socket count is zero.
func ExampleNew() {
	p := tpool.New(func(tenant string) string {
		return "postgres://user:pass@localhost/app?sslmode=disable"
	}, tpool.Config{})
	defer p.Close()

	st := p.Stat()
	fmt.Printf("budget limit=%d total=%d tenants=%d\n",
		st.BudgetLimit, st.TotalConns, len(st.Tenants))
	// Output:
	// budget limit=500 total=0 tenants=0
}

// ExampleDefaultClasses prints the shipped entitlement profiles.
func ExampleDefaultClasses() {
	for _, c := range tpool.DefaultClasses() {
		fmt.Printf("%-6s min=%d max=%d burst=%d ttl=%s\n",
			c.Name, c.Min, c.Max, c.Burst, c.BurstTTL)
	}
	// Output:
	// small  min=1 max=5 burst=2 ttl=30s
	// medium min=1 max=10 burst=4 ttl=30s
	// large  min=1 max=15 burst=5 ttl=30s
}

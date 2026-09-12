package tpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Tests that depend on sleep/goroutines run inside the synctest bubble:
// virtual clock, no real waiting, no flake window.

func TestBudgetBasicAcquireRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(10)

		if err := b.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if _, used := b.snapshot(); used != 1 {
			t.Fatalf("used = %d, want 1", used)
		}

		b.Release(1)
		if _, used := b.snapshot(); used != 0 {
			t.Fatalf("used = %d, want 0", used)
		}
	})
}

func TestBudgetBlocksAtLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(2)
		b.Acquire(context.Background(), 2)

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if err := b.Acquire(ctx, 1); err != context.DeadlineExceeded {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
	})
}

func TestBudgetReleaseWakesWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(1)
		b.Acquire(context.Background(), 1)

		done := make(chan error, 1)
		go func() {
			done <- b.Acquire(context.Background(), 1)
		}()

		time.Sleep(50 * time.Millisecond)
		b.Release(1)

		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("waiter not woken")
		}
	})
}

func TestBudgetSetLimitGrows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(1)
		b.Acquire(context.Background(), 1)

		done := make(chan error, 1)
		go func() {
			done <- b.Acquire(context.Background(), 1)
		}()

		time.Sleep(50 * time.Millisecond)
		b.SetLimit(2)

		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("waiter not woken after SetLimit")
		}
	})
}

func TestBudgetContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(1)
		b.Acquire(context.Background(), 1)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- b.Acquire(ctx, 1)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("expected Canceled, got %v", err)
			}
		case <-time.After(time.Second):
			t.Error("waiter not woken after cancel")
		}

		if _, used := b.snapshot(); used != 0 {
			t.Fatalf("used = %d, want 0 after cancel", used)
		}
	})
}

func TestBudgetSetLimitShrink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(5)
		b.Acquire(context.Background(), 3)
		b.SetLimit(2)
		if _, used := b.snapshot(); used != 3 {
			t.Fatalf("used = %d, want 3 (shrink is best-effort)", used)
		}
	})
}

func TestBudgetMultipleTenants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(10)

		var wg sync.WaitGroup
		for range 3 {
			wg.Go(func() {
				for range 5 {
					b.Acquire(context.Background(), 1)
					time.Sleep(time.Millisecond)
					b.Release(1)
				}
			})
		}
		wg.Wait()

		if _, used := b.snapshot(); used != 0 {
			t.Fatalf("used = %d, want 0 after all releases", used)
		}
	})
}

func TestBudgetReconcileTightensOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(10)
		b.Acquire(context.Background(), 4)

		b.reconcile(3) // leak: a socket died without releasing its token
		if _, used := b.snapshot(); used != 3 {
			t.Fatalf("leak repair failed: used = %d, want 3", used)
		}

		b.reconcile(9) // async destroy still counts the dead socket: never loosens
		if _, used := b.snapshot(); used != 3 {
			t.Fatalf("reconcile loosened used to %d, want 3", used)
		}
	})
}

func TestBudgetTryAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(2)
		if !b.TryAcquire(2) {
			t.Fatal("TryAcquire should succeed")
		}
		if b.TryAcquire(1) {
			t.Fatal("TryAcquire should fail at limit")
		}
		b.Release(2)
		if !b.TryAcquire(1) {
			t.Fatal("TryAcquire should succeed after release")
		}
	})
}

// TestBudgetFairnessUnderContention hammers the budget while it toggles
// between saturated (waiters queued) and free (lock-free fast path), in the
// shade of -race. It exercises the waiterFlag transitions that the fast path
// depends on: every goroutine must complete, and the sum of grants must equal
// the sum of releases.
func TestBudgetFairnessUnderContention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(8)

		const workers = 32
		const rounds = 200
		var grants atomic.Int64

		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for range rounds {
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					if err := b.Acquire(ctx, 1); err != nil {
						t.Errorf("acquire under contention: %v", err)
						cancel()
						return
					}
					grants.Add(1)
					b.Release(1)
					cancel()
				}
			})
		}
		wg.Wait()

		if got := grants.Load(); got != workers*rounds {
			t.Fatalf("grants = %d, want %d", got, workers*rounds)
		}
		if _, used := b.snapshot(); used != 0 {
			t.Fatalf("used = %d, want 0 after all releases", used)
		}
	})
}

// TestBudgetPriorityToWaiters: with the budget saturated, an acquire that
// arrives later joins the FIFO queue and does NOT jump ahead of those already
// waiting — even with the lock-free fast path active. The first released token
// goes to the oldest waiter, not the newcomer.
func TestBudgetPriorityToWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(1)
		b.Acquire(context.Background(), 1)

		served := make(chan string, 8)
		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := b.Acquire(ctx, 1); err != nil {
				served <- "waiter:err"
				return
			}
			b.Release(1)
			served <- "waiter"
		}()
		time.Sleep(10 * time.Millisecond) // waiter queues up

		go func() {
			if err := b.Acquire(context.Background(), 1); err != nil {
				served <- "newcomer:err"
				return
			}
			b.Release(1)
			served <- "newcomer"
		}()
		time.Sleep(10 * time.Millisecond) // newcomer queues behind

		b.Release(1) // single free token: must go to the waiter

		select {
		case got := <-served:
			if got != "waiter" {
				t.Fatalf("first served was %s, want waiter (FIFO broken)", got)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter nunca servido")
		}
		if got := <-served; got != "newcomer" {
			t.Fatalf("second served was %s, want newcomer", got)
		}
	})
}

// TestBudgetWaiters: waitersLen reflects the depth of the budget queue.
func TestBudgetWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewBudget(1)
		b.Acquire(context.Background(), 1)

		done := make(chan error, 2)
		go func() { done <- b.Acquire(context.Background(), 1) }()
		time.Sleep(10 * time.Millisecond)
		go func() { done <- b.Acquire(context.Background(), 1) }()
		time.Sleep(10 * time.Millisecond)

		if w := b.waitersLen(); w != 2 {
			t.Fatalf("waiters = %d, want 2", w)
		}

		b.Release(1)
		<-done
		b.Release(1)
		<-done

		if w := b.waitersLen(); w != 0 {
			t.Fatalf("waiters = %d after release, want 0", w)
		}
	})
}

func BenchmarkBudgetAcquireRelease(b *testing.B) {
	bg := NewBudget(1 << 30)
	b.ReportAllocs()
	for range b.N {
		if err := bg.Acquire(context.Background(), 1); err != nil {
			b.Fatal(err)
		}
		bg.Release(1)
	}
}

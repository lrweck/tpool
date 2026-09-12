package tpool

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// waiter is a blocked Acquire waiting for tokens.
type waiter struct {
	n     int64
	ready chan struct{}
	ctx   context.Context
}

const (
	// waiterFlag is the high bit of state: set while waiters are queued.
	waiterFlag int64 = -1 << 63
	// usedMask isolates the token-count bits of state.
	usedMask int64 = 1<<63 - 1
)

// Budget is a resizable semaphore. Acquire blocks (in FIFO order) while used
// tokens exceed the limit; Release and SetLimit wake waiters.
//
// Locking model: the uncontended path is lock-free. used and a "waiters
// present" flag share one atomic word (state), so a tenant that never hits
// the ceiling acquires and releases a token without touching mu. mu is only
// acquired to enqueue/wake waiters or hand tokens to them (notify), and by
// the reconciler/SetLimit.
//
// FIFO: the fast path yields whenever the waiters flag is set, and the flag
// is set in the same atomic op that proves exhaustion -- so a token freed by
// a concurrent fast Release is never lost to a queued waiter: any Release
// whose Add lands after that proof reads the flag set and notifies under mu.
type Budget struct {
	mu      sync.Mutex
	limit   atomic.Int64
	state   atomic.Int64 // waiterFlag bit | used token count
	waiters list.List
}

var ErrBudgetExhausted = errors.New("tpool: budget limit exceeded")

// NewBudget returns a Budget with the given token limit.
func NewBudget(n int64) *Budget {
	b := &Budget{}
	b.limit.Store(n)
	return b
}

// Acquire reserves n tokens, blocking until the limit allows it or ctx ends.
func (b *Budget) Acquire(ctx context.Context, n int64) error {
	if n > b.limit.Load() {
		return ErrBudgetExhausted
	}
	// Lock-free fast path: no waiters, no contention on mu.
	if b.reserve(n) {
		return nil
	}

	b.mu.Lock()
	for {
		s := b.state.Load()
		used := s & usedMask
		if s >= 0 && b.limit.Load()-used >= n {
			// No waiters queued and tokens are free: grant directly.
			if b.state.CompareAndSwap(s, s+n) {
				b.mu.Unlock()
				return nil
			}
			continue
		}
		if ctx.Err() != nil {
			b.mu.Unlock()
			return ctx.Err()
		}
		if s >= 0 {
			// Exhausted and flag not yet set: set it in the same op that
			// proved exhaustion, so concurrent fast Releases can't miss
			// us (see FIFO comment on Budget).
			if !b.state.CompareAndSwap(s, s|waiterFlag) {
				continue
			}
		}
		break
	}
	w := &waiter{n: n, ready: make(chan struct{}, 1), ctx: ctx}
	e := b.waiters.PushBack(w)
	b.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			if w.n > 0 {
				b.state.Add(-w.n)
				w.n = 0
				b.notify()
			}
			b.waiters.Remove(e)
			b.clearFlagIfEmpty()
			b.mu.Unlock()
			return ctx.Err()
		case <-w.ready:
			if w.n > 0 {
				return nil
			}
		}
	}
}

// reserve is the lock-free fast path: it claims n tokens only when no waiter
// is queued, preserving FIFO fairness without taking mu.
func (b *Budget) reserve(n int64) bool {
	for {
		s := b.state.Load()
		if s < 0 {
			return false
		}
		if (s&usedMask)+n > b.limit.Load() {
			return false
		}
		if b.state.CompareAndSwap(s, s+n) {
			return true
		}
	}
}

// Release returns n tokens.
func (b *Budget) Release(n int64) {
	for {
		s := b.state.Load()
		if s < 0 {
			// Waiters queued: free the token and hand off under mu so the
			// oldest waiter gets it before any newcomer.
			b.mu.Lock()
			b.state.Add(-n)
			b.notify()
			b.mu.Unlock()
			return
		}
		if b.state.CompareAndSwap(s, s-n) {
			return
		}
	}
}

// TryAcquire reserves n tokens if available without blocking. Honors the
// waiters flag so it never jumps the queue.
func (b *Budget) TryAcquire(n int64) bool {
	for {
		s := b.state.Load()
		if s < 0 || (s&usedMask)+n > b.limit.Load() {
			return false
		}
		if b.state.CompareAndSwap(s, s+n) {
			return true
		}
	}
}

// SetLimit changes the token limit, waking cashable waiters.
func (b *Budget) SetLimit(n int64) {
	b.mu.Lock()
	b.limit.Store(n)
	b.notify()
	b.mu.Unlock()
}

// notify hands available tokens to queued waiters in FIFO order. Must hold mu.
func (b *Budget) notify() {
	for e := b.waiters.Front(); e != nil; {
		w := e.Value.(*waiter)
		s := b.state.Load()
		used := s & usedMask
		limit := b.limit.Load()
		if used > limit || limit-used < w.n {
			break
		}
		if !b.state.CompareAndSwap(s, s+w.n) {
			continue
		}
		next := e.Next()
		b.waiters.Remove(e)
		if w.ctx.Err() != nil {
			b.state.Add(-w.n)
			w.n = 0
		}
		w.ready <- struct{}{}
		e = next
	}
	b.clearFlagIfEmpty()
}

// clearFlagIfEmpty drops the waiters flag once no waiters remain. Must hold mu.
func (b *Budget) clearFlagIfEmpty() {
	if b.waiters.Len() != 0 {
		return
	}
	for {
		s := b.state.Load()
		if s >= 0 {
			return
		}
		if b.state.CompareAndSwap(s, s&usedMask) {
			return
		}
	}
}

func (b *Budget) snapshot() (limit, used int64) {
	return b.limit.Load(), b.state.Load() & usedMask
}

// waitersLen returns how many Acquire calls are blocked on the budget queue.
func (b *Budget) waitersLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.waiters.Len()
}

// reconcile repairs leaked tokens: a socket that died without releasing its
// token (a failed dial runs BeforeConnect but no BeforeClose). used is exact
// bookkeeping otherwise, so reconcile only tightens it — never loosens.
// Loosening would re-book tokens freed by async destroys (puddle still counts
// the dying socket) and overcommit the ceiling.
func (b *Budget) reconcile(n int64) {
	b.mu.Lock()
	for {
		s := b.state.Load()
		used := s & usedMask
		if n >= used {
			break
		}
		if b.state.CompareAndSwap(s, s-(used-n)) {
			b.notify()
			break
		}
	}
	b.mu.Unlock()
}

// reconciler periodically re-anchors budget usage to physical pool stats.
func (p *Pool) reconciler(ctx context.Context) {
	t := time.NewTicker(p.cfg.ReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.budget.reconcile(p.liveConns())
		}
	}
}

// liveConns counts connections that hold a budget token: acquired, idle and
// constructing. TotalConns already includes constructing ones, so adding
// ConstructingConns again would double-count zombies blocked on acquireToken.
func (p *Pool) liveConns() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var total int64
	for _, tp := range p.tenants {
		total += int64(tp.base.Stat().TotalConns())
		if tp.burst != nil {
			total += int64(tp.burst.Stat().TotalConns())
		}
	}
	return total
}

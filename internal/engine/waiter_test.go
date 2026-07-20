package engine

import (
	"errors"
	"testing"
	"time"
)

// The waiter helpers are pure list operations on the control-loop goroutine, so
// they can be tested directly on a bare Engine without a running cluster.

func newWaiterEngine() *Engine {
	return &Engine{}
}

// mkWaiter builds a waiter in term 1; tests that care about term mismatch set
// the field explicitly.
func mkWaiter(index int, deadline time.Time) (*waiter, chan error) {
	ch := make(chan error, 1)
	return &waiter{index: index, term: 1, resultCh: ch, deadline: deadline}, ch
}

func TestFireCommittedWaiters_ResolvesAtOrBelowCommit(t *testing.T) {
	e := newWaiterEngine()
	future := time.Now().Add(time.Minute)

	w1, c1 := mkWaiter(1, future)
	w2, c2 := mkWaiter(2, future)
	w3, c3 := mkWaiter(3, future)
	e.waiters = []*waiter{w1, w2, w3}

	// Commit index 2 → waiters 1 and 2 fire with nil, waiter 3 stays pending.
	e.fireCommittedWaiters(2, 1)

	if err := recvNoBlock(t, c1); err != nil {
		t.Fatalf("waiter 1: got %v, want nil (committed)", err)
	}
	if err := recvNoBlock(t, c2); err != nil {
		t.Fatalf("waiter 2: got %v, want nil (committed)", err)
	}
	if len(e.waiters) != 1 || e.waiters[0].index != 3 {
		t.Fatalf("expected only waiter 3 to remain, got %+v", e.waiters)
	}
	// c3 must NOT have fired.
	select {
	case v := <-c3:
		t.Fatalf("waiter 3 fired prematurely with %v", v)
	default:
	}
}

func TestSweepExpiredWaiters_FailsPastDeadline(t *testing.T) {
	e := newWaiterEngine()
	past := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Minute)

	wExpired, cExpired := mkWaiter(1, past)
	wLive, cLive := mkWaiter(2, future)
	e.waiters = []*waiter{wExpired, wLive}

	e.sweepExpiredWaiters(time.Now())

	if err := recvNoBlock(t, cExpired); !errors.Is(err, ErrCommitTimeout) {
		t.Fatalf("expired waiter: got %v, want ErrCommitTimeout", err)
	}
	if len(e.waiters) != 1 || e.waiters[0].index != 2 {
		t.Fatalf("live waiter should remain, got %+v", e.waiters)
	}
	select {
	case v := <-cLive:
		t.Fatalf("live waiter fired unexpectedly with %v", v)
	default:
	}
}

func TestFailAllWaiters(t *testing.T) {
	e := newWaiterEngine()
	future := time.Now().Add(time.Minute)
	w1, c1 := mkWaiter(1, future)
	w2, c2 := mkWaiter(2, future)
	e.waiters = []*waiter{w1, w2}

	// Simulate a stepdown: all pending proposals fail with ErrNotLeader.
	e.failAllWaiters(ErrNotLeader)

	if err := recvNoBlock(t, c1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("waiter 1: got %v, want ErrNotLeader", err)
	}
	if err := recvNoBlock(t, c2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("waiter 2: got %v, want ErrNotLeader", err)
	}
	if len(e.waiters) != 0 {
		t.Fatalf("waiters should be empty after failAll, got %d", len(e.waiters))
	}
}

// recvNoBlock reads one value from a result channel that must already be ready.
func recvNoBlock(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case v := <-ch:
		return v
	default:
		t.Fatalf("expected a value on result channel, none present")
		return nil
	}
}

// A waiter registered under one term must not be acknowledged once the term has
// moved on. The index it is waiting for may now hold a different leader's entry,
// and committing that entry says nothing about this client's write.
func TestFireCommittedWaiters_TermMismatchFails(t *testing.T) {
	e := newWaiterEngine()
	future := time.Now().Add(time.Minute)

	stale, staleCh := mkWaiter(1, future)
	stale.term = 1
	current, currentCh := mkWaiter(2, future)
	current.term = 2
	e.waiters = []*waiter{stale, current}

	// We are now leader of term 2, and index 2 has committed.
	e.fireCommittedWaiters(2, 2)

	if err := recvNoBlock(t, staleCh); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale-term waiter: got %v, want ErrNotLeader", err)
	}
	if err := recvNoBlock(t, currentCh); err != nil {
		t.Fatalf("current-term waiter: got %v, want nil", err)
	}
	if len(e.waiters) != 0 {
		t.Fatalf("expected all waiters resolved, %d remain", len(e.waiters))
	}
}

package engine

import (
	"testing"
	"time"
)

// A bounded read trades freshness for latency, but the trade has to be honest:
// the client names a bound, and a node that cannot prove it is inside that bound
// refuses rather than answering anyway. Otherwise the bound is decorative.

func TestBoundedRead_RequiresAnExplicitBound(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 0
	node.LastApplied = 0

	for _, d := range []time.Duration{0, -time.Second} {
		if err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: d}, time.Second); err != ErrMaxStalenessRequired {
			t.Fatalf("MaxStaleness %v: got %v, want ErrMaxStalenessRequired — a "+
				"client accepting staleness must say how much", d, err)
		}
	}
}

// A follower that has heard from the leader recently is inside any reasonable
// bound and answers immediately, with no round trip.
func TestBoundedRead_ServesWhenRecentlyInContact(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 3)
	node.CommitIndex = 0
	node.LastApplied = 0
	// Demote to follower and record fresh contact from a leader.
	if err := node.BecomeFollower(7, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	node.NoteContactForTest()

	if err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Second}, time.Second); err != nil {
		t.Fatalf("bounded read refused despite fresh contact: %v", err)
	}
}

// The case the tier exists to handle: a node cut off long enough that it cannot
// honour the client's bound must refuse, even though it holds a perfectly
// readable value locally.
func TestBoundedRead_RefusesWhenBeyondTheBound(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 3)
	if err := node.BecomeFollower(7, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	node.NoteContactForTest()

	// A bound far tighter than the contact we can prove.
	time.Sleep(30 * time.Millisecond)
	err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Millisecond}, time.Second)
	if err != ErrTooStale {
		t.Fatalf("got %v, want ErrTooStale — the node cannot prove it is within "+
			"the bound the client named", err)
	}
}

// A LEADER is subject to the same rule. A partitioned leader is just another
// stale server, and "ask the leader" must not become a way around the staleness
// rules.
func TestBoundedRead_LeaderWithoutQuorumIsAlsoStale(t *testing.T) {
	e, _ := newLeaderEngineSized(t, 7, 3)

	// No quorum has ever been confirmed, so the leader can prove nothing about
	// how current it is.
	if err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Second}, time.Second); err != ErrTooStale {
		t.Fatalf("got %v, want ErrTooStale — a leader that has not confirmed a "+
			"quorum is as stale as any other node", err)
	}

	// Once a majority is confirmed, the same read succeeds.
	e.noteQuorumConfirmed()
	if err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Second}, time.Second); err != nil {
		t.Fatalf("bounded read refused after a confirmed quorum: %v", err)
	}
}

// A single-node cluster has nobody it could be behind.
func TestBoundedRead_SingleNodeIsNeverStale(t *testing.T) {
	e, _ := newLeaderEngineSized(t, 7, 1)

	if got := e.Staleness(); got != 0 {
		t.Fatalf("staleness = %v, want 0 for a single-node cluster", got)
	}
	if err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Millisecond}, time.Second); err != nil {
		t.Fatalf("single-node bounded read refused: %v", err)
	}
}

// The second condition: a node can be receiving heartbeats happily while its own
// apply loop lags, which would pass the contact check while serving state it
// KNOWS is behind. Without this the bound quietly does not mean what it says.
func TestBoundedRead_RefusesWhenApplyLagsCommit(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 3)
	if err := node.BecomeFollower(7, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	node.NoteContactForTest()

	// Contact is fresh, but we know about committed entries we have not applied.
	node.CommitIndex = 10
	node.LastApplied = 3

	// No control loop is running here, so the apply wait cannot be satisfied and
	// must time out into a refusal rather than an answer.
	err := e.Read(ReadOptions{Consistency: BoundedStale, MaxStaleness: time.Minute}, 100*time.Millisecond)
	if err != ErrTooStale {
		t.Fatalf("got %v, want ErrTooStale — the node has not applied entries it "+
			"already knows are committed", err)
	}
}

// The zero value of ReadOptions must be the safe one. This is why Consistency is
// an enum rather than a `RequireLinearizable bool`, whose zero value would have
// been false and would have made the dangerous mode the accidental default.
func TestReadOptions_ZeroValueIsLinearizable(t *testing.T) {
	var opts ReadOptions
	if opts.Consistency != Linearizable {
		t.Fatalf("zero-value Consistency = %v, want Linearizable", opts.Consistency)
	}
}

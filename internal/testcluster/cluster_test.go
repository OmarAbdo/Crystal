package testcluster

import (
	"testing"
	"time"
)

// These tests validate the harness itself against behavior the current
// implementation is already believed to have. They are the floor: if these do
// not hold, no conclusion drawn from a harness test about Phase 4 is worth
// anything.

func TestElectsSingleLeader(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)
	t.Logf("node %d elected", leader.ID)
}

func TestWriteReplicatesToAllNodes(t *testing.T) {
	c := New(t, Options{Size: 3})
	c.WaitLeader(5 * time.Second)

	if err := c.Set("colour", "crystal", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "colour", "crystal", 5*time.Second)
}

// A leader that loses contact with the cluster must be replaced, and the
// replacement must come from the majority side.
func TestLeaderFailoverElectsFromMajority(t *testing.T) {
	c := New(t, Options{Size: 3})
	old := c.WaitLeader(5 * time.Second)

	c.Isolate(old.ID)
	survivors := c.Others(old.ID)

	next := c.WaitLeaderAmong(survivors, 5*time.Second)
	if next.ID == old.ID {
		t.Fatalf("isolated node %d is still leading", old.ID)
	}
	t.Logf("leadership moved %d → %d", old.ID, next.ID)
}

// The minority side of a partition must never ELECT a leader. This is the
// assertion that catches a quorum-counting mistake, and it is the reason the
// harness models a partition rather than a crash: a crashed node cannot elect
// itself either, so killing the process would prove nothing.
//
// The minority is chosen from non-leaders deliberately. Sweeping the incumbent
// into the minority tests a different and currently unimplemented property — see
// TestPartitionedLeaderStepsDown below.
func TestMinorityPartitionElectsNoLeader(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	// Split 2 | 3, with the incumbent on the majority side.
	nonLeaders := c.Others(leader.ID)
	minority := nonLeaders[:2]
	majority := c.Others(minority...)

	for _, a := range minority {
		for _, b := range majority {
			c.Cut(a, b)
			c.Cut(b, a)
		}
	}

	// The majority side keeps (or re-elects) a leader; the minority campaigns
	// forever and never wins.
	c.WaitLeaderAmong(majority, 5*time.Second)
	c.WaitNoLeaderAmong(minority, 1*time.Second)
}

// A leader cut off from a quorum should stop believing it is leader.
//
// Raft's Figure 2 does not require this: a stale leader simply cannot commit
// anything, so safety holds without it. It matters anyway, because two things
// downstream assume a node that says "I am leader" can still reach a quorum:
//
//   - /get serves reads straight from the local state machine (F12), so a
//     deposed leader answers confidently with arbitrarily stale data.
//   - clients are routed to it (F10) and their writes hang until the proposal
//     deadline rather than being redirected to the leader that actually exists.
//
// The mechanism is the same one §8 requires before serving a read-only request:
// exchange heartbeats with a majority and step down if that fails (etcd calls it
// CheckQuorum). Implemented in F19, sharing its per-peer ack machinery with
// ReadIndex.
func TestPartitionedLeaderStepsDown(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	c.Isolate(leader.ID)

	// The majority elects a replacement...
	c.WaitLeaderAmong(c.Others(leader.ID), 5*time.Second)

	// ...and the old leader should have noticed it is alone.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !leader.Raft.IsLeader() {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("node %d still believes it is leader after losing its quorum", leader.ID)
}

// Data written before a partition must still be there after the partitioned node
// rejoins, and the rejoining node must catch up.
func TestPartitionedNodeCatchesUpAfterHeal(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	// Isolate a follower and write while it is away.
	away := c.Others(leader.ID)[0]
	c.Isolate(away)

	if err := c.Set("k1", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set during partition: %v", err)
	}
	c.WaitApplied(c.Others(away), "k1", "v1", 5*time.Second)

	// The isolated node must not have it.
	if _, ok := c.Nodes[away].Store.Get("k1"); ok {
		t.Fatalf("isolated node %d somehow received the write", away)
	}

	c.Heal(away)
	c.WaitApplied([]int{away}, "k1", "v1", 5*time.Second)
}

// A lossy network slows progress but must not break it.
func TestProgressUnderPacketLoss(t *testing.T) {
	c := New(t, Options{Size: 3, Seed: 42})
	c.WaitLeader(5 * time.Second)
	c.SetDropRate(0.25)

	if err := c.Set("lossy", "ok", 5*time.Second); err != nil {
		t.Fatalf("Set under loss: %v", err)
	}
	c.WaitApplied(c.ids(), "lossy", "ok", 10*time.Second)
}

// ---- F9: elections must not wait for the slowest peer ----

// §5.2: a candidate wins "if it receives votes from a majority of the servers".
// The majority, not everyone. Waiting for all peers made every election as slow
// as the slowest one — and against a black-holed peer that means the full RPC
// timeout (1s), which is longer than the election timeout itself (300–600ms), so
// elections re-armed before they could finish.
//
// One peer here is cut in both directions but still running, so its RequestVote
// never fails fast the way a refused connection would; it simply never answers.
// The remaining two nodes are a majority of three and must elect promptly.
// Five nodes, two of them black-holed, leaving exactly a majority of three. The
// three must elect among themselves without ever waiting on the two that will
// never answer.
func TestElectionCompletesWithoutUnreachablePeer(t *testing.T) {
	c := New(t, Options{Size: 5})
	c.WaitLeader(5 * time.Second)

	// The unreachable peers must HANG, not fail fast. A refused connection costs
	// the caller nothing, so it would not distinguish waiting-for-a-quorum from
	// waiting-for-everyone; only a peer that accepts and never answers does.
	const blackhole = 2 * time.Second
	c.SetBlackholeDelay(blackhole)

	// Black-hole one node, then remove whoever is leading among the rest. That
	// leaves three reachable nodes — still a majority of five, so an election
	// must succeed.
	c.Isolate(5)
	leader := c.WaitLeaderAmong(c.Others(5), 5*time.Second)
	c.Isolate(leader.ID)

	remaining := c.Others(5, leader.ID)
	start := time.Now()
	c.WaitLeaderAmong(remaining, 10*time.Second)
	elapsed := time.Since(start)

	// Waiting for all peers would cost the full blackhole delay per attempt on top
	// of the election timeout that triggered it. Waiting for a majority costs one
	// election timeout (300–600ms) plus a round trip. The bound sits between them.
	if elapsed > blackhole {
		t.Fatalf("election took %s with two unreachable peers, which is at least "+
			"the %s a black-holed peer costs — the candidate is waiting for every "+
			"peer instead of for a majority", elapsed, blackhole)
	}
	t.Logf("elected in %s with two of five nodes black-holed for %s", elapsed, blackhole)
}

// The control loop must keep serving while an election is in flight. If vote
// gathering blocks the loop, a write submitted moments later waits on the
// election instead of on replication.
func TestControlLoopResponsiveDuringElection(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	// Slow every RPC so elections and replication both take real time.
	c.SetDelay(40 * time.Millisecond)
	c.Isolate(leader.ID)

	next := c.WaitLeaderAmong(c.Others(leader.ID), 5*time.Second)

	// The deposed leader's control loop must still be answering its own queue
	// rather than parked inside a vote round.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.SetVia(leader, "ignored", "value", 2*time.Second)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deposed node's control loop never answered a proposal — it is " +
			"blocked inside an election")
	}

	// And the live cluster keeps making progress throughout.
	if err := c.SetVia(next, "k", "v", 3*time.Second); err != nil {
		t.Fatalf("write to the new leader failed: %v", err)
	}
}

// ---- F13/F20: disruption from a rejoining node ----

// The check must not prevent legitimate elections: once the leader really is
// gone, followers stop hearing from it, the stickiness window lapses, and a new
// leader is elected normally.
func TestStickinessDoesNotBlockRealElections(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	c.Isolate(leader.ID)

	next := c.WaitLeaderAmong(c.Others(leader.ID), 5*time.Second)
	t.Logf("leadership moved %d → %d despite the stickiness check", leader.ID, next.ID)
}

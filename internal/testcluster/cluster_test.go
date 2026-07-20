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
// CheckQuorum). Crystal has no such check — a partitioned leader keeps the role
// indefinitely, which this test demonstrates and F19 tracks.
func TestPartitionedLeaderStepsDown(t *testing.T) {
	t.Skip("F19: no CheckQuorum — a leader that loses its quorum never steps down")

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

package testcluster

import (
	"errors"
	"testing"
	"time"

	"crystal/internal/engine"
)

// §8: "Read-only operations can be handled without writing anything into the
// log. However, with no additional measures, this would run the risk of
// returning stale data, since the leader responding to the request might have
// been superseded by a newer leader of which it is unaware."
//
// That sentence describes a specific, reachable scenario, and these tests build
// it: partition the leader, let the majority elect a replacement and accept a
// write, then ask the old leader — which still believes it leads — for the value.

func TestLinearizableReadReturnsCommittedWrite(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, ok, err := c.Read(leader, "k", 3*time.Second)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok || got != "v1" {
		t.Fatalf("read %q (found=%v), want v1", got, ok)
	}
}

// The core linearizability property: a read issued after a write completes must
// observe that write. Repeated, because a read-after-write race would show up
// intermittently rather than reliably.
func TestReadYourWrite(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	for i, v := range []string{"a", "b", "c", "d", "e"} {
		if err := c.SetVia(leader, "k", v, 3*time.Second); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		got, ok, err := c.Read(leader, "k", 3*time.Second)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !ok || got != v {
			t.Fatalf("read %d returned %q (found=%v), want %q — a read after a "+
				"completed write did not observe it", i, got, ok, v)
		}
	}
}

// THE test this whole mechanism exists for. A partitioned leader still believes
// it leads and its local state machine still holds the last value it knew. Asked
// for a linearizable read, it must refuse rather than answer from that state.
func TestPartitionedLeaderRefusesLinearizableRead(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "before", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "before", 5*time.Second)

	// Cut the leader off. It keeps "before" in its local state machine.
	c.Isolate(leader.ID)

	// The majority elects a replacement and moves on without the old leader.
	next := c.WaitLeaderAmong(c.Others(leader.ID), 5*time.Second)
	if err := c.SetVia(next, "k", "after", 3*time.Second); err != nil {
		t.Fatalf("Set on the new leader: %v", err)
	}

	// The stale node holds "before" locally...
	if v, _ := c.Nodes[leader.ID].Store.Get("k"); v != "before" {
		t.Fatalf("precondition: stale node holds %q, expected the old value", v)
	}

	// ...and must refuse to serve it as a linearizable read.
	got, _, err := c.Read(c.Nodes[leader.ID], "k", 4*time.Second)
	if err == nil {
		t.Fatalf("partitioned node served a linearizable read (%q) instead of "+
			"refusing — this is the §8 stale read", got)
	}
	if !errors.Is(err, engine.ErrReadTimeout) && !errors.Is(err, engine.ErrNotLeader) {
		t.Fatalf("refused with %v, want ErrReadTimeout or ErrNotLeader", err)
	}
	t.Logf("partitioned node correctly refused: %v", err)
}

// The window CheckQuorum cannot cover, and therefore the one ReadIndex exists
// for.
//
// CheckQuorum only reacts after a full grace period of silence, so for up to one
// election timeout after a partition the old leader still believes — correctly,
// as far as it can tell — that it leads. A read arriving inside that window is
// admitted by a node that has already been superseded. ReadIndex is what holds
// it: the read waits for a majority to confirm leadership, that confirmation
// never comes, and the read is refused rather than answered from local state.
//
// Issuing the read immediately after the cut is what keeps this test inside the
// window; waiting for the replacement election would let CheckQuorum resolve it
// first and prove nothing about ReadIndex.
func TestReadRefusedImmediatelyAfterPartition(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "before", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "before", 5*time.Second)

	c.Isolate(leader.ID)

	// No waiting: the node still believes it leads, and its state machine still
	// holds a value it would happily return.
	if leader.Raft.IsLeader() != true {
		t.Skip("leader stepped down faster than the test could read; rerun")
	}

	got, _, err := c.Read(leader, "k", 4*time.Second)
	if err == nil {
		t.Fatalf("a leader partitioned moments ago served %q as a linearizable "+
			"read; it cannot know whether that value is current", got)
	}
	if !errors.Is(err, engine.ErrReadTimeout) && !errors.Is(err, engine.ErrNotLeader) {
		t.Fatalf("refused with %v, want ErrReadTimeout or ErrNotLeader", err)
	}
	t.Logf("refused inside the CheckQuorum window: %v", err)
}

// Reads must survive a leadership change: the client retries against whoever
// leads next and gets the current value, not an error forever.
func TestReadsResumeAfterFailover(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "v1", 5*time.Second)

	c.Isolate(leader.ID)
	next := c.WaitLeaderAmong(c.Others(leader.ID), 5*time.Second)

	got, ok, err := c.Read(next, "k", 4*time.Second)
	if err != nil {
		t.Fatalf("read on the new leader: %v", err)
	}
	if !ok || got != "v1" {
		t.Fatalf("read %q (found=%v), want v1", got, ok)
	}
}

// Reads must keep working through packet loss — refusing on the first lost
// heartbeat would make the mechanism useless in practice.
func TestLinearizableReadsUnderPacketLoss(t *testing.T) {
	c := New(t, Options{Size: 3, Seed: 7})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.SetDropRate(0.2)

	got, ok, err := c.Read(leader, "k", 5*time.Second)
	if err != nil {
		t.Fatalf("read under 20%% loss: %v", err)
	}
	if !ok || got != "v" {
		t.Fatalf("read %q (found=%v), want v", got, ok)
	}
}

// ---- F21: followers serve linearizable reads ----

// The point of the exercise. A follower answers a linearizable read from its own
// state machine after fetching a confirmed read index from the leader — only the
// index crosses the network, so read capacity scales with the cluster instead of
// being pinned to the leader.
func TestFollowersServeLinearizableReads(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}

	for _, id := range c.Others(leader.ID) {
		got, ok, err := c.Read(c.Nodes[id], "k", 3*time.Second)
		if err != nil {
			t.Fatalf("follower %d refused a linearizable read: %v", id, err)
		}
		if !ok || got != "v1" {
			t.Fatalf("follower %d returned %q (found=%v), want v1", id, got, ok)
		}
	}
}

// A follower read must observe a write that has already been acknowledged, even
// though the follower may not have applied it yet when the read arrives. This is
// what the read index buys: the follower waits for its own apply to reach the
// leader's committed frontier before answering.
func TestFollowerReadObservesAcknowledgedWrite(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)
	follower := c.Nodes[c.Others(leader.ID)[0]]

	// Slow the network so replication to the follower genuinely lags the write
	// acknowledgement — without this the follower is usually already caught up
	// and the read index never has to do any work.
	c.SetDelay(15 * time.Millisecond)

	for i, v := range []string{"a", "b", "c", "d", "e"} {
		if err := c.SetVia(leader, "k", v, 3*time.Second); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		got, ok, err := c.Read(follower, "k", 3*time.Second)
		if err != nil {
			t.Fatalf("follower read %d: %v", i, err)
		}
		if !ok || got != v {
			t.Fatalf("follower read %d returned %q (found=%v), want %q — a "+
				"linearizable read missed an acknowledged write", i, got, ok, v)
		}
	}
}

// A partitioned FOLLOWER must refuse. It cannot reach the leader for a read
// index, and its own state may be arbitrarily old — this is exactly the case
// where serving locally would be a stale read.
func TestPartitionedFollowerRefusesLinearizableRead(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "before", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "before", 5*time.Second)

	follower := c.Nodes[c.Others(leader.ID)[0]]
	c.Isolate(follower.ID)

	// The cluster moves on without it.
	if err := c.SetVia(leader, "k", "after", 3*time.Second); err != nil {
		t.Fatalf("Set after partition: %v", err)
	}

	// It still holds the old value locally...
	if v, _ := follower.Store.Get("k"); v != "before" {
		t.Fatalf("precondition: isolated follower holds %q", v)
	}

	// ...and must refuse to serve it as linearizable.
	got, _, err := c.Read(follower, "k", 2*time.Second)
	if err == nil {
		t.Fatalf("isolated follower served %q as a linearizable read", got)
	}
	if !errors.Is(err, engine.ErrNotLeader) && !errors.Is(err, engine.ErrReadTimeout) {
		t.Fatalf("refused with %v, want ErrNotLeader or ErrReadTimeout", err)
	}
	t.Logf("isolated follower correctly refused: %v", err)
}

// A follower that has not yet learned who the leader is cannot fetch a read
// index, and must say so rather than answer from whatever it happens to hold.
func TestFollowerWithoutKnownLeaderRefuses(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	follower := c.Nodes[c.Others(leader.ID)[0]]
	// Cut only the follower's path TO the leader; it keeps receiving heartbeats,
	// so it still believes a leader exists, but cannot ask for a read index.
	c.Cut(follower.ID, leader.ID)

	if _, _, err := c.Read(follower, "k", 2*time.Second); err == nil {
		t.Fatal("follower answered a linearizable read without reaching the leader")
	}
}

// ---- F22: bounded-staleness reads ----

// The tier exists so a client can trade freshness for latency deliberately —
// but the bound it names is enforced, not advisory. A node inside the bound
// answers locally with no round trip; a node outside it refuses even though it
// holds a perfectly readable value.
func TestBoundedRead_ServedWhileInContact(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "v1", 5*time.Second)

	// Every node is in contact, so every node can answer within a generous bound.
	for _, id := range c.ids() {
		got, ok, err := c.ReadBounded(c.Nodes[id], "k", 2*time.Second, 2*time.Second)
		if err != nil {
			t.Fatalf("node %d refused a bounded read while in contact: %v", id, err)
		}
		if !ok || got != "v1" {
			t.Fatalf("node %d returned %q (found=%v), want v1", id, got, ok)
		}
	}
}

// A partitioned follower serves bounded reads only while its budget lasts, then
// refuses. This is the behaviour that makes the tier honest: the client's answer
// is wrong by at most the bound it chose, rather than arbitrarily wrong.
func TestBoundedRead_RefusedOnceBudgetIsSpent(t *testing.T) {
	c := New(t, Options{Size: 3})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "v1", 5*time.Second)

	follower := c.Nodes[c.Others(leader.ID)[0]]
	c.Isolate(follower.ID)

	// Immediately after the cut it is still well inside a 1s budget.
	if _, _, err := c.ReadBounded(follower, "k", time.Second, 2*time.Second); err != nil {
		t.Fatalf("isolated follower refused a bounded read immediately after the "+
			"cut, while still inside its budget: %v", err)
	}

	// Once the budget is spent it must refuse, even though the value is right
	// there in its state machine.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		_, _, err := c.ReadBounded(follower, "k", 300*time.Millisecond, time.Second)
		if errors.Is(err, engine.ErrTooStale) {
			t.Logf("refused once the budget was spent: %v", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("isolated follower kept serving bounded reads past its budget — the " +
		"bound is decorative")
}

// A partitioned LEADER is subject to the same rule. "Ask the leader" must not be
// a way around the staleness rules.
func TestBoundedRead_PartitionedLeaderIsAlsoStale(t *testing.T) {
	c := New(t, Options{Size: 5})
	leader := c.WaitLeader(5 * time.Second)

	if err := c.SetVia(leader, "k", "v1", 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.WaitApplied(c.ids(), "k", "v1", 5*time.Second)

	c.Isolate(leader.ID)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _, err := c.ReadBounded(leader, "k", 300*time.Millisecond, time.Second)
		if errors.Is(err, engine.ErrTooStale) || errors.Is(err, engine.ErrNotLeader) {
			t.Logf("partitioned leader refused a bounded read: %v", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("partitioned leader kept serving bounded reads past its budget")
}

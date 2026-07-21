package store

import (
	"testing"

	"crystal/internal/raft"
)

// Raft on its own gives at-least-once, not exactly-once. A leader can commit an
// entry and die before answering; the client sees a timeout, retries, and the
// command applies a second time. §8's remedy is client-assigned serial numbers
// plus a per-client record in the state machine.

func TestApply_DedupesRepeatedSequence(t *testing.T) {
	m := NewMemoryStateMachine()

	set := raft.Command{Op: raft.OpSet, Key: "k", Value: "v1", ClientID: "c1", Seq: 1}
	if err := m.Apply(1, set); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	// Someone else moves the key on.
	if err := m.Apply(2, raft.Command{Op: raft.OpSet, Key: "k", Value: "v2"}); err != nil {
		t.Fatalf("second write: %v", err)
	}

	// The original client retries its command. It must NOT be re-executed.
	if err := m.Apply(3, set); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, _ := m.Get("k"); got != "v2" {
		t.Fatalf("k = %q, want v2 — a retransmitted command was applied again", got)
	}
}

// The case that makes dedup matter even for a store whose operations look
// idempotent. Applying set(k,v) twice is harmless; a retried DELETE is not.
func TestApply_RetriedDeleteDoesNotDestroyRecreatedKey(t *testing.T) {
	m := NewMemoryStateMachine()

	if err := m.Apply(1, raft.Command{Op: raft.OpSet, Key: "k", Value: "original"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	del := raft.Command{Op: raft.OpDelete, Key: "k", ClientID: "c1", Seq: 1}
	if err := m.Apply(2, del); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The client times out and, unaware the delete committed, retries. Meanwhile
	// somebody recreates the key.
	if err := m.Apply(3, raft.Command{Op: raft.OpSet, Key: "k", Value: "recreated"}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if err := m.Apply(4, del); err != nil {
		t.Fatalf("retried delete: %v", err)
	}

	got, ok := m.Get("k")
	if !ok || got != "recreated" {
		t.Fatalf("k = %q (found=%v), want recreated — a retried delete destroyed a "+
			"value written after the original delete", got, ok)
	}
}

// A duplicate must replay the ORIGINAL outcome, errors included, or a client
// retrying a failed command could be told it succeeded.
func TestApply_DuplicateReplaysOriginalError(t *testing.T) {
	m := NewMemoryStateMachine()

	bad := raft.Command{Op: raft.OpSet, Key: "", Value: "v", ClientID: "c1", Seq: 1}
	first := m.Apply(1, bad)
	if first == nil {
		t.Fatal("expected the empty-key command to fail")
	}

	second := m.Apply(2, bad)
	if second == nil {
		t.Fatal("duplicate of a failed command reported success")
	}
	if second.Error() != first.Error() {
		t.Fatalf("replayed error %q, want the original %q", second, first)
	}
}

// Sequence numbers advance; only a repeat or a regression is a duplicate.
func TestApply_AdvancingSequencesAllExecute(t *testing.T) {
	m := NewMemoryStateMachine()

	for i, v := range []string{"a", "b", "c"} {
		cmd := raft.Command{Op: raft.OpSet, Key: "k", Value: v,
			ClientID: "c1", Seq: uint64(i + 1)}
		if err := m.Apply(i+1, cmd); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		if got, _ := m.Get("k"); got != v {
			t.Fatalf("after seq %d: k = %q, want %q", i+1, got, v)
		}
	}
}

// Clients are independent: one client's sequence must not suppress another's.
func TestApply_SequencesAreScopedPerClient(t *testing.T) {
	m := NewMemoryStateMachine()

	if err := m.Apply(1, raft.Command{Op: raft.OpSet, Key: "a", Value: "1",
		ClientID: "c1", Seq: 1}); err != nil {
		t.Fatalf("c1: %v", err)
	}
	// Same sequence number, different client — must execute.
	if err := m.Apply(2, raft.Command{Op: raft.OpSet, Key: "b", Value: "2",
		ClientID: "c2", Seq: 1}); err != nil {
		t.Fatalf("c2: %v", err)
	}

	if got, ok := m.Get("b"); !ok || got != "2" {
		t.Fatalf("b = %q (found=%v) — one client's sequence suppressed another's",
			got, ok)
	}
}

// Commands without a client tag are applied every time. Exactly-once is opt-in.
func TestApply_UntaggedCommandsAreNotDeduped(t *testing.T) {
	m := NewMemoryStateMachine()

	if err := m.Apply(1, raft.Command{Op: raft.OpSet, Key: "k", Value: "first"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := m.Apply(2, raft.Command{Op: raft.OpSet, Key: "k", Value: "second"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got, _ := m.Get("k"); got != "second" {
		t.Fatalf("k = %q, want second", got)
	}
}

// Dedup state is STATE. A node that restarted from a snapshot, or a follower
// that received one, must not forget which commands it had already applied —
// otherwise the next retransmission it sees is executed again, and the guarantee
// silently evaporates exactly when a client is most likely to be retrying.
func TestSnapshot_CarriesTheDedupTable(t *testing.T) {
	m := NewMemoryStateMachine()

	del := raft.Command{Op: raft.OpDelete, Key: "k", ClientID: "c1", Seq: 7}
	if err := m.Apply(1, raft.Command{Op: raft.OpSet, Key: "k", Value: "v"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := m.Apply(2, del); err != nil {
		t.Fatalf("delete: %v", err)
	}

	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// A fresh node restores that snapshot, then the key is recreated.
	restored := NewMemoryStateMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := restored.Apply(3, raft.Command{Op: raft.OpSet, Key: "k", Value: "recreated"}); err != nil {
		t.Fatalf("recreate: %v", err)
	}

	// The client's retry arrives at the restored node. It must still be
	// recognized as a duplicate.
	if err := restored.Apply(4, del); err != nil {
		t.Fatalf("retried delete: %v", err)
	}
	if got, ok := restored.Get("k"); !ok || got != "recreated" {
		t.Fatalf("k = %q (found=%v) — the restored node forgot the dedup table and "+
			"re-applied a retransmission", got, ok)
	}
}

// The data itself must survive a snapshot round trip too, alongside the sessions.
func TestSnapshot_CarriesDataAndSessionsTogether(t *testing.T) {
	m := NewMemoryStateMachine()
	if err := m.Apply(1, raft.Command{Op: raft.OpSet, Key: "a", Value: "1",
		ClientID: "c1", Seq: 3}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := NewMemoryStateMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got, ok := restored.Get("a"); !ok || got != "1" {
		t.Fatalf("a = %q (found=%v), want 1", got, ok)
	}
	// Seq 3 already seen, so seq 2 is a regression and must be suppressed.
	if err := restored.Apply(2, raft.Command{Op: raft.OpSet, Key: "a", Value: "stale",
		ClientID: "c1", Seq: 2}); err != nil {
		t.Fatalf("stale apply: %v", err)
	}
	if got, _ := restored.Get("a"); got != "1" {
		t.Fatalf("a = %q, want 1 — an out-of-order retransmission was applied", got)
	}
}

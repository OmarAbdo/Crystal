package store

import (
	"testing"

	"crystal/internal/raft"
)

func TestApply_SetAndGet(t *testing.T) {
	sm := NewMemoryStateMachine()
	if err := sm.Apply(1, raft.Command{Op: raft.OpSet, Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Apply set: %v", err)
	}
	got, ok := sm.Get("k")
	if !ok || got != "v" {
		t.Fatalf("Get(k) = %q,%v, want v,true", got, ok)
	}
}

func TestApply_Delete(t *testing.T) {
	sm := NewMemoryStateMachine()
	sm.Apply(1, raft.Command{Op: raft.OpSet, Key: "k", Value: "v"})
	if err := sm.Apply(2, raft.Command{Op: raft.OpDelete, Key: "k"}); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}
	if _, ok := sm.Get("k"); ok {
		t.Fatalf("expected key gone after delete")
	}
}

func TestApply_RejectsEmptyKey(t *testing.T) {
	sm := NewMemoryStateMachine()
	if err := sm.Apply(1, raft.Command{Op: raft.OpSet, Key: "", Value: "v"}); err == nil {
		t.Fatalf("expected error for empty key on set")
	}
	if err := sm.Apply(2, raft.Command{Op: raft.OpDelete, Key: ""}); err == nil {
		t.Fatalf("expected error for empty key on delete")
	}
}

func TestApply_RejectsUnknownOp(t *testing.T) {
	sm := NewMemoryStateMachine()
	if err := sm.Apply(1, raft.Command{Op: "frobnicate", Key: "k"}); err == nil {
		t.Fatalf("expected error for unknown op")
	}
}

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	sm := NewMemoryStateMachine()
	sm.Apply(1, raft.Command{Op: raft.OpSet, Key: "a", Value: "1"})
	sm.Apply(2, raft.Command{Op: raft.OpSet, Key: "b", Value: "2"})

	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh state machine and confirm equality.
	restored := NewMemoryStateMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, ok := restored.Get(k)
		if !ok || got != want {
			t.Fatalf("restored Get(%q) = %q,%v, want %q,true", k, got, ok, want)
		}
	}
}

func TestRestore_ReplacesExistingState(t *testing.T) {
	// Restore must REPLACE, not merge: keys not in the snapshot should vanish.
	sm := NewMemoryStateMachine()
	sm.Apply(1, raft.Command{Op: raft.OpSet, Key: "stale", Value: "old"})

	other := NewMemoryStateMachine()
	other.Apply(1, raft.Command{Op: raft.OpSet, Key: "fresh", Value: "new"})
	snap, _ := other.Snapshot()

	if err := sm.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := sm.Get("stale"); ok {
		t.Fatalf("expected 'stale' to be gone after full restore")
	}
	if v, ok := sm.Get("fresh"); !ok || v != "new" {
		t.Fatalf("expected 'fresh'='new' after restore, got %q,%v", v, ok)
	}
}

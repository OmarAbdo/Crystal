package raft

import (
	"errors"
	"path/filepath"
	"testing"
)

// Receiving a snapshot is the one moment a follower deliberately destroys log
// entries it has already fsync-acked to a leader. The ordering of the steps
// around that destruction decides whether a crash in the middle is recoverable.
//
// A persisted snapshot that covers entries still present in the log is
// harmless — recovery reconciles the overlap. A log that has discarded entries
// with no persisted snapshot covering them is unrecoverable: those entries were
// acknowledged as durable, the state that replaced them was never written, and
// nothing on disk can reconstruct either. So persist must come first.

func TestHandleInstallSnapshot_PersistBeforeLogReset(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)

	// Give the follower a short log, all of it superseded by the snapshot.
	for i := 0; i < 3; i++ {
		if _, err := rl.AppendLeader([]byte(`{"op":"noop"}`), 1); err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
	}

	var order []string
	restore := func([]byte) error {
		order = append(order, "restore")
		return nil
	}
	persist := func() error {
		order = append(order, "persist")
		// The log must still be intact at this point: nothing may be discarded
		// before the snapshot that replaces it is durable.
		if got := rl.FirstIndex(); got != 1 {
			t.Errorf("log was reset before persist: FirstIndex = %d, want 1", got)
		}
		return nil
	}

	resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 4,
	}, restore, persist)

	if resp.Term != 1 {
		t.Fatalf("resp.Term = %d, want 1", resp.Term)
	}
	if len(order) != 2 || order[0] != "restore" || order[1] != "persist" {
		t.Fatalf("call order = %v, want [restore persist]", order)
	}
	if got := rl.FirstIndex(); got != 51 {
		t.Fatalf("FirstIndex = %d, want 51 after the reset", got)
	}
	commit, applied := rn.CommitAndApplyBoundary()
	if commit != 50 || applied != 50 {
		t.Fatalf("commit/applied = %d/%d, want 50/50", commit, applied)
	}
}

// If the snapshot cannot be persisted, nothing may be destroyed on its behalf.
func TestHandleInstallSnapshot_PersistFailureLeavesLogIntact(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)
	for i := 0; i < 3; i++ {
		if _, err := rl.AppendLeader([]byte(`{"op":"noop"}`), 1); err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
	}

	rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 4,
	},
		func([]byte) error { return nil },
		func() error { return errors.New("disk full") },
	)

	if got := rl.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1 (log must survive a failed persist)", got)
	}
	if got := rl.LatestIndex(); got != 3 {
		t.Fatalf("LatestIndex = %d, want 3", got)
	}
	commit, applied := rn.CommitAndApplyBoundary()
	if commit != 0 || applied != 0 {
		t.Fatalf("commit/applied = %d/%d, want 0/0 — nothing was installed", commit, applied)
	}
}

// ---- Recovery side: a persisted snapshot may legitimately overlap the WAL ----

// Persisting before truncating means a crash can leave a snapshot whose
// LastIncludedIndex is at or above entries still in the WAL. Recovery has to
// reconcile that, or the log's index bookkeeping starts describing entries that
// are not there.

func TestRestoreOffset_DropsSupersededEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "superseded.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	defer rl.Close()

	// WAL holds 1..10; the snapshot covers through 50. Every entry is obsolete.
	for i := 0; i < 10; i++ {
		if _, err := rl.AppendLeader([]byte(`{"op":"noop"}`), 1); err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
	}

	if err := rl.RestoreOffset(50, 4); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}

	if got := rl.FirstIndex(); got != 51 {
		t.Fatalf("FirstIndex = %d, want 51", got)
	}
	if got := rl.LatestIndex(); got != 50 {
		t.Fatalf("LatestIndex = %d, want 50 (the snapshot boundary)", got)
	}
	if got := rl.LatestTerm(); got != 4 {
		t.Fatalf("LatestTerm = %d, want 4", got)
	}
	// The stale entries must be gone from disk too, or the next recovery
	// resurrects them.
	reopened, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, ok := reopened.GetEntry(1); ok {
		t.Fatal("superseded entry 1 survived the WAL rewrite")
	}
}

func TestRestoreOffset_PartialOverlapKeepsSuffix(t *testing.T) {
	rl := newTestLog(t)

	// WAL holds 1..8; the snapshot covers through 5. Entries 6..8 are still live.
	for i := 0; i < 8; i++ {
		if _, err := rl.AppendLeader([]byte(`{"op":"noop"}`), 1); err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
	}

	if err := rl.RestoreOffset(5, 1); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}

	if got := rl.FirstIndex(); got != 6 {
		t.Fatalf("FirstIndex = %d, want 6", got)
	}
	if got := rl.LatestIndex(); got != 8 {
		t.Fatalf("LatestIndex = %d, want 8 (suffix retained)", got)
	}
	for _, idx := range []int{6, 7, 8} {
		if _, ok := rl.GetEntry(idx); !ok {
			t.Fatalf("live entry %d was dropped", idx)
		}
	}
	if _, ok := rl.GetEntry(5); ok {
		t.Fatal("entry 5 is covered by the snapshot and should be gone")
	}
}

// The empty-WAL case: a node restarting from a snapshot with nothing appended
// after it. This is the common path and must keep working.
func TestRestoreOffset_EmptyLogAlignsToBoundary(t *testing.T) {
	rl := newTestLog(t)

	if err := rl.RestoreOffset(50, 4); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}

	if got := rl.FirstIndex(); got != 51 {
		t.Fatalf("FirstIndex = %d, want 51", got)
	}
	entry, err := rl.AppendLeader([]byte(`{"op":"noop"}`), 5)
	if err != nil {
		t.Fatalf("AppendLeader: %v", err)
	}
	if entry.Index != 51 {
		t.Fatalf("next appended index = %d, want 51", entry.Index)
	}
}

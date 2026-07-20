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

// ---- F4: a failed install must not count as replication ----

// recordingTransport captures the last InstallSnapshot response it returned.
type recordingTransport struct {
	Transport
	resp InstallSnapshotResponse
}

func (r *recordingTransport) InstallSnapshot(string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return r.resp, nil
}

// The leader advances a peer's matchIndex on the strength of an InstallSnapshot
// reply. If every failure path returns the same bytes as success, it advances on
// failures too — and matchIndex is what AdvanceCommitIndex counts. A follower
// that failed to restore is then indistinguishable from one holding the data, so
// a phantom quorum commits entries that exist on one server. Kill that server and
// the committed entries are gone: Leader Completeness violated by bookkeeping.
func TestInstallSnapshotTo_NoProgressOnFailure(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	if err := rn.BecomeFollower(4, 0); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	tr := &recordingTransport{resp: InstallSnapshotResponse{Term: 4, Success: false}}
	r := NewReplicator(tr)

	r.InstallSnapshotTo(rn, 2, "peer-2", InstallSnapshotRequest{
		Term: 4, LeaderID: 1, LastIncludedIndex: 50, LastIncludedTerm: 4,
	})

	rn.mu.RLock()
	match := rn.MatchIndex[2]
	rn.mu.RUnlock()
	if match != 0 {
		t.Fatalf("MatchIndex[2] = %d after a FAILED install, want 0 — the leader is "+
			"counting a follower that does not have the data", match)
	}
}

func TestInstallSnapshotTo_AdvancesOnSuccess(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	if err := rn.BecomeFollower(4, 0); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	tr := &recordingTransport{resp: InstallSnapshotResponse{Term: 4, Success: true}}
	r := NewReplicator(tr)

	r.InstallSnapshotTo(rn, 2, "peer-2", InstallSnapshotRequest{
		Term: 4, LeaderID: 1, LastIncludedIndex: 50, LastIncludedTerm: 4,
	})

	rn.mu.RLock()
	match := rn.MatchIndex[2]
	rn.mu.RUnlock()
	if match != 50 {
		t.Fatalf("MatchIndex[2] = %d, want 50", match)
	}
}

// Every receiver outcome must be distinguishable by the leader. A restore that
// errors, and a snapshot already covered, are not successes it may count.
func TestHandleInstallSnapshot_ReportsSuccessAccurately(t *testing.T) {
	t.Run("restore failure is not success", func(t *testing.T) {
		rn := newTestNode(t, 1, []int{2, 3}, 3)
		rl := newTestLog(t)
		resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term: 1, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 1,
		},
			func([]byte) error { return errors.New("corrupt snapshot") },
			func() error { return nil },
		)
		if resp.Success {
			t.Fatal("restore failed but the response reported Success")
		}
	})

	t.Run("persist failure is not success", func(t *testing.T) {
		rn := newTestNode(t, 1, []int{2, 3}, 3)
		rl := newTestLog(t)
		resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term: 1, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 1,
		},
			func([]byte) error { return nil },
			func() error { return errors.New("disk full") },
		)
		if resp.Success {
			t.Fatal("persist failed but the response reported Success")
		}
	})

	t.Run("stale term is not success", func(t *testing.T) {
		rn := newTestNode(t, 1, []int{2, 3}, 3)
		rl := newTestLog(t)
		if err := rn.BecomeFollower(7, 2); err != nil {
			t.Fatalf("seed term: %v", err)
		}
		resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term: 3, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 3,
		},
			func([]byte) error { return nil },
			func() error { return nil },
		)
		if resp.Success {
			t.Fatal("stale-term request reported Success")
		}
	})

	// Already covered IS a success: the follower genuinely holds everything
	// through that index, which is exactly what the leader is asking about.
	t.Run("already covered is success", func(t *testing.T) {
		rn := newTestNode(t, 1, []int{2, 3}, 3)
		rl := newTestLog(t)
		if err := rl.RestoreOffset(60, 4); err != nil {
			t.Fatalf("RestoreOffset: %v", err)
		}
		resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term: 4, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 4,
		},
			func([]byte) error { return nil },
			func() error { return nil },
		)
		if !resp.Success {
			t.Fatal("a snapshot we already cover should report Success")
		}
	})

	t.Run("successful install is success", func(t *testing.T) {
		rn := newTestNode(t, 1, []int{2, 3}, 3)
		rl := newTestLog(t)
		resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term: 1, LeaderID: 2, LastIncludedIndex: 50, LastIncludedTerm: 1,
		},
			func([]byte) error { return nil },
			func() error { return nil },
		)
		if !resp.Success {
			t.Fatal("a clean install did not report Success")
		}
	})
}

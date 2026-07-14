package raft

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// newTestLog builds a RaftLog backed by a fresh temp-dir WAL file. Using the
// real file lets the tests also verify durability/recovery, not just the cache.
func newTestLog(t *testing.T) *RaftLog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	t.Cleanup(func() { rl.Close() })
	return rl
}

// entry is a tiny helper for building log entries in tests.
func entry(index, term int, cmd string) LogEntry {
	return LogEntry{Index: index, Term: term, Command: []byte(cmd)}
}

// seedLeaderEntries appends entries as the leader would, so the log has a known
// starting shape. Returns the entries for convenience.
func seedLeaderEntries(t *testing.T, rl *RaftLog, terms ...int) []LogEntry {
	t.Helper()
	var out []LogEntry
	for _, term := range terms {
		e, err := rl.AppendLeader([]byte("x"), term)
		if err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
		out = append(out, e)
	}
	return out
}

func TestAppendEntriesToLog_EmptyLogAcceptsFromStart(t *testing.T) {
	rl := newTestLog(t)

	// Leader sends entry 1 with prevLogIndex 0 (nothing precedes it).
	match, _, _, ok := rl.AppendEntriesToLog(0, 0, []LogEntry{entry(1, 1, "a")})
	if !ok {
		t.Fatalf("expected append to succeed on empty log")
	}
	if match != 1 {
		t.Fatalf("matchIndex = %d, want 1", match)
	}
	if got := rl.LatestIndex(); got != 1 {
		t.Fatalf("LatestIndex = %d, want 1", got)
	}
}

func TestAppendEntriesToLog_RejectsMissingPrevEntry(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1) // log has entry {1,1}

	// Leader claims prevLogIndex=5 which we don't have. Must reject and tell the
	// leader to back up to just past our log's end (index 2).
	match, cterm, cindex, ok := rl.AppendEntriesToLog(5, 1, []LogEntry{entry(6, 1, "z")})
	if ok {
		t.Fatalf("expected rejection for missing prev entry")
	}
	if match != 0 {
		t.Fatalf("matchIndex = %d, want 0 on rejection", match)
	}
	if cterm != 0 {
		t.Fatalf("conflictTerm = %d, want 0 for a missing-entry rejection", cterm)
	}
	if cindex != 2 {
		t.Fatalf("conflictIndex = %d, want 2 (lastIndex+1)", cindex)
	}
}

func TestAppendEntriesToLog_RejectsTermMismatchWithConflictHint(t *testing.T) {
	rl := newTestLog(t)
	// Our log: index1 term1, index2 term2, index3 term2.
	seedLeaderEntries(t, rl, 1, 2, 2)

	// Leader sends prevLogIndex=3 but with prevLogTerm=1 (we hold term 2 there).
	// We must reject and report conflictTerm=2 and the FIRST index we hold for
	// term 2, which is index 2 — letting the leader skip the whole term in one hop.
	_, cterm, cindex, ok := rl.AppendEntriesToLog(3, 1, nil)
	if ok {
		t.Fatalf("expected rejection for term mismatch at prevLogIndex")
	}
	if cterm != 2 {
		t.Fatalf("conflictTerm = %d, want 2", cterm)
	}
	if cindex != 2 {
		t.Fatalf("conflictIndex = %d, want 2 (first index of conflicting term)", cindex)
	}
}

// TestAppendEntriesToLog_OverwritesDivergentEntries is the path we could not
// exercise on a live cluster: a follower holds uncommitted entries from a stale
// term, and the new leader forces them to be overwritten (§5.3 step 3).
func TestAppendEntriesToLog_OverwritesDivergentEntries(t *testing.T) {
	rl := newTestLog(t)
	// Follower's divergent log: {1,1}, {2,2}, {3,2}. The {2,2}/{3,2} entries
	// came from a leader that never committed them.
	seedLeaderEntries(t, rl, 1, 2, 2)

	// New leader (term 3) agrees up to index 1, then sends different entries for
	// indexes 2 and 3. prevLogIndex=1, prevLogTerm=1 matches our index 1.
	newEntries := []LogEntry{entry(2, 3, "new2"), entry(3, 3, "new3"), entry(4, 3, "new4")}
	match, _, _, ok := rl.AppendEntriesToLog(1, 1, newEntries)
	if !ok {
		t.Fatalf("expected overwrite append to succeed")
	}
	if match != 4 {
		t.Fatalf("matchIndex = %d, want 4", match)
	}

	// Index 2 and 3 must now hold the leader's term-3 entries, not the old term-2.
	for _, want := range newEntries {
		got, ok := rl.GetEntry(want.Index)
		if !ok {
			t.Fatalf("missing entry at index %d after overwrite", want.Index)
		}
		if got.Term != want.Term || string(got.Command) != string(want.Command) {
			t.Fatalf("entry %d = {term:%d cmd:%q}, want {term:%d cmd:%q}",
				want.Index, got.Term, got.Command, want.Term, want.Command)
		}
	}
	if got := rl.LatestIndex(); got != 4 {
		t.Fatalf("LatestIndex = %d, want 4", got)
	}
}

// TestAppendEntriesToLog_OverwriteSurvivesRecovery confirms the conflict
// truncation actually rewrote the WAL on disk — reopening the file must yield
// the overwritten log, not the stale one.
func TestAppendEntriesToLog_OverwriteSurvivesRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recover.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	seedLeaderEntries(t, rl, 1, 2, 2)

	if _, _, _, ok := rl.AppendEntriesToLog(1, 1, []LogEntry{entry(2, 3, "new2")}); !ok {
		t.Fatalf("overwrite append failed")
	}
	rl.Close()

	// Reopen from disk and confirm the recovered log matches the overwrite.
	rl2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopen NewRaftLog: %v", err)
	}
	defer rl2.Close()

	if got := rl2.LatestIndex(); got != 2 {
		t.Fatalf("recovered LatestIndex = %d, want 2 (truncated log)", got)
	}
	e, ok := rl2.GetEntry(2)
	if !ok || e.Term != 3 || string(e.Command) != "new2" {
		t.Fatalf("recovered entry 2 = %+v, want term 3 cmd new2", e)
	}
}

// TestAppendEntriesToLog_IdempotentOnMatchingEntries verifies that re-sending
// entries the follower already holds is a no-op (no truncation, no error),
// which matters for heartbeats and retransmissions.
func TestAppendEntriesToLog_IdempotentOnMatchingEntries(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 2)

	// Re-send the exact entries we already have.
	same := []LogEntry{entry(2, 1, "x"), entry(3, 2, "x")}
	match, _, _, ok := rl.AppendEntriesToLog(1, 1, same)
	if !ok {
		t.Fatalf("expected idempotent re-send to succeed")
	}
	if match != 3 {
		t.Fatalf("matchIndex = %d, want 3", match)
	}
	if got := rl.LatestIndex(); got != 3 {
		t.Fatalf("LatestIndex = %d, want 3 (unchanged)", got)
	}
}

// TestAppendEntriesToLog_HeartbeatIsNoOp verifies an empty-entries append (pure
// heartbeat) passes the consistency check and reports the prev index as match.
func TestAppendEntriesToLog_HeartbeatIsNoOp(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1)

	match, _, _, ok := rl.AppendEntriesToLog(2, 1, nil)
	if !ok {
		t.Fatalf("heartbeat consistency check should pass")
	}
	if match != 2 {
		t.Fatalf("matchIndex = %d, want 2 for heartbeat at prevLogIndex 2", match)
	}
}

// TestRecoverPartialFrame reproduces review bug #5: a node that crashed
// mid-write leaves a truncated trailing frame in the WAL. Recovery must load
// the good entries, truncate the partial tail, and succeed — on every platform.
// The buggy recover() called Truncate on the O_APPEND handle, which fails with
// "Access is denied" on Windows, so a crashed node could not restart.
func TestRecoverPartialFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.wal")

	// Write two good entries via the normal durable path.
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	seedLeaderEntries(t, rl, 1, 1)
	rl.Close()

	// Append a corrupt trailing frame: a length prefix that promises more bytes
	// than actually follow (simulating a crash mid-write).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(999)); err != nil {
		t.Fatalf("write bogus length: %v", err)
	}
	f.Write([]byte("truncated")) // fewer than 999 bytes
	f.Close()

	// Recovery must succeed, keep the 2 good entries, and truncate the bad tail.
	rl2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("recovery of partial WAL failed (review bug #5): %v", err)
	}
	defer rl2.Close()

	if got := rl2.LatestIndex(); got != 2 {
		t.Fatalf("recovered LatestIndex = %d, want 2 (good entries only)", got)
	}

	// The truncation must be durable: a subsequent append lands at index 3, and
	// reopening once more yields a clean 3-entry log (no resurrected bad frame).
	if _, err := rl2.AppendLeader([]byte("x"), 2); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if got := rl2.LatestIndex(); got != 3 {
		t.Fatalf("post-recovery append: LatestIndex = %d, want 3", got)
	}
}

// ---- Compaction / index-offset (review bug #4) ----

// TestCompactionPreservesIndexMath is the direct regression for review bug #4:
// after TruncateBeforeIndex, GetEntry must still return the correct entry for a
// post-boundary index (previously it read cache[index-1] and was off by the
// compacted amount).
func TestCompactionPreservesIndexMath(t *testing.T) {
	rl := newTestLog(t)
	// Log: idx1:t1 .. idx5:t1.
	seedLeaderEntries(t, rl, 1, 1, 1, 1, 1)

	// Compact through index 3 (term 1). Entries 1-3 discarded; 4,5 remain.
	if err := rl.TruncateBeforeIndex(3, 1); err != nil {
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}

	if got := rl.FirstIndex(); got != 4 {
		t.Fatalf("FirstIndex = %d, want 4 after compaction", got)
	}
	if got := rl.LatestIndex(); got != 5 {
		t.Fatalf("LatestIndex = %d, want 5", got)
	}

	// GetEntry(4) and (5) must return the right entries, not off-by-3 garbage.
	for _, idx := range []int{4, 5} {
		e, ok := rl.GetEntry(idx)
		if !ok {
			t.Fatalf("GetEntry(%d) missing after compaction", idx)
		}
		if e.Index != idx {
			t.Fatalf("GetEntry(%d) returned entry with Index %d (off-by-N bug)", idx, e.Index)
		}
	}

	// Compacted indices are gone.
	if _, ok := rl.GetEntry(2); ok {
		t.Fatalf("GetEntry(2) should be gone after compaction through 3")
	}
}

// TestTermAtSnapshotBoundary verifies TermAt answers lastIncludedTerm at the
// boundary index, which the post-snapshot consistency check needs (Figure 12).
func TestTermAtSnapshotBoundary(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 2, 2, 3) // idx1:t1, idx2:t2, idx3:t2, idx4:t3

	if err := rl.TruncateBeforeIndex(3, 2); err != nil { // boundary idx3 term2
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}

	if got := rl.TermAt(3); got != 2 {
		t.Fatalf("TermAt(boundary=3) = %d, want 2 (lastIncludedTerm)", got)
	}
	if got := rl.TermAt(4); got != 3 {
		t.Fatalf("TermAt(4) = %d, want 3", got)
	}
}

// TestGetEntriesFromAfterCompaction confirms replication reads are correct
// post-compaction and that requesting a compacted index returns nil (signaling
// the leader to fall back to InstallSnapshot).
func TestGetEntriesFromAfterCompaction(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 1, 1, 1) // idx1..5
	if err := rl.TruncateBeforeIndex(3, 1); err != nil {
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}

	got := rl.GetEntriesFrom(4)
	if len(got) != 2 || got[0].Index != 4 || got[1].Index != 5 {
		t.Fatalf("GetEntriesFrom(4) = %+v, want entries 4,5", got)
	}

	// A compacted index is unavailable via the log.
	if got := rl.GetEntriesFrom(2); got != nil {
		t.Fatalf("GetEntriesFrom(2) = %+v, want nil (compacted away)", got)
	}
}

// TestConflictTruncationAfterCompaction ensures the follower splice slice math
// uses the offset: overwriting a post-boundary entry must not panic or corrupt.
func TestConflictTruncationAfterCompaction(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 1, 2, 2) // idx1..5
	if err := rl.TruncateBeforeIndex(3, 1); err != nil {
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}
	// Cache now holds idx4:t2, idx5:t2, firstIndex=4.

	// Leader agrees up to idx4 (term 2), sends a conflicting idx5:t3.
	match, _, _, ok := rl.AppendEntriesToLog(4, 2, []LogEntry{entry(5, 3, "new5")})
	if !ok {
		t.Fatalf("expected append/overwrite to succeed post-compaction")
	}
	if match != 5 {
		t.Fatalf("matchIndex = %d, want 5", match)
	}
	e, _ := rl.GetEntry(5)
	if e.Term != 3 || string(e.Command) != "new5" {
		t.Fatalf("entry 5 = {t:%d cmd:%q}, want {t:3 cmd:new5}", e.Term, e.Command)
	}
}

// TestRecoverCompactedWAL simulates restart after compaction: the on-disk WAL
// starts at index 4, and RestoreOffset re-establishes the boundary. Index math
// must be correct after reload.
func TestRecoverCompactedWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compacted.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	seedLeaderEntries(t, rl, 1, 1, 1, 2, 2) // idx1..5
	if err := rl.TruncateBeforeIndex(3, 1); err != nil {
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}
	rl.Close()

	// Reopen: recover() reads the WAL (entries 4,5) and sets firstIndex from the
	// first entry. Then the startup path calls RestoreOffset with the snapshot
	// meta (index 3, term 1).
	rl2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rl2.Close()
	rl2.RestoreOffset(3, 1)

	if got := rl2.FirstIndex(); got != 4 {
		t.Fatalf("recovered FirstIndex = %d, want 4", got)
	}
	if got := rl2.LatestIndex(); got != 5 {
		t.Fatalf("recovered LatestIndex = %d, want 5", got)
	}
	e, ok := rl2.GetEntry(4)
	if !ok || e.Index != 4 {
		t.Fatalf("recovered GetEntry(4) wrong: %+v ok=%v", e, ok)
	}
	if got := rl2.TermAt(3); got != 1 {
		t.Fatalf("recovered TermAt(boundary) = %d, want 1", got)
	}
}

// TestRecoverFullyCompactedWAL covers the empty-cache case: everything was
// compacted, so the WAL is empty and RestoreOffset must seed firstIndex and
// nextIndex from the snapshot alone.
func TestRecoverFullyCompactedWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	seedLeaderEntries(t, rl, 1, 1, 1) // idx1..3
	if err := rl.TruncateBeforeIndex(3, 1); err != nil {
		t.Fatalf("TruncateBeforeIndex: %v", err)
	}
	rl.Close()

	rl2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rl2.Close()
	rl2.RestoreOffset(3, 1)

	if got := rl2.LatestIndex(); got != 3 {
		t.Fatalf("fully-compacted LatestIndex = %d, want 3 (boundary)", got)
	}
	if got := rl2.LatestTerm(); got != 1 {
		t.Fatalf("fully-compacted LatestTerm = %d, want 1", got)
	}
	// Next leader append must land at index 4, not 1.
	e, err := rl2.AppendLeader([]byte("x"), 5)
	if err != nil {
		t.Fatalf("AppendLeader after full compaction: %v", err)
	}
	if e.Index != 4 {
		t.Fatalf("post-compaction append landed at index %d, want 4", e.Index)
	}
}

// ---- InstallSnapshot log reset (Figure 13 steps 6–7) ----

// TestResetToSnapshot_RetainSuffix covers step 6: the follower already holds an
// entry at lastIncludedIndex with the matching term, so entries after it are
// retained and only the boundary advances.
func TestResetToSnapshot_RetainSuffix(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 2, 2, 2) // idx1..5

	// Snapshot covers through idx3 (term 2), which we hold. Entries 4,5 stay.
	if err := rl.ResetToSnapshot(3, 2); err != nil {
		t.Fatalf("ResetToSnapshot: %v", err)
	}
	if got := rl.FirstIndex(); got != 4 {
		t.Fatalf("FirstIndex = %d, want 4", got)
	}
	if got := rl.LatestIndex(); got != 5 {
		t.Fatalf("LatestIndex = %d, want 5 (suffix retained)", got)
	}
	if e, ok := rl.GetEntry(4); !ok || e.Index != 4 {
		t.Fatalf("expected entry 4 retained, got %+v ok=%v", e, ok)
	}
}

// TestResetToSnapshot_DiscardWholeLog covers step 7: the follower's log conflicts
// with (or is entirely behind) the snapshot, so the whole log is discarded and
// the node starts fresh at the boundary.
func TestResetToSnapshot_DiscardWholeLog(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 1) // idx1..3, all term 1

	// Snapshot covers through idx5 term 4 — we don't hold idx5 at all, and our
	// entries are stale. Discard everything; next append lands at idx6.
	if err := rl.ResetToSnapshot(5, 4); err != nil {
		t.Fatalf("ResetToSnapshot: %v", err)
	}
	if got := rl.FirstIndex(); got != 6 {
		t.Fatalf("FirstIndex = %d, want 6", got)
	}
	if got := rl.LatestIndex(); got != 5 {
		t.Fatalf("LatestIndex = %d, want 5 (boundary, empty cache)", got)
	}
	if got := rl.TermAt(5); got != 4 {
		t.Fatalf("TermAt(boundary) = %d, want 4", got)
	}
	e, err := rl.AppendLeader([]byte("x"), 4)
	if err != nil {
		t.Fatalf("AppendLeader after reset: %v", err)
	}
	if e.Index != 6 {
		t.Fatalf("post-reset append landed at %d, want 6", e.Index)
	}
}

// TestResetToSnapshot_StaleIsIgnored verifies a snapshot at or below the current
// boundary is a no-op (retransmission / out-of-order delivery).
func TestResetToSnapshot_StaleIsIgnored(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 1, 1, 1)
	if err := rl.ResetToSnapshot(3, 1); err != nil {
		t.Fatalf("first reset: %v", err)
	}
	// A stale snapshot at index 2 must not move the boundary back.
	if err := rl.ResetToSnapshot(2, 1); err != nil {
		t.Fatalf("stale reset: %v", err)
	}
	if got := rl.FirstIndex(); got != 4 {
		t.Fatalf("FirstIndex = %d, want 4 (stale snapshot ignored)", got)
	}
}

// TestResetToSnapshot_SurvivesRecovery confirms the log reset is durable: after
// discarding the whole log and installing a snapshot boundary, reopening the WAL
// and re-seeding the offset yields the same empty-but-positioned log.
func TestResetToSnapshot_SurvivesRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapreset.wal")
	rl, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	seedLeaderEntries(t, rl, 1, 1, 1)
	if err := rl.ResetToSnapshot(5, 4); err != nil {
		t.Fatalf("ResetToSnapshot: %v", err)
	}
	rl.Close()

	rl2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rl2.Close()
	rl2.RestoreOffset(5, 4)

	if got := rl2.LatestIndex(); got != 5 {
		t.Fatalf("recovered LatestIndex = %d, want 5", got)
	}
	e, err := rl2.AppendLeader([]byte("y"), 4)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if e.Index != 6 {
		t.Fatalf("recovered append landed at %d, want 6", e.Index)
	}
}

func TestGetEntriesFrom(t *testing.T) {
	rl := newTestLog(t)
	seedLeaderEntries(t, rl, 1, 1, 2, 2, 3)

	got := rl.GetEntriesFrom(3)
	if len(got) != 3 {
		t.Fatalf("GetEntriesFrom(3) returned %d entries, want 3", len(got))
	}
	if got[0].Index != 3 || got[2].Index != 5 {
		t.Fatalf("GetEntriesFrom(3) = indexes %d..%d, want 3..5", got[0].Index, got[2].Index)
	}

	// Past the end returns nil.
	if got := rl.GetEntriesFrom(99); got != nil {
		t.Fatalf("GetEntriesFrom past end = %v, want nil", got)
	}
}

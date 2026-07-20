package engine

import (
	"path/filepath"
	"testing"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// Compaction is the one operation that destroys data on purpose, so the index it
// is told to compact to has to be exactly right. §7 defines the snapshot's last
// included index as "the last entry in the log that the snapshot replaces (the
// last entry the state machine had applied)" — applied, not committed. The two
// diverge routinely on a follower, whose commitIndex is advanced from the HTTP
// goroutine by SetFollowerCommitIndex and can jump between the apply pass and the
// compaction pass of a single tick.

// newCompactEngine builds an engine with a real log, state machine and snapshot
// store, holding entryCount single-term entries.
func newCompactEngine(t *testing.T, entryCount int) (*Engine, *raft.RaftLog, *store.SnapshotManager, *raft.RaftNode) {
	t.Helper()
	dir := t.TempDir()

	rl, err := raft.NewRaftLog(filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	t.Cleanup(func() { rl.Close() })
	rl.SetCompactionThreshold(1) // compact at the first opportunity

	for i := 0; i < entryCount; i++ {
		cmd, err := raft.EncodeCommand(raft.Command{Op: raft.OpSet, Key: "k", Value: "v"})
		if err != nil {
			t.Fatalf("EncodeCommand: %v", err)
		}
		if _, err := rl.AppendLeader(cmd, 1); err != nil {
			t.Fatalf("AppendLeader: %v", err)
		}
	}

	node, err := raft.NewRaftNode(1, []int{2, 3}, 3, filepath.Join(dir, "raft.meta"), raft.Follower)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	sm := store.NewMemoryStateMachine()
	snaps := store.NewSnapshotManager(filepath.Join(dir, "snapshot.json"))
	cfg := &config.Config{NodeID: 1, DataDir: dir, Peers: map[int]string{2: "a", 3: "b"}}

	return New(cfg, node, rl, sm, snaps), rl, snaps, node
}

// The follower case: commitIndex has run ahead of lastApplied. Compacting to
// commitIndex would write a snapshot claiming entries 4 and 5 whose effects are
// not in it, then delete those entries — unrecoverable divergence. Compaction
// must stop at lastApplied.
func TestCompactUsesLastApplied(t *testing.T) {
	e, rl, snaps, node := newCompactEngine(t, 5)

	node.CommitIndex = 5
	node.LastApplied = 3

	e.maybeCompact()

	snap, err := snaps.Read()
	if err != nil {
		t.Fatalf("Read snapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("no snapshot written, expected compaction to run")
	}
	if snap.Meta.LastIncludedIndex != 3 {
		t.Fatalf("snapshot LastIncludedIndex = %d, want 3 (lastApplied)",
			snap.Meta.LastIncludedIndex)
	}

	// Entries 4 and 5 are committed but unapplied — they must survive, because
	// the apply loop still has to read them.
	if got := rl.FirstIndex(); got != 4 {
		t.Fatalf("FirstIndex = %d, want 4", got)
	}
	for _, idx := range []int{4, 5} {
		if _, ok := rl.GetEntry(idx); !ok {
			t.Fatalf("entry %d was truncated but has not been applied", idx)
		}
	}
}

// TermAt returns 0 for an index it cannot resolve. Compacting anyway writes a
// snapshot with LastIncludedTerm 0, and TruncateBeforeIndex then bails out on the
// already-compacted check — leaving a persisted snapshot whose term is a lie.
// After a restart RestoreOffset seeds that 0, and every AppendEntries consistency
// check against the snapshot boundary fails forever.
func TestCompactRefusesUnknownTerm(t *testing.T) {
	e, rl, snaps, node := newCompactEngine(t, 3)

	// lastApplied points past the end of the log: TermAt cannot resolve it.
	node.CommitIndex = 9
	node.LastApplied = 9

	e.maybeCompact()

	snap, err := snaps.Read()
	if err != nil {
		t.Fatalf("Read snapshot: %v", err)
	}
	if snap != nil {
		t.Fatalf("snapshot written for an unresolvable term: %+v", snap.Meta)
	}
	if got := rl.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1 (log must be untouched)", got)
	}
}

// The leader case, where commitIndex and lastApplied agree: compaction still runs
// and still covers everything applied.
func TestCompactRunsWhenFullyApplied(t *testing.T) {
	e, rl, snaps, node := newCompactEngine(t, 5)

	node.CommitIndex = 5
	node.LastApplied = 5

	e.maybeCompact()

	snap, err := snaps.Read()
	if err != nil {
		t.Fatalf("Read snapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("no snapshot written, expected compaction to run")
	}
	if snap.Meta.LastIncludedIndex != 5 {
		t.Fatalf("snapshot LastIncludedIndex = %d, want 5", snap.Meta.LastIncludedIndex)
	}
	if got := rl.FirstIndex(); got != 6 {
		t.Fatalf("FirstIndex = %d, want 6", got)
	}
}

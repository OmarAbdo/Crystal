package engine

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// The apply loop is where State Machine Safety (§5.4.3) is either upheld or
// quietly abandoned. The property is that every replica applies the same entries
// in the same order; a replica that skips one has diverged from the cluster and
// there is no mechanism in Raft to notice or repair that — its state machine is
// simply wrong from then on, and it will happily serve reads from it.
//
// None of the three failure conditions here is recoverable at runtime, so the
// only honest response is to stop. A crashed node rejoins and catches up; a
// diverged node does not.

// haltingEngine wraps an Engine whose fatalf is captured instead of exiting.
type haltingEngine struct {
	*Engine
	halts []string
}

func (h *haltingEngine) halted() bool { return len(h.halts) > 0 }

// errStateMachine applies successfully until failAt, then fails forever.
type errStateMachine struct {
	store.StateMachine
	failAt int
}

func (e *errStateMachine) Apply(index int, cmd raft.Command) error {
	if index >= e.failAt {
		return errors.New("simulated state machine failure")
	}
	return e.StateMachine.Apply(index, cmd)
}

func newApplyEngine(t *testing.T, sm store.StateMachine) (*haltingEngine, *raft.RaftLog, *raft.RaftNode) {
	t.Helper()
	dir := t.TempDir()

	rl, err := raft.NewRaftLog(filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	t.Cleanup(func() { rl.Close() })

	node, err := raft.NewRaftNode(1, []int{2, 3}, 3, filepath.Join(dir, "raft.meta"), raft.Follower)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	if sm == nil {
		sm = store.NewMemoryStateMachine()
	}
	snaps := store.NewSnapshotManager(filepath.Join(dir, "snapshot.json"))
	cfg := &config.Config{NodeID: 1, DataDir: dir, Peers: map[int]string{2: "a", 3: "b"}}

	h := &haltingEngine{Engine: New(cfg, node, rl, sm, snaps)}
	h.Engine.fatalf = func(format string, args ...any) {
		h.halts = append(h.halts, format)
	}
	return h, rl, node
}

func appendSet(t *testing.T, rl *raft.RaftLog, key string) {
	t.Helper()
	cmd, err := raft.EncodeCommand(raft.Command{Op: raft.OpSet, Key: key, Value: "v"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	if _, err := rl.AppendLeader(cmd, 1); err != nil {
		t.Fatalf("AppendLeader: %v", err)
	}
}

func TestApplyCommitted_AppliesInOrder(t *testing.T) {
	h, rl, node := newApplyEngine(t, nil)
	appendSet(t, rl, "a")
	appendSet(t, rl, "b")
	node.CommitIndex = 2

	h.applyCommitted()

	if h.halted() {
		t.Fatalf("unexpected halt: %v", h.halts)
	}
	if _, lastApplied := node.CommitAndApplyBoundary(); lastApplied != 2 {
		t.Fatalf("lastApplied = %d, want 2", lastApplied)
	}
	if _, ok := h.stateMachine.Get("b"); !ok {
		t.Fatal("entry 2 was not applied")
	}
}

// A committed index with no entry behind it means the log lost something it had
// promised to keep. Skipping it and advancing lastApplied — the old behavior —
// diverges this replica from every other one, silently.
func TestApplyCommitted_HaltsOnMissingEntry(t *testing.T) {
	h, rl, node := newApplyEngine(t, nil)
	appendSet(t, rl, "a")
	node.CommitIndex = 3 // entries 2 and 3 do not exist

	h.applyCommitted()

	if !h.halted() {
		t.Fatal("expected a halt on a missing log entry, got none")
	}
	if !strings.Contains(h.halts[0], "missing") {
		t.Fatalf("halt message = %q, want it to name the missing entry", h.halts[0])
	}
	// lastApplied must not have run past the entry we could not apply.
	if _, lastApplied := node.CommitAndApplyBoundary(); lastApplied != 1 {
		t.Fatalf("lastApplied = %d, want 1 (must not advance past a failure)", lastApplied)
	}
}

func TestApplyCommitted_HaltsOnCorruptCommand(t *testing.T) {
	h, rl, node := newApplyEngine(t, nil)
	appendSet(t, rl, "a")
	if _, err := rl.AppendLeader([]byte("{not valid json"), 1); err != nil {
		t.Fatalf("AppendLeader: %v", err)
	}
	node.CommitIndex = 2

	h.applyCommitted()

	if !h.halted() {
		t.Fatal("expected a halt on an undecodable command, got none")
	}
	if _, lastApplied := node.CommitAndApplyBoundary(); lastApplied != 1 {
		t.Fatalf("lastApplied = %d, want 1", lastApplied)
	}
}

// An Apply that returns an error has, by definition, not applied. Counting it as
// applied is the same divergence by a different route.
func TestApplyCommitted_HaltsOnStateMachineError(t *testing.T) {
	sm := &errStateMachine{StateMachine: store.NewMemoryStateMachine(), failAt: 2}
	h, rl, node := newApplyEngine(t, sm)
	appendSet(t, rl, "a")
	appendSet(t, rl, "b")
	node.CommitIndex = 2

	h.applyCommitted()

	if !h.halted() {
		t.Fatal("expected a halt on a state machine error, got none")
	}
	if _, lastApplied := node.CommitAndApplyBoundary(); lastApplied != 1 {
		t.Fatalf("lastApplied = %d, want 1 (entry 2 did not apply)", lastApplied)
	}
}

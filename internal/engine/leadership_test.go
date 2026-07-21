package engine

import (
	"path/filepath"
	"testing"
	"time"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// Leadership in this engine has historically had two owners: the control loop,
// and any HTTP goroutine that calls BecomeFollower from an RPC receiver. These
// tests pin the rules that keep the two from contradicting each other.

// newLeaderEngine returns an engine whose node is Leader at the given term.
func newLeaderEngine(t *testing.T, term int) (*Engine, *raft.RaftNode) {
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

	// Climb to `term` as leader: sit at term-1, campaign into term, win.
	if err := node.BecomeFollower(term-1, 0); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	got, err := node.BecomeCandidate()
	if err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if got != term {
		t.Fatalf("candidate term = %d, want %d", got, term)
	}
	if !node.BecomeLeader(term, rl.LatestIndex()) {
		t.Fatal("BecomeLeader refused")
	}

	cfg := &config.Config{NodeID: 1, DataDir: dir, Peers: map[int]string{2: "a", 3: "b"}}
	e := NewWithTransport(cfg, node, rl, store.NewMemoryStateMachine(),
		store.NewSnapshotManager(filepath.Join(dir, "snapshot.json")), stubTransport{})
	return e, node
}

// stubTransport answers nothing; these tests never exercise the network.
type stubTransport struct{}

func (stubTransport) AppendEntries(string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errUnreachable
}
func (stubTransport) PreVote(string, raft.PreVoteRequest) (raft.PreVoteResponse, error) {
	return raft.PreVoteResponse{}, errUnreachable
}
func (stubTransport) ReadIndex(string, raft.ReadIndexRequest) (raft.ReadIndexResponse, error) {
	return raft.ReadIndexResponse{}, errUnreachable
}
func (stubTransport) RequestVote(string, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errUnreachable
}
func (stubTransport) InstallSnapshot(string, raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return raft.InstallSnapshotResponse{}, errUnreachable
}

// ---- F8: the stepdown guard ----

// stepDownCh is buffered, so a term reported by a replicator can arrive after
// the situation that produced it has already been resolved -- including after
// this node has won a NEW election at that very term. Deposing a leader on a
// term that is not actually higher than its own turns one stale message into a
// self-sustaining oscillation: step down, re-elect, receive the next stale
// report, step down again.
func TestHandleStepDown_IgnoresStaleTerm(t *testing.T) {
	e, node := newLeaderEngine(t, 7)

	// A report for our own term proves nothing: we are the leader of term 7.
	e.handleStepDown(7)

	if !node.IsLeader() {
		t.Fatal("leader stepped down on a report of its own term")
	}
	if got := node.CurrentTerm(); got != 7 {
		t.Fatalf("term = %d, want 7", got)
	}
}

func TestHandleStepDown_IgnoresOlderTerm(t *testing.T) {
	e, node := newLeaderEngine(t, 7)

	e.handleStepDown(5)

	if !node.IsLeader() {
		t.Fatal("leader stepped down on a report of an older term")
	}
}

func TestHandleStepDown_YieldsToHigherTerm(t *testing.T) {
	e, node := newLeaderEngine(t, 7)

	e.handleStepDown(9)

	if node.IsLeader() {
		t.Fatal("leader ignored a genuinely higher term")
	}
	if got := node.CurrentTerm(); got != 9 {
		t.Fatalf("term = %d, want 9", got)
	}
}

// A pending proposal must not survive a stepdown: whatever the new leader does
// with that index, this node can no longer promise it committed.
func TestHandleStepDown_FailsPendingWaiters(t *testing.T) {
	e, _ := newLeaderEngine(t, 7)

	w, ch := mkWaiter(1, time.Now().Add(time.Minute))
	w.term = 7
	e.waiters = []*waiter{w}

	e.handleStepDown(9)

	if err := recvNoBlock(t, ch); err != ErrNotLeader {
		t.Fatalf("waiter got %v, want ErrNotLeader", err)
	}
	if len(e.waiters) != 0 {
		t.Fatalf("waiters remain after stepdown: %d", len(e.waiters))
	}
}

// ---- F7: reconciling leadership changed out-of-band ----

// A leader deposed by an inbound RPC (on an HTTP goroutine) leaves the control
// loop believing it is still leading. reconcileLeadership is what closes that
// gap, and it runs first on every tick.
func TestReconcileInboundStepdown(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	e.startReplicators()
	if len(e.replicators) == 0 {
		t.Fatal("expected replicators after promotion")
	}

	w, ch := mkWaiter(1, time.Now().Add(time.Minute))
	w.term = 7
	e.waiters = []*waiter{w}

	// An inbound higher-term RPC deposes us without telling the control loop.
	if err := node.BecomeFollower(9, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}

	e.reconcileLeadership()

	if len(e.replicators) != 0 {
		t.Fatalf("%d replicators still running after stepdown", len(e.replicators))
	}
	if err := recvNoBlock(t, ch); err != ErrNotLeader {
		t.Fatalf("waiter got %v, want ErrNotLeader", err)
	}
}

// The damaging half of the same bug: replicators that survive a stepdown and are
// then REUSED after a new election. Each one compares follower replies against
// the term it was started for, so under a newer term every ordinary reply looks
// like a higher term and trips a stepdown — the node deposes itself moments
// after winning. startReplicators must replace a stale set, not short-circuit on
// a non-empty map.
func TestStartReplicatorsReplacesStaleTerm(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	e.startReplicators()

	if e.replicatorsTerm != 7 {
		t.Fatalf("replicatorsTerm = %d, want 7", e.replicatorsTerm)
	}
	for _, pr := range e.replicators {
		if pr.term != 7 {
			t.Fatalf("replicator for peer %d has term %d, want 7", pr.peerID, pr.term)
		}
	}

	// Deposed out of band, then re-elected at a later term.
	if err := node.BecomeFollower(8, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	term, err := node.BecomeCandidate() // → 9
	if err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if !node.BecomeLeader(term, 0) {
		t.Fatal("BecomeLeader refused")
	}

	e.startReplicators()

	if e.replicatorsTerm != term {
		t.Fatalf("replicatorsTerm = %d, want %d", e.replicatorsTerm, term)
	}
	for _, pr := range e.replicators {
		if pr.term != term {
			t.Fatalf("peer %d still replicating for stale term %d (current %d) — "+
				"its replies will read as higher terms and depose us",
				pr.peerID, pr.term, term)
		}
	}
}

// Re-entering reconcile while already leading at the same term must not churn
// the replicators; they are long-lived by design.
func TestReconcileLeadershipIsIdempotent(t *testing.T) {
	e, _ := newLeaderEngine(t, 7)

	e.reconcileLeadership()
	first := make(map[int]*peerReplicator, len(e.replicators))
	for id, pr := range e.replicators {
		first[id] = pr
	}
	if len(first) == 0 {
		t.Fatal("expected reconcile to start replicators for a leader")
	}

	e.reconcileLeadership()

	for id, pr := range e.replicators {
		if first[id] != pr {
			t.Fatalf("peer %d replicator was replaced on an idempotent reconcile", id)
		}
	}
}

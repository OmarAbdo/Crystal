package engine

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// ReadIndex (F12) and CheckQuorum (F19) both rest on one claim: that this node is
// still the leader at the instant it answers. A leader cannot know that on its
// own — nothing tells a deposed leader it has been replaced — so the only honest
// evidence is a majority saying so, and the evidence has to be NEWER than the
// question. These tests pin exactly that.

// testConfig builds a simple voter configuration from a list of node IDs.
func testConfig(ids ...int) raft.Configuration {
	voters := make(map[int]string, len(ids))
	for _, id := range ids {
		voters[id] = fmt.Sprintf("peer-%d", id)
	}
	return raft.NewConfiguration(voters)
}

// newLeaderEngineSized builds an engine whose node is Leader at term, in a
// cluster of the given size. Peers are numbered 2..size.
func newLeaderEngineSized(t *testing.T, term, size int) (*Engine, *raft.RaftNode) {
	t.Helper()
	dir := t.TempDir()

	peers := make(map[int]string, size-1)
	peerIDs := make([]int, 0, size-1)
	for id := 2; id <= size; id++ {
		peers[id] = fmt.Sprintf("peer-%d", id)
		peerIDs = append(peerIDs, id)
	}

	rl, err := raft.NewRaftLog(filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	t.Cleanup(func() { rl.Close() })

	voters := map[int]string{1: "self"}
	for _, pid := range peerIDs {
		voters[pid] = peers[pid]
	}
	node, err := raft.NewRaftNode(1, raft.NewConfiguration(voters),
		filepath.Join(dir, "raft.meta"), raft.Follower)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	if err := node.BecomeFollower(term-1, 0); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	if _, err := node.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if !node.BecomeLeader(term, rl.LatestIndex()) {
		t.Fatal("BecomeLeader refused")
	}

	cfg := &config.Config{NodeID: 1, DataDir: dir, Peers: peers}
	e := NewWithTransport(cfg, node, rl, store.NewMemoryStateMachine(),
		store.NewSnapshotManager(filepath.Join(dir, "snapshot.json")), stubTransport{})
	return e, node
}

// assertPending fails if a read has already been resolved.
func assertPending(t *testing.T, ch chan readResult, msg string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s (got %+v)", msg, v)
	default:
	}
}

// recvRead reads one resolved read result that must already be ready.
func recvRead(t *testing.T, ch chan readResult) readResult {
	t.Helper()
	select {
	case v := <-ch:
		return v
	default:
		t.Fatalf("expected a value on the read channel, none present")
		return readResult{}
	}
}

// ---- F12: ReadIndex ----

// The trap this mechanism exists to avoid, and the one that makes a naive
// implementation silently wrong. A replication round that STARTED before the read
// was admitted carries a follower assertion made before the read existed. When
// that round's reply lands after the read registers, counting it confirms a
// quorum on evidence predating the request — the exact stale-read hole §8 closes.
// The dissertation (§6.4) requires heartbeats *initiated after* the read index is
// recorded, which is why rounds are numbered at send rather than timed on arrival.
func TestReadIndex_IgnoresAcksFromRoundsStartedBeforeTheRead(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 5
	node.LastApplied = 5

	// A round is already in flight when the read arrives.
	staleSeq := e.nextRoundSeq()

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})
	assertPending(t, ch, "read released before any quorum evidence")

	// Its reply lands now. The ack is newer than the read; the EVIDENCE is not.
	e.handleAck(ackReport{peerID: 2, startSeq: staleSeq})
	assertPending(t, ch, "read released on evidence that predates it")

	// A round begun after the read is admissible.
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	if r := recvRead(t, ch); r.err != nil {
		t.Fatalf("read failed on valid post-read evidence: %v", r.err)
	}
}

// §8 precaution 1: "a leader must have the latest information on which entries
// are committed. […] To find out, it needs to commit an entry from its term."
// Until the term's no-op commits, commitIndex may understate the truth and a read
// against it could miss a write that is already committed cluster-wide.
func TestReadIndex_WaitsForTheTermNoopToCommit(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 5
	node.LastApplied = 5
	e.noopIndex = 9 // appended on election, not yet committed

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	assertPending(t, ch, "read released before the term's no-op committed")

	node.CommitIndex = 9
	node.LastApplied = 9
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	if r := recvRead(t, ch); r.err != nil {
		t.Fatalf("read failed after the no-op committed: %v", r.err)
	}
}

// The read index is a promise about what the state machine contains. Releasing
// before lastApplied reaches it lets the caller observe state older than the
// index the read was admitted at.
func TestReadIndex_WaitsForStateMachineToCatchUp(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 8
	node.LastApplied = 3 // committed, not yet applied

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	assertPending(t, ch, "read released while the state machine lagged the read index")

	node.LastApplied = 8
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	if r := recvRead(t, ch); r.err != nil {
		t.Fatalf("read failed once applied caught up: %v", r.err)
	}
}

func TestReadIndex_RequiresAMajority(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 5)
	node.CommitIndex = 5
	node.LastApplied = 5

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})

	// Self + 1 = 2 of 5. Not a majority.
	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	assertPending(t, ch, "read released with 2 of 5 nodes confirming")

	// Self + 2 = 3 of 5.
	e.handleAck(ackReport{peerID: 3, startSeq: e.nextRoundSeq()})
	if r := recvRead(t, ch); r.err != nil {
		t.Fatalf("read failed with a genuine majority: %v", r.err)
	}
}

// A read admitted under one term must never be answered under another: the node
// may have been deposed and re-elected, and the state it would read is no longer
// the state the read was promised.
func TestReadIndex_FailsOnTermChange(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 5
	node.LastApplied = 5

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})
	assertPending(t, ch, "read released immediately")

	if err := node.BecomeFollower(9, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	e.fireReadWaiters()

	if r := recvRead(t, ch); r.err != ErrNotLeader {
		t.Fatalf("read got %v, want ErrNotLeader after being deposed", r.err)
	}
}

// A read that cannot be confirmed is REFUSED, never answered from local state.
// Falling back would produce exactly the stale answer the confirmation prevents.
func TestReadIndex_TimesOutRatherThanServingStale(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 5
	node.LastApplied = 5

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})

	// No acks ever arrive — we are partitioned.
	e.sweepExpiredReads(time.Now().Add(readTimeout + time.Second))

	if r := recvRead(t, ch); r.err != ErrReadTimeout {
		t.Fatalf("read got %v, want ErrReadTimeout", r.err)
	}
}

// A single-node cluster is its own majority and must not wait for anybody.
func TestReadIndex_SingleNodeClusterServesImmediately(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 1)
	node.CommitIndex = 5
	node.LastApplied = 5

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})

	if r := recvRead(t, ch); r.err != nil {
		t.Fatalf("single-node read got %v, want nil", r.err)
	}
}

// ---- F19: CheckQuorum ----

func TestCheckQuorum_KeepsLeadershipWhileAcksArrive(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	e.resetLeadershipEvidence()

	e.handleAck(ackReport{peerID: 2, startSeq: e.nextRoundSeq()})
	e.checkQuorum()

	if !node.IsLeader() {
		t.Fatal("leader stepped down while a majority was responding")
	}
}

// The F19 failure itself: a leader cut off from its cluster keeps the role
// indefinitely, and everything built on leadership trusts that claim.
func TestCheckQuorum_StepsDownWhenMajorityGoesSilent(t *testing.T) {
	e, node := newLeaderEngine(t, 7)

	stale := time.Now().Add(-2 * quorumGrace)
	e.lastAck = map[int]peerAck{
		2: {startSeq: 1, at: stale},
		3: {startSeq: 1, at: stale},
	}

	e.checkQuorum()

	if node.IsLeader() {
		t.Fatal("leader retained the role after a full grace period of silence")
	}
	// The term must NOT be bumped: we have no evidence of a higher one, only
	// evidence that we can no longer prove we hold this one. Inventing a term
	// here would disrupt whichever leader the majority side has settled on.
	if got := node.CurrentTerm(); got != 7 {
		t.Fatalf("term = %d, want 7 — stepping down must not invent a new term", got)
	}
}

func TestCheckQuorum_FailsPendingReadsOnStepdown(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	node.CommitIndex = 5
	node.LastApplied = 5

	ch := make(chan readResult, 1)
	e.handleRead(Read{ResultCh: ch, awaitApply: true})

	stale := time.Now().Add(-2 * quorumGrace)
	e.lastAck = map[int]peerAck{2: {at: stale}, 3: {at: stale}}
	e.checkQuorum()

	if r := recvRead(t, ch); r.err != ErrNotLeader {
		t.Fatalf("pending read got %v, want ErrNotLeader", r.err)
	}
}

// A brand-new leader has not had time to hear from anyone and must not depose
// itself the instant it is promoted.
func TestCheckQuorum_GracePeriodForFreshLeader(t *testing.T) {
	e, node := newLeaderEngine(t, 7)
	e.resetLeadershipEvidence()

	e.checkQuorum()

	if !node.IsLeader() {
		t.Fatal("freshly promoted leader deposed itself before any round completed")
	}
}

func TestCheckQuorum_SingleNodeNeverStepsDown(t *testing.T) {
	e, node := newLeaderEngineSized(t, 7, 1)

	e.checkQuorum()

	if !node.IsLeader() {
		t.Fatal("single-node cluster stepped down")
	}
}

package engine

// Engine is the central orchestration loop of CrystalDB.
// It is the only goroutine that mutates the RaftNode's CommitIndex
// and LastApplied, which eliminates an entire class of race conditions
// that existed in the original main.go.
//
// Responsibilities:
//   1. Dequeue proposals from the ProposalQueue
//   2. Write to the RaftLog (leader only)
//   3. Trigger parallel replication to peers
//   4. Poll for quorum and advance CommitIndex
//   5. Apply committed entries to the StateMachine in index order
//   6. Trigger log compaction when the log grows past the threshold
//
// The engine does NOT hold any locks while waiting for replication —
// replication goroutines call RaftNode.UpdatePeerProgress independently,
// and the engine polls CommitIndex on each tick.

import (
	"log"
	"time"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

const (
	tickInterval      = 20 * time.Millisecond
	proposalTimeout   = 2 * time.Second
	quorumPollTimeout = 1500 * time.Millisecond
)

// Proposal is a client write request flowing through the engine.
type Proposal struct {
	Command  raft.Command
	ResultCh chan error // nil error = committed successfully
}

// Engine drives the consensus and application loop.
type Engine struct {
	node         *raft.RaftNode
	raftLog      *raft.RaftLog
	stateMachine store.StateMachine
	snapshots    *store.SnapshotManager
	replicator   *raft.Replicator
	peers        map[int]string
	proposals    chan Proposal
}

// New creates an Engine. The proposals channel is exposed via ProposalQueue().
func New(
	cfg *config.Config,
	node *raft.RaftNode,
	raftLog *raft.RaftLog,
	sm store.StateMachine,
	snapshots *store.SnapshotManager,
) *Engine {
	return &Engine{
		node:         node,
		raftLog:      raftLog,
		stateMachine: sm,
		snapshots:    snapshots,
		replicator:   raft.NewReplicator(),
		peers:        cfg.Peers,
		proposals:    make(chan Proposal, 100),
	}
}

// ProposalQueue returns the channel callers use to submit write proposals.
func (e *Engine) ProposalQueue() chan<- Proposal {
	return e.proposals
}

// Run starts the engine loop. Call in a goroutine; it runs until ctx is cancelled
// or the process exits. Using a done channel keeps it compatible with Go 1.22
// without importing context into the engine.
func (e *Engine) Run(done <-chan struct{}) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case prop := <-e.proposals:
			e.handleProposal(prop)

		case <-ticker.C:
			e.onTick()
		}
	}
}

// handleProposal processes a single write proposal end-to-end.
func (e *Engine) handleProposal(prop Proposal) {
	if !e.node.IsLeader() {
		prop.ResultCh <- ErrNotLeader
		return
	}

	// Encode the application command into opaque bytes for the log envelope.
	cmdBytes, err := raft.EncodeCommand(prop.Command)
	if err != nil {
		prop.ResultCh <- err
		return
	}

	term := e.node.CurrentTerm()
	entry, err := e.raftLog.AppendLeader(cmdBytes, term)
	if err != nil {
		prop.ResultCh <- err
		return
	}

	// Fan out replication in parallel. Each goroutine calls
	// node.UpdatePeerProgress on success — no lock contention here.
	for peerID, addr := range e.peers {
		go e.replicator.ReplicateEntry(e.node, peerID, addr, entry)
	}

	// Poll until committed or timed out.
	committed := e.waitForCommit(entry)
	if !committed {
		prop.ResultCh <- ErrCommitTimeout
		return
	}

	// Apply all newly committed entries in order.
	e.applyCommitted()

	// Compact if the log has grown past the threshold.
	commitIndex, _ := e.node.CommitAndApplyBoundary()
	if e.raftLog.NeedsCompaction(commitIndex) {
		e.compact(commitIndex)
	}

	prop.ResultCh <- nil
}

// waitForCommit polls quorum state until entry.Index is committed or timeout.
func (e *Engine) waitForCommit(entry raft.LogEntry) bool {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(quorumPollTimeout)
	defer timeout.Stop()

	for {
		localLatest := e.raftLog.LatestIndex()
		localTerm := e.raftLog.TermAt(localLatest)
		term := e.node.CurrentTerm()

		e.node.AdvanceCommitIndex(localLatest, localTerm, term)

		commitIndex, _ := e.node.CommitAndApplyBoundary()
		if commitIndex >= entry.Index {
			return true
		}

		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
			// continue polling
		}
	}
}

// applyCommitted applies all entries between LastApplied and CommitIndex.
func (e *Engine) applyCommitted() {
	commitIndex, lastApplied := e.node.CommitAndApplyBoundary()

	for lastApplied < commitIndex {
		nextApply := e.node.AdvanceLastApplied()
		entry, ok := e.raftLog.GetEntry(nextApply)
		if !ok {
			log.Printf("[ENGINE] Missing log entry at index %d — skipping", nextApply)
			continue
		}

		cmd, err := raft.DecodeCommand(entry.Command)
		if err != nil {
			log.Printf("[ENGINE] Corrupt command at index %d: %v", nextApply, err)
			continue
		}

		if err := e.stateMachine.Apply(nextApply, cmd); err != nil {
			log.Printf("[ENGINE] State machine error at index %d: %v", nextApply, err)
		}

		_, lastApplied = e.node.CommitAndApplyBoundary()
	}
}

// onTick runs on every ticker cycle for background maintenance.
// On followers: advance application of committed entries pushed by leader.
func (e *Engine) onTick() {
	if !e.node.IsLeader() {
		e.applyCommitted()
	}
}

// compact takes a snapshot of the state machine and truncates the WAL.
func (e *Engine) compact(commitIndex int) {
	term := e.raftLog.TermAt(commitIndex)
	meta := store.SnapshotMeta{
		LastIncludedIndex: commitIndex,
		LastIncludedTerm:  term,
	}

	if err := e.snapshots.Write(meta, e.stateMachine); err != nil {
		log.Printf("[ENGINE] Snapshot write failed: %v", err)
		return
	}

	if err := e.raftLog.TruncateBeforeIndex(commitIndex); err != nil {
		log.Printf("[ENGINE] WAL truncation failed: %v", err)
		return
	}

	log.Printf("[ENGINE] Compacted log up to index %d", commitIndex)
}

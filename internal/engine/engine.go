package engine

// Engine is the central orchestration loop of CrystalDB.
//
// Concurrency model (etcd-style):
//   - A single control goroutine (Run) owns all consensus STATE transitions:
//     it is the ONLY goroutine that advances CommitIndex/LastApplied, applies
//     entries, runs elections, and fires client waiters. This preserves the
//     single-writer discipline that keeps commit/apply race-free.
//   - One long-lived REPLICATION goroutine per peer (peerReplicator) owns all
//     outbound RPC latency for that peer. A slow or black-holed peer therefore
//     slows only itself — it can no longer stall heartbeats, proposals, or the
//     control loop. Replicators only call the node's already-locked mutators
//     (UpdatePeerProgress / BacktrackNextIndex) and report a higher observed
//     term back to the control loop via a channel.
//
// Proposals are non-blocking: handleProposal appends to the log, registers a
// pending waiter keyed by log index, notifies the replicators, and returns.
// When the control loop advances CommitIndex past a waiter's index it fires the
// waiter's result channel. A deadline sweep fails waiters that never commit, and
// a stepdown fails all outstanding waiters at once.

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"crystal/internal/config"
	"crystal/internal/raft"
	"crystal/internal/store"
)

const (
	tickInterval    = 20 * time.Millisecond
	proposalTimeout = 2 * time.Second

	// Election timing (§5.2). The election timeout is chosen randomly from
	// [electionTimeoutMin, electionTimeoutMax] per node, per election, so that
	// in most cases only one node times out and split votes are rare. The
	// heartbeat interval must be comfortably shorter than electionTimeoutMin so
	// a live leader's heartbeats keep resetting followers' timers.
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond

	// replicationTimeout bounds a single outbound RPC. It is a liveness knob, not
	// a safety one: a leader that blocks forever on a black-holed peer stops
	// making progress for every other peer too.
	replicationTimeout = 1 * time.Second
)

// Proposal is a client write request flowing through the engine.
type Proposal struct {
	Command  raft.Command
	ResultCh chan error // nil error = committed successfully
}

// waiter couples a client's result channel to the log index it is waiting to
// have committed, plus a deadline after which the proposal fails.
type waiter struct {
	index    int
	resultCh chan error
	deadline time.Time
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

	// Control-loop-only state (accessed solely from the Run goroutine).
	rng             *rand.Rand
	electionTimeout time.Duration // current randomized deadline for this cycle
	waiters         []*waiter     // pending client proposals, ascending by index

	// Per-peer replication goroutines, live only while we are leader.
	replicators map[int]*peerReplicator
	replWG      sync.WaitGroup

	// stepDownCh carries a higher term observed by any replication goroutine.
	// The control loop drains it and steps down. Buffered so a replicator never
	// blocks reporting it.
	stepDownCh chan int

	// fatalf halts the node on a condition from which it cannot continue
	// correctly. It is a field only so tests can observe the halt instead of
	// killing the test binary; in production it is log.Fatalf.
	fatalf func(format string, args ...any)
}

// New creates an Engine using the production HTTP transport.
func New(
	cfg *config.Config,
	node *raft.RaftNode,
	raftLog *raft.RaftLog,
	sm store.StateMachine,
	snapshots *store.SnapshotManager,
) *Engine {
	return NewWithTransport(cfg, node, raftLog, sm, snapshots,
		raft.NewHTTPTransport(replicationTimeout))
}

// NewWithTransport creates an Engine over an arbitrary transport. Tests use it
// to run a whole cluster in one process over a fake network, where links can be
// cut in one direction at a chosen moment.
func NewWithTransport(
	cfg *config.Config,
	node *raft.RaftNode,
	raftLog *raft.RaftLog,
	sm store.StateMachine,
	snapshots *store.SnapshotManager,
	transport raft.Transport,
) *Engine {
	e := &Engine{
		node:         node,
		raftLog:      raftLog,
		stateMachine: sm,
		snapshots:    snapshots,
		replicator:   raft.NewReplicator(transport),
		peers:        cfg.Peers,
		proposals:    make(chan Proposal, 100),
		rng:          rand.New(rand.NewSource(time.Now().UnixNano() + int64(node.NodeID()))),
		replicators:  make(map[int]*peerReplicator),
		stepDownCh:   make(chan int, len(cfg.Peers)+1),
		fatalf:       log.Fatalf,
	}
	e.resetElectionTimeout()
	return e
}

// resetElectionTimeout picks a fresh randomized election deadline from the
// [min, max] interval. Called at startup and after each election attempt so
// that repeated split votes desynchronize (§5.2).
func (e *Engine) resetElectionTimeout() {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	e.electionTimeout = electionTimeoutMin + time.Duration(e.rng.Int63n(span))
}

// ProposalQueue returns the channel callers use to submit write proposals.
func (e *Engine) ProposalQueue() chan<- Proposal {
	return e.proposals
}

// Run starts the control loop. Call in a goroutine; it runs until done is closed.
// It never blocks on replication — RPC latency lives entirely in the per-peer
// replication goroutines.
func (e *Engine) Run(done <-chan struct{}) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	defer e.stopReplicators()

	for {
		select {
		case <-done:
			e.failAllWaiters(ErrNotLeader)
			return

		case prop := <-e.proposals:
			e.handleProposal(prop)

		case higherTerm := <-e.stepDownCh:
			e.handleStepDown(higherTerm)

		case <-ticker.C:
			e.onTick()
		}
	}
}

// ---- Proposal handling (non-blocking) ----

// handleProposal appends a proposal to the log and registers a waiter, then
// returns immediately. The commit is confirmed later by the control loop when
// CommitIndex advances past the entry's index. It does NOT wait on replication.
func (e *Engine) handleProposal(prop Proposal) {
	if !e.node.IsLeader() {
		prop.ResultCh <- ErrNotLeader
		return
	}

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

	e.waiters = append(e.waiters, &waiter{
		index:    entry.Index,
		resultCh: prop.ResultCh,
		deadline: time.Now().Add(proposalTimeout),
	})

	// Wake the replicators so the new entry ships promptly instead of waiting
	// for the next heartbeat tick.
	e.notifyReplicators()
}

// fireCommittedWaiters resolves every waiter whose index is now committed,
// removing it from the pending list. Called from the control loop after commit
// advancement.
func (e *Engine) fireCommittedWaiters(commitIndex int) {
	if len(e.waiters) == 0 {
		return
	}
	kept := e.waiters[:0]
	for _, w := range e.waiters {
		if w.index <= commitIndex {
			w.resultCh <- nil
		} else {
			kept = append(kept, w)
		}
	}
	e.waiters = kept
}

// sweepExpiredWaiters fails any waiter past its deadline with ErrCommitTimeout.
func (e *Engine) sweepExpiredWaiters(now time.Time) {
	if len(e.waiters) == 0 {
		return
	}
	kept := e.waiters[:0]
	for _, w := range e.waiters {
		if now.After(w.deadline) {
			w.resultCh <- ErrCommitTimeout
		} else {
			kept = append(kept, w)
		}
	}
	e.waiters = kept
}

// failAllWaiters resolves every pending waiter with err (used on stepdown and
// shutdown). Leaves the waiter list empty.
func (e *Engine) failAllWaiters(err error) {
	for _, w := range e.waiters {
		w.resultCh <- err
	}
	e.waiters = e.waiters[:0]
}

// ---- Control loop tick ----

// onTick drives the Figure 2 "Rules for Servers" for the local role, without
// ever blocking on an RPC.
//
//   - Leader: advance commit index from the refreshed matchIndex quorum, fire
//     any newly-committed waiters, apply, and compact. Heartbeats and retries
//     are handled continuously by the per-peer replication goroutines, so the
//     tick does no network I/O.
//   - Follower/Candidate: if the randomized election timeout elapsed with no
//     contact, start an election.
//
// Both roles apply committed entries and expire stale waiters.
func (e *Engine) onTick() {
	if e.node.IsLeader() {
		localLatest := e.raftLog.LatestIndex()
		e.node.AdvanceCommitIndex(localLatest, e.raftLog.TermAt, e.node.CurrentTerm())
		commitIndex, _ := e.node.CommitAndApplyBoundary()
		e.fireCommittedWaiters(commitIndex)
	} else if e.node.TimeSinceContact() >= e.electionTimeout {
		e.runElection()
	}

	e.applyCommitted()
	e.maybeCompact()
	e.sweepExpiredWaiters(time.Now())
}

// handleStepDown reacts to a higher term observed by a replication goroutine:
// step down to follower, stop the replicators, and fail all pending proposals.
func (e *Engine) handleStepDown(higherTerm int) {
	if higherTerm <= e.node.CurrentTerm() && !e.node.IsLeader() {
		return // already handled / stale signal
	}
	log.Printf("[ENGINE] Stepping down: observed higher term %d", higherTerm)
	if err := e.node.BecomeFollower(higherTerm, 0); err != nil {
		log.Printf("[ENGINE] Stepdown persist failed: %v", err)
	}
	e.stopReplicators()
	e.failAllWaiters(ErrNotLeader)
}

// applyCommitted applies all entries between LastApplied and CommitIndex.
//
// Every failure here is fatal, and lastApplied is advanced only after an entry
// has actually been applied. This is not defensiveness — it is the only way to
// hold State Machine Safety (§5.4.3), which requires that every replica apply the
// same entries in the same order. A replica that skips an entry and advances
// anyway has diverged from the cluster, and Raft has no mechanism to detect or
// repair that: the log will look consistent forever while the state machine
// underneath it is wrong, and reads will be served from it.
//
// The three conditions below are all unrecoverable at runtime, so the node stops:
//
//   - a missing entry means the log lost something it had promised to keep,
//   - an undecodable command means the WAL is corrupt at that index,
//   - an Apply error means the entry did not take effect.
//
// A crashed node rejoins and catches up from the leader. A diverged node does
// not, and nothing tells its operator it happened. Crashing is the safer failure.
func (e *Engine) applyCommitted() {
	commitIndex, lastApplied := e.node.CommitAndApplyBoundary()

	for lastApplied < commitIndex {
		next := lastApplied + 1

		entry, ok := e.raftLog.GetEntry(next)
		if !ok {
			e.fatalf("[ENGINE] log entry %d is missing but commitIndex is %d; "+
				"cannot apply without diverging from the cluster", next, commitIndex)
			return
		}

		cmd, err := raft.DecodeCommand(entry.Command)
		if err != nil {
			e.fatalf("[ENGINE] corrupt command in committed entry %d: %v", next, err)
			return
		}

		if err := e.stateMachine.Apply(next, cmd); err != nil {
			e.fatalf("[ENGINE] state machine failed to apply committed entry %d: %v", next, err)
			return
		}

		lastApplied = e.node.AdvanceLastApplied()
	}
}

// maybeCompact triggers a snapshot + WAL truncation when the log has grown past
// the threshold. Driven from the tick so a leader still compacts while a lagging
// follower is unreachable — the situation that later forces an InstallSnapshot.
//
// It compacts to lastApplied, NOT commitIndex. A snapshot is a serialization of
// the state machine, and the state machine reflects exactly the entries that have
// been applied; §7 defines the last included index as "the last entry the state
// machine had applied". The two boundaries diverge routinely on a follower, whose
// commitIndex is advanced from the HTTP goroutine by SetFollowerCommitIndex and
// can jump between applyCommitted and maybeCompact within a single tick.
// Compacting to commitIndex there would write a snapshot claiming entries whose
// effects it does not contain, and then delete the very entries needed to repair
// the gap.
func (e *Engine) maybeCompact() {
	_, lastApplied := e.node.CommitAndApplyBoundary()
	if e.raftLog.NeedsCompaction(lastApplied) {
		e.compact(lastApplied)
	}
}

// compact takes a snapshot of the state machine at snapshotIndex (which must be
// lastApplied) and truncates the WAL up to it.
func (e *Engine) compact(snapshotIndex int) {
	// TermAt returns 0 for an index it cannot resolve. Compacting on that answer
	// persists a snapshot whose LastIncludedTerm is a fabrication, and
	// TruncateBeforeIndex then bails out on its already-compacted check — so the
	// lie outlives the call. After a restart RestoreOffset seeds term 0 as the
	// snapshot boundary and every AppendEntries consistency check against that
	// boundary fails, permanently. Refuse instead.
	term := e.raftLog.TermAt(snapshotIndex)
	if term == 0 {
		log.Printf("[ENGINE] Skipping compaction: no known term for index %d", snapshotIndex)
		return
	}

	meta := store.SnapshotMeta{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  term,
	}

	if err := e.snapshots.Write(meta, e.stateMachine); err != nil {
		log.Printf("[ENGINE] Snapshot write failed: %v", err)
		return
	}

	if err := e.raftLog.TruncateBeforeIndex(snapshotIndex, term); err != nil {
		log.Printf("[ENGINE] WAL truncation failed: %v", err)
		return
	}

	log.Printf("[ENGINE] Compacted log up to index %d", snapshotIndex)
}

// ---- Elections ----

// runElection conducts one election attempt (§5.2): transition to candidate,
// bump the term, vote for self, request votes from all peers in parallel, and
// become leader on a majority. A newer term observed in any response steps us
// back down to follower. Regardless of outcome, a fresh randomized timeout is
// armed so a failed election retries after a different delay.
//
// Vote gathering is done inline (bounded, one round) rather than via the
// per-peer replicators, which only run while we are leader.
func (e *Engine) runElection() {
	term, err := e.node.BecomeCandidate()
	if err != nil {
		log.Printf("[ENGINE] Election abort: cannot persist candidate state: %v", err)
		e.resetElectionTimeout()
		return
	}

	lastLogIndex := e.raftLog.LatestIndex()
	lastLogTerm := e.raftLog.LatestTerm()
	req := raft.RequestVoteRequest{
		Term:         term,
		CandidateID:  e.node.NodeID(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	highestTerm := term
	won := false

	for peerID, addr := range e.peers {
		wg.Add(1)
		go func(pid int, a string) {
			defer wg.Done()
			resp, ok := e.replicator.RequestVoteFrom(pid, a, req)
			if !ok {
				return
			}
			mu.Lock()
			if resp.Term > highestTerm {
				highestTerm = resp.Term
			}
			mu.Unlock()
			if resp.VoteGranted {
				if e.node.RecordVoteAndCheckMajority(term) {
					mu.Lock()
					won = true
					mu.Unlock()
				}
			}
		}(peerID, addr)
	}
	wg.Wait()

	e.resetElectionTimeout()

	if highestTerm > term {
		log.Printf("[ENGINE] Election lost: observed higher term %d", highestTerm)
		if err := e.node.BecomeFollower(highestTerm, 0); err != nil {
			log.Printf("[ENGINE] Stepdown persist failed: %v", err)
		}
		return
	}

	// Assume leadership only if we won AND BecomeLeader confirms we are still a
	// candidate in this election's term (atomic check inside BecomeLeader,
	// closing the stepdown race).
	if won && e.node.BecomeLeader(term, lastLogIndex) {
		// Append a no-op entry in our new term (§8) so prior-term entries can
		// reach the commit frontier without waiting for the first client write.
		if noop, err := raft.EncodeCommand(raft.Command{Op: raft.OpNoop}); err != nil {
			log.Printf("[ENGINE] Failed to encode leader no-op: %v", err)
		} else if _, err := e.raftLog.AppendLeader(noop, term); err != nil {
			log.Printf("[ENGINE] Failed to append leader no-op: %v", err)
		}

		// Start the per-peer replication goroutines; they immediately send a
		// first round that both ships the no-op and asserts authority.
		e.startReplicators()
	}
}

// ---- Per-peer replication goroutine lifecycle ----

// startReplicators launches one replication goroutine per peer. Idempotent: if
// replicators are already running it does nothing. Called on becoming leader.
func (e *Engine) startReplicators() {
	if len(e.replicators) > 0 {
		return
	}
	term := e.node.CurrentTerm()
	for peerID, addr := range e.peers {
		pr := &peerReplicator{
			engine: e,
			peerID: peerID,
			addr:   addr,
			term:   term,
			notify: make(chan struct{}, 1),
			stop:   make(chan struct{}),
		}
		e.replicators[peerID] = pr
		e.replWG.Add(1)
		go pr.run(&e.replWG)
	}
	log.Printf("[ENGINE] Started %d replication goroutines for term %d", len(e.replicators), term)
}

// stopReplicators signals all replication goroutines to exit and waits for them.
// Idempotent. Called on stepdown and shutdown.
func (e *Engine) stopReplicators() {
	if len(e.replicators) == 0 {
		return
	}
	for _, pr := range e.replicators {
		close(pr.stop)
	}
	e.replWG.Wait()
	e.replicators = make(map[int]*peerReplicator)
}

// notifyReplicators nudges every replication goroutine that new work is
// available. The per-peer notify channel is buffered depth 1, so a nudge is
// coalesced rather than queued — a replicator always sends the latest log state.
func (e *Engine) notifyReplicators() {
	for _, pr := range e.replicators {
		select {
		case pr.notify <- struct{}{}:
		default: // already pending
		}
	}
}

// reportHigherTerm is called by a replication goroutine that saw a follower on a
// newer term. It forwards the term to the control loop for stepdown. Non-blocking.
func (e *Engine) reportHigherTerm(term int) {
	select {
	case e.stepDownCh <- term:
	default:
	}
}

// buildSnapshotRequest assembles an InstallSnapshot RPC from the latest on-disk
// snapshot. ok is false if no snapshot exists yet. Safe for concurrent use by
// replication goroutines: it only reads immutable snapshot state and node/log
// accessors that take their own locks.
func (e *Engine) buildSnapshotRequest() (raft.InstallSnapshotRequest, bool) {
	snap, err := e.snapshots.Read()
	if err != nil || snap == nil {
		if err != nil {
			log.Printf("[ENGINE] Cannot read snapshot for shipping: %v", err)
		}
		return raft.InstallSnapshotRequest{}, false
	}

	data, err := snap.EncodeState()
	if err != nil {
		log.Printf("[ENGINE] Cannot encode snapshot for shipping: %v", err)
		return raft.InstallSnapshotRequest{}, false
	}

	return raft.InstallSnapshotRequest{
		Term:              e.node.CurrentTerm(),
		LeaderID:          e.node.NodeID(),
		LastIncludedIndex: snap.Meta.LastIncludedIndex,
		LastIncludedTerm:  snap.Meta.LastIncludedTerm,
		Data:              data,
	}, true
}

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
	"sync/atomic"
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
//
// term is the leader term the entry was appended in, and it is what makes the
// acknowledgement honest. An index alone does not identify an entry: if this
// node is deposed and later re-elected, the index the client is waiting on may
// by then hold a DIFFERENT leader's entry. Acking on index alone would tell the
// client its write committed when what actually committed was somebody else's.
type waiter struct {
	index    int
	term     int
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
	// replicatorsTerm is the term they were started for; a mismatch against the
	// node's current term means they are stale and must be replaced, not reused.
	replicators     map[int]*peerReplicator
	replicatorsTerm int
	replWG          sync.WaitGroup

	// stepDownCh carries a higher term observed by any replication goroutine.
	// The control loop drains it and steps down. Buffered so a replicator never
	// blocks reporting it.
	stepDownCh chan int

	// voteCh carries RequestVote replies back to the control loop, which is the
	// only goroutine that tallies them. Buffered for one reply per peer so an
	// election goroutine never blocks on a control loop busy elsewhere.
	voteCh chan voteResult

	// ---- Leadership evidence (shared by CheckQuorum and ReadIndex) ----

	// roundSeq numbers replication rounds. A replicator claims one BEFORE it
	// sends, so an ack carrying seq N proves the follower recognized our
	// leadership at some point after round N began. Atomic: claimed from the
	// per-peer replicator goroutines.
	roundSeq atomic.Uint64

	// ackCh carries proof-of-leadership from the replicators to the control loop.
	ackCh chan ackReport

	// lastAck is the control loop's record of that evidence, per peer. Written
	// only by the control loop.
	lastAck map[int]peerAck

	// reads are pending linearizable reads, and readCh is how they arrive.
	reads  []*readWaiter
	readCh chan Read

	// noopIndex is the index of the no-op this leader appended on election. Until
	// it commits, the leader does not yet know the true commit frontier (§8), so
	// no read may be served.
	noopIndex int

	// done is closed when Run returns. Election goroutines outlive a single
	// election attempt and select on it so they cannot leak past shutdown.
	done chan struct{}

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
		voteCh:       make(chan voteResult, len(cfg.Peers)+1),
		ackCh:        make(chan ackReport, 4*(len(cfg.Peers)+1)),
		lastAck:      make(map[int]peerAck, len(cfg.Peers)),
		readCh:       make(chan Read, 100),
		done:         make(chan struct{}),
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
	// Release any election goroutine still parked on voteCh.
	defer close(e.done)

	for {
		select {
		case <-done:
			e.failAllWaiters(ErrNotLeader)
			e.failAllReads(ErrNotLeader)
			return

		case prop := <-e.proposals:
			e.handleProposal(prop)

		case higherTerm := <-e.stepDownCh:
			e.handleStepDown(higherTerm)

		case v := <-e.voteCh:
			e.handleVoteResult(v)

		case a := <-e.ackCh:
			e.handleAck(a)

		case r := <-e.readCh:
			e.handleRead(r)

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
		term:     term,
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
//
// currentTerm is checked against each waiter's term because a committed index is
// not proof that the CLIENT's entry committed there. If this node was deposed and
// re-elected between the proposal and now, the index may hold an entry from the
// intervening leader — committing that entry says nothing about the write this
// waiter is holding a client on. Acknowledging it would be a false positive, and
// a false positive on a write acknowledgement is a linearizability violation the
// client has no way to detect.
func (e *Engine) fireCommittedWaiters(commitIndex, currentTerm int) {
	if len(e.waiters) == 0 {
		return
	}
	kept := e.waiters[:0]
	for _, w := range e.waiters {
		switch {
		case w.term != currentTerm:
			// The term moved on; this entry's fate is no longer ours to report.
			w.resultCh <- ErrNotLeader
		case w.index <= commitIndex:
			w.resultCh <- nil
		default:
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
	// Reconcile first: leadership can change out from under the control loop on
	// an HTTP goroutine, and everything below assumes the two agree.
	e.reconcileLeadership()

	if role, term := e.node.State(); role == raft.Leader {
		localLatest := e.raftLog.LatestIndex()
		e.node.AdvanceCommitIndex(localLatest, e.raftLog.TermAt, term)
		commitIndex, _ := e.node.CommitAndApplyBoundary()
		e.fireCommittedWaiters(commitIndex, term)
	} else if e.node.TimeSinceContact() >= e.electionTimeout {
		e.runElection()
	}

	e.applyCommitted()
	e.maybeCompact()

	// Reads depend on applied progress and on leadership evidence, both of which
	// may have moved above.
	e.fireReadWaiters()

	// CheckQuorum runs after the read pass so a leader that is about to step down
	// still gets one chance to satisfy reads it can legitimately serve.
	if e.node.IsLeader() {
		e.checkQuorum()
	}

	now := time.Now()
	e.sweepExpiredWaiters(now)
	e.sweepExpiredReads(now)
}

// reconcileLeadership makes the control loop's view of leadership agree with the
// node's, and is the first thing every tick does.
//
// Role changes have two sources. The control loop causes them deliberately, via
// runElection and handleStepDown. But an inbound RPC also causes them: a
// higher-term AppendEntries or RequestVote arrives on an HTTP goroutine and
// deposes this node inside the receiver, and the control loop is never told. The
// consequences are not subtle:
//
//   - The per-peer replicators keep running, still carrying the term they were
//     started for. They no-op while IsLeader is false, but they are still there.
//   - On re-election, startReplicators used to short-circuit on a non-empty map,
//     so those stale-term goroutines resumed under the NEW term while reporting
//     against the OLD one. Every ordinary reply then looked like a higher term
//     and triggered a stepdown — a leader deposing itself moments after winning.
//   - Waiters registered under the old term survive, and can be acknowledged
//     against an index that now belongs to a different leader's entry.
//
// Rather than trying to intercept every path that can depose the node, the
// control loop simply observes the truth once per tick and makes its own state
// match. Reconciling is idempotent, so it costs a comparison in the common case.
func (e *Engine) reconcileLeadership() {
	role, term := e.node.State()

	if role != raft.Leader {
		// Not (or no longer) leader: tear down anything that implies we are.
		if len(e.replicators) > 0 {
			log.Printf("[ENGINE] Reconciling: no longer leader, stopping replicators")
			e.stopReplicators()
			e.failAllWaiters(ErrNotLeader)
		}
		// Reads are refused whether or not we were running replicators: an
		// out-of-band stepdown can land between two ticks.
		e.failAllReads(ErrNotLeader)
		return
	}

	// Leader, but the replicators belong to an older term — or are missing
	// entirely because we were promoted somewhere other than runElection.
	// startReplicators replaces a stale set rather than short-circuiting.
	if len(e.replicators) == 0 || e.replicatorsTerm != term {
		e.startReplicators()
	}
}

// handleStepDown reacts to a higher term observed by a replication goroutine:
// step down to follower, stop the replicators, and fail all pending proposals.
//
// The guard is a plain term comparison. It used to also require !IsLeader, which
// inverted the intent: a stale report whose term was NOT higher than ours would
// pass the guard precisely when we were the leader — the one case where acting on
// it is most damaging. stepDownCh is buffered, so a report can outlive the
// situation that produced it and arrive after this node has legitimately won a
// new election at that same term.
func (e *Engine) handleStepDown(higherTerm int) {
	if higherTerm <= e.node.CurrentTerm() {
		return // stale or already handled
	}
	log.Printf("[ENGINE] Stepping down: observed higher term %d", higherTerm)
	if err := e.node.BecomeFollower(higherTerm, 0); err != nil {
		log.Printf("[ENGINE] Stepdown persist failed: %v", err)
	}
	e.stopReplicators()
	e.failAllWaiters(ErrNotLeader)
	e.failAllReads(ErrNotLeader)
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

// runElection starts one election attempt (§5.2) and RETURNS IMMEDIATELY.
//
// It used to gather votes inline and end in wg.Wait(), which was wrong twice
// over. It blocked the control loop for as long as the slowest peer took to
// answer — up to the full RPC timeout, during which no proposal, tick, apply or
// stepdown was processed. And it waited for ALL peers when §5.2 says a candidate
// wins "if it receives votes from a majority", i.e. the moment the majority
// arrives. With a 300–600ms election timeout and a 1s RPC timeout, a single
// black-holed peer made every election overrun its own timeout, so elections
// re-armed before they could finish and the cluster could fail to elect at all.
//
// Now each peer's RequestVote runs on its own goroutine and reports back through
// voteCh; the control loop tallies in handleVoteResult and promotes on the vote
// that reaches a majority, whenever that is.
func (e *Engine) runElection() {
	term, err := e.node.BecomeCandidate()
	if err != nil {
		log.Printf("[ENGINE] Election abort: cannot persist candidate state: %v", err)
		e.resetElectionTimeout()
		return
	}

	// Arm the next timeout now: if this election stalls, the retry is already
	// scheduled and does not depend on anything completing.
	e.resetElectionTimeout()

	lastLogIndex, lastLogTerm := e.raftLog.LastLogState()
	req := raft.RequestVoteRequest{
		Term:         term,
		CandidateID:  e.node.NodeID(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// A single-node cluster has already reached a majority with its own vote and
	// has nobody to ask, so it must win here rather than waiting for a reply that
	// will never come.
	if len(e.peers) == 0 {
		e.becomeLeader(term, lastLogIndex)
		return
	}

	for peerID, addr := range e.peers {
		go func(pid int, a string) {
			resp, ok := e.replicator.RequestVoteFrom(pid, a, req)
			if !ok {
				return
			}
			select {
			case e.voteCh <- voteResult{electionTerm: term, resp: resp}:
			case <-e.done:
			}
		}(peerID, addr)
	}
}

// voteResult carries one RequestVote reply back to the control loop.
// electionTerm is the term the vote was SOLICITED in, which is what makes a
// late reply from an abandoned election identifiable and discardable.
type voteResult struct {
	electionTerm int
	resp         raft.RequestVoteResponse
}

// handleVoteResult tallies one vote on the control loop. It is the only place
// leadership is assumed, so the decision needs no lock of its own.
func (e *Engine) handleVoteResult(v voteResult) {
	// A reply to an election we have already moved past tells us nothing.
	role, term := e.node.State()
	if term != v.electionTerm || role != raft.Candidate {
		return
	}

	if v.resp.Term > term {
		log.Printf("[ENGINE] Election lost: observed higher term %d", v.resp.Term)
		if err := e.node.BecomeFollower(v.resp.Term, 0); err != nil {
			log.Printf("[ENGINE] Stepdown persist failed: %v", err)
		}
		return
	}

	if !v.resp.VoteGranted {
		return
	}

	// Promote on the vote that reaches the majority — not on the last vote to
	// arrive (§5.2).
	if e.node.RecordVoteAndCheckMajority(v.electionTerm) {
		e.becomeLeader(v.electionTerm, e.raftLog.LatestIndex())
	}
}

// becomeLeader promotes this node if it is still a candidate in electionTerm,
// appends the term's no-op, and starts replication.
func (e *Engine) becomeLeader(electionTerm, lastLogIndex int) {
	// BecomeLeader re-checks under the node lock that we are still a candidate in
	// this term, so a stepdown that landed between the tally and here cannot be
	// stomped.
	if !e.node.BecomeLeader(electionTerm, lastLogIndex) {
		return
	}

	// Evidence from a previous term proves nothing about this one, and the
	// CheckQuorum clock must start now rather than judging us for silence we have
	// not yet had a chance to break.
	e.resetLeadershipEvidence()

	// Append a no-op entry in our new term (§8) so prior-term entries can reach
	// the commit frontier without waiting for the first client write. Its index
	// is also the floor for reads: until it commits, this leader does not know
	// the true commit frontier and must not answer a read.
	e.noopIndex = 0
	if noop, err := raft.EncodeCommand(raft.Command{Op: raft.OpNoop}); err != nil {
		log.Printf("[ENGINE] Failed to encode leader no-op: %v", err)
	} else if entry, err := e.raftLog.AppendLeader(noop, electionTerm); err != nil {
		log.Printf("[ENGINE] Failed to append leader no-op: %v", err)
	} else {
		e.noopIndex = entry.Index
	}

	// Start the per-peer replication goroutines; they immediately send a
	// first round that both ships the no-op and asserts authority.
	e.startReplicators()
}

// ---- Per-peer replication goroutine lifecycle ----

// startReplicators launches one replication goroutine per peer for the node's
// current term. If replicators from an EARLIER term are still running they are
// stopped first: reusing them would leave each goroutine comparing follower
// replies against a term the cluster has left behind, so every ordinary reply
// would read as a higher term and trigger a spurious stepdown.
func (e *Engine) startReplicators() {
	term := e.node.CurrentTerm()
	if len(e.replicators) > 0 {
		if e.replicatorsTerm == term {
			return
		}
		log.Printf("[ENGINE] Replacing replicators from stale term %d with term %d",
			e.replicatorsTerm, term)
		e.stopReplicators()
	}
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
	e.replicatorsTerm = term
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
	e.replicatorsTerm = 0
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

// ---- Inbound RPC facade ----
//
// The engine is the single place that owns a node's Raft dependencies, so it is
// also where inbound RPCs are bound to them. The transport layer sees only these
// three methods and knows nothing of RaftNode, RaftLog, or the snapshot store.
//
// The receivers themselves live in the raft package; these methods only supply
// the collaborators that package cannot import (store) or hold (the log).

// HandleAppendEntries binds the AppendEntries receiver to this node's log.
func (e *Engine) HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse {
	return e.node.HandleAppendEntries(e.raftLog, req)
}

// HandleRequestVote binds the RequestVote receiver to this node's log. The log
// state is passed as a reader, not a snapshot, so the §5.4.1 comparison is made
// against the log as it stands inside the vote's critical section (F1c).
func (e *Engine) HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse {
	return e.node.HandleRequestVote(req, e.raftLog.LastLogState)
}

// HandleInstallSnapshot binds the InstallSnapshot receiver to this node's state
// machine and snapshot store. The raft package takes these as callbacks because
// it cannot import store (store imports raft).
func (e *Engine) HandleInstallSnapshot(req raft.InstallSnapshotRequest) raft.InstallSnapshotResponse {
	restore := func(data []byte) error {
		return e.stateMachine.Restore(data)
	}
	persist := func() error {
		return e.snapshots.Write(store.SnapshotMeta{
			LastIncludedIndex: req.LastIncludedIndex,
			LastIncludedTerm:  req.LastIncludedTerm,
		}, e.stateMachine)
	}
	return e.node.HandleInstallSnapshot(e.raftLog, req, restore, persist)
}

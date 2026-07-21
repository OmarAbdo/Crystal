package engine

// Leadership evidence: CheckQuorum (F19) and ReadIndex (F12).
//
// Both answer the same question — "am I still the leader RIGHT NOW?" — and Raft
// gives only one honest way to answer it: ask a majority and wait for them to
// say so. A leader cannot know it has been deposed. Nothing tells it. A newer
// leader is elected by a quorum that excludes it, and it keeps its role, its
// term, and its committed log, all of them stale, until it next manages to talk
// to someone. §8 states the consequence plainly: a read served from that node
// "would run the risk of returning stale data, since the leader responding to
// the request might have been superseded by a newer leader of which it is
// unaware."
//
// So both mechanisms are built on the same evidence: a per-peer record of the
// most recent replication round the follower acknowledged at our current term.
//
//   - ReadIndex consumes it positively: hold the read until a majority has
//     acknowledged a round that began AFTER the read arrived.
//   - CheckQuorum consumes it negatively: if a majority has not acknowledged
//     anything for a full election timeout, stop claiming to be leader.
//
// THE SUBTLE PART, and the reason rounds are numbered rather than timestamped on
// arrival: an ack is only evidence for reads that were registered before its
// round STARTED. A round sent at T=0 that returns at T=100 carries a follower's
// assertion made at some instant in between — and that assertion says nothing
// about a read that arrived at T=50. Counting it would confirm a quorum with
// evidence predating the read, which is precisely the stale-read hole the
// mechanism exists to close. The dissertation (§6.4) requires heartbeats
// *initiated after* the read index is recorded; the round sequence is how that
// is enforced here.

import (
	"log"
	"math"
	"time"

	"crystal/internal/raft"
)

const (
	// readTimeout bounds a linearizable read. Exceeding it means we could not
	// confirm leadership with a majority, which is a refusal to answer — never a
	// reason to answer from local state anyway.
	readTimeout = 2 * time.Second

	// quorumGrace is how long a leader may go without hearing from a majority
	// before it steps down. It is an election timeout because that is how long it
	// takes the other side of a partition to replace us: stepping down sooner
	// would churn on transient slowness, later would extend the window in which
	// two nodes both answer to the name "leader".
	quorumGrace = electionTimeoutMax
)

// Read asks the leader to establish a read index. It carries no key: consensus
// only has to say WHEN a local read is safe, never what it returns.
type Read struct {
	ResultCh chan readResult

	// awaitApply is true when the requester will serve the read from THIS node's
	// state machine, false when the index is being handed to a follower that does
	// its own applying.
	awaitApply bool
}

// readResult is what a resolved read reports: the confirmed index, or why it
// could not be confirmed.
type readResult struct {
	index int
	err   error
}

// ackReport is a replicator telling the control loop that a peer acknowledged
// our leadership during the round that started at startSeq.
type ackReport struct {
	peerID   int
	startSeq uint64
}

// peerAck is the control loop's record of the most recent such acknowledgement.
type peerAck struct {
	startSeq uint64    // highest round-start this peer has acked
	at       time.Time // when the ack was recorded, for CheckQuorum
}

// readWaiter is a read index held until leadership has been confirmed.
//
// awaitApply distinguishes the two consumers of one mechanism. A read this node
// will SERVE must also wait for its own state machine to reach the index. A read
// index being handed to a FOLLOWER over the ReadIndex RPC must not: the follower
// does its own applying, and making the leader wait for state it will never read
// would add latency for nothing.
type readWaiter struct {
	readIndex  int
	term       int    // leader term the read was admitted under
	barrier    uint64 // only rounds started after this count as evidence
	awaitApply bool
	deadline   time.Time
	resultCh   chan readResult
}

// applyWaiter blocks until this node's state machine has applied through index.
// It is how a FOLLOWER waits after the leader has handed it a read index. No term
// or quorum condition applies: the leader already made that decision, and a
// replica only ever applies entries that were committed by a quorum.
type applyWaiter struct {
	index    int
	deadline time.Time
	resultCh chan error
}

// ---- Public entry points ----

// LinearizableRead blocks until it is safe for THIS node to answer a read from
// its own state machine, wherever it sits in the cluster.
//
//   - Leader: admit a read index, confirm leadership with a quorum round, wait
//     for local apply.
//   - Follower: ask the leader for a confirmed read index, then wait for local
//     apply.
//
// The follower path is the point of the exercise. Only an integer crosses the
// network; the state machine access, the lookup and the response body all stay
// local. Read throughput therefore scales with the number of replicas instead of
// being pinned to whatever one machine can serve, which is the shape a
// coordination store actually needs — reads outnumber writes heavily.
//
// The leader's share drops to one small RPC per read, and because pending reads
// share a quorum round, its confirmation cost is per-round rather than per-read.
//
// The trade is latency: a follower read costs a round trip to the leader that a
// local read would not. Throughput and leader offload are bought with per-request
// latency, deliberately.
func (e *Engine) LinearizableRead(timeout time.Duration) error {
	if e.node.IsLeader() {
		return e.leaderRead(timeout)
	}
	return e.followerRead(timeout)
}

// leaderRead is the leader serving its own read.
func (e *Engine) leaderRead(timeout time.Duration) error {
	resultCh := make(chan readResult, 1)
	select {
	case e.readCh <- Read{ResultCh: resultCh, awaitApply: true}:
	case <-time.After(timeout):
		return ErrReadTimeout
	case <-e.done:
		return ErrNotLeader
	}

	select {
	case r := <-resultCh:
		return r.err
	case <-time.After(timeout):
		return ErrReadTimeout
	}
}

// followerRead fetches a read index from the leader, then waits for this node's
// own state machine to reach it.
func (e *Engine) followerRead(timeout time.Duration) error {
	leaderID := e.node.CurrentLeader()
	if leaderID == 0 {
		return ErrNotLeader // no leader known; the client should retry
	}
	addr, ok := e.peers[leaderID]
	if !ok {
		return ErrNotLeader // leader outside our peer map; nothing we can do
	}

	resp, ok := e.replicator.ReadIndexFrom(addr, raft.ReadIndexRequest{
		FromNodeID: e.node.NodeID(),
	})
	if !ok || !resp.Success {
		// The leader is unreachable, has been deposed, or could not confirm its
		// own quorum. Refusing is correct — answering from local state here is
		// precisely the stale read this whole path exists to prevent.
		return ErrNotLeader
	}

	return e.awaitApplied(resp.ReadIndex, timeout)
}

// awaitApplied blocks until the local state machine has applied through index.
func (e *Engine) awaitApplied(index int, timeout time.Duration) error {
	// Fast path: already caught up, so no trip through the control loop. This is
	// the common case on a healthy follower, where replication has usually
	// outrun the read that is asking about it.
	if _, applied := e.node.CommitAndApplyBoundary(); applied >= index {
		return nil
	}

	resultCh := make(chan error, 1)
	select {
	case e.applyCh <- applyWaiter{
		index:    index,
		deadline: time.Now().Add(timeout),
		resultCh: resultCh,
	}:
	case <-time.After(timeout):
		return ErrReadTimeout
	case <-e.done:
		return ErrReadTimeout
	}

	select {
	case err := <-resultCh:
		return err
	case <-time.After(timeout):
		return ErrReadTimeout
	}
}

// HandleReadIndex answers a follower asking for a read index — the receiving half
// of the ReadIndex RPC. It runs on an RPC goroutine and hands the decision to the
// control loop, which owns it.
func (e *Engine) HandleReadIndex(req raft.ReadIndexRequest) raft.ReadIndexResponse {
	resultCh := make(chan readResult, 1)
	select {
	// awaitApply is false: the follower does its own applying, so blocking here
	// on the leader's apply progress would add latency for state we never read.
	case e.readCh <- Read{ResultCh: resultCh, awaitApply: false}:
	case <-time.After(readTimeout):
		return raft.ReadIndexResponse{Term: e.node.CurrentTerm()}
	case <-e.done:
		return raft.ReadIndexResponse{Term: e.node.CurrentTerm()}
	}

	select {
	case r := <-resultCh:
		if r.err != nil {
			return raft.ReadIndexResponse{Term: e.node.CurrentTerm()}
		}
		return raft.ReadIndexResponse{
			Term:      e.node.CurrentTerm(),
			ReadIndex: r.index,
			Success:   true,
		}
	case <-time.After(readTimeout):
		return raft.ReadIndexResponse{Term: e.node.CurrentTerm()}
	}
}

// nextRoundSeq claims a round number. Called by replicator goroutines before
// they send, hence atomic.
func (e *Engine) nextRoundSeq() uint64 { return e.roundSeq.Add(1) }

// reportAck forwards proof-of-leadership to the control loop. Non-blocking: a
// dropped ack costs at most one extra round of latency, and blocking a
// replicator here would be worse than the delay.
func (e *Engine) reportAck(peerID int, startSeq uint64) {
	select {
	case e.ackCh <- ackReport{peerID: peerID, startSeq: startSeq}:
	default:
	}
}

// handleAck records one peer's acknowledgement. Control loop only.
func (e *Engine) handleAck(a ackReport) {
	prev := e.lastAck[a.peerID]
	// Replies can overtake each other; never let an older round overwrite a
	// newer one, or a read could be released on evidence it already rejected.
	if a.startSeq > prev.startSeq {
		prev.startSeq = a.startSeq
	}
	prev.at = time.Now()
	e.lastAck[a.peerID] = prev

	// A read may now be satisfiable.
	e.fireReadWaiters()
}

// resetLeadershipEvidence clears per-peer acks and starts the CheckQuorum clock.
// Called on promotion: evidence gathered under a previous term proves nothing
// about this one, but a brand-new leader must not be judged for silence it has
// not had time to break.
func (e *Engine) resetLeadershipEvidence() {
	now := time.Now()
	e.lastAck = make(map[int]peerAck, len(e.peers))
	for peerID := range e.peers {
		e.lastAck[peerID] = peerAck{at: now}
	}
}

// ---- CheckQuorum (F19) ----

// checkQuorum steps the leader down if it has not heard from a majority within
// quorumGrace.
//
// Raft does not require this: a partitioned leader cannot commit anything, so
// safety holds without it. It is required by everything BUILT on leadership.
// A node that answers "I am the leader" is taken at its word by clients being
// redirected to it (F10) and, before ReadIndex existed, by its own read path.
// Left alone, a deposed leader keeps that claim indefinitely.
//
// Single-node clusters are exempt: there is no majority to hear from beyond
// themselves, and they are never wrong about it.
func (e *Engine) checkQuorum() {
	if len(e.peers) == 0 {
		return
	}

	cutoff := time.Now().Add(-quorumGrace)
	reachable := 1 // ourselves
	for _, ack := range e.lastAck {
		if ack.at.After(cutoff) {
			reachable++
		}
	}

	if reachable > e.node.ClusterSize()/2 {
		// A majority is reachable right now. Bounded reads measure their freshness
		// against this instant.
		e.noteQuorumConfirmed()
		return
	}

	log.Printf("[ENGINE] CheckQuorum: only %d of %d nodes reachable within %s — "+
		"stepping down", reachable, e.node.ClusterSize(), quorumGrace)

	// Step down at the SAME term. We have no evidence of a higher one — only
	// evidence that we can no longer prove we lead this one. Inventing a term
	// bump here would disrupt whichever leader the majority side has already
	// settled on.
	term := e.node.CurrentTerm()
	if err := e.node.BecomeFollower(term, 0); err != nil {
		log.Printf("[ENGINE] CheckQuorum stepdown failed: %v", err)
		return
	}
	e.stopReplicators()
	e.failAllWaiters(ErrNotLeader)
	e.failAllReads(ErrNotLeader)
}

// ---- ReadIndex (F12) ----

// handleRead admits a linearizable read (§8). Control loop only.
//
// Two preconditions, both from §8, and neither optional:
//
//  1. "a leader must have the latest information on which entries are
//     committed. […] at the start of its term, it may not know which those are.
//     To find out, it needs to commit an entry from its term." That is the no-op
//     appended on election; until it commits, this leader's commitIndex may
//     understate the truth and a read against it could miss a committed write.
//
//  2. "a leader must check whether it has been deposed before processing a
//     read-only request." That is the quorum confirmation, completed later in
//     readSatisfied.
//
// The read index is pinned here, at admission. Everything after this is waiting
// for the cluster to catch up to that instant.
func (e *Engine) handleRead(r Read) {
	role, term := e.node.State()
	if role != raft.Leader {
		r.ResultCh <- readResult{err: ErrNotLeader}
		return
	}

	commitIndex, _ := e.node.CommitAndApplyBoundary()
	readIndex := commitIndex
	if e.noopIndex > readIndex {
		// The term's no-op has not committed yet; the read must not be released
		// before it does, so hold the read at least that high.
		readIndex = e.noopIndex
	}

	w := &readWaiter{
		readIndex: readIndex,
		term:      term,
		// Only rounds started from now on count. Anything already in flight
		// carries a follower assertion older than this read.
		barrier:    e.roundSeq.Load(),
		awaitApply: r.awaitApply,
		deadline:   time.Now().Add(readTimeout),
		resultCh:   r.ResultCh,
	}
	e.reads = append(e.reads, w)

	// Hurry a fresh round rather than waiting for the next heartbeat: the read is
	// blocked on evidence that only a new round can produce.
	e.notifyReplicators()

	// A single-node cluster is its own majority and needs no round at all.
	e.fireReadWaiters()
}

// readSatisfied reports whether w may now be released.
func (e *Engine) readSatisfied(w *readWaiter, role raft.NodeRole, term int, commitIndex, lastApplied int) bool {
	if role != raft.Leader || term != w.term {
		return false // caller turns this into ErrNotLeader
	}

	// §8 precaution 1: a current-term entry must be committed.
	if e.noopIndex > commitIndex {
		return false
	}

	// The state machine must actually contain everything up to the read index, or
	// a local read could miss a write the read index promises. Required only when
	// THIS node will serve the read; a follower does its own applying, and making
	// the leader wait for state it will never read would add latency for nothing.
	if w.awaitApply && lastApplied < w.readIndex {
		return false
	}

	// §8 precaution 2: a majority must have confirmed our leadership using
	// evidence generated AFTER the read was admitted. See the file header for why
	// the barrier is a round-start rather than an arrival time.
	confirmed := 1 // ourselves
	for _, ack := range e.lastAck {
		if ack.startSeq > w.barrier {
			confirmed++
		}
	}
	return confirmed > e.node.ClusterSize()/2
}

// fireReadWaiters releases every read whose conditions are met and fails those
// that can no longer be honored. Control loop only.
func (e *Engine) fireReadWaiters() {
	if len(e.reads) == 0 {
		return
	}

	role, term := e.node.State()
	commitIndex, lastApplied := e.node.CommitAndApplyBoundary()

	kept := e.reads[:0]
	for _, w := range e.reads {
		switch {
		case role != raft.Leader || term != w.term:
			// We are no longer the leader that admitted this read. Refuse; do not
			// fall back to a local read, which is exactly the stale answer the
			// mechanism exists to prevent.
			w.resultCh <- readResult{err: ErrNotLeader}
		case e.readSatisfied(w, role, term, commitIndex, lastApplied):
			w.resultCh <- readResult{index: w.readIndex}
		default:
			kept = append(kept, w)
		}
	}
	e.reads = kept
}

// sweepExpiredReads fails reads that could not be confirmed in time.
func (e *Engine) sweepExpiredReads(now time.Time) {
	if len(e.reads) == 0 {
		return
	}
	kept := e.reads[:0]
	for _, w := range e.reads {
		if now.After(w.deadline) {
			w.resultCh <- readResult{err: ErrReadTimeout}
		} else {
			kept = append(kept, w)
		}
	}
	e.reads = kept
}

// failAllReads resolves every pending read with err.
func (e *Engine) failAllReads(err error) {
	for _, w := range e.reads {
		w.resultCh <- readResult{err: err}
	}
	e.reads = e.reads[:0]
}

// ---- Apply waiters: the follower half of a linearizable read ----

// handleApplyWait registers a waiter, or resolves it at once if this node has
// already applied far enough. Control loop only.
func (e *Engine) handleApplyWait(w applyWaiter) {
	if _, applied := e.node.CommitAndApplyBoundary(); applied >= w.index {
		w.resultCh <- nil
		return
	}
	e.applyWaiters = append(e.applyWaiters, w)
}

// fireApplyWaiters releases waiters whose index this node has now applied.
func (e *Engine) fireApplyWaiters() {
	if len(e.applyWaiters) == 0 {
		return
	}
	_, applied := e.node.CommitAndApplyBoundary()

	kept := e.applyWaiters[:0]
	for _, w := range e.applyWaiters {
		if applied >= w.index {
			w.resultCh <- nil
		} else {
			kept = append(kept, w)
		}
	}
	e.applyWaiters = kept
}

// sweepExpiredApplyWaiters fails waiters whose node never caught up — typically
// because it has been partitioned away from the leader shipping the entries.
// Refusing is correct: the read index is a promise about what the answer will
// contain, and we cannot keep it.
func (e *Engine) sweepExpiredApplyWaiters(now time.Time) {
	if len(e.applyWaiters) == 0 {
		return
	}
	kept := e.applyWaiters[:0]
	for _, w := range e.applyWaiters {
		if now.After(w.deadline) {
			w.resultCh <- ErrReadTimeout
		} else {
			kept = append(kept, w)
		}
	}
	e.applyWaiters = kept
}

func (e *Engine) failAllApplyWaiters(err error) {
	for _, w := range e.applyWaiters {
		w.resultCh <- err
	}
	e.applyWaiters = e.applyWaiters[:0]
}

// ---- Bounded-staleness reads (F22) ----

// Consistency selects what a read guarantees. The zero value is Linearizable, so
// a caller that has not thought about consistency gets the safe answer rather
// than the fast one — the opposite of what a `RequireLinearizable bool` field
// would have produced, since a bool's zero value is false.
type Consistency int

const (
	// Linearizable reads reflect everything committed at the moment the read was
	// admitted, confirmed against a quorum. Any node can serve one (F21).
	Linearizable Consistency = iota

	// BoundedStale reads are answered from local state, but only if this node can
	// prove it has been in contact with a leader within MaxStaleness. A node that
	// cannot refuses rather than answering.
	BoundedStale
)

// ReadOptions is what a client asks for.
type ReadOptions struct {
	Consistency Consistency

	// MaxStaleness bounds how out of date the answer may be. Required for
	// BoundedStale and ignored otherwise.
	MaxStaleness time.Duration
}

// Read serves a read under the given options.
func (e *Engine) Read(opts ReadOptions, timeout time.Duration) error {
	switch opts.Consistency {
	case BoundedStale:
		return e.boundedRead(opts.MaxStaleness, timeout)
	default:
		return e.LinearizableRead(timeout)
	}
}

// boundedRead answers from local state if this node's knowledge is recent
// enough, and refuses otherwise.
//
// Two conditions, and both are needed:
//
//  1. Our contact with a leader is within maxStaleness. This bounds how wrong we
//     could be, which is the only thing a replica can honestly promise about its
//     own freshness — it cannot know what it has not been told.
//
//  2. We have applied everything we already know to be committed. A node can be
//     receiving heartbeats happily while its own apply loop lags, which would
//     pass the first check while serving state it KNOWS is behind. Without this,
//     the bound quietly does not mean what it says.
//
// Note what is absent: any comparison of clocks between machines. Both ends of
// the staleness interval are measured on this node with a monotonic clock, so
// unlike a lease read (§8: "this would rely on timing for safety (it assumes
// bounded clock skew)") this tier assumes nothing about anybody else's clock.
func (e *Engine) boundedRead(maxStaleness, timeout time.Duration) error {
	if maxStaleness <= 0 {
		return ErrMaxStalenessRequired
	}

	if age := e.staleness(); age > maxStaleness {
		log.Printf("[ENGINE] refusing bounded read: %v stale, client allows %v",
			age.Truncate(time.Millisecond), maxStaleness)
		return ErrTooStale
	}

	// Condition 2: catch up to what we already know is committed. The wait is
	// bounded by the read timeout; a node that cannot even apply what it has been
	// told is committed has no business answering.
	commitIndex, _ := e.node.CommitAndApplyBoundary()
	if err := e.awaitApplied(commitIndex, timeout); err != nil {
		return ErrTooStale
	}
	return nil
}

// staleness reports how long it has been since this node last had confirmation
// that it was current with a leader.
//
//   - Follower or candidate: time since the last valid contact from a leader.
//   - Leader: time since a majority last confirmed our leadership. A partitioned
//     leader is just another stale server, and gets no exemption — "ask the
//     leader" must not be a way around the staleness rules.
//   - Single-node cluster: always current; there is nobody it could be behind.
func (e *Engine) staleness() time.Duration {
	if len(e.peers) == 0 {
		return 0
	}
	if !e.node.IsLeader() {
		return e.node.TimeSinceContact()
	}
	nanos := e.lastQuorumNanos.Load()
	if nanos == 0 {
		// Promoted but no round has completed yet: we have confirmed nothing.
		return time.Duration(math.MaxInt64)
	}
	return time.Since(time.Unix(0, nanos))
}

// Staleness exposes the current staleness for reporting to clients.
func (e *Engine) Staleness() time.Duration { return e.staleness() }

// noteQuorumConfirmed records that a majority was reachable just now. Called
// from the control loop; stored atomically because reads are served from RPC
// goroutines and must not touch control-loop-owned state.
func (e *Engine) noteQuorumConfirmed() {
	e.lastQuorumNanos.Store(time.Now().UnixNano())
}

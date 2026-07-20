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

// Read is a client's linearizable read request. It carries no key: the engine's
// job is only to establish that a local read is safe NOW, after which the caller
// reads the state machine directly.
type Read struct {
	ResultCh chan error // nil = the local state machine may now be read
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

// readWaiter is a read held until leadership has been confirmed.
type readWaiter struct {
	readIndex int       // state machine must have applied at least this far
	term      int       // leader term the read was admitted under
	barrier   uint64    // only rounds started after this count as evidence
	deadline  time.Time
	resultCh  chan error
}

// ReadQueue returns the channel callers use to submit linearizable reads.
func (e *Engine) ReadQueue() chan<- Read { return e.readCh }

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
		r.ResultCh <- ErrNotLeader
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
		barrier:  e.roundSeq.Load(),
		deadline: time.Now().Add(readTimeout),
		resultCh: r.ResultCh,
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

	// The state machine must actually contain everything up to the read index,
	// or a local read could miss a write the read index promises.
	if lastApplied < w.readIndex {
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
			w.resultCh <- ErrNotLeader
		case e.readSatisfied(w, role, term, commitIndex, lastApplied):
			w.resultCh <- nil
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
			w.resultCh <- ErrReadTimeout
		} else {
			kept = append(kept, w)
		}
	}
	e.reads = kept
}

// failAllReads resolves every pending read with err.
func (e *Engine) failAllReads(err error) {
	for _, w := range e.reads {
		w.resultCh <- err
	}
	e.reads = e.reads[:0]
}

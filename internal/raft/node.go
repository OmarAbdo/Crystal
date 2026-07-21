package raft

// RaftNode owns the consensus state of a single cluster member.
//
// Lock discipline (critical — prevents deadlocks):
//   - RaftNode.mu protects all fields on RaftNode itself.
//   - RaftLog.mu protects all fields on RaftLog.
//   - The two may be held at once, but only in the ORDER rn.mu → rl.mu. A
//     goroutine holding rl.mu must never try to take rn.mu.
//
// This ordering is safe by construction, not by vigilance: RaftLog holds no
// reference to RaftNode and calls nothing on it, so the reverse edge cannot be
// written without first adding that dependency. Keep it that way — the moment a
// RaftLog method needs to consult the node, this invariant has to be re-derived
// rather than assumed.
//
// An earlier version of this comment claimed the two locks were NEVER held
// simultaneously. That was already false when it was written (AdvanceCommitIndex
// calls termAt, which takes rl.mu, while holding rn.mu), and the RPC receivers
// now hold rn.mu across their log work deliberately — the term decision and the
// log mutation it authorizes have to be one atomic step, or a stale-term request
// can splice entries in behind a concurrent term change.
//
// Persistent state (CurrentTerm, VotedFor) is written to a small JSON
// metadata file on every term change. This is separate from the WAL because:
//   - The WAL is append-only; metadata needs in-place updates.
//   - Losing this across a restart allows double-voting, breaking safety.

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"crystal/internal/fsutil"
)

// DefaultMinElectionTimeout is the §6 stickiness window used when the caller has
// not set one. The engine overrides it with its own electionTimeoutMin so the
// two cannot drift apart.
const DefaultMinElectionTimeout = 300 * time.Millisecond

// RaftNode holds the consensus state for one node.
type RaftNode struct {
	mu sync.RWMutex

	// Identity
	nodeID      int
	peerIDs     []int
	clusterSize int // total voting members (peers + self); fixed at startup

	// Volatile state (reset on restart — safe per Raft spec)
	Role        NodeRole
	LeaderID    int
	CommitIndex int
	LastApplied int

	// Election bookkeeping (volatile).
	// lastContact is the moment we last heard from a legitimate leader or granted
	// a vote; the engine compares it against a randomized election timeout to
	// decide when to start an election. votesGranted counts votes in the current
	// election (candidate only).
	lastContact  time.Time
	votesGranted int

	// minElectionTimeout is the §6 stickiness window: a RequestVote arriving
	// within this long of hearing from a live leader is ignored outright, term
	// and all. The engine owns the election timing and sets this to match its own
	// electionTimeoutMin; the default keeps a bare RaftNode sensible in tests.
	minElectionTimeout time.Duration

	// Leader-only volatile state (reinitialized after each election, Figure 2).
	// NextIndex is the index of the next log entry the leader will send to a
	// peer (initialized to leader's lastIndex+1). MatchIndex is the highest
	// index known to be replicated on that peer (initialized to 0).
	NextIndex  map[int]int // peerID → next index to send
	MatchIndex map[int]int // peerID → highest log index confirmed replicated

	// Persistent state (must survive restarts). It is only ever updated through
	// commitPersistentLocked, which writes to store BEFORE adopting the value —
	// see the comment there for why the order matters.
	persistent PersistentState
	store      stateStore
}

// NewRaftNode creates a node, loading persistent state from disk if it exists.
// clusterSize is the total number of voting members (len(peers)+1); it fixes
// the majority threshold used for elections and commitment.
func NewRaftNode(nodeID int, peerIDs []int, clusterSize int, metadataPath string, initialRole NodeRole) (*RaftNode, error) {
	rn := &RaftNode{
		nodeID:             nodeID,
		peerIDs:            peerIDs,
		clusterSize:        clusterSize,
		Role:               initialRole,
		LeaderID:           0, // unknown until we hear from a leader or win an election
		CommitIndex:        0,
		LastApplied:        0,
		lastContact:        time.Now(),
		minElectionTimeout: DefaultMinElectionTimeout,
		NextIndex:          make(map[int]int),
		MatchIndex:         make(map[int]int),
		store:              fileStateStore{path: metadataPath},
		persistent: PersistentState{
			CurrentTerm: 0, // first boot; first election bumps to 1 (Figure 2)
			VotedFor:    -1,
		},
	}

	for _, pid := range peerIDs {
		rn.NextIndex[pid] = 1
		rn.MatchIndex[pid] = 0
	}

	if err := rn.loadPersistentState(); err != nil {
		return nil, err
	}

	return rn, nil
}

// termDecisionHook, when non-nil, is invoked by the RPC receivers at the instant
// they have formed their view of currentTerm and are about to act on it. It is
// always nil in production.
//
// It exists because the atomicity of that decision cannot be tested any other
// way. The window between reading the term and using it is microseconds wide, so
// racing goroutines at it proves nothing when they pass — the test has to be able
// to stall one request there and run another to completion. Everything the hook
// guards is inside the receiver's critical section, so an implementation that has
// correctly made the decision atomic will hold rn.mu while the hook runs; that is
// the property the tests rely on.
var termDecisionHook func(reqTerm int)

// ---- Public read accessors (safe for concurrent callers) ----

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role == Leader
}

// CurrentTerm returns the node's current Raft term.
func (rn *RaftNode) CurrentTerm() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.persistent.CurrentTerm
}

// State returns role and term atomically.
func (rn *RaftNode) State() (NodeRole, int) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role, rn.persistent.CurrentTerm
}

// NodeID returns the immutable node identifier.
func (rn *RaftNode) NodeID() int {
	return rn.nodeID
}

// SetMinElectionTimeout sets the §6 stickiness window. The engine calls this so
// the window matches the election timing it actually uses; a mismatch either
// shields a dead leader for too long or leaves a live one unprotected.
func (rn *RaftNode) SetMinElectionTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.minElectionTimeout = d
}

// CurrentLeader returns the ID of the leader this node last heard from, or 0 if
// none is known (between a leader's failure and the next election).
//
// It is a hint, not a guarantee: the value is only as fresh as the last
// AppendEntries this node received, and the named leader may already have been
// deposed. Clients use it to avoid rediscovering the leader by guessing, and must
// still be prepared to be redirected again.
func (rn *RaftNode) CurrentLeader() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.LeaderID
}

// ---- Mutation methods ----

// UpdatePeerProgress records that peerID has replicated up to matchIndex.
// Called by the replicator after a successful AppendEntries response.
// Per Figure 2, a success advances both matchIndex and nextIndex for the peer.
func (rn *RaftNode) UpdatePeerProgress(peerID, matchIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// M3: an unknown peer would create a map entry, and AdvanceCommitIndex counts
	// one index per MatchIndex entry to find the quorum median. An extra entry
	// shifts that median and can commit on less than a majority. Reject rather
	// than silently growing the map.
	if _, known := rn.MatchIndex[peerID]; !known {
		log.Printf("[RAFT] ignoring progress from unknown peer %d", peerID)
		return
	}

	if matchIndex > rn.MatchIndex[peerID] {
		rn.MatchIndex[peerID] = matchIndex
		log.Printf("[RAFT] Peer %d matchIndex → %d", peerID, matchIndex)
	}
	// nextIndex is always one past the highest confirmed match.
	if matchIndex+1 > rn.NextIndex[peerID] {
		rn.NextIndex[peerID] = matchIndex + 1
	}
}

// NextIndexFor returns the next log index the leader should send to peerID.
func (rn *RaftNode) NextIndexFor(peerID int) int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	ni := rn.NextIndex[peerID]
	if ni < 1 {
		return 1
	}
	return ni
}

// BacktrackNextIndex lowers nextIndex for peerID after an AppendEntries
// rejection so the leader can find the point where the logs agree (§5.3).
//
// It uses the follower's conflict hints when available: conflictIndex is the
// first index the follower holds for the conflicting term (or its log length+1
// when it is simply missing entries). This lets the leader skip an entire term
// of conflicting entries in one step rather than decrementing by one per RPC.
// When no hint is supplied (conflictIndex <= 0) it falls back to a plain
// decrement. nextIndex never drops below 1.
func (rn *RaftNode) BacktrackNextIndex(peerID, conflictIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	current := rn.NextIndex[peerID]
	var next int
	switch {
	case conflictIndex > 0 && conflictIndex < current:
		next = conflictIndex
	default:
		next = current - 1
	}
	if next < 1 {
		next = 1
	}
	rn.NextIndex[peerID] = next
	log.Printf("[RAFT] Peer %d nextIndex backtracked → %d", peerID, next)
}

// AdvanceCommitIndex checks whether a new commit index can be established
// based on quorum agreement. It reads the local latest index from the caller
// (to avoid holding both locks) and returns the new commit index if advanced,
// or 0 if not.
//
// Raft safety rule (§5.4.2): a leader may only advance commitIndex to an entry
// N when log[N].term == currentTerm. It must NOT commit an entry from a previous
// term merely because a majority has replicated it — Figure 8 shows how doing so
// lets a later higher-term leader overwrite a supposedly-committed entry.
// Previous-term entries commit implicitly, once a current-term entry above them
// reaches quorum.
//
// termAt returns the term of the entry at a given log index. It is supplied by
// the caller rather than read from a stored RaftLog reference so that RaftNode
// keeps no dependency on the log type. It is invoked while rn.mu is held, which
// is permitted: the lock order is rn.mu → rl.mu (see the file header).
func (rn *RaftNode) AdvanceCommitIndex(localLatestIndex int, termAt func(index int) int, currentTerm int) (newCommitIndex int, advanced bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.Role != Leader {
		return rn.CommitIndex, false
	}

	// Gather all known replicated indices (self + peers).
	indices := make([]int, 0, len(rn.peerIDs)+1)
	indices = append(indices, localLatestIndex)
	for _, idx := range rn.MatchIndex {
		indices = append(indices, idx)
	}

	// Sort descending; the median is the highest index replicated on a majority.
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	quorumIndex := indices[len(indices)/2]

	// Commit only if the QUORUM INDEX ITSELF is a current-term entry (§5.4.2).
	// Checking the term of quorumIndex — not the leader's last entry — is what
	// prevents the Figure 8 violation.
	if quorumIndex > rn.CommitIndex && termAt(quorumIndex) == currentTerm {
		rn.CommitIndex = quorumIndex
		log.Printf("[RAFT] CommitIndex advanced to %d", rn.CommitIndex)
		return rn.CommitIndex, true
	}

	return rn.CommitIndex, false
}

// HandleAppendEntries implements the follower side of the AppendEntries RPC
// receiver (Figure 2). It is the single authority for the consistency check and
// commit advancement, keeping that consensus logic out of the transport layer.
//
// Steps:
//  1. Reply false if req.Term < currentTerm (§5.1) — stale leader.
//  2. If req.Term > currentTerm (or we're a candidate/leader at the same term),
//     recognize the sender as leader and step down (§5.2). Any valid AppendEntries
//     also refreshes our sense that a leader is alive.
//  3. Run the log-matching check + splice (steps 2–4) via RaftLog.AppendEntriesToLog.
//  4. On success, advance commitIndex = min(leaderCommit, last new index) (step 5).
//
// It takes the RaftLog explicitly rather than holding a reference so that
// RaftNode keeps no dependency on the log type. The whole receiver runs under
// rn.mu, and the log calls happen inside it — the lock order is rn.mu → rl.mu
// (see the file header).
//
// Holding the lock across the log work is the point, not an accident. The term
// check in step 1 authorizes everything that follows it; if the check and the
// mutation are separate critical sections, two concurrent requests can both read
// the same currentTerm, the newer one can raise the term, and the older one —
// already past its check — still splices its entries in and reports Success. That
// is a Log Matching violation produced entirely by lock granularity, and it is
// reachable whenever a leader change overlaps in-flight replication.
func (rn *RaftNode) HandleAppendEntries(rl *RaftLog, req AppendEntriesRequest) AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if termDecisionHook != nil {
		termDecisionHook(req.Term)
	}

	currentTerm := rn.persistent.CurrentTerm

	// Step 1: reject a stale leader outright.
	if req.Term < currentTerm {
		return AppendEntriesResponse{Term: currentTerm, Success: false}
	}

	// Step 2: the leader's term is current or newer. Accept its authority and,
	// if the term advanced (or we thought we were leader/candidate), step down.
	// The term must be persisted before we act on the entries (Figure 2,
	// stable-storage rule); on failure we neither adopt it nor touch the log.
	if req.Term > currentTerm || rn.Role != Follower {
		if err := rn.becomeFollowerLocked(req.Term, req.LeaderID); err != nil {
			log.Printf("[RAFT] BecomeFollower during AppendEntries failed: %v", err)
			return AppendEntriesResponse{Term: currentTerm, Success: false}
		}
		currentTerm = rn.persistent.CurrentTerm
	} else {
		// Same term, already a follower: just refresh the known leader.
		rn.LeaderID = req.LeaderID
	}

	// A valid AppendEntries from the current leader resets the election timer:
	// this is how a live leader's heartbeats suppress spurious elections (§5.2).
	rn.lastContact = time.Now()

	// Steps 2–4: consistency check and splice, owned by the log.
	matchIndex, conflictTerm, conflictIndex, ok := rl.AppendEntriesToLog(
		req.PrevLogIndex, req.PrevLogTerm, req.Entries)
	if !ok {
		return AppendEntriesResponse{
			Term:          currentTerm,
			Success:       false,
			ConflictTerm:  conflictTerm,
			ConflictIndex: conflictIndex,
		}
	}

	// Step 5: advance our commit index toward the leader's.
	rn.setFollowerCommitIndexLocked(req.LeaderCommit, matchIndex)

	return AppendEntriesResponse{
		Term:       currentTerm,
		Success:    true,
		MatchIndex: matchIndex,
	}
}

// HandleInstallSnapshot implements the InstallSnapshot receiver (Figure 13). It
// is invoked when the leader has already compacted past a follower's nextIndex
// and must ship the whole snapshot instead of log entries.
//
// The state-machine restore and snapshot-persist steps are passed as callbacks
// because the raft package cannot import store (store imports raft). This keeps
// all consensus/term logic here while the effects live in the caller's layer.
//
//	restore(data)   — reset the state machine from the snapshot bytes (step 8)
//	persist()       — durably write the snapshot to disk (step 5), called only
//	                  after the log has been reset so a crash can't leave a
//	                  snapshot that references discarded entries
//
// Steps 6–7 (log reset) are performed by rl.ResetToSnapshot.
func (rn *RaftNode) HandleInstallSnapshot(
	rl *RaftLog,
	req InstallSnapshotRequest,
	restore func(data []byte) error,
	persist func() error,
) InstallSnapshotResponse {
	// As in HandleAppendEntries, the term decision and everything it authorizes
	// are one critical section. This receiver destroys log state and resets the
	// state machine on the strength of that decision, so a request that lost the
	// term race must not be able to act on a check it passed before the race was
	// settled. Lock order is rn.mu → rl.mu (see the file header).
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if termDecisionHook != nil {
		termDecisionHook(req.Term)
	}

	currentTerm := rn.persistent.CurrentTerm

	// Step 1: reject a stale leader.
	if req.Term < currentTerm {
		return InstallSnapshotResponse{Term: currentTerm}
	}

	// Accept the leader's authority; step down if the term advanced or we were
	// not already a follower. Persist-before-respond is honored by
	// becomeFollowerLocked.
	if req.Term > currentTerm || rn.Role != Follower {
		if err := rn.becomeFollowerLocked(req.Term, req.LeaderID); err != nil {
			log.Printf("[RAFT] BecomeFollower during InstallSnapshot failed: %v", err)
			return InstallSnapshotResponse{Term: currentTerm}
		}
		currentTerm = rn.persistent.CurrentTerm
	} else {
		rn.LeaderID = req.LeaderID
	}
	rn.lastContact = time.Now() // a valid snapshot is a sign of life

	// If we already cover this snapshot's index there is nothing to do — but this
	// IS a success: we genuinely hold everything through that index, which is
	// exactly what the leader is asking about. Reporting failure here would stall
	// a follower that is already caught up.
	if req.LastIncludedIndex <= rl.FirstIndex()-1 {
		return InstallSnapshotResponse{Term: currentTerm, Success: true}
	}

	// Step 8: reset the state machine from the snapshot data.
	if err := restore(req.Data); err != nil {
		log.Printf("[RAFT] InstallSnapshot state restore failed: %v", err)
		return InstallSnapshotResponse{Term: currentTerm}
	}

	// Step 5: persist the snapshot BEFORE anything is discarded on its behalf.
	//
	// The log reset below destroys entries this follower has already fsync-acked
	// to a leader. If we crash between the reset and the persist, those entries
	// are gone and the state that replaced them was never written — nothing on
	// disk can reconstruct either. The reverse ordering fails safely: a persisted
	// snapshot covering entries still present in the log is reconciled on the
	// next startup by RaftLog.RestoreOffset.
	if err := persist(); err != nil {
		log.Printf("[RAFT] InstallSnapshot persist failed: %v", err)
		return InstallSnapshotResponse{Term: currentTerm}
	}

	// Raise commit/applied to the boundary BEFORE the log shrinks. The engine's
	// apply loop runs on another goroutine and halts on a committed entry it
	// cannot read (see Engine.applyCommitted); shrinking the log first would
	// expose exactly that state for as long as the two calls are apart.
	rn.seedFromSnapshotLocked(req.LastIncludedIndex)

	// Steps 6–7: reconcile the log against the snapshot boundary.
	if err := rl.ResetToSnapshot(req.LastIncludedIndex, req.LastIncludedTerm); err != nil {
		log.Printf("[RAFT] InstallSnapshot log reset failed: %v", err)
		return InstallSnapshotResponse{Term: currentTerm}
	}

	log.Printf("[RAFT] Installed snapshot through index %d term %d",
		req.LastIncludedIndex, req.LastIncludedTerm)
	return InstallSnapshotResponse{Term: currentTerm, Success: true}
}

// snapshotFloor returns the follower's current compaction boundary, used to
// detect a redundant (stale) InstallSnapshot.
func (rn *RaftNode) snapshotFloor(rl *RaftLog) int {
	return rl.FirstIndex() - 1
}

// ---- Election ----

// noteContact records that we just heard from a legitimate leader or granted a
// vote, resetting the election-timeout clock.
func (rn *RaftNode) noteContact() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.lastContact = time.Now()
}

// TimeSinceContact reports how long it has been since the last valid contact
// from a leader or vote grant. The engine compares this against a randomized
// election timeout to decide when to start an election.
func (rn *RaftNode) TimeSinceContact() time.Duration {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return time.Since(rn.lastContact)
}

// ClusterSize returns the fixed number of voting members.
func (rn *RaftNode) ClusterSize() int {
	return rn.clusterSize
}

// BecomeCandidate starts a new election (Figure 2, Candidates): it increments
// the term, votes for itself, resets the election timer, and persists the new
// term+vote to stable storage before any RequestVote RPCs go out. It returns
// the new term so the caller can build the RequestVote requests. The caller
// supplies the candidate's last log index/term (read from the RaftLog under its
// own lock) to preserve the never-hold-both-locks discipline.
func (rn *RaftNode) BecomeCandidate() (term int, err error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Record the new term and self-vote on stable storage BEFORE adopting them.
	// Adopting first would let a persist failure leave the node campaigning in a
	// term it cannot prove it entered: after a crash it would return at the old
	// term with no vote recorded and could vote for someone else in the term it
	// had already spent on itself.
	next := PersistentState{
		CurrentTerm: rn.persistent.CurrentTerm + 1,
		VotedFor:    rn.nodeID,
	}
	if err := rn.commitPersistentLocked(next); err != nil {
		return 0, fmt.Errorf("persist candidate state: %w", err)
	}

	rn.Role = Candidate
	rn.LeaderID = 0
	rn.votesGranted = 1 // vote for self
	rn.lastContact = time.Now()

	log.Printf("[RAFT] Node %d starting election for term %d", rn.nodeID, rn.persistent.CurrentTerm)
	return rn.persistent.CurrentTerm, nil
}

// RecordVoteAndCheckMajority tallies one granted vote for the current election
// and reports whether the candidate now holds a majority. It ignores votes if
// the node is no longer a candidate or the vote came from a stale term.
func (rn *RaftNode) RecordVoteAndCheckMajority(voteTerm int) (wonMajority bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.Role != Candidate || voteTerm != rn.persistent.CurrentTerm {
		return false
	}
	rn.votesGranted++
	return rn.votesGranted > rn.clusterSize/2
}

// HandleRequestVote implements the RequestVote receiver (Figure 2). It is the
// single authority for the vote decision, keeping consensus logic out of
// transport. The §5.4.1 election restriction is enforced via the up-to-date
// comparison. Vote grants are persisted before responding (stable-storage rule).
// lastLogState is supplied as a callback rather than as two pre-read values so
// that our side of the §5.4.1 comparison is sampled INSIDE the vote's critical
// section. Sampling it beforehand lets a concurrent AppendEntries extend our log
// between the sample and the decision, so we would grant a vote to a candidate
// that is no longer as up-to-date as we are — and the election restriction is
// precisely what Leader Completeness rests on. Calling it here is legal under the
// rn.mu → rl.mu order (see the file header).
func (rn *RaftNode) HandleRequestVote(req RequestVoteRequest, lastLogState func() (index, term int)) RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	myLastIndex, myLastTerm := lastLogState()

	// Step 1: reject a candidate from an older term.
	if req.Term < rn.persistent.CurrentTerm {
		return RequestVoteResponse{Term: rn.persistent.CurrentTerm, VoteGranted: false}
	}

	// §6, final paragraph: "servers disregard RequestVote RPCs when they believe
	// a current leader exists. Specifically, if a server receives a RequestVote
	// RPC within the minimum election timeout of hearing from a current leader,
	// it does not update its term or grant its vote."
	//
	// This is the paper's remedy for disruptive servers. A node that was briefly
	// partitioned comes back with a term inflated by every election it held
	// alone; without this check its first RequestVote drags the whole cluster up
	// to that term and deposes a leader that was working perfectly. Note what is
	// NOT done here: the term is not adopted. Replying with our own lower term is
	// the entire point — acknowledging theirs is the disruption.
	//
	// The guard is deliberately narrow. It applies only when we know of a live
	// leader (LeaderID != 0); a node that has merely granted a vote recently has
	// no leader to protect and must stay free to vote again. And it lapses after
	// one minimum election timeout, so a leader that has genuinely died stops
	// being shielded and a real election proceeds.
	if rn.LeaderID != 0 && time.Since(rn.lastContact) < rn.minElectionTimeout {
		log.Printf("[RAFT] Node %d ignoring RequestVote from %d (term %d): heard from "+
			"leader %d %v ago", rn.nodeID, req.CandidateID, req.Term,
			rn.LeaderID, time.Since(rn.lastContact).Truncate(time.Millisecond))
		return RequestVoteResponse{Term: rn.persistent.CurrentTerm, VoteGranted: false}
	}

	// Decide everything against a candidate copy of the persistent state, and
	// adopt it only once it is durable. Nothing below mutates rn.persistent
	// directly.
	next := rn.persistent

	// A newer term means we must step down and clear our old vote before we can
	// consider granting one in the new term (§5.1, "All Servers" rule).
	steppedUp := req.Term > next.CurrentTerm
	if steppedUp {
		next.CurrentTerm = req.Term
		next.VotedFor = -1
	}

	// Step 2: grant the vote iff we haven't voted for anyone else this term AND
	// the candidate's log is at least as up-to-date as ours (§5.4.1).
	alreadyVoted := next.VotedFor != -1 && next.VotedFor != req.CandidateID
	upToDate := candidateUpToDate(req.LastLogTerm, req.LastLogIndex, myLastTerm, myLastIndex)
	grant := !alreadyVoted && upToDate
	if grant {
		next.VotedFor = req.CandidateID
	}

	// If the write fails we have promised nothing and changed nothing: report our
	// durable term (NOT the term we failed to record — the candidate would read
	// that as proof we stepped up) and deny.
	if err := rn.commitPersistentLocked(next); err != nil {
		log.Printf("[RAFT] persist during vote decision failed: %v", err)
		return RequestVoteResponse{Term: rn.persistent.CurrentTerm, VoteGranted: false}
	}

	if steppedUp {
		rn.Role = Follower
		rn.LeaderID = 0
	}
	if !grant {
		return RequestVoteResponse{Term: rn.persistent.CurrentTerm, VoteGranted: false}
	}

	rn.lastContact = time.Now() // granting a vote resets our election timer
	log.Printf("[RAFT] Node %d granted vote to %d for term %d", rn.nodeID, req.CandidateID, req.Term)
	return RequestVoteResponse{Term: rn.persistent.CurrentTerm, VoteGranted: true}
}

// candidateUpToDate implements Raft's "at least as up-to-date" comparison
// (§5.4.1): the log with the later last-entry term is more up-to-date; if the
// terms tie, the longer log wins. Returns true iff the candidate is at least
// as up-to-date as the voter.
func candidateUpToDate(candTerm, candIndex, voterTerm, voterIndex int) bool {
	if candTerm != voterTerm {
		return candTerm > voterTerm
	}
	return candIndex >= voterIndex
}

// SetFollowerCommitIndex is called on followers when the leader's
// heartbeat/append carries a leaderCommit value.
func (rn *RaftNode) SetFollowerCommitIndex(leaderCommit, latestLogIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.setFollowerCommitIndexLocked(leaderCommit, latestLogIndex)
}

// setFollowerCommitIndexLocked is the body of SetFollowerCommitIndex for callers
// already holding rn.mu.
//
// Caller must hold rn.mu (write).
func (rn *RaftNode) setFollowerCommitIndexLocked(leaderCommit, latestLogIndex int) {
	// Raft §5.3: commitIndex = min(leaderCommit, index of last new entry)
	newCommit := leaderCommit
	if latestLogIndex < newCommit {
		newCommit = latestLogIndex
	}

	if newCommit > rn.CommitIndex {
		rn.CommitIndex = newCommit
		log.Printf("[RAFT] Follower commitIndex → %d", rn.CommitIndex)
	}
}

// AdvanceLastApplied increments LastApplied by one and returns the new value.
// The engine calls this in a loop until LastApplied == CommitIndex.
func (rn *RaftNode) AdvanceLastApplied() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.LastApplied++
	return rn.LastApplied
}

// CommitAndApplyBoundary returns (commitIndex, lastApplied) atomically
// so the engine can decide how many entries to apply without racing.
func (rn *RaftNode) CommitAndApplyBoundary() (commitIndex, lastApplied int) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.CommitIndex, rn.LastApplied
}

// SeedFromSnapshot sets CommitIndex and LastApplied to a snapshot's last
// included index at startup. Without this, a node that restarts from a snapshot
// starts with commit/applied at 0 and the apply loop walks through already-
// compacted indices ("Missing log entry — skipping") — those entries are gone
// but their effects are already baked into the restored state machine.
// Raises the boundaries monotonically; never lowers them.
func (rn *RaftNode) SeedFromSnapshot(lastIncludedIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.seedFromSnapshotLocked(lastIncludedIndex)
}

// seedFromSnapshotLocked is the body of SeedFromSnapshot for callers already
// holding rn.mu.
//
// Caller must hold rn.mu (write).
func (rn *RaftNode) seedFromSnapshotLocked(lastIncludedIndex int) {
	if lastIncludedIndex > rn.CommitIndex {
		rn.CommitIndex = lastIncludedIndex
	}
	if lastIncludedIndex > rn.LastApplied {
		rn.LastApplied = lastIncludedIndex
	}
}

// BecomeFollower transitions the node to follower state, updating term and
// persisting. Called both when a higher term is observed (election loss,
// stale-leader stepdown) and when a same-term leader asserts authority over a
// candidate.
//
// VotedFor is cleared ONLY when the term actually increases (review bug #3): a
// new term is a fresh voting round, but a same-term stepdown must preserve the
// vote already cast this term to honor Raft's one-vote-per-term invariant.
func (rn *RaftNode) BecomeFollower(term, leaderID int) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.becomeFollowerLocked(term, leaderID)
}

// becomeFollowerLocked is BecomeFollower's body, for callers that already hold
// rn.mu — the RPC receivers, which must not drop the lock between deciding on a
// term and acting on it.
//
// Caller must hold rn.mu (write).
func (rn *RaftNode) becomeFollowerLocked(term, leaderID int) error {
	next := rn.persistent
	if term > next.CurrentTerm {
		next.CurrentTerm = term
		next.VotedFor = -1
	}

	// Persist the term bump before acting on it. On failure the node keeps its
	// old term and role rather than stepping down into a term it cannot prove it
	// reached. A same-term stepdown changes nothing persistent, so
	// commitPersistentLocked skips the write and this cannot fail — a leader
	// yielding to a same-term leader must never be blocked by a broken disk.
	if err := rn.commitPersistentLocked(next); err != nil {
		return fmt.Errorf("persist follower state: %w", err)
	}

	rn.Role = Follower
	rn.LeaderID = leaderID
	return nil
}

// BecomeLeader promotes the node to leader after winning an election. It is
// guarded against a stepdown race (review bug #2): between tallying the winning
// vote and calling this, a higher-term RPC on an HTTP goroutine may have already
// stepped the node down to follower. Promoting anyway would create two leaders
// in that higher term. So this no-ops (returning false) unless, under the lock,
// the node is STILL a candidate in electionTerm.
//
// On success it reinitializes leader volatile state per Figure 2: nextIndex for
// every peer to lastLogIndex+1, matchIndex to 0. The leader then re-probes each
// follower via the AppendEntries consistency check, backtracking as needed.
func (rn *RaftNode) BecomeLeader(electionTerm, lastLogIndex int) (promoted bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.Role != Candidate || rn.persistent.CurrentTerm != electionTerm {
		log.Printf("[RAFT] Node %d abandoning stale election win (term %d, role %s, current term %d)",
			rn.nodeID, electionTerm, rn.Role, rn.persistent.CurrentTerm)
		return false
	}

	rn.Role = Leader
	rn.LeaderID = rn.nodeID
	for pid := range rn.MatchIndex {
		rn.NextIndex[pid] = lastLogIndex + 1
		rn.MatchIndex[pid] = 0
	}
	log.Printf("[RAFT] Node %d became Leader for term %d", rn.nodeID, rn.persistent.CurrentTerm)
	return true
}

// ---- Persistent state management ----

// stateStore is the durability seam for Raft's Figure 2 persistent state. It
// exists so the node's transitions can be tested against a failing disk: every
// one of them must leave in-memory state untouched when Save fails, and that
// property is invisible to a test that can only ever succeed.
type stateStore interface {
	// Load returns the stored state. ok is false when nothing has been stored
	// yet (first boot), in which case the caller keeps its defaults.
	Load() (state PersistentState, ok bool, err error)

	// Save durably records state. It must not return nil until the write has
	// reached stable storage.
	Save(state PersistentState) error
}

// fileStateStore keeps the state as a small JSON document. It is separate from
// the WAL because the WAL is append-only and this record needs in-place update.
type fileStateStore struct {
	path string
}

func (f fileStateStore) Load() (PersistentState, bool, error) {
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return PersistentState{}, false, nil
	}
	if err != nil {
		return PersistentState{}, false, fmt.Errorf("read raft metadata: %w", err)
	}

	var ps PersistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return PersistentState{}, false, fmt.Errorf("parse raft metadata: %w", err)
	}
	return ps, true, nil
}

// Save writes term+votedFor durably, not merely atomically. Figure 2 requires
// this record to be on stable storage before the node responds to an RPC; if it
// is only in the page cache, a power cut resurrects the node in a term it has
// already voted in, it votes a second time, and two leaders are elected for that
// term. WriteFileAtomic fsyncs both the file and its directory, so the record —
// and the rename that publishes it — actually survive.
func (f fileStateStore) Save(state PersistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := fsutil.WriteFileAtomic(f.path, data, 0666); err != nil {
		return fmt.Errorf("persist raft metadata: %w", err)
	}
	return nil
}

func (rn *RaftNode) loadPersistentState() error {
	ps, ok, err := rn.store.Load()
	if err != nil {
		return err
	}
	if !ok {
		// First boot: write the constructor defaults so the file exists and the
		// node's durable term is unambiguous from here on.
		return rn.store.Save(rn.persistent)
	}

	rn.persistent = ps
	log.Printf("[RAFT] Loaded persistent state: term=%d votedFor=%d", ps.CurrentTerm, ps.VotedFor)
	return nil
}

// commitPersistentLocked durably records next and, only on success, adopts it as
// the node's in-memory state. This ordering is the whole point: adopting first
// and persisting second leaves the node acting on a term it may not be able to
// prove it reached.
//
// A no-op change skips the write entirely, so transitions that alter no
// persistent state (a same-term stepdown) cannot be blocked by a broken disk.
//
// Caller must hold rn.mu (write).
func (rn *RaftNode) commitPersistentLocked(next PersistentState) error {
	if next == rn.persistent {
		return nil
	}
	if err := rn.store.Save(next); err != nil {
		return err
	}
	rn.persistent = next
	return nil
}

// HandlePreVote answers a pre-vote straw poll (dissertation §9.6).
//
// The defining property is that it changes NOTHING. No term is adopted, no vote
// is recorded, no timer is reset. A pre-vote is a question — "would you vote for
// me if I asked?" — and answering a question must not commit the answerer to
// anything, or the poll would itself become the disruption it prevents.
//
// A grant requires all three of:
//
//   - no live leader within the stickiness window (§6), because a node happy
//     with its current leader should not encourage a challenger;
//   - the candidate's hypothetical term actually exceeds ours, since a candidate
//     that is behind cannot win;
//   - the candidate's log is at least as up-to-date as ours (§5.4.1), the same
//     restriction a real vote applies — otherwise the poll would green-light a
//     campaign the real election must reject.
func (rn *RaftNode) HandlePreVote(req PreVoteRequest, lastLogState func() (index, term int)) PreVoteResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	resp := PreVoteResponse{Term: rn.persistent.CurrentTerm}

	if rn.LeaderID != 0 && time.Since(rn.lastContact) < rn.minElectionTimeout {
		return resp
	}
	if req.Term <= rn.persistent.CurrentTerm {
		return resp
	}

	myLastIndex, myLastTerm := lastLogState()
	resp.VoteGranted = candidateUpToDate(req.LastLogTerm, req.LastLogIndex, myLastTerm, myLastIndex)
	return resp
}

// NoteContactForTest records contact from a leader. It exists so tests can place
// a node at a known point on the staleness clock without standing up a cluster.
func (rn *RaftNode) NoteContactForTest() { rn.noteContact() }

package raft

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newTestNode(t *testing.T, nodeID int, peerIDs []int, clusterSize int) *RaftNode {
	t.Helper()
	meta := filepath.Join(t.TempDir(), "raft.meta")
	rn, err := NewRaftNode(nodeID, peerIDs, clusterSize, meta, Follower)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	return rn
}

// staticLogState adapts a fixed (index, term) pair to the callback
// HandleRequestVote takes. Real callers pass RaftLog.LastLogState, which is read
// inside the vote's critical section; tests that are not exercising that timing
// just need a constant.
func staticLogState(index, term int) func() (int, int) {
	return func() (int, int) { return index, term }
}

// ---- §5.4.1 up-to-date comparison ----

func TestCandidateUpToDate(t *testing.T) {
	tests := []struct {
		name                                       string
		candTerm, candIndex, voterTerm, voterIndex int
		want                                       bool
	}{
		{"higher last term wins", 3, 1, 2, 9, true},
		{"lower last term loses", 2, 9, 3, 1, false},
		{"same term longer log wins", 2, 5, 2, 3, true},
		{"same term equal length ok", 2, 4, 2, 4, true},
		{"same term shorter log loses", 2, 3, 2, 5, false},
		{"empty candidate vs empty voter", 0, 0, 0, 0, true},
		{"empty candidate vs nonempty voter", 0, 0, 1, 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := candidateUpToDate(tc.candTerm, tc.candIndex, tc.voterTerm, tc.voterIndex)
			if got != tc.want {
				t.Fatalf("candidateUpToDate(%d,%d,%d,%d) = %v, want %v",
					tc.candTerm, tc.candIndex, tc.voterTerm, tc.voterIndex, got, tc.want)
			}
		})
	}
}

// ---- HandleRequestVote ----

func TestHandleRequestVote_GrantsWhenEligible(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3) // starts at term 0, votedFor -1

	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 1, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	}, staticLogState(0, 0))

	if !resp.VoteGranted {
		t.Fatalf("expected vote granted")
	}
	if resp.Term != 1 {
		t.Fatalf("resp.Term = %d, want 1 (stepped up)", resp.Term)
	}
	if rn.persistent.VotedFor != 2 {
		t.Fatalf("VotedFor = %d, want 2", rn.persistent.VotedFor)
	}
}

func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	// Advance our term to 5 so a term-3 candidate is stale.
	if err := rn.BecomeFollower(5, 0); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}

	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 3, CandidateID: 2, LastLogIndex: 10, LastLogTerm: 5,
	}, staticLogState(0, 0))

	if resp.VoteGranted {
		t.Fatalf("expected rejection of stale-term candidate")
	}
	if resp.Term != 5 {
		t.Fatalf("resp.Term = %d, want 5 (our current term)", resp.Term)
	}
}

func TestHandleRequestVote_RejectsWhenAlreadyVotedOther(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)

	// Grant to candidate 2 for term 1.
	first := rn.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}, staticLogState(0, 0))
	if !first.VoteGranted {
		t.Fatalf("setup: first vote should be granted")
	}

	// Candidate 3 asks for the same term — must be denied (one vote per term).
	second := rn.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 3}, staticLogState(0, 0))
	if second.VoteGranted {
		t.Fatalf("expected denial: already voted for candidate 2 this term")
	}
}

func TestHandleRequestVote_GrantIsIdempotentForSameCandidate(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)

	rn.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}, staticLogState(0, 0))
	// Same candidate re-asks (e.g. retransmit) — should still be granted.
	resp := rn.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}, staticLogState(0, 0))
	if !resp.VoteGranted {
		t.Fatalf("expected repeat grant to same candidate")
	}
}

func TestHandleRequestVote_RejectsLessUpToDateLog(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)

	// Our log is at index 5 term 3. Candidate's log ends at term 2 — behind us.
	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 4, CandidateID: 2, LastLogIndex: 9, LastLogTerm: 2,
	}, staticLogState(5 /*myLastIndex*/, 3 /*myLastTerm*/))

	if resp.VoteGranted {
		t.Fatalf("expected denial: candidate log is less up-to-date (§5.4.1)")
	}
	// But we still stepped up to the newer term 4.
	if resp.Term != 4 {
		t.Fatalf("resp.Term = %d, want 4 (term still advances on denial)", resp.Term)
	}
}

func TestHandleRequestVote_HigherTermResetsPriorVote(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)

	// Vote for candidate 2 in term 1.
	rn.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}, staticLogState(0, 0))
	// Candidate 3 arrives with a higher term 2 — the higher term clears the old
	// vote, so 3 is now eligible.
	resp := rn.HandleRequestVote(RequestVoteRequest{Term: 2, CandidateID: 3}, staticLogState(0, 0))
	if !resp.VoteGranted {
		t.Fatalf("expected grant: higher term should reset prior vote")
	}
}

// ---- AdvanceCommitIndex quorum + current-term safety (§5.4.2) ----

func TestAdvanceCommitIndex_CommitsOnQuorumCurrentTerm(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.Role = Leader
	rn.persistent.CurrentTerm = 5
	// Peer 2 has replicated up to index 4, peer 3 up to index 2.
	rn.MatchIndex[2] = 4
	rn.MatchIndex[3] = 2

	// Leader's own latest index 4. Indices are {4(self),4,2}; sorted desc
	// {4,4,2}, median at position len/2=1 → 4. Entry 4 is in the current term 5.
	termAt := func(int) int { return 5 }
	newCommit, advanced := rn.AdvanceCommitIndex(4, termAt, 5)
	if !advanced {
		t.Fatalf("expected commit index to advance")
	}
	if newCommit != 4 {
		t.Fatalf("commitIndex = %d, want 4 (quorum median)", newCommit)
	}
}

func TestAdvanceCommitIndex_RefusesOldTermEntry(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.Role = Leader
	rn.persistent.CurrentTerm = 5
	rn.MatchIndex[2] = 4
	rn.MatchIndex[3] = 4

	// Even though index 4 is on a majority, its term (3) is NOT the current term
	// (5). Per §5.4.2 a leader must not commit an entry from a previous term by
	// counting replicas — so commit must NOT advance.
	termAt := func(int) int { return 3 } // entry 4 is a stale-term entry
	_, advanced := rn.AdvanceCommitIndex(4, termAt, 5)
	if advanced {
		t.Fatalf("expected NO commit: entry is from a previous term (§5.4.2)")
	}
}

// TestAdvanceCommitIndex_Figure8 reproduces the exact Figure 8 violation the
// external review found: the quorum index is an OLD-term entry even though the
// leader's LAST entry is current-term. The buggy guard (checking the last
// entry's term) would commit a previous-term entry by replica-counting, which
// a higher-term candidate could then overwrite. The correct guard checks the
// term of the quorum index itself.
func TestAdvanceCommitIndex_Figure8(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.Role = Leader
	rn.persistent.CurrentTerm = 4

	// Leader log: idx1:t1, idx2:t2, idx3:t4 (just appended this term locally).
	// Peers replicated only up to idx2. Quorum median over {3(self),2,2} = 2.
	rn.MatchIndex[2] = 2
	rn.MatchIndex[3] = 2

	// termAt reflects the real log: index 2 is term 2, index 3 is term 4.
	termAt := func(i int) int {
		switch i {
		case 1:
			return 1
		case 2:
			return 2
		case 3:
			return 4
		}
		return 0
	}

	// localLatestIndex = 3. Quorum index = 2, whose term is 2 ≠ currentTerm 4.
	// Commit MUST NOT advance, even though the last entry (idx3) is current-term.
	_, advanced := rn.AdvanceCommitIndex(3, termAt, 4)
	if advanced {
		t.Fatalf("Figure 8 violation: committed a previous-term entry via quorum count")
	}
}

func TestAdvanceCommitIndex_NonLeaderDoesNotAdvance(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.Role = Follower
	rn.MatchIndex[2] = 4
	rn.MatchIndex[3] = 4

	termAt := func(int) int { return 5 }
	_, advanced := rn.AdvanceCommitIndex(4, termAt, 5)
	if advanced {
		t.Fatalf("followers must not advance commit index via quorum")
	}
}

// ---- Election tally ----

func TestRecordVoteAndCheckMajority(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	if _, err := rn.BecomeCandidate(); err != nil { // term 1, votesGranted=1 (self)
		t.Fatalf("BecomeCandidate: %v", err)
	}

	// One more vote → 2 of 3 = majority.
	if won := rn.RecordVoteAndCheckMajority(1); !won {
		t.Fatalf("expected majority after 2nd vote in a 3-node cluster")
	}
}

func TestRecordVoteAndCheckMajority_IgnoresStaleTerm(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	if _, err := rn.BecomeCandidate(); err != nil { // now term 1
		t.Fatalf("BecomeCandidate: %v", err)
	}
	// A vote tagged with a different term must not count.
	if won := rn.RecordVoteAndCheckMajority(99); won {
		t.Fatalf("stale-term vote should not count toward majority")
	}
}

// ---- BecomeLeader guard (dual-leader race, review bug #2) ----

func TestBecomeLeader_PromotesWhenStillCandidate(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	term, err := rn.BecomeCandidate() // term 1, Role=Candidate
	if err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if ok := rn.BecomeLeader(term, 0); !ok {
		t.Fatalf("expected promotion while still candidate at election term")
	}
	if !rn.IsLeader() {
		t.Fatalf("node should be leader after successful BecomeLeader")
	}
}

// TestBecomeLeader_NoOpAfterStepdown reproduces review bug #2: between winning
// the vote and calling BecomeLeader, a higher-term RPC steps the node down to
// follower. BecomeLeader must NOT stomp that stepdown — otherwise we'd have two
// leaders in the higher term (split brain).
func TestBecomeLeader_NoOpAfterStepdown(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	electionTerm, err := rn.BecomeCandidate() // term 1
	if err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}

	// A concurrent higher-term AppendEntries/RequestVote arrives: we step down.
	if err := rn.BecomeFollower(electionTerm+5, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}

	// The stale election result now tries to promote us. It must be rejected.
	if ok := rn.BecomeLeader(electionTerm, 0); ok {
		t.Fatalf("BecomeLeader must no-op after a stepdown to a higher term")
	}
	if rn.IsLeader() {
		t.Fatalf("split brain: promoted to leader after stepping down to a higher term")
	}
	if got := rn.CurrentTerm(); got != electionTerm+5 {
		t.Fatalf("term = %d, want %d (stepdown preserved)", got, electionTerm+5)
	}
}

// ---- BecomeFollower vote preservation (review bug #3) ----

// TestBecomeFollower_SameTermPreservesVote verifies one-vote-per-term: a
// same-term stepdown (e.g. a candidate receiving a valid AppendEntries from the
// term's leader) must NOT erase the vote already cast this term.
func TestBecomeFollower_SameTermPreservesVote(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	// Cast a vote for candidate 2 at term 3.
	rn.HandleRequestVote(RequestVoteRequest{Term: 3, CandidateID: 2}, staticLogState(0, 0))
	if rn.persistent.VotedFor != 2 {
		t.Fatalf("setup: VotedFor = %d, want 2", rn.persistent.VotedFor)
	}

	// Step down to a follower in the SAME term 3 (leaderID 2 asserts authority).
	if err := rn.BecomeFollower(3, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	if rn.persistent.VotedFor != 2 {
		t.Fatalf("same-term stepdown erased the vote: VotedFor = %d, want 2", rn.persistent.VotedFor)
	}
}

// TestBecomeFollower_HigherTermClearsVote verifies that advancing to a NEW term
// does clear the old vote so we can vote again in the new term.
func TestBecomeFollower_HigherTermClearsVote(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.HandleRequestVote(RequestVoteRequest{Term: 3, CandidateID: 2}, staticLogState(0, 0))

	if err := rn.BecomeFollower(4, 3); err != nil { // higher term
		t.Fatalf("BecomeFollower: %v", err)
	}
	if rn.persistent.VotedFor != -1 {
		t.Fatalf("higher-term stepdown should clear vote: VotedFor = %d, want -1", rn.persistent.VotedFor)
	}
}

// ---- Persistence round-trip ----

func TestPersistentStateSurvivesReload(t *testing.T) {
	meta := filepath.Join(t.TempDir(), "raft.meta")
	rn, err := NewRaftNode(1, []int{2, 3}, 3, meta, Follower)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	// Cast a vote at term 7.
	if err := rn.BecomeFollower(7, 0); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	rn.HandleRequestVote(RequestVoteRequest{Term: 7, CandidateID: 3}, staticLogState(0, 0))

	// Reload from the same metadata file — term and vote must persist.
	rn2, err := NewRaftNode(1, []int{2, 3}, 3, meta, Follower)
	if err != nil {
		t.Fatalf("reload NewRaftNode: %v", err)
	}
	if rn2.persistent.CurrentTerm != 7 {
		t.Fatalf("reloaded term = %d, want 7", rn2.persistent.CurrentTerm)
	}
	if rn2.persistent.VotedFor != 3 {
		t.Fatalf("reloaded votedFor = %d, want 3", rn2.persistent.VotedFor)
	}
}

// ---- F3: persist before mutate ----
//
// Figure 2's stable-storage rule is only meaningful if the in-memory state never
// runs ahead of the disk. A node that has incremented its term and voted for
// itself in memory, but failed to write that down, will come back after a crash
// believing it never participated in that term — and can vote a second time in
// it. So every one of these transitions must persist FIRST and adopt the new
// state only on success.

// failingStore is a stateStore whose Save always fails, standing in for a full
// disk, a permissions error, or an I/O fault.
type failingStore struct {
	loaded PersistentState
	saves  int
}

func (f *failingStore) Load() (PersistentState, bool, error) { return f.loaded, true, nil }

func (f *failingStore) Save(PersistentState) error {
	f.saves++
	return errors.New("simulated persist failure")
}

// newFailingNode returns a node whose persistence is broken, seeded with a known
// starting state.
func newFailingNode(t *testing.T, seed PersistentState) (*RaftNode, *failingStore) {
	t.Helper()
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	fs := &failingStore{loaded: seed}
	rn.persistent = seed
	rn.store = fs
	return rn, fs
}

func TestBecomeCandidate_PersistFailureLeavesStateUnchanged(t *testing.T) {
	rn, fs := newFailingNode(t, PersistentState{CurrentTerm: 4, VotedFor: -1})

	term, err := rn.BecomeCandidate()
	if err == nil {
		t.Fatal("BecomeCandidate succeeded despite persist failure, want error")
	}
	if term != 0 {
		t.Fatalf("term = %d, want 0 on failure", term)
	}
	if fs.saves != 1 {
		t.Fatalf("Save called %d times, want 1", fs.saves)
	}

	// The node must not believe it is a candidate in term 5 having voted for
	// itself — none of that reached disk.
	if rn.persistent.CurrentTerm != 4 {
		t.Fatalf("CurrentTerm = %d, want 4 (unchanged)", rn.persistent.CurrentTerm)
	}
	if rn.persistent.VotedFor != -1 {
		t.Fatalf("VotedFor = %d, want -1 (unchanged)", rn.persistent.VotedFor)
	}
	if rn.Role != Follower {
		t.Fatalf("Role = %s, want Follower (unchanged)", rn.Role)
	}
}

func TestHandleRequestVote_PersistFailureDoesNotRecordVote(t *testing.T) {
	rn, _ := newFailingNode(t, PersistentState{CurrentTerm: 4, VotedFor: -1})

	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 9, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	}, staticLogState(0, 0))

	if resp.VoteGranted {
		t.Fatal("vote granted despite persist failure")
	}
	// We must also not advertise the higher term we could not record: the
	// candidate would treat that as proof we stepped up.
	if resp.Term != 4 {
		t.Fatalf("resp.Term = %d, want 4 (our durable term)", resp.Term)
	}
	if rn.persistent.CurrentTerm != 4 {
		t.Fatalf("CurrentTerm = %d, want 4 (unchanged)", rn.persistent.CurrentTerm)
	}
	if rn.persistent.VotedFor != -1 {
		t.Fatalf("VotedFor = %d, want -1 (no vote recorded)", rn.persistent.VotedFor)
	}
}

func TestBecomeFollower_PersistFailureLeavesTermUnchanged(t *testing.T) {
	rn, _ := newFailingNode(t, PersistentState{CurrentTerm: 4, VotedFor: 3})
	rn.Role = Leader

	if err := rn.BecomeFollower(9, 2); err == nil {
		t.Fatal("BecomeFollower succeeded despite persist failure, want error")
	}

	if rn.persistent.CurrentTerm != 4 {
		t.Fatalf("CurrentTerm = %d, want 4 (unchanged)", rn.persistent.CurrentTerm)
	}
	if rn.persistent.VotedFor != 3 {
		t.Fatalf("VotedFor = %d, want 3 (unchanged)", rn.persistent.VotedFor)
	}
}

// Stepping down at the SAME term changes no persistent state, so it must not be
// blocked by a broken disk — a leader that learns of a same-term leader has to
// be able to yield regardless.
func TestBecomeFollower_SameTermSucceedsWithoutPersist(t *testing.T) {
	rn, fs := newFailingNode(t, PersistentState{CurrentTerm: 4, VotedFor: 3})
	rn.Role = Candidate

	if err := rn.BecomeFollower(4, 2); err != nil {
		t.Fatalf("BecomeFollower at same term: %v", err)
	}
	if fs.saves != 0 {
		t.Fatalf("Save called %d times, want 0 (nothing changed)", fs.saves)
	}
	if rn.Role != Follower {
		t.Fatalf("Role = %s, want Follower", rn.Role)
	}
	if rn.LeaderID != 2 {
		t.Fatalf("LeaderID = %d, want 2", rn.LeaderID)
	}
	if rn.persistent.VotedFor != 3 {
		t.Fatalf("VotedFor = %d, want 3 preserved", rn.persistent.VotedFor)
	}
}

// ---- F13: §6 leader stickiness ----

// §6, final paragraph: "if a server receives a RequestVote RPC within the
// minimum election timeout of hearing from a current leader, it does not update
// its term or grant its vote."
//
// Both halves matter. Refusing the vote is the obvious part; refusing to adopt
// the candidate's term is what actually protects the leader, because a term
// bump is how a disruptive candidate deposes one.
func TestHandleRequestVote_IgnoresCandidateWhileLeaderIsAlive(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.SetMinElectionTimeout(300 * time.Millisecond)

	// A leader has just been heard from.
	if err := rn.BecomeFollower(5, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	rn.noteContact()

	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 9, CandidateID: 3, LastLogIndex: 100, LastLogTerm: 9,
	}, staticLogState(0, 0))

	if resp.VoteGranted {
		t.Fatal("granted a vote while a leader was known to be alive")
	}
	if resp.Term != 5 {
		t.Fatalf("resp.Term = %d, want 5 — our own term, not the candidate's", resp.Term)
	}
	if rn.persistent.CurrentTerm != 5 {
		t.Fatalf("CurrentTerm = %d, want 5 — adopting the candidate's term is the "+
			"disruption the check exists to prevent", rn.persistent.CurrentTerm)
	}
}

// Once the window lapses, a real election must be able to proceed.
func TestHandleRequestVote_GrantsAfterStickinessWindowLapses(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.SetMinElectionTimeout(20 * time.Millisecond)

	if err := rn.BecomeFollower(5, 2); err != nil {
		t.Fatalf("BecomeFollower: %v", err)
	}
	rn.noteContact()
	time.Sleep(40 * time.Millisecond) // leader has gone quiet

	resp := rn.HandleRequestVote(RequestVoteRequest{
		Term: 6, CandidateID: 3, LastLogIndex: 0, LastLogTerm: 0,
	}, staticLogState(0, 0))

	if !resp.VoteGranted {
		t.Fatal("refused a vote after the stickiness window lapsed — a dead " +
			"leader would be shielded forever")
	}
}

// The window protects a LEADER, not a recent vote. A node that has only granted
// a vote knows of no leader and must stay free to vote again in a later term,
// or a split vote could never be resolved.
func TestHandleRequestVote_StickinessDoesNotApplyWithoutAKnownLeader(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rn.SetMinElectionTimeout(time.Second)

	// Grant a vote — this refreshes lastContact but leaves LeaderID unset.
	first := rn.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 2},
		staticLogState(0, 0))
	if !first.VoteGranted {
		t.Fatal("first vote was not granted")
	}

	// A later term must still be considered, immediately.
	second := rn.HandleRequestVote(RequestVoteRequest{Term: 6, CandidateID: 3},
		staticLogState(0, 0))
	if !second.VoteGranted {
		t.Fatal("refused a higher-term vote with no leader known — stickiness " +
			"must key on a live leader, not on any recent contact")
	}
}

package raft

// RaftNode owns the consensus state of a single cluster member.
//
// Lock discipline (critical — prevents deadlocks):
//   - RaftNode.mu protects all fields on RaftNode itself.
//   - RaftLog.mu protects all fields on RaftLog.
//   - These two locks are NEVER held simultaneously by the same goroutine.
//   - Any method that needs data from both acquires one, reads what it needs,
//     releases it, then acquires the other.
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
)

// RaftNode holds the consensus state for one node.
type RaftNode struct {
	mu sync.RWMutex

	// Identity
	nodeID  int
	peerIDs []int

	// Volatile state (reset on restart — safe per Raft spec)
	Role        NodeRole
	LeaderID    int
	CommitIndex int
	LastApplied int
	MatchIndex  map[int]int // peerID → highest log index confirmed replicated

	// Persistent state (must survive restarts)
	persistent     PersistentState
	metadataPath   string
}

// NewRaftNode creates a node, loading persistent state from disk if it exists.
func NewRaftNode(nodeID int, peerIDs []int, metadataPath string, initialRole NodeRole) (*RaftNode, error) {
	rn := &RaftNode{
		nodeID:       nodeID,
		peerIDs:      peerIDs,
		Role:         initialRole,
		LeaderID:     1, // bootstrap assumption; overwritten by elections
		CommitIndex:  0,
		LastApplied:  0,
		MatchIndex:   make(map[int]int),
		metadataPath: metadataPath,
		persistent: PersistentState{
			CurrentTerm: 1,
			VotedFor:    -1,
		},
	}

	for _, pid := range peerIDs {
		rn.MatchIndex[pid] = 0
	}

	if err := rn.loadPersistentState(); err != nil {
		return nil, err
	}

	return rn, nil
}

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

// ---- Mutation methods ----

// UpdatePeerProgress records that peerID has replicated up to matchIndex.
// Called by the replicator after a successful append response.
func (rn *RaftNode) UpdatePeerProgress(peerID, matchIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if matchIndex > rn.MatchIndex[peerID] {
		rn.MatchIndex[peerID] = matchIndex
		log.Printf("[RAFT] Peer %d matchIndex → %d", peerID, matchIndex)
	}
}

// AdvanceCommitIndex checks whether a new commit index can be established
// based on quorum agreement. It reads the local latest index from the caller
// (to avoid holding both locks) and returns the new commit index if advanced,
// or 0 if not.
//
// Raft safety rule: a leader may only commit an entry from the current term.
// Entries from previous terms are committed implicitly when a current-term
// entry is committed (§5.4.2).
func (rn *RaftNode) AdvanceCommitIndex(localLatestIndex, localLatestTerm, currentTerm int) (newCommitIndex int, advanced bool) {
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

	// Only commit if quorumIndex is in the current term (safety invariant).
	if quorumIndex > rn.CommitIndex && localLatestTerm == currentTerm {
		rn.CommitIndex = quorumIndex
		log.Printf("[RAFT] CommitIndex advanced to %d", rn.CommitIndex)
		return rn.CommitIndex, true
	}

	return rn.CommitIndex, false
}

// SetFollowerCommitIndex is called on followers when the leader's
// heartbeat/append carries a leaderCommit value.
func (rn *RaftNode) SetFollowerCommitIndex(leaderCommit, latestLogIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

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

// BecomeFollower transitions the node to follower state, updating term
// and persisting. Called when a higher term is observed in any RPC.
func (rn *RaftNode) BecomeFollower(term, leaderID int) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.Role = Follower
	rn.LeaderID = leaderID
	rn.persistent.CurrentTerm = term
	rn.persistent.VotedFor = -1

	return rn.savePersistentStateLocked()
}

// BecomeLeader transitions the node to leader (called after winning election).
func (rn *RaftNode) BecomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.Role = Leader
	rn.LeaderID = rn.nodeID
	// Reset peer progress; leader re-probes from its own log tail.
	for pid := range rn.MatchIndex {
		rn.MatchIndex[pid] = 0
	}
	log.Printf("[RAFT] Node %d became Leader for term %d", rn.nodeID, rn.persistent.CurrentTerm)
}

// ---- Persistent state management ----

func (rn *RaftNode) loadPersistentState() error {
	data, err := os.ReadFile(rn.metadataPath)
	if os.IsNotExist(err) {
		// First boot: defaults already set in constructor.
		return rn.savePersistentState()
	}
	if err != nil {
		return fmt.Errorf("read raft metadata: %w", err)
	}

	var ps PersistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("parse raft metadata: %w", err)
	}

	rn.persistent = ps
	log.Printf("[RAFT] Loaded persistent state: term=%d votedFor=%d", ps.CurrentTerm, ps.VotedFor)
	return nil
}

func (rn *RaftNode) savePersistentState() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.savePersistentStateLocked()
}

// savePersistentStateLocked writes term+votedFor to disk.
// Caller must hold rn.mu (write).
func (rn *RaftNode) savePersistentStateLocked() error {
	data, err := json.Marshal(rn.persistent)
	if err != nil {
		return err
	}
	// Write to temp file then rename for atomic update.
	tmp := rn.metadataPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0666); err != nil {
		return fmt.Errorf("write raft metadata tmp: %w", err)
	}
	return os.Rename(tmp, rn.metadataPath)
}

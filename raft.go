package main

import "sync"

type NodeRole string

const (
	Leader    NodeRole = "Leader"
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
)

type RaftNode struct {
	mu          sync.RWMutex
	Role        NodeRole
	CurrentTerm int
	LeaderID    int

	// Production tracking indices
	CommitIndex int         // Highest log index known to be committed
	MatchIndex  map[int]int // For each peer, the highest log index known to be replicated
}

func NewRaftNode(initialRole NodeRole, peerIDs []int) *RaftNode {
	matches := make(map[int]int)
	for _, pid := range peerIDs {
		matches[pid] = 0
	}

	return &RaftNode{
		Role:        initialRole,
		CurrentTerm: 1,
		LeaderID:    1,
		CommitIndex: 0,
		MatchIndex:  matches,
	}
}

// IsLeader checks if this node is currently authorized to accept writes
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role == Leader
}

// UpdatePeerProgress records how far a follower has come
func (rn *RaftNode) UpdatePeerProgress(peerID int, index int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if index > rn.MatchIndex[peerID] {
		rn.MatchIndex[peerID] = index
	}
}

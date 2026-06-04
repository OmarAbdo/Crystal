package main

import (
	"sync"
)

// NodeRole represents the current consensus state of the node
type NodeRole string

const (
	Leader    NodeRole = "Leader"
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
)

type RaftNode struct {
	mu        sync.RWMutex
	Role      NodeRole
	CurrentTerm int
	LeaderID  int // Tracks who the current active leader is
}

func NewRaftNode(initialRole NodeRole) *RaftNode {
	return &RaftNode{
		Role:        initialRole,
		CurrentTerm: 1,
		LeaderID:    1, // Let's hardcode Node 1 as the initial leader for now
	}
}

// IsLeader checks if this node is currently authorized to accept writes
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role == Leader
}
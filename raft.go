package main

// This handles non-blocking status adjustments, consensus term evaluations, and centralizes core state data structures.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

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
	CommitIndex int
	LastApplied int
	MatchIndex  map[int]int
	peerIDs     []int
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
		LastApplied: 0,
		MatchIndex:  matches,
		peerIDs:     peerIDs,
	}
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role == Leader
}

func (rn *RaftNode) GetState() (NodeRole, int) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.Role, rn.CurrentTerm
}

func (rn *RaftNode) UpdatePeerProgress(peerID int, index int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if index > rn.MatchIndex[peerID] {
		rn.MatchIndex[peerID] = index
		log.Printf("[RAFT] Peer %d MatchIndex updated to %d", peerID, index)
	}
}

func (rn *RaftNode) CheckQuorumAndCommit(store *CrystalStore) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.Role != Leader {
		return false
	}

	store.mu.RLock()
	localNext := store.nextIndex - 1
	store.mu.RUnlock()

	indices := make([]int, 0, len(rn.peerIDs)+1)
	indices = append(indices, localNext)
	for _, idx := range rn.MatchIndex {
		indices = append(indices, idx)
	}

	sort.Ints(indices)
	quorumIdx := indices[(len(indices)-1)/2]

	if quorumIdx > rn.CommitIndex {
		if entry, ok := store.GetEntry(quorumIdx); ok && entry.Term == rn.CurrentTerm {
			rn.CommitIndex = quorumIdx
			log.Printf("[RAFT] CommitIndex advanced via replication quorum to: %d", rn.CommitIndex)
			return true
		}
	}
	return false
}

func (rn *RaftNode) ReplicateToPeer(peerID int, addr string, entry LogEntry) {
	client := &http.Client{Timeout: 1 * time.Second}
	jsonData, _ := json.Marshal(entry)
	url := fmt.Sprintf("http://%s/internal/append", addr)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[NETWORK] Failed sending append to peer %d at %s: %v", peerID, addr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		rn.UpdatePeerProgress(peerID, entry.Index)
	}
}

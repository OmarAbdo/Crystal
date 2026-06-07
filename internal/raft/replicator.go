package raft

// Replicator handles outbound replication RPCs to peer nodes.
// It is stateless: all state lives in RaftNode and RaftLog.
// Each call to ReplicateEntry fires a single HTTP POST and
// notifies the node of the result via UpdatePeerProgress.
//
// Future: replace HTTP with a persistent gRPC stream per peer,
// add retry with backoff, implement InstallSnapshot RPC for
// catching up peers that have fallen too far behind the log.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

const replicationTimeout = 1 * time.Second

// Replicator sends log entries to peer nodes.
type Replicator struct {
	client *http.Client
}

// NewReplicator creates a Replicator with a bounded HTTP client.
func NewReplicator() *Replicator {
	return &Replicator{
		client: &http.Client{Timeout: replicationTimeout},
	}
}

// ReplicateEntry sends a single LogEntry to the peer at addr and
// updates the RaftNode's MatchIndex on success.
func (r *Replicator) ReplicateEntry(node *RaftNode, peerID int, addr string, entry LogEntry) {
	_, term := node.State()

	node.mu.RLock()
	commitIndex := node.CommitIndex
	node.mu.RUnlock()

	req := AppendEntryRequest{
		LeaderID:     node.NodeID(),
		LeaderTerm:   term,
		LeaderCommit: commitIndex,
		Entry:        entry,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		log.Printf("[REPLICATOR] Failed to marshal entry for peer %d: %v", peerID, err)
		return
	}

	url := fmt.Sprintf("http://%s/internal/append", addr)
	resp, err := r.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("[REPLICATOR] Peer %d (%s) unreachable: %v", peerID, addr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[REPLICATOR] Peer %d rejected append: HTTP %d", peerID, resp.StatusCode)
		return
	}

	var result AppendEntryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[REPLICATOR] Peer %d returned unparseable response: %v", peerID, err)
		return
	}

	if result.Success {
		node.UpdatePeerProgress(peerID, result.MatchIndex)
	}
}

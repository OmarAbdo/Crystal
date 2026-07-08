package raft

// Replicator handles outbound AppendEntries RPCs to peer nodes.
// It is stateless: all state lives in RaftNode and RaftLog.
// Each call to ReplicateTo fires a single HTTP POST carrying the entries
// from the peer's nextIndex onward, then advances matchIndex/nextIndex on
// success or backtracks nextIndex on a log-inconsistency rejection.
//
// Future: replace HTTP with a persistent gRPC stream per peer,
// add retry with backoff, implement InstallSnapshot RPC for
// catching up peers that have fallen too far behind the log.

import (
	"bytes"
	"encoding/json"
	"fmt"
	stdlog "log"
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

// ReplicateTo sends one AppendEntries RPC to the peer at addr, carrying every
// entry from the peer's nextIndex onward (or none, for a pure heartbeat). It
// implements the leader half of §5.3:
//
//   - Build prevLogIndex/prevLogTerm from the entry just before nextIndex.
//   - On success: advance the peer's matchIndex/nextIndex.
//   - On rejection due to log inconsistency: backtrack nextIndex using the
//     follower's conflict hint, so the next call re-probes further back.
//   - On a higher term in the reply: the peer has seen a newer term; the leader
//     must step down. We surface that to the caller via the returned term so
//     the engine/election layer can call BecomeFollower.
//
// Returns the follower's reported term (0 if the peer was unreachable) so the
// caller can detect a stale-leader situation.
func (r *Replicator) ReplicateTo(node *RaftNode, log *RaftLog, peerID int, addr string) int {
	_, term := node.State()

	node.mu.RLock()
	commitIndex := node.CommitIndex
	node.mu.RUnlock()

	nextIndex := node.NextIndexFor(peerID)
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = log.TermAt(prevLogIndex)
	}

	entries := log.GetEntriesFrom(nextIndex)

	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     node.NodeID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	result, ok := r.send(peerID, addr, req)
	if !ok {
		return 0
	}

	// The follower has moved to a higher term: we are a stale leader.
	if result.Term > term {
		return result.Term
	}

	if result.Success {
		node.UpdatePeerProgress(peerID, result.MatchIndex)
	} else {
		// Log inconsistency: back up and retry on the next replication tick.
		node.BacktrackNextIndex(peerID, result.ConflictIndex)
	}

	return result.Term
}

// send marshals and POSTs a single AppendEntries request, returning the decoded
// response. ok is false if the peer was unreachable or returned a bad response.
func (r *Replicator) send(peerID int, addr string, req AppendEntriesRequest) (AppendEntriesResponse, bool) {
	payload, err := json.Marshal(req)
	if err != nil {
		stdlog.Printf("[REPLICATOR] Failed to marshal request for peer %d: %v", peerID, err)
		return AppendEntriesResponse{}, false
	}

	url := fmt.Sprintf("http://%s/internal/append", addr)
	resp, err := r.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) unreachable: %v", peerID, addr, err)
		return AppendEntriesResponse{}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		stdlog.Printf("[REPLICATOR] Peer %d rejected append: HTTP %d", peerID, resp.StatusCode)
		return AppendEntriesResponse{}, false
	}

	var result AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d returned unparseable response: %v", peerID, err)
		return AppendEntriesResponse{}, false
	}

	return result, true
}

// RequestVoteFrom sends a single RequestVote RPC to the peer at addr and returns
// the decoded response. ok is false if the peer was unreachable or replied
// badly. The engine tallies votes and detects term-stepdown from the responses.
func (r *Replicator) RequestVoteFrom(peerID int, addr string, req RequestVoteRequest) (RequestVoteResponse, bool) {
	payload, err := json.Marshal(req)
	if err != nil {
		stdlog.Printf("[REPLICATOR] Failed to marshal vote request for peer %d: %v", peerID, err)
		return RequestVoteResponse{}, false
	}

	url := fmt.Sprintf("http://%s/internal/vote", addr)
	resp, err := r.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) unreachable for vote: %v", peerID, addr, err)
		return RequestVoteResponse{}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		stdlog.Printf("[REPLICATOR] Peer %d rejected vote request: HTTP %d", peerID, resp.StatusCode)
		return RequestVoteResponse{}, false
	}

	var result RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d returned unparseable vote response: %v", peerID, err)
		return RequestVoteResponse{}, false
	}

	return result, true
}

// InstallSnapshotTo ships a full snapshot to a follower that has fallen behind
// the leader's compacted log. On success it advances the peer's match/nextIndex
// to the snapshot boundary (the peer's log now matches through
// LastIncludedIndex). Returns the follower's term so the engine can detect a
// stale-leader stepdown (0 if unreachable).
func (r *Replicator) InstallSnapshotTo(node *RaftNode, peerID int, addr string, req InstallSnapshotRequest) int {
	_, term := node.State()

	payload, err := json.Marshal(req)
	if err != nil {
		stdlog.Printf("[REPLICATOR] Failed to marshal snapshot for peer %d: %v", peerID, err)
		return 0
	}

	url := fmt.Sprintf("http://%s/internal/snapshot", addr)
	resp, err := r.client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) unreachable for snapshot: %v", peerID, addr, err)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		stdlog.Printf("[REPLICATOR] Peer %d rejected snapshot: HTTP %d", peerID, resp.StatusCode)
		return 0
	}

	var result InstallSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d returned unparseable snapshot response: %v", peerID, err)
		return 0
	}

	if result.Term > term {
		return result.Term // stale leader
	}

	// The follower is now caught up through the snapshot boundary.
	node.UpdatePeerProgress(peerID, req.LastIncludedIndex)
	return result.Term
}

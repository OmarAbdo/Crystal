package raft

// Replicator is the leader's outbound half of §5.3. It is stateless: all state
// lives in RaftNode and RaftLog, and every call is one RPC round to one peer.
//
// It owns no goroutines and no scheduling. When and how often to replicate is
// the engine's decision (one long-lived goroutine per peer); what to send and
// how to interpret the answer is this file's.
//
// All network access goes through a Transport, so a test can substitute a fake
// network and provoke the partitions that make Raft interesting.

import (
	stdlog "log"
)

// Replicator sends log entries and snapshots to peer nodes.
type Replicator struct {
	transport Transport
}

// NewReplicator creates a Replicator that sends over the given transport.
func NewReplicator(transport Transport) *Replicator {
	return &Replicator{transport: transport}
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

	result, err := r.transport.AppendEntries(addr, AppendEntriesRequest{
		Term:         term,
		LeaderID:     node.NodeID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) append failed: %v", peerID, addr, err)
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

// RequestVoteFrom sends a single RequestVote RPC to the peer at addr and returns
// the decoded response. ok is false if the peer was unreachable or replied
// badly. The engine tallies votes and detects term-stepdown from the responses.
func (r *Replicator) RequestVoteFrom(peerID int, addr string, req RequestVoteRequest) (RequestVoteResponse, bool) {
	resp, err := r.transport.RequestVote(addr, req)
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) vote request failed: %v", peerID, addr, err)
		return RequestVoteResponse{}, false
	}
	return resp, true
}

// InstallSnapshotTo ships a full snapshot to a follower that has fallen behind
// the leader's compacted log. On success it advances the peer's match/nextIndex
// to the snapshot boundary (the peer's log now matches through
// LastIncludedIndex). Returns the follower's term so the engine can detect a
// stale-leader stepdown (0 if unreachable).
func (r *Replicator) InstallSnapshotTo(node *RaftNode, peerID int, addr string, req InstallSnapshotRequest) int {
	_, term := node.State()

	result, err := r.transport.InstallSnapshot(addr, req)
	if err != nil {
		stdlog.Printf("[REPLICATOR] Peer %d (%s) snapshot failed: %v", peerID, addr, err)
		return 0
	}

	if result.Term > term {
		return result.Term // stale leader
	}

	// The follower is now caught up through the snapshot boundary.
	node.UpdatePeerProgress(peerID, req.LastIncludedIndex)
	return result.Term
}

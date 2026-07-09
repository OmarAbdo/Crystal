package engine

// peerReplicator is a long-lived goroutine that owns all outbound replication
// to a single peer while this node is leader. It loops:
//
//	wait for (a notify signal OR the heartbeat interval OR stop)
//	→ send one AppendEntries/InstallSnapshot round to the peer
//	→ if the peer reported a higher term, tell the control loop to step down
//
// Because each peer has its own goroutine, a slow or unreachable peer only
// delays its own loop — it cannot stall heartbeats to healthy peers, block
// client proposals, or hold up the control loop. The replicator mutates only the
// node's already-locked progress fields (via Replicator.ReplicateTo /
// InstallSnapshotTo → UpdatePeerProgress / BacktrackNextIndex); it never touches
// CommitIndex/LastApplied, preserving the control loop's single-writer role.

import (
	"log"
	"sync"
	"time"
)

type peerReplicator struct {
	engine *Engine
	peerID int
	addr   string
	term   int // the leader term this replicator was started for

	notify chan struct{} // buffered depth 1: coalesced "new work" nudge
	stop   chan struct{} // closed to terminate the goroutine
}

// run is the replication loop. It exits when stop is closed.
func (pr *peerReplicator) run(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	// Send an immediate first round so a new leader asserts authority (and ships
	// its no-op) without waiting a full heartbeat interval.
	pr.replicateOnce()

	for {
		select {
		case <-pr.stop:
			return
		case <-pr.notify:
			pr.replicateOnce()
		case <-ticker.C:
			pr.replicateOnce()
		}
	}
}

// replicateOnce performs a single replication round to the peer, choosing
// AppendEntries or InstallSnapshot based on whether the peer's nextIndex has
// fallen below the leader's compacted boundary. A higher follower term is
// forwarded to the control loop for stepdown.
func (pr *peerReplicator) replicateOnce() {
	e := pr.engine

	// Stop touching the network the moment we are no longer leader; the control
	// loop will tear us down, but avoid a spurious final RPC.
	if !e.node.IsLeader() {
		return
	}

	nextIndex := e.node.NextIndexFor(pr.peerID)

	var followerTerm int
	if nextIndex < e.raftLog.FirstIndex() {
		req, ok := e.buildSnapshotRequest()
		if !ok {
			// No snapshot on disk yet — fall back to a normal append attempt.
			followerTerm = e.replicator.ReplicateTo(e.node, e.raftLog, pr.peerID, pr.addr)
		} else {
			followerTerm = e.replicator.InstallSnapshotTo(e.node, pr.peerID, pr.addr, req)
		}
	} else {
		followerTerm = e.replicator.ReplicateTo(e.node, e.raftLog, pr.peerID, pr.addr)
	}

	if followerTerm > pr.term {
		log.Printf("[REPLICATOR] Peer %d reported higher term %d > %d", pr.peerID, followerTerm, pr.term)
		e.reportHigherTerm(followerTerm)
	}
}

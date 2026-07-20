package main

// main.go is a composition root — it wires subsystems together and starts them.
// No business logic lives here. If you find yourself adding domain logic here,
// it belongs in one of the internal packages instead.

import (
	"log"
	"net/http"
	"os"

	"crystal/internal/config"
	"crystal/internal/engine"
	"crystal/internal/raft"
	"crystal/internal/store"
	"crystal/internal/transport"
)

func main() {
	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Cannot create data directory %q: %v", cfg.DataDir, err)
	}

	// ---- Build the Raft log (WAL) ----
	raftLog, err := raft.NewRaftLog(cfg.WALPath())
	if err != nil {
		log.Fatalf("Cannot open WAL at %s: %v", cfg.WALPath(), err)
	}
	defer raftLog.Close()
	raftLog.SetCompactionThreshold(cfg.CompactionThreshold)

	// ---- Build the state machine ----
	stateMachine := store.NewMemoryStateMachine()
	snapshots := store.NewSnapshotManager(cfg.SnapshotPath())

	// ---- Build the Raft node ----
	var peerIDs []int
	for pid := range cfg.Peers {
		peerIDs = append(peerIDs, pid)
	}

	// All nodes start as followers. A randomized election timeout will elect a
	// leader (§5.2) — there is no hardcoded bootstrap leader anymore.
	clusterSize := len(cfg.Peers) + 1
	node, err := raft.NewRaftNode(cfg.NodeID, peerIDs, clusterSize, cfg.MetadataPath(), raft.Follower)
	if err != nil {
		log.Fatalf("Cannot initialize Raft node: %v", err)
	}

	// Restore from snapshot if one exists: load state, seed the log's compaction
	// offset, and seed the node's commit/applied boundaries so the apply loop
	// does not walk through already-compacted indices.
	if err := restoreFromSnapshot(snapshots, stateMachine, raftLog, node); err != nil {
		log.Fatalf("Snapshot restore failed: %v", err)
	}

	// ---- Build and start the engine ----
	eng := engine.New(cfg, node, raftLog, stateMachine, snapshots)
	done := make(chan struct{})
	defer close(done)
	go eng.Run(done)

	// ---- Build and start the HTTP server ----
	// The engine is the inbound RPC facade: it already owns the node, log, state
	// machine and snapshot store, so the HTTP layer never has to know about them.
	srv := transport.NewServer(node, eng.ProposalQueue(), eng.ReadQueue(), stateMachine, eng, cfg.Peers)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	log.Printf("[CRYSTALDB] Node %d listening on %s (role: %s, data: %s)",
		cfg.NodeID, cfg.Port, raft.Follower, cfg.DataDir)

	if err := http.ListenAndServe(cfg.Port, mux); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// restoreFromSnapshot loads the latest snapshot (if any) into the state machine
// and re-establishes the compaction boundary. The standard Raft startup recovery
// sequence: apply the snapshot to the state machine, tell the log where the log
// now starts (so post-snapshot WAL entries stay addressable), and seed the
// node's commit/applied boundaries to the snapshot's last included index.
func restoreFromSnapshot(
	snapshots *store.SnapshotManager,
	sm store.StateMachine,
	raftLog *raft.RaftLog,
	node *raft.RaftNode,
) error {
	snap, err := snapshots.Read()
	if err != nil {
		return err
	}
	if snap == nil {
		// No snapshot: replay the full WAL from entry 1. Nothing to do here
		// because RaftLog.recover() already loaded the cache, and the engine
		// will apply entries as they are committed.
		log.Printf("[STARTUP] No snapshot found; will replay full WAL")
		return nil
	}

	log.Printf("[STARTUP] Restoring snapshot at index %d term %d",
		snap.Meta.LastIncludedIndex, snap.Meta.LastIncludedTerm)

	// Encode the snapshot state back to bytes for the generic Restore interface.
	raw, err := snap.EncodeState()
	if err != nil {
		return err
	}
	if err := sm.Restore(raw); err != nil {
		return err
	}

	// Re-establish the compaction offset and commit/applied boundaries so the
	// index math and apply loop are consistent with the compacted WAL.
	// RestoreOffset also reconciles a WAL that overlaps the snapshot, which is a
	// legitimate on-disk state: snapshots are persisted before the entries they
	// cover are discarded, so a crash in that window leaves both.
	if err := raftLog.RestoreOffset(snap.Meta.LastIncludedIndex, snap.Meta.LastIncludedTerm); err != nil {
		return err
	}
	node.SeedFromSnapshot(snap.Meta.LastIncludedIndex)

	return nil
}
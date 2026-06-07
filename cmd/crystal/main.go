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

	// ---- Build the state machine ----
	stateMachine := store.NewMemoryStateMachine()
	snapshots := store.NewSnapshotManager(cfg.SnapshotPath())

	// Restore from snapshot if one exists, then replay any WAL tail.
	if err := restoreFromSnapshot(snapshots, stateMachine, raftLog); err != nil {
		log.Fatalf("Snapshot restore failed: %v", err)
	}

	// ---- Build the Raft node ----
	var peerIDs []int
	for pid := range cfg.Peers {
		peerIDs = append(peerIDs, pid)
	}

	// Bootstrap: node 1 starts as leader. Elections will replace this.
	initialRole := raft.Follower
	if cfg.NodeID == 1 {
		initialRole = raft.Leader
	}

	node, err := raft.NewRaftNode(cfg.NodeID, peerIDs, cfg.MetadataPath(), initialRole)
	if err != nil {
		log.Fatalf("Cannot initialize Raft node: %v", err)
	}

	// ---- Build and start the engine ----
	eng := engine.New(cfg, node, raftLog, stateMachine, snapshots)
	done := make(chan struct{})
	defer close(done)
	go eng.Run(done)

	// ---- Build and start the HTTP server ----
	srv := transport.NewServer(node, eng.ProposalQueue(), stateMachine, raftLog)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	log.Printf("[CRYSTALDB] Node %d listening on %s (role: %s, data: %s)",
		cfg.NodeID, cfg.Port, initialRole, cfg.DataDir)

	if err := http.ListenAndServe(cfg.Port, mux); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// restoreFromSnapshot loads the latest snapshot (if any) into the state machine,
// then replays WAL entries that follow the snapshot's last included index.
// This is the standard Raft startup recovery sequence.
func restoreFromSnapshot(
	snapshots *store.SnapshotManager,
	sm store.StateMachine,
	raftLog *raft.RaftLog,
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

	return sm.Restore(raw)
}
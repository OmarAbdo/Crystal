package main

import (
	"log"
	"net/http"
)

func main() {
	cfg := ParseFlags()
	store := NewCrystalStore()

	// Initialize Raft State
	// Default to Follower, but if ID is 1, make it the initial Leader
	initialRole := Follower
	if cfg.NodeID == 1 {
		initialRole = Leader
	}
	raft := NewRaftNode(initialRole)

	// Pass both store and raft state down to our endpoints
	http.HandleFunc("/set", HandleSet(store, raft, cfg))
	http.HandleFunc("/get", HandleGet(store))
	http.HandleFunc("/internal/append", HandleInternalAppend(store))

	log.Printf("[MAIN] Crystal Node %d starting as %s on port %s...", cfg.NodeID, initialRole, cfg.Port)
	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

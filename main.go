package main

import (
	"log"
	"net/http"
)

func main() {
	// 1. Parse who we are and who our neighbors are
	cfg := ParseFlags()

	store := NewCrystalStore()

	http.HandleFunc("/set", HandleSet(store))
	http.HandleFunc("/get", HandleGet(store))

	// 2. Start our server using the dynamic port from our config flags
	log.Printf("[MAIN] Crystal Node %d starting on port %s...", cfg.NodeID, cfg.Port)
	log.Printf("[MAIN] Tracked peers: %v", cfg.Peers)

	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

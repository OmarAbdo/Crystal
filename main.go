package main

import (
	"log"
	"net/http"
)

func main() {
	store := NewCrystalStore()
	port := ":8080"

	// Registering handlers by passing our store instance down
	http.HandleFunc("/set", HandleSet(store))
	http.HandleFunc("/get", HandleGet(store))

	log.Printf("[MAIN] Crystal node starting on port %s...", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

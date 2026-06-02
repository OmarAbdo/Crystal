package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// CrystalStore holds our in-memory key-value data safely using an RWMutex
type CrystalStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewCrystalStore initializes our database map
func NewCrystalStore() *CrystalStore {
	return &CrystalStore{
		data: make(map[string]string),
	}
}

// Put inserts or updates a key safely
func (cs *CrystalStore) Put(key, value string) {
	cs.mu.Lock()
	defer cs.mu.Unlock() // Using defer ensures we unlock no matter what
	cs.data[key] = value
}

// Get retrieves a key safely, returning the value and whether it exists
func (cs *CrystalStore) Get(key string) (string, bool) {
	cs.mu.RLock() // RLock allows multiple concurrent readers
	defer cs.mu.RUnlock()
	val, ok := cs.data[key]
	return val, ok
}

// KeyValueRequest defines the JSON structure for incoming write requests
type KeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	store := NewCrystalStore()
	port := ":8080"

	// HTTP Handler for writing data
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var req KeyValueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Key == "" {
			http.Error(w, "Key cannot be empty", http.StatusBadRequest)
			return
		}

		store.Put(req.Key, req.Value)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK\n")
	})

	// HTTP Handler for reading data
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key parameter", http.StatusBadRequest)
			return
		}

		val, exists := store.Get(key)
		if !exists {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": val})
	})

	log.Printf("Crystal node starting on port %s...", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type KeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// HandleSet handles writing to the log and state machine
// HandleSet now validates if the current node is the cluster leader before modifying state
func HandleSet(store *CrystalStore, raft *RaftNode, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CRITICAL CHECK: If we are not the leader, reject the client's write!
		if !raft.IsLeader() {
			http.Error(w, fmt.Sprintf("Not the leader. Please send requests to Node %d", raft.LeaderID), http.StatusForbidden)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var req KeyValueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// 1. Log locally
		entry := store.AppendToLog(req.Key, req.Value)
		store.ApplyEntry(entry)

		// 2. Replicate to peers
		go store.ReplicateToPeers(cfg.Peers, entry)

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK\n")
	}
}

// HandleGet handles reading from the state machine
func HandleGet(store *CrystalStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}

// HandleInternalAppend processes replication requests from other cluster nodes
func HandleInternalAppend(store *CrystalStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var entry LogEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Save the entry to our own log and state machine
		store.ApplyEntry(entry)

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "AppendOK\n")
	}
}

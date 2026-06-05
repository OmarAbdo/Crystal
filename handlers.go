package main

// Your handlers are now lean, synchronized conduits.
// They drop user mutations into a proposal channel, forcing execution through a safe queue architecture.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type KeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Proposal struct {
	Key      string
	Value    string
	ResultCh chan bool
}

// Global engine proposal queue to mimic decoupled execution seen in production etcd
var ProposalQueue = make(chan Proposal, 100)

func HandleSet(raft *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !raft.IsLeader() {
			http.Error(w, fmt.Sprintf("Not the leader. Route traffic to Node %d", raft.LeaderID), http.StatusForbidden)
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

		if req.Key == "" {
			http.Error(w, "Key cannot be empty", http.StatusBadRequest)
			return
		}

		resCh := make(chan bool, 1)
		ProposalQueue <- Proposal{
			Key:      req.Key,
			Value:    req.Value,
			ResultCh: resCh,
		}

		select {
		case success := <-resCh:
			if success {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "OK\n")
			} else {
				http.Error(w, "Commit verification failed", http.StatusInternalServerError)
			}
		case <-time.After(2000 * time.Millisecond):
			http.Error(w, "Cluster write timeout: State engine busy or lost quorum", http.StatusServiceUnavailable)
		}
	}
}

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

		// Direct append to follower's disk WAL file
		if err := store.TruncateAndAppendFollower(entry); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "AppendOK\n")
	}
}

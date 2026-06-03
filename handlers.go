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
func HandleSet(store *CrystalStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		entry := store.AppendToLog(req.Key, req.Value)
		store.ApplyEntry(entry)

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
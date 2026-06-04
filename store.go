package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type LogEntry struct {
	Index int    `json:"index"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CrystalStore struct {
	mu        sync.RWMutex
	data      map[string]string
	log       []LogEntry
	nextIndex int
}

func NewCrystalStore() *CrystalStore {
	return &CrystalStore{
		data:      make(map[string]string),
		log:       make([]LogEntry, 0),
		nextIndex: 1,
	}
}

func (cs *CrystalStore) AppendToLog(key, value string) LogEntry {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	entry := LogEntry{
		Index: cs.nextIndex,
		Key:   key,
		Value: value,
	}
	cs.log = append(cs.log, entry)
	cs.nextIndex++

	log.Printf("[STORE] Log Appended: Index %d -> Set %s = %s", entry.Index, entry.Key, entry.Value)
	return entry
}

func (cs *CrystalStore) ApplyEntry(entry LogEntry) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.data[entry.Key] = entry.Value
	log.Printf("[STORE] State Machine Applied: Index %d", entry.Index)
}

func (cs *CrystalStore) Get(key string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	val, ok := cs.data[key]
	return val, ok
}

// ReplicateToPeers forwards a log entry to all tracked peer nodes
func (cs *CrystalStore) ReplicateToPeers(peers map[int]string, entry LogEntry) {
	// Create an HTTP client with a quick timeout so we don't hang forever if a node is dead
	client := &http.Client{Timeout: 2 * time.Second}

	for peerID, peerAddr := range peers {
		// Prepare the JSON payload
		jsonData, err := json.Marshal(entry)
		if err != nil {
			log.Printf("[STORE] Failed to marshal entry for peer %d: %v", peerID, err)
			continue
		}

		// Build the URL to the peer's internal endpoint
		url := fmt.Sprintf("http://%s/internal/append", peerAddr)

		// Send the log entry to the peer
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[STORE] Failed to send log to peer %d at %s: %v", peerID, peerAddr, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.Printf("[STORE] Successfully replicated index %d to peer %d", entry.Index, peerID)
		} else {
			log.Printf("[STORE] Peer %d rejected log index %d with status: %s", peerID, entry.Index, resp.Status)
		}
	}
}

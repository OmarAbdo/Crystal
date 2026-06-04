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
	// Loop through peers, and execute the network's I/O concurrently
	for peerID, peerAddr := range peers {
		// we must pass peerID and peerAddr as arguments into the goroutine
		// to avoid data race issues with the loop variables
		go func(id int, addr string) {
			// which http client you ask? which instance of which we are getting its pointer?
			// so http.Client{Timeout: 2 * time.Second} without the & would create a
			// new http.Client struct value, and then we would take its address to get
			// a pointer to it. This is perfectly valid in Go, and it allows us to create
			// a new http.Client instance with a specific timeout for each goroutine
			//  without sharing the same client across goroutines, which
			// could lead to unintended side effects if the client is modified concurrently.
			// By using &http.Client{Timeout: 2 * time.Second}, we ensure that each goroutine
			// has its own independent http.Client instance with the specified timeout.
			client := &http.Client{Timeout: 2 * time.Second}
			jsonData, err := json.Marshal(entry)
			if err != nil {
				log.Printf("[STORE] Failed to marshal log entry for peer %d: %v", id, err)
				return
			}

			url := fmt.Sprintf("http://%s/internal/append", addr)
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("[STORE] Failed to send log entry to peer %d at %s: %v", id, addr, err)
				return
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				log.Printf("[STORE] Successfully replicated log entry to peer %d at %s", id, addr)
			} else {
				log.Printf("[STORE] Peer %d rejected log index %d: %s", id, entry.Index, resp.Status)
			}

		}(peerID, peerAddr) // executing the anonymous function in a goroutine with the current peerID and peerAddr
	}
}

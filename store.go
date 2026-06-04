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
// ReplicateToPeers now blocks until a quorum of nodes acknowledges the log entry
func (cs *CrystalStore) ReplicateToPeers(peers map[int]string, entry LogEntry) bool {
	// 1. We start with 1 vote (the leader itself automatically counts)
	successes := 1
	targetQuorum := (len(peers)+1)/2 + 1 // Formula for majority

	// A channel to collect successes from concurrent goroutines
	ackChan := make(chan bool, len(peers))

	for peerID, peerAddr := range peers {
		go func(id int, addr string) {
			client := &http.Client{Timeout: 1 * time.Second}
			jsonData, _ := json.Marshal(entry)
			url := fmt.Sprintf("http://%s/internal/append", addr)

			resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				ackChan <- false
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				ackChan <- true
			} else {
				ackChan <- false
			}
		}(peerID, peerAddr)
	}

	// 2. Wait for responses or a global timeout
	timeout := time.After(1500 * time.Millisecond)

	for i := 0; i < len(peers); i++ {
		select {
		case success := <-ackChan:
			if success {
				successes++
			}
			// If we hit quorum early, we can stop waiting and return success!
			if successes >= targetQuorum {
				return true
			}
		case <-timeout:
			log.Printf("[STORE] Quorum timeout waiting for index %d", entry.Index)
			return false
		}
	}

	return successes >= targetQuorum
}

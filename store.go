package main

import (
	"log"
	"sync"
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

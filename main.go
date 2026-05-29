package main

import (
	"sync"
)

// CrystalStore holds our in-memory key-value data safely
type CrystalStore struct {
	mu sync.RWMutex
	data map[string]string
}

// NewCrystalStore initializes our database
func NewCrystalStore() *CrystalStore {
	return &CrystalStore{
		data: make(map[string]string),
	}	
}

// Set adds or updates a key-value pair in the store
func (cs *CrystalStore) Set(key, value string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.data[key] = value
}

// Get retrieves a key, returning the value and whether it exists
func (cs *CrystalStore) Get(key string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	value, exists := cs.data[key]
	return value, exists
}
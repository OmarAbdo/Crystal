package main

// This houses your in-memory placeholder for the future LSM-tree state machine,
// the persistent append-only WAL layer, and the central execution loop.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type LogEntry struct {
	Index int    `json:"index"`
	Term  int    `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CrystalStore struct {
	mu        sync.RWMutex
	data      map[string]string // Future MemTable / LSM-Tree entry point
	logFile   *os.File
	walPath   string
	logCache  []LogEntry
	nextIndex int
}

func NewCrystalStore(walPath string) (*CrystalStore, error) {
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	cs := &CrystalStore{
		data:      make(map[string]string),
		logFile:   file,
		walPath:   walPath,
		logCache:  make([]LogEntry, 0),
		nextIndex: 1,
	}

	if err := cs.recoverWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover WAL: %w", err)
	}

	return cs, nil
}

func (cs *CrystalStore) recoverWAL() error {
	if _, err := cs.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		var length uint32
		if err := binary.Read(cs.logFile, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(cs.logFile, buf); err != nil {
			return err
		}

		var entry LogEntry
		if err := json.Unmarshal(buf, &entry); err != nil {
			return err
		}

		cs.logCache = append(cs.logCache, entry)
		cs.nextIndex = entry.Index + 1
	}
	log.Printf("[WAL] Recovered %d log entries from disk. NextIndex: %d", len(cs.logCache), cs.nextIndex)
	return nil
}

func (cs *CrystalStore) AppendToWAL(key, value string, term int) (LogEntry, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	entry := LogEntry{
		Index: cs.nextIndex,
		Term:  term,
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return LogEntry{}, err
	}

	// Write frame length prefix followed by payload
	length := uint32(len(data))
	if err := binary.Write(cs.logFile, binary.BigEndian, length); err != nil {
		return LogEntry{}, err
	}

	if _, err := cs.logFile.Write(data); err != nil {
		return LogEntry{}, err
	}

	// Explicit fsync for durability guarantees matching etcd behavior
	if err := cs.logFile.Sync(); err != nil {
		return LogEntry{}, err
	}

	cs.logCache = append(cs.logCache, entry)
	cs.nextIndex++
	return entry, nil
}

func (cs *CrystalStore) TruncateAndAppendFollower(entry LogEntry) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// If entry index conflicts with cache, truncate cache and reset disk WAL file
	if entry.Index <= len(cs.logCache) {
		cs.logCache = cs.logCache[:entry.Index-1]
		if err := cs.logFile.Truncate(0); err != nil {
			return err
		}
		if _, err := cs.logFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		// Rewrite unconflicting cache entries to disk
		for _, e := range cs.logCache {
			data, _ := json.Marshal(e)
			length := uint32(len(data))
			_ = binary.Write(cs.logFile, binary.BigEndian, length)
			_, _ = cs.logFile.Write(data)
		}
		_ = cs.logFile.Sync()
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	length := uint32(len(data))
	if err := binary.Write(cs.logFile, binary.BigEndian, length); err != nil {
		return err
	}
	if _, err := cs.logFile.Write(data); err != nil {
		return err
	}
	if err := cs.logFile.Sync(); err != nil {
		return err
	}

	cs.logCache = append(cs.logCache, entry)
	cs.nextIndex = entry.Index + 1
	return nil
}

func (cs *CrystalStore) ApplyToStateMachine(entry LogEntry) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// This is where your LSM-tree / MemTable component will accept entries
	cs.data[entry.Key] = entry.Value
	log.Printf("[STATE MACHINE] Committed entry applied safely: Index %d (%s = %s)", entry.Index, entry.Key, entry.Value)
}

func (cs *CrystalStore) Get(key string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	val, ok := cs.data[key]
	return val, ok
}

func (cs *CrystalStore) GetEntry(index int) (LogEntry, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if index <= 0 || index > len(cs.logCache) {
		return LogEntry{}, false
	}
	return cs.logCache[index-1], true
}

func (cs *CrystalStore) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.logFile.Close()
}

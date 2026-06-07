package store

// Snapshot handles durable serialization of state machine state.
// A snapshot captures the full KV map at a given log index so that
// the WAL entries before that index can be discarded (log compaction).
//
// Snapshot file format:
//   [SnapshotMeta JSON header]\n[state JSON payload]
//
// Keeping the meta as a readable header makes debugging straightforward.

import (
	"encoding/json"
	"fmt"
	"os"
)

// SnapshotMeta records which log index and term this snapshot covers.
type SnapshotMeta struct {
	LastIncludedIndex int `json:"last_included_index"`
	LastIncludedTerm  int `json:"last_included_term"`
}

// SnapshotFile is the on-disk representation.
type SnapshotFile struct {
	Meta  SnapshotMeta      `json:"meta"`
	State map[string]string `json:"state"`
}

// SnapshotManager reads and writes snapshots to a file path.
type SnapshotManager struct {
	path string
}

// NewSnapshotManager creates a manager for the given path.
func NewSnapshotManager(path string) *SnapshotManager {
	return &SnapshotManager{path: path}
}

// Write serializes the state machine snapshot to disk atomically.
// "Atomically" here means: write to .tmp, fsync, rename. A crash
// mid-write leaves the previous snapshot intact.
func (sm *SnapshotManager) Write(meta SnapshotMeta, stateMachine StateMachine) error {
	raw, err := stateMachine.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot state machine: %w", err)
	}

	state, err := snapshotDecode(raw)
	if err != nil {
		return fmt.Errorf("decode snapshot for writing: %w", err)
	}

	sf := SnapshotFile{Meta: meta, State: state}
	data, err := json.Marshal(sf)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	tmp := sm.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create snapshot tmp: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	f.Close()

	return os.Rename(tmp, sm.path)
}

// Read loads the most recent snapshot from disk.
// Returns (nil, nil) if no snapshot exists yet.
func (sm *SnapshotManager) Read() (*SnapshotFile, error) {
	data, err := os.ReadFile(sm.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	var sf SnapshotFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}

	return &sf, nil
}

// EncodeState re-serializes the snapshot's state map so it can be passed to
// StateMachine.Restore without the SnapshotFile knowing about the StateMachine type.
func (sf *SnapshotFile) EncodeState() ([]byte, error) {
	return snapshotEncode(sf.State)
}

// ---- internal serialization helpers shared by StateMachine and SnapshotManager ----

func snapshotEncode(data map[string]string) ([]byte, error) {
	return json.Marshal(data)
}

func snapshotDecode(raw []byte) (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode snapshot state: %w", err)
	}
	return m, nil
}

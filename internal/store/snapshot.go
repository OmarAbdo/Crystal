package store

// Snapshot handles durable serialization of state machine state.
// A snapshot captures the full state machine at a given log index so that
// the WAL entries before that index can be discarded (log compaction).
//
// The snapshot file knows the METADATA but not the shape of the state. It
// carries whatever bytes StateMachine.Snapshot produced, verbatim, and hands
// them back to StateMachine.Restore unexamined. That matters more than it looks:
// the state machine's contents are its own business, and a snapshot format that
// mirrored them would have to change every time they did — which it would have
// had to when the dedup session table (F14) was added alongside the key-value
// data.

import (
	"encoding/json"
	"fmt"
	"os"

	"crystal/internal/fsutil"
)

// SnapshotMeta records which log index and term this snapshot covers.
type SnapshotMeta struct {
	LastIncludedIndex int `json:"last_included_index"`
	LastIncludedTerm  int `json:"last_included_term"`
}

// SnapshotFile is the on-disk representation: metadata the Raft layer needs,
// plus the state machine's own opaque serialization.
type SnapshotFile struct {
	Meta  SnapshotMeta    `json:"meta"`
	State json.RawMessage `json:"state"`
}

// SnapshotManager reads and writes snapshots to a file path.
type SnapshotManager struct {
	path string
}

// NewSnapshotManager creates a manager for the given path.
func NewSnapshotManager(path string) *SnapshotManager {
	return &SnapshotManager{path: path}
}

// Write serializes the state machine snapshot to disk atomically and durably:
// write to .tmp, fsync the file, rename, fsync the directory. A crash mid-write
// leaves the previous snapshot intact; a crash just after leaves the new one,
// never neither. The directory fsync matters because the log is truncated on the
// strength of this snapshot existing — losing the rename would discard entries
// covered by a snapshot that is no longer reachable.
func (sm *SnapshotManager) Write(meta SnapshotMeta, stateMachine StateMachine) error {
	raw, err := stateMachine.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot state machine: %w", err)
	}

	data, err := json.Marshal(SnapshotFile{Meta: meta, State: raw})
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	if err := fsutil.WriteFileAtomic(sm.path, data, 0666); err != nil {
		return fmt.Errorf("persist snapshot: %w", err)
	}
	return nil
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

// EncodeState returns the state machine's bytes exactly as they were written, to
// be handed to StateMachine.Restore or shipped in an InstallSnapshot RPC.
func (sf *SnapshotFile) EncodeState() ([]byte, error) {
	if len(sf.State) == 0 {
		return nil, fmt.Errorf("snapshot at index %d has no state", sf.Meta.LastIncludedIndex)
	}
	return sf.State, nil
}

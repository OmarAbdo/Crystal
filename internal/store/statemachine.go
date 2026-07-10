package store

// StateMachine is the interface that the Raft engine drives.
// It knows nothing about Raft — it only receives decoded Commands
// and answers Get queries.
//
// This interface is the seam that makes the storage backend swappable:
//   - MemoryStateMachine: current implementation, map[string]string
//   - LSMStateMachine:    future implementation backed by a MemTable + SSTables
//
// The engine calls Apply in index order, sequentially, from a single goroutine.
// Implementations do not need to handle concurrent Apply calls.

import (
	"fmt"
	"log"
	"sync"

	"crystal/internal/raft"
)

// StateMachine is the contract for any storage backend.
type StateMachine interface {
	// Apply executes a decoded command against the state machine.
	// Called by the engine in strict log-index order.
	Apply(index int, cmd raft.Command) error

	// Get retrieves a value by key. Returns (value, true) or ("", false).
	Get(key string) (string, bool)

	// Snapshot serializes the entire state machine state to bytes.
	// Used for log compaction: the engine writes this to disk, then
	// calls RaftLog.TruncateBeforeIndex to discard the covered log entries.
	Snapshot() ([]byte, error)

	// Restore replaces the entire state machine state from a snapshot.
	// Called when a new node receives a snapshot from the leader instead
	// of replaying the full log from entry 1.
	Restore(data []byte) error
}

// MemoryStateMachine is the in-memory implementation backed by a Go map.
// It is safe for concurrent Get calls but Apply must be called serially.
type MemoryStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemoryStateMachine returns an empty in-memory state machine.
func NewMemoryStateMachine() *MemoryStateMachine {
	return &MemoryStateMachine{
		data: make(map[string]string),
	}
}

// Apply executes a command. Supported ops: set, delete.
func (m *MemoryStateMachine) Apply(index int, cmd raft.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch cmd.Op {
	case raft.OpSet:
		if cmd.Key == "" {
			return fmt.Errorf("set: empty key at index %d", index)
		}
		m.data[cmd.Key] = cmd.Value
		log.Printf("[STATE MACHINE] index=%d set %q = %q", index, cmd.Key, cmd.Value)

	case raft.OpDelete:
		if cmd.Key == "" {
			return fmt.Errorf("delete: empty key at index %d", index)
		}
		delete(m.data, cmd.Key)
		log.Printf("[STATE MACHINE] index=%d delete %q", index, cmd.Key)

	case raft.OpNoop:
		// Leader's post-election no-op (§8): advances the commit frontier without
		// mutating state. Nothing to apply.

	default:
		return fmt.Errorf("unknown op %q at index %d", cmd.Op, index)
	}

	return nil
}

// Get returns the value for key, or ("", false) if absent.
func (m *MemoryStateMachine) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

// Snapshot serializes the current map to JSON.
func (m *MemoryStateMachine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return snapshotEncode(m.data)
}

// Restore replaces the map with the decoded snapshot.
func (m *MemoryStateMachine) Restore(data []byte) error {
	decoded, err := snapshotDecode(data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = decoded
	return nil
}

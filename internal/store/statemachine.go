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
	"encoding/json"
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

// session records the last command applied for one client, so a retransmission
// can be recognized and ignored (§8).
//
// Raft alone gives at-least-once, not exactly-once. A leader can commit an entry
// and die before answering; the client, seeing a timeout, retries, and the
// command applies twice. §8: "The solution is for clients to assign unique
// serial numbers to every command. Then, the state machine tracks the latest
// serial number processed for each client, along with the associated response."
//
// Err is that "associated response". For set and delete there is no return value,
// so the outcome is entirely captured by whether it succeeded; an operation that
// returned data would store it alongside.
type session struct {
	LastSeq uint64 `json:"last_seq"`
	Err     string `json:"err,omitempty"` // "" means the command succeeded
}

// machineState is what a snapshot of this state machine contains.
//
// The sessions table is in here deliberately, and leaving it out would be a
// quiet correctness bug: a node that restarted from a snapshot, or a follower
// that received one, would forget which commands it had already applied and
// would re-apply the next retransmission it saw. Dedup state is state.
//
// KNOWN LIMITATION (F23): sessions are never expired, so the table grows by one
// small entry per distinct client, forever, and that growth is carried in every
// snapshot. Coordination clients are typically few and long-lived so this is
// slow, but it is unbounded and therefore wrong for a long-running store. The
// real fix is client leases — a client registers, renews, and its session is
// reclaimed when it lapses — which also gives a client a way to learn that its
// session expired and that retries are no longer safe. Evicting entries without
// telling anyone would silently reopen the hole this closes, so it should not be
// done as a bare LRU.
type machineState struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

// MemoryStateMachine is the in-memory implementation backed by a Go map.
// It is safe for concurrent Get calls but Apply must be called serially.
type MemoryStateMachine struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
}

// NewMemoryStateMachine returns an empty in-memory state machine.
func NewMemoryStateMachine() *MemoryStateMachine {
	return &MemoryStateMachine{
		data:     make(map[string]string),
		sessions: make(map[string]session),
	}
}

// Apply executes a command. Supported ops: set, delete.
//
// A command carrying a ClientID and Seq is deduplicated: if this client's last
// applied sequence is at or above this one, the command is a retransmission and
// its recorded outcome is replayed instead of executing it again.
//
// Why this matters even for a store whose operations look idempotent: applying
// set(k,v) twice is harmless, but a retried DELETE is not. A client deletes k,
// times out, and retries; in between, someone else recreated k. Without dedup
// the retry destroys the new value. The hazard is not repetition, it is
// repetition after the world has moved on.
//
// Commands without a ClientID are applied as before. That keeps ad-hoc clients
// (curl, scripts) working; exactly-once is something a client opts into by
// identifying itself.
func (m *MemoryStateMachine) Apply(index int, cmd raft.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cmd.ClientID != "" {
		if prev, seen := m.sessions[cmd.ClientID]; seen && cmd.Seq <= prev.LastSeq {
			log.Printf("[STATE MACHINE] index=%d duplicate seq %d from client %s — replaying outcome",
				index, cmd.Seq, cmd.ClientID)
			if prev.Err == "" {
				return nil
			}
			return fmt.Errorf("%s", prev.Err)
		}
	}

	err := m.execute(index, cmd)

	if cmd.ClientID != "" {
		entry := session{LastSeq: cmd.Seq}
		if err != nil {
			entry.Err = err.Error()
		}
		m.sessions[cmd.ClientID] = entry
	}
	return err
}

// execute performs the command itself. Caller holds m.mu.
func (m *MemoryStateMachine) execute(index int, cmd raft.Command) error {
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

// Snapshot serializes the data AND the dedup sessions.
func (m *MemoryStateMachine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(machineState{Data: m.data, Sessions: m.sessions})
}

// Restore replaces the state machine from a snapshot, dedup table included.
func (m *MemoryStateMachine) Restore(data []byte) error {
	var st machineState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("decode snapshot state: %w", err)
	}
	if st.Data == nil {
		st.Data = make(map[string]string)
	}
	if st.Sessions == nil {
		st.Sessions = make(map[string]session)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = st.Data
	m.sessions = st.Sessions
	return nil
}

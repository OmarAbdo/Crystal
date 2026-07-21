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
//
// THE ONE RULE: a state machine must be deterministic. Every replica applies the
// same log and must reach the same state, so nothing here may consult the local
// clock, a random source, or anything else the replicas do not share. Where time
// is needed — session expiry — it comes from the log (Command.Timestamp), which
// makes "now" a replicated value that every node agrees on at every log position.

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"crystal/internal/raft"
)

// DefaultSessionTTL is how long a client session survives without use before it
// is reclaimed.
//
// It is deliberately far longer than any plausible retry window. What a session
// promises is "your retransmission will be recognized"; a retry arriving an hour
// after the original is not a retransmission in any meaningful sense, it is a
// fresh decision by a client that long since gave up.
const DefaultSessionTTL = time.Hour

// sweepInterval is how many applied commands pass between expiry sweeps.
//
// Sweeping every apply would be O(sessions) per command. Sweeping on a COUNT of
// applies rather than on elapsed time is what keeps it deterministic: every
// replica sweeps at exactly the same log positions.
const sweepInterval = 1000

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
// Err is that "associated response". For set and delete there is no return
// value, so the outcome is entirely captured by whether it succeeded; an
// operation that returned data would store it alongside.
type session struct {
	LastSeq uint64 `json:"last_seq"`
	Err     string `json:"err,omitempty"` // "" means the command succeeded

	// LastUsed is the replicated timestamp of this client's last command, and is
	// what expiry is measured against.
	LastUsed int64 `json:"last_used"`
}

// machineState is what a snapshot of this state machine contains.
//
// The sessions table is in here deliberately, and leaving it out would be a
// quiet correctness bug: a node that restarted from a snapshot, or a follower
// that received one, would forget which commands it had already applied and
// would re-apply the next retransmission it saw. Dedup state is state.
//
// Now and AppliesSinceSweep are here for the same reason. Expiry is driven by
// them, so a restored node that reset either would expire sessions at different
// log positions than its peers — which is to say, it would diverge.
type machineState struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`

	// Now is the state machine's clock: the highest timestamp seen in the log.
	Now int64 `json:"now"`

	// AppliesSinceSweep is progress toward the next expiry sweep.
	AppliesSinceSweep int `json:"applies_since_sweep"`
}

// MemoryStateMachine is the in-memory implementation backed by a Go map.
// It is safe for concurrent Get calls but Apply must be called serially.
type MemoryStateMachine struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session

	// now is the replicated clock: the highest Command.Timestamp applied so far.
	// It only moves forward, so a leader whose wall clock jumps backwards cannot
	// rewind time for everyone and resurrect expired sessions.
	now int64

	appliesSinceSweep int
	sessionTTL        time.Duration
}

// NewMemoryStateMachine returns an empty in-memory state machine.
func NewMemoryStateMachine() *MemoryStateMachine {
	return &MemoryStateMachine{
		data:       make(map[string]string),
		sessions:   make(map[string]session),
		sessionTTL: DefaultSessionTTL,
	}
}

// SetSessionTTL overrides how long an unused client session survives. A
// non-positive value is ignored.
//
// Every node in a cluster must be configured with the SAME value: the TTL is
// part of the replicated decision procedure, and nodes disagreeing about when a
// session expires would apply the same log to different states.
func (m *MemoryStateMachine) SetSessionTTL(d time.Duration) {
	if d <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionTTL = d
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

	// Advance the replicated clock, monotonically.
	if cmd.Timestamp > m.now {
		m.now = cmd.Timestamp
	}
	m.expireSessions()

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
		entry := session{LastSeq: cmd.Seq, LastUsed: m.now}
		if err != nil {
			entry.Err = err.Error()
		}
		m.sessions[cmd.ClientID] = entry
	}
	return err
}

// expireSessions reclaims sessions unused for longer than the TTL, bounding a
// table that would otherwise grow by one entry per distinct client forever and
// carry that growth in every snapshot.
//
// Both halves of the trigger are deterministic on purpose. The cutoff uses the
// REPLICATED clock, not time.Now(), and the cadence is a count of applies, not
// an interval — so every replica expires exactly the same sessions at exactly
// the same log positions. Using local time for either would let two nodes apply
// the same log and disagree about the result, which is the one thing a state
// machine may never do.
//
// A consequence worth stating: an idle cluster never sweeps, because nothing is
// applied. That is fine. An idle cluster is not accumulating sessions either —
// the clock advances only with traffic, and traffic is the only thing that grows
// the table.
//
// Caller holds m.mu.
func (m *MemoryStateMachine) expireSessions() {
	m.appliesSinceSweep++
	if m.appliesSinceSweep < sweepInterval {
		return
	}
	m.appliesSinceSweep = 0

	cutoff := m.now - int64(m.sessionTTL)
	for id, s := range m.sessions {
		if s.LastUsed < cutoff {
			delete(m.sessions, id)
		}
	}
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

// SessionCount reports how many client sessions are currently tracked.
func (m *MemoryStateMachine) SessionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// Snapshot serializes the data, the dedup sessions, and the replicated clock.
func (m *MemoryStateMachine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(machineState{
		Data:              m.data,
		Sessions:          m.sessions,
		Now:               m.now,
		AppliesSinceSweep: m.appliesSinceSweep,
	})
}

// Restore replaces the state machine from a snapshot, dedup table and clock
// included.
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
	m.now = st.Now
	m.appliesSinceSweep = st.AppliesSinceSweep
	return nil
}

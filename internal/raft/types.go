package raft

import "encoding/json"

// NodeRole represents the consensus role of a node in the cluster.
type NodeRole string

const (
	Leader    NodeRole = "Leader"
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
)

// LogEntry is the unit of replication in the Raft log.
// It is intentionally opaque about what Command contains —
// the Raft layer treats Command as an arbitrary byte slice.
// Only the state machine decodes it. This boundary is what
// allows the state machine to evolve independently of consensus.
type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command []byte `json:"command"` // opaque to the Raft layer
}

// CommandOp identifies the type of state machine operation.
type CommandOp string

const (
	OpSet    CommandOp = "set"
	OpDelete CommandOp = "delete"
	// OpNoop is a leader's no-op entry, appended immediately on election (§8).
	// It carries no state change; its purpose is to give the new leader a
	// current-term entry to commit, which implicitly commits any prior-term
	// entries that were replicated but not yet committable under §5.4.2.
	OpNoop CommandOp = "noop"
)

// Command is the application-level payload carried inside a LogEntry.
// Only the state machine package imports and decodes this type.
type Command struct {
	Op    CommandOp `json:"op"`
	Key   string    `json:"key"`
	Value string    `json:"value,omitempty"` // empty for delete
}

// EncodeCommand serializes a Command into bytes for embedding in a LogEntry.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes bytes from a LogEntry into a Command.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// RequestVoteRequest is the payload sent to a peer's /internal/vote endpoint
// during an election (Figure 2). LastLogIndex/LastLogTerm let the voter apply
// the §5.4.1 election restriction: it only grants a vote to a candidate whose
// log is at least as up-to-date as its own.
type RequestVoteRequest struct {
	Term         int `json:"term"`           // candidate's term
	CandidateID  int `json:"candidate_id"`   // candidate requesting the vote
	LastLogIndex int `json:"last_log_index"` // index of candidate's last log entry
	LastLogTerm  int `json:"last_log_term"`  // term of candidate's last log entry
}

// RequestVoteResponse is returned by the voter.
type RequestVoteResponse struct {
	Term        int  `json:"term"`         // currentTerm, for candidate to update itself
	VoteGranted bool `json:"vote_granted"` // true if the candidate received the vote
}

// InstallSnapshotRequest is the payload sent to a follower's /internal/snapshot
// endpoint (Figure 13). Crystal sends the whole snapshot in one RPC rather than
// chunked (our snapshots are tiny JSON maps), so the paper's offset/done fields
// collapse away. Data is the serialized state-machine snapshot.
type InstallSnapshotRequest struct {
	Term              int    `json:"term"`                // leader's term
	LeaderID          int    `json:"leader_id"`           // so follower can redirect clients
	LastIncludedIndex int    `json:"last_included_index"` // snapshot replaces entries up to here
	LastIncludedTerm  int    `json:"last_included_term"`  // term of LastIncludedIndex
	Data              []byte `json:"data"`                // serialized state machine state
}

// InstallSnapshotResponse carries the follower's term so a stale leader can
// detect it must step down.
type InstallSnapshotResponse struct {
	Term int `json:"term"`
}

// PersistentState is the Raft state that MUST survive node restarts.
// Losing this data can cause a node to vote twice in the same term,
// which breaks Raft's fundamental safety guarantee.
type PersistentState struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"` // -1 means no vote cast this term
}

// AppendEntriesRequest is the payload sent to a follower's /internal/append endpoint.
// It mirrors the AppendEntries RPC from Figure 2 of the Raft paper: it carries the
// log-matching anchor (PrevLogIndex/PrevLogTerm), a batch of new entries (empty for
// a pure heartbeat), and the leader's commit index so the follower can advance its
// own commitIndex without a separate message.
type AppendEntriesRequest struct {
	Term         int        `json:"term"`          // leader's term
	LeaderID     int        `json:"leader_id"`     // so follower can redirect clients
	PrevLogIndex int        `json:"prev_log_index"` // index of entry immediately preceding Entries
	PrevLogTerm  int        `json:"prev_log_term"`  // term of PrevLogIndex entry
	Entries      []LogEntry `json:"entries"`        // new entries to store (empty = heartbeat)
	LeaderCommit int        `json:"leader_commit"`  // leader's commitIndex
}

// AppendEntriesResponse is returned by the follower after processing an
// AppendEntriesRequest.
//
// Success reports whether the follower's log contained an entry matching
// PrevLogIndex/PrevLogTerm (§5.3). MatchIndex is the highest index now known
// to be replicated on the follower (valid only when Success is true).
//
// ConflictTerm and ConflictIndex are the §5.3 optimization: on a rejection,
// the follower reports the term of the conflicting entry and the first index
// it stores for that term, so the leader can skip a whole term's worth of
// entries in one nextIndex decrement instead of backing up one index at a time.
type AppendEntriesResponse struct {
	Term          int  `json:"term"`           // currentTerm, for leader to update itself
	Success       bool `json:"success"`
	MatchIndex    int  `json:"match_index"`    // highest replicated index (when Success)
	ConflictTerm  int  `json:"conflict_term"`  // term of the conflicting entry (when !Success)
	ConflictIndex int  `json:"conflict_index"` // first index of ConflictTerm, or follower log length+1
}

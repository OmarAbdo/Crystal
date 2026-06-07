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

// PersistentState is the Raft state that MUST survive node restarts.
// Losing this data can cause a node to vote twice in the same term,
// which breaks Raft's fundamental safety guarantee.
type PersistentState struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"` // -1 means no vote cast this term
}

// AppendEntryRequest is the payload sent to a follower's /internal/append endpoint.
// It carries both the log entry and the leader's current commit index so the
// follower can advance its own commitIndex without a separate heartbeat.
type AppendEntryRequest struct {
	LeaderID     int      `json:"leader_id"`
	LeaderTerm   int      `json:"leader_term"`
	LeaderCommit int      `json:"leader_commit"`
	Entry        LogEntry `json:"entry"`
}

// AppendEntryResponse is returned by the follower after processing an AppendEntryRequest.
type AppendEntryResponse struct {
	Success    bool `json:"success"`
	MatchIndex int  `json:"match_index"`
}

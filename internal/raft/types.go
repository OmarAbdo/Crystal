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

// PreVoteRequest asks a peer whether it WOULD vote for this candidate, without
// anyone changing any state (dissertation §9.6).
//
// An isolated node that keeps timing out increments its term on every attempt.
// When it rejoins, that inflated term propagates — not only through RequestVote
// but through the AppendEntries response it sends the leader, which Figure 2
// obliges the leader to honour by stepping down. A healthy cluster is disrupted
// by a node that was never a candidate for anything.
//
// Pre-vote closes it at the source: the term is only incremented once a majority
// has said the candidate could plausibly win. A node alone on the wrong side of a
// partition never gets that majority, so its term never moves, so it has nothing
// disruptive to say when it returns.
//
// Term here is the term the candidate WOULD campaign in (currentTerm + 1). It is
// hypothetical; no receiver adopts it.
type PreVoteRequest struct {
	Term         int `json:"term"`           // the term the candidate would use
	CandidateID  int `json:"candidate_id"`
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
}

// PreVoteResponse reports whether the peer would grant a real vote. Term is the
// responder's actual current term, which lets a candidate discover it is behind.
type PreVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
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
// detect it must step down, and whether the snapshot was actually installed.
//
// Figure 13 returns only the term, because the paper's receiver has no failure
// modes short of a stale term: the snapshot is written to a file and the state
// machine is reset, and neither is modelled as fallible. Ours are — restore can
// reject corrupt data, persist can hit a full disk, the log reset can fail — and
// the leader advances the peer's matchIndex on the strength of this reply.
// Without Success, a follower that failed to install is indistinguishable from
// one holding the data, so a phantom quorum commits entries stored on a single
// server. Leader Completeness, lost to a missing bool.
type InstallSnapshotResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"` // true only if the snapshot is installed and durable
}

// ReadIndexRequest asks the leader for a read index: the log position a replica
// must have applied before it may answer a linearizable read.
//
// This is what lets a FOLLOWER serve a linearizable read. The leader supplies
// only a number — confirmed current by a quorum round — and the follower does the
// rest: it waits for its own apply to reach that index, then answers from its own
// state machine. The payload never touches the leader, so read throughput scales
// with the cluster instead of being pinned to one node.
//
// The soundness argument is short. A replica only applies committed entries, and
// commitment requires a quorum, so nothing a replica has applied can be phantom.
// The read index just pins how far it must have caught up for the answer to
// reflect everything committed as of the moment the read was admitted.
type ReadIndexRequest struct {
	FromNodeID int `json:"from_node_id"` // for logging only
}

// ReadIndexResponse carries the confirmed index, or reports that this node could
// not supply one. Term lets the asker learn it is talking to the wrong node.
type ReadIndexResponse struct {
	Term      int  `json:"term"`
	ReadIndex int  `json:"read_index"`
	Success   bool `json:"success"` // false if we are not the leader or lost quorum
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

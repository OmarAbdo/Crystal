package transport

// HTTP transport layer. Handlers are thin: they validate input,
// translate to domain types, and delegate to the engine or store.
// They know nothing about Raft internals — only about the engine's
// Proposal type and the StateMachine's Get interface.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"crystal/internal/engine"
	"crystal/internal/raft"
	"crystal/internal/store"
)

const (
	writeTimeout = 2 * time.Second

	// readTimeout bounds a linearizable read end to end. It is slightly longer
	// than the engine's own read deadline so the engine's specific error
	// (ErrReadTimeout) reaches the client instead of being masked by ours.
	readTimeout = 3 * time.Second
)

// leaderHintHeader carries the leader's address on a redirect, so a client can
// follow it without parsing prose.
const leaderHintHeader = "X-Raft-Leader"

// stalenessHeader reports how out of date the answering node's knowledge was, so
// a client that asked for a bound can see how much of it was actually used.
const stalenessHeader = "X-Raft-Staleness"

// Server holds the dependencies injected into HTTP handlers.
type Server struct {
	node         leaderChecker
	proposals    chan<- engine.Proposal
	reader       reader
	stateMachine store.StateMachine
	rpc          rpcHandler

	// peers maps node ID → address, used to turn the leader ID this node knows
	// into something the client can actually connect to.
	peers map[int]string
}

// leaderChecker is the subset of RaftNode the client-facing handlers need.
type leaderChecker interface {
	IsLeader() bool
	NodeID() int

	// CurrentLeader returns the ID of the leader this node last heard from, or 0
	// when none is known.
	CurrentLeader() int
}

// reader establishes when a local read is safe. The engine implements it, and
// handles the leader/follower split internally — a follower fetches a read index
// from the leader and then serves the read itself, so this layer does not need
// to know which role it is running on.
type reader interface {
	Read(opts engine.ReadOptions, timeout time.Duration) error

	// Staleness reports how long since this node last confirmed it was current
	// with a leader, for reporting back to the client.
	Staleness() time.Duration
}

// rpcHandler owns the full AppendEntries and RequestVote receiver logic (term
// checks, consistency check, commit advancement, vote decision). The transport
// layer stays thin: it decodes the request, calls this, and encodes the
// response. All consensus logic lives behind this seam in the raft package.
type rpcHandler interface {
	HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse
	HandlePreVote(req raft.PreVoteRequest) raft.PreVoteResponse
	HandleReadIndex(req raft.ReadIndexRequest) raft.ReadIndexResponse
	HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse
	HandleInstallSnapshot(req raft.InstallSnapshotRequest) raft.InstallSnapshotResponse
}

// NewServer wires up the HTTP server dependencies.
func NewServer(
	node leaderChecker,
	proposals chan<- engine.Proposal,
	rd reader,
	sm store.StateMachine,
	rpc rpcHandler,
	peers map[int]string,
) *Server {
	return &Server{
		node:         node,
		proposals:    proposals,
		reader:       rd,
		stateMachine: sm,
		rpc:          rpc,
		peers:        peers,
	}
}

// RegisterRoutes mounts all routes on the provided mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/set", s.handleSet)
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/internal/append", s.handleInternalAppend)
	mux.HandleFunc("/internal/prevote", s.handleInternalPreVote)
	mux.HandleFunc("/internal/readindex", s.handleInternalReadIndex)
	mux.HandleFunc("/internal/vote", s.handleInternalVote)
	mux.HandleFunc("/internal/snapshot", s.handleInternalSnapshot)
}

// ---- Public client-facing handlers ----

// setRequest is a write. ClientID and Seq are optional, and supplying them is
// how a client opts into exactly-once semantics (§8): a retry after a timeout is
// then recognized as a retransmission rather than applied a second time.
type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`

	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	if !s.node.IsLeader() {
		s.redirectToLeader(w)
		return
	}

	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}

	if err := validateClientTag(req.ClientID, req.Seq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.submitAndRespond(w, raft.Command{
		Op: raft.OpSet, Key: req.Key, Value: req.Value,
		ClientID: req.ClientID, Seq: req.Seq,
	})
}

// deleteRequest is a delete. Supplying ClientID and Seq matters MORE here than
// for a set: a retried delete after the key has been recreated by someone else
// destroys the new value. The hazard is not repetition, it is repetition after
// the world has moved on.
type deleteRequest struct {
	Key string `json:"key"`

	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	if !s.node.IsLeader() {
		s.redirectToLeader(w)
		return
	}

	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}

	if err := validateClientTag(req.ClientID, req.Seq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.submitAndRespond(w, raft.Command{
		Op: raft.OpDelete, Key: req.Key,
		ClientID: req.ClientID, Seq: req.Seq,
	})
}

// handleGet serves a read. Three tiers, and the default is the safe one:
//
//	?consistency=linearizable  (default) reflects everything committed when the
//	    read was admitted. ANY node serves it — a follower fetches a
//	    quorum-confirmed read index from the leader and then answers locally, so
//	    read capacity scales with the cluster rather than being pinned to one node.
//
//	?consistency=bounded&max_staleness=2s  answers from local state, but only if
//	    this node has confirmed contact with a leader within the bound. A node
//	    that cannot REFUSES; the bound is enforced, not advisory. max_staleness is
//	    required — a client accepting staleness has to say how much.
//
//	?consistency=local  whatever this node holds, no guarantee at all. An
//	    operational and debugging tool ("what does THIS node think"), not a
//	    consistency level.
//
// Every answer carries X-Raft-Staleness, so a client can see how much of its
// budget was actually used.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}

	switch consistency := r.URL.Query().Get("consistency"); consistency {
	case "", "linearizable":
		if !s.serveRead(w, engine.ReadOptions{Consistency: engine.Linearizable}) {
			return
		}

	case "bounded":
		maxStaleness, err := parseMaxStaleness(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !s.serveRead(w, engine.ReadOptions{
			Consistency:  engine.BoundedStale,
			MaxStaleness: maxStaleness,
		}) {
			return
		}

	case "local":
		// Whatever this node holds, with no guarantee whatsoever. This is an
		// operational and debugging tool — "what does THIS node think" — not a
		// consistency level, and it is deliberately not reachable by accident.

	default:
		http.Error(w, fmt.Sprintf("unknown consistency %q: want linearizable, "+
			"bounded or local", consistency), http.StatusBadRequest)
		return
	}

	val, exists := s.stateMachine.Get(key)
	if !exists {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set(stalenessHeader, s.reader.Staleness().Truncate(time.Millisecond).String())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

// parseMaxStaleness reads the bound a bounded read must respect. It is REQUIRED:
// a client asking for possibly-stale data has to say how stale it will tolerate,
// rather than inheriting a number the server picked for it.
func parseMaxStaleness(r *http.Request) (time.Duration, error) {
	raw := r.URL.Query().Get("max_staleness")
	if raw == "" {
		return 0, fmt.Errorf("consistency=bounded requires max_staleness (e.g. max_staleness=2s)")
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid max_staleness %q: %w", raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("max_staleness must be positive, got %s", d)
	}
	return d, nil
}

// serveRead asks the engine whether a local read is permissible under opts. It
// returns false if the request has already been answered with an error.
func (s *Server) serveRead(w http.ResponseWriter, opts engine.ReadOptions) bool {
	err := s.reader.Read(opts, readTimeout)
	switch {
	case err == nil:
		return true

	case errors.Is(err, engine.ErrNotLeader):
		// Either we do not know who leads, or the leader would not confirm. Send
		// the client somewhere useful rather than quietly serving local state.
		s.redirectToLeader(w)

	case errors.Is(err, engine.ErrTooStale):
		// The client named a bound and we cannot meet it. Reporting our actual
		// staleness lets it decide whether to widen the bound or go elsewhere.
		w.Header().Set(stalenessHeader, s.reader.Staleness().Truncate(time.Millisecond).String())
		http.Error(w, err.Error(), http.StatusServiceUnavailable)

	case errors.Is(err, engine.ErrMaxStalenessRequired):
		http.Error(w, err.Error(), http.StatusBadRequest)

	case errors.Is(err, engine.ErrReadTimeout):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)

	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	return false
}

// ---- Internal cluster handler ----

func (s *Server) handleInternalAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// All term checks, the log-matching consistency check, and commit
	// advancement happen inside the raft package. The response carries the
	// follower's term and (on rejection) the conflict hints the leader uses
	// to backtrack nextIndex.
	resp := s.rpc.HandleAppendEntries(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleInternalPreVote is the pre-vote endpoint. Answering it commits this node
// to nothing, which is what makes the straw poll safe to run.
func (s *Server) handleInternalPreVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req raft.PreVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.rpc.HandlePreVote(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleInternalReadIndex is the ReadIndex endpoint: a follower asking this
// leader for the log position it must reach before serving a linearizable read.
// Only an integer travels back — the follower does the reading.
func (s *Server) handleInternalReadIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req raft.ReadIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.rpc.HandleReadIndex(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleInternalVote is the RequestVote endpoint. Like the append handler, it is
// thin: the term check, up-to-date restriction, and vote persistence all live in
// the raft package.
func (s *Server) handleInternalVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req raft.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.rpc.HandleRequestVote(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleInternalSnapshot is the InstallSnapshot endpoint, used when the leader
// has compacted past this follower's nextIndex. Thin as ever: decode, delegate
// to the raft package, encode.
func (s *Server) handleInternalSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req raft.InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.rpc.HandleInstallSnapshot(req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ---- Shared helpers ----

// validateClientTag rejects a half-specified client tag. A ClientID without a
// Seq cannot be deduplicated (every request would look like sequence zero, so
// the second one would be discarded as a duplicate of the first), and a Seq
// without a ClientID has nothing to be a sequence OF. Either both or neither.
func validateClientTag(clientID string, seq uint64) error {
	switch {
	case clientID == "" && seq == 0:
		return nil // opted out of exactly-once
	case clientID == "":
		return fmt.Errorf("seq given without client_id: a sequence number needs " +
			"a client to be a sequence of")
	case seq == 0:
		return fmt.Errorf("client_id given without seq: exactly-once needs a " +
			"per-client sequence number, starting at 1")
	default:
		return nil
	}
}

// redirectToLeader rejects a write with 421 and tells the client where the
// leader actually is.
//
// §8: "that server will reject the client's request and supply information about
// the most recent leader it has heard from." This used to name s.node.NodeID() —
// the node doing the rejecting — which tells the client nothing it did not
// already know and leaves it rediscovering the leader by guessing.
//
// 421 Misdirected Request is the accurate status: the request was well formed and
// the client is authorized, it simply arrived at a server that cannot serve it.
//
// A leader we know by ID but not by address is still worth naming: the client may
// have routing information this node does not.
func (s *Server) redirectToLeader(w http.ResponseWriter) {
	leaderID := s.node.CurrentLeader()
	if leaderID == 0 {
		// Genuinely leaderless — between a failure and the next election. Saying
		// so beats naming a node we know is wrong.
		http.Error(w, "not the leader; no known leader, retry shortly",
			http.StatusMisdirectedRequest)
		return
	}

	addr, known := s.peers[leaderID]
	if !known {
		http.Error(w, fmt.Sprintf("not the leader; leader is node %d", leaderID),
			http.StatusMisdirectedRequest)
		return
	}

	w.Header().Set(leaderHintHeader, addr)
	http.Error(w, fmt.Sprintf("not the leader; leader is node %d at %s", leaderID, addr),
		http.StatusMisdirectedRequest)
}

// submitAndRespond sends a command to the engine and writes the HTTP response.
func (s *Server) submitAndRespond(w http.ResponseWriter, cmd raft.Command) {
	resultCh := make(chan error, 1)

	select {
	case s.proposals <- engine.Proposal{Command: cmd, ResultCh: resultCh}:
	case <-time.After(writeTimeout):
		http.Error(w, "proposal queue full", http.StatusServiceUnavailable)
		return
	}

	select {
	case err := <-resultCh:
		if err == nil {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "OK")
			return
		}
		if errors.Is(err, engine.ErrNotLeader) {
			// We were leader when the proposal was accepted and were deposed
			// before it committed. The client needs the same redirect it would
			// have got had it arrived a moment later — including the hint, since
			// by now we usually know who replaced us.
			s.redirectToLeader(w)
			return
		}
		if errors.Is(err, engine.ErrCommitTimeout) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)

	case <-time.After(writeTimeout):
		http.Error(w, "engine response timeout", http.StatusServiceUnavailable)
	}
}

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

// Server holds the dependencies injected into HTTP handlers.
type Server struct {
	node         leaderChecker
	proposals    chan<- engine.Proposal
	reads        chan<- engine.Read
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

// rpcHandler owns the full AppendEntries and RequestVote receiver logic (term
// checks, consistency check, commit advancement, vote decision). The transport
// layer stays thin: it decodes the request, calls this, and encodes the
// response. All consensus logic lives behind this seam in the raft package.
type rpcHandler interface {
	HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse
	HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse
	HandleInstallSnapshot(req raft.InstallSnapshotRequest) raft.InstallSnapshotResponse
}

// NewServer wires up the HTTP server dependencies.
func NewServer(
	node leaderChecker,
	proposals chan<- engine.Proposal,
	reads chan<- engine.Read,
	sm store.StateMachine,
	rpc rpcHandler,
	peers map[int]string,
) *Server {
	return &Server{
		node:         node,
		proposals:    proposals,
		reads:        reads,
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
	mux.HandleFunc("/internal/vote", s.handleInternalVote)
	mux.HandleFunc("/internal/snapshot", s.handleInternalSnapshot)
}

// ---- Public client-facing handlers ----

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

	s.submitAndRespond(w, raft.Command{Op: raft.OpSet, Key: req.Key, Value: req.Value})
}

type deleteRequest struct {
	Key string `json:"key"`
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

	s.submitAndRespond(w, raft.Command{Op: raft.OpDelete, Key: req.Key})
}

// handleGet serves a read. It is LINEARIZABLE by default (§8): the request is
// admitted by the leader, held until a majority has confirmed that leadership is
// still current, and only then answered from the local state machine.
//
// `?consistency=stale` opts out, reading local state immediately from whichever
// node is asked. That is the old behavior, and it is genuinely useful — for
// convergence checks, dashboards, anything that would rather have a fast answer
// than a current one. It is opt-in because a client that has not thought about
// staleness should not silently receive it.
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
		if !s.awaitLinearizableRead(w) {
			return // response already written
		}
	case "stale":
		// Read whatever this node has, leader or not.
	default:
		http.Error(w, fmt.Sprintf("unknown consistency %q: want linearizable or stale",
			consistency), http.StatusBadRequest)
		return
	}

	val, exists := s.stateMachine.Get(key)
	if !exists {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

// awaitLinearizableRead blocks until the engine confirms a local read is safe.
// It returns false if the request has been answered with an error instead.
//
// The engine deliberately does not see the key. Its only job is to establish
// that this node's applied state is current as of the moment the read arrived;
// what gets read afterwards is not a consensus concern.
func (s *Server) awaitLinearizableRead(w http.ResponseWriter) bool {
	if !s.node.IsLeader() {
		// Only the leader can establish this. Send the client somewhere useful
		// rather than quietly serving stale local state.
		s.redirectToLeader(w)
		return false
	}

	resultCh := make(chan error, 1)
	select {
	case s.reads <- engine.Read{ResultCh: resultCh}:
	case <-time.After(readTimeout):
		http.Error(w, "read queue full", http.StatusServiceUnavailable)
		return false
	}

	select {
	case err := <-resultCh:
		switch {
		case err == nil:
			return true
		case errors.Is(err, engine.ErrNotLeader):
			s.redirectToLeader(w)
		case errors.Is(err, engine.ErrReadTimeout):
			// We could not prove we are still leader. Refusing is the point.
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return false

	case <-time.After(readTimeout):
		http.Error(w, "engine read timeout", http.StatusServiceUnavailable)
		return false
	}
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

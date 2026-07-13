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

const writeTimeout = 2 * time.Second

// Server holds the dependencies injected into HTTP handlers.
type Server struct {
	node         leaderChecker
	proposals    chan<- engine.Proposal
	stateMachine store.StateMachine
	rpc          rpcHandler
}

// leaderChecker is the subset of RaftNode the client-facing handlers need.
type leaderChecker interface {
	IsLeader() bool
	NodeID() int
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
	sm store.StateMachine,
	rpc rpcHandler,
) *Server {
	return &Server{
		node:         node,
		proposals:    proposals,
		stateMachine: sm,
		rpc:          rpc,
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
		http.Error(w,
			fmt.Sprintf("not the leader; route to node %d", s.node.NodeID()),
			http.StatusMisdirectedRequest)
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
		http.Error(w,
			fmt.Sprintf("not the leader; route to node %d", s.node.NodeID()),
			http.StatusMisdirectedRequest)
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

	val, exists := s.stateMachine.Get(key)
	if !exists {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": val})
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
			http.Error(w, err.Error(), http.StatusMisdirectedRequest)
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

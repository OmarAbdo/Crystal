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
	raftLog      appendReceiver
}

// leaderChecker is the subset of RaftNode the transport layer needs.
type leaderChecker interface {
	IsLeader() bool
	NodeID() int
}

// appendReceiver is the subset of RaftLog the internal append handler needs.
type appendReceiver interface {
	AppendFollower(entry raft.LogEntry) error
	LatestIndex() int
}

// NewServer wires up the HTTP server dependencies.
func NewServer(
	node leaderChecker,
	proposals chan<- engine.Proposal,
	sm store.StateMachine,
	raftLog appendReceiver,
) *Server {
	return &Server{
		node:         node,
		proposals:    proposals,
		stateMachine: sm,
		raftLog:      raftLog,
	}
}

// RegisterRoutes mounts all routes on the provided mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/set", s.handleSet)
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/internal/append", s.handleInternalAppend)
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

	var req raft.AppendEntryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.raftLog.AppendFollower(req.Entry); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := raft.AppendEntryResponse{
		Success:    true,
		MatchIndex: req.Entry.Index,
	}
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

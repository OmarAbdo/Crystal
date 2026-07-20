package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crystal/internal/engine"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// §8: "If the client's first choice is not the leader, that server will reject
// the client's request and supply information about the most recent leader it has
// heard from." A rejection that names the rejecting node tells the client nothing
// it did not already know, and leaves it to rediscover the leader by guessing.

// fakeLeaderChecker stands in for RaftNode in the redirect tests.
type fakeLeaderChecker struct {
	isLeader bool
	nodeID   int
	leaderID int
}

func (f fakeLeaderChecker) IsLeader() bool     { return f.isLeader }
func (f fakeLeaderChecker) NodeID() int        { return f.nodeID }
func (f fakeLeaderChecker) CurrentLeader() int { return f.leaderID }

// nilRPC satisfies rpcHandler; the client-facing tests never reach it.
type nilRPC struct{}

func (nilRPC) HandleAppendEntries(raft.AppendEntriesRequest) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{}
}
func (nilRPC) HandleRequestVote(raft.RequestVoteRequest) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{}
}
func (nilRPC) HandleInstallSnapshot(raft.InstallSnapshotRequest) raft.InstallSnapshotResponse {
	return raft.InstallSnapshotResponse{}
}

func newTestServer(t *testing.T, node leaderChecker, peers map[int]string) *Server {
	t.Helper()
	return NewServer(node, make(chan engine.Proposal, 1), store.NewMemoryStateMachine(),
		nilRPC{}, peers)
}

func postSet(t *testing.T, s *Server, key, value string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(setRequest{Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/set", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	s.handleSet(w, req)
	return w
}

func TestHandleSet_RedirectsToKnownLeader(t *testing.T) {
	node := fakeLeaderChecker{isLeader: false, nodeID: 1, leaderID: 2}
	s := newTestServer(t, node, map[int]string{2: "10.0.0.2:8080", 3: "10.0.0.3:8080"})

	w := postSet(t, s, "k", "v")

	if w.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMisdirectedRequest)
	}

	body := w.Body.String()
	if strings.Contains(body, "node 1") {
		t.Fatalf("redirect names the rejecting node, not the leader: %q", body)
	}
	if !strings.Contains(body, "2") {
		t.Fatalf("redirect does not name leader node 2: %q", body)
	}
	// The address is the part a client can actually act on.
	if !strings.Contains(body, "10.0.0.2:8080") {
		t.Fatalf("redirect omits the leader's address: %q", body)
	}
	if got := w.Header().Get(leaderHintHeader); got != "10.0.0.2:8080" {
		t.Fatalf("%s = %q, want the leader's address", leaderHintHeader, got)
	}
}

// Between a leader's failure and the next election there is genuinely no leader
// to name. Saying so is more useful than naming a node we know is wrong.
func TestHandleSet_ReportsUnknownLeader(t *testing.T) {
	node := fakeLeaderChecker{isLeader: false, nodeID: 1, leaderID: 0}
	s := newTestServer(t, node, map[int]string{2: "10.0.0.2:8080"})

	w := postSet(t, s, "k", "v")

	if w.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMisdirectedRequest)
	}
	if body := w.Body.String(); !strings.Contains(body, "no known leader") {
		t.Fatalf("body = %q, want it to report that no leader is known", body)
	}
	if got := w.Header().Get(leaderHintHeader); got != "" {
		t.Fatalf("%s = %q, want empty when no leader is known", leaderHintHeader, got)
	}
}

// A leader whose address we do not hold (not in our peer map) is still worth
// naming by ID — the client may know how to reach it even when we do not.
func TestHandleSet_NamesLeaderWithoutAddress(t *testing.T) {
	node := fakeLeaderChecker{isLeader: false, nodeID: 1, leaderID: 7}
	s := newTestServer(t, node, map[int]string{2: "10.0.0.2:8080"})

	w := postSet(t, s, "k", "v")

	if body := w.Body.String(); !strings.Contains(body, "7") {
		t.Fatalf("body = %q, want it to name leader node 7", body)
	}
}

func TestHandleDelete_RedirectsToKnownLeader(t *testing.T) {
	node := fakeLeaderChecker{isLeader: false, nodeID: 1, leaderID: 3}
	s := newTestServer(t, node, map[int]string{3: "10.0.0.3:8080"})

	body, _ := json.Marshal(deleteRequest{Key: "k"})
	req := httptest.NewRequest(http.MethodPost, "/delete", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	s.handleDelete(w, req)

	if w.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusMisdirectedRequest)
	}
	if got := w.Header().Get(leaderHintHeader); got != "10.0.0.3:8080" {
		t.Fatalf("%s = %q", leaderHintHeader, got)
	}
}

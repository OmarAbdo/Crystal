package raft

// Transport is the seam between consensus and the network.
//
// It exists so the cluster can be tested. Every interesting Raft failure is a
// network failure — a partition that splits the cluster, a link that drops one
// direction, a peer that black-holes requests instead of refusing them — and
// none of those can be provoked through a real http.Client against real sockets
// with any reliability. Behind this interface a test can cut a single directed
// link at a chosen moment and assert what the cluster does about it.
//
// The interface is deliberately request/response and address-keyed, mirroring
// what the Raft RPCs actually are, so the HTTP implementation stays a thin
// marshalling layer with no consensus logic in it.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Transport delivers Raft RPCs to the peer listening at addr.
//
// An error means the peer could not be reached or did not answer usefully; the
// caller treats that as "no information" and retries later, exactly as §5.5
// prescribes. It must never be used to signal a negative Raft outcome — a
// rejected AppendEntries is a successful RPC carrying Success: false.
type Transport interface {
	AppendEntries(addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)
	PreVote(addr string, req PreVoteRequest) (PreVoteResponse, error)
	RequestVote(addr string, req RequestVoteRequest) (RequestVoteResponse, error)
	InstallSnapshot(addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// HTTPTransport is the production Transport: one JSON POST per RPC.
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport returns a Transport whose requests time out after timeout.
// The timeout matters for liveness rather than safety: a leader that blocks
// forever on a black-holed peer stops making progress for everyone else.
func NewHTTPTransport(timeout time.Duration) *HTTPTransport {
	return &HTTPTransport{client: &http.Client{Timeout: timeout}}
}

func (t *HTTPTransport) AppendEntries(addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return postJSON[AppendEntriesRequest, AppendEntriesResponse](t.client, addr, "internal/append", req)
}

func (t *HTTPTransport) PreVote(addr string, req PreVoteRequest) (PreVoteResponse, error) {
	return postJSON[PreVoteRequest, PreVoteResponse](t.client, addr, "internal/prevote", req)
}

func (t *HTTPTransport) RequestVote(addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	return postJSON[RequestVoteRequest, RequestVoteResponse](t.client, addr, "internal/vote", req)
}

func (t *HTTPTransport) InstallSnapshot(addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return postJSON[InstallSnapshotRequest, InstallSnapshotResponse](t.client, addr, "internal/snapshot", req)
}

// postJSON marshals req, POSTs it to the peer's endpoint, and decodes the reply.
// It is a free function rather than a method because Go methods cannot be
// generic, and the three RPCs differ only in their types and path.
func postJSON[Req any, Resp any](client *http.Client, addr, path string, req Req) (Resp, error) {
	var resp Resp

	payload, err := json.Marshal(req)
	if err != nil {
		return resp, fmt.Errorf("marshal %s request: %w", path, err)
	}

	url := fmt.Sprintf("http://%s/%s", addr, path)
	httpResp, err := client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return resp, fmt.Errorf("post %s to %s: %w", path, addr, err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("peer %s returned HTTP %d for %s", addr, httpResp.StatusCode, path)
	}

	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode %s response from %s: %w", path, addr, err)
	}

	return resp, nil
}

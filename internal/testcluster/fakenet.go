package testcluster

// fakeNet is an in-process stand-in for the network between Raft nodes.
//
// It exists because Raft's interesting behavior only appears under partition,
// and partitions cannot be provoked reliably against real sockets. What matters
// is not that messages are lost, but exactly WHICH messages, in WHICH direction,
// at WHICH moment — a leader that can send but not receive behaves very
// differently from one that can do neither, and only one of those is reachable
// by killing a process.
//
// Two properties are worth stating because they are easy to get wrong and
// silently weaken every test built on top:
//
//   - Cuts are DIRECTED. Cutting a→b leaves b→a intact.
//   - A cut applies to the RESPONSE leg as well as the request leg. A delivered
//     request whose reply is lost must look to the caller exactly like a request
//     that never arrived; if the reply slipped through, the caller would learn
//     something a real partition would never have told it.

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"crystal/internal/raft"
)

// rpcHandler is the receiving half of a node — the same three methods the HTTP
// transport delivers to in production.
type rpcHandler interface {
	HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse
	HandlePreVote(req raft.PreVoteRequest) raft.PreVoteResponse
	HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse
	HandleInstallSnapshot(req raft.InstallSnapshotRequest) raft.InstallSnapshotResponse
}

// fakeNet routes RPCs between registered nodes, subject to the current cuts.
type fakeNet struct {
	mu       sync.RWMutex
	handlers map[string]rpcHandler // addr → receiver
	cut      map[string]bool       // "from→to" → dropped
	dropRate float64               // probability an otherwise-deliverable RPC is lost
	delay    time.Duration         // per-RPC delivery delay

	// blackholeDelay is how long a CUT link stalls before reporting failure. Zero
	// means fail immediately (a refused connection); non-zero models a peer that
	// accepts and never answers, so the caller pays its full RPC timeout.
	blackholeDelay time.Duration
	rng      *rand.Rand
}

func newFakeNet(seed int64) *fakeNet {
	return &fakeNet{
		handlers: make(map[string]rpcHandler),
		cut:      make(map[string]bool),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (n *fakeNet) register(addr string, h rpcHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[addr] = h
}

func linkKey(from, to string) string { return from + "→" + to }

// cutLink drops traffic from → to, in that direction only.
func (n *fakeNet) cutLink(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut[linkKey(from, to)] = true
}

func (n *fakeNet) healLink(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.cut, linkKey(from, to))
}

// isolate cuts every link in both directions between addr and the rest of the
// cluster. The node keeps running: it still ticks, still times out, still
// campaigns — it simply cannot be heard or hear. That is what makes it a useful
// model of a partition rather than of a crash.
func (n *fakeNet) isolate(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.handlers {
		if other == addr {
			continue
		}
		n.cut[linkKey(addr, other)] = true
		n.cut[linkKey(other, addr)] = true
	}
}

func (n *fakeNet) heal(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.handlers {
		delete(n.cut, linkKey(addr, other))
		delete(n.cut, linkKey(other, addr))
	}
}

func (n *fakeNet) healAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = make(map[string]bool)
}

// setDropRate makes a fraction of otherwise-deliverable RPCs vanish. Unlike a
// cut this is symmetric and random, modelling a lossy link rather than a split.
func (n *fakeNet) setDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// setBlackholeDelay makes cut links hang for d before failing, instead of
// failing at once.
func (n *fakeNet) setBlackholeDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blackholeDelay = d
}

func (n *fakeNet) setDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// route resolves the receiver for an RPC from → to, or an error describing why
// the call cannot be delivered. It also applies the configured delay, outside
// the lock, so a slow link does not serialize the whole network.
func (n *fakeNet) route(from, to string) (rpcHandler, time.Duration, error) {
	n.mu.Lock()
	if n.cut[linkKey(from, to)] {
		hang := n.blackholeDelay
		n.mu.Unlock()
		// A cut link fails immediately by default, which models a refused
		// connection. With blackholeDelay set it instead hangs first, modelling a
		// peer that accepts the connection and never answers — the failure that
		// actually costs a caller its RPC timeout, and the one that matters for
		// any code that waits on a peer rather than on a quorum.
		return nil, hang, fmt.Errorf("link %s is cut", linkKey(from, to))
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		n.mu.Unlock()
		return nil, 0, fmt.Errorf("packet from %s to %s dropped", from, to)
	}
	h, ok := n.handlers[to]
	delay := n.delay
	n.mu.Unlock()

	if !ok {
		return nil, 0, fmt.Errorf("no node listening at %s", to)
	}
	return h, delay, nil
}

// responseBlocked reports whether the reply leg to→from is cut. It is checked
// AFTER the handler has run, because a real partition that drops the response
// still let the request take effect on the receiver — the sender simply never
// finds out. Tests that assume otherwise would be testing a network that does
// not exist.
func (n *fakeNet) responseBlocked(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cut[linkKey(to, from)]
}

// nodeTransport is one node's view of the fake network: a raft.Transport that
// tags every outbound call with the sender's address so cuts can be directed.
type nodeTransport struct {
	net  *fakeNet
	from string
}

// deliver runs one RPC through the network: check the request leg, invoke the
// receiver, then check the response leg.
func deliver[Req any, Resp any](
	t *nodeTransport,
	to string,
	req Req,
	call func(rpcHandler, Req) Resp,
) (Resp, error) {
	var zero Resp

	h, delay, err := t.net.route(t.from, to)
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return zero, err
	}

	resp := call(h, req)

	// The request landed and took effect; if the reply cannot get back, the
	// caller must be told nothing at all.
	if t.net.responseBlocked(t.from, to) {
		return zero, fmt.Errorf("response from %s to %s is cut", to, t.from)
	}
	return resp, nil
}

func (t *nodeTransport) AppendEntries(addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return deliver(t, addr, req, func(h rpcHandler, r raft.AppendEntriesRequest) raft.AppendEntriesResponse {
		return h.HandleAppendEntries(r)
	})
}

func (t *nodeTransport) PreVote(addr string, req raft.PreVoteRequest) (raft.PreVoteResponse, error) {
	return deliver(t, addr, req, func(h rpcHandler, r raft.PreVoteRequest) raft.PreVoteResponse {
		return h.HandlePreVote(r)
	})
}

func (t *nodeTransport) RequestVote(addr string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return deliver(t, addr, req, func(h rpcHandler, r raft.RequestVoteRequest) raft.RequestVoteResponse {
		return h.HandleRequestVote(r)
	})
}

func (t *nodeTransport) InstallSnapshot(addr string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return deliver(t, addr, req, func(h rpcHandler, r raft.InstallSnapshotRequest) raft.InstallSnapshotResponse {
		return h.HandleInstallSnapshot(r)
	})
}

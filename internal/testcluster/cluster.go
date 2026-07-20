package testcluster

// Cluster runs a real Raft cluster inside one test process: real RaftNodes, real
// RaftLogs on real temp-directory WALs, real Engines with their control loops and
// per-peer replicators — everything except the sockets, which are fakeNet.
//
// The point is to test the code that ships. Nothing here reimplements consensus
// or stubs a receiver; the nodes are wired exactly as main.go wires them, so a
// bug in that wiring shows up here rather than hiding behind a test-only
// arrangement that happens to work.
//
// Every poll loop samples CheckSingleLeaderPerTerm. Election Safety is the
// invariant most likely to be broken by a change and least likely to be noticed
// by a test that was looking at something else, so the harness watches for it
// continuously rather than only where a test thinks to assert it.

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"crystal/internal/config"
	"crystal/internal/engine"
	"crystal/internal/raft"
	"crystal/internal/store"
)

// Node is one cluster member and everything it owns.
type Node struct {
	ID      int
	Addr    string
	Engine  *engine.Engine
	Raft    *raft.RaftNode
	Log     *raft.RaftLog
	Store   *store.MemoryStateMachine
	DataDir string

	done chan struct{}
}

// Cluster is a set of nodes over a shared fake network.
type Cluster struct {
	t     *testing.T
	Nodes map[int]*Node
	net   *fakeNet

	// termLeaders records which node was seen as leader in each term, so that a
	// second leader in the same term is caught the moment it is observed.
	mu          sync.Mutex
	termLeaders map[int]int
}

// Options tune a cluster for a particular test.
type Options struct {
	Size                int
	Seed                int64
	CompactionThreshold int // 0 = default
}

// New builds and starts a cluster of the given size. It registers cleanup, so a
// test never has to remember to stop it.
func New(t *testing.T, opts Options) *Cluster {
	t.Helper()
	if opts.Size <= 0 {
		opts.Size = 3
	}
	if opts.Seed == 0 {
		opts.Seed = 1
	}

	c := &Cluster{
		t:           t,
		Nodes:       make(map[int]*Node, opts.Size),
		net:         newFakeNet(opts.Seed),
		termLeaders: make(map[int]int),
	}

	addr := func(id int) string { return fmt.Sprintf("node-%d", id) }

	for id := 1; id <= opts.Size; id++ {
		peers := make(map[int]string, opts.Size-1)
		peerIDs := make([]int, 0, opts.Size-1)
		for other := 1; other <= opts.Size; other++ {
			if other == id {
				continue
			}
			peers[other] = addr(other)
			peerIDs = append(peerIDs, other)
		}

		dir := t.TempDir()
		cfg := &config.Config{
			NodeID:              id,
			DataDir:             dir,
			Peers:               peers,
			CompactionThreshold: opts.CompactionThreshold,
		}

		rl, err := raft.NewRaftLog(cfg.WALPath())
		if err != nil {
			t.Fatalf("node %d: NewRaftLog: %v", id, err)
		}
		rl.SetCompactionThreshold(opts.CompactionThreshold)

		rn, err := raft.NewRaftNode(id, peerIDs, opts.Size, cfg.MetadataPath(), raft.Follower)
		if err != nil {
			t.Fatalf("node %d: NewRaftNode: %v", id, err)
		}

		sm := store.NewMemoryStateMachine()
		snaps := store.NewSnapshotManager(cfg.SnapshotPath())
		tr := &nodeTransport{net: c.net, from: addr(id)}
		eng := engine.NewWithTransport(cfg, rn, rl, sm, snaps, tr)

		n := &Node{
			ID: id, Addr: addr(id), Engine: eng, Raft: rn, Log: rl,
			Store: sm, DataDir: dir, done: make(chan struct{}),
		}
		c.Nodes[id] = n
		c.net.register(n.Addr, eng)
	}

	for _, n := range c.Nodes {
		go n.Engine.Run(n.done)
	}

	t.Cleanup(c.Stop)
	return c
}

// Stop shuts every node's control loop down and closes its WAL.
func (c *Cluster) Stop() {
	for _, n := range c.Nodes {
		select {
		case <-n.done:
		default:
			close(n.done)
		}
	}
	// Give the control loops a moment to unwind before the temp dirs vanish.
	time.Sleep(50 * time.Millisecond)
	for _, n := range c.Nodes {
		n.Log.Close()
	}
}

// ---- Invariant checking ----

// CheckSingleLeaderPerTerm records the leaders currently observed and fails the
// test if two different nodes ever claim the same term (Election Safety, §5.2).
//
// It is called from inside every poll loop rather than at the end of a test,
// because a double leader is usually transient: by the time a test finishes
// asserting whatever it was actually interested in, the cluster has often
// resolved itself and the violation has vanished without trace.
func (c *Cluster) CheckSingleLeaderPerTerm() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, n := range c.Nodes {
		role, term := n.Raft.State()
		if role != raft.Leader {
			continue
		}
		if prev, seen := c.termLeaders[term]; seen && prev != n.ID {
			c.t.Fatalf("ELECTION SAFETY VIOLATED: nodes %d and %d both led term %d",
				prev, n.ID, term)
		}
		c.termLeaders[term] = n.ID
	}
}

// ---- Waiting helpers ----
//
// Every wait polls at a fine granularity and samples the safety invariant on each
// pass, so a violation is caught while it exists rather than after it heals.

const pollInterval = 5 * time.Millisecond

// WaitLeader blocks until exactly one node among candidates believes it is
// leader, and returns it.
func (c *Cluster) WaitLeader(timeout time.Duration) *Node {
	c.t.Helper()
	return c.WaitLeaderAmong(c.ids(), timeout)
}

// WaitLeaderAmong blocks until one of the given nodes is leader. Use it after a
// partition, where the majority side is expected to elect and the minority side
// must not.
func (c *Cluster) WaitLeaderAmong(ids []int, timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		c.CheckSingleLeaderPerTerm()

		var leaders []*Node
		for _, id := range ids {
			if n := c.Nodes[id]; n != nil && n.Raft.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(pollInterval)
	}

	c.dumpState()
	c.t.Fatalf("no single leader among %v within %s", ids, timeout)
	return nil
}

// WaitNoLeaderAmong asserts that no node in the set becomes leader for the whole
// duration. This is the minority-partition assertion: a node that cannot reach a
// quorum must never conclude it won.
func (c *Cluster) WaitNoLeaderAmong(ids []int, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.CheckSingleLeaderPerTerm()
		for _, id := range ids {
			if n := c.Nodes[id]; n != nil && n.Raft.IsLeader() {
				c.t.Fatalf("node %d became leader without a quorum", id)
			}
		}
		time.Sleep(pollInterval)
	}
}

// WaitApplied blocks until every listed node has key = value in its state
// machine, proving the write reached them through the log and was applied.
func (c *Cluster) WaitApplied(ids []int, key, value string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		c.CheckSingleLeaderPerTerm()
		all := true
		for _, id := range ids {
			got, ok := c.Nodes[id].Store.Get(key)
			if !ok || got != value {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(pollInterval)
	}

	c.dumpState()
	c.t.Fatalf("nodes %v did not converge on %s=%s within %s", ids, key, value, timeout)
}

// ---- Actions ----

// Set proposes a write through the current leader and waits for its result.
func (c *Cluster) Set(key, value string, timeout time.Duration) error {
	c.t.Helper()
	leader := c.WaitLeader(timeout)
	return c.SetVia(leader, key, value, timeout)
}

// SetVia proposes a write through a specific node, which lets a test aim a
// proposal at a node it knows has been deposed or partitioned.
func (c *Cluster) SetVia(n *Node, key, value string, timeout time.Duration) error {
	c.t.Helper()
	resultCh := make(chan error, 1)

	select {
	case n.Engine.ProposalQueue() <- engine.Proposal{
		Command:  raft.Command{Op: raft.OpSet, Key: key, Value: value},
		ResultCh: resultCh,
	}:
	case <-time.After(timeout):
		return fmt.Errorf("node %d: proposal queue full", n.ID)
	}

	select {
	case err := <-resultCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("node %d: proposal timed out", n.ID)
	}
}

// Read performs a linearizable read through n's engine, returning the value once
// leadership has been confirmed with a majority. It mirrors what the HTTP GET
// handler does: ask the engine to establish that a local read is safe, then read.
func (c *Cluster) Read(n *Node, key string, timeout time.Duration) (string, bool, error) {
	c.t.Helper()
	resultCh := make(chan error, 1)

	select {
	case n.Engine.ReadQueue() <- engine.Read{ResultCh: resultCh}:
	case <-time.After(timeout):
		return "", false, fmt.Errorf("node %d: read queue full", n.ID)
	}

	select {
	case err := <-resultCh:
		if err != nil {
			return "", false, err
		}
		v, ok := n.Store.Get(key)
		return v, ok, nil
	case <-time.After(timeout):
		return "", false, fmt.Errorf("node %d: read timed out", n.ID)
	}
}

// Isolate severs a node from the rest of the cluster in both directions. The
// node keeps running — it ticks, times out, and campaigns into the void.
func (c *Cluster) Isolate(id int) {
	c.net.isolate(c.Nodes[id].Addr)
}

// Heal restores a node's links.
func (c *Cluster) Heal(id int) {
	c.net.heal(c.Nodes[id].Addr)
}

// HealAll restores every link in the cluster.
func (c *Cluster) HealAll() { c.net.healAll() }

// Cut severs one direction only: from can no longer reach to, but to can still
// reach from. Asymmetric failures are where leader-election bugs live.
func (c *Cluster) Cut(from, to int) {
	c.net.cutLink(c.Nodes[from].Addr, c.Nodes[to].Addr)
}

// SetDropRate makes the given fraction of RPCs vanish at random.
func (c *Cluster) SetDropRate(p float64) { c.net.setDropRate(p) }

// SetDelay adds latency to every RPC.
func (c *Cluster) SetDelay(d time.Duration) { c.net.setDelay(d) }

// Others returns every node ID except the ones given.
func (c *Cluster) Others(exclude ...int) []int {
	skip := make(map[int]bool, len(exclude))
	for _, id := range exclude {
		skip[id] = true
	}
	var out []int
	for id := range c.Nodes {
		if !skip[id] {
			out = append(out, id)
		}
	}
	return out
}

func (c *Cluster) ids() []int {
	out := make([]int, 0, len(c.Nodes))
	for id := range c.Nodes {
		out = append(out, id)
	}
	return out
}

// dumpState prints per-node state on failure. A Raft test that fails without
// this is nearly unreadable — the useful information is which node thought what,
// and it is gone by the time the assertion reports.
func (c *Cluster) dumpState() {
	c.t.Helper()
	c.t.Logf("---- cluster state ----")
	for id := 1; id <= len(c.Nodes); id++ {
		n, ok := c.Nodes[id]
		if !ok {
			continue
		}
		role, term := n.Raft.State()
		commit, applied := n.Raft.CommitAndApplyBoundary()
		c.t.Logf("node %d: role=%s term=%d commit=%d applied=%d lastLog=%d",
			id, role, term, commit, applied, n.Log.LatestIndex())
	}
	c.t.Logf("dir: %s", filepath.Dir(c.Nodes[1].DataDir))
}

// SetBlackholeDelay makes cut links hang for d before reporting failure, rather
// than failing at once. A refused connection and a black hole are different
// failures: the first costs the caller nothing, the second costs it a full RPC
// timeout. Code that waits on a peer instead of on a quorum only misbehaves
// against the second.
func (c *Cluster) SetBlackholeDelay(d time.Duration) { c.net.setBlackholeDelay(d) }

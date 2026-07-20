//go:build integration

// Package integration drives a real multi-node CrystalDB cluster over HTTP to
// verify the end-to-end Raft behaviors that unit tests cannot: leader election,
// automatic failover, committed-data survival across a leader change, and
// convergence of a follower that missed entries while down.
//
// Run with:  go test -tags integration ./internal/integration/ -v
//
// These tests build the crystal binary and launch OS processes, so they are
// slower and gated behind the `integration` build tag to keep the default
// `go test ./...` fast and hermetic.
package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type node struct {
	id                  int
	port                int
	cmd                 *exec.Cmd
	dir                 string
	compactionThreshold int // 0 = default
}

func (n *node) addr() string { return fmt.Sprintf("localhost:%d", n.port) }

// buildBinary compiles the crystal command once into a temp path.
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "crystal")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	// The module root is two levels up from internal/integration.
	build := exec.Command("go", "build", "-o", bin, "crystal/cmd/crystal")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build crystal: %v", err)
	}
	return bin
}

// startCluster launches n nodes wired as peers of each other and returns them.
func startCluster(t *testing.T, bin string, n int) []*node {
	return startClusterWithCompaction(t, bin, n, 0)
}

// startClusterWithCompaction is startCluster with an explicit compaction
// threshold (0 = default), used to force snapshotting in tests.
func startClusterWithCompaction(t *testing.T, bin string, n, compaction int) []*node {
	t.Helper()
	basePort := 18080
	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &node{id: i + 1, port: basePort + i, dir: t.TempDir(), compactionThreshold: compaction}
	}

	for _, self := range nodes {
		var peers []string
		for _, other := range nodes {
			if other.id != self.id {
				peers = append(peers, fmt.Sprintf("%d:localhost:%d", other.id, other.port))
			}
		}
		self.cmd = launch(t, bin, self, peers)
	}

	t.Cleanup(func() {
		for _, nd := range nodes {
			stop(nd)
		}
	})
	return nodes
}

func launch(t *testing.T, bin string, nd *node, peers []string) *exec.Cmd {
	t.Helper()
	peerArg := ""
	for i, p := range peers {
		if i > 0 {
			peerArg += ","
		}
		peerArg += p
	}
	args := []string{
		fmt.Sprintf("-id=%d", nd.id),
		fmt.Sprintf("-port=%d", nd.port),
		fmt.Sprintf("-data-dir=%s", nd.dir),
		fmt.Sprintf("-peers=%s", peerArg),
	}
	if nd.compactionThreshold > 0 {
		args = append(args, fmt.Sprintf("-compaction-threshold=%d", nd.compactionThreshold))
	}
	cmd := exec.Command(bin, args...)
	// Surface node logs on failure for debugging.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start node %d: %v", nd.id, err)
	}
	return cmd
}

func stop(nd *node) {
	if nd.cmd != nil && nd.cmd.Process != nil {
		_ = nd.cmd.Process.Kill()
		_, _ = nd.cmd.Process.Wait()
	}
}

// ---- HTTP helpers ----

func trySet(addr, key, value string) (int, error) {
	body := fmt.Sprintf(`{"key":%q,"value":%q}`, key, value)
	resp, err := http.Post("http://"+addr+"/set", "application/json", bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// tryGet reads a key with ?consistency=stale.
//
// These tests poll individual nodes to watch replication converge, which is
// exactly what a stale read is for: they are asking "has this entry reached THIS
// node yet", a question about one replica's local state. A linearizable read
// answers a different question — "what is the cluster's current value" — and
// followers correctly refuse it, so using the default here would test the read
// contract rather than replication.
//
// tryGetLinearizable covers the default contract separately.
func tryGet(addr, key string) (string, int, error) {
	return doGet(addr, key, "stale")
}

// tryGetLinearizable uses the default read path: leader-only, quorum-confirmed.
func tryGetLinearizable(addr, key string) (string, int, error) {
	return doGet(addr, key, "linearizable")
}

func doGet(addr, key, consistency string) (string, int, error) {
	resp, err := http.Get("http://" + addr + "/get?key=" + key + "&consistency=" + consistency)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return buf.String(), resp.StatusCode, nil
}

// findLeader probes every live node until one accepts a write, returning it.
// It retries for up to timeout to allow an election to settle.
func findLeader(t *testing.T, nodes []*node, key, value string, timeout time.Duration) *node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, nd := range nodes {
			if nd.cmd == nil {
				continue
			}
			code, err := trySet(nd.addr(), key, value)
			if err == nil && code == http.StatusOK {
				return nd
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no leader accepted a write within %s", timeout)
	return nil
}

// waitConverged blocks until every live node reports key=value on a stale read.
func waitConverged(t *testing.T, nodes []*node, key, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, nd := range nodes {
			if nd.cmd == nil {
				continue
			}
			body, code, err := tryGet(nd.addr(), key)
			if err != nil || code != http.StatusOK || !bytes.Contains([]byte(body), []byte(value)) {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %s=%s within %s", key, value, timeout)
}

// ---- Tests ----

func TestClusterElectsLeaderAndReplicates(t *testing.T) {
	bin := buildBinary(t)
	nodes := startCluster(t, bin, 3)

	leader := findLeader(t, nodes, "k1", "v1", 8*time.Second)
	t.Logf("elected leader: node %d", leader.id)

	// Give replication a moment, then confirm every node has the value.
	time.Sleep(500 * time.Millisecond)
	for _, nd := range nodes {
		body, code, err := tryGet(nd.addr(), "k1")
		if err != nil || code != http.StatusOK {
			t.Fatalf("node %d get k1: code=%d err=%v", nd.id, code, err)
		}
		if !bytes.Contains([]byte(body), []byte("v1")) {
			t.Fatalf("node %d missing replicated value: %q", nd.id, body)
		}
	}
}

func TestLeaderFailoverPreservesData(t *testing.T) {
	bin := buildBinary(t)
	nodes := startCluster(t, bin, 3)

	leader := findLeader(t, nodes, "before", "yes", 8*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Kill the leader.
	t.Logf("killing leader node %d", leader.id)
	stop(leader)
	leader.cmd = nil

	// A new leader must emerge among the survivors and accept a write.
	survivors := make([]*node, 0, 2)
	for _, nd := range nodes {
		if nd.cmd != nil {
			survivors = append(survivors, nd)
		}
	}
	newLeader := findLeader(t, survivors, "after", "ok", 8*time.Second)
	t.Logf("new leader: node %d", newLeader.id)
	if newLeader.id == leader.id {
		t.Fatalf("killed leader somehow still leading")
	}

	time.Sleep(500 * time.Millisecond)

	// Both the pre-failover and post-failover data must be present on survivors.
	for _, nd := range survivors {
		if body, _, _ := tryGet(nd.addr(), "before"); !bytes.Contains([]byte(body), []byte("yes")) {
			t.Fatalf("node %d lost pre-failover committed data: %q", nd.id, body)
		}
		if body, _, _ := tryGet(nd.addr(), "after"); !bytes.Contains([]byte(body), []byte("ok")) {
			t.Fatalf("node %d missing post-failover data: %q", nd.id, body)
		}
	}
}

func TestRejoiningFollowerConverges(t *testing.T) {
	bin := buildBinary(t)
	nodes := startCluster(t, bin, 3)

	findLeader(t, nodes, "seed", "0", 8*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Take one follower down. Pick a node that is not currently the leader.
	victim := nodes[2]
	if code, _ := trySet(victim.addr(), "probe", "x"); code == http.StatusOK {
		// victim happened to be leader; use a different node.
		victim = nodes[1]
	}
	t.Logf("taking node %d down", victim.id)
	stop(victim)
	victim.cmd = nil

	// Write several entries while it is down (quorum of the other two holds).
	live := []*node{}
	for _, nd := range nodes {
		if nd.cmd != nil {
			live = append(live, nd)
		}
	}
	leader := findLeader(t, live, "w1", "1", 8*time.Second)
	trySet(leader.addr(), "w2", "2")
	trySet(leader.addr(), "w3", "3")
	time.Sleep(300 * time.Millisecond)

	// Restart the victim; it must catch up via nextIndex-driven replication.
	t.Logf("restarting node %d", victim.id)
	var peers []string
	for _, other := range nodes {
		if other.id != victim.id {
			peers = append(peers, fmt.Sprintf("%d:localhost:%d", other.id, other.port))
		}
	}
	victim.cmd = launch(t, bin, victim, peers)

	// Poll the victim until it has all three entries or we time out.
	deadline := time.Now().Add(8 * time.Second)
	for {
		body, _, err := tryGet(victim.addr(), "w3")
		if err == nil && bytes.Contains([]byte(body), []byte("3")) {
			break // converged
		}
		if time.Now().After(deadline) {
			t.Fatalf("rejoining node %d did not converge to w3 in time (last: %q)", victim.id, body)
		}
		time.Sleep(150 * time.Millisecond)
	}
	// And the earlier entries too.
	for k, v := range map[string]string{"w1": "1", "w2": "2"} {
		if body, _, _ := tryGet(victim.addr(), k); !bytes.Contains([]byte(body), []byte(v)) {
			t.Fatalf("rejoined node missing %s=%s: %q", k, v, body)
		}
	}
}

// TestFollowerCatchesUpViaSnapshot is the Phase 2 payoff: a follower falls
// behind, the leader COMPACTS past the follower's nextIndex (so the entries it
// needs are gone from the log), and the follower must catch up via an
// InstallSnapshot RPC rather than log shipping. Compaction threshold is set low
// so a handful of writes triggers it.
func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	bin := buildBinary(t)
	nodes := startClusterWithCompaction(t, bin, 3, 5)

	// Establish a leader and seed one committed entry everywhere.
	findLeader(t, nodes, "seed", "0", 8*time.Second)
	time.Sleep(500 * time.Millisecond)

	// Take a non-leader node down.
	victim := nodes[2]
	if code, _ := trySet(victim.addr(), "probe", "x"); code == http.StatusOK {
		victim = nodes[1]
	}
	t.Logf("taking node %d down", victim.id)
	stop(victim)
	victim.cmd = nil

	live := []*node{}
	for _, nd := range nodes {
		if nd.cmd != nil {
			live = append(live, nd)
		}
	}
	leader := findLeader(t, live, "k0", "v0", 8*time.Second)

	// Write well past the compaction threshold so the leader snapshots and
	// discards the entries the down follower still needs.
	const n = 20
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if code, err := trySet(leader.addr(), key, val); err != nil || code != http.StatusOK {
			leader = findLeader(t, live, key, val, 5*time.Second)
		}
	}
	time.Sleep(1 * time.Second) // let compaction run on the tick

	// Restart the victim. Its nextIndex (~1) is now below the leader's compacted
	// FirstIndex, so the leader must ship a snapshot to catch it up.
	t.Logf("restarting node %d (must catch up via InstallSnapshot)", victim.id)
	var peers []string
	for _, other := range nodes {
		if other.id != victim.id {
			peers = append(peers, fmt.Sprintf("%d:localhost:%d", other.id, other.port))
		}
	}
	victim.compactionThreshold = 5
	victim.cmd = launch(t, bin, victim, peers)

	// The victim must eventually serve the latest key.
	lastKey := fmt.Sprintf("k%d", n)
	lastVal := fmt.Sprintf("v%d", n)
	deadline := time.Now().Add(10 * time.Second)
	for {
		body, _, err := tryGet(victim.addr(), lastKey)
		if err == nil && bytes.Contains([]byte(body), []byte(lastVal)) {
			break // caught up via snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower %d did not catch up via snapshot (last %s: %q)", victim.id, lastKey, body)
		}
		time.Sleep(150 * time.Millisecond)
	}

	// An earlier key that was compacted away on the leader must also be present
	// on the follower — proving it came from the snapshot, not log shipping.
	if body, _, _ := tryGet(victim.addr(), "k1"); !bytes.Contains([]byte(body), []byte("v1")) {
		t.Fatalf("follower missing compacted-then-snapshotted key k1: %q", body)
	}
}

// TestLinearizableReadContract exercises the default /get path over real HTTP:
// the leader answers only after confirming leadership with a majority, and every
// follower refuses with a redirect rather than serving its own local state.
//
// The harness proves the mechanism; this proves the wiring — that the read
// actually travels transport -> engine -> quorum confirmation and back, and that
// the refusal reaches the client as a usable HTTP response.
func TestLinearizableReadContract(t *testing.T) {
	bin := buildBinary(t)
	nodes := startCluster(t, bin, 3)

	leader := findLeader(t, nodes, "lin", "yes", 8*time.Second)
	t.Logf("elected leader: node %d", leader.id)

	// Wait for the write to reach every node before asserting anything about
	// reads; a follower that has not applied it yet would 404 a stale read for
	// reasons unrelated to the read contract.
	waitConverged(t, nodes, "lin", "yes", 8*time.Second)

	// The leader serves the linearizable read.
	body, code, err := tryGetLinearizable(leader.addr(), "lin")
	if err != nil {
		t.Fatalf("linearizable read on leader: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("linearizable read on leader: code=%d body=%q", code, body)
	}
	if !bytes.Contains([]byte(body), []byte("yes")) {
		t.Fatalf("leader returned %q, want the committed value", body)
	}

	// Followers refuse it, and say where to go instead.
	for _, nd := range nodes {
		if nd.id == leader.id {
			continue
		}
		body, code, err := tryGetLinearizable(nd.addr(), "lin")
		if err != nil {
			t.Fatalf("node %d: %v", nd.id, err)
		}
		if code != http.StatusMisdirectedRequest {
			t.Fatalf("follower %d answered a linearizable read: code=%d body=%q — "+
				"only the leader can establish that a read is current",
				nd.id, code, body)
		}
		if !bytes.Contains([]byte(body), []byte("leader")) {
			t.Fatalf("follower %d refused without naming the leader: %q", nd.id, body)
		}

		// The same node still answers a stale read: opting out is explicit.
		if _, code, _ := tryGet(nd.addr(), "lin"); code != http.StatusOK {
			t.Fatalf("follower %d refused an explicit stale read: code=%d", nd.id, code)
		}
	}
}

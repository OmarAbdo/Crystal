package config

import "testing"

// M1: the majority threshold is derived from len(peers)+1, so a peers list that
// includes this node makes its arithmetic differ from every other node's. Two
// nodes can then both believe they hold a quorum, and the failure is silent —
// the cluster starts and runs, incorrectly.
func TestValidate_RejectsSelfInPeers(t *testing.T) {
	cfg := &Config{
		NodeID:  2,
		DataDir: "data",
		Peers:   map[int]string{1: "localhost:9001", 2: "localhost:9002"},
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("accepted a peers list containing the node's own ID")
	}
}

func TestValidate_AcceptsPeersWithoutSelf(t *testing.T) {
	cfg := &Config{
		NodeID:  2,
		DataDir: "data",
		Peers:   map[int]string{1: "localhost:9001", 3: "localhost:9003"},
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("rejected a valid peers list: %v", err)
	}
}

func TestParsePeers(t *testing.T) {
	peers := map[int]string{}
	if err := parsePeers("1:localhost:9001,3:localhost:9003", peers); err != nil {
		t.Fatalf("parsePeers: %v", err)
	}
	if len(peers) != 2 || peers[1] != "localhost:9001" || peers[3] != "localhost:9003" {
		t.Fatalf("parsed %v", peers)
	}
}

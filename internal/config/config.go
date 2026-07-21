package config

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Config holds all runtime configuration for a CrystalDB node.
// All file paths are derived from DataDir to keep data for each node isolated.
type Config struct {
	NodeID   int
	Port     string    // e.g. ":8080"
	DataDir  string    // directory for WAL, metadata, snapshot files
	Peers    map[int]string // peerID → "host:port"

	// CompactionThreshold is the cache size that triggers a snapshot + WAL
	// truncation. 0 means "use the RaftLog default" (1000). Exposed mainly so
	// integration tests can force compaction with a small log.
	CompactionThreshold int

	// SessionTTL is how long an unused client session survives before the state
	// machine reclaims it (F23). 0 means the store default.
	//
	// EVERY NODE MUST BE CONFIGURED IDENTICALLY. This value is part of the
	// replicated decision procedure: nodes that disagree about when a session
	// expires will apply the same log and reach different states.
	SessionTTL time.Duration
}

// SelfAddr returns this node's own address as peers would reach it. The port is
// authoritative; the host is a placeholder for the bootstrap configuration,
// which only needs self to be present as a voter — nobody dials themselves.
func (c *Config) SelfAddr() string {
	return "localhost" + c.Port
}

// WALPath returns the full path to this node's WAL file.
func (c *Config) WALPath() string {
	return filepath.Join(c.DataDir, "raft.wal")
}

// MetadataPath returns the path to the persistent Raft state file (term/votedFor).
func (c *Config) MetadataPath() string {
	return filepath.Join(c.DataDir, "raft.meta")
}

// SnapshotPath returns the path to the latest snapshot file.
func (c *Config) SnapshotPath() string {
	return filepath.Join(c.DataDir, "snapshot.json")
}

// ParseFlags parses command-line flags and returns a validated Config.
func ParseFlags() (*Config, error) {
	idFlag := flag.Int("id", 1, "Unique integer ID for this node (must be unique in cluster)")
	portFlag := flag.String("port", "8080", "TCP port for this node to listen on")
	dataDirFlag := flag.String("data-dir", "data", "Directory for WAL, metadata, and snapshot files")
	peersFlag := flag.String("peers", "", "Comma-separated peer list: id:host:port,id:host:port")
	compactionFlag := flag.Int("compaction-threshold", 0, "Log size that triggers compaction (0 = default 1000)")
	sessionTTLFlag := flag.Duration("session-ttl", 0,
		"How long an unused client session survives (0 = default 1h). Must be identical on every node.")

	flag.Parse()

	cfg := &Config{
		NodeID:              *idFlag,
		Port:                ":" + *portFlag,
		DataDir:             *dataDirFlag,
		Peers:               make(map[int]string),
		CompactionThreshold: *compactionFlag,
		SessionTTL:          *sessionTTLFlag,
	}

	if err := parsePeers(*peersFlag, cfg.Peers); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// parsePeers parses "1:localhost:9001,2:localhost:9002" into the map.
func parsePeers(raw string, peers map[int]string) error {
	if raw == "" {
		return nil
	}

	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid peer entry %q: expected id:host:port", entry)
		}

		var peerID int
		if _, err := fmt.Sscanf(parts[0], "%d", &peerID); err != nil {
			return fmt.Errorf("invalid peer ID in %q: %w", entry, err)
		}

		peers[peerID] = parts[1]
	}

	return nil
}

func (c *Config) validate() error {
	if c.NodeID <= 0 {
		return fmt.Errorf("node ID must be a positive integer, got %d", c.NodeID)
	}
	if c.DataDir == "" {
		return fmt.Errorf("data-dir must not be empty")
	}
	// A node listed among its own peers inflates clusterSize on that node alone,
	// so it computes a different majority from everyone else. Two nodes could
	// then both believe they hold a quorum. This is a typo in a launch script,
	// and it is silent — the cluster runs, incorrectly.
	if addr, ok := c.Peers[c.NodeID]; ok {
		return fmt.Errorf("node %d is listed in its own -peers as %q: peers must "+
			"name only the OTHER cluster members", c.NodeID, addr)
	}
	return nil
}
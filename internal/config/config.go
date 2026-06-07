package config

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
)

// Config holds all runtime configuration for a CrystalDB node.
// All file paths are derived from DataDir to keep data for each node isolated.
type Config struct {
	NodeID   int
	Port     string    // e.g. ":8080"
	DataDir  string    // directory for WAL, metadata, snapshot files
	Peers    map[int]string // peerID → "host:port"
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

	flag.Parse()

	cfg := &Config{
		NodeID:  *idFlag,
		Port:    ":" + *portFlag,
		DataDir: *dataDirFlag,
		Peers:   make(map[int]string),
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
	return nil
}
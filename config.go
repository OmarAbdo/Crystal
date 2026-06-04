package main

import (
	"flag"
	"strings"
	"fmt"
)

// Config holds the cluster settings for this specific node
type Config struct {
	NodeID int
	Port   string
	Peers  map[int]string // Maps a Node ID -> its network address (e.g., 2 -> "localhost:8002")
}

// ParseFlags reads command-line inputs to configure our node
func ParseFlags() *Config {
	idFlag := flag.Int("id", 1, "Unique ID for this node")
	portFlag := flag.String("port", "8080", "Port for this node to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer ID:addr (e.g., 2:localhost:8002,3:localhost:8003)")

	flag.Parse()

	peerMap := make(map[int]string)
	if *peersFlag != "" {
		// Split peers by comma
		pairs := strings.Split(*peersFlag, ",")
		for _, pair := range pairs {
			// Split each pair by colon (e.g., "2:localhost:8002")
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				var peerID int
				// Simple parsing logic: convert string ID to int
				_, err := fmt.Sscanf(parts[0], "%d", &peerID)
				if err == nil {
					peerMap[peerID] = parts[1]
				}
			}
		}
	}

	return &Config{
		NodeID: *idFlag,
		Port:   ":" + *portFlag,
		Peers:  peerMap,
	}
}
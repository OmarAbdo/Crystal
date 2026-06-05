package main

// Modified to capture a dedicated directory flag for storing the node's individual raw WAL binary file.

import (
	"flag"
	"fmt"
	"strings"
)

type Config struct {
	NodeID  int
	Port    string
	WALPath string
	Peers   map[int]string
}

func ParseFlags() *Config {
	idFlag := flag.Int("id", 1, "Unique ID for this node")
	portFlag := flag.String("port", "8080", "Port for this node to listen on")
	walFlag := flag.String("wal", "crystal.wal", "File path for the write-ahead log file")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer ID:addr")

	flag.Parse()

	peerMap := make(map[int]string)
	if *peersFlag != "" {
		pairs := strings.Split(*peersFlag, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				var peerID int
				_, err := fmt.Sscanf(parts[0], "%d", &peerID)
				if err == nil {
					peerMap[peerID] = parts[1]
				}
			}
		}
	}

	return &Config{
		NodeID:  *idFlag,
		Port:    ":" + *portFlag,
		WALPath: *walFlag,
		Peers:   peerMap,
	}
}

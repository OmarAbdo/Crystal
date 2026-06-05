package main

// This ties the subsystems together and launches a dedicated background engine loop.
// This background loop explicitly controls proposal validation, disk persistence, peer network execution, and linear state execution.

import (
	"log"
	"net/http"
	"time"
)

func main() {
	cfg := ParseFlags()

	store, err := NewCrystalStore(cfg.WALPath)
	if err != nil {
		log.Fatalf("Fatal: Engine could not start storage: %v", err)
	}
	defer store.Close()

	var peerIDs []int
	for pid := range cfg.Peers {
		peerIDs = append(peerIDs, pid)
	}

	initialRole := Follower
	if cfg.NodeID == 1 {
		initialRole = Leader
	}

	raft := NewRaftNode(initialRole, peerIDs)

	// Central background consensus orchestration engine
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case prop := <-ProposalQueue:
				if !raft.IsLeader() {
					prop.ResultCh <- false
					continue
				}

				_, term := raft.GetState()
				// Step 1: Write directly to local disk WAL immediately
				entry, err := store.AppendToWAL(prop.Key, prop.Value, term)
				if err != nil {
					prop.ResultCh <- false
					continue
				}

				// Step 2: Trigger network replication outbound jobs
				for pID, pAddr := range cfg.Peers {
					go raft.ReplicateToPeer(pID, pAddr, entry)
				}

				// Step 3: Monitor state changes until committed or timed out
				committed := false
				timeout := time.After(1500 * time.Millisecond)
			WaitLoop:
				for {
					raft.CheckQuorumAndCommit(store)
					raft.mu.RLock()
					if raft.CommitIndex >= entry.Index {
						committed = true
					}
					raft.mu.RUnlock()

					if committed {
						break WaitLoop
					}

					select {
					case <-timeout:
						break WaitLoop
					case <-ticker.C:
						// Keep evaluating quorum on ticks
					}
				}

				if committed {
					// Step 4: Advance execution parameters and apply log to state machine
					raft.mu.Lock()
					for raft.CommitIndex > raft.LastApplied {
						raft.LastApplied++
						if e, ok := store.GetEntry(raft.LastApplied); ok {
							store.ApplyToStateMachine(e)
						}
					}
					raft.mu.Unlock()
					prop.ResultCh <- true
				} else {
					prop.ResultCh <- false
				}

			case <-ticker.C:
				// Regular background tasks (e.g., Follower state verification)
				if !raft.IsLeader() {
					raft.mu.Lock()
					// Followers evaluate if they can safely apply logs to state machine based on external rules
					// Once a heartbeat system is added, followers use this to advance LastApplied safely
					raft.mu.Unlock()
				}
			}
		}
	}()

	http.HandleFunc("/set", HandleSet(raft))
	http.HandleFunc("/get", HandleGet(store))
	http.HandleFunc("/internal/append", HandleInternalAppend(store))

	log.Printf("[SERVER] Crystal Node %d listening on %s as %s", cfg.NodeID, cfg.Port, initialRole)
	if err := http.ListenAndServe(cfg.Port, nil); err != nil {
		log.Fatalf("Server startup failed: %v", err)
	}
}

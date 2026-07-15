# Crystal — a distributed key-value store built on Raft

<p align="center">
  <img src="logo.png" alt="Crystal Logo" width="200">
</p>

Crystal is a small, readable, **paper-faithful** implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go, wrapped
around a replicated key-value store. It is written to be *read*: every
consensus decision is traceable to a section of Ongaro & Ousterhout's
_"In Search of an Understandable Consensus Algorithm"_ (the paper lives in the
repo root), and the code is layered so each concern sits in one place.

> _"Raft separates leader election, log replication, and safety … and it
> reduces the degree of nondeterminism and the ways servers can be inconsistent
> with each other."_ — Raft paper, §1

If you want the guided tour, start with **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**
(how the pieces fit) and **[docs/RAFT.md](docs/RAFT.md)** (how the code maps to
the paper, with the safety intuition spelled out).

---

## What it does

A Crystal cluster is a set of nodes that agree on an ordered log of write
commands and apply them to identical copies of a key-value map. Clients `set`,
`get`, and `delete` keys; the cluster keeps the data consistent and available as
long as a majority of nodes are up — including across leader crashes.

### Implemented (feature-complete against Figure 2 + §5 + §7)

| Area | What's there |
|------|--------------|
| **Leader election** | Randomized election timeouts (§5.2); no hardcoded leader; step-down on higher term everywhere |
| **Log replication** | AppendEntries with `prevLogIndex/prevLogTerm` matching, batched entries, and §5.3 conflict-hint backtracking |
| **Safety** | Election restriction (§5.4.1), the Figure 8 commit rule (§5.4.2), one-vote-per-term, persist-before-respond |
| **Durability** | Framed write-ahead log, fsync on append, crash recovery with partial-frame truncation |
| **Log compaction** | State-machine snapshots + WAL truncation, and the InstallSnapshot RPC (Figure 13) to catch up lagging followers |
| **Concurrency** | etcd-style single-writer control loop + one replication goroutine per peer; non-blocking client proposals |
| **Persistence** | `currentTerm` / `votedFor` survive restarts (losing them breaks safety) |
| **Tests** | Unit tests for the tricky invariants + an integration suite that drives a real 3-node cluster over HTTP |

### Not yet built

- **Storage engine.** The state machine is an in-memory `map[string]string`. The
  `StateMachine` interface is the seam for a future LSM-tree backend — see the
  roadmap below.
- **Dynamic membership** (§6 joint consensus) — the cluster set is fixed at startup.
- **Pre-vote / leadership transfer** — a rejoining node can still force a term bump.
- **Service discovery** — peers are passed on the command line.

---

## Quick start

**Prerequisites:** Go 1.24+.

```bash
git clone https://github.com/OmarAbdo/Crystal.git
cd Crystal
go build ./...
```

Run a three-node cluster locally, each node in its own terminal with its own
data directory:

```bash
# node 1
go run ./cmd/crystal -id=1 -port=8001 -data-dir=data/n1 \
  -peers="2:localhost:8002,3:localhost:8003"

# node 2
go run ./cmd/crystal -id=2 -port=8002 -data-dir=data/n2 \
  -peers="1:localhost:8001,3:localhost:8003"

# node 3
go run ./cmd/crystal -id=3 -port=8003 -data-dir=data/n3 \
  -peers="1:localhost:8001,2:localhost:8002"
```

Within a few hundred milliseconds one node wins an election and becomes leader.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-id` | `1` | Unique integer node ID (must be unique in the cluster) |
| `-port` | `8080` | TCP port to listen on |
| `-data-dir` | `data` | Directory for this node's WAL, metadata, and snapshot |
| `-peers` | `""` | Comma-separated `id:host:port` list of the *other* nodes |
| `-compaction-threshold` | `0` (→ 1000) | Cached-entry count that triggers a snapshot + WAL truncation |

### Talking to the cluster

Writes must go to the **leader**; a follower replies `421 Misdirected Request`
telling you which node to route to.

```bash
# set a key (on the leader)
curl -i -X POST -H "Content-Type: application/json" \
  -d '{"key":"color","value":"crystal"}' http://localhost:8001/set

# read it back (any node serves reads)
curl -i "http://localhost:8001/get?key=color"

# delete it
curl -i -X POST -H "Content-Type: application/json" \
  -d '{"key":"color"}' http://localhost:8001/delete
```

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/set` | POST | Set a key (leader only) |
| `/get` | GET | Read a key (any node) |
| `/delete` | POST | Delete a key (leader only) |
| `/internal/append` | POST | AppendEntries RPC receiver (node-to-node) |
| `/internal/vote` | POST | RequestVote RPC receiver (node-to-node) |
| `/internal/snapshot` | POST | InstallSnapshot RPC receiver (node-to-node) |

---

## Layout

```
cmd/crystal/main.go        composition root — wires everything, no logic
internal/
  config/                  flag parsing, per-node path derivation
  raft/
    node.go                consensus state: terms, votes, elections, commit rule
    log.go                 durable framed WAL + in-memory cache + compaction
    replicator.go          outbound AppendEntries/RequestVote/InstallSnapshot
    types.go               LogEntry, Command, RPC request/response shapes
  engine/
    engine.go              single-writer control loop (commit/apply/elect)
    replicator_loop.go     one long-lived replication goroutine per peer
    errors.go              ErrNotLeader / ErrCommitTimeout
  store/
    statemachine.go        StateMachine interface + in-memory implementation
    snapshot.go            durable snapshot read/write
  transport/http.go        thin HTTP handlers, delegate to engine/raft
  integration/             real-cluster tests (build tag: integration)
docs/                      architecture + Raft-mapping walkthroughs
```

The dependency arrow is one-directional: `store` imports `raft`, never the
reverse. `raft` knows nothing about HTTP or key-value semantics; it moves opaque
`[]byte` commands. That boundary is what lets the storage backend change without
touching consensus.

---

## Testing

```bash
go test ./...                                    # fast, hermetic unit tests
go test -tags integration ./internal/integration -v   # spins up a real 3-node cluster
```

The integration suite builds the binary, launches OS processes, elects a leader,
kills it, and asserts re-election, committed-data survival, follower
convergence, and snapshot-based catch-up. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#testing-strategy) for the
philosophy (unit tests pin invariants; integration tests prove the whole thing
actually agrees).

> **Note on `-race`:** the race detector needs cgo, which isn't wired up on the
> primary dev box (Windows, no C toolchain). Run `-race` in Linux CI if you add it.

---

## Roadmap

Crystal was built consensus-first: get agreement provably right, *then* make the
thing it agrees about interesting.

- [x] **Consensus core** — elections, replication, safety (Figure 2, §5)
- [x] **Durability** — framed WAL, fsync, crash recovery
- [x] **Compaction** — snapshots + InstallSnapshot (§7)
- [x] **Concurrency** — single-writer loop + per-peer replicators
- [ ] **Storage engine** — swap the in-memory map for an LSM-tree
      (MemTable → SSTables, background compaction) behind the same
      `StateMachine` interface
- [ ] **Dynamic membership** — §6 joint consensus
- [ ] **Pre-vote & leadership transfer** — avoid needless term bumps
- [ ] **Service discovery** — stop hand-writing the peer list

---

## License

MIT. See [LICENSE](LICENSE).

## Contact

- Repository: <https://github.com/OmarAbdo/Crystal>
- Issues: <https://github.com/OmarAbdo/Crystal/issues>

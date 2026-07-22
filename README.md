# Crystal — a distributed key-value store built on Raft

<p align="center">
  <img src="logo.png" alt="Crystal Logo" width="200">
</p>

Crystal is a small, readable, **paper-faithful** implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go, wrapped
around a replicated key-value store. It is written to be _read_: every
consensus decision is traceable to a section of Ongaro & Ousterhout's
_"In Search of an Understandable Consensus Algorithm"_ (the paper lives in the
repo root), and the code is layered so each concern sits in one place.

> _"Raft separates leader election, log replication, and safety … and it
> reduces the degree of nondeterminism and the ways servers can be inconsistent
> with each other."_ — Raft paper, §1

If you want the guided tour, start with **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**
(how the pieces fit) and **[docs/RAFT.md](docs/RAFT.md)** (how the code maps to
the paper, with the safety intuition spelled out). For the runtime — the control
loop, the goroutines, and every channel between them — see
**[docs/CONCURRENCY.md](docs/CONCURRENCY.md)**.

---

## What it does

A Crystal cluster is a set of nodes that agree on an ordered log of write
commands and apply them to identical copies of a key-value map. Clients `set`,
`get`, and `delete` keys; the cluster keeps the data consistent and available as
long as a majority of nodes are up — including across leader crashes,
partitions, and restarts, without losing an acknowledged write.

### Implemented

| Area                 | What's there                                                                                                                               |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Leader election**  | Randomized election timeouts (§5.2); no hardcoded leader; step-down on higher term everywhere                                              |
| **Log replication**  | AppendEntries with `prevLogIndex/prevLogTerm` matching, batched entries, and §5.3 conflict-hint backtracking                               |
| **Safety**           | Election restriction (§5.4.1), the Figure 8 commit rule (§5.4.2), one-vote-per-term, persist-before-respond                                |
| **Durability**       | Framed write-ahead log, fsync on append, crash recovery with partial-frame truncation                                                      |
| **Log compaction**   | State-machine snapshots + WAL truncation, and the InstallSnapshot RPC (Figure 13) to catch up lagging followers                            |
| **Linearizability**  | Linearizable reads served by _any_ node via ReadIndex, plus an enforced bounded-staleness tier (§8)                                        |
| **Client semantics** | Exactly-once writes through client sessions, with expiry driven by a replicated clock (§8)                                                 |
| **Leader stability** | Pre-vote, CheckQuorum, and the §6 stickiness check — an isolated node cannot depose a healthy leader                                       |
| **Concurrency**      | etcd-style single-writer control loop + one replication goroutine per peer; non-blocking client proposals                                  |
| **Persistence**      | `currentTerm` / `votedFor` survive restarts (losing them breaks safety)                                                                    |
| **Tests**            | Unit tests for the tricky invariants, an in-process cluster over a fake network, and an integration suite driving real processes over HTTP |

### In progress

- **Dynamic membership** (§6 joint consensus). The joint-quorum math, learners,
  and the configuration type are implemented and unit-tested; what remains is
  carrying configuration entries through the log, the two-phase transition, and
  the admin API. Until that lands, the cluster set is fixed at startup.

### Not yet built

- **Leadership transfer** — a leader cannot yet hand off deliberately.
- **Service discovery** — peers are passed on the command line.

---

## Quick start

**Prerequisites:** Go 1.24+.

```bash
git clone https://github.com/OmarAbdo/Crystal.git
cd Crystal
go build ./...
```

Run three nodes, each in its own terminal with its own data directory. Each
node is told about the _other_ two — listing yourself is rejected at startup,
because a node that counts itself twice computes a different majority than
everyone else and the cluster runs, incorrectly, in silence.

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

Within a few hundred milliseconds a leader is elected. There is no bootstrap
leader; every node starts as a follower and the randomized election timeout
(300–600 ms) decides it.

### Flags

| Flag                    | Default      | Meaning                                                    |
| ----------------------- | ------------ | ---------------------------------------------------------- |
| `-id`                   | `1`          | Unique positive node ID                                    |
| `-port`                 | `8080`       | TCP port to listen on                                      |
| `-data-dir`             | `data`       | Directory for this node's WAL, metadata, and snapshot      |
| `-peers`                | `""`         | Comma-separated `id:host:port` list of the **other** nodes |
| `-compaction-threshold` | `0` (→ 1000) | Cached entries that trigger a snapshot + WAL truncation    |
| `-session-ttl`          | `0` (→ 1h)   | How long an unused client session survives                 |

> **`-session-ttl` must be identical on every node.** It is part of the
> replicated decision procedure: nodes that disagree about when a session
> expires apply the same log and reach different states.

---

## Using it

### Writes

Writes go to the leader. A follower answers `421 Misdirected Request` and names
the leader in the `X-Raft-Leader` header, so a client can follow the redirect
without parsing prose or rediscovering the cluster by guessing.

```bash
curl -i -X POST -H "Content-Type: application/json" \
  -d '{"key":"color","value":"crystal"}' http://localhost:8001/set

curl -i -X POST -H "Content-Type: application/json" \
  -d '{"key":"color"}' http://localhost:8001/delete
```

### Exactly-once writes

Raft on its own is _at-least-once_: a leader can commit an entry and die before
answering, and the client's retry applies the command a second time. Add a
`client_id` and a per-client `seq` (starting at 1) and the state machine
recognizes the retransmission and replays the original outcome instead.

```bash
curl -i -X POST -H "Content-Type: application/json" \
  -d '{"key":"color","value":"crystal","client_id":"cli-7","seq":42}' \
  http://localhost:8001/set
```

This matters more for `delete` than for `set`: a retried delete, arriving after
someone else has recreated the key, destroys the new value. The hazard is not
repetition — it is repetition after the world has moved on.

Both fields or neither; a half-specified tag is rejected. Session state lives in
the state machine, is included in snapshots, and expires on a replicated clock
taken from the log rather than from any node's `time.Now()`.

### Reads

`/get` offers three tiers, and the default is the safe one.

```bash
# linearizable (default) — reflects everything committed when the read was admitted
curl -i "http://localhost:8001/get?key=color"

# bounded staleness — local state, but only within a bound you name
curl -i "http://localhost:8002/get?key=color&consistency=bounded&max_staleness=2s"

# local — whatever this node holds, no guarantee at all
curl -i "http://localhost:8003/get?key=color&consistency=local"
```

**Linearizable reads are served by any node, not just the leader.** A follower
asks the leader for a _read index_ — a log position, confirmed current by a
quorum round — then waits for its own state machine to reach it and answers
locally. The payload never touches the leader, so read capacity scales with the
cluster instead of being pinned to one node. Nothing a replica has applied can
be phantom, because applying requires commitment and commitment requires a
quorum; the read index just pins how far it must have caught up.

**Bounded staleness is enforced, not advisory.** `max_staleness` is required —
a client accepting stale data has to say how much — and a node that cannot meet
the bound returns `503` with its actual staleness rather than quietly serving
old state. Every response carries `X-Raft-Staleness`.

### Endpoints

| Endpoint              | Method | Purpose                                |
| --------------------- | ------ | -------------------------------------- |
| `/set`                | POST   | Write a key (leader only)              |
| `/delete`             | POST   | Delete a key (leader only)             |
| `/get`                | GET    | Read a key (any node; `?consistency=`) |
| `/internal/append`    | POST   | AppendEntries receiver                 |
| `/internal/vote`      | POST   | RequestVote receiver                   |
| `/internal/prevote`   | POST   | Pre-vote receiver                      |
| `/internal/readindex` | POST   | ReadIndex receiver (follower → leader) |
| `/internal/snapshot`  | POST   | InstallSnapshot receiver               |

---

## How it fits together

```text
cmd/crystal/main.go        composition root — wiring only, no logic
internal/
  config/                  flags, per-node path derivation, startup validation
  raft/                    consensus core — knows nothing about HTTP or key-values
    node.go                terms, votes, roles, RPC receivers, commit rule
    log.go                 framed WAL + in-memory cache + compaction
    configuration.go       membership, joint quorums, learners
    replicator.go          outbound RPCs
    transport.go           Transport seam (HTTP in production, fake net in tests)
    types.go               LogEntry, Command, RPC shapes
  engine/                  orchestration — the only writer of consensus state
    engine.go              single-writer control loop: elect, commit, apply, compact
    quorum.go              ReadIndex, CheckQuorum, read/apply waiters
    replicator_loop.go     one long-lived replication goroutine per peer
  store/                   state machine + snapshots
    statemachine.go        StateMachine interface, in-memory impl, client sessions
    snapshot.go            durable snapshot read/write
  transport/http.go        thin HTTP handlers — decode, delegate, encode
  fsutil/                  atomic file replace + directory fsync
  testcluster/             in-process cluster over a fake network
  integration/             real OS processes over real HTTP (build tag: integration)
docs/                      architecture and Raft-mapping walkthroughs
```

Dependencies point one way: `transport → engine → {raft, store}`, and
`store → raft`. The `raft` package never imports `store` or anything above it —
it moves opaque `[]byte` commands and has no idea they are key-value
operations. That boundary is what lets the storage backend be replaced (Later, if I decide so) without
touching consensus, and it is enforced by the direction of the import graph
rather than by convention.

### The concurrency model

One control goroutine owns every consensus _state_ transition — advancing
commit and applied indices, applying entries, running elections, firing client
waiters. It is the single writer, and that is what keeps commit/apply
race-free.

All outbound RPC latency lives in one long-lived replication goroutine per
peer. A slow or black-holed peer therefore slows only itself: it cannot stall
heartbeats, proposals, or the control loop. Elections work the same way —
votes are gathered on their own goroutines and tallied by the control loop,
which promotes on the vote that _reaches_ a majority rather than waiting for
the last reply to arrive.

Client proposals never block. A write appends to the log, registers a waiter
keyed by log index and term, and returns; the control loop resolves the waiter
when the commit index passes it.

Both locks in the `raft` package have a fixed order — `RaftNode.mu → RaftLog.mu`
— and the log never calls back into the node.

---

## Testing

```bash
go test ./...                                          # unit + in-process cluster
go test ./internal/testcluster/ -count=3               # repeat the timing-sensitive ones
go test -tags integration ./internal/integration/      # real processes over real HTTP
```

Three layers, each proving something the others cannot:

**Unit tests** pin the invariants that are easy to break and hard to notice:
the Figure 8 commit rule, the §5.4.1 election restriction, WAL recovery from a
partial frame, joint-quorum math.

**The in-process harness** ([internal/testcluster/](internal/testcluster/)) runs
a whole cluster in one process over a fake network, so a test can cut a link in
_one direction_ at a chosen instant, isolate a node, black-hole it (which is
not the same as refusing its connections — a refused connection fails
instantly, and that difference has hidden real bugs), inject drop rates and
delays, and assert single-leader-per-term continuously.

**Integration tests** build the binary, launch real OS processes, elect a
leader, kill it, and assert re-election, survival of committed data, follower
convergence, and snapshot-based catch-up.

[CI](.github/workflows/ci.yml) runs `go vet`, the full suite under `-race`, the
harness three times over (a single green run hides a flake), and the integration
suite — on Linux, because the race detector needs cgo and the primary dev box
is Windows without a C toolchain.

> A test that passes may be proving nothing. Several tests here passed against
> the _broken_ implementation before they were sharpened. Run a new test against
> the unfixed code before trusting it.

---

## Roadmap

- [x] Consensus core — elections, replication, safety (Figure 2, §5)
- [x] Durability — framed WAL, fsync, crash recovery
- [x] Compaction — snapshots + InstallSnapshot (§7)
- [x] Single-writer control loop + per-peer replicators
- [x] Linearizable reads on any node, enforced bounded staleness
- [x] Exactly-once client sessions with replicated expiry (§8)
- [x] Pre-vote, CheckQuorum, leader stickiness
- [ ] **Membership changes** (§6) — joint quorum math and learners are in;
      configuration entries in the log, the two-phase transition, and the admin
      API remain
- [ ] Leadership transfer
- [ ] Service discovery — stop hand-writing the peer list

---

## License

MIT. See [LICENSE](LICENSE).

## Contact

- Repository: <https://github.com/OmarAbdo/Crystal>
- Issues: <https://github.com/OmarAbdo/Crystal/issues>

# Crystal — Raft Conformance Fix Plan

Source of truth during execution. Derived from the 2026-07-21 code review of HEAD
`3ce740d` against *In Search of an Understandable Consensus Algorithm* (extended
version, May 2014).

**Decision (2026-07-21):** `stash@{0}` (the reverted Phase 0–2 work) is **not**
used. Every fix here is re-derived from scratch on clean `3ce740d`. Do not
`git stash pop`. The stash stays as a reference artifact only.

## Invariants for this work

- One finding per commit. Never combine two.
- Test-first: each step names a test that fails before the fix and passes after.
- After each step: `go build ./...`, `go vet ./...`, `go test ./...`.
  A failing test is a hard stop — revert, document, ask.
- Findings are numbered F1–F18 and keep those numbers forever, including in
  commit messages (`fix(raft): F2 fsync persistent state`).

## Status legend

`[ ]` not started · `[~]` in progress · `[x]` done · `[-]` deferred (with reason)

---

## Phase 0 — Durability and fail-stop

No structural change. Highest safety-per-line-changed in the tree. These three
are what stand between a power cut and a silently divergent cluster.

- [ ] **F2 — fsync persistent state.** `savePersistentStateLocked`
      ([node.go:629](../internal/raft/node.go#L629)) does `os.WriteFile` + `Rename`
      with no `Sync` on the temp file and no fsync of the containing directory.
      Figure 2's "updated on stable storage before responding to RPCs" is not
      satisfied, so a crash after granting a vote can resurrect the node with
      `votedFor = -1` in the same term → two leaders → Election Safety violated.
      *Fix:* open/write/`Sync`/`Close`/`Rename`/fsync-dir. Add a shared
      `fsyncDir` helper; apply it to `SnapshotManager.Write`
      ([snapshot.go:77](../internal/store/snapshot.go#L77)) and
      `rewriteWALLocked` ([log.go:572](../internal/raft/log.go#L572)) too — both
      rename without durably committing the directory entry.
      *Test:* `TestSavePersistentStateIsDurable` (inject a writer that records
      the call order; assert Sync precedes Rename).
      *Blast radius:* every term change and vote. Low risk, pure addition.

- [ ] **F3 — persist before mutate.** `BecomeCandidate`
      ([node.go:405](../internal/raft/node.go#L405)) increments the term, votes for
      itself, and flips to Candidate *before* calling
      `savePersistentStateLocked`, with no rollback on failure. On persist error
      the node keeps an unpersisted term it has already voted in — after a crash
      it can vote again in that same term.
      *Fix:* build the candidate `PersistentState` value, persist it, and only
      then commit it to `rn.persistent` and flip `Role`. Same shape for the vote
      grant in `HandleRequestVote` ([node.go:473](../internal/raft/node.go#L473)),
      which currently sets `VotedFor` in memory and then reports
      `VoteGranted: false` if the persist fails.
      *Test:* `TestBecomeCandidate_PersistFailureLeavesStateUnchanged`,
      `TestHandleRequestVote_PersistFailureDoesNotRecordVote`.
      *Blast radius:* election path. Needs a persist seam on `RaftNode` to
      inject failure — introduce a small `stateStore` interface.

- [ ] **F6 — apply failures must be fatal.** `applyCommitted`
      ([engine.go:271](../internal/engine/engine.go#L271)) calls
      `AdvanceLastApplied()` unconditionally, then `continue`s past a missing
      entry, a corrupt command, or a state-machine error. Every one of those
      means this replica skipped an entry its peers applied — State Machine
      Safety (§5.4.3) gone, silently.
      *Fix:* none of the three is recoverable at runtime. Read the entry and
      decode it *before* advancing `lastApplied`; on any error, log fatally and
      halt. A crash is recoverable; divergence is not.
      *Test:* `TestApplyCommitted_HaltsOnMissingEntry` (needs the halt action
      injected, not a raw `log.Fatalf`, so the test can observe it).
      *Blast radius:* apply loop. Introduces a `fatal func(...)` seam on Engine.

---

## Phase 1 — Atomic term handling

The RPC receivers currently make their Figure 2 decisions across several
independent lock acquisitions. This phase makes each receiver decide once.

- [ ] **F1 — `HandleAppendEntries` term TOCTOU.**
      ([node.go:242](../internal/raft/node.go#L242)) Reads `currentTerm`, releases,
      reads `State()`, releases, calls `BecomeFollower`, releases, then splices
      the log with no node lock held. Two concurrent HTTP goroutines carrying
      terms 6 and 7 both pass step 1 against a stale `currentTerm = 5`; the
      term-7 one wins the `BecomeFollower` race and the term-6 one still splices
      its entries in and replies `Success`. Direct Log Matching violation.
      *Fix:* one critical section decides `reject` / `accept-at-term-T` and
      publishes T; after the splice, re-verify `CurrentTerm == T` under the lock
      before returning `Success`, else return failure without acking.
      *Test:* `TestHandleAppendEntries_ConcurrentTermsRejectStale` (two
      goroutines, `-race`, assert the lower-term append is not applied).
      *Blast radius:* the hottest path in the system. Do not combine with F17.

- [ ] **F1b — `HandleInstallSnapshot` has the identical structure.**
      ([node.go:308](../internal/raft/node.go#L308)) Same fix, separate commit.
      *Test:* `TestHandleInstallSnapshot_ConcurrentTermsRejectStale`.

- [ ] **F11 — lock discipline is documented as a lie.** The node.go header says
      `RaftNode.mu` and `RaftLog.mu` are "NEVER held simultaneously";
      `AdvanceCommitIndex` ([node.go:217](../internal/raft/node.go#L217)) calls
      `termAt(quorumIndex)` — which takes `rl.mu` — while holding `rn.mu`. No
      deadlock today because nothing goes `rl.mu → rn.mu`, but the invariant is
      false and the next `RaftLog` method that consults the node deadlocks.
      *Fix:* adopt an explicit **order**: `rn.mu` may be taken before `rl.mu`,
      never the reverse. Rewrite the header comment and the `AdvanceCommitIndex`
      doc comment to state the order. Audit every call site for violations.
      Note F1 will likely add more `rn.mu`-held-across-log-call paths, so land
      this decision first.
      *Test:* none directly; enforced by review + `go vet`/`-race` in CI.
      *Blast radius:* documentation plus an audit. No behavior change.

---

## Phase 2 — Snapshot and compaction correctness

- [ ] **F17 — `HandleInstallSnapshot` resets the log before persisting the
      snapshot.** ([node.go:349](../internal/raft/node.go#L349)) Order is
      `restore` → `ResetToSnapshot` (rewrites/discards the WAL) → `persist`. A
      crash in that window leaves a follower that has discarded fsync-acked
      committed entries with no snapshot covering them. The code comment argues
      for this order; the argument is backwards — a persisted snapshot covering
      entries still present in the log is harmless, the reverse is unrecoverable.
      *Fix:* `restore` → `persist` → `SeedFromSnapshot` → `ResetToSnapshot`.
      *Test:* `TestHandleInstallSnapshot_PersistBeforeLogReset` (record call
      order), plus a failure-path test asserting the log is untouched when
      persist fails.

- [ ] **F4 — `InstallSnapshotResponse` has no success flag.**
      ([types.go:89](../internal/raft/types.go#L89)) Every failure path in the
      receiver returns the byte-identical `{Term}`, so the leader can't
      distinguish "restored" from "restore failed" and unconditionally runs
      `UpdatePeerProgress(peerID, req.LastIncludedIndex)`
      ([replicator.go:196](../internal/raft/replicator.go#L196)). A phantom
      quorum then commits entries held by one server → Leader Completeness gone.
      *Fix:* add `Success bool`; leader advances progress only on `Success`.
      Figure 13 omits it only because the paper's receiver has no failure modes
      short of a stale term; ours does.
      *Test:* `TestInstallSnapshotTo_NoProgressOnFailure`.

- [ ] **F5 — compaction snapshots at `commitIndex`, not `lastApplied`.**
      ([engine.go:299](../internal/engine/engine.go#L299)) The state machine
      reflects `lastApplied`; §7 defines last-included-index as "the last entry
      the state machine had applied". On a follower these diverge routinely
      because `SetFollowerCommitIndex` runs on the HTTP goroutine and can jump
      `commitIndex` between `applyCommitted()` and `maybeCompact()` in one tick.
      Result: a snapshot claiming entries whose effects it does not contain, and
      then `TruncateBeforeIndex` deletes the entries that would have fixed it.
      *Fix:* compact at `lastApplied`. Also guard the companion bug — `term :=
      TermAt(commitIndex)` returns `0` for an already-compacted index, and the
      snapshot file gets written with `LastIncludedTerm: 0` before
      `TruncateBeforeIndex` bails, poisoning the post-snapshot consistency check
      across a restart. Refuse to compact when `TermAt` returns 0.
      *Test:* `TestCompactUsesLastApplied`, `TestCompactRefusesUnknownTerm`.

---

## Phase 3 — Verification harness

Everything after this point is concurrency and partition behavior. Unit tests
cannot demonstrate the bugs, so the harness comes before the restructure it is
meant to validate. Build it fresh; do not lift it from the stash.

- [ ] **F18 — in-process cluster harness.** New `internal/testcluster`:
      real `Engine`/`RaftNode`/`RaftLog` instances wired over an injectable
      transport with directed link cuts (request *and* response legs), seeded
      drops, and delays. Helpers: `SetViaLeader`, `WaitConverged`,
      `WaitLeaderAmong`, `Isolate`, `Heal`. Every poll iteration asserts
      `CheckSingleLeaderPerTerm` — the harness should fail on Election Safety
      violations regardless of what the individual test is looking at.
      *Prerequisite:* extract a `raft.Transport` interface so `Replicator` is no
      longer hardwired to `http.Client`
      ([replicator.go:97](../internal/raft/replicator.go#L97)). That refactor is
      its own commit, landed first, with no behavior change.
      *Test:* `TestHarness_ElectsSingleLeader`, `TestHarness_PartitionHealsData`.

- [ ] **F18b — CI.** GitHub Actions on ubuntu (so `-race` actually runs; it
      can't on the Windows box — no cgo): `go vet`, `-race` unit tests, `-race`
      integration tests. Race detection is the only thing that will reliably
      catch regressions of F1.

---

## Phase 4 — Leadership has one owner

Currently two: the control loop, and any HTTP goroutine that calls
`BecomeFollower`. These four findings are one bug wearing four hats.

- [ ] **F7 — inbound-RPC stepdown never reconciles the engine.**
      `HandleRequestVote`/`HandleAppendEntries` demote a leader on the HTTP
      goroutine; `stopReplicators()` is only reachable via `handleStepDown`
      (outbound path) or shutdown. Stale replicators survive with the old
      `pr.term`, and `startReplicators` no-ops on re-election because of the
      `len(e.replicators) > 0` guard
      ([engine.go:418](../internal/engine/engine.go#L418)). They then resume
      under a new term while reporting against the old one.
      *Fix:* `reconcileLeadership()` as the first thing on every tick — detect
      leader→follower transitions, tear down replicators, fail waiters, and
      self-heal missing replicators. Track `replicatorsTerm` and replace stale
      sets rather than short-circuiting on non-empty.
      *Test:* `TestReconcileInboundStepdown`,
      `TestStartReplicatorsReplacesStaleTerm`.

- [ ] **F8 — `handleStepDown` guard is inverted for leaders.**
      ([engine.go:259](../internal/engine/engine.go#L259))
      `higherTerm <= CurrentTerm() && !IsLeader()` lets a *stale* signal whose
      term is ≤ our own depose a healthy leader. With F7 this is a self-sustaining
      oscillation: re-elected at term 7, stale replicator reports 7, node deposes
      itself immediately.
      *Fix:* `if higherTerm <= e.node.CurrentTerm() { return }`.
      *Test:* `TestHandleStepDown_IgnoresStaleTerm`.

- [ ] **F15 — waiters are not term-scoped.**
      ([engine.go:188](../internal/engine/engine.go#L188)) `fireCommittedWaiters`
      resolves on `w.index <= commitIndex` alone. A waiter that survives an
      unnoticed stepdown (F7) and a re-election inside its 2s deadline gets acked
      `nil` when its index now holds a *different* leader's entry — the client is
      told its write committed when it never did.
      *Fix:* record `term` on `waiter`; ack only on term match, else
      `ErrNotLeader`.
      *Test:* `TestFireCommittedWaiters_TermMismatchFails`.

- [ ] **F9 — `runElection` blocks the control loop and waits for all peers.**
      ([engine.go:337](../internal/engine/engine.go#L337)) Called synchronously
      from `onTick`, ends in `wg.Wait()`, each RPC has a 1s timeout. The control
      loop stalls up to a full second — no proposals, applies, or `stepDownCh`
      drains — and §5.2 says a candidate wins "if it receives votes from a
      majority", i.e. the moment the majority arrives, not when the slowest peer
      answers. With a 300–600ms election timeout and a 1s RPC timeout, one
      black-holed peer makes every election overrun its own timeout.
      *Fix:* fire the RPCs and return; deliver results on a `voteCh` handled by
      the control loop; promote the instant a majority is tallied. Single-node
      clusters must win immediately.
      *Test:* `TestRunElectionDoesNotBlockOnSlowPeer`, harness test with one
      peer black-holed asserting sub-timeout election.

---

## Phase 5 — Client-facing protocol (§8)

- [ ] **F10 — redirect names the wrong node.**
      ([http.go:86](../internal/transport/http.go#L86), [:118](../internal/transport/http.go#L118))
      `route to node %d` prints `s.node.NodeID()` — the node that just rejected
      the request. §8: the server should "supply information about the most
      recent leader it has heard from". `RaftNode.LeaderID` is tracked faithfully
      and never exposed.
      *Fix:* `CurrentLeader()` accessor; add to the `leaderChecker` interface;
      return a real hint. Also reconcile the 421-vs-403 drift between the
      handler and the comment in `errors.go`.
      *Test:* `TestHandleSet_RedirectsToKnownLeader`.

- [ ] **F12 — no linearizable reads at all.**
      ([http.go:136](../internal/transport/http.go#L136)) `handleGet` reads the
      local state machine with no leader check whatsoever — any follower answers
      from its own lagging state, and a deposed leader serves confidently stale
      data. §8 requires both precautions: (a) commit a no-op from the current
      term before serving reads — the no-op *is* appended
      ([engine.go:399](../internal/engine/engine.go#L399)) but reads are never
      gated on it committing; (b) "exchange heartbeat messages with a majority of
      the cluster before responding to read-only requests."
      *Fix:* ReadIndex. `readIndex = max(commitIndex, leaderNoopIndex)`; release
      the read when a current-term entry is committed, `lastApplied >= readIndex`,
      **and** a quorum of peers has acked a replication round that *started after*
      the read was registered. That last clause is the subtle one — see the note
      below. `/get` becomes leader-only and linearizable by default, with
      `?consistency=stale` for the old local read.
      *Test:* harness test — partitioned leader must time out
      (`ErrReadTimeout`), never serve stale.

      > **Known trap, recorded so we don't re-introduce it:** counting acks by
      > "any ack that arrived after registration" is unsound. A round *sent*
      > before the read registered can deliver its response *after* it; the
      > follower's "still your term" assertion predates the read, so the quorum is
      > confirmed with stale evidence (window ≈ one RTT, up to the 1s timeout).
      > The dissertation (§6.4) requires heartbeats *initiated after* the read
      > index is recorded. Carry a per-peer round-start counter through the ack
      > channel and count only rounds that started after registration.

- [ ] **F14 — no exactly-once client semantics.** No client IDs, no serial
      numbers, no dedup table. Concretely, `ErrCommitTimeout`
      ([engine.go:211](../internal/engine/engine.go#L211)) is a lie: the entry
      stays in the log and usually commits afterwards, so the client's retry
      applies it twice. `set`/`delete` are idempotent so it's survivable today —
      it stops being survivable the moment a non-idempotent op exists.
      *Fix:* §8's mechanism — client-assigned serial numbers, state machine
      tracks the latest serial and cached response per client, replays the
      response instead of re-executing.
      *Test:* `TestApply_DedupesRepeatedSerial`, harness test that a retried
      write after a timeout applies once.
      *Note:* this changes the wire format and the `StateMachine` interface.
      Land it after Phase 4 is stable.

---

## Phase 6 — §6 disruption avoidance and membership

- [ ] **F13 — no leader-stickiness check on `RequestVote`.**
      ([node.go:442](../internal/raft/node.go#L442)) §6, final paragraph:
      "if a server receives a RequestVote RPC within the minimum election timeout
      of hearing from a current leader, it does not update its term or grant its
      vote." We grant on term + log criteria alone, so a briefly-partitioned node
      returns with an inflated term and deposes a healthy leader on contact.
      `lastContact` already exists; the check is a few lines.
      *Fix:* deny (without bumping the term) when
      `TimeSinceContact() < electionTimeoutMin` and we know of a live leader.
      *Test:* harness — isolate a follower, heal it, assert the leader survives.
      *Follow-on:* pre-vote is the stronger fix and should be considered after
      this lands; §6's check is the paper's own remedy and comes first.

- [ ] **F16 — no membership changes (§6 joint consensus).** `clusterSize` is
      fixed at construction from `-peers`. Largest single item in the plan.
      *Sub-item to do now regardless:* `parsePeers`
      ([config.go:69](../internal/config/config.go#L69)) does not reject a peers
      list containing the node's own ID, which silently inflates the majority
      threshold on that node alone. Cheap, land it in Phase 7.
      *Decision:* defer joint consensus until Phases 0–5 are green. Re-scope
      then.

---

## Phase 7 — Minors and hygiene

Batchable; each still gets its own commit.

- [ ] **M1** — `parsePeers` must reject the node's own ID (see F16).
- [ ] **M2** — cache positional invariant. `AppendEntriesToLog`
      ([log.go:260](../internal/raft/log.go#L260)) appends without checking
      `entry.Index == firstIndex + len(cache)`. That invariant underpins every
      `getEntryLocked`; a violation is silent and unrecoverable. Assert it.
      Related: `rl.cache[:entry.Index-rl.firstIndex]`
      ([log.go:248](../internal/raft/log.go#L248)) panics on a negative bound.
- [ ] **M3** — `UpdatePeerProgress` ([node.go:132](../internal/raft/node.go#L132))
      silently creates a map entry for an unknown `peerID`, which would inflate
      the `indices` slice in `AdvanceCommitIndex` and shift the quorum median.
      Reject unknown peers explicitly.
- [ ] **M4** — `buildSnapshotRequest` ([engine.go:474](../internal/engine/engine.go#L474))
      re-reads and re-encodes the snapshot from disk on every replication round;
      at a 100ms heartbeat that is 10 full reads/sec per lagging follower. Cache
      it, keyed on `LastIncludedIndex`.
- [ ] **M5** — `writeEntryToDisk` ([log.go:498](../internal/raft/log.go#L498))
      fsyncs per entry, so a 100-entry batch does 100 fsyncs. Batch the writes
      and sync once, as `rewriteWALLocked` already does.
- [ ] **M6** — `cmd/crystal/data/raft.wal` and `raft.meta` are tracked in git
      despite `.gitignore` covering the patterns. `git rm --cached`.

---

## Sequencing rationale

Phases 0–2 are local, unit-testable, and each closes a path to silent data loss;
they land first because they are cheap and independent. Phase 3 exists because
Phases 4–6 cannot be honestly verified without partition testing. Phase 4 is the
one structural change: it gives leadership a single owner, which makes F7, F8,
F15 and F9 collapse into one coherent design instead of four patches. Phase 5
changes the client contract and so waits for a stable core. Phase 6's membership
work is deliberately last and may be re-scoped.

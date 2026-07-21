# Crystal — Raft Conformance Fix Plan

Source of truth during execution. Derived from the 2026-07-21 code review of HEAD
`3ce740d` against *In Search of an Understandable Consensus Algorithm* (extended
version, May 2014).

**Decision (2026-07-21):** `stash@{0}` (the reverted Phase 0–2 work) is **not**
used. Every fix here is re-derived from scratch on clean `3ce740d`. Do not
`git stash pop`. The stash stays as a reference artifact only.

## Invariants for this work

- One finding per commit — unless splitting would leave the tree uncompilable or
  in a state that is incoherent rather than merely incomplete. That happened
  twice (F1b, and all of Phase 4); both are recorded where they occurred.
- Test-first: each step names a test that fails before the fix and passes after.
  Where the test cannot distinguish fixed from broken, say so rather than
  claiming coverage — see the InstallSnapshot note in F1.
- After each step: `go build ./...`, `go vet ./...`, `go test ./...`, plus the
  harness and the integration suite. A failing test is a hard stop.
- Findings keep their numbers forever, including in commit messages
  (`fix(raft): F2 fsync persistent state`). F19 and F1c were added mid-flight.

## Status legend

`[ ]` not started · `[~]` in progress · `[x]` done · `[-]` deferred (with reason)

---

## Phases 0–4 — COMPLETE

All entries below are landed on `raft-conformance-fixes`. Build, vet, unit,
harness and integration suites green after every commit. Original bug analysis is
in the commit messages, which are the durable record; these are one-liners.

### Phase 0 — Durability and fail-stop ✅
- [x] **F2** `7e3aa53` — persistent state was atomic but never fsynced. New
      `internal/fsutil` (write→fsync→rename→fsync-dir) applied to raft metadata,
      snapshots and the WAL rewrite. `SyncDir` is a documented no-op on Windows.
- [x] **F3** `71ccf2a` — term/vote adopted in memory before being persisted, no
      rollback. `stateStore` seam + `commitPersistentLocked` (save, then adopt).
- [x] **F5** `e23918b` — compaction snapshotted at `commitIndex`, not
      `lastApplied`; also refused to compact on an unresolvable term.
- [x] **F6** `08836b4` — apply failures skipped entries and advanced
      `lastApplied` anyway. Now fatal, via a `fatalf` seam.
- [x] **F17** `ebabf61` — snapshot log-reset ran before the persist that
      justified it. Reordered; `RestoreOffset` now reconciles the snapshot/WAL
      overlap that persist-first legitimizes.

### Phase 1 — Atomic term handling ✅
- [x] **F11** `d2081b0` — the "locks are never held together" invariant was
      already false. Replaced with the order `rn.mu → rl.mu`, safe by
      construction because `RaftLog` never calls `RaftNode`.
- [x] **F1 + F1b** `6912774` — the RPC term decision was split across separate
      lock acquisitions from the mutation it authorized, so a stale-term request
      could splice entries in behind a concurrent term change. Both receivers now
      decide and act under one lock. `termDecisionHook` makes the race
      deterministic; pre-fix the test fails with *entry 1 has term 6 at
      currentTerm 7*.
- [x] **F1c** `38458d0` — *(not in the original review; found auditing lock
      order)* the vote sampled log state before taking the lock, so the §5.4.1
      comparison could tear or go stale. `LastLogState` + callback under `rn.mu`.

### Phase 2 — Snapshot and compaction ✅
F5 and F17 were pulled forward into Phase 0, ahead of F6: F6 makes a missing
committed entry fatal, and both of those bugs *generate* missing committed
entries. Landing F6 first would have turned two silent corruptions into two
crashes. Only **F4** remains — see Phase 5 below, where it now sits.

### Phase 3 — Verification harness ✅
- [x] **Transport seam** `f223e13` — `raft.Transport` behind `Replicator`, no
      behavior change.
- [x] **Engine RPC facade** `01ccfbe` — `main.go`'s `rpcBinding` deleted, so the
      harness exercises the real wiring rather than a copy free to drift.
- [x] **F18** `f11cb7d` — `internal/testcluster`: real nodes over a fake network
      with directed cuts on both legs, seeded drops, delay, and
      `SetBlackholeDelay`. `CheckSingleLeaderPerTerm` sampled in every poll.
      6.8s vs 48s for the process suite.
- [x] **F18b** `b4e1033` — CI on ubuntu, `-race` throughout (cgo is unavailable
      on the Windows box, so this is the only place races are detected). Harness
      repeated `-count=3`. Also **M6**, untracking the committed WAL/meta.

### Phase 4 — Leadership has one owner ✅
- [x] **F7, F8, F15, F9** `f0f47e1` — landed as one commit because they are four
      symptoms of one cause: leadership had two owners, the control loop and any
      HTTP goroutine handling an inbound higher-term RPC. The fixes interlock —
      `reconcileLeadership` (F7) is what makes F8's guard safe to simplify, and
      F15's term check is meaningless without F7 noticing the stepdown. F9 was
      A/B verified: with the blocking `wg.Wait()` restored and two of five peers
      black-holed, the cluster fails to elect at all within 10s; with the fix it
      elects in ~407ms.

---

## Open work

### Read scalability — done (2026-07-21, from design review with Omar)

The original F12 made reads linearizable but leader-only, which pinned read
capacity to one machine — the wrong shape for a coordination store, where reads
outnumber writes heavily. Two findings came out of that discussion:

- [x] **F21 — followers serve linearizable reads.** *(done `81ad47c`)* The
      mistake was conflating two things: ReadIndex needs the LEADER to supply the
      index, not to serve the read. New `ReadIndex` RPC — a follower fetches a
      quorum-confirmed index, waits for its own apply to reach it, and answers
      locally. Only an integer crosses the network, so read capacity scales with
      the cluster. Cost: one round trip of latency per follower read, traded for
      throughput deliberately.
- [x] **F22 — bounded-staleness tier.** *(done `0710ae9`)* `ReadOptions` with a
      `Consistency` enum (zero value = `Linearizable`, so the safe mode is the
      accidental default — a `bool` would have inverted that) and a **required**
      `MaxStaleness`. The bound measures time since this node last confirmed
      currency with a leader, and applies to the leader too. Needs no clock-skew
      assumption: both ends of the interval are measured on one machine.
      Unbounded reads are demoted to `?consistency=local`, an ops/debug tool.

- [x] **F19 — a leader that loses its quorum never steps down.** *(done
      `66b5b4d`, with F12 as planned — they share the per-peer ack machinery)* *(found
      2026-07-21 while writing the F18 minority test; not in the original review)*
      Quorum counting is correct — no minority node ever wins an election — but
      the incumbent keeps `Role == Leader` indefinitely after being cut off. Not a
      Figure 2 violation (a stale leader cannot commit), but two things downstream
      assume a node claiming leadership can reach a quorum: `/get` serves local
      state (**F12**) so a deposed leader answers with arbitrarily stale data, and
      clients are redirected to it (**F10**) only to hang until the deadline.
      *Fix:* CheckQuorum — track per-peer ack recency, step down when a majority
      has not been heard from within an election timeout. **This is the same
      machinery ReadIndex needs, so build it once in F12 and let F19 fall out.**
      *Test:* `TestPartitionedLeaderStepsDown`, currently `t.Skip`ped in
      `internal/testcluster/cluster_test.go` with the reason inline.

- [x] **F4 — `InstallSnapshotResponse` has no success flag.** *(done `b47d2c8`)*
      — added `Success bool`; leader advances progress only on it; "already
      covered" reports success deliberately (the follower does hold that data).
      7 tests.

      Original analysis:
      ([types.go:89](../internal/raft/types.go#L89)) Every failure path in the
      receiver returns the byte-identical `{Term}`, so the leader cannot tell
      "restored" from "restore failed" and unconditionally runs
      `UpdatePeerProgress(peerID, req.LastIncludedIndex)`. A phantom quorum then
      commits entries held by one server — Leader Completeness gone. Figure 13
      omits the flag only because the paper's receiver has no failure modes short
      of a stale term; ours does.
      *Fix:* add `Success bool`; advance progress only on it.
      *Test:* `TestInstallSnapshotTo_NoProgressOnFailure`.

## Phase 5 — Client-facing protocol (§8)

- [x] **F10 — redirect names the wrong node.** *(done `c1cd6cf`)* — added
      `RaftNode.CurrentLeader()`, gave the `Server` the peer map so it can turn a
      leader ID into a connectable address, and added the `X-Raft-Leader` header.
      Three distinct cases (addressable / known-by-ID-only / genuinely
      leaderless). The post-stepdown `ErrNotLeader` path redirects the same way.
      Corrected the 403-vs-421 comment drift. 4 tests.

      Original analysis:
      ([http.go:86](../internal/transport/http.go#L86), [:118](../internal/transport/http.go#L118))
      `route to node %d` prints `s.node.NodeID()` — the node that just rejected
      the request. §8: the server should "supply information about the most
      recent leader it has heard from". `RaftNode.LeaderID` is tracked faithfully
      and never exposed.
      *Fix:* `CurrentLeader()` accessor; add to the `leaderChecker` interface;
      return a real hint. Also reconcile the 421-vs-403 drift between the
      handler and the comment in `errors.go`.
      *Test:* `TestHandleSet_RedirectsToKnownLeader`.

- [x] **F12 — no linearizable reads at all.** *(done `66b5b4d`)* — ReadIndex
      with round-sequence barriers; `/get` linearizable by default,
      `?consistency=stale` opts out; refuses rather than serving stale.
      The recorded in-flight-ack trap is enforced and pinned by
      `TestReadIndex_IgnoresAcksFromRoundsStartedBeforeTheRead`.

      **Division of labour worth remembering:** CheckQuorum only reacts after a
      full grace period, so for up to one election timeout after a partition the
      old leader still believes it leads. ReadIndex covers precisely that window.
      Neither alone is sufficient.

      Original analysis:
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

- [x] **F14 — no exactly-once client semantics.** *(done `a743bd3`)* — optional
      `ClientID`/`Seq` on `Command`; per-client last-sequence + outcome in the
      state machine, replayed for retransmissions. The dedup table is **in the
      snapshot** (omitting it would make a restored node re-apply retries).
      Required decoupling `SnapshotFile.State` to `json.RawMessage`, which also
      removed a pointless decode/re-encode round trip. `ErrCommitTimeout`'s
      documentation corrected — it never meant the write failed.

      **Follow-up filed: F23 — sessions never expire.** The table grows one entry
      per distinct client forever and rides in every snapshot. The fix is client
      leases (register / renew / reclaim on lapse), which also lets a client learn
      its session is gone and that retries are no longer safe. A bare LRU would
      silently reopen the hole F14 closes and is the wrong answer.

      Original analysis: No client IDs, no serial
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

- [x] **F13 — no leader-stickiness check on `RequestVote`.** *(done `8fc7be0`)*
      — but **necessary, not sufficient**. Writing the harness test showed the
      disruption never travels through RequestVote at all: the isolated node's
      inflated term reaches the leader via the **AppendEntries response**, which
      Figure 2 obliges it to honour. §6's check stops the vote, not the term.
      Hence F20.

- [x] **F20 — no pre-vote.** *(found 2026-07-21 while testing F13; done
      `bb3d553`)* An isolated node spent a term on every timeout and returned far
      ahead of the cluster. Pre-vote (dissertation §9.6) asks a majority whether a
      campaign could succeed *before* incrementing anything, so a node that cannot
      reach a quorum never moves its term and has nothing disruptive to say on
      rejoin. `HandlePreVote` changes no state at all — that is what makes the
      poll safe. Measured before: isolated node reached term 5 vs cluster term 1,
      dragged the leader to 7. After: term held through 2s of failed polls,
      genuine failover still ~540ms.

      Original analysis:
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

- [~] **F16 — no membership changes (§6 joint consensus).** *(step 1 of 3 done,
      `386372b`)*

      **Done — step 1: membership is a `Configuration`, not an integer.**
      `raft.Configuration{Voters, OldVoters, Learners}` owns every quorum
      decision: `HasQuorum` requires separate majorities from both memberships
      when joint, `QuorumIndex` takes the lower frontier of the two. Vote and
      pre-vote tallies now record *which* servers voted (a count is meaningless
      under a joint config). `BecomeLeader` rebuilds progress maps for exactly
      the current membership. The engine's peer set derives from the node's
      configuration rather than startup config. No behavior change; 12 new tests
      for the quorum math.

      **Remaining — step 2: configuration entries in the log.** `OpConfig`
      command; applied **on append, not on commit** (§6: "a server always uses
      the latest configuration in its log, regardless of whether the entry is
      committed"), which means the log-append path must notify the node and a
      truncation must roll the configuration back — so the node needs a
      configuration *history* keyed by index, not a single value. The
      configuration must also go into snapshots (§7: "the snapshot also includes
      the latest configuration in the log as of last included index").

      **Remaining — step 3: the transition itself.** Leader appends `C_old,new`;
      once it commits, appends `C_new`; once *that* commits, a leader not in
      `C_new` steps down (§6). Learner catch-up phase before promotion. Admin
      API to add/remove a server. §6's disruption remedies are already in place
      (F13 stickiness, F20 pre-vote), which is what makes removed servers safe.

      Original analysis: `clusterSize` is
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

- [x] **M1** *(done `6c811f4`)* — `parsePeers` must reject the node's own ID (see F16).
- [x] **M2** *(done `6c811f4`)* — cache positional invariant. `AppendEntriesToLog`
      ([log.go:260](../internal/raft/log.go#L260)) appends without checking
      `entry.Index == firstIndex + len(cache)`. That invariant underpins every
      `getEntryLocked`; a violation is silent and unrecoverable. Assert it.
      Related: `rl.cache[:entry.Index-rl.firstIndex]`
      ([log.go:248](../internal/raft/log.go#L248)) panics on a negative bound.
- [x] **M3** *(done `6c811f4`)* — `UpdatePeerProgress` ([node.go:132](../internal/raft/node.go#L132))
      silently creates a map entry for an unknown `peerID`, which would inflate
      the `indices` slice in `AdvanceCommitIndex` and shift the quorum median.
      Reject unknown peers explicitly.
- [x] **M4** *(done `6c811f4`)* — `buildSnapshotRequest` ([engine.go:474](../internal/engine/engine.go#L474))
      re-reads and re-encodes the snapshot from disk on every replication round;
      at a 100ms heartbeat that is 10 full reads/sec per lagging follower. Cache
      it, keyed on `LastIncludedIndex`.
- [x] **M5** *(done `6c811f4`)* — `writeEntryToDisk` ([log.go:498](../internal/raft/log.go#L498))
      fsyncs per entry, so a 100-entry batch does 100 fsyncs. Batch the writes
      and sync once, as `rewriteWALLocked` already does.
- [x] **M6** — done in `b4e1033`. `cmd/crystal/data/raft.wal` and `raft.meta`
      were tracked despite `.gitignore` covering the patterns; ignore rules do not
      apply to files already in the index.

---

## Sequencing rationale

Phases 0–2 were local, unit-testable, and each closed a path to silent data loss,
so they landed first. Phase 3 existed because Phases 4–6 cannot be honestly
verified without partition testing — a judgement the F9 A/B check vindicated: the
old blocking election was not merely slow, it prevented election entirely against
black-holed peers, and no unit test would have shown that. Phase 4 was the one
structural change, giving leadership a single owner so F7, F8, F15 and F9
collapsed into one design instead of four patches.

**What remains, and why in this order.** F10 is a one-line correctness fix and
should land first. F4 is small and closes a Leader Completeness hole. F12 is the
largest remaining item and should absorb F19, since CheckQuorum and ReadIndex
need the same per-peer ack machinery — building it twice would be the mistake.
F13 is cheap once the harness is trusted. F14 changes the wire format and the
StateMachine interface, so it wants a stable core beneath it. F16 (joint
consensus) stays last and may be re-scoped.

# Crystal ↔ the Raft paper

This document maps Crystal's code to _"In Search of an Understandable Consensus
Algorithm"_ (Ongaro & Ousterhout, 2014 — in the repo root). If you've read the
paper, this shows you where each piece lives. If you haven't, this builds the
intuition alongside the code. Section numbers (§5.2, etc.) and figure numbers
refer to the paper.

Raft's whole design pitch is **understandability through decomposition**:

> _"Raft separates leader election, log replication, and safety … Raft reduces
> the degree of nondeterminism and the ways servers can be inconsistent with each
> other."_ — §1

So we'll take those pieces one at a time.

---

## The three roles and the term

Every node is at all times a **Leader**, **Follower**, or **Candidate**
([`raft/types.go`](../internal/raft/types.go)). Time is divided into **terms** —
monotonically increasing integers, each beginning with an election. A term is
Raft's logical clock: any message carrying a higher term than yours means you're
behind, and the universal reaction is to step down and adopt it.

> _"Terms act as a logical clock in Raft, and they allow servers to detect
> obsolete information such as stale leaders."_ — §5.1

In the code, `currentTerm` and `votedFor` are the two fields that **must** survive
a crash (`PersistentState`), because forgetting them lets a node vote twice in one
term. They're written to `raft.meta` via temp-file+rename on every change.

---

## Leader election (§5.2)

**The mechanism.** Followers expect to hear from a leader regularly (heartbeats).
If a follower's randomized **election timeout** elapses with no contact, it
assumes there's no leader, becomes a candidate, increments its term, votes for
itself, and asks everyone else for a vote. A candidate that collects a majority
becomes leader.

> _"Raft uses randomized timers to elect leaders. This adds only a small amount of
> mechanism to the heartbeats already required for any consensus algorithm."_ — §1

**Why randomized?** If every follower used the same timeout, they'd all become
candidates at once, split the vote, and no one would win — repeatedly. Randomizing
each node's timeout (Crystal: 300–600 ms, heartbeat 100 ms) means one node almost
always times out first and wins before the others wake up.

Where it lives:

| Step | Code |
|------|------|
| Timeout fires → become candidate | `engine.runElection` → `RaftNode.BecomeCandidate` |
| Send RequestVote to all peers (in parallel) | `engine.runElection` → `Replicator.RequestVoteFrom` |
| Tally votes, check majority | `RaftNode.RecordVoteAndCheckMajority` |
| Win → become leader | `RaftNode.BecomeLeader` |
| See a higher term → step down | `RaftNode.BecomeFollower` |

**A subtle race.** Between "I counted a majority" and "I become leader," an
incoming RPC on an HTTP goroutine could carry a higher term and step this node
down. If we promoted anyway, we'd have **two leaders in that term**.
`BecomeLeader` closes this: it re-checks, under the lock, that the node is *still*
a candidate at *this* election's term, and no-ops otherwise.

```go
func (rn *RaftNode) BecomeLeader(electionTerm, lastLogIndex int) (promoted bool) {
    rn.mu.Lock()
    defer rn.mu.Unlock()
    if rn.Role != Candidate || rn.persistent.CurrentTerm != electionTerm {
        return false // stepped down while votes were being counted — abandon
    }
    rn.Role = Leader
    // ... reinitialize nextIndex[]/matchIndex[] (Figure 2)
    return true
}
```

---

## Log replication (§5.3)

Once elected, the leader is the sole entry point for writes. It appends each
client command to its log and replicates it via **AppendEntries**.

> _"The leader appends the command to its log as a new entry, then issues
> AppendEntries RPCs in parallel to each of the other servers to replicate the
> entry."_ — §5.3

Each AppendEntries carries `prevLogIndex` / `prevLogTerm` — the entry
*immediately before* the new ones. The follower accepts the batch **only if** its
log contains a matching entry at that position. This is the **Log Matching
Property**, and it's what guarantees that two logs agreeing at an index agree on
everything before it, too.

```
leader:   1(t1) 2(t1) 3(t2) 4(t2) 5(t3)
                          ↑ prevLogIndex=4, prevLogTerm=2, entries=[5(t3)]
follower: 1(t1) 2(t1) 3(t2) 4(t2)         ← has (4,t2)? yes → accept
follower: 1(t1) 2(t1) 3(t2)               ← has index 4? no  → reject, ask leader to back up
follower: 1(t1) 2(t1) 3(t2) 4(t9)         ← term mismatch at 4 → reject with conflict hint
```

Receiver logic: `RaftNode.HandleAppendEntries` does the term check and step-down;
the log-matching splice itself is `RaftLog.AppendEntriesToLog`.

**The conflict-hint optimization.** When a follower rejects, a naïve leader would
decrement `nextIndex` by one and retry — one RPC per index, painfully slow after a
long divergence. Instead the follower reports the *conflicting term* and the
*first index it holds for that term*, so the leader skips the whole conflicting
term in a single hop (`RaftNode.BacktrackNextIndex`). This is the §5.3 optimization
the paper mentions in a footnote.

**Idempotency matters.** A retransmitted or heartbeat batch that the follower
already has must be a **no-op** — it must not truncate and rewrite the WAL for
entries it already agrees on. `AppendEntriesToLog` only truncates on a genuine
term conflict; matching entries are skipped with no disk write. Getting this wrong
silently corrupts a follower's log on the first real split-brain (a bug that hid
for two phases here because live testing only ever exercised the missing-entry
path, never a true term conflict).

---

## Safety (§5.4) — the part that's easy to get wrong

Replication makes logs *agree*; safety makes sure they agree on the *right*
thing. Two rules do the heavy lifting.

### The election restriction (§5.4.1)

A candidate can only win if its log is **at least as up-to-date** as a majority.
This guarantees the new leader already has every committed entry, so leaders never
need to *receive* missing committed entries — the log only ever flows leader →
follower.

> _"Raft … guarantees that all the committed entries from previous terms are
> present on each new leader from the moment of its election."_ — §5.4

"Up-to-date" is defined precisely: compare last-entry terms; if they tie, the
longer log wins. That's `candidateUpToDate` in [`node.go`](../internal/raft/node.go):

```go
func candidateUpToDate(candTerm, candIndex, voterTerm, voterIndex int) bool {
    if candTerm != voterTerm {
        return candTerm > voterTerm
    }
    return candIndex >= voterIndex
}
```

A voter runs this before granting, inside `HandleRequestVote`.

### The Figure 8 commit rule (§5.4.2)

This is the trap. A leader replicates an entry from a **previous** term to a
majority. Is it committed? **No — not yet.** Committing it on replication-count
alone lets a *different* future leader, one that didn't have that entry,
legitimately overwrite it. Figure 8 in the paper walks through the exact sequence.

> _"To eliminate problems like the one in Figure 8, Raft never commits log entries
> from previous terms by counting replicas. Only log entries from the leader's
> current term are committed by counting replicas."_ — §5.4.2

The fix in `AdvanceCommitIndex`: find the highest index replicated on a majority
(the quorum index), but only advance `commitIndex` to it if **that entry's own
term equals the current term.** Older-term entries commit *implicitly*, carried
along once a current-term entry above them reaches quorum.

```go
sort.Sort(sort.Reverse(sort.IntSlice(indices)))
quorumIndex := indices[len(indices)/2]                 // highest index on a majority
if quorumIndex > rn.CommitIndex && termAt(quorumIndex) == currentTerm {
    rn.CommitIndex = quorumIndex                        // safe to commit
}
```

The bug this replaced checked the *leader's last entry's* term instead of the
*quorum index's* term — which is exactly the Figure 8 violation. It's a one-token
difference in the code and the whole difference between safe and unsafe in
practice.

### The election no-op (§8)

Because previous-term entries only commit once a current-term entry sits above
them, a brand-new leader with a pile of uncommitted previous-term entries would
have to wait for the next client write to commit them. So on winning, the leader
immediately appends a **no-op** entry in its own term
([`OpNoop`](../internal/raft/types.go)). Committing that no-op drags all the
prior-term entries below it across the commit frontier.

---

## Persistence & durability

| What | Where | Why it must survive a crash |
|------|-------|-----------------------------|
| Log entries | `raft.wal` (framed, fsync'd) | A committed entry that vanishes breaks the state-machine safety property |
| `currentTerm`, `votedFor` | `raft.meta` (temp+rename) | Voting twice in one term elects two leaders |
| State machine | `snapshot.json` | Lets a node skip replaying the whole log |

Raft's Figure 2 is explicit that these are written to **stable storage before
responding to RPCs** — Crystal honors this: `HandleRequestVote` persists the vote
before replying, and `BecomeFollower`/`BecomeCandidate` persist the term before
acting. See [ARCHITECTURE.md](ARCHITECTURE.md#durability-the-write-ahead-log) for
the WAL format and the O_APPEND / temp-file-rewrite gotcha.

---

## Log compaction (§7)

The log can't grow forever. Once entries are applied, their effect lives in the
state machine, so the prefix can be replaced by a **snapshot**.

> _"Snapshotting is the simplest approach to compaction. In snapshotting, the
> entire current system state is written to a snapshot on stable storage, then the
> entire log up to that point is discarded."_ — §7

Crystal snapshots the state machine (`SnapshotManager.Write`) and truncates the
covered WAL prefix (`RaftLog.TruncateBeforeIndex`). After that the log no longer
starts at index 1, which is why every index↔cache translation goes through an
offset (`cacheIdx = index - firstIndex`), and why the snapshot boundary's term is
retained after its entry is gone.

**InstallSnapshot (Figure 13).** A follower that's fallen behind the compacted
boundary can't be caught up by shipping log entries — they've been discarded. The
leader ships the whole snapshot instead. The replicator picks this automatically:

```go
if nextIndex < e.raftLog.FirstIndex() {
    // the entries this peer needs are gone; send the snapshot
    followerTerm = e.replicator.InstallSnapshotTo(...)
} else {
    followerTerm = e.replicator.ReplicateTo(...)
}
```

The receiver (`RaftNode.HandleInstallSnapshot`) follows Figure 13: term check,
restore the state machine (step 8), reconcile the log — retain the suffix if the
snapshot is a prefix we already hold, else discard the whole log (steps 6–7) — and
persist. Crystal sends the snapshot in one RPC rather than chunked, because its
snapshots are tiny JSON maps, so the paper's `offset`/`done` fields collapse away.

Integration test `TestFollowerCatchesUpViaSnapshot` proves the end-to-end path: a
follower kept offline past a compaction rejoins and is made whole by a snapshot,
not by log replay.

---

## What Crystal deliberately leaves out

The paper covers more than a first correct implementation needs. Crystal stops at
the feature-complete safety core (Figure 2 + §5 + §7) and skips, for now:

- **Cluster membership changes (§6).** Joint consensus, where two configurations'
  majorities overlap during a transition. Crystal's cluster set is fixed at
  startup.
- **Pre-vote.** A partitioned node that rejoins can still bump the term and force
  an unnecessary election. Pre-vote (a probe round before really incrementing the
  term) avoids that churn.
- **Leadership transfer.** Gracefully handing leadership to a specific node.
- **Client linearizability details (§8).** Read leases, dedup of retried client
  commands. Crystal serves reads from any node's state machine, which is fast but
  can return slightly stale data under a partition.

These are the natural next steps, and each has a clean place to slot into the
existing layers — which was the point of drawing them where they are.

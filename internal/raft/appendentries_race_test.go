package raft

import (
	"sync"
	"testing"
	"time"
)

// setTermDecisionHook installs fn for the duration of one test. The hook is a
// package-level var, so tests using it must not run in parallel with each other.
func setTermDecisionHook(t *testing.T, fn func(reqTerm int)) {
	t.Helper()
	termDecisionHook = fn
	t.Cleanup(func() { termDecisionHook = nil })
}

// The AppendEntries receiver decides, in Figure 2 step 1, whether the sender's
// term entitles it to mutate our log. If that decision and the mutation it
// authorizes are not one atomic step, a request that was rejected in principle
// can still be applied in practice.
//
// The scenario below is built so that both LEGAL interleavings converge on the
// same final state, which makes the illegal one unambiguous:
//
//	node starts at term 5 with an empty log
//	A sends AppendEntries(term 7) carrying entry 1@7
//	B sends AppendEntries(term 6) carrying entry 1@6
//
//	B then A -> B accepted at term 6 (entry 1@6), A accepted at term 7,
//	            conflict at index 1, truncate and replace -> term 7, entry 1@7
//	A then B -> A accepted at term 7 (entry 1@7), B rejected (6 < 7)
//	                                              -> term 7, entry 1@7
//
// Either way: term 7, entry 1@7. The only way to end at term 7 with entry 1@6 is
// for B to pass the term check against a currentTerm it read before A raised it,
// and then splice anyway.

// A TOCTOU cannot be demonstrated by racing goroutines and hoping: the window is
// a few microseconds wide and a passing run proves nothing. termDecisionHook
// (see node.go) lets the test stall the slow request at exactly the point where
// it has formed its view of currentTerm, run the fast request to completion, and
// then release it. The stall is a sleep rather than a rendezvous because after
// the fix the hook fires while rn.mu is held — a rendezvous would deadlock, and
// that deadlock would itself be the proof that the decision is now atomic.
func TestHandleAppendEntries_ConcurrentTermsRejectStale(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)
	if err := rn.BecomeFollower(5, 2); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	// Stall only the term-6 request, long enough for the term-7 one to finish.
	setTermDecisionHook(t, func(reqTerm int) {
		if reqTerm == 6 {
			time.Sleep(150 * time.Millisecond)
		}
	})

	mkReq := func(term int) AppendEntriesRequest {
		return AppendEntriesRequest{
			Term:         term,
			LeaderID:     2,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{{Index: 1, Term: term, Command: []byte(`{"op":"noop"}`)}},
			LeaderCommit: 0,
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rn.HandleAppendEntries(rl, mkReq(6)) // stalls inside the hook
	}()

	// Give the stalled request time to reach the hook, then run the newer term.
	time.Sleep(20 * time.Millisecond)
	rn.HandleAppendEntries(rl, mkReq(7))
	wg.Wait()

	if got := rn.CurrentTerm(); got != 7 {
		t.Fatalf("currentTerm = %d, want 7", got)
	}

	entry, ok := rl.GetEntry(1)
	if !ok {
		t.Fatal("entry 1 missing; one of the two appends must win")
	}
	if entry.Term != 7 {
		t.Fatalf("entry 1 has term %d at currentTerm 7 — a stale-term AppendEntries "+
			"mutated the log after the node had moved past it", entry.Term)
	}
}

// A request whose term is below ours must be rejected outright and must leave the
// log untouched, whatever else is happening. The sequential case, for a clear
// failure message when the concurrent one regresses.
func TestHandleAppendEntries_StaleTermDoesNotMutateLog(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)
	if err := rn.BecomeFollower(7, 2); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	resp := rn.HandleAppendEntries(rl, AppendEntriesRequest{
		Term:         6,
		LeaderID:     3,
		PrevLogIndex: 0,
		Entries:      []LogEntry{{Index: 1, Term: 6, Command: []byte(`{"op":"noop"}`)}},
	})

	if resp.Success {
		t.Fatal("stale-term AppendEntries reported Success")
	}
	if resp.Term != 7 {
		t.Fatalf("resp.Term = %d, want 7", resp.Term)
	}
	if _, ok := rl.GetEntry(1); ok {
		t.Fatal("stale-term AppendEntries wrote an entry")
	}
}

// The same atomicity requirement applies to InstallSnapshot, which likewise
// decides on the term and then destroys log state on the strength of it.
//
// Unlike AppendEntries, this one cannot be pinned to a single expected outcome.
// Both legal interleavings are genuinely different and genuinely valid: a
// term-6 snapshot arriving at a term-5 node is legitimate, and if it lands first
// the later term-7 snapshot at a lower index is correctly skipped as already
// covered. Final state alone therefore cannot distinguish "term-6 won the race
// fairly" from "term-6 acted after losing it".
//
// What must hold in every interleaving is that the log's snapshot boundary and
// the term recorded at that boundary come from the SAME request. A torn result —
// one request's index paired with the other's term — is only reachable if the
// decision and the mutation came apart.
func TestHandleInstallSnapshot_ConcurrentTermsStayConsistent(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)
	if err := rn.BecomeFollower(5, 2); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	setTermDecisionHook(t, func(reqTerm int) {
		if reqTerm == 6 {
			time.Sleep(150 * time.Millisecond)
		}
	})

	noop := func([]byte) error { return nil }
	persist := func() error { return nil }
	install := func(term, index int) {
		rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
			Term:              term,
			LeaderID:          2,
			LastIncludedIndex: index,
			LastIncludedTerm:  term,
		}, noop, persist)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		install(6, 30)
	}()

	time.Sleep(20 * time.Millisecond)
	install(7, 20)
	wg.Wait()

	if got := rn.CurrentTerm(); got != 7 {
		t.Fatalf("currentTerm = %d, want 7", got)
	}

	boundary := rl.FirstIndex() - 1
	boundaryTerm := rl.TermAt(boundary)
	switch {
	case boundary == 30 && boundaryTerm == 6: // term-6 snapshot won the race
	case boundary == 20 && boundaryTerm == 7: // term-7 snapshot won the race
	default:
		t.Fatalf("snapshot boundary %d carries term %d — index and term came from "+
			"different requests", boundary, boundaryTerm)
	}

	// commit/applied must match whichever snapshot actually landed.
	commit, applied := rn.CommitAndApplyBoundary()
	if commit != boundary || applied != boundary {
		t.Fatalf("commit/applied = %d/%d, want %d/%d to match the installed snapshot",
			commit, applied, boundary, boundary)
	}
}

// The sequential guarantee that the concurrent test cannot express: once the node
// is at term 7, a term-6 snapshot must be rejected outright and must not touch
// the log, no matter how far ahead its index is.
func TestHandleInstallSnapshot_StaleTermDoesNotInstall(t *testing.T) {
	rn := newTestNode(t, 1, []int{2, 3}, 3)
	rl := newTestLog(t)
	if err := rn.BecomeFollower(7, 2); err != nil {
		t.Fatalf("seed term: %v", err)
	}

	installed := false
	resp := rn.HandleInstallSnapshot(rl, InstallSnapshotRequest{
		Term: 6, LeaderID: 3, LastIncludedIndex: 30, LastIncludedTerm: 6,
	},
		func([]byte) error { installed = true; return nil },
		func() error { return nil },
	)

	if resp.Term != 7 {
		t.Fatalf("resp.Term = %d, want 7", resp.Term)
	}
	if installed {
		t.Fatal("stale-term snapshot was restored into the state machine")
	}
	if got := rl.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1 — stale snapshot moved the boundary", got)
	}
	if commit, applied := rn.CommitAndApplyBoundary(); commit != 0 || applied != 0 {
		t.Fatalf("commit/applied = %d/%d, want 0/0", commit, applied)
	}
}

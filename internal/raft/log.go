package raft

// RaftLog owns the Write-Ahead Log (WAL) and the in-memory log cache.
// It is the single source of truth for log entries in the Raft layer.
//
// WAL format (per entry):
//   [4 bytes big-endian uint32 length][N bytes JSON-encoded LogEntry]
//
// This framed format allows reliable recovery: we read the length prefix
// first, then read exactly that many bytes. A partial write (e.g. crash
// mid-write) is detected as an unexpected EOF and truncated safely.
//
// Compaction: when a snapshot is taken at index S, all entries with
// Index <= S are no longer needed for replication. TruncateBeforeIndex
// rewrites the WAL from entry S+1 onward, keeping disk usage bounded.
//
// Index offset (review bug #4): the cache does NOT start at log index 1 after a
// compaction. cache[0] holds the entry at firstIndex, where
// firstIndex == lastIncludedIndex + 1. All index↔cache translation therefore
// goes through cacheIdx = index - firstIndex. The entry at lastIncludedIndex
// itself is no longer in the cache, but its term (lastIncludedTerm) is retained
// so the AppendEntries consistency check for the first post-snapshot entry still
// works (Figure 12).

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"crystal/internal/fsutil"
)

const defaultCompactionThreshold = 1000

// RaftLog manages the durable log of Raft entries.
type RaftLog struct {
	mu sync.RWMutex

	walFile   *os.File
	walPath   string
	cache     []LogEntry // in-memory mirror of the WAL; cache[0] == firstIndex
	nextIndex int        // next index to assign on AppendLeader

	// Snapshot offset. lastIncludedIndex/Term describe the entry the latest
	// snapshot replaced; firstIndex == lastIncludedIndex + 1 is the log index of
	// cache[0]. Before any compaction these are 0/0/1 (a fresh log starts at 1).
	firstIndex        int
	lastIncludedIndex int
	lastIncludedTerm  int

	// compactionThreshold is the number of committed entries after which
	// a snapshot should be triggered to keep the WAL bounded.
	compactionThreshold int
}

// NewRaftLog opens (or creates) the WAL at walPath and recovers any existing
// entries into the in-memory cache.
//
// Recovery runs first on a plain O_RDWR handle (not O_APPEND) so that a partial
// trailing frame from a crash mid-write can be truncated — Truncate is rejected
// on an O_APPEND handle on Windows (review bug #5). Only after recovery has
// fixed up the file do we open the durable append handle used for all writes.
func NewRaftLog(walPath string) (*RaftLog, error) {
	rl := &RaftLog{
		walPath:             walPath,
		cache:               make([]LogEntry, 0),
		nextIndex:           1,
		firstIndex:          1, // no compaction yet: log starts at index 1
		compactionThreshold: defaultCompactionThreshold,
	}

	if err := rl.recover(); err != nil {
		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	rl.walFile = file

	return rl, nil
}

// recover reads all framed entries from the WAL into the in-memory cache. It
// handles partial trailing writes (crash mid-write) by stopping at the first
// read error and truncating the file to the last known-good position.
//
// It opens its own O_RDWR (non-append) handle so the in-place Truncate works on
// all platforms, and closes it before returning; NewRaftLog then reopens the
// append handle for normal operation.
func (rl *RaftLog) recover() error {
	f, err := os.OpenFile(rl.walPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open WAL for recovery: %w", err)
	}
	defer f.Close()

	var lastGoodPos int64

	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		var length uint32
		if err := binary.Read(f, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			// Partial frame: truncate to last good position and stop.
			log.Printf("[WAL] Partial frame detected at offset %d — truncating to last good position", pos)
			if truncErr := f.Truncate(lastGoodPos); truncErr != nil {
				return fmt.Errorf("truncate partial WAL frame: %w", truncErr)
			}
			break
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			log.Printf("[WAL] Partial payload detected at offset %d — truncating", pos)
			if truncErr := f.Truncate(lastGoodPos); truncErr != nil {
				return fmt.Errorf("truncate partial WAL payload: %w", truncErr)
			}
			break
		}

		var entry LogEntry
		if err := json.Unmarshal(buf, &entry); err != nil {
			return fmt.Errorf("corrupt WAL entry at offset %d: %w", pos, err)
		}

		// The first entry read fixes firstIndex: after a compaction the WAL no
		// longer starts at index 1. RestoreOffset (called before recover during
		// snapshot startup) sets lastIncludedIndex/Term; here we align firstIndex
		// to what the WAL actually contains.
		if len(rl.cache) == 0 {
			rl.firstIndex = entry.Index
		}

		rl.cache = append(rl.cache, entry)
		rl.nextIndex = entry.Index + 1
		lastGoodPos = pos + 4 + int64(length)
	}

	// Ensure the truncation is durable before we reopen for appends.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync WAL after recovery: %w", err)
	}

	log.Printf("[WAL] Recovered %d entries. NextIndex: %d", len(rl.cache), rl.nextIndex)
	return nil
}

// AppendLeader appends a new entry to the WAL as the cluster leader.
// It assigns the next available index and persists to disk before returning.
func (rl *RaftLog) AppendLeader(cmd []byte, term int) (LogEntry, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry := LogEntry{
		Index:   rl.nextIndex,
		Term:    term,
		Command: cmd,
	}

	if err := rl.writeEntryToDisk(entry); err != nil {
		return LogEntry{}, err
	}

	rl.cache = append(rl.cache, entry)
	rl.nextIndex++
	return entry, nil
}

// AppendEntriesToLog implements the log-mutation half of the follower's
// AppendEntries receiver (Figure 2, steps 2–4). It assumes term checks have
// already been done by the caller and focuses purely on the log:
//
//	Step 2: reject if the log has no entry at prevLogIndex matching prevLogTerm.
//	Step 3: for each new entry, if an existing entry conflicts (same index,
//	        different term), delete it and everything after it.
//	Step 4: append any new entries not already present.
//
// Idempotency matters: a heartbeat or a retransmitted batch that already
// matches the log must be a no-op — we must NOT truncate and rewrite the WAL
// for entries we already have, both for performance and to avoid needlessly
// destroying entries the leader still considers replicated.
//
// Returns (matchIndex, conflictTerm, conflictIndex, ok). When ok is false the
// consistency check failed and the conflict hints guide the leader's nextIndex
// backtracking (§5.3 optimization). matchIndex is the index of the last entry
// now guaranteed to match the leader's log.
func (rl *RaftLog) AppendEntriesToLog(prevLogIndex, prevLogTerm int, entries []LogEntry) (matchIndex, conflictTerm, conflictIndex int, ok bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	lastIndex := rl.lastIndexLocked()

	// Step 2: consistency check at prevLogIndex.
	// prevLogIndex == 0 means the leader is sending from the very start of the
	// log; there is no preceding entry to match, so the check trivially passes.
	if prevLogIndex > 0 {
		if prevLogIndex > lastIndex {
			// We are missing the entry entirely. Ask the leader to back up to
			// just past our log's end.
			return 0, 0, lastIndex + 1, false
		}
		// prevLogIndex may equal the snapshot boundary, where the term is
		// lastIncludedTerm and there is no cache entry.
		var prevTerm int
		if prevLogIndex == rl.lastIncludedIndex {
			prevTerm = rl.lastIncludedTerm
		} else {
			prev, _ := rl.getEntryLocked(prevLogIndex)
			prevTerm = prev.Term
		}
		if prevTerm != prevLogTerm {
			// Term mismatch at prevLogIndex. Report the conflicting term and the
			// first index we hold for that term so the leader can skip it in one hop.
			// Never walk below firstIndex — earlier entries are compacted away.
			ct := prevTerm
			ci := prevLogIndex
			for ci > rl.firstIndex {
				e, _ := rl.getEntryLocked(ci - 1)
				if e.Term != ct {
					break
				}
				ci--
			}
			return 0, ct, ci, false
		}
	}

	// Steps 3 & 4: splice in the new entries, truncating only on a real conflict.
	appended := 0
	for _, entry := range entries {
		existing, exists := rl.getEntryLocked(entry.Index)
		switch {
		case exists && existing.Term == entry.Term:
			// Already present and matching — skip (idempotent, no disk write).
			continue
		case exists && existing.Term != entry.Term:
			// Real conflict (§5.3 step 3): delete this and all following entries.
			// Cache offset: cache[entry.Index - firstIndex] is the conflicting slot.
			rl.cache = rl.cache[:entry.Index-rl.firstIndex]
			if err := rl.rewriteWALLocked(); err != nil {
				log.Printf("[WAL] rewrite after conflict truncation failed: %v", err)
				return 0, 0, 0, false
			}
			fallthrough
		default:
			// M2: the cache is positional — cache[i] holds index firstIndex+i, and
			// every index translation in this file depends on it. An append that
			// does not continue the sequence would break that mapping silently, and
			// from then on getEntryLocked returns the wrong entry for every index.
			// The consistency check above should make this unreachable; if it is
			// ever reached the log is not what we think it is, and continuing would
			// corrupt it further.
			if want := rl.firstIndex + len(rl.cache); entry.Index != want {
				log.Printf("[WAL] refusing non-contiguous append: entry index %d, "+
					"expected %d (firstIndex=%d, cached=%d)",
					entry.Index, want, rl.firstIndex, len(rl.cache))
				return 0, 0, 0, false
			}

			// M5: buffer the write; the batch is fsynced once below rather than
			// once per entry.
			if err := writeEntryTo(rl.walFile, entry); err != nil {
				log.Printf("[WAL] append follower entry failed: %v", err)
				return 0, 0, 0, false
			}
			rl.cache = append(rl.cache, entry)
			rl.nextIndex = entry.Index + 1
			appended++
		}
	}

	// M5: one fsync for the whole batch. Durability is unchanged — nothing is
	// acknowledged to the leader until this returns — but a 100-entry
	// AppendEntries costs one sync instead of a hundred. This matters more since
	// F1, because the node lock is held across this call.
	if appended > 0 {
		if err := rl.walFile.Sync(); err != nil {
			log.Printf("[WAL] fsync after follower append failed: %v", err)
			return 0, 0, 0, false
		}
	}

	// matchIndex is the last index we now agree with the leader on: either the
	// last entry we appended, or prevLogIndex if this was a bare heartbeat.
	matchIndex = prevLogIndex
	if len(entries) > 0 {
		matchIndex = entries[len(entries)-1].Index
	}
	return matchIndex, 0, 0, true
}

// GetEntry returns the LogEntry at the given 1-based index.
func (rl *RaftLog) GetEntry(index int) (LogEntry, bool) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.getEntryLocked(index)
}

func (rl *RaftLog) getEntryLocked(index int) (LogEntry, bool) {
	cacheIdx := index - rl.firstIndex
	if cacheIdx < 0 || cacheIdx >= len(rl.cache) {
		return LogEntry{}, false
	}
	return rl.cache[cacheIdx], true
}

// LatestIndex returns the index of the last entry in the log. When the cache is
// empty it falls back to lastIncludedIndex — after a full compaction the log's
// "end" is the snapshot boundary, not 0.
func (rl *RaftLog) LatestIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.lastIndexLocked()
}

func (rl *RaftLog) lastIndexLocked() int {
	if len(rl.cache) == 0 {
		return rl.lastIncludedIndex
	}
	return rl.cache[len(rl.cache)-1].Index
}

// LatestTerm returns the term of the last entry in the log. When the cache is
// empty it falls back to lastIncludedTerm (the snapshot boundary term), so a
// fully-compacted node still advertises the correct log term in elections.
func (rl *RaftLog) LatestTerm() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if len(rl.cache) == 0 {
		return rl.lastIncludedTerm
	}
	return rl.cache[len(rl.cache)-1].Term
}

// LastLogState returns the index and term of the last entry as one consistent
// pair, under a single lock acquisition.
//
// Callers need both halves to describe the same moment. Reading them through
// separate LatestIndex/LatestTerm calls can tear — an append between the two
// yields one entry's index paired with another's term — and the §5.4.1
// up-to-date comparison built on that pair would be comparing against a log
// state that never existed.
func (rl *RaftLog) LastLogState() (index, term int) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if len(rl.cache) == 0 {
		return rl.lastIncludedIndex, rl.lastIncludedTerm
	}
	last := rl.cache[len(rl.cache)-1]
	return last.Index, last.Term
}

// FirstIndex returns the lowest log index still present in the cache
// (lastIncludedIndex + 1). The leader uses it to decide whether a follower's
// nextIndex has fallen below the compacted boundary and needs a snapshot.
func (rl *RaftLog) FirstIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.firstIndex
}

// GetEntriesFrom returns a copy of all entries with Index >= startIndex.
// The leader uses this to build the Entries batch for an AppendEntries RPC
// starting at a follower's nextIndex. Returns nil if startIndex is past the end
// of the log (heartbeat case) or has been compacted away (caller must fall back
// to InstallSnapshot).
func (rl *RaftLog) GetEntriesFrom(startIndex int) []LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	cacheIdx := startIndex - rl.firstIndex
	if cacheIdx < 0 {
		// startIndex precedes the compacted boundary; the entries are gone.
		return nil
	}
	if cacheIdx >= len(rl.cache) {
		return nil
	}

	out := make([]LogEntry, len(rl.cache)-cacheIdx)
	copy(out, rl.cache[cacheIdx:])
	return out
}

// TermAt returns the term of the entry at index, or 0 if not found. It also
// answers for the snapshot boundary: TermAt(lastIncludedIndex) == lastIncludedTerm,
// which the AppendEntries consistency check needs for the first post-snapshot
// entry (Figure 12).
func (rl *RaftLog) TermAt(index int) int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if index == rl.lastIncludedIndex {
		return rl.lastIncludedTerm
	}
	entry, ok := rl.getEntryLocked(index)
	if !ok {
		return 0
	}
	return entry.Term
}

// SetCompactionThreshold overrides the number of cached entries after which
// compaction is triggered. A non-positive value is ignored (keeps the default).
func (rl *RaftLog) SetCompactionThreshold(n int) {
	if n <= 0 {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.compactionThreshold = n
}

// NeedsCompaction returns true when the log has grown past the threshold and
// there is something to compact. appliedIndex is the state machine's applied
// boundary — the highest index a snapshot may legally cover (§7) — so a log that
// is long but entirely unapplied is correctly left alone.
func (rl *RaftLog) NeedsCompaction(appliedIndex int) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.cache) >= rl.compactionThreshold && appliedIndex > 0
}

// TruncateBeforeIndex discards all entries with Index <= snapshotIndex from both
// the in-memory cache and the WAL, and advances the snapshot offset so the
// remaining entries stay addressable: firstIndex becomes snapshotIndex+1, and
// lastIncludedIndex/Term record the boundary.
//
// snapshotTerm is the term of the entry at snapshotIndex; the caller supplies it
// (via TermAt) because after truncation that entry is gone from the cache but its
// term is still needed for the post-snapshot consistency check (Figure 12).
//
// Called after a snapshot has been durably written, so those entries are no
// longer needed for log replay or peer catch-up via log shipping.
func (rl *RaftLog) TruncateBeforeIndex(snapshotIndex, snapshotTerm int) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if snapshotIndex < rl.firstIndex {
		// Nothing new to discard (already compacted at or past this point).
		return nil
	}

	newCache := make([]LogEntry, 0, len(rl.cache))
	for _, e := range rl.cache {
		if e.Index > snapshotIndex {
			newCache = append(newCache, e)
		}
	}
	rl.cache = newCache

	rl.lastIncludedIndex = snapshotIndex
	rl.lastIncludedTerm = snapshotTerm
	rl.firstIndex = snapshotIndex + 1

	return rl.rewriteWALLocked()
}

// ResetToSnapshot applies an incoming InstallSnapshot to the log (Figure 13,
// steps 6–7). Two cases:
//
//	Step 6: if we already hold an entry at lastIncludedIndex whose term matches
//	        lastIncludedTerm, the snapshot is a prefix of our log — retain the
//	        entries that follow it and just advance the compaction boundary.
//	Step 7: otherwise our log conflicts with or is entirely behind the snapshot;
//	        discard the whole log and start fresh at lastIncludedIndex+1.
//
// In both cases firstIndex/lastIncludedIndex/lastIncludedTerm are updated and the
// WAL is rewritten to match, so the on-disk log stays consistent with the cache.
func (rl *RaftLog) ResetToSnapshot(lastIncludedIndex, lastIncludedTerm int) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// If the snapshot is older than what we've already compacted, ignore it.
	if lastIncludedIndex <= rl.lastIncludedIndex {
		return nil
	}

	existing, ok := rl.getEntryLocked(lastIncludedIndex)
	if ok && existing.Term == lastIncludedTerm {
		// Step 6: retain the suffix after the boundary.
		newCache := make([]LogEntry, 0, len(rl.cache))
		for _, e := range rl.cache {
			if e.Index > lastIncludedIndex {
				newCache = append(newCache, e)
			}
		}
		rl.cache = newCache
	} else {
		// Step 7: discard the entire log.
		rl.cache = rl.cache[:0]
	}

	rl.lastIncludedIndex = lastIncludedIndex
	rl.lastIncludedTerm = lastIncludedTerm
	rl.firstIndex = lastIncludedIndex + 1
	if rl.nextIndex < rl.firstIndex {
		rl.nextIndex = rl.firstIndex
	}
	if len(rl.cache) > 0 {
		rl.nextIndex = rl.cache[len(rl.cache)-1].Index + 1
	}

	return rl.rewriteWALLocked()
}

// RestoreOffset seeds the snapshot boundary at startup, when a snapshot exists
// on disk, and reconciles whatever the WAL happens to contain against it.
//
// The WAL and the snapshot can legitimately disagree, because a snapshot is
// persisted BEFORE the entries it covers are discarded (see
// RaftNode.HandleInstallSnapshot). A crash in that window — or in the equivalent
// window during compaction — leaves a snapshot at index S with the WAL still
// holding entries at or below S. Recovery has to resolve that, or firstIndex
// describes entries the log does not have and every index translation after it
// is wrong.
//
// Three cases:
//
//   - WAL empty: align the boundary so the next append assigns S+1.
//   - WAL entirely at or below S: every entry is superseded. Drop them all.
//   - WAL extends past S: keep the contiguous run starting at S+1 and drop the
//     rest. A gap after S is not repairable here, so anything beyond it goes too;
//     the leader will re-ship it.
//
// The WAL is rewritten to match, so the next recovery sees the reconciled log
// rather than repeating this work — or worse, resurrecting the dropped entries.
func (rl *RaftLog) RestoreOffset(lastIncludedIndex, lastIncludedTerm int) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.lastIncludedIndex = lastIncludedIndex
	rl.lastIncludedTerm = lastIncludedTerm
	rl.firstIndex = lastIncludedIndex + 1

	if len(rl.cache) == 0 {
		rl.nextIndex = lastIncludedIndex + 1
		return nil
	}

	// Keep only the entries that continue the log contiguously from the snapshot
	// boundary.
	kept := make([]LogEntry, 0, len(rl.cache))
	expected := lastIncludedIndex + 1
	for _, e := range rl.cache {
		if e.Index != expected {
			continue
		}
		kept = append(kept, e)
		expected++
	}

	dropped := len(rl.cache) - len(kept)
	rl.cache = kept
	rl.nextIndex = expected

	if dropped == 0 {
		return nil
	}

	log.Printf("[WAL] Snapshot at index %d supersedes %d WAL entries — rewriting",
		lastIncludedIndex, dropped)
	return rl.rewriteWALLocked()
}

// Close flushes and closes the underlying WAL file.
func (rl *RaftLog) Close() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.walFile.Close()
}

// writeEntryToDisk appends entry to the WAL file and fsyncs it. This is the
// durable single-entry write on the leader/follower append path: fsync ensures
// the entry survives a crash before we acknowledge it (matching etcd's
// durability guarantee).
func (rl *RaftLog) writeEntryToDisk(entry LogEntry) error {
	if err := writeEntryTo(rl.walFile, entry); err != nil {
		return err
	}
	if err := rl.walFile.Sync(); err != nil {
		return fmt.Errorf("fsync WAL: %w", err)
	}
	return nil
}

// writeEntryTo writes a single framed entry ([4-byte big-endian length][JSON])
// to w without fsyncing. Shared by the durable append path and the WAL rewrite
// path (which fsyncs once at the end instead of per entry).
func writeEntryTo(w io.Writer, entry LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}

	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write WAL frame length: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write WAL payload: %w", err)
	}

	return nil
}

// rewriteWALLocked rewrites the WAL to contain exactly the entries currently in
// rl.cache. Used after follower conflict-truncation and after compaction.
// Caller must hold rl.mu (write).
//
// It writes a fresh file and atomically renames it over the old WAL rather than
// truncating in place. This is required for correctness on two counts:
//
//   - The WAL is opened with O_APPEND, under which every write goes to the end
//     of file regardless of seek position, and (on Windows) Truncate on the
//     append handle is rejected outright. In-place rewriting is therefore not an
//     option with the current handle.
//   - The temp-file-then-rename dance is crash-safe: a crash mid-rewrite leaves
//     the original WAL fully intact, so recovery never sees a half-written log.
//
// After the rename the old handle is closed and a new append handle is opened
// on the replacement file so subsequent AppendLeader/writeEntryToDisk calls
// continue to work.
func (rl *RaftLog) rewriteWALLocked() error {
	tmpPath := rl.walPath + ".rewrite"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("open WAL rewrite temp: %w", err)
	}

	for _, e := range rl.cache {
		if err := writeEntryTo(tmp, e); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync WAL rewrite temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close WAL rewrite temp: %w", err)
	}

	// Release the current handle before replacing the file (Windows cannot
	// rename over an open file).
	if err := rl.walFile.Close(); err != nil {
		return fmt.Errorf("close WAL before rewrite swap: %w", err)
	}
	if err := os.Rename(tmpPath, rl.walPath); err != nil {
		return fmt.Errorf("rename WAL rewrite into place: %w", err)
	}
	// The rename must itself be durable. The temp file's contents are fsynced
	// above, but until the directory entry is on stable storage a crash can
	// resurrect the pre-rewrite WAL — reviving entries this rewrite deliberately
	// discarded (a follower's conflicting suffix, or a compacted prefix).
	if err := fsutil.SyncDir(filepath.Dir(rl.walPath)); err != nil {
		return fmt.Errorf("fsync WAL directory after rewrite: %w", err)
	}

	// Reopen the append handle on the freshly written WAL.
	f, err := os.OpenFile(rl.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("reopen WAL after rewrite: %w", err)
	}
	rl.walFile = f
	return nil
}

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

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

const defaultCompactionThreshold = 1000

// RaftLog manages the durable log of Raft entries.
type RaftLog struct {
	mu sync.RWMutex

	walFile   *os.File
	walPath   string
	cache     []LogEntry // in-memory mirror of the WAL, index-1 aligned
	nextIndex int        // next index to assign on AppendLeader

	// compactionThreshold is the number of committed entries after which
	// a snapshot should be triggered to keep the WAL bounded.
	compactionThreshold int
}

// NewRaftLog opens (or creates) the WAL at walPath and recovers any
// existing entries into the in-memory cache.
func NewRaftLog(walPath string) (*RaftLog, error) {
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	rl := &RaftLog{
		walFile:             file,
		walPath:             walPath,
		cache:               make([]LogEntry, 0),
		nextIndex:           1,
		compactionThreshold: defaultCompactionThreshold,
	}

	if err := rl.recover(); err != nil {
		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	return rl, nil
}

// recover reads all framed entries from the WAL into the in-memory cache.
// It handles partial trailing writes (crash mid-write) by stopping at any
// read error after at least one successful entry, and truncating the file
// to the last known-good position.
func (rl *RaftLog) recover() error {
	if _, err := rl.walFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var lastGoodPos int64

	for {
		pos, err := rl.walFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		var length uint32
		if err := binary.Read(rl.walFile, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			// Partial frame: truncate to last good position and stop.
			log.Printf("[WAL] Partial frame detected at offset %d — truncating to last good position", pos)
			if truncErr := rl.walFile.Truncate(lastGoodPos); truncErr != nil {
				return fmt.Errorf("truncate partial WAL frame: %w", truncErr)
			}
			break
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(rl.walFile, buf); err != nil {
			log.Printf("[WAL] Partial payload detected at offset %d — truncating", pos)
			if truncErr := rl.walFile.Truncate(lastGoodPos); truncErr != nil {
				return fmt.Errorf("truncate partial WAL payload: %w", truncErr)
			}
			break
		}

		var entry LogEntry
		if err := json.Unmarshal(buf, &entry); err != nil {
			return fmt.Errorf("corrupt WAL entry at offset %d: %w", pos, err)
		}

		rl.cache = append(rl.cache, entry)
		rl.nextIndex = entry.Index + 1
		lastGoodPos = pos + 4 + int64(length)
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

// AppendFollower appends or overwrites an entry received from the leader.
// If the incoming entry conflicts with an existing cache entry at the same
// index, all entries from that index onward are discarded (Raft §5.3).
// The WAL is rewritten from scratch to reflect the truncated + new state.
func (rl *RaftLog) AppendFollower(entry LogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cacheIdx := entry.Index - 1 // cache is 0-based, log index is 1-based

	if cacheIdx < len(rl.cache) {
		// Conflict: truncate in-memory cache and rewrite WAL from scratch.
		rl.cache = rl.cache[:cacheIdx]
		if err := rl.rewriteWAL(); err != nil {
			return fmt.Errorf("rewrite WAL after truncation: %w", err)
		}
	}

	if err := rl.writeEntryToDisk(entry); err != nil {
		return err
	}

	rl.cache = append(rl.cache, entry)
	rl.nextIndex = entry.Index + 1
	return nil
}

// GetEntry returns the LogEntry at the given 1-based index.
func (rl *RaftLog) GetEntry(index int) (LogEntry, bool) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.getEntryLocked(index)
}

func (rl *RaftLog) getEntryLocked(index int) (LogEntry, bool) {
	cacheIdx := index - 1
	if cacheIdx < 0 || cacheIdx >= len(rl.cache) {
		return LogEntry{}, false
	}
	return rl.cache[cacheIdx], true
}

// LatestIndex returns the index of the last entry in the log, or 0 if empty.
func (rl *RaftLog) LatestIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if len(rl.cache) == 0 {
		return 0
	}
	return rl.cache[len(rl.cache)-1].Index
}

// TermAt returns the term of the entry at index, or 0 if not found.
// Used by quorum checks to validate that a candidate commit index
// was written in the current term (Raft leader completeness property).
func (rl *RaftLog) TermAt(index int) int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	entry, ok := rl.getEntryLocked(index)
	if !ok {
		return 0
	}
	return entry.Term
}

// NeedsCompaction returns true when the log has grown past the threshold.
// The engine calls this after each commit to decide whether to trigger
// a snapshot + log truncation cycle.
func (rl *RaftLog) NeedsCompaction(commitIndex int) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.cache) >= rl.compactionThreshold && commitIndex > 0
}

// TruncateBeforeIndex discards all entries with Index <= snapshotIndex
// from both the in-memory cache and the WAL file.
// Called after a snapshot has been durably written, so those entries
// are no longer needed for log replay or peer catch-up via log shipping.
func (rl *RaftLog) TruncateBeforeIndex(snapshotIndex int) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	newCache := make([]LogEntry, 0, len(rl.cache))
	for _, e := range rl.cache {
		if e.Index > snapshotIndex {
			newCache = append(newCache, e)
		}
	}
	rl.cache = newCache

	return rl.rewriteWAL()
}

// Close flushes and closes the underlying WAL file.
func (rl *RaftLog) Close() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.walFile.Close()
}

// writeEntryToDisk serializes entry and appends it to the WAL file
// with a 4-byte big-endian length prefix. Calls fsync for durability.
func (rl *RaftLog) writeEntryToDisk(entry LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}

	length := uint32(len(data))
	if err := binary.Write(rl.walFile, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write WAL frame length: %w", err)
	}

	if _, err := rl.walFile.Write(data); err != nil {
		return fmt.Errorf("write WAL payload: %w", err)
	}

	// fsync: ensures entry survives a crash before we acknowledge it.
	// This matches etcd's durability guarantee.
	if err := rl.walFile.Sync(); err != nil {
		return fmt.Errorf("fsync WAL: %w", err)
	}

	return nil
}

// rewriteWAL truncates the WAL file to zero and rewrites all entries
// currently in rl.cache. Used after follower truncation and compaction.
func (rl *RaftLog) rewriteWAL() error {
	if err := rl.walFile.Truncate(0); err != nil {
		return err
	}
	if _, err := rl.walFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for _, e := range rl.cache {
		if err := rl.writeEntryToDisk(e); err != nil {
			return err
		}
	}
	return nil
}

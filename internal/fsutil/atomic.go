// Package fsutil provides the durable-write primitive that every piece of
// Crystal's on-disk state depends on.
//
// Raft's correctness rests on a single storage claim: when a node says it has
// recorded something, that record survives a power cut. Figure 2 states it as
// "updated on stable storage before responding to RPCs". A write that has
// reached the page cache but not the platter does not satisfy that claim — and
// the failure is not a lost write, it is a node that comes back believing it
// never voted in a term it already voted in, which elects two leaders.
//
// Durably replacing a file takes four steps, and skipping any one of them
// silently reintroduces the hole:
//
//  1. write the new contents to a temp file in the SAME directory (a rename is
//     only atomic within a filesystem),
//  2. fsync the temp file, so its contents are on stable storage,
//  3. rename it over the target, which is atomic — readers see the old file or
//     the new one, never a half-written hybrid,
//  4. fsync the DIRECTORY, so the rename itself is on stable storage.
//
// Step 4 is the one that is usually missing. Without it the new file's contents
// are durable but the directory entry pointing at them may not be, so a crash
// can resurrect the previous version of the file.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// tempSuffix is appended to the target path to form the staging file. It lives
// in the same directory as the target so the rename stays within one filesystem.
const tempSuffix = ".tmp"

// WriteFileAtomic durably replaces the file at path with data, following the
// write/fsync/rename/fsync-dir sequence described in the package comment.
//
// On failure the file at path is left exactly as it was: the new contents only
// become visible at the rename, which is the last fallible step before the
// directory sync. A failed call may leave the temp file behind; the next
// successful call reuses and overwrites it.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + tempSuffix

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create temp file %s: %w", tmpPath, err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temp file %s: %w", tmpPath, err)
	}

	// Contents durable before the rename makes them reachable.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp file %s: %w", tmpPath, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename %s into place: %w", tmpPath, err)
	}

	// Rename durable, so a crash cannot resurrect the previous file.
	if err := SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("fsync directory of %s: %w", path, err)
	}

	return nil
}

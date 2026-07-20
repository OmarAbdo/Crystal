//go:build windows

package fsutil

// SyncDir is a no-op on Windows.
//
// The POSIX idiom — open the directory and fsync the handle — is not available:
// CreateFile on a directory requires FILE_FLAG_BACKUP_SEMANTICS, which Go's
// os.Open does not pass, and FlushFileBuffers on a directory handle is rejected
// outright. There is no supported way to flush a directory's metadata from user
// space.
//
// The gap this leaves is narrower than it looks. NTFS journals metadata
// operations, so a MoveFileEx that has returned is recorded in the log and is
// replayed on mount; the ordering guarantee we need from step 4 is provided by
// the journal rather than by us. It is not the same guarantee as an explicit
// fsync, and a node running on a volume with write caching enabled and no
// battery backing can still lose the rename — which is a reason to prefer Linux
// for anything durable, not a reason to fail every write on Windows.
//
// Returning nil keeps the contract uniform: callers always call SyncDir and
// always check the error, and the platform difference stays in one place.
func SyncDir(dir string) error {
	return nil
}

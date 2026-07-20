//go:build !windows

package fsutil

import (
	"fmt"
	"os"
)

// SyncDir fsyncs a directory so that renames and creations within it are
// durable. On POSIX filesystems a directory is a file, and its entries are only
// guaranteed on stable storage once that file has been fsynced.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory %s: %w", dir, err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync directory %s: %w", dir, err)
	}
	return nil
}

package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomic_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := WriteFileAtomic(path, []byte("hello"), 0o666); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("content = %q, want %q", got, "hello")
	}
}

func TestWriteFileAtomic_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := os.WriteFile(path, []byte("old and considerably longer"), 0o666); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := WriteFileAtomic(path, []byte("new"), 0o666); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	// A short write over a long file must not leave the tail of the old one.
	if string(got) != "new" {
		t.Fatalf("content = %q, want %q", got, "new")
	}
}

// The temp file is an implementation detail of the rename dance; it must never
// survive a successful write, or a directory listing of the data dir grows a
// stale .tmp per term change.
func TestWriteFileAtomic_LeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := WriteFileAtomic(path, []byte("x"), 0o666); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("directory contains %v, want exactly [state.json]", names)
	}
}

// A failed write must leave the previous version of the file untouched. This is
// the whole point of writing to a temp file first: the Raft metadata record on
// disk is either the old term or the new one, never a truncated hybrid.
func TestWriteFileAtomic_FailureLeavesOriginalIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := os.WriteFile(path, []byte("original"), 0o666); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Occupy the temp path with a directory so creating the temp file fails.
	if err := os.Mkdir(path+tempSuffix, 0o777); err != nil {
		t.Fatalf("block temp path: %v", err)
	}

	if err := WriteFileAtomic(path, []byte("replacement"), 0o666); err == nil {
		t.Fatal("WriteFileAtomic succeeded, want error")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("content = %q, want original file preserved", got)
	}
}

func TestWriteFileAtomic_MissingDirectoryErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-dir", "state.json")

	if err := WriteFileAtomic(path, []byte("x"), 0o666); err == nil {
		t.Fatal("WriteFileAtomic succeeded into a missing directory, want error")
	}
}

// SyncDir must succeed on a real directory on every platform we build for.
// On Windows it is a documented no-op; the contract is that callers can always
// call it and check the error.
func TestSyncDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}
}

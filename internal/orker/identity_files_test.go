package orker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readThrough reads a file through the fixed name a workload uses.
func readThrough(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// TestIdentityFiles_FirstWriteAndSwap: a token and its expiration are read
// through fixed names that never change; a refresh replaces both at once and
// keeps the set before it until the refresh after; the files are 0444, the
// directories 0555, and the token has no trailing newline.
func TestIdentityFiles_FirstWriteAndSwap(t *testing.T) {
	parent := t.TempDir()
	d, err := newIdentityFiles(parent, "aws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Remove() })
	exp1 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	if _, err := d.Swap([]byte("jwt-one"), exp1); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(parent, "aws")
	if got := readThrough(t, dir, "token"); got != "jwt-one" {
		t.Fatalf("token = %q", got)
	}
	if got := readThrough(t, dir, "expiration"); got != "2026-11-19T12:00:00Z" {
		t.Fatalf("expiration = %q", got)
	}
	for name, want := range map[string]string{"token": "current/token", "expiration": "current/expiration", "current": ".v1"} {
		if got, err := os.Readlink(filepath.Join(dir, name)); err != nil || got != want {
			t.Fatalf("%s -> %q, %v; want %q", name, got, err, want)
		}
	}
	assertMode(t, dir, 0o555)
	assertMode(t, filepath.Join(dir, ".v1"), 0o555)
	assertMode(t, filepath.Join(dir, ".v1", "token"), 0o444)
	assertMode(t, filepath.Join(dir, ".v1", "expiration"), 0o444)

	exp2 := exp1.Add(time.Hour)
	if _, err := d.Swap([]byte("jwt-two"), exp2); err != nil {
		t.Fatal(err)
	}
	if got := readThrough(t, dir, "token"); got != "jwt-two" {
		t.Fatalf("after a swap, token = %q", got)
	}
	if got, _ := os.Readlink(filepath.Join(dir, "current")); got != ".v2" {
		t.Fatalf("current -> %q, want .v2", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".v1")); err != nil {
		t.Fatalf("the set before the last refresh is kept until the refresh after: %v", err)
	}
	if _, err := d.Swap([]byte("jwt-three"), exp2.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".v1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".v1 survived two refreshes: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".v2")); err != nil {
		t.Fatalf(".v2 is the set before the last and is kept: %v", err)
	}
	assertMode(t, dir, 0o555)
}

// TestIdentityFiles_AFailedSwapLeavesTheOldSet: a refresh that cannot write
// its files leaves current on the old set, whole, and says so.
func TestIdentityFiles_AFailedSwapLeavesTheOldSet(t *testing.T) {
	parent := t.TempDir()
	d, err := newIdentityFiles(parent, "aws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Remove() })
	if _, err := d.Swap([]byte("jwt-one"), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	d.write = func(string, []byte) error { return errors.New("no space left on device") }
	if _, err := d.Swap([]byte("jwt-two"), time.Unix(2, 0)); err == nil || !strings.Contains(err.Error(), "no space") {
		t.Fatalf("a failed write = %v", err)
	}
	dir := filepath.Join(parent, "aws")
	if got := readThrough(t, dir, "token"); got != "jwt-one" {
		t.Fatalf("token after a failed swap = %q", got)
	}
	if got, _ := os.Readlink(filepath.Join(dir, "current")); got != ".v1" {
		t.Fatalf("current -> %q, want .v1", got)
	}
	assertMode(t, dir, 0o555)
	d.write = writeIdentityFile
	if _, err := d.Swap([]byte("jwt-three"), time.Unix(3, 0)); err != nil {
		t.Fatalf("the swap after a failed one: %v", err)
	}
	if got := readThrough(t, dir, "token"); got != "jwt-three" {
		t.Fatalf("token = %q", got)
	}
}

// TestIdentityFiles_Refusals: a name that is not one identity's, and a
// token that is empty or carries a newline, are refused before anything is
// written.
func TestIdentityFiles_Refusals(t *testing.T) {
	parent := t.TempDir()
	for _, name := range []string{"", ".", "..", "a/b", "Aws", ".v1", "-a", strings.Repeat("a", 64)} {
		if _, err := newIdentityFiles(parent, name); err == nil {
			t.Errorf("name %q was accepted", name)
		}
	}
	d, err := newIdentityFiles(parent, "aws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Remove() })
	for _, token := range []string{"", "a\nb", "jwt\n"} {
		if _, err := d.Swap([]byte(token), time.Unix(1, 0)); err == nil {
			t.Errorf("token %q was accepted", token)
		}
	}
	if _, err := os.Lstat(filepath.Join(parent, "aws", "current")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused token wrote a set: %v", err)
	}
}

// TestIdentityFiles_Remove: the sealed tree is made writable and removed.
func TestIdentityFiles_Remove(t *testing.T) {
	parent := t.TempDir()
	d, err := newIdentityFiles(parent, "aws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Remove() })
	for i := range 3 {
		if _, err := d.Swap([]byte("jwt"), time.Unix(int64(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(parent, "aws")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the identity's directory survived its removal: %v", err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}

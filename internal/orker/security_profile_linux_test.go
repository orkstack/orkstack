//go:build linux

package orker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The way through /proc that a link to an open descriptor takes exists only
// where /proc does, so the test of it is compiled for Linux alone, where a
// missing /proc is a failure of the host and never a skip.

// TestSecurityPolicy_ASourceWhoseWayPassesThroughAnOwnDirectoryIsTheOrkers: a
// source is the Orker's own when the kernel, opening it, looks a name up in
// one of the Orker's directories, wherever it ends. A link to one of the
// Orker's descriptors ends at a name the link's text gives, a deleted file's
// for instance, that lies in none of them; the way there passes through
// /proc, so it is the Orker's own. The rule is the same for every own
// directory, the work root included, and for a way that leaves one again by
// a link's "..": no lookup in one is judged by what it reads.
func TestSecurityPolicy_ASourceWhoseWayPassesThroughAnOwnDirectoryIsTheOrkers(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Fatalf("this host has no /proc/self/fd, which every Linux the Orker runs on mounts: %v", err)
	}
	dir := t.TempDir()
	gone, err := os.CreateTemp(dir, "deleted-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gone.Close() })
	if err := os.Remove(gone.Name()); err != nil {
		t.Fatal(err)
	}
	descriptor := filepath.Join(dir, "descriptor")
	if err := os.Symlink(fmt.Sprintf("/proc/self/fd/%d", gone.Fd()), descriptor); err != nil {
		t.Fatal(err)
	}
	devfd := filepath.Join(dir, "devfd")
	if err := os.Symlink(fmt.Sprintf("/dev/fd/%d", gone.Fd()), devfd); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(dir, "work")
	if err := os.MkdirAll(filepath.Join(work, "ork-run-task-1-x"), 0o755); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	through := filepath.Join(dir, "through")
	if err := os.Symlink(filepath.Join(work, "ork-run-task-1-x")+"/../../plain", through); err != nil {
		t.Fatal(err)
	}
	policy := DefaultSecurityPolicy().WithWorkRoot(work)
	assertOwn(t, policy, "file:"+descriptor, "file:"+devfd, "file:"+through)
	assertNotOwn(t, policy, "file:"+plain)
}

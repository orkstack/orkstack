//go:build linux

package orker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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
	must.OK(t, err)
	t.Cleanup(func() { _ = gone.Close() })
	must.OK(t, os.Remove(gone.Name()))
	descriptor := filepath.Join(dir, "descriptor")
	must.OK(t, os.Symlink(fmt.Sprintf("/proc/self/fd/%d", gone.Fd()), descriptor))
	devfd := filepath.Join(dir, "devfd")
	must.OK(t, os.Symlink(fmt.Sprintf("/dev/fd/%d", gone.Fd()), devfd))
	work := filepath.Join(dir, "work")
	must.OK(t, os.MkdirAll(filepath.Join(work, "ork-run-task-1-x"), 0o755))
	plain := filepath.Join(dir, "plain")
	must.OK(t, os.WriteFile(plain, []byte("x"), 0o600))
	through := filepath.Join(dir, "through")
	must.OK(t, os.Symlink(filepath.Join(work, "ork-run-task-1-x")+"/../../plain", through))
	policy := DefaultSecurityPolicy().WithWorkRoot(work)
	assertOwn(t, policy, "file:"+descriptor, "file:"+devfd, "file:"+through)
	assertNotOwn(t, policy, "file:"+plain)
}

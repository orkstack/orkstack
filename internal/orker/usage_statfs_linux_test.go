//go:build linux

package orker

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// TestCgroup2Mounted_AgreesWithTheHost: the statfs check says v2 exactly when
// this process's own cgroup list is the single unified "0::" line.
func TestCgroup2Mounted_AgreesWithTheHost(t *testing.T) {
	list, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatalf("no /proc/self/cgroup on Linux: %v", err)
	}
	h := hostCgroups()
	v2, err := h.v2(h.mount)
	if err != nil {
		t.Fatalf("no cgroup mount at %s: %v", h.mount, err)
	}
	unified := strings.HasPrefix(strings.TrimSpace(string(list)), "0::") && strings.Count(strings.TrimSpace(string(list)), "\n") == 0
	if v2 != unified {
		t.Errorf("cgroup2Mounted = %v, but /proc/self/cgroup is %q", v2, list)
	}
	if _, err := cgroup2Mounted(t.TempDir()); err != nil {
		t.Errorf("cgroup2Mounted of a plain directory = %v, want false and no error", err)
	} else if ok, _ := cgroup2Mounted(t.TempDir()); ok {
		t.Error("a plain directory reads as a cgroup v2 mount")
	}
}

// TestVerifyCgroupFS_RefusesWhatIsNotACgroup: a directory on any filesystem
// but cgroupfs is unverified, and this host's own cgroup mount is accepted.
func TestVerifyCgroupFS_RefusesWhatIsNotACgroup(t *testing.T) {
	f, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := verifyCgroupFS(f); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("a temp directory = %v, want errCgroupUnverified", err)
	}
	root, err := os.Open("/sys/fs/cgroup")
	if err != nil {
		// The refusal above is the case under test; the mount is extra.
		t.Logf("no cgroup mount here to verify: %v", err)
		return
	}
	defer root.Close()
	if err := verifyCgroupFS(root); err != nil {
		t.Fatalf("this host's cgroup mount = %v, want it accepted", err)
	}
}

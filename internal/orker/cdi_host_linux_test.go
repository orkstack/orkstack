//go:build linux

package orker

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestTrustedSpecDir: a root-owned directory chain without group or world
// write passes, through a root-owned link (/var/run -> /run); a directory a
// user owns, or one a link leads into from a writable place, does not.
func TestTrustedSpecDir(t *testing.T) {
	if err := trustedSpecDir("/etc"); err != nil {
		t.Fatalf("/etc refused: %v", err)
	}
	if _, err := os.Lstat("/var/run"); err == nil {
		if err := trustedSpecDir("/var/run"); err != nil {
			t.Fatalf("/var/run refused: %v", err)
		}
	}
	own := t.TempDir()
	if os.Geteuid() != 0 {
		if err := trustedSpecDir(own); err == nil {
			t.Fatalf("%s, owned by uid %d, passed", own, os.Geteuid())
		}
	}
	link := filepath.Join(own, "link")
	if err := os.Symlink("/etc", link); err != nil {
		t.Fatal(err)
	}
	if err := trustedSpecDir(link); err == nil {
		t.Fatalf("a link in a user's directory passed")
	}
}

// TestGIDOwnsForbidden: a group owning a block device is refused, and one
// owning nothing is not.
func TestGIDOwnsForbidden(t *testing.T) {
	var blockGID uint32
	found := false
	entries, _ := os.ReadDir("/dev")
	for _, e := range entries {
		info, err := os.Lstat(filepath.Join("/dev", e.Name()))
		if err == nil && info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice == 0 {
			if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Gid != 0 {
				blockGID, found = st.Gid, true
				break
			}
		}
	}
	if found {
		what, owns := gidOwnsForbidden(blockGID, "")
		if !owns || !strings.Contains(what, "block device") {
			t.Fatalf("gid %d owns a block device: got %q, %v", blockGID, what, owns)
		}
	}
	if what, owns := gidOwnsForbidden(4_000_000_000, ""); owns {
		t.Fatalf("an unused gid owns %q", what)
	}
	sock := filepath.Join(t.TempDir(), "docker.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if what, owns := gidOwnsForbidden(uint32(os.Getegid()), sock); !owns || !strings.Contains(what, "runtime socket") {
		t.Fatalf("the runtime socket's group: got %q, %v", what, owns)
	}
}

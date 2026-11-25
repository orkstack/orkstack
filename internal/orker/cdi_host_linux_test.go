//go:build linux

package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
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

// TestHashHostHook: a root-owned file under root-owned directories is hashed
// from the one open file; one a user owns, a directory, and a path that is not
// there are refused naming why (CDI_HOOK_PATH's cause).
func TestHashHostHook(t *testing.T) {
	const bin = "/usr/bin/true"
	raw, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("no %s on this host: %v", bin, err)
	}
	got, err := hashHostHook(bin)
	if err != nil {
		t.Fatalf("hashHostHook(%s) = %v", bin, err)
	}
	sum := sha256.Sum256(raw)
	if got.SHA256 != hex.EncodeToString(sum[:]) || !strings.HasPrefix(got.Resolved, "/") {
		t.Fatalf("hashHostHook(%s) = %+v, want the digest of its bytes", bin, got)
	}
	if _, err := hashHostHook("/usr/bin"); err == nil {
		t.Error("a directory passed")
	}
	if _, err := hashHostHook("/usr/bin/absent-" + t.Name()); err == nil {
		t.Error("a path that is not there passed")
	}
	if os.Geteuid() != 0 {
		own := filepath.Join(t.TempDir(), "hook")
		if err := os.WriteFile(own, raw, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := hashHostHook(own); err == nil || !strings.Contains(err.Error(), "root") {
			t.Errorf("a file of uid %d: %v, want a refusal naming root", os.Geteuid(), err)
		}
	}
}

// TestInspectHostMount: a mount source is resolved through links and
// classified by its own stat.
func TestInspectHostMount(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "lib.so")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.so")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	real, _ := filepath.EvalSymlinks(file)
	for _, tc := range []struct {
		path string
		kind mountKind
	}{{file, mountFile}, {link, mountFile}, {dir, mountDir}, {"/dev/null", mountDevice}} {
		got, err := inspectHostMount(tc.path)
		if err != nil || got.Kind != tc.kind {
			t.Errorf("inspectHostMount(%s) = %+v, %v, want kind %s", tc.path, got, err, tc.kind)
		}
		if tc.path == link && got.Resolved != real {
			t.Errorf("a link resolved to %s, want %s", got.Resolved, real)
		}
	}
	sock := filepath.Join(dir, "s")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Skipf("no unix socket here: %v", err)
	}
	defer l.Close()
	if got, err := inspectHostMount(sock); err != nil || got.Kind != mountSocket {
		t.Errorf("inspectHostMount(socket) = %+v, %v", got, err)
	}
	if _, err := inspectHostMount(filepath.Join(dir, "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a missing source: %v, want not-exist", err)
	}
}

// TestInspectCDIDevice: the CDI audit's inspection carries the node's
// permission bits, which the plain inspection leaves out.
func TestInspectCDIDevice(t *testing.T) {
	got, err := inspectCDIDevice("/dev/null")
	if err != nil || got.Mode == 0 || got.Mode != got.Mode.Perm() {
		t.Fatalf("inspectCDIDevice(/dev/null) = %+v, %v", got, err)
	}
	plain, _ := inspectHostDevice("/dev/null")
	if plain.Mode != 0 {
		t.Errorf("inspectHostDevice fills Mode: %+v", plain)
	}
}

// TestWalkTrust_HoldsEveryHopsDirectory: a mount source reached through a chain
// of links is judged by every directory the chain leads through, including one
// that only a link in the middle lies in, which neither the written path's
// directories nor the resolved path's include.
func TestWalkTrust_HoldsEveryHopsDirectory(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"a", "c", "d"} {
		if err := os.Mkdir(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(root, "d", "lib.so")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// a/l1 -> c/l2 (absolute), c/l2 -> ../d/lib.so (relative to c).
	if err := os.Symlink(filepath.Join(root, "c", "l2"), filepath.Join(root, "a", "l1")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../d/lib.so", filepath.Join(root, "c", "l2")); err != nil {
		t.Fatal(err)
	}
	written := filepath.Join(root, "a", "l1")
	var seen []string
	trustAll := func(p string) error { seen = append(seen, p); return nil }
	if why := walkTrust(written, trustAll); why != "" {
		t.Fatalf("walkTrust with everything trusted = %q", why)
	}
	for _, want := range []string{filepath.Join(root, "a"), filepath.Join(root, "a", "l1"), filepath.Join(root, "c"), filepath.Join(root, "c", "l2"), filepath.Join(root, "d"), file} {
		found := false
		for _, p := range seen {
			found = found || p == want
		}
		if !found {
			t.Errorf("walkTrust never judged %s; it judged %v", want, seen)
		}
	}
	for _, untrusted := range []string{filepath.Join(root, "c"), filepath.Join(root, "c", "l2"), filepath.Join(root, "d"), filepath.Join(root, "a", "l1")} {
		why := walkTrust(written, func(p string) error {
			if p == untrusted {
				return fmt.Errorf("%s is writable by its group", p)
			}
			return nil
		})
		if !strings.Contains(why, untrusted) {
			t.Errorf("with %s untrusted walkTrust = %q, want a refusal naming it", untrusted, why)
		}
	}
	// A loop is refused.
	if err := os.Symlink(filepath.Join(root, "loop2"), filepath.Join(root, "loop1")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "loop1"), filepath.Join(root, "loop2")); err != nil {
		t.Fatal(err)
	}
	if why := walkTrust(filepath.Join(root, "loop1"), trustAll); !strings.Contains(why, "too many") {
		t.Errorf("a link loop = %q", why)
	}
	// The real rule: a path under a user's own directory is not root's.
	if os.Geteuid() != 0 {
		if why := mountTrust(file, file); why == "" {
			t.Errorf("a file under %s, owned by uid %d, was trusted", root, os.Geteuid())
		}
	}
	if why := mountTrust("/usr/bin/true", "/usr/bin/true"); why != "" {
		if _, err := os.Stat("/usr/bin/true"); err == nil {
			t.Errorf("/usr/bin/true is not trusted: %s", why)
		}
	}
}

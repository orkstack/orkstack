//go:build linux

package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// trustedSpecDir refuses a CDI spec directory a non-root user could change
// (design §4.10.1): the directory and every ancestor, as written and as
// resolved, must be root-owned; a directory must not be group- or
// world-writable. A link is allowed only where it is root's, in a directory
// only root may write (as /var/run -> /run is), because the resolved chain is
// held to the same rule.
func trustedSpecDir(dir string) error {
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", dir, err)
	}
	for _, chain := range []string{filepath.Clean(dir), resolved} {
		for p := chain; ; p = filepath.Dir(p) {
			if err := trustedEntry(p); err != nil {
				return fmt.Errorf("spec directory %s: %w", dir, err)
			}
			if p == "/" {
				break
			}
		}
	}
	return nil
}

func trustedEntry(p string) error {
	info, err := os.Lstat(p)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: no owner on this platform", p)
	}
	if st.Uid != 0 {
		return fmt.Errorf("%s is owned by uid %d, not root", p, st.Uid)
	}
	if info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s is writable by its group or by everyone (%s)", p, info.Mode().Perm())
	}
	return nil
}

// groupNameOf is the gid's name from the host's group database; "" when it
// has none.
func groupNameOf(gid uint32) string {
	g, err := user.LookupGroupId(fmt.Sprint(gid))
	if err != nil {
		return ""
	}
	return g.Name
}

// gidOwnsForbidden names what gid owns that no gpu unit's group may: the
// runtime socket (socket, when set), /dev/mem, /dev/kmem, a console, a tty or
// a block device anywhere under /dev.
func gidOwnsForbidden(gid uint32, socket string) (string, bool) {
	if socket != "" {
		if g, ok := ownerGID(socket); ok && g == gid {
			return socket + ", the runtime socket", true
		}
	}
	var found string
	_ = filepath.WalkDir("/dev", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			switch p {
			case "/dev/shm", "/dev/mqueue", "/dev/hugepages":
				return fs.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil || info.Mode()&os.ModeDevice == 0 {
			return nil
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Gid != gid {
			return nil
		}
		name := filepath.Base(p)
		switch {
		case info.Mode()&os.ModeCharDevice == 0:
			found = p + ", a block device"
		case name == "mem" || name == "kmem" || name == "console" || name == "tty" ||
			strings.HasPrefix(name, "tty") || strings.HasPrefix(p, "/dev/pts/"):
			found = p + ", a memory, console or terminal device"
		default:
			return nil
		}
		return fs.SkipAll
	})
	return found, found != ""
}

func ownerGID(p string) (uint32, bool) {
	info, err := os.Stat(p)
	if err != nil {
		return 0, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Gid, true
}

// hostCDI is the audit's view of this host: its devices, groups and spec
// directories, with socket the container runtime's unix socket ("" when not
// known).
func hostCDI(drivers map[uint32]string, socket string) cdiHost {
	return cdiHost{
		inspect:      inspectCDIDevice,
		arch:         runtime.GOARCH,
		hookBinary:   hashHostHook,
		inspectMount: inspectHostMount,
		drivers:      drivers,
		groupName:    groupNameOf,
		forbiddenFor: func(gid uint32) (string, bool) { return gidOwnsForbidden(gid, socket) },
		trustedDir:   trustedSpecDir,
	}
}

// runtimeSocketPath is the container runtime's unix socket, as DOCKER_HOST
// names it or at Docker's default; a gpu unit's group may not own it.
func runtimeSocketPath() string {
	if host, ok := strings.CutPrefix(os.Getenv("DOCKER_HOST"), "unix://"); ok && host != "" {
		return host
	}
	return "/var/run/docker.sock"
}

// inspectCDIDevice is inspectHostDevice with the node's permission bits, which
// an NVIDIA spec's fileMode is held to.
func inspectCDIDevice(p string) (hostDevice, error) {
	dev, err := inspectHostDevice(p)
	if err != nil {
		return hostDevice{}, err
	}
	info, err := os.Lstat(dev.Resolved)
	if err != nil {
		return hostDevice{}, err
	}
	dev.Mode = info.Mode().Perm()
	return dev, nil
}

// maxHookBinary bounds the hook binary the Orker reads: NVIDIA's is about
// 7 MiB.
const maxHookBinary = 64 << 20

// hashHostHook resolves the hook binary at p, holds the file and every
// ancestor, as written and as resolved, to root's and not group- or
// world-writable (design §4.10 item 4: otherwise CDI_HOOK_PATH), and hashes
// the bytes it read from the one open file, never a path twice.
func hashHostHook(p string) (hostHook, error) {
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return hostHook{}, fmt.Errorf("resolve %s: %w", p, err)
	}
	for _, chain := range []string{filepath.Dir(filepath.Clean(p)), filepath.Dir(resolved)} {
		for d := chain; ; d = filepath.Dir(d) {
			if err := trustedEntry(d); err != nil {
				return hostHook{}, fmt.Errorf("hook directory: %w", err)
			}
			if d == "/" {
				break
			}
		}
	}
	f, err := os.OpenFile(resolved, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return hostHook{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return hostHook{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !ok:
		return hostHook{}, fmt.Errorf("%s: no owner on this platform", resolved)
	case !info.Mode().IsRegular():
		return hostHook{}, fmt.Errorf("%s is not a regular file", resolved)
	case st.Uid != 0:
		return hostHook{}, fmt.Errorf("%s is owned by uid %d, not root", resolved, st.Uid)
	case info.Mode().Perm()&0o022 != 0:
		return hostHook{}, fmt.Errorf("%s is writable by its group or by everyone (%s)", resolved, info.Mode().Perm())
	case info.Size() > maxHookBinary:
		return hostHook{}, fmt.Errorf("%s is larger than %d bytes", resolved, maxHookBinary)
	}
	sum := sha256.New()
	n, err := io.Copy(sum, io.LimitReader(f, maxHookBinary+1))
	if err != nil {
		return hostHook{}, err
	}
	// The size was checked from the stat; the extra byte read catches a file
	// that grew between the stat and the read.
	if n > maxHookBinary {
		return hostHook{}, fmt.Errorf("%s is larger than %d bytes", resolved, maxHookBinary)
	}
	return hostHook{Resolved: resolved, SHA256: hex.EncodeToString(sum.Sum(nil))}, nil
}

// mountTrust says why a mount source is not root's, or "": every entry the
// resolution of the written path walks through (each directory, each symlink
// and every directory a symlink's target leads through, hop by hop) and the
// file itself must be root-owned and not group- or world-writable
// (trustedEntry, the rule the hook binary is held to). The resolved path is
// walked too.
func mountTrust(written, resolved string) string {
	if why := walkTrust(written, trustedEntry); why != "" {
		return why
	}
	return walkTrust(resolved, trustedEntry)
}

// maxTrustHops bounds the symlinks walkTrust follows, as the kernel bounds them.
const maxTrustHops = 40

// walkTrust resolves p one component at a time, as the kernel does, and holds
// every directory it descends through, every symlink it follows and the final
// entry to trust. A symlink's target is walked from where the link lies (or from
// the root when absolute), so a directory that only a link in the middle of a
// chain leads through is held to the same rule as the directories above the
// written path, which a walk of the written and the resolved paths alone misses.
func walkTrust(p string, trust func(string) error) string {
	resolved := "/"
	parts := strings.Split(p, "/")
	for hops := 0; len(parts) > 0; {
		comp := parts[0]
		parts = parts[1:]
		switch comp {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}
		if err := trust(resolved); err != nil {
			return err.Error()
		}
		next := filepath.Join(resolved, comp)
		info, err := os.Lstat(next)
		if err != nil {
			return err.Error()
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = next
			continue
		}
		if hops++; hops > maxTrustHops {
			return "too many symbolic links in " + p
		}
		if err := trust(next); err != nil {
			return err.Error()
		}
		target, err := os.Readlink(next)
		if err != nil {
			return err.Error()
		}
		if filepath.IsAbs(target) {
			resolved = "/"
		}
		parts = append(strings.Split(target, "/"), parts...)
	}
	if err := trust(resolved); err != nil {
		return err.Error()
	}
	return ""
}

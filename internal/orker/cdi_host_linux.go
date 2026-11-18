//go:build linux

package orker

import (
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
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
		inspect:      inspectHostDevice,
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

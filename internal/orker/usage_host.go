package orker

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

// cgroupHost is where this host keeps processes' cgroup lists and the cgroup
// mount: /proc and /sys/fs/cgroup in production, a directory in a test.
type cgroupHost struct {
	proc  string
	mount string
	// v2 reports whether mount is the unified (v2) hierarchy (statfs).
	v2 func(mount string) (bool, error)
	// verify accepts a directory only on a cgroup filesystem (cgroupPin).
	verify cgroupVerify
}

// hostCgroups is this host's.
func hostCgroups() cgroupHost {
	return cgroupHost{proc: "/proc", mount: "/sys/fs/cgroup", v2: cgroup2Mounted, verify: verifyCgroupFS}
}

// locate finds the cgroup of the container with this id whose process is
// pid, and returns its reader. A pid this host has no record of (the
// container ended, or a remote daemon's) is gone or not visible; a rootless
// container on cgroup v1 has no cgroup to read.
func (h cgroupHost) locate(pid int, d dialect, id string) (cgroupReader, usageMethod, error) {
	if pid <= 0 {
		return nil, usageUnmeasured, fmt.Errorf("pid %d: %w", pid, errCgroupGone)
	}
	list, err := os.ReadFile(filepath.Join(h.proc, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		// No such process here: a container that already ended, or a pid
		// from another host's daemon. Neither is a cgroup this Orker sees.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, usageUnmeasured, fmt.Errorf("pid %d has no process on this host: %w", pid, errCgroupNotFound)
		}
		return nil, usageUnmeasured, fmt.Errorf("pid %d: %w: %v", pid, errCgroupNotFound, err)
	}
	v2, err := h.v2(h.mount)
	if err != nil {
		return nil, usageUnmeasured, fmt.Errorf("%s: %w: %v", h.mount, errCgroupNotFound, err)
	}
	if !v2 && d.rootless {
		return nil, usageUnmeasured, errNoCgroup
	}
	paths, err := parseProcCgroup(string(list), d, id)
	if err != nil {
		return nil, usageUnmeasured, err
	}
	if v2 {
		if paths.v2 == "" {
			return nil, usageUnmeasured, fmt.Errorf("no cgroup v2 line: %w", errCgroupNotFound)
		}
		dir := filepath.Join(h.mount, filepath.FromSlash(paths.v2))
		pin, err := pinAt(h.verify, dir)
		if err != nil {
			return nil, usageUnmeasured, err
		}
		return func(final bool) (cgroupReading, error) { return readCgroupV2(dir, final, pin) }, usageCgroupV2, nil
	}
	cpu, memory := paths.v1["cpuacct"], paths.v1["memory"]
	if cpu == "" || memory == "" {
		return nil, usageUnmeasured, fmt.Errorf("no cgroup v1 cpuacct and memory lines: %w", errCgroupNotFound)
	}
	cpuDir := filepath.Join(h.mount, "cpuacct", filepath.FromSlash(cpu))
	memoryDir := filepath.Join(h.mount, "memory", filepath.FromSlash(memory))
	pin, err := pinAt(h.verify, cpuDir, memoryDir)
	if err != nil {
		return nil, usageUnmeasured, err
	}
	return func(final bool) (cgroupReading, error) { return readCgroupV1(cpuDir, memoryDir, final, pin) }, usageCgroupV1, nil
}

// probeCgroupIdentity checks, at boot, that this host can verify a cgroup's
// at all: its cgroup mount must be a cgroup filesystem. A host whose mount
// is not (an Orker in a container with another filesystem mounted there)
// measures no attempt.
func probeCgroupIdentity(h cgroupHost) error {
	v2, err := h.v2(h.mount)
	if err != nil {
		return fmt.Errorf("%s: %w: %v", h.mount, errCgroupNotFound, err)
	}
	dir := h.mount
	if !v2 {
		dir = filepath.Join(h.mount, "memory")
	}
	_, err = verifiedDir(h.verify, dir)
	return err
}

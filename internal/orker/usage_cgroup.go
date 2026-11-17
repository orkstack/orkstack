package orker

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Measured usage (E94) is read from the cgroup the container runs in: found
// from /proc/<pid>/cgroup by the engine's exact leaf for its id (the
// dialect's cgroupLeaf), opened as a root at each read (the leaf names the
// container's id, which is never reused), and read file by file without
// following a link. A value that is missing where it is required, negative,
// out of range or unreadable is an error, never a zero.
//
// Why not github.com/containerd/cgroups/v3, which parses /proc/<pid>/cgroup
// and reads the v1 and v2 stat files: it was weighed and kept out (decision
// S3-B5, 2026-11-16). It reads each file by path, without O_NOFOLLOW and not
// through one opened directory, so a cgroup removed and made again at its
// path between two reads, or a link where a file should be, could feed one
// reading another cgroup's numbers. This reader opens the cgroup once as an
// os.Root, reads every file of a reading through that one fd with
// O_NOFOLLOW, and holds the directory to the identity pinned at locate
// (cgroupPin), so a reading is one cgroup's or none: the boundary the Orker
// keeps between a task's container and its own host.

var (
	// errCgroupNotFound: no line of /proc/<pid>/cgroup ends in the
	// container's leaf (CGROUP_NOT_VISIBLE).
	errCgroupNotFound = errors.New("the container's cgroup is not in the process's cgroup list")
	// errCgroupRefused: a line ends in the leaf but its path is not one to
	// open (relative, a traversal, a control character).
	errCgroupRefused = errors.New("the container's cgroup path is refused")
	// errCgroupGone: the cgroup directory no longer exists (the container
	// ended and its cgroup was removed).
	errCgroupGone = errors.New("the container's cgroup is gone")
	// errCgroupEmpty: cgroup.procs lists no process.
	errCgroupEmpty = errors.New("the container's cgroup has no process")
	// errCgroupRead: a required value is missing, or a value is malformed.
	errCgroupRead = errors.New("the container's cgroup could not be read")
	// errCgroupUnverified: the directory's identity cannot be verified: it
	// is not on a cgroup filesystem, or the kernel gives no handle for it
	// (CGROUP_UNVERIFIED). Never answered by device and inode alone.
	errCgroupUnverified = errors.New("the container's cgroup identity cannot be verified")
)

// cgroupPaths is where a container's cgroup is: the v2 path (from the "0::"
// line) and, on a v1 or hybrid host, each v1 controller's path.
type cgroupPaths struct {
	v2 string
	v1 map[string]string
}

// parseProcCgroup reads a /proc/<pid>/cgroup listing and keeps the lines whose
// path ends in the container's exact leaf as dialect d names it. A line that
// ends in the leaf but whose path is not absolute and plain is refused; one
// that ends elsewhere is not the container's.
func parseProcCgroup(content string, d dialect, id string) (cgroupPaths, error) {
	var out cgroupPaths
	found := false
	for line := range strings.SplitSeq(content, "\n") {
		hierarchy, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		controllers, path, ok := strings.Cut(rest, ":")
		if !ok || path == "" {
			continue
		}
		segs := strings.Split(path, "/")
		parent := ""
		if len(segs) > 1 {
			parent = segs[len(segs)-2]
		}
		if !d.cgroupLeaf(id, parent, segs[len(segs)-1]) {
			continue
		}
		if err := plainCgroupPath(path); err != nil {
			return cgroupPaths{}, err
		}
		found = true
		if hierarchy == "0" && controllers == "" {
			out.v2 = path
			continue
		}
		for controller := range strings.SplitSeq(controllers, ",") {
			if controller == "" || strings.HasPrefix(controller, "name=") {
				continue
			}
			if out.v1 == nil {
				out.v1 = map[string]string{}
			}
			out.v1[controller] = path
		}
	}
	if !found {
		return cgroupPaths{}, errCgroupNotFound
	}
	return out, nil
}

// plainCgroupPath holds a path to an absolute, plain shape: no "." or ".."
// segment, no empty segment, no control character.
func plainCgroupPath(path string) error {
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("%q is not absolute: %w", path, errCgroupRefused)
	}
	for _, r := range path {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%q has a control character: %w", path, errCgroupRefused)
		}
	}
	for _, seg := range strings.Split(path[1:], "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%q is not a plain path: %w", path, errCgroupRefused)
		}
	}
	return nil
}

// cgroupReading is one read of a container's cgroup.
type cgroupReading struct {
	cpuSeconds float64 // cumulative CPU time
	memory     int64   // current memory
	memoryPeak int64   // the cgroup's own peak when peakRead, else current
	peakRead   bool    // memoryPeak is the kernel's recorded peak
	limit      int64   // memory limit; 0 is none
	oomKills   int64   // processes the OOM killer ended in it
}

// cgroupFileMax bounds every cgroup file read: the files read here are a few
// hundred bytes.
const cgroupFileMax = 64 << 10

// cgroupDir is one opened cgroup directory, read without following a link.
type cgroupDir struct{ root *os.Root }

func openCgroupDir(dir string) (*cgroupDir, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%s: %w", dir, errCgroupGone)
		}
		return nil, fmt.Errorf("open %s: %w: %v", dir, errCgroupRead, err)
	}
	return &cgroupDir{root: root}, nil
}

func (c *cgroupDir) close() { _ = c.root.Close() }

// read reads one file; a missing file is fs.ErrNotExist, a link is refused,
// and a cgroup removed under the read (the container ended) is errCgroupGone.
func (c *cgroupDir) read(name string) ([]byte, error) {
	f, err := c.root.OpenFile(name, os.O_RDONLY|oNoFollow, 0)
	if err != nil {
		return nil, c.goneIf(err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, cgroupFileMax+1))
	if err != nil {
		return nil, c.goneIf(err)
	}
	if len(b) > cgroupFileMax {
		return nil, fmt.Errorf("%s is larger than %d bytes: %w", name, cgroupFileMax, errCgroupRead)
	}
	return b, nil
}

// goneIf turns a failure the cgroup's removal explains into errCgroupGone: a
// file vanished while the directory itself is gone, or ENODEV (a removed
// cgroup's open files). Any other failure, a missing file in a cgroup still
// there included, is returned as it is.
func (c *cgroupDir) goneIf(err error) error {
	if errors.Is(err, syscall.ENODEV) {
		return fmt.Errorf("%w: %v", errCgroupGone, err)
	}
	if errors.Is(err, fs.ErrNotExist) {
		if _, statErr := c.root.Stat("."); statErr != nil {
			return fmt.Errorf("%w: %v", errCgroupGone, err)
		}
		if _, statErr := c.root.Stat("cgroup.procs"); errors.Is(statErr, fs.ErrNotExist) {
			return fmt.Errorf("%w: %v", errCgroupGone, err)
		}
	}
	return err
}

// required reads a file that must be there.
func (c *cgroupDir) required(name string) ([]byte, error) {
	b, err := c.read(name)
	if errors.Is(err, errCgroupGone) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %v", name, errCgroupRead, err)
	}
	return b, nil
}

// optional reads a file that may be absent: found false then.
func (c *cgroupDir) optional(name string) ([]byte, bool, error) {
	b, err := c.read(name)
	if errors.Is(err, errCgroupGone) {
		return nil, false, err
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w: %v", name, errCgroupRead, err)
	}
	return b, true, nil
}

// hasProcess holds cgroup.procs to a non-empty list: a cgroup whose processes
// all ended (or one that is not the container's) reads nothing worth keeping.
func (c *cgroupDir) hasProcess() error {
	f, err := c.root.OpenFile("cgroup.procs", os.O_RDONLY|oNoFollow, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return errCgroupGone
	}
	if err != nil {
		if gone := c.goneIf(err); errors.Is(gone, errCgroupGone) {
			return gone
		}
		return fmt.Errorf("cgroup.procs: %w: %v", errCgroupRead, err)
	}
	defer f.Close()
	// Non-empty is the whole question: a few bytes answer it, however many
	// processes the container runs.
	head := make([]byte, 32)
	n, err := io.ReadFull(f, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		if gone := c.goneIf(err); errors.Is(gone, errCgroupGone) {
			return gone
		}
		return fmt.Errorf("cgroup.procs: %w: %v", errCgroupRead, err)
	}
	if len(bytes.TrimSpace(head[:n])) == 0 {
		return errCgroupEmpty
	}
	return nil
}

// cgroupReader reads an attempt's container's cgroup. final is the read
// after the container ended: an empty cgroup is then read for its totals,
// which the kernel keeps until the cgroup is removed, instead of ending the
// reading.
type cgroupReader func(final bool) (cgroupReading, error)

// cgroupPin holds the directories one attempt's reader resolved at locate,
// so a later read of a path whose cgroup was removed and made again — by
// anything, for any container — is the attempt's cgroup gone, never
// another's totals. A directory is held to its device and inode
// (os.SameFile), and that identity is sound only because of where it is: it
// is trusted only on a cgroup filesystem (kernfs), checked at locate, whose
// 64-bit node id is the inode number and is allocated cyclically, never
// reused within a boot. A directory anywhere else, where an inode number may
// be handed straight to the next directory made, is unverified and read as
// nothing (CGROUP_UNVERIFIED). The kernel's file handle is not taken: on the
// 64-bit kernels an Orker runs on (linux/amd64 and arm64), kernfs's handle
// is that same node id and would add no identity, while a host that refuses
// name_to_handle_at would measure nothing for no safety gained.
type cgroupPin struct {
	verify cgroupVerify
	seen   []os.FileInfo
}

// cgroupVerify accepts an opened directory only on a cgroup filesystem, or
// says errCgroupUnverified (verifyCgroupFS on Linux).
type cgroupVerify func(dir *os.File) error

// pinAt is a pin of dirs as they are now, at locate: each one verified on a
// cgroup filesystem and its identity taken before any read. A directory
// already gone is the container's cgroup gone.
func pinAt(verify cgroupVerify, dirs ...string) (*cgroupPin, error) {
	if verify == nil {
		return nil, fmt.Errorf("%w: no way to verify a cgroup on this host", errCgroupUnverified)
	}
	p := &cgroupPin{verify: verify}
	for _, dir := range dirs {
		info, err := verifiedDir(verify, dir)
		if err != nil {
			return nil, err
		}
		p.seen = append(p.seen, info)
	}
	return p, nil
}

// verifiedDir opens dir as a directory, without following a link, verifies
// it is on a cgroup filesystem, and returns its identity.
func verifiedDir(verify cgroupVerify, dir string) (os.FileInfo, error) {
	if verify == nil {
		return nil, fmt.Errorf("%w: no way to verify a cgroup on this host", errCgroupUnverified)
	}
	f, err := os.OpenFile(dir, os.O_RDONLY|oNoFollow|oDirectory, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s: %w", dir, errCgroupGone)
	}
	// A link, or something that is not a directory, where the cgroup
	// should be is never a cgroup to read.
	if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.ENOTDIR) {
		return nil, fmt.Errorf("%s is not a cgroup directory: %w: %v", dir, errCgroupRefused, err)
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w: %v", dir, errCgroupRead, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w: %v", dir, errCgroupRead, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory: %w", dir, errCgroupRefused)
	}
	if err := verify(f); err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	return info, nil
}

// check holds c, the reader's i-th directory, to the one pinned at locate:
// the same device and inode, so the same cgroup filesystem it was verified
// on and the same cgroup.
func (p *cgroupPin) check(i int, c *cgroupDir) error {
	if p == nil || p.verify == nil || i >= len(p.seen) {
		return fmt.Errorf("directory %d was never pinned: %w", i, errCgroupUnverified)
	}
	info, err := c.root.Stat(".")
	if err != nil {
		if gone := c.goneIf(err); errors.Is(gone, errCgroupGone) {
			return gone
		}
		return fmt.Errorf("stat the cgroup: %w: %v", errCgroupRead, err)
	}
	if !os.SameFile(p.seen[i], info) {
		return fmt.Errorf("the cgroup at this path is not the one pinned at locate: %w", errCgroupGone)
	}
	return nil
}

// admit holds c to having a process, or, on the final read, to existing:
// the totals of a cgroup whose processes all ended are the container's
// whole use.
func (c *cgroupDir) admit(final bool) error {
	err := c.hasProcess()
	if final && errors.Is(err, errCgroupEmpty) {
		return nil
	}
	return err
}

// readCgroupV2 reads a cgroup v2 directory: cpu.stat's usage_usec
// (required), memory.peak (else memory.current, then not exact),
// memory.current, memory.max ("max" is no limit) and memory.events'
// oom_kill.
// Every read is held to pin, the identity taken at locate.
func readCgroupV2(dir string, final bool, pin *cgroupPin) (cgroupReading, error) {
	c, err := openCgroupDir(dir)
	if err != nil {
		return cgroupReading{}, err
	}
	defer c.close()
	if err := pin.check(0, c); err != nil {
		return cgroupReading{}, err
	}
	if err := c.admit(final); err != nil {
		return cgroupReading{}, err
	}
	var r cgroupReading
	stat, err := c.required("cpu.stat")
	if err != nil {
		return cgroupReading{}, err
	}
	usec, ok, err := keyedValue(stat, "usage_usec")
	if err != nil || !ok {
		return cgroupReading{}, fmt.Errorf("cpu.stat usage_usec: %w", errCgroupRead)
	}
	r.cpuSeconds = float64(usec) / 1e6
	current, haveCurrent, err := c.optionalValue("memory.current")
	if err != nil {
		return cgroupReading{}, err
	}
	peak, havePeak, err := c.optionalValue("memory.peak")
	if err != nil {
		return cgroupReading{}, err
	}
	switch {
	case havePeak:
		r.memoryPeak, r.peakRead = peak, true
		r.memory = peak
	case haveCurrent:
		r.memoryPeak = current
	default:
		return cgroupReading{}, fmt.Errorf("neither memory.peak nor memory.current: %w", errCgroupRead)
	}
	if haveCurrent {
		r.memory = current
	}
	if r.limit, err = c.limit("memory.max", "max"); err != nil {
		return cgroupReading{}, err
	}
	if r.oomKills, err = c.oomKills("memory.events"); err != nil {
		return cgroupReading{}, err
	}
	return r, nil
}

// readCgroupV1 reads the v1 cpuacct and memory controllers' directories:
// cpuacct.usage (nanoseconds, required), memory.max_usage_in_bytes (else
// memory.usage_in_bytes), memory.limit_in_bytes (the kernel's "no limit" is
// a value at or above 2^62) and memory.oom_control's oom_kill.
// Every read is held to pin, the identity taken at locate.
func readCgroupV1(cpuDir, memoryDir string, final bool, pin *cgroupPin) (cgroupReading, error) {
	cpu, err := openCgroupDir(cpuDir)
	if err != nil {
		return cgroupReading{}, err
	}
	defer cpu.close()
	memory, err := openCgroupDir(memoryDir)
	if err != nil {
		return cgroupReading{}, err
	}
	defer memory.close()
	if err := pin.check(0, cpu); err != nil {
		return cgroupReading{}, err
	}
	if err := pin.check(1, memory); err != nil {
		return cgroupReading{}, err
	}
	if err := memory.admit(final); err != nil {
		return cgroupReading{}, err
	}
	var r cgroupReading
	usage, err := cpu.required("cpuacct.usage")
	if err != nil {
		return cgroupReading{}, err
	}
	ns, err := plainValue(usage)
	if err != nil {
		return cgroupReading{}, fmt.Errorf("cpuacct.usage: %w", err)
	}
	r.cpuSeconds = float64(ns) / 1e9
	current, haveCurrent, err := memory.optionalValue("memory.usage_in_bytes")
	if err != nil {
		return cgroupReading{}, err
	}
	peak, havePeak, err := memory.optionalValue("memory.max_usage_in_bytes")
	if err != nil {
		return cgroupReading{}, err
	}
	switch {
	case havePeak:
		r.memoryPeak, r.peakRead = peak, true
	case haveCurrent:
		r.memoryPeak = current
	default:
		return cgroupReading{}, fmt.Errorf("neither memory.max_usage_in_bytes nor memory.usage_in_bytes: %w", errCgroupRead)
	}
	r.memory = current
	if !haveCurrent {
		r.memory = peak
	}
	if r.limit, err = memory.limit("memory.limit_in_bytes", ""); err != nil {
		return cgroupReading{}, err
	}
	if r.limit >= 1<<62 {
		r.limit = 0
	}
	if r.oomKills, err = memory.oomKills("memory.oom_control"); err != nil {
		return cgroupReading{}, err
	}
	return r, nil
}

func (c *cgroupDir) optionalValue(name string) (int64, bool, error) {
	b, ok, err := c.optional(name)
	if err != nil || !ok {
		return 0, false, err
	}
	v, err := plainValue(b)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %w", name, err)
	}
	return v, true, nil
}

// limit reads a memory limit; none (the file absent, or its no-limit word)
// is 0.
func (c *cgroupDir) limit(name, unlimited string) (int64, error) {
	b, ok, err := c.optional(name)
	if err != nil || !ok {
		return 0, err
	}
	if unlimited != "" && string(bytes.TrimSpace(b)) == unlimited {
		return 0, nil
	}
	v, err := plainValue(b)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

// oomKills reads the oom_kill counter of a keyed file; absent is 0.
func (c *cgroupDir) oomKills(name string) (int64, error) {
	b, ok, err := c.optional(name)
	if err != nil || !ok {
		return 0, err
	}
	v, _, err := keyedValue(b, "oom_kill")
	if err != nil {
		return 0, fmt.Errorf("%s oom_kill: %w", name, err)
	}
	return v, nil
}

// plainValue reads a file holding one non-negative integer that fits an
// int64.
func plainValue(b []byte) (int64, error) {
	return parseCount(string(bytes.TrimSpace(b)))
}

// keyedValue reads key's value from a "key value" per line file.
func keyedValue(b []byte, key string) (int64, bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		k, v, ok := strings.Cut(scanner.Text(), " ")
		if !ok || k != key {
			continue
		}
		n, err := parseCount(strings.TrimSpace(v))
		return n, true, err
	}
	return 0, false, scanner.Err()
}

func parseCount(s string) (int64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n > math.MaxInt64 {
		return 0, fmt.Errorf("%q is not a count: %w", s, errCgroupRead)
	}
	return int64(n), nil
}

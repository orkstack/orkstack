package orker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// The GPU host lock (design §4.7.5) is one directory, flock'ed exclusively for
// the Orker's life. Which process holds it is read from /proc/locks for the
// operator's diagnostic only, never for a decision: no established Go parser
// of /proc/locks exists (prometheus/procfs, the usual reader of /proc, has
// none), so parseProcLocks is a small strict one, fuzzed by FuzzParseProcLocks.

// errHostLockMismatch is the GPU host lock's fail-closed answer: the directory
// at the lock's path, or the one a probe sees, is not the one this Orker holds.
// It is drift (HOST_LOCK_MISMATCH), latched until a restart takes the new lock.
var errHostLockMismatch = errors.New("the GPU host lock directory is not the one this Orker holds")

// errHolderNotListed is a lookup, asked while the lock is held, that found no
// line of /proc/locks naming a holder of its file: the holder cannot be named.
var errHolderNotListed = errors.New("no line of /proc/locks names a holder of the lock directory")

// errProcLocksMalformed is parseProcLocks' only error.
var errProcLocksMalformed = errors.New("/proc/locks has a line this parser does not read")

// gpuHostLock is the held lock: its directory's fd, dev and inode, and the
// boot it was taken in.
type gpuHostLock struct {
	path     string
	fd       int
	dev, ino uint64
	boot     string
}

// identity is the lock's name on the containers bound under it
// (ork.units.lock): <boot-id>:<dev>:<inode>. Holding the flock proves every
// earlier holder of this identity has exited.
func (l *gpuHostLock) identity() string { return fmt.Sprintf("%s:%d:%d", l.boot, l.dev, l.ino) }

// view is the lock directory's path and the held fd's dev and inode.
func (l *gpuHostLock) view() (string, uint64, uint64) { return l.path, l.dev, l.ino }

// checkSeen fails closed unless dev and ino, as a probe container saw the lock
// directory through its read-only bind, are the held directory's.
func (l *gpuHostLock) checkSeen(dev, ino uint64) error {
	if dev != l.dev || ino != l.ino {
		return fmt.Errorf("%w: a probe sees %d:%d at %s, this Orker holds %d:%d",
			errHostLockMismatch, dev, ino, l.path, l.dev, l.ino)
	}
	return nil
}

// lockHolder is who held the lock when a waiter first found it held, for the
// control-channel detail. pids and comms are parallel; err says why none was
// found.
type lockHolder struct {
	pids  []int
	comms []string
	err   error
}

func (h lockHolder) String() string {
	if len(h.pids) == 0 {
		if h.err != nil {
			return "held by a process this Orker cannot name: " + h.err.Error()
		}
		return "held by a process this Orker cannot name"
	}
	parts := make([]string, len(h.pids))
	for i, pid := range h.pids {
		parts[i] = fmt.Sprintf("pid %d (%s)", pid, h.comms[i])
	}
	return "held by " + strings.Join(parts, ", ")
}

// devMajorMinor splits a Linux dev_t as the kernel prints it in /proc/locks,
// with glibc's encoding (each half 32 bits). golang.org/x/sys/unix's Major
// and Minor are not used: they follow the build's GOOS, and this file reads
// Linux's /proc on every build.
func devMajorMinor(dev uint64) (major, minor uint64) {
	major = uint64(uint32((dev>>8)&0xfff | (dev>>32)&^0xfff))
	minor = uint64(uint32(dev&0xff | (dev>>12)&^0xff))
	return major, minor
}

// makeDev is devMajorMinor's inverse: the dev_t of major and minor.
func makeDev(major, minor uint64) uint64 {
	return minor&0xff | (major&0xfff)<<8 | (minor&^0xff)<<12 | (major&^0xfff)<<32
}

// parseMajorMinor reads a mountinfo "major:minor" (both decimal) as a dev_t.
func parseMajorMinor(s string) (uint64, error) {
	major, minor, ok := strings.Cut(s, ":")
	maj, errMaj := strconv.ParseUint(major, 10, 32)
	mnr, errMin := strconv.ParseUint(minor, 10, 32)
	if !ok || errMaj != nil || errMin != nil || !isDecimal(major) || !isDecimal(minor) {
		return 0, fmt.Errorf("a mount device %q that is not major:minor", s)
	}
	return makeDev(maj, mnr), nil
}

// parseProcLocks returns the pids holding an flock(2) write lock on the inode
// ino of device dev, ascending, from a /proc/locks body. Waiters ("->"),
// other lock types and other files are skipped; a line of any other shape
// than the kernel's fails the whole read with errProcLocksMalformed.
//
// A line is: "<id>: [->] <type> <mode> <rw> <pid> <maj>:<min>:<ino> <start>
// <end>", fields separated by spaces, major and minor in hex, the rest in
// decimal, the end "EOF" or decimal, the pid -1 for an OFD lock and 0 for a
// holder outside the reader's pid namespace (neither can be named).
func parseProcLocks(r io.Reader, dev, ino uint64) ([]int, error) {
	wantMajor, wantMinor := devMajorMinor(dev)
	var holders []int
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if line == "" {
			continue
		}
		malformed := func(why string) error {
			return fmt.Errorf("%w: line %d %q: %s", errProcLocksMalformed, n, line, why)
		}
		var f []string
		for _, s := range strings.Split(line, " ") {
			if s != "" {
				f = append(f, s)
			}
		}
		if len(f) == 0 || !strings.HasSuffix(f[0], ":") || !isDecimal(strings.TrimSuffix(f[0], ":")) {
			return nil, malformed("no lock id")
		}
		waiter := len(f) > 1 && f[1] == "->"
		if waiter {
			f = f[2:]
		} else {
			f = f[1:]
		}
		if len(f) != 7 {
			return nil, malformed("not seven fields after the id")
		}
		for _, word := range f[:3] {
			if !isUpperWord(word) {
				return nil, malformed("a type, mode or access that is not a word")
			}
		}
		pid, err := strconv.Atoi(f[3])
		if err != nil || (pid < 0 && pid != -1) || f[3] != strconv.Itoa(pid) {
			return nil, malformed("a pid that is not a number")
		}
		node := strings.Split(f[4], ":")
		if len(node) != 3 {
			return nil, malformed("a node that is not major:minor:inode")
		}
		major, errMaj := strconv.ParseUint(node[0], 16, 32)
		minor, errMin := strconv.ParseUint(node[1], 16, 32)
		inode, errIno := strconv.ParseUint(node[2], 10, 64)
		if errMaj != nil || errMin != nil || errIno != nil || !isDecimal(node[2]) {
			return nil, malformed("a node that is not major:minor:inode")
		}
		if !isDecimal(f[5]) || (f[6] != "EOF" && !isDecimal(f[6])) {
			return nil, malformed("a range that is not start and end")
		}
		if waiter || pid < 1 || f[0] != "FLOCK" || f[2] != "WRITE" ||
			major != wantMajor || minor != wantMinor || inode != ino {
			continue
		}
		if !slices.Contains(holders, pid) {
			holders = append(holders, pid)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", errProcLocksMalformed, err)
	}
	slices.Sort(holders)
	return holders, nil
}

func isDecimal(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isUpperWord(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

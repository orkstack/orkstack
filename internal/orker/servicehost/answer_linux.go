//go:build linux

package servicehost

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/sys/unix"
)

// resolveFlags are the four a step is opened with (14 §4.3): no symbolic
// link anywhere on the path, nothing above the handle, no other
// filesystem, no /proc magic link. Spike S1 proved each against a hostile
// container's links.
const resolveFlags = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV | unix.RESOLVE_NO_MAGICLINKS

// AnswerDir is the Orker's one handle on a service's answer mount (14
// §3.1): every read is relative to it, and nothing is resolved by path
// again.
type AnswerDir struct {
	fd int
}

// NewAnswerDir takes ownership of fd, a directory descriptor (O_PATH is
// enough) on the root of a service's answer mount, opened by the caller
// from a process it has verified to be that container's.
func NewAnswerDir(fd int) (*AnswerDir, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("servicehost: the answer handle: %w", err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(fd)
		return nil, errors.New("servicehost: the answer handle is not a directory")
	}
	return &AnswerDir{fd: fd}, nil
}

// OpenAnswerDir opens path as an answer handle. It is for a directory the
// Orker itself controls (a test, a local service test run); a container's
// mount is opened through its verified process instead.
func OpenAnswerDir(path string) (*AnswerDir, error) {
	fd, err := unix.Open(path, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("servicehost: open %s: %w", path, err)
	}
	return NewAnswerDir(fd)
}

// Close releases the handle. A kept handle keeps a stopped container's
// memory filesystem alive (spike S1), so every attempt closes it.
func (d *AnswerDir) Close() error {
	if d == nil || d.fd < 0 {
		return nil
	}
	err := unix.Close(d.fd)
	d.fd = -1
	return err
}

// ReadStep reads the step at name, a fixed relative name the profile gives
// ("status.json", "lanes/2/message.json"), and copies its bytes once into
// the Orker's memory (14 §4.3). It is ErrNotYet when nothing is at name,
// and a *ViolationError for anything but a regular file with one name and
// at most bound bytes.
func (d *AnswerDir) ReadStep(name string, bound int64) ([]byte, error) {
	b, _, err := d.ReadStepID(name, bound)
	return b, err
}

// ReadStepID is ReadStep, and also names the file it read: a step
// rewritten by rename is a new file even when its bytes are the same, which
// is how a refreshed status is told from a stale one.
func (d *AnswerDir) ReadStepID(name string, bound int64) ([]byte, StepID, error) {
	b, id, err := d.readStep(name, bound)
	return b, id, err
}

func (d *AnswerDir) readStep(name string, bound int64) ([]byte, StepID, error) {
	if err := fixedName(name); err != nil {
		return nil, StepID{}, err
	}
	how := &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_NOCTTY | unix.O_CLOEXEC,
		Resolve: resolveFlags,
	}
	fd, err := openat2(d.fd, name, how)
	switch {
	case errors.Is(err, unix.ENOENT):
		return nil, StepID{}, ErrNotYet
	case errors.Is(err, unix.ELOOP), errors.Is(err, unix.EXDEV):
		return nil, StepID{}, violation(name, "a link, or a path that leaves the answer mount")
	case errors.Is(err, unix.ENOTDIR):
		return nil, StepID{}, violation(name, "a step's directory is not a directory")
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return nil, StepID{}, violation(name, "the Orker may not read it")
	case errors.Is(err, unix.ENXIO), errors.Is(err, unix.ENODEV), errors.Is(err, unix.EISDIR),
		errors.Is(err, unix.ENAMETOOLONG), errors.Is(err, unix.EINVAL), errors.Is(err, unix.EOPNOTSUPP):
		return nil, StepID{}, violation(name, "not a regular file the Orker can open (%v)", err)
	case errors.Is(err, unix.ENOSYS):
		return nil, StepID{}, ErrUnsupported
	case err != nil:
		return nil, StepID{}, fmt.Errorf("servicehost: open %s: %w", name, err)
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, StepID{}, fmt.Errorf("servicehost: stat %s: %w", name, err)
	}
	switch {
	case st.Mode&unix.S_IFMT != unix.S_IFREG:
		return nil, StepID{}, violation(name, "not a regular file")
	case st.Nlink != 1:
		return nil, StepID{}, stepLinks(name, uint64(st.Nlink))
	case st.Size > bound:
		return nil, StepID{}, violation(name, "%d bytes; at most %d", st.Size, bound)
	}
	// One copy, straight into the Orker's buffer, one byte past the bound
	// so a file that grows while it is read is seen.
	buf := make([]byte, st.Size, st.Size+1)
	n := 0
	for {
		if n == len(buf) {
			buf = buf[:len(buf)+1]
		}
		m, err := unix.Read(fd, buf[n:])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, StepID{}, fmt.Errorf("servicehost: read %s: %w", name, err)
		}
		n += m
		if int64(n) > bound || int64(n) > st.Size {
			return nil, StepID{}, violation(name, "grew while it was read")
		}
		if m == 0 {
			break
		}
	}
	buf = buf[:n]
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return nil, StepID{}, fmt.Errorf("servicehost: stat %s: %w", name, err)
	}
	if after.Nlink != 1 {
		return nil, StepID{}, stepLinks(name, uint64(after.Nlink))
	}
	if after.Size != st.Size || after.Mtim != st.Mtim || int64(n) != st.Size {
		return nil, StepID{}, violation(name, "rewritten while it was read")
	}
	return buf, StepID{Dev: uint64(st.Dev), Ino: st.Ino, MtimeNs: st.Mtim.Nano()}, nil
}

// stepLinks judges a step's link count other than one: none is a step the
// container renamed a newer one over while the Orker read it, read again at
// the next look; more than one is a hard link, which breaks the contract.
func stepLinks(name string, n uint64) error {
	switch n {
	case 1:
		return nil
	case 0:
		return ErrNotYet
	}
	return violation(name, "a file with %d names; a step has one", n)
}

// openat2 opens name relative to dir, retrying an interrupted call and an
// EAGAIN that RESOLVE_* answers when a rename raced the lookup.
func openat2(dir int, name string, how *unix.OpenHow) (int, error) {
	for range 8 {
		fd, err := unix.Openat2(dir, name, how)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		return fd, err
	}
	return -1, fmt.Errorf("servicehost: open %s: %w", name, io.ErrNoProgress)
}

// OpenContainerAnswer opens the answer mount of the container whose first
// process is pid, at path inside the container (14 §3.1; spike S1): a
// process handle on pid first, then the mount's root through that
// process's own root, then proof the handle still names a live process, so
// a pid the kernel has given another process is never read. The mount
// must be a memory filesystem. The handle is kept for the attempt and
// never resolved by number again.
func OpenContainerAnswer(pid int, path string) (*AnswerDir, error) {
	if pid <= 0 || !strings.HasPrefix(path, "/") || strings.Contains(path, "..") {
		return nil, fmt.Errorf("servicehost: open the answer of pid %d at %q", pid, path)
	}
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) {
			return nil, ErrUnsupported
		}
		return nil, fmt.Errorf("servicehost: a process handle on %d: %w", pid, err)
	}
	defer unix.Close(pidfd)
	// The container's root, then the mount beneath it with no symbolic link
	// followed and nothing outside that root: an image that made its answer
	// path a link (to the host's /dev/shm, say) would otherwise be read
	// through it.
	root, err := unix.Open(fmt.Sprintf("/proc/%d/root", pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("servicehost: open the root of %d: %w", pid, err)
	}
	fd, err := unix.Openat2(root, strings.TrimPrefix(path, "/"), &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	_ = unix.Close(root)
	if err != nil {
		return nil, fmt.Errorf("servicehost: open the answer mount of %d: %w", pid, err)
	}
	if err := unix.PidfdSendSignal(pidfd, 0, nil, 0); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("servicehost: process %d ended while its mount was opened: %w", pid, err)
	}
	var st unix.Statfs_t
	if err := unix.Fstatfs(fd, &st); err != nil || st.Type != unix.TMPFS_MAGIC {
		_ = unix.Close(fd)
		return nil, errors.New("servicehost: the answer mount is not a memory filesystem")
	}
	return NewAnswerDir(fd)
}

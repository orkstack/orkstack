//go:build linux

package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
)

// openGPULock opens the lock directory O_RDONLY|O_DIRECTORY|O_CLOEXEC and
// records its dev and inode and this boot's id. It takes no lock: tryLock and
// wait do.
func openGPULock(path string) (*gpuHostLock, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open the GPU host lock directory %s: %w", path, err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("stat the GPU host lock directory %s: %w", path, err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("read this boot's id for the GPU host lock: %w", err)
	}
	return &gpuHostLock{path: path, fd: fd, dev: uint64(st.Dev), ino: st.Ino, boot: strings.TrimSpace(string(boot))}, nil
}

// tryLock takes the exclusive flock without waiting: false, nil while another
// open file description holds it; an error for a filesystem that cannot lock.
func (l *gpuHostLock) tryLock() (bool, error) {
	err := syscall.Flock(l.fd, syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.EWOULDBLOCK):
		return false, nil
	default:
		return false, fmt.Errorf("lock the GPU host lock directory %s: %w", l.path, err)
	}
}

// wait takes the lock, trying every interval until ctx ends. The first time
// it finds the lock held it tells onHeld who holds it, once; the Orker then
// holds its gpu profile unavailable (GPU_HOST_LOCK_HELD) with that detail. A
// blocking flock is not used, because it cannot be cancelled when the Orker
// stops.
func (l *gpuHostLock) wait(ctx context.Context, every time.Duration, onHeld func(lockHolder)) error {
	told := false
	for {
		ok, err := l.tryLock()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if !told {
			told = true
			onHeld(l.holder())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

// holder names who holds the lock, from /proc/locks and /proc/<pid>/comm.
func (l *gpuHostLock) holder() lockHolder {
	f, err := os.Open("/proc/locks")
	if err != nil {
		return lockHolder{err: err}
	}
	defer func() { _ = f.Close() }()
	pids, err := parseProcLocks(f, l.dev, l.ino)
	if err != nil {
		return lockHolder{err: err}
	}
	h := lockHolder{pids: pids, comms: make([]string, len(pids))}
	for i, pid := range pids {
		comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if err != nil {
			h.comms[i] = "?"
			continue
		}
		h.comms[i] = strings.TrimSpace(string(comm))
	}
	return h
}

// check fails closed unless the directory at the lock's path is still the
// held one and the held one is not unlinked: before every bind, so a lock
// directory removed or recreated stops binding (HOST_LOCK_MISMATCH).
func (l *gpuHostLock) check() error {
	var held syscall.Stat_t
	if err := syscall.Fstat(l.fd, &held); err != nil {
		return fmt.Errorf("%w: stat the held directory: %v", errHostLockMismatch, err)
	}
	if held.Nlink == 0 {
		return fmt.Errorf("%w: the held directory %s was removed", errHostLockMismatch, l.path)
	}
	var now syscall.Stat_t
	if err := syscall.Stat(l.path, &now); err != nil {
		return fmt.Errorf("%w: stat %s: %v", errHostLockMismatch, l.path, err)
	}
	if dev := uint64(now.Dev); dev != l.dev || now.Ino != l.ino {
		return fmt.Errorf("%w: %s is %d:%d, this Orker holds %d:%d",
			errHostLockMismatch, l.path, dev, now.Ino, l.dev, l.ino)
	}
	return nil
}

// close releases the lock and the fd. Safe to call twice.
func (l *gpuHostLock) close() {
	if l.fd < 0 {
		return
	}
	_ = syscall.Close(l.fd)
	l.fd = -1
}

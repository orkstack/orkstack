//go:build unix

package orker

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// errIdentityHeld means another live process holds the identity lock.
var errIdentityHeld = errors.New("the orker identity lock is held")

// lockIdentity takes the exclusive, non-blocking advisory lock on file, which
// the kernel releases when the process exits, however it exits. It reports
// errIdentityHeld only when another process holds it; a filesystem that cannot
// lock at all (ENOLCK, EOPNOTSUPP) is a work-directory failure that says so.
func lockIdentity(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
		return errIdentityHeld
	default:
		return fmt.Errorf("%s: lock the orker identity %s: %w", ReasonWorkdirFailed, file.Name(), err)
	}
}

// unlockIdentity releases the lock lockIdentity took.
func unlockIdentity(file *os.File) {
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

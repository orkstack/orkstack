package orker

import (
	"errors"
	"os"
)

// errIdentityHeld means another live process holds the identity lock. It is
// declared on every platform so claimIdentity compiles on each; no process on
// Windows ever returns it.
var errIdentityHeld = errors.New("the orker identity lock is held")

// errHostUnsupported is what an Orker says on a host it does not run on. The
// package builds for Windows only so that orkctl can; an Orker never starts
// there.
var errHostUnsupported = errors.New("an Orker runs on Linux or macOS, not Windows")

// lockIdentity refuses: an Orker's identity lock is an advisory file lock the
// kernel releases with the process, and the Orker does not run on Windows.
func lockIdentity(*os.File) error {
	return errHostUnsupported
}

// unlockIdentity has nothing to release: lockIdentity never takes a lock.
func unlockIdentity(*os.File) {}

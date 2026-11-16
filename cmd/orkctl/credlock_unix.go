//go:build unix

package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// credentialsLockWait bounds how long one orkctl waits for another's hold
// on the logins: a refresh takes at most its client's 30 seconds.
const credentialsLockWait = 45 * time.Second

// lockFile takes an exclusive advisory lock on f, waiting at most
// credentialsLockWait, and says which file it waited on when it gives up.
func lockFile(f *os.File) error {
	deadline := time.Now().Add(credentialsLockWait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("another orkctl has held %s for %s; try again", f.Name(), credentialsLockWait)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// unlockFile releases lockFile's lock.
func unlockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

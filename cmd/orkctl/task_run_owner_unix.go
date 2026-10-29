//go:build unix

package main

import (
	"errors"
	"os"
	"syscall"
)

// errTaskRunOwnerHeld means a live run holds the lock.
var errTaskRunOwnerHeld = errors.New("held by a live run")

// lockTaskRunOwner takes the exclusive, non-blocking advisory lock on file,
// which the kernel releases when the process exits, however it exits.
func lockTaskRunOwner(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return errTaskRunOwnerHeld
	}
	return err
}

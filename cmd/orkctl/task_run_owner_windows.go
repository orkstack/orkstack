//go:build windows

package main

import (
	"errors"
	"os"
)

// errTaskRunOwnerHeld means a live run holds the lock.
var errTaskRunOwnerHeld = errors.New("held by a live run")

// lockTaskRunOwner refuses: `orkctl task run` refuses a Windows host before
// it claims anything (requireLocalRunHost).
func lockTaskRunOwner(*os.File) error {
	return errors.New("a task run's identity lock needs a Linux or macOS host")
}

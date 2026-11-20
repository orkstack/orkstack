// Package servicehost is the Orker's side of the Orker Service Contract
// (docs/ORKER_SERVICE_CONTRACT.md): the files a service container and its
// Orker exchange, how the Orker reads the container's steps without
// trusting the container, and the lanes, deadlines and watchdog that hold a
// service to its profile. It names no container runtime: what it needs of
// one is the interfaces it declares.
package servicehost

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNotYet is a step that is not there yet: its name does not exist.
var ErrNotYet = errors.New("the step is not there yet")

// ErrUnsupported is a host that cannot read an answer mount safely: the
// Orker then hosts no service (14 §3.1).
var ErrUnsupported = errors.New("this host cannot read a service's files safely")

// ViolationError is something the container wrote that breaks the contract
// (14 §4.2, rule 4): a link, a device, a socket, a FIFO or a directory at a
// name the Orker reads, a file with more than one name, or a file over its
// step's bound. It ends the attempt SERVICE_CONTRACT_VIOLATION.
type ViolationError struct {
	Name   string
	Reason string
}

func (e *ViolationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Name, e.Reason)
}

func violation(name, format string, args ...any) error {
	return &ViolationError{Name: name, Reason: fmt.Sprintf(format, args...)}
}

// StepID names one file the Orker read: its device, inode and modification
// time. Two reads with one StepID read one write.
type StepID struct {
	Dev     uint64
	Ino     uint64
	MtimeNs int64
}

// fixedName holds a name to the profile's shape: relative, no empty, "."
// or ".." element, no NUL.
func fixedName(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
		return fmt.Errorf("servicehost: %q is not a step name", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("servicehost: %q is not a step name", name)
		}
	}
	return nil
}

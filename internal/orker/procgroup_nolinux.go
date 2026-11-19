//go:build !linux

package orker

import "os/exec"

// startTied starts cmd. Off Linux there is no parent-death signal, so a
// runtime CLI dies with its Orker only through killAll (the third stop
// signal); the Orker runs in production on Linux (procgroup_linux.go).
func startTied(cmd *exec.Cmd) error { return cmd.Start() }

package main

import (
	"fmt"
	"runtime"
)

// requireLocalRunHost refuses a local-run command on a host that cannot run
// one. `orkctl task run` and `orkctl activity test` start a container through
// the dispatcher an Orker uses, against a Docker daemon on this machine, and an
// Orker runs on Linux or macOS. Every other command only talks to the engine,
// so this one check guards exactly these two.
//
// The refusal is the first thing the command's own code does, before it reads
// its flags or a file: a Windows user is told the platform is the problem, not
// that a file is missing. Cobra's own argument-count and flag-syntax checks
// still come first.
func requireLocalRunHost(goos, command string) error {
	if goos != "windows" {
		return nil
	}
	return fmt.Errorf("orkctl %s needs a Linux or macOS host with a Docker daemon: it runs a container "+
		"through the dispatcher an Orker uses, and an Orker runs on Linux or macOS. "+
		"Every other orkctl command works on this host", command)
}

// hostOS is the operating system the local-run commands check: the real one,
// unless a test set the seam.
func (g *globalFlags) hostOS() string {
	if g.goos != "" {
		return g.goos
	}
	return runtime.GOOS
}

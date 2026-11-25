//go:build linux

package orker

import (
	"fmt"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
)

// startTied starts cmd so that it dies with this process: the kernel sends it
// SIGKILL (Pdeathsig) when its parent goes, however the parent goes — a
// SIGKILL, a crash, an OOM kill — and not only on the third stop signal,
// which is the one death killAll covers. A runtime CLI that outlived its Orker
// would run for good: a `docker events` watcher on a container stuck in
// Created never ends. The daemon owns the containers, so killing a CLI never
// touches one; the next boot's reap removes what the Orker left.
//
// Linux sends the parent-death signal when the thread that started the child
// exits, not the process (prctl(2), golang/go#27505), and the Go runtime ends
// a thread whose goroutine returns while locked to it. So every child is
// started from one goroutine locked to its thread for the life of the
// process: the signal then comes only when the process itself goes.
//
// Weighed and not used: exec.CommandContext with Cancel and WaitDelay binds a
// child to a context, which a dying process never cancels; and the process
// group killAll already kills is reached only by a process still alive to
// send the signal.
func startTied(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
	spawnerOnce.Do(func() { go spawner() })
	done := make(chan error, 1)
	spawns <- spawn{cmd: cmd, done: done}
	return <-done
}

type spawn struct {
	cmd  *exec.Cmd
	done chan<- error
}

var (
	spawnerOnce sync.Once
	spawns      = make(chan spawn)
)

// spawner starts every tied child, from a thread it never gives back. Every
// start queues behind the one before it, which costs a fork and an exec each:
// cmd.Start must stay the only work done here.
func spawner() {
	runtime.LockOSThread()
	for s := range spawns {
		s.done <- startRecovered(s.cmd)
	}
}

// startRecovered is cmd.Start with a panic made the caller's error: on the
// spawner's goroutine a panic would otherwise end the whole Orker, where on
// the caller's it ended only the attempt.
func startRecovered(cmd *exec.Cmd) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("start a runtime CLI: panic: %v", r)
		}
	}()
	return notStarted(cmd.Start())
}

//go:build unix

package orker

import (
	"errors"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestCLIChildren_RunInAGroupOfTheirOwnAndDieWithKillAll: a runtime CLI the
// Orker starts leads a process group of its own, so a signal to the Orker's
// group never reaches it, and killAll kills it.
func TestCLIChildren_RunInAGroupOfTheirOwnAndDieWithKillAll(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep binary")
	}
	var children cliChildren
	cmd := exec.Command(sleep, "60")
	wait, err := children.start(cmd)
	must.OKf(t, err, "start")
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	must.OKf(t, err, "Getpgid")
	if pgid != cmd.Process.Pid || pgid == syscall.Getpgrp() {
		t.Fatalf("child pgid = %d (own pid %d, ours %d), want a group of its own", pgid, cmd.Process.Pid, syscall.Getpgrp())
	}
	if n := children.killAll(); n != 1 {
		t.Fatalf("killAll signalled %d, want 1", n)
	}
	err = wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(exit.String(), "killed") {
		t.Fatalf("wait = %v, want the child killed", err)
	}
	if n := children.killAll(); n != 0 {
		t.Errorf("killAll after the child was waited for signalled %d, want 0", n)
	}
}

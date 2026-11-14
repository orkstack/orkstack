//go:build unix

package orker

import (
	"os/exec"
	"syscall"
)

// ownProcessGroup makes cmd the leader of a process group of its own, so a
// signal to the Orker's group never reaches it.
func ownProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessGroup sends SIGKILL to the group pid leads.
func killProcessGroup(pid int) error {
	return syscall.Kill(-pid, syscall.SIGKILL)
}

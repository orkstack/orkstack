package orker

import (
	"os"
	"os/exec"
)

// ownProcessGroup does nothing on Windows, which has no process groups to
// signal; a console's Ctrl-C reaches only the console's own processes.
func ownProcessGroup(*exec.Cmd) {}

// killProcessGroup kills the process itself.
func killProcessGroup(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}

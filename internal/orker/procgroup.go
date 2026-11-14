package orker

import (
	"os/exec"
	"sync"
)

// cliChildren is every process the Orker started to drive its container
// runtime and has not yet waited for. Each runs in a process group of its own
// (ownProcessGroup), so a signal sent to the Orker's group — a terminal's
// Ctrl-C, a process manager signalling the whole group — never reaches a
// CLI, and through it a task's container. The Orker's third stop signal kills
// them all (killAll) before it exits, so none outlives it.
type cliChildren struct {
	mu   sync.Mutex
	pids map[int]struct{}
}

// start starts cmd in a group of its own and tracks it until the returned wait
// has returned.
func (c *cliChildren) start(cmd *exec.Cmd) (wait func() error, err error) {
	ownProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pid := cmd.Process.Pid
	c.mu.Lock()
	if c.pids == nil {
		c.pids = map[int]struct{}{}
	}
	c.pids[pid] = struct{}{}
	c.mu.Unlock()
	return func() error {
		err := cmd.Wait()
		c.mu.Lock()
		delete(c.pids, pid)
		c.mu.Unlock()
		return err
	}, nil
}

// run is start, then wait: exec.Cmd.Run for a tracked child.
func (c *cliChildren) run(cmd *exec.Cmd) error {
	wait, err := c.start(cmd)
	if err != nil {
		return err
	}
	return wait()
}

// killAll kills the process group of every tracked child and reports how many
// it signalled. The children's containers are not touched: the daemon owns
// them, and the next boot's reap removes what this Orker labelled.
func (c *cliChildren) killAll() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for pid := range c.pids {
		if killProcessGroup(pid) == nil {
			n++
		}
	}
	return n
}

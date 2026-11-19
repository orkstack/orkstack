//go:build integration && linux

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_AKilledOrkerLeavesNoRuntimeCLI: an Orker SIGKILLed while
// its task's container runs leaves none of the runtime CLIs it started (the
// attach, the `docker events` watcher, the wait) running after it: each dies
// with the Orker. The container itself is the daemon's and runs on. Once, an
// Orker killed between a container's create and start left its events
// watcher running for good, its parent systemd.
func TestIntegration_AKilledOrkerLeavesNoRuntimeCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("orker-kill-children.txt")

	orker := startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+uniqueName("orker-killed"), "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
	awaitActivityPoller(ctx, t, s.temporal, s.queue)
	name := uniqueName("orker-killed")
	s.apply(finishingOrkflow(name, 120))
	runID := s.submit(name)
	t.Cleanup(func() { removeRunContainers(t, runID) })

	deadline := time.Now().Add(2 * time.Minute)
	for {
		out, _, _ := s.cli(0, "run", "logs", runID)
		if strings.Contains(out, "work: started") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task never started within 2m; logs:\n%s", out)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The positive control: while the Orker lives, it has a runtime CLI that
	// would not end by itself while the container runs (the attach, or the
	// events watcher), alive right before the kill. A short-lived one (an
	// inspect) could end on its own and prove nothing.
	children := childrenOf(t, orker.Process.Pid)
	lasting := 0
	for pid, cmdline := range children {
		if (strings.Contains(cmdline, " start ") && strings.Contains(cmdline, "--attach")) ||
			strings.Contains(cmdline, " events ") {
			if processAlive(pid) {
				lasting++
			}
		}
	}
	if lasting == 0 {
		t.Fatalf("the orker runs its task's container but no attach or events CLI of its is alive to observe: %v", children)
	}
	t.Logf("the orker's children before the kill (%d lasting): %v", lasting, children)

	must.OKf(t, orker.Process.Signal(syscall.SIGKILL), "SIGKILL the orker")
	_ = orker.Wait()

	deadline = time.Now().Add(10 * time.Second)
	for {
		var left []string
		for pid, cmdline := range children {
			if processAlive(pid) {
				left = append(left, fmt.Sprintf("%d %s", pid, cmdline))
			}
		}
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime CLIs outlived their SIGKILLed orker by 10s: %v", left)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if running := runningContainers(t, runID); len(running) != 1 {
		t.Fatalf("the task's container reads %v after the orker died, want it still running: killing a CLI never touches one", running)
	}
	t.Logf("EVIDENCE %d runtime CLIs died with their SIGKILLed orker; its container runs on", len(children))
}

// childrenOf maps every live child of pid to its command line.
func childrenOf(t *testing.T, pid int) map[int]string {
	t.Helper()
	stats, err := filepath.Glob("/proc/[0-9]*/stat")
	must.OKf(t, err, "list /proc")
	children := map[int]string{}
	for _, path := range stats {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		fields := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))
		if len(fields) < 2 || fields[1] != strconv.Itoa(pid) || fields[0] == "Z" {
			continue
		}
		child, err := strconv.Atoi(filepath.Base(filepath.Dir(path)))
		if err != nil {
			continue
		}
		cmdline, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "cmdline"))
		children[child] = strings.ReplaceAll(strings.TrimRight(string(cmdline), "\x00"), "\x00", " ")
	}
	return children
}

// processAlive reports whether pid has not exited; a zombie has.
func processAlive(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	fields := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))
	return len(fields) > 0 && fields[0] != "Z" && fields[0] != "X"
}

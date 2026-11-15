//go:build linux

package dist_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// beUndumpableUntilStopped makes this process undumpable, as ork and orker
// make themselves, says so by writing ready, then waits for SIGTERM, or two
// minutes if none comes.
func beUndumpableUntilStopped(ready string) int {
	if _, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, 4 /* PR_SET_DUMPABLE */, 0, 0, 0, 0, 0); errno != 0 {
		return 3
	}
	if r, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, 3 /* PR_GET_DUMPABLE */, 0, 0, 0, 0, 0); errno != 0 || r != 0 {
		return 4
	}
	if err := os.WriteFile(ready, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return 5
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	select {
	case <-stop:
	case <-time.After(2 * time.Minute):
	}
	return 0
}

// down runs `scripts/stack.sh down` in the throwaway repository.
func (s *stackRepo) down() (int, string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(s.root, "scripts", "stack.sh"), "down")
	cmd.Env = []string{"PATH=" + s.path, "HOME=" + s.logs}
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	s.t.Fatalf("run scripts/stack.sh down: %v\n%s", err, out)
	return 0, ""
}

// recordedPid is the pid NAME's pidfile records, its first field.
func (s *stackRepo) recordedPid(name string) int {
	s.t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.root, ".run", name+".pid"))
	must.OK(s.t, err)
	fields := strings.Fields(string(raw))
	if len(fields) != 2 {
		s.t.Fatalf("%s.pid holds %q; want a pid and its start time", name, raw)
	}
	pid, err := strconv.Atoi(fields[0])
	must.OK(s.t, err)
	return pid
}

// running reports whether pid is a live process, a zombie not counting.
func running(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	rest := string(raw)
	if i := strings.LastIndex(rest, ") "); i >= 0 {
		rest = rest[i+2:]
	}
	return !strings.HasPrefix(rest, "Z")
}

// TestDevStack_DownStopsAnUndumpableEngineAndLeavesAReusedPidAlone: ork and
// orker make themselves undumpable, which hides /proc/<pid>/exe from their own
// user, so `make down` finds what `make up` started by the pid and start time
// it recorded (boot and start time). An undumpable engine it recorded is
// stopped. A recorded pid the kernel has since given another process, which
// shows another start time, is never signalled, and neither is a pidfile that
// records a pid alone, as an earlier release wrote: down says it left each
// alone.
func TestDevStack_DownStopsAnUndumpableEngineAndLeavesAReusedPidAlone(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)
	ready := filepath.Join(s.logs, "ork-undumpable")
	s.write(filepath.Join(s.root, "bin", "ork"), "#!/bin/sh\n"+
		`echo "$*" >> `+shellQuote(filepath.Join(s.logs, "ork"))+"\n"+
		undumpableVar+"="+shellQuote(ready)+" exec "+shellQuote(os.Args[0])+"\n")

	if code, out := s.up(); code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	engine := s.recordedPid("ork")
	// The stand-in says it is undumpable once prctl has made it so.
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, err := os.ReadFile(ready)
		if err == nil && strings.TrimSpace(string(raw)) == strconv.Itoa(engine) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the engine stand-in (pid %d) never said it was undumpable", engine)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The Orker's recorded pid now names another process, whose start time is
	// not the one recorded: what a reused pid looks like.
	worker := s.recordedPid("orker")
	if p, err := os.FindProcess(worker); err == nil {
		_ = p.Kill()
	}
	sleepPath, err := exec.LookPath("sleep")
	must.OK(t, err)
	other := exec.Command(sleepPath, "120")
	must.OK(t, other.Start())
	t.Cleanup(func() { _ = other.Process.Kill(); _ = other.Wait() })
	must.OK(t, os.WriteFile(filepath.Join(s.root, ".run", "orker.pid"),
		[]byte(strconv.Itoa(other.Process.Pid)+" 1@not-this-boot\n"), 0o644))
	// The UI's pidfile is one an earlier release wrote, a pid alone, naming
	// another live process: unconfirmed, so never signalled.
	ui := s.recordedPid("ui")
	if p, err := os.FindProcess(ui); err == nil {
		_ = p.Kill()
	}
	legacy := exec.Command(sleepPath, "120")
	must.OK(t, legacy.Start())
	t.Cleanup(func() { _ = legacy.Process.Kill(); _ = legacy.Wait() })
	must.OK(t, os.WriteFile(filepath.Join(s.root, ".run", "ui.pid"),
		[]byte(strconv.Itoa(legacy.Process.Pid)+"\n"), 0o644))

	code, out := s.down()
	if code != 0 {
		t.Fatalf("scripts/stack.sh down exited %d; want 0:\n%s", code, out)
	}
	deadline = time.Now().Add(30 * time.Second)
	for running(engine) {
		if time.Now().After(deadline) {
			t.Fatalf("the undumpable engine (pid %d) still runs after down:\n%s", engine, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(out, "ork stopped (pid "+strconv.Itoa(engine)+")") {
		t.Errorf("down did not say it stopped the engine:\n%s", out)
	}
	if err := other.Process.Signal(syscall.Signal(0)); err != nil || !running(other.Process.Pid) {
		t.Fatalf("down signalled a process its pidfile's pid now names (pid %d): %v\n%s", other.Process.Pid, err, out)
	}
	if !strings.Contains(out, "orker is not running") || !strings.Contains(out, "left alone") {
		t.Errorf("down did not say it left the Orker's reused pid alone:\n%s", out)
	}
	if err := legacy.Process.Signal(syscall.Signal(0)); err != nil || !running(legacy.Process.Pid) {
		t.Fatalf("down signalled the pid a pidfile with no identity names (pid %d): %v\n%s", legacy.Process.Pid, err, out)
	}
	if !strings.Contains(out, "ui is not running") {
		t.Errorf("down did not say it left the UI's unconfirmed pid alone:\n%s", out)
	}
}

// TestDevStack_DownStopsAGroupWhoseLeaderHasEnded: what make up started can
// outlive its leader (bun ends, vite runs on). The group the recorded pid led
// is still that one while it has members, since no process can be given that
// pid meanwhile, so down stops it rather than call it not running.
func TestDevStack_DownStopsAGroupWhoseLeaderHasEnded(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)
	if code, out := s.up(); code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	ui := s.recordedPid("ui")
	if p, err := os.FindProcess(ui); err == nil {
		_ = p.Kill()
	}
	sleepPath, err := exec.LookPath("sleep")
	must.OK(t, err)
	// A leader that starts a member of its own group and ends.
	memberFile := filepath.Join(t.TempDir(), "member")
	leader := exec.Command("sh", "-c", shellQuote(sleepPath)+" 120 & echo $! > "+shellQuote(memberFile))
	leader.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	must.OK(t, leader.Run())
	raw, err := os.ReadFile(memberFile)
	must.OK(t, err)
	member, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	must.OK(t, err)
	t.Cleanup(func() { _ = syscall.Kill(member, syscall.SIGKILL) })
	if !running(member) {
		t.Fatalf("the group's member (pid %d) is not running", member)
	}
	must.OK(t, os.WriteFile(filepath.Join(s.root, ".run", "ui.pid"),
		[]byte(strconv.Itoa(leader.Process.Pid)+" 1@ended\n"), 0o644))

	code, out := s.down()
	if code != 0 {
		t.Fatalf("scripts/stack.sh down exited %d; want 0:\n%s", code, out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for running(member) {
		if time.Now().After(deadline) {
			t.Fatalf("the group's member (pid %d) still runs after down:\n%s", member, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(out, "ui stopped (pid "+strconv.Itoa(leader.Process.Pid)+")") {
		t.Errorf("down did not say it stopped the UI's group:\n%s", out)
	}
}

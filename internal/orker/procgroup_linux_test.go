//go:build linux

package orker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// cliParentEnv makes this test binary the helper Orker of
// TestCLIChildren_DieWithTheOrker (init, below) instead of a test run.
const cliParentEnv = "ORK_TEST_CLI_CHILD_PARENT"

// init makes this test binary the helper Orker when cliParentEnv is set, so
// the helper is no test of its own (and no skip in a ledger): it starts a
// runtime CLI child from a goroutine whose OS thread then exits (a thread's
// exit is what Linux's parent-death signal follows), prints the child's pid
// once the kernel no longer lists that thread, and blocks until it is killed.
func init() {
	if os.Getenv(cliParentEnv) != "1" {
		return
	}
	var children cliChildren
	type started struct{ pid, thread int }
	report := make(chan started)
	go func() {
		// Locked and never unlocked: the runtime ends this OS thread when
		// the goroutine returns.
		runtime.LockOSThread()
		cmd := exec.Command("sleep", "300")
		if _, err := children.start(cmd); err != nil {
			fmt.Println("start:", err)
			os.Exit(2)
		}
		report <- started{pid: cmd.Process.Pid, thread: syscall.Gettid()}
	}()
	got := <-report
	// The thread is gone once the kernel no longer lists it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(fmt.Sprintf("/proc/self/task/%d", got.thread)); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			fmt.Println("the locked thread never exited")
			os.Exit(2)
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println(got.pid)
	// Blocks until killed; reading stdin keeps the runtime from reading
	// this as a deadlock whatever else runs.
	_, _ = io.Copy(io.Discard, os.Stdin)
	select {}
}

// TestCLIChildren_DieWithTheOrker: a runtime CLI the Orker started (a
// `docker events` watcher above all) dies with the Orker, however the Orker
// dies, even when the OS thread that started it is long gone. An Orker
// SIGKILLed between a container's create and start once left its watcher
// running for good, its parent systemd, its container stuck in Created.
func TestCLIChildren_DieWithTheOrker(t *testing.T) {
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("no sleep binary")
	}
	helper := exec.Command(os.Args[0], "-test.run", "^$")
	helper.Env = append(os.Environ(), cliParentEnv+"=1")
	out, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = helper.Process.Kill(); _ = helper.Wait() })

	pid := 0
	var said []string
	lines := bufio.NewScanner(out)
	for pid == 0 && lines.Scan() {
		if n, err := strconv.Atoi(strings.TrimSpace(lines.Text())); err == nil {
			pid = n
		} else {
			said = append(said, lines.Text())
		}
	}
	if pid == 0 {
		t.Fatalf("the helper Orker reported no child; it said: %q", said)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

	// The thread that started the child has exited; the Orker has not, so
	// the child lives on.
	if !alive(pid) {
		t.Fatal("the child died when the thread that started it exited, while its Orker lived")
	}

	if err := helper.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = helper.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for alive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d outlived its SIGKILLed Orker by 10s", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// alive reports whether pid is a process that has not exited: a zombie
// awaiting its reaper has exited.
func alive(pid int) bool {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	return len(fields) > 0 && fields[0] != "Z" && fields[0] != "X"
}

// A start that panics comes back as the caller's error, not the end of the
// Orker: the spawner keeps serving after it.
func TestStartTied_APanickingStartIsTheCallersError(t *testing.T) {
	var cmd *exec.Cmd // a nil command panics in Start
	err := startRecovered(cmd)
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("startRecovered(nil) = %v, want the panic as an error", err)
	}
	ok := exec.Command("true")
	if err := startTied(ok); err != nil {
		t.Fatalf("a start after a panicking one failed: %v", err)
	}
	_ = ok.Wait()
}

//go:build integration

package orker_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
)

// groupSignalChildVar marks the helper process of
// TestIntegration_AGroupSignalNeverReachesTheContainer.
const groupSignalChildVar = "ORK_TEST_GROUP_SIGNAL_CHILD"

// syncWriter is a writer the attach's two streams share.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestIntegration_AGroupSignalNeverReachesTheContainer: a signal sent to the
// Orker's whole process group — a terminal's Ctrl-C, a process manager that
// signals the group — never reaches a task's container through the runtime
// CLI the Orker started (docs/OCI_ACTIVITY_CONTRACT.md §6.3). A helper
// process plays the Orker in a process group of its own: it starts a
// container through the runtime, as a dispatch does, then signals its own
// group with SIGINT. The container, which traps SIGINT and says so, must not
// hear it; its ticks carrying on through the window are the positive control
// that the window was real.
func TestIntegration_AGroupSignalNeverReachesTheContainer(t *testing.T) {
	if os.Getenv(groupSignalChildVar) != "" {
		groupSignalChild(t)
		return
	}
	requireDocker(t)
	pinned := pinnedReference(t)
	label := fmt.Sprintf("ork.run=ork-test-pgid-%d-%d", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() { removeLabelled(t, label) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestIntegration_AGroupSignalNeverReachesTheContainer$", "-test.v")
	cmd.Env = append(os.Environ(), groupSignalChildVar+"=1", "ORK_TEST_PGID_IMAGE="+pinned,
		"ORK_TEST_PGID_LABEL="+label)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the helper Orker failed: %v\n%s", err, out)
	}
	var result string
	for line := range strings.SplitSeq(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			result = rest
		}
	}
	if result == "" {
		t.Fatalf("the helper Orker reported nothing:\n%s", out)
	}
	if strings.Contains(result, "GOT_SIGINT") {
		t.Fatalf("a SIGINT to the Orker's process group reached the container: %s", result)
	}
	if !strings.Contains(result, "state=running") {
		t.Fatalf("the container did not keep running through the group signal: %s", result)
	}
	if !strings.Contains(result, "ticks-after=true") {
		t.Fatalf("the container made no progress in the window, so the window proves nothing: %s", result)
	}
}

// groupSignalChild is the helper Orker: it runs in a group of its own, which
// it then signals.
func groupSignalChild(t *testing.T) {
	cli := containerCLI(t)
	image, label := os.Getenv("ORK_TEST_PGID_IMAGE"), os.Getenv("ORK_TEST_PGID_LABEL")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	created, err := exec.CommandContext(ctx, cli, "create", "--label", label, image, "sh", "-c",
		`trap 'echo GOT_SIGINT; exit 0' INT; echo ready; i=0; while true; do i=$((i+1)); echo tick $i; sleep 0.1; done`).Output()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := orker.ContainerID(strings.TrimSpace(string(created)))
	runtime := orker.NewDockerRuntime()
	defer func() { _ = runtime.Remove(context.WithoutCancel(ctx), id) }()

	logs := &syncWriter{}
	if err := runtime.Start(ctx, id, logs, logs); err != nil {
		t.Fatalf("start: %v", err)
	}
	for ticks(logs.String()) < 3 {
		if ctx.Err() != nil {
			t.Fatalf("the container never printed: %q", logs.String())
		}
		waitALittle(ctx)
	}

	// The Orker itself survives the group signal, as a stopping Orker does:
	// it handles the signal.
	signal.Ignore(syscall.SIGINT)
	if err := syscall.Kill(0, syscall.SIGINT); err != nil {
		t.Fatalf("signal the group: %v", err)
	}
	before := ticks(logs.String())
	// The window: until the container has ticked ten more times, which a
	// container that heard the signal never does.
	for ticks(logs.String()) < before+10 && !strings.Contains(logs.String(), "GOT_SIGINT") && ctx.Err() == nil {
		waitALittle(ctx)
	}
	state, _ := exec.CommandContext(ctx, cli, "inspect", "-f", "{{.State.Status}}", string(id)).Output()
	fmt.Printf("RESULT state=%s ticks-after=%v log=%q\n", strings.TrimSpace(string(state)),
		ticks(logs.String()) >= before+10, lastLines(logs.String(), 3))
}

// ticks counts the container's tick lines.
func ticks(log string) int {
	n := 0
	scanner := bufio.NewScanner(strings.NewReader(log))
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "tick ") {
			n++
		}
	}
	return n
}

func lastLines(log string, n int) string {
	lines := strings.Split(strings.TrimSpace(log), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], " | ")
}

// waitALittle yields to the container between two reads of its log.
func waitALittle(ctx context.Context) {
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// removeLabelled removes every container carrying label, whatever state the
// helper left it in.
func removeLabelled(t *testing.T, label string) {
	cli := containerCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ids, _ := exec.CommandContext(ctx, cli, "ps", "-aq", "--filter", "label="+label).Output()
	for id := range strings.FieldsSeq(string(ids)) {
		_ = exec.CommandContext(ctx, cli, "rm", "-f", id).Run()
	}
}

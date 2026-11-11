//go:build integration

package main

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
)

// replicaForwarder is the load balancer in front of two replicas, reduced to
// what a watch sees of one: every new connection goes to the current backend,
// and whatever ends a backend's side of a connection ends the client's the
// same way, a FIN as a FIN and a read error as a reset.
type replicaForwarder struct {
	listener net.Listener
	mu       sync.Mutex
	backend  string
}

func newReplicaForwarder(t *testing.T, backend string) *replicaForwarder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &replicaForwarder{listener: listener, backend: backend}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go f.relay(conn.(*net.TCPConn))
		}
	}()
	return f
}

func (f *replicaForwarder) sendTo(backend string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backend = backend
}

func (f *replicaForwarder) relay(client *net.TCPConn) {
	defer client.Close()
	f.mu.Lock()
	backend := f.backend
	f.mu.Unlock()
	upstream, err := net.Dial("tcp", backend)
	if err != nil {
		return // the client's connection closes: a replica that is not there
	}
	server := upstream.(*net.TCPConn)
	defer server.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := io.Copy(server, client); err == nil {
			_ = server.CloseWrite()
		}
	}()
	if _, err := io.Copy(client, server); err == nil {
		_ = client.CloseWrite()
	} else {
		_ = client.SetLinger(0)
	}
	<-done
}

// watchProcess is a real `orkctl run watch`, whose output a test can wait on
// while it runs.
type watchProcess struct {
	mu     sync.Mutex
	stdout strings.Builder
	stderr strings.Builder
	exited chan int
}

func startWatchProcess(t *testing.T, c *awaitCLI, server, runID string) *watchProcess {
	t.Helper()
	// ORK_TEST_WATCH_ORKCTL names another build of orkctl, to hold a release's
	// client against this engine by hand; unset, the test runs this tree's.
	orkctl := c.orkctl
	if other := os.Getenv("ORK_TEST_WATCH_ORKCTL"); other != "" {
		orkctl = other
	}
	return startOrkctlProcess(t, c, orkctl, "run", "watch", runID, "--server", server, "-n", c.namespace)
}

// startOrkctlProcess starts a real orkctl whose output a test can wait on.
func startOrkctlProcess(t *testing.T, c *awaitCLI, orkctl string, args ...string) *watchProcess {
	t.Helper()
	cmd := exec.CommandContext(c.ctx, orkctl, args...)
	cmd.Dir = c.repoRoot
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start run watch: %v", err)
	}
	w := &watchProcess{exited: make(chan int, 1)}
	var readers sync.WaitGroup
	readers.Add(2)
	collect := func(from io.Reader, into *strings.Builder) {
		defer readers.Done()
		scanner := bufio.NewScanner(from)
		for scanner.Scan() {
			w.mu.Lock()
			into.WriteString(scanner.Text() + "\n")
			w.mu.Unlock()
		}
	}
	go collect(stdout, &w.stdout)
	go collect(stderr, &w.stderr)
	go func() {
		readers.Wait()
		err := cmd.Wait()
		var exit *exec.ExitError
		switch {
		case err == nil:
			w.exited <- 0
		case errors.As(err, &exit):
			w.exited <- exit.ExitCode()
		default:
			w.exited <- -1
		}
	}()
	return w
}

func (w *watchProcess) output() (string, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stdout.String(), w.stderr.String()
}

// awaitOutput waits until said holds of the watch's output. A watch that ends
// first, or a minute without it, fails the test with what the watch printed.
func (w *watchProcess) awaitOutput(t *testing.T, what string, said func(stdout, stderr string) bool) {
	t.Helper()
	deadline := time.NewTimer(time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		stdout, stderr := w.output()
		if said(stdout, stderr) {
			return
		}
		select {
		case code := <-w.exited:
			w.exited <- code
			stdout, stderr = w.output()
			t.Fatalf("run watch ended with exit %d before %s\nstdout:\n%s\nstderr:\n%s", code, what, stdout, stderr)
		case <-deadline.C:
			t.Fatalf("run watch did not reach %s within a minute\nstdout:\n%s\nstderr:\n%s", what, stdout, stderr)
		case <-tick.C:
		}
	}
}

const replicaExitOrkflowYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: replica-exit
version: "1.0.0"
tasks:
  - id: gate
    await:
      timeout_seconds: 600
`

// watchThroughReplicaExit watches a live run through replica A, ends A with
// sig while the stream is open, then finishes the run on replica B. The watch
// must carry on through B to the run's end, printing every event once.
func watchThroughReplicaExit(t *testing.T, sig syscall.Signal) {
	t.Helper()
	c := newAwaitCLI(t, 1, false) // engine 0 is replica B, which survives
	doomed, doomedExited := c.startSignalledEngine()

	path := filepath.Join(t.TempDir(), "replica-exit.ork.yaml")
	if err := os.WriteFile(path, []byte(replicaExitOrkflowYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "orkflow", "create", path)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "replica-exit", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	runID := submitted.Run.ID
	c.waitAwaiting(0, runID, "gate")

	forwarder := newReplicaForwarder(t, doomed.addr)
	watch := startWatchProcess(t, c, "http://"+forwarder.listener.Addr().String(), runID)
	var before []int64
	watch.awaitOutput(t, "the run's events so far", func(stdout, _ string) bool {
		before = printedSequences(t, stdout)
		return len(before) > 0 && strings.Contains(stdout, "gate")
	})

	forwarder.sendTo(strings.TrimPrefix(c.engines[0], "http://"))
	if err := doomed.process.Signal(sig); err != nil {
		t.Fatalf("signal replica A: %v", err)
	}
	select {
	case <-doomedExited:
	case <-time.After(time.Minute):
		t.Fatalf("replica A did not exit within a minute of %v", sig)
	}

	// The watch has seen the stream end and said so before the run moves, so
	// the events that follow can only reach it through its resume.
	watch.awaitOutput(t, "a resume", func(_, stderr string) bool {
		return len(resumeLines(stderr)) > 0
	})
	c.must(0, "run", "resolve", runID, "--task", "gate", "--values", `{"ok":true}`)

	select {
	case code := <-watch.exited:
		stdout, stderr := watch.output()
		if code != 0 {
			t.Fatalf("run watch exit %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		if !strings.Contains(stdout, "finished with status SUCCEEDED") {
			t.Fatalf("the watch did not report the run's end\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
		}
		// A fresh watch on the survivor is the whole stream; the resumed one
		// must have printed exactly the same numbers, in order.
		whole, _, code := c.on(0, "run", "watch", runID)
		if code != 0 {
			t.Fatalf("whole watch exit %d", code)
		}
		want := printedSequences(t, whole)
		if got := printedSequences(t, stdout); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("watched events %v, want the run's %v\nstderr:\n%s", got, want, stderr)
		}
		if len(want) <= len(before) {
			t.Fatalf("no event followed the replica's exit: %v then %v", before, want)
		}
		t.Logf("resume lines: %v", resumeLines(stderr))
	case <-time.After(2 * time.Minute):
		stdout, stderr := watch.output()
		t.Fatalf("run watch did not end\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
}

// A replica that dies leaves the kernel to end its connections: the FIN (or a
// reset, when the replica had unread bytes) is all a watch gets.
func TestIntegration_WatchResumesOnAnotherReplicaWhenOneIsKilled(t *testing.T) {
	watchThroughReplicaExit(t, syscall.SIGKILL)
}

// A replica told to stop ends its handlers first, and tells a watch the stream
// stopped short. Run with ORK_TEST_WATCH_ORKCTL, this holds an older orkctl too.
func TestIntegration_WatchResumesOnAnotherReplicaWhenOneIsStopped(t *testing.T) {
	watchThroughReplicaExit(t, syscall.SIGTERM)
}

// TestIntegration_AStoppingReplicaAnswersUnavailable: what a real engine stopped
// with SIGTERM says on the wire, read by a bare client that knows nothing of
// OrkStack's resume rules. Unavailable, from the engine and not from the client,
// is the answer every orkctl has resumed on. A finished run on the same replica
// still ends cleanly just before.
func TestIntegration_AStoppingReplicaAnswersUnavailable(t *testing.T) {
	c := newAwaitCLI(t, 1, false)
	replica, exited := c.startSignalledEngine()

	path := filepath.Join(t.TempDir(), "replica-exit.ork.yaml")
	if err := os.WriteFile(path, []byte(replicaExitOrkflowYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "orkflow", "create", path)
	submit := func() string {
		var submitted submitJSON
		if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "replica-exit", "--json")), &submitted); err != nil {
			t.Fatalf("decode submit: %v", err)
		}
		c.waitAwaiting(0, submitted.Run.ID, "gate")
		return submitted.Run.ID
	}
	client := orkstackv1connect.NewRunServiceClient(h2cClient(t), "http://"+replica.addr)
	open := func(runID string) *connect.ServerStreamForClient[orkstackv1.Event] {
		stream, err := client.StreamEvents(c.ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID, Namespace: c.namespace}))
		if err != nil {
			t.Fatalf("StreamEvents: %v", err)
		}
		t.Cleanup(func() { _ = stream.Close() })
		return stream
	}

	finished := submit()
	c.must(0, "run", "resolve", finished, "--task", "gate", "--values", `{"ok":true}`)
	whole := open(finished)
	sawEnd := false
	for whole.Receive() {
		sawEnd = sawEnd || api.IsTerminalEvent(whole.Msg())
	}
	if err := whole.Err(); err != nil || !sawEnd {
		t.Fatalf("a finished run's stream ended %v, terminal event seen %v; want a clean end after it", err, sawEnd)
	}

	live := open(submit())
	if !live.Receive() {
		t.Fatalf("no event from the live run: %v", live.Err())
	}
	if err := replica.process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal the replica: %v", err)
	}
	for live.Receive() {
	}
	err := live.Err()
	if connect.CodeOf(err) != connect.CodeUnavailable || !connect.IsWireError(err) {
		t.Fatalf("the stream ended %v (code %v, from the engine %v), want the engine's Unavailable",
			err, connect.CodeOf(err), connect.IsWireError(err))
	}
	if !strings.Contains(err.Error(), api.ErrEngineStopping.Error()) {
		t.Fatalf("the stream ended %q, want it to say the replica is shutting down", err)
	}
	select {
	case <-exited:
	case <-time.After(time.Minute):
		t.Fatal("the replica did not exit within a minute of SIGTERM")
	}
}

// talkingOrkflow is a task that writes numbered lines for a few seconds, each of
// its tries, and then exits with exit. Its name changes with its pool, which is
// the test's own.
func talkingOrkflow(pool string, lines, exit int, retry string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: talking
tasks:
  - id: talk
    uses: shell
    pool: %s
%s    with:
      op: exec
      command:
        - sh
        - -c
        - i=0; while [ $i -lt %d ]; do i=$((i+1)); echo line-$i; sleep 0.25; done; exit %d
`, pool, retry, lines, exit)
}

func talkLines(lines, tries int) string {
	var b strings.Builder
	for range tries {
		for i := 1; i <= lines; i++ {
			fmt.Fprintf(&b, "line-%d\n", i)
		}
	}
	return b.String()
}

// submitTalking applies and submits doc on the surviving replica, and starts
// `run logs -f` of its task through a forwarder to the doomed one, returning
// once the first line has printed.
func submitTalking(t *testing.T, c *awaitCLI, doc, doomed string) (string, *replicaForwarder, *watchProcess) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "talking.ork.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "orkflow", "create", path)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "talking", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	forwarder := newReplicaForwarder(t, doomed)
	logs := startOrkctlProcess(t, c, c.orkctl, "run", "logs", submitted.Run.ID, "--task", "talk", "-f",
		"--server", "http://"+forwarder.listener.Addr().String(), "-n", c.namespace)
	logs.awaitOutput(t, "the task's first line", func(stdout, _ string) bool {
		return strings.Contains(stdout, "line-1\n")
	})
	return submitted.Run.ID, forwarder, logs
}

// requireWholeLog waits for `run logs -f` to end and holds it to the task's
// whole output, printed exactly once, having resumed at least once.
func requireWholeLog(t *testing.T, logs *watchProcess, want string) {
	t.Helper()
	select {
	case code := <-logs.exited:
		stdout, stderr := logs.output()
		if code != 0 {
			t.Fatalf("run logs exit %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		if stdout != want {
			t.Fatalf("run logs printed\n%s\nwant\n%s\nstderr:\n%s", stdout, want, stderr)
		}
		if !strings.Contains(stderr, "read task logs: the stream of task talk was interrupted") {
			t.Fatalf("no resume was said, so the replica's exit proved nothing\nstderr:\n%s", stderr)
		}
		t.Logf("stderr:\n%s", stderr)
	case <-time.After(3 * time.Minute):
		stdout, stderr := logs.output()
		t.Fatalf("run logs did not end\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
}

func followThroughReplicaExit(t *testing.T, sig syscall.Signal) {
	t.Helper()
	c := newAwaitCLI(t, 1, true) // engine 0 is the replica that survives
	doomed, doomedExited := c.startSignalledEngine()
	const lines = 48
	_, forwarder, logs := submitTalking(t, c, talkingOrkflow(c.pool, lines, 0, ""), doomed.addr)

	forwarder.sendTo(strings.TrimPrefix(c.engines[0], "http://"))
	if err := doomed.process.Signal(sig); err != nil {
		t.Fatalf("signal the replica: %v", err)
	}
	select {
	case <-doomedExited:
	case <-time.After(time.Minute):
		t.Fatalf("the replica did not exit within a minute of %v", sig)
	}
	requireWholeLog(t, logs, talkLines(lines, 1))
}

func TestIntegration_RunLogsFollowResumesWhenAReplicaIsKilled(t *testing.T) {
	followThroughReplicaExit(t, syscall.SIGKILL)
}

func TestIntegration_RunLogsFollowResumesWhenAReplicaIsStopped(t *testing.T) {
	followThroughReplicaExit(t, syscall.SIGTERM)
}

// TestIntegration_RunLogsFollowKeepsATryThatEndedDuringAnOutage: no replica
// answers while the task's first try finishes and its retry starts, so the
// engine that finally answers has moved on from the cursor's attempt. The tail
// of the first try is still printed, before the second, and nothing twice.
func TestIntegration_RunLogsFollowKeepsATryThatEndedDuringAnOutage(t *testing.T) {
	c := newAwaitCLI(t, 1, true)
	doomed, doomedExited := c.startSignalledEngine()
	const lines = 24
	retry := "    retry:\n      maximum_attempts: 2\n      initial_interval_seconds: 1\n"
	runID, forwarder, logs := submitTalking(t, c, talkingOrkflow(c.pool, lines, 1, retry), doomed.addr)

	nobody := freeAddress(t)
	forwarder.sendTo(nobody)
	if err := doomed.process.Kill(); err != nil {
		t.Fatalf("kill the replica: %v", err)
	}
	<-doomedExited

	// The outage lasts until the second try has written, seen from the survivor.
	deadline := time.Now().Add(90 * time.Second)
	for {
		stdout, _, _ := c.on(0, "run", "logs", runID, "--task", "talk", "--attempt", "2")
		if strings.Contains(stdout, "line-1\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the second try wrote nothing within 90 s")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if stdout, stderr := logs.output(); strings.Contains(stdout, fmt.Sprintf("line-%d\n", lines)) {
		t.Fatalf("the first try's last line printed during the outage, so this proves nothing\n%s\n%s", stdout, stderr)
	}
	forwarder.sendTo(strings.TrimPrefix(c.engines[0], "http://"))
	requireWholeLog(t, logs, talkLines(lines, 2))
}

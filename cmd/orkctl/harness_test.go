package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// fixedNow is the injected clock. Every timestamp the API stamps is therefore
// deterministic, which is what makes the human-output golden files stable.
var fixedNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// stubStarter stands in for Temporal. Tests drive run state through the event
// bus and run store directly.
//
// A started run emits its own RUN_CREATED as event 1, because that is what
// RunWorkflow does on a real cluster: the API publishes no event of its own on
// the happy path, so a stub that did not emit it would be simulating an engine
// that does not exist.
type stubStarter struct {
	mu        sync.Mutex
	bus       *engine.EventBus
	started   []string
	canceled  []string
	reasons   []string
	startErr  error
	cancelErr error
	onStart   func(runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload)
}

func (s *stubStarter) StartRun(
	_ context.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) error {
	s.mu.Lock()
	s.started = append(s.started, runID)
	onStart := s.onStart
	bus := s.bus
	err := s.startErr
	s.mu.Unlock()
	if onStart != nil {
		onStart(runID, wf, input)
	}
	if err == nil && bus != nil {
		bus.Publish(&orkstackv1.Event{
			RunId:     runID,
			Sequence:  1,
			Type:      orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
			Status:    orkstackv1.Status_STATUS_PENDING,
			Message:   fmt.Sprintf("run created for orkflow %s@%s", wf.GetName(), wf.GetVersion()),
			Timestamp: timestamppb.New(fixedNow),
		}, runID+"#1")
	}
	return err
}

func (s *stubStarter) CancelRun(_ context.Context, runID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, runID)
	s.reasons = append(s.reasons, reason)
	return s.cancelErr
}

func (s *stubStarter) startedRuns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.started...)
}

func (s *stubStarter) canceledRuns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.canceled...)
}

// fixture is the real API server (the same one ork serves) listening on
// a loopback port, plus the stores behind it.
type fixture struct {
	t       *testing.T
	baseURL string
	bus     *engine.EventBus
	runs    *engine.RunStore
	starter *stubStarter
}

// newFixture starts the real h2c Connect server in-process, mirroring
// internal/api/service_test.go's harness.
func newFixture(t *testing.T, plugins ...*orkstackv1.PluginInfo) *fixture {
	t.Helper()
	// A stray ORK_SERVER in the developer's environment must not reach the
	// commands under test.
	t.Setenv(serverEnvVar, "")

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	starter := &stubStarter{bus: bus}
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      bus,
		Starter:  starter,
		Plugins:  plugins,
		Now:      func() time.Time { return fixedNow },
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})

	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	return &fixture{
		t:       t,
		baseURL: "http://" + listener.Addr().String(),
		bus:     bus,
		runs:    runs,
		starter: starter,
	}
}

// result is one captured CLI invocation.
type result struct {
	stdout string
	stderr string
	code   int
}

// run executes orkctl with --server pointed at the fixture, capturing both
// streams and the process exit code.
func (f *fixture) run(args ...string) result {
	f.t.Helper()
	var stdout bytes.Buffer
	outcome := f.runTo(&stdout, args...)
	outcome.stdout = stdout.String()
	return outcome
}

// runTo executes orkctl writing stdout into out, so a test can observe output
// while the command is still running.
func (f *fixture) runTo(out io.Writer, args ...string) result {
	f.t.Helper()
	full := append(append([]string(nil), args...), "--server", f.baseURL)
	var stderr bytes.Buffer
	code := Execute(full, out, &stderr)
	return result{stderr: stderr.String(), code: code}
}

// applyExample registers examples/hello.ork.yaml through the CLI.
func (f *fixture) applyExample() result {
	f.t.Helper()
	return f.run("apply", examplePath(f.t))
}

// examplePath resolves the repo's example orkflow from cmd/orkctl.
func examplePath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "hello.ork.yaml"))
	if err != nil {
		t.Fatalf("resolve example path: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("example orkflow missing: %v", err)
	}
	return path
}

// publish appends one event to a run's stream.
func (f *fixture) publish(
	runID string,
	eventType orkstackv1.EventType,
	status orkstackv1.Status,
	message string,
) {
	f.t.Helper()
	f.bus.Publish(&orkstackv1.Event{
		RunId:     runID,
		Type:      eventType,
		Status:    status,
		Message:   message,
		Timestamp: timestamppb.New(fixedNow),
	}, "")
}

// finishRun records a terminal run snapshot in the read model, as the engine's
// projection activity would.
func (f *fixture) finishRun(runID string, status orkstackv1.Status, runErr *orkstackv1.Error) {
	f.t.Helper()
	run, ok := f.runs.Get(runID)
	if !ok {
		f.t.Fatalf("run %s is not in the store", runID)
	}
	run.Status = status
	run.Error = runErr
	run.StartedAt = timestamppb.New(fixedNow)
	run.FinishedAt = timestamppb.New(fixedNow)
	f.runs.Apply(run)
}

// requireExit fails the test unless the invocation exited with want.
func (r result) requireExit(t *testing.T, want int) result {
	t.Helper()
	if r.code != want {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", r.code, want, r.stdout, r.stderr)
	}
	return r
}

// requireContains fails the test unless stdout contains every fragment.
func (r result) requireContains(t *testing.T, fragments ...string) result {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(r.stdout, fragment) {
			t.Fatalf("stdout does not contain %q\nstdout:\n%s", fragment, r.stdout)
		}
	}
	return r
}

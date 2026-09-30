package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
	_ engine.RunPlan,
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

// memoryRegistry is the namespace registry the CLI fixture talks to: the
// durable store's semantics in a map, including the conditional writes an
// update compare-and-swaps on.
//
// The clock is the fixture's fixed one, so a namespace's created/updated
// columns are stable and the human golden files can hold them.
type memoryRegistry struct {
	mu   sync.Mutex
	docs map[string]*orkstackv1.Namespace
	seq  int
}

func newMemoryRegistry() *memoryRegistry {
	return &memoryRegistry{docs: map[string]*orkstackv1.Namespace{}}
}

func (r *memoryRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name = artifacts.ResolveNamespace(name)
	if ns, ok := r.docs[name]; ok {
		return proto.Clone(ns).(*orkstackv1.Namespace), nil
	}
	if name == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
}

func (r *memoryRegistry) List(_ context.Context) ([]*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byName := map[string]*orkstackv1.Namespace{
		artifacts.DefaultNamespace: artifacts.DefaultNamespaceDocument(),
	}
	for name, ns := range r.docs {
		byName[name] = proto.Clone(ns).(*orkstackv1.Namespace)
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out, nil
}

func (r *memoryRegistry) Put(
	_ context.Context,
	ns *orkstackv1.Namespace,
	cond artifacts.PutConditions,
) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := artifacts.ResolveNamespace(ns.GetName())
	// allowed_secrets is held to the store's own rule, by the store's own
	// function, so this double refuses exactly what the store refuses.
	if err := artifacts.ValidNamespaceSecrets(ns); err != nil {
		return nil, err
	}
	existing, exists := r.docs[name]
	if cond.IfAbsent && exists {
		return nil, fmt.Errorf("put %q: %w", name, artifacts.ErrPreconditionFailed)
	}
	if cond.IfMatch != "" && (!exists || existing.GetEtag() != cond.IfMatch) {
		return nil, fmt.Errorf("put %q: %w", name, artifacts.ErrPreconditionFailed)
	}
	r.seq++
	stored := proto.Clone(ns).(*orkstackv1.Namespace)
	stored.Name = name
	stored.Etag = fmt.Sprintf("etag-%d", r.seq)
	stored.UpdatedAt = timestamppb.New(fixedNow)
	stored.CreatedAt = timestamppb.New(fixedNow)
	if exists && existing.GetCreatedAt() != nil {
		stored.CreatedAt = existing.GetCreatedAt()
	}
	// The hold's since is the registry's, by the rule the durable store
	// applies, on the fixture's held clock.
	artifacts.StampNamespaceHold(stored, existing, fixedNow)
	r.docs[name] = stored
	return proto.Clone(stored).(*orkstackv1.Namespace), nil
}

func (r *memoryRegistry) Delete(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name = artifacts.ResolveNamespace(name)
	if _, ok := r.docs[name]; !ok {
		return fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
	}
	delete(r.docs, name)
	return nil
}

// fixture is the real API server (the same one ork serves) listening on
// a loopback port, plus the stores behind it.
type fixture struct {
	t          *testing.T
	baseURL    string
	bus        *engine.EventBus
	runs       *engine.RunStore
	starter    *stubStarter
	namespaces *memoryRegistry
	// configPath is where this fixture's orkctl config file lives. Every
	// invocation is pointed at it, so a test never reads or writes the
	// developer's real one.
	configPath string
	// requestsMu and requestLog record every HTTP request the fixture's
	// listener actually received, in arrival order — the wire, not a fake
	// behind a handler. A test proving a CLI command refused something
	// LOCALLY, before any RPC left the process, asserts on requests()
	// rather than on a handler-level fake never being called: the fake not
	// being called only proves the Connect handler chose not to reach it,
	// which a request that reached the server and got rejected there would
	// also produce. Only an empty diff on requests() proves nothing
	// crossed the wire at all.
	requestsMu sync.Mutex
	requestLog []string
	// timeoutLog is the Connect-Timeout-Ms header each request carried, in
	// the same order: the deadline the CLI gave that one request.
	timeoutLog []string
	// bodyLog is each ScheduleService request's raw body (nil for every
	// other request), in the same order, so a test can decode exactly what
	// the CLI put on the wire.
	bodyLog [][]byte
}

// lastRequest decodes the body of the newest request sent to path into msg,
// and reports whether one was sent. The CLI's Connect clients send unary
// requests as uncompressed binary protobuf.
func (f *fixture) lastRequest(path string, msg proto.Message) bool {
	f.requestsMu.Lock()
	defer f.requestsMu.Unlock()
	for i := len(f.requestLog) - 1; i >= 0; i-- {
		if f.requestLog[i] == path {
			if err := proto.Unmarshal(f.bodyLog[i], msg); err != nil {
				f.t.Fatalf("decode request to %s: %v", path, err)
			}
			return true
		}
	}
	return false
}

// timeoutHeaders returns the Connect-Timeout-Ms header of every request
// received so far, in arrival order ("" when a request carried none).
func (f *fixture) timeoutHeaders() []string {
	f.requestsMu.Lock()
	defer f.requestsMu.Unlock()
	return append([]string(nil), f.timeoutLog...)
}

// requests returns every HTTP request path this fixture's server has
// received so far, in arrival order.
func (f *fixture) requests() []string {
	f.requestsMu.Lock()
	defer f.requestsMu.Unlock()
	return append([]string(nil), f.requestLog...)
}

// newFixture starts the real h2c Connect server in-process, mirroring
// internal/api/service_test.go's harness.
func newFixture(t *testing.T, activities ...*orkstackv1.ActivityInfo) *fixture {
	t.Helper()
	return newFixtureWith(t, nil, activities...)
}

// newFixtureWith is newFixture with the service config left open for one more
// seam, so a test about task logs does not need a second copy of this wiring.
func newFixtureWith(
	t *testing.T,
	configure func(*api.Config),
	activities ...*orkstackv1.ActivityInfo,
) *fixture {
	t.Helper()
	// A stray ORK_SERVER or ORK_NAMESPACE in the developer's environment must
	// not reach the commands under test, and neither must a config file they
	// saved with `orkctl namespace use`: both would silently move every
	// command in this package into another namespace.
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	configPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv(configEnvVar, configPath)

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	starter := &stubStarter{bus: bus}
	namespaces := newMemoryRegistry()
	counter := 0
	cfg := api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: namespaces,
		Runs:       runs,
		Bus:        bus,
		Starter:    starter,
		Activities: staticActivities(activities),
		Now:        func() time.Time { return fixedNow },
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	}
	if configure != nil {
		configure(&cfg)
	}
	svc := api.NewService(cfg)

	f := &fixture{
		t:          t,
		bus:        bus,
		runs:       runs,
		starter:    starter,
		namespaces: namespaces,
		configPath: configPath,
	}

	// recording wraps the real mux so requests() reflects exactly what hit
	// the wire — see the field comment on fixture.requestLog.
	mux := api.NewMux(svc, api.MuxOptions{})
	recording := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requestsMu.Lock()
		f.requestLog = append(f.requestLog, r.URL.Path)
		f.timeoutLog = append(f.timeoutLog, r.Header.Get("Connect-Timeout-Ms"))
		// Only unary schedule requests are buffered: a streaming body must
		// reach the handler as it arrives.
		var body []byte
		if strings.Contains(r.URL.Path, "ScheduleService") {
			body, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		f.bodyLog = append(f.bodyLog, body)
		f.requestsMu.Unlock()
		mux.ServeHTTP(w, r)
	})

	server := api.NewServer("127.0.0.1:0", recording)
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

	f.baseURL = "http://" + listener.Addr().String()
	return f
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

// requireStderrContains fails the test unless stderr contains every fragment.
func (r result) requireStderrContains(t *testing.T, fragments ...string) result {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(r.stderr, fragment) {
			t.Fatalf("stderr does not contain %q\nstderr:\n%s", fragment, r.stderr)
		}
	}
	return r
}

// staticActivities is the catalog seam a test wires by hand.
//
// It is the whole implementation Phase 1 needs: which activities exist is
// ENGINE CONFIGURATION (§4.2), so a fixture that states the configuration is
// stating the real thing rather than faking a worker. Phase 3 replaces it with
// the catalog that resolves each reference's index digest at apply time.
type staticActivities []*orkstackv1.ActivityInfo

func (a staticActivities) ListActivities(context.Context) ([]*orkstackv1.ActivityInfo, error) {
	return append([]*orkstackv1.ActivityInfo(nil), a...), nil
}

//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
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

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The harness of the continuation chaos suite (T3,
// continuation_chaos_integration_test.go): real Temporal and real MinIO, with
// every engine replica, projection worker and Orker an in-process worker the
// test owns and stops itself (AGENTS.md §11.1: never by name or pattern).
// Outages are simulated with internal/testsupport/faultproxy, never by
// stopping the shared infrastructure; the test's own control client dials
// Temporal directly, so signals, cancels and history reads go on throughout.

// ── workflow log capture ────────────────────────────────────────────────────

// chaosLogEntry is one line a replica's workflow code logged. The SDK's
// workflow logger is replay-aware, so each line is one decision.
type chaosLogEntry struct {
	At     time.Time      `json:"at"`
	Source string         `json:"source"`
	Level  string         `json:"level"`
	Msg    string         `json:"msg"`
	Fields map[string]any `json:"fields,omitempty"`
}

// chaosLogBook keeps every line every replica's workflow code logged, so a
// test can wait for a decision (a hold, a switch, a give-up) instead of
// inferring it from what did not happen.
type chaosLogBook struct {
	mu      sync.Mutex
	entries []chaosLogEntry
	notify  chan struct{}
}

func newChaosLogBook() *chaosLogBook { return &chaosLogBook{notify: make(chan struct{})} }

func (b *chaosLogBook) add(e chaosLogEntry) {
	b.mu.Lock()
	b.entries = append(b.entries, e)
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}

// logger is the SDK logger one source (a replica) writes through.
func (b *chaosLogBook) logger(source string) sdklog.Logger {
	return &chaosLogger{book: b, source: source}
}

// matching is every entry with msg whose run_id field is runID (any run when
// runID is empty).
func (b *chaosLogBook) matching(msg, runID string) []chaosLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []chaosLogEntry
	for _, e := range b.entries {
		if e.Msg != msg {
			continue
		}
		if runID != "" && fmt.Sprint(e.Fields["run_id"]) != runID {
			continue
		}
		out = append(out, e)
	}
	return out
}

// forRun is every entry naming runID, in order.
func (b *chaosLogBook) forRun(runID string) []chaosLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []chaosLogEntry
	for _, e := range b.entries {
		if fmt.Sprint(e.Fields["run_id"]) == runID || fmt.Sprint(e.Fields["WorkflowID"]) == runID {
			out = append(out, e)
		}
	}
	return out
}

// wait blocks until an entry with msg for runID exists, and returns the
// first one.
func (b *chaosLogBook) wait(t *testing.T, ctx context.Context, msg, runID string, within time.Duration) chaosLogEntry {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	for {
		b.mu.Lock()
		notify := b.notify
		b.mu.Unlock()
		if found := b.matching(msg, runID); len(found) > 0 {
			return found[0]
		}
		select {
		case <-notify:
		case <-deadline.C:
			t.Fatalf("run %s never logged %q within %s", runID, msg, within)
		case <-ctx.Done():
			t.Fatalf("run %s: context ended waiting for %q: %v", runID, msg, ctx.Err())
		}
	}
}

type chaosLogger struct {
	book   *chaosLogBook
	source string
	prefix []any
}

func (l *chaosLogger) log(level, msg string, keyvals []any) {
	fields := map[string]any{}
	all := append(append([]any(nil), l.prefix...), keyvals...)
	for i := 0; i+1 < len(all); i += 2 {
		fields[fmt.Sprint(all[i])] = all[i+1]
	}
	l.book.add(chaosLogEntry{At: time.Now(), Source: l.source, Level: level, Msg: msg, Fields: fields})
}

func (l *chaosLogger) Debug(msg string, keyvals ...any) {}
func (l *chaosLogger) Info(msg string, keyvals ...any)  { l.log("info", msg, keyvals) }
func (l *chaosLogger) Warn(msg string, keyvals ...any)  { l.log("warn", msg, keyvals) }
func (l *chaosLogger) Error(msg string, keyvals ...any) { l.log("error", msg, keyvals) }
func (l *chaosLogger) With(keyvals ...any) sdklog.Logger {
	return &chaosLogger{book: l.book, source: l.source, prefix: append(append([]any(nil), l.prefix...), keyvals...)}
}

// logNoSwitch is the writer's operator log line for a generation that will
// never switch (2.13); the others are declared beside the workflow tests
// (continuation_paths_test.go).
const logNoSwitch = "run will not carry on into a fresh history"

// ── the stack ───────────────────────────────────────────────────────────────

// chaosStack is one deployment over a private Temporal namespace and private
// queues: engine replicas, a projection worker and Orkers are started by the
// scenario, each one an in-process worker it can stop.
type chaosStack struct {
	t          *testing.T
	ctx        context.Context
	scenario   string // C<n>: the transcript group
	address    string // the Temporal frontend, upstream of any proxy
	namespace  string
	control    client.Client // dials Temporal directly: signals, cancels, history
	runQueue   string
	orkerQueue string
	cfg        artifacts.Config
	store      *artifacts.S3RunSnapshotStore // direct: what the test reads and writes
	values     *memValues
	runs       *engine.RunStore
	bus        *engine.EventBus
	archive    *engine.RunArchiver
	reader     *engine.TemporalRunReader // the control reader
	starter    *engine.TemporalStarter
	logs       *chaosLogBook
	transcript *chaosTranscript

	mu         sync.Mutex
	replicas   map[string]*chaosReplica
	projection worker.Worker
	orkers     map[string]*chaosOrker
	attempts   map[string]int // task[/item] -> ExecuteTask invocations
	canceled   map[string]int // task[/item] -> attempts whose context was canceled
	ranOn      map[string][]string
}

func newChaosStack(t *testing.T, ctx context.Context, scenario string) *chaosStack {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("local run archive: %v", err)
	}
	return newChaosStackWith(t, ctx, scenario, cfg, local)
}

// newChaosStackWith is newChaosStack over the object storage cfg names and
// the run archive archive (a scenario that retires runs needs both in a
// bucket of its own).
func newChaosStackWith(t *testing.T, ctx context.Context, scenario string, cfg artifacts.Config, local artifacts.RunArchive) *chaosStack {
	t.Helper()
	address := temporalAddress()
	namespace := chaosNamespace(scenario)
	control := dialRoutingCluster(t, ctx, address, namespace)
	store, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	label := chaosLabel(scenario)
	s := &chaosStack{
		t: t, ctx: ctx, scenario: scenario, address: address, namespace: namespace, control: control,
		runQueue:   "ork-chaos-" + label + "-run-" + suffix,
		orkerQueue: "ork-chaos-" + label + "-tasks-" + suffix,
		cfg:        cfg, store: store,
		values: &memValues{objects: map[string][]byte{}},
		runs:   engine.NewRunStore(), bus: engine.NewEventBus(),
		archive:  engine.NewRunArchiver(local),
		logs:     newChaosLogBook(),
		replicas: map[string]*chaosReplica{}, orkers: map[string]*chaosOrker{},
		attempts: map[string]int{}, canceled: map[string]int{}, ranOn: map[string][]string{},
	}
	s.transcript = newChaosTranscript(t, scenario)
	s.reader = engine.NewTemporalRunReader(control).WithSnapshotStore(store)
	s.starter = engine.NewTemporalStarterWithOrkerQueue(control, s.runQueue, s.orkerQueue)
	t.Cleanup(func() { s.transcript.write(t, s.logs) })
	return s
}

// chaosNamespace is a private Temporal namespace for one scenario, under the
// operator's ORK_TEMPORAL_NAMESPACE prefix.
func chaosNamespace(scenario string) string {
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	return fmt.Sprintf("%s-chaos-%s-%d-%d", base, chaosLabel(scenario), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// chaosLabel is a scenario name as a namespace or queue segment: "C2/adopted"
// becomes "c2-adopted".
func chaosLabel(scenario string) string {
	return strings.ReplaceAll(strings.ToLower(scenario), "/", "-")
}

// dial is one client of this stack's namespace for source: its identity (the
// worker identity history records) and its workflow logger, through address
// (the upstream, or a proxy in front of it).
func (s *chaosStack) dial(source, address string) client.Client {
	s.t.Helper()
	if address == "" {
		address = s.address
	}
	c, err := client.DialContext(s.ctx, client.Options{
		HostPort: address, Namespace: s.namespace, Identity: source, Logger: s.logs.logger(source),
	})
	if err != nil {
		s.t.Fatalf("dial %s for %s: %v", address, source, err)
	}
	s.t.Cleanup(c.Close)
	return c
}

// snapshotStoreVia is a run snapshot store whose S3 client dials endpoint
// (a faultproxy address) instead of MinIO directly.
func (s *chaosStack) snapshotStoreVia(proxy *faultproxy.Proxy) *artifacts.S3RunSnapshotStore {
	s.t.Helper()
	cfg := s.cfg
	cfg.S3Endpoint = "http://" + proxy.Addr()
	store, err := artifacts.NewS3RunSnapshotStore(s.ctx, cfg)
	if err != nil {
		s.t.Fatalf("snapshot store via proxy: %v", err)
	}
	return store
}

// minioProxy is a faultproxy in front of the stack's MinIO.
func (s *chaosStack) minioProxy() *faultproxy.Proxy {
	s.t.Helper()
	endpoint := strings.TrimPrefix(strings.TrimPrefix(s.cfg.S3Endpoint, "http://"), "https://")
	return faultproxy.New(s.t, strings.TrimSuffix(endpoint, "/"))
}

// temporalProxy is a faultproxy in front of the Temporal frontend.
func (s *chaosStack) temporalProxy() *faultproxy.Proxy {
	s.t.Helper()
	return faultproxy.New(s.t, s.address)
}

// replicaOptions is how one engine replica is built.
type replicaOptions struct {
	temporal    string               // the address its client dials; "" is the frontend itself
	store       engine.SnapshotStore // its snapshot store; nil is MinIO directly
	beforeWrite func(ctx context.Context, kind string) error
	beforeRead  func(ctx context.Context, offset int64) error
	client      client.Client // an existing client to share (its identity is the replica's)
	extra       []func(w worker.Worker)
	deployment  task.Deployment // the Worker Deployment Version it runs; zero is unversioned
	identity    string          // the identity its run worker polls with; "" is its name
	// eachValues, when set, is where its CountEachItems reads a fan-out's
	// array (the object store a real Orker committed it to); nil is the
	// stack's in-memory values.
	eachValues engine.EachValuesReader
}

// chaosReplica is one engine replica's run worker: RunWorkflow and every
// local activity cmd/ork registers on it.
type chaosReplica struct {
	name    string
	client  client.Client
	worker  worker.Worker
	store   engine.SnapshotStore
	reader  *engine.TemporalRunReader
	mu      sync.Mutex
	stopped bool
}

// startReplica starts an engine replica named name.
func (s *chaosStack) startReplica(name string, o replicaOptions) *chaosReplica {
	s.t.Helper()
	c := o.client
	if c == nil {
		c = s.dial(name, o.temporal)
	}
	var store engine.SnapshotStore = s.store
	if o.store != nil {
		store = o.store
	}
	activities := engine.NewSnapshotActivities(store)
	if o.beforeWrite != nil || o.beforeRead != nil {
		activities = activities.WithContinuationHooksForTest(o.beforeWrite, o.beforeRead)
	}
	identity := name
	if o.identity != "" {
		identity = o.identity
	}
	options := worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
		Identity:                    identity,
	}
	if o.deployment.BuildID != "" {
		options = o.deployment.Apply(options)
	}
	w := worker.New(c, s.runQueue, options)
	engine.RegisterWorkflows(w)
	if o.eachValues != nil {
		registerRunActivitiesWith(w, activities, s.values, o.eachValues)
	} else {
		registerRunActivities(w, activities, s.values)
	}
	for _, register := range o.extra {
		register(w)
	}
	if err := w.Start(); err != nil {
		s.t.Fatalf("start replica %s: %v", name, err)
	}
	r := &chaosReplica{name: name, client: c, worker: w, store: store,
		reader: engine.NewTemporalRunReader(c).WithSnapshotStore(s.store)}
	s.mu.Lock()
	s.replicas[name] = r
	s.mu.Unlock()
	s.t.Cleanup(r.stop)
	s.transcript.step("replica %s started (run worker on %s)", name, s.runQueue)
	return r
}

// stop stops the replica's run worker: it stops polling, and a local
// activity still running in it has its late result dropped by the SDK.
// Idempotent: a scenario step and a cleanup.
func (r *chaosReplica) stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	r.worker.Stop()
}

// startProjection starts the projection worker (PublishEvent on the
// engine-event channel, archiving), over c (the control client when nil).
func (s *chaosStack) startProjection(c client.Client) worker.Worker {
	s.t.Helper()
	if c == nil {
		c = s.control
	}
	w := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(s.bus, s.runs, s.archive).WithEventSource(s.reader).WithSnapshotStore(s.store).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := w.Start(); err != nil {
		s.t.Fatalf("start the projection worker: %v", err)
	}
	var once sync.Once
	stoppable := &stoppableWorker{Worker: w, stop: func() { once.Do(w.Stop) }}
	s.t.Cleanup(stoppable.Stop)
	s.mu.Lock()
	s.projection = stoppable
	s.mu.Unlock()
	s.transcript.step("projection worker started")
	return stoppable
}

// startCapableProjection is startProjection polling with this build's
// engine identity, as cmd/ork's projection worker does: the snapshot-overlay
// fleet check reads every poller of the projection channel, and an
// anonymous one counts as a release that cannot read an overlay.
func (s *chaosStack) startCapableProjection(name string) worker.Worker {
	s.t.Helper()
	w := worker.New(s.control, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true,
		DisableWorkflowWorker: true, Identity: engine.EngineIdentity(fmt.Sprintf("%d@%s@", os.Getpid(), name))})
	w.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(s.bus, s.runs, s.archive).WithEventSource(s.reader).WithSnapshotStore(s.store).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := w.Start(); err != nil {
		s.t.Fatalf("start the projection worker: %v", err)
	}
	var once sync.Once
	stoppable := &stoppableWorker{Worker: w, stop: func() { once.Do(w.Stop) }}
	s.t.Cleanup(stoppable.Stop)
	s.mu.Lock()
	s.projection = stoppable
	s.mu.Unlock()
	s.transcript.step("projection worker %s started with this build's capabilities", name)
	return stoppable
}

// stoppableWorker makes Stop idempotent, so a scenario's stop and the
// cleanup's never stop one worker twice.
type stoppableWorker struct {
	worker.Worker
	stop func()
}

func (w *stoppableWorker) Stop() { w.stop() }

// chaosBehavior is one Orker's ExecuteTask: orker is the Orker's identity.
type chaosBehavior func(ctx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)

// chaosOrker is one in-process Orker on the stack's task channel. It hosts
// pins (a session worker) unless noPins.
type chaosOrker struct {
	name    string
	worker  worker.Worker
	mu      sync.Mutex
	stopped bool
}

func (o *chaosOrker) stop() {
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return
	}
	o.stopped = true
	o.mu.Unlock()
	o.worker.Stop()
}

// startOrker starts an Orker named name on the stack's task channel (or on
// queue, when set), counting every attempt it is handed.
func (s *chaosStack) startOrker(name string, behavior chaosBehavior, queue ...string) *chaosOrker {
	s.t.Helper()
	q := s.orkerQueue
	if len(queue) > 0 {
		q = queue[0]
	}
	return s.startOrkerWith(name, q, s.control, behavior)
}

// startOrkerWith starts an Orker named name on queue over c.
func (s *chaosStack) startOrkerWith(name, q string, c client.Client, behavior chaosBehavior) *chaosOrker {
	return s.startOrkerVersioned(name, q, c, task.Deployment{}, behavior)
}

// startOrkerVersioned starts an Orker named name on queue over c, in the
// Worker Deployment Version deployment (zero: unversioned).
func (s *chaosStack) startOrkerVersioned(name, q string, c client.Client, deployment task.Deployment, behavior chaosBehavior) *chaosOrker {
	s.t.Helper()
	return s.startOrkerSized(name, q, c, deployment, 2000, behavior)
}

// startOrkerSized is startOrkerVersioned running at most slots tasks at once,
// as a real Orker's orker.max_concurrent bounds it.
func (s *chaosStack) startOrkerSized(name, q string, c client.Client, deployment task.Deployment, slots int, behavior chaosBehavior) *chaosOrker {
	s.t.Helper()
	options := worker.Options{
		DisableRegistrationAliasing:        true,
		DisableWorkflowWorker:              true,
		EnableSessionWorker:                true,
		MaxConcurrentSessionExecutionSize:  4,
		MaxConcurrentActivityExecutionSize: slots,
		Identity:                           name,
	}
	if deployment.BuildID != "" {
		options = deployment.Apply(options)
	}
	w := worker.New(c, q, options)
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		key := attemptKey(in)
		s.mu.Lock()
		s.attempts[key]++
		s.ranOn[key] = append(s.ranOn[key], name)
		s.mu.Unlock()
		r, err := behavior(actx, s, name, in)
		if actx.Err() != nil {
			s.mu.Lock()
			s.canceled[key]++
			s.mu.Unlock()
		}
		if r != nil && r.GetOrkerId() == "" {
			r.OrkerId = name
		}
		return r, err
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})
	if err := w.Start(); err != nil {
		s.t.Fatalf("start orker %s: %v", name, err)
	}
	o := &chaosOrker{name: name, worker: w}
	s.mu.Lock()
	s.orkers[name] = o
	s.mu.Unlock()
	s.t.Cleanup(o.stop)
	s.transcript.step("orker %s started on %s", name, q)
	return o
}

func attemptKey(in *orkstackv1.ExecuteTaskInput) string {
	if in.GetEach() != nil {
		return fmt.Sprintf("%s[%d]", in.GetTaskId(), in.GetEach().GetIndex())
	}
	return in.GetTaskId()
}

// attemptsOf is how many attempts the Orkers were handed for key, and how
// many of those saw their context canceled.
func (s *chaosStack) attemptsOf(key string) (attempts, canceled int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[key], s.canceled[key]
}

// orkersOf is the Orkers key ran on, in order.
func (s *chaosStack) orkersOf(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranOn[key]...)
}

// instant answers a task at once, with its values committed to the stack's
// store (so a fan-out can count a list, and an output can be read back).
func instant(_ context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetEach() != nil {
		r := itemResult(in)
		r.OrkerId = orker
		return r, nil
	}
	data := valuesJSON(map[string]any{"task": in.GetTaskId()})
	if strings.HasPrefix(in.GetTaskId(), "list") {
		data = []byte(partsJSON(listSizeOf(in.GetTaskId())))
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: orker, ValuesJson: data,
		ValuesInlined: true, ValuesRef: s.values.put("mem://values/"+in.GetRunId()+"/"+in.GetTaskId(), data)}, nil
}

// listSizeOf reads N from a task id "listN" (6 when unnamed).
func listSizeOf(id string) int {
	var n int
	if _, err := fmt.Sscanf(id, "list%d", &n); err == nil && n > 0 {
		return n
	}
	return 6
}

// gates hold chosen tasks until the test releases them; a held attempt
// heartbeats, so the heartbeat timeout never retries it.
type gates struct {
	mu      sync.Mutex
	release map[string]chan struct{}
	entered map[string]chan struct{}
}

func newGates() *gates {
	return &gates{release: map[string]chan struct{}{}, entered: map[string]chan struct{}{}}
}

func (g *gates) chans(key string) (release, entered chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release[key] == nil {
		g.release[key] = make(chan struct{})
		g.entered[key] = make(chan struct{})
	}
	return g.release[key], g.entered[key]
}

// hold blocks the attempt for key until released or canceled, heartbeating.
func (g *gates) hold(ctx context.Context, key string) error {
	release, entered := g.chans(key)
	g.mu.Lock()
	select {
	case <-entered:
	default:
		close(entered)
	}
	g.mu.Unlock()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			activity.RecordHeartbeat(ctx)
		}
	}
}

// open releases key (idempotent).
func (g *gates) open(key string) {
	release, _ := g.chans(key)
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-release:
	default:
		close(release)
	}
}

// waitEntered blocks until an attempt for key is held.
func (g *gates) waitEntered(t *testing.T, key string, within time.Duration) {
	t.Helper()
	_, entered := g.chans(key)
	select {
	case <-entered:
	case <-time.After(within):
		t.Fatalf("no attempt of %s reached its gate within %s", key, within)
	}
}

// gated holds every task g knows a gate for, then answers instantly.
func gated(g *gates, keys ...string) chaosBehavior {
	held := map[string]bool{}
	for _, k := range keys {
		held[k] = true
	}
	return func(ctx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if key := attemptKey(in); held[key] || held[in.GetTaskId()] {
			if err := g.hold(ctx, key); err != nil {
				return nil, err
			}
		}
		return instant(ctx, s, orker, in)
	}
}

// ── a hook that blocks a local activity until the test releases it ─────────

// blockingHook blocks the first call matching want until released, and
// never returns because of its context (2.11): only the test releases it,
// with nil or an error of its choosing. Later calls pass through.
type blockingHook struct {
	mu       sync.Mutex
	want     func(kind string, offset int64) bool
	fired    bool
	entered  chan time.Time
	release  chan error
	released bool
	onEntry  func()
}

func newBlockingHook(want func(kind string, offset int64) bool) *blockingHook {
	return &blockingHook{want: want, entered: make(chan time.Time, 1), release: make(chan error, 1)}
}

func (h *blockingHook) call(kind string, offset int64) error {
	h.mu.Lock()
	if h.fired || !h.want(kind, offset) {
		h.mu.Unlock()
		return nil
	}
	h.fired = true
	onEntry := h.onEntry
	h.mu.Unlock()
	if onEntry != nil {
		onEntry()
	}
	h.entered <- time.Now()
	return <-h.release
}

func (h *blockingHook) write(_ context.Context, kind string) error { return h.call(kind, -1) }
func (h *blockingHook) read(_ context.Context, offset int64) error { return h.call("", offset) }

// waitEntered returns when the hook was entered.
func (h *blockingHook) waitEntered(t *testing.T, within time.Duration) time.Time {
	t.Helper()
	select {
	case at := <-h.entered:
		h.entered <- at // keep it readable
		return at
	case <-time.After(within):
		t.Fatalf("the hook was never entered within %s", within)
	}
	return time.Time{}
}

// open releases the hook with err (once; later calls do nothing).
func (h *blockingHook) open(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return
	}
	h.released = true
	h.release <- err
}

// ── plans and starts ────────────────────────────────────────────────────────

// plan is the stack's routing plan with a history limit, a continuation
// budget (nil: none) and the default in-flight cap.
func (s *chaosStack) plan(maxEvents, maxBytes int64, budget *engine.ContinuationBudget) engine.RunPlan {
	plan := rawPlan(s.orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.History = &engine.HistoryLimit{MaxEvents: maxEvents, MaxBytes: maxBytes, Reserve: engine.DefaultHistoryReserve}
	plan.Continuation = budget
	limit := engine.DefaultInFlightLimit(engine.DefaultRunMaxInFlight)
	plan.InFlight = &limit
	return plan
}

// tcPlan is the plan of the T3 rows C1–C12, C14–C16, C18–C23 and C28–C30
// (table TC): 1,000 events, the default bytes limit, a 120-event and 1 MiB
// budget, and a 5-minute restore timeout.
func (s *chaosStack) tcPlan() (engine.RunPlan, engine.ContinuationBudget) {
	budget := engine.DefaultContinuationBudget(120, 1<<20, 5*time.Minute)
	return s.plan(1000, engine.DefaultHistoryMaxBytes, &budget), budget
}

func (s *chaosStack) newRunID() string {
	return fmt.Sprintf("ork-chaos-%s-%d", chaosLabel(s.scenario), time.Now().UnixNano())
}

// start starts a run of wf with plan.
func (s *chaosStack) start(runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) {
	s.t.Helper()
	if wf.GetNamespace() == "" {
		wf.Namespace = "default"
	}
	if err := s.starter.StartRun(s.ctx, runID, wf, input, plan); err != nil {
		s.t.Fatalf("StartRun %s: %v", runID, err)
	}
	s.transcript.step("run %s started", runID)
}

// ── observing a run ─────────────────────────────────────────────────────────

// current is the run's current execution and its status.
func (s *chaosStack) current(runID string) (string, enums.WorkflowExecutionStatus) {
	s.t.Helper()
	described, err := s.control.DescribeWorkflowExecution(s.ctx, runID, "")
	if err != nil {
		s.t.Fatalf("describe %s: %v", runID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	return info.GetExecution().GetRunId(), info.GetStatus()
}

// chain is the run's executions, oldest first.
func (s *chaosStack) chain(runID string) []string {
	s.t.Helper()
	execution, _ := s.current(runID)
	carry := carryOf(s.t, s.history(runID, execution))
	var out []string
	for _, seg := range carry.GetSegments() {
		out = append(out, seg.GetExecutionId())
	}
	return append(out, execution)
}

// waitChain blocks until the run has more than n executions.
func (s *chaosStack) waitChain(runID string, n int, within time.Duration) []string {
	s.t.Helper()
	deadline := time.Now().Add(within)
	for {
		if chain := s.chain(runID); len(chain) > n {
			return chain
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s did not pass %d executions within %s", runID, n, within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// history is one execution's whole history.
func (s *chaosStack) history(runID, execution string) *historypb.History {
	s.t.Helper()
	recorded := &historypb.History{}
	iter := s.control.GetWorkflowHistory(s.ctx, runID, execution, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			s.t.Fatalf("read history of %s/%s: %v", runID, execution, err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	return recorded
}

// histories is every execution's history, oldest first.
func (s *chaosStack) histories(runID string) ([]string, []*historypb.History) {
	chain := s.chain(runID)
	out := make([]*historypb.History, len(chain))
	for i, execution := range chain {
		out[i] = s.history(runID, execution)
	}
	return chain, out
}

// wait blocks until the run's chain has closed, and returns its last
// execution's close status and the workflow error.
func (s *chaosStack) wait(runID string, within time.Duration) (enums.WorkflowExecutionStatus, error) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, within)
	defer cancel()
	err := s.control.GetWorkflow(ctx, runID, "").Get(ctx, nil)
	if ctx.Err() != nil {
		s.t.Fatalf("run %s did not end within %s", runID, within)
	}
	_, status := s.current(runID)
	s.transcript.step("run %s closed %s (%v)", runID, status, err)
	return status, err
}

// streamCollector is a stream opened through a reader, kept until it ends.
type streamCollector struct {
	mu     sync.Mutex
	events []*orkstackv1.Event
	// grew is closed, and replaced, each time an event arrives, so a waiter
	// wakes on the stream itself rather than on a clock.
	grew chan struct{}
	done chan error
}

func (s *chaosStack) openStream(reader *engine.TemporalRunReader, runID string, from int64) *streamCollector {
	c := &streamCollector{grew: make(chan struct{}), done: make(chan error, 1)}
	go func() {
		c.done <- reader.StreamEvents(s.ctx, runID, from, func(event *orkstackv1.Event) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.events = append(c.events, event)
			close(c.grew)
			c.grew = make(chan struct{})
			return nil
		})
	}()
	return c
}

// waitFor blocks until the stream has delivered an event matching want and
// returns it, failing when the stream ends without one or within passes.
func (c *streamCollector) waitFor(t *testing.T, within time.Duration, what string, want func(*orkstackv1.Event) bool) *orkstackv1.Event {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	seen, ended := 0, false
	for {
		c.mu.Lock()
		for ; seen < len(c.events); seen++ {
			if want(c.events[seen]) {
				e := c.events[seen]
				c.mu.Unlock()
				return e
			}
		}
		grew := c.grew
		c.mu.Unlock()
		if ended {
			t.Fatalf("the stream ended after %d events without %s", seen, what)
		}
		select {
		case <-grew:
		case err := <-c.done:
			c.done <- err // result() still reads how the stream ended
			ended = true  // one more look at what arrived before the end
		case <-deadline.C:
			t.Fatalf("the stream delivered no %s within %s (%d events)", what, within, seen)
		}
	}
}

// result waits for the stream to end and returns what it saw.
func (c *streamCollector) result(t *testing.T, within time.Duration) []*orkstackv1.Event {
	t.Helper()
	select {
	case err := <-c.done:
		if err != nil {
			t.Fatalf("the stream ended with %v", err)
		}
	case <-time.After(within):
		t.Fatalf("the stream did not end within %s", within)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*orkstackv1.Event(nil), c.events...)
}

// requireOneRunRead checks the run reads as one run that ended with status
// (and code, when set): its stream opened at submit, Events(0) and the
// archive all agree and are contiguous. It returns Events(0).
func (s *chaosStack) requireOneRunRead(runID string, stream *streamCollector, status orkstackv1.Status, code string) []*orkstackv1.Event {
	s.t.Helper()
	run, err := s.reader.GetRun(s.ctx, runID)
	if err != nil {
		s.t.Fatalf("GetRun: %v", err)
	}
	if run.GetId() != runID || run.GetStatus() != status || (code != "" && run.GetError().GetCode() != code) {
		s.t.Fatalf("GetRun = %s %v %v, want %v %s", run.GetId(), run.GetStatus(), run.GetError(), status, code)
	}
	events, err := s.reader.Events(s.ctx, runID, 0)
	if err != nil {
		s.t.Fatalf("Events(0): %v", err)
	}
	requireContiguous(s.t, events)
	if stream != nil {
		streamed := stream.result(s.t, time.Minute)
		if strings.Join(eventKeys(streamed), "\n") != strings.Join(eventKeys(events), "\n") {
			s.t.Fatalf("the stream opened at submit saw\n%s\nEvents(0) reads\n%s",
				strings.Join(eventKeys(streamed), "\n"), strings.Join(eventKeys(events), "\n"))
		}
	}
	archived := awaitArchivedEvents(s.t, s.ctx, s.archive, runID)
	if strings.Join(eventKeys(archived), "\n") != strings.Join(eventKeys(events), "\n") {
		s.t.Fatalf("the archive body's events differ from Events(0)")
	}
	s.transcript.step("run %s reads as one run: %v %s, %d events contiguous, stream == Events(0) == archive",
		runID, run.GetStatus(), run.GetError().GetCode(), len(events))
	return events
}

// ── reading histories ───────────────────────────────────────────────────────

// historyFacts is what one execution's history recorded, decoded.
type historyFacts struct {
	tasks          map[int64]string // ExecuteTask scheduled event id -> task[/item] key
	scheduledAt    map[string][]int64
	startedAttempt map[string][]int32
	completedAt    map[string][]int64
	failedAt       map[string][]int64
	canceledAt     map[string][]int64
	timedOutAt     map[string][]int64
	continuedAt    int64 // the WorkflowExecutionContinuedAsNew event id, 0 if none
	closed         enums.EventType
	taskFailures   []*historypb.HistoryEvent // WorkflowTaskFailed events
	taskStarted    []*historypb.HistoryEvent // WorkflowTaskStarted events
	markers        map[string]int            // local-activity marker counts by activity type
	signaledAt     []int64
	cancelAskedAt  int64
	length         int
}

func factsOf(t *testing.T, h *historypb.History) historyFacts {
	t.Helper()
	f := historyFacts{tasks: map[int64]string{}, scheduledAt: map[string][]int64{}, startedAttempt: map[string][]int32{},
		completedAt: map[string][]int64{}, failedAt: map[string][]int64{}, canceledAt: map[string][]int64{},
		timedOutAt: map[string][]int64{}, markers: map[string]int{}, length: len(h.GetEvents())}
	for _, event := range h.GetEvents() {
		id := event.GetEventId()
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			var in *orkstackv1.ExecuteTaskInput
			if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
				t.Fatal(err)
			}
			key := attemptKey(in)
			f.tasks[id] = key
			f.scheduledAt[key] = append(f.scheduledAt[key], id)
		}
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			if key, ok := f.tasks[attrs.GetScheduledEventId()]; ok {
				f.startedAttempt[key] = append(f.startedAttempt[key], attrs.GetAttempt())
			}
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			if key, ok := f.tasks[attrs.GetScheduledEventId()]; ok {
				f.completedAt[key] = append(f.completedAt[key], id)
			}
		}
		if attrs := event.GetActivityTaskFailedEventAttributes(); attrs != nil {
			if key, ok := f.tasks[attrs.GetScheduledEventId()]; ok {
				f.failedAt[key] = append(f.failedAt[key], id)
			}
		}
		if attrs := event.GetActivityTaskCanceledEventAttributes(); attrs != nil {
			if key, ok := f.tasks[attrs.GetScheduledEventId()]; ok {
				f.canceledAt[key] = append(f.canceledAt[key], id)
			}
		}
		if attrs := event.GetActivityTaskTimedOutEventAttributes(); attrs != nil {
			if key, ok := f.tasks[attrs.GetScheduledEventId()]; ok {
				f.timedOutAt[key] = append(f.timedOutAt[key], id)
			}
		}
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "LocalActivity" {
			var data struct{ ActivityType string }
			_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data)
			f.markers[data.ActivityType]++
		}
		switch event.GetEventType() {
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			f.continuedAt = id
			f.closed = event.GetEventType()
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
			f.closed = event.GetEventType()
		case enums.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			f.taskFailures = append(f.taskFailures, event)
		case enums.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			f.taskStarted = append(f.taskStarted, event)
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			f.signaledAt = append(f.signaledAt, id)
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			f.cancelAskedAt = id
		}
	}
	return f
}

// unhandledCommandFailures is every WorkflowTaskFailed with cause
// UNHANDLED_COMMAND.
func (f historyFacts) unhandledCommandFailures() []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range f.taskFailures {
		if e.GetWorkflowTaskFailedEventAttributes().GetCause() == enums.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			out = append(out, e)
		}
	}
	return out
}

// lastCompletion is the largest completion event id over keys.
func (f historyFacts) lastCompletion(keys ...string) int64 {
	var last int64
	for _, k := range keys {
		for _, id := range f.completedAt[k] {
			last = max(last, id)
		}
	}
	return last
}

// ── transcripts ─────────────────────────────────────────────────────────────

// chaosTranscript is one scenario's evidence, under
// artifacts/continuation-chaos/C<n>/ (E3): its steps as they happened, the
// figures it quotes, and every workflow log line of its runs.
type chaosTranscript struct {
	mu      sync.Mutex
	group   string
	t       *testing.T
	lines   []string
	figures map[string]any
	runs    []string
}

func newChaosTranscript(t *testing.T, scenario string) *chaosTranscript {
	return &chaosTranscript{group: "continuation-chaos/" + scenario, t: t, figures: map[string]any{}}
}

func (tr *chaosTranscript) step(format string, args ...any) {
	line := time.Now().UTC().Format("15:04:05.000") + " " + fmt.Sprintf(format, args...)
	tr.mu.Lock()
	tr.lines = append(tr.lines, line)
	tr.mu.Unlock()
	tr.t.Log(line)
}

// figure records a quoted figure (and logs it).
func (tr *chaosTranscript) figure(name string, value any) {
	tr.mu.Lock()
	tr.figures[name] = value
	tr.mu.Unlock()
	encoded, _ := json.Marshal(value)
	tr.t.Logf("FIGURE %s %s: %s", tr.group, name, encoded)
}

func (tr *chaosTranscript) run(runID string) {
	tr.mu.Lock()
	tr.runs = append(tr.runs, runID)
	tr.mu.Unlock()
}

func (tr *chaosTranscript) write(t *testing.T, logs *chaosLogBook) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	verdict := "PASS"
	if t.Failed() {
		verdict = "FAIL"
	}
	body := strings.Join(append(append([]string(nil), tr.lines...), "verdict: "+verdict), "\n") + "\n"
	proof.Write(t, tr.group, "transcript.txt", []byte(body))
	figures := map[string]any{"verdict": verdict}
	for k, v := range tr.figures {
		figures[k] = v
	}
	proof.WriteJSON(t, tr.group, "figures.json", figures)
	var entries []chaosLogEntry
	for _, runID := range tr.runs {
		entries = append(entries, logs.forRun(runID)...)
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].At.Before(entries[j].At) })
	proof.WriteJSON(t, tr.group, "workflow-log.json", entries)
}

// ── real Orkers (Docker) ────────────────────────────────────────────────────

// orkerCall is one ExecuteTask.
type orkerCall func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)

// realOrkerStores are the object stores every real Orker of a stack shares,
// as the Orkers of one deployment do: artifacts, commits, logs and attempt
// records in the stack's MinIO bucket.
type realOrkerStores struct {
	store    artifacts.Store
	commits  artifacts.CommitStore
	logs     artifacts.LogStore
	refs     artifacts.RefPredictor
	attempts artifacts.AttemptStore
}

func (s *chaosStack) realStores() *realOrkerStores {
	s.t.Helper()
	requireDockerForCommitE2E(s.t)
	store, err := artifacts.New(s.cfg)
	if err != nil {
		s.t.Fatalf("open artifact store: %v", err)
	}
	commits, err := artifacts.NewCommitStore(s.ctx, s.cfg)
	if err != nil {
		s.t.Fatalf("open commit store: %v", err)
	}
	if err := artifacts.ProbeCommitStore(s.ctx, commits); err != nil {
		infraAbsent(s.t, "the object store's conditional create is not usable: %v", err)
	}
	logs, err := artifacts.NewLogStore(s.cfg)
	if err != nil {
		s.t.Fatalf("open log store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		s.t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	attempts, err := artifacts.NewAttemptStore(s.cfg)
	if err != nil {
		s.t.Fatalf("open attempt store: %v", err)
	}
	return &realOrkerStores{store: store, commits: commits, logs: logs, refs: refs, attempts: attempts}
}

// startRealOrker starts a real Orker named name — real Docker runtime, real
// commit and attempt stores, its own work root and identity — on queue, over
// c (the control client when nil; a client through a faultproxy is how a
// scenario makes the process unreachable, as a killed host is). wrap, when
// set, sits in front of its ExecuteTask.
func (s *chaosStack) startRealOrker(name, pool, queue string, stores *realOrkerStores, c client.Client,
	wrap func(next orkerCall) orkerCall,
) (*chaosOrker, string) {
	s.t.Helper()
	call, orkerID := s.realOrkerCall(name, pool, stores, wrap)
	if c == nil {
		c = s.control
	}
	o := s.startOrkerWith(name, queue, c, func(ctx context.Context, _ *chaosStack, _ string,
		in *orkstackv1.ExecuteTaskInput,
	) (*orkstackv1.ExecuteTaskResult, error) {
		return call(ctx, in)
	})
	return o, orkerID
}

// realOrkerCall boots a real Orker's activities — real Docker runtime, real
// commit and attempt stores, its own work root and identity — and returns its
// ExecuteTask (behind wrap, when set) and its Orker id.
func (s *chaosStack) realOrkerCall(name, pool string, stores *realOrkerStores, wrap func(next orkerCall) orkerCall) (orkerCall, string) {
	s.t.Helper()
	orkerID := proof.OrkerID("chaos-" + chaosLabel(s.scenario) + "-" + name)
	activities := orker.NewActivitiesWithOptions(stores.store, orker.Options{
		Logs: stores.logs, Commits: stores.commits, Attempts: stores.attempts, Refs: stores.refs,
		Pool: pool, OrkerID: orkerID, WorkRoot: chaosWorkRoot(s.t, name),
	})
	if err := activities.Boot(s.ctx); err != nil {
		infraAbsent(s.t, "orker boot checks failed: %v", err)
	}
	s.t.Cleanup(activities.Close)
	call := orkerCall(activities.ExecuteTask)
	if wrap != nil {
		call = wrap(call)
	}
	return call, orkerID
}

// chaosWorkRoot is a fresh work root for one Orker of this test, under
// ORK_ORKER_WORKDIR or the repository's .data/work, which the daemon can
// bind-mount (AGENTS.md §11.1: never the parent itself).
func chaosWorkRoot(t *testing.T, name string) string {
	t.Helper()
	parent := os.Getenv(orker.WorkRootVar)
	if parent == "" {
		abs, err := filepath.Abs(filepath.Join("..", "..", ".data", "work"))
		if err != nil {
			t.Fatalf("resolve work root: %v", err)
		}
		parent = abs
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatalf("create work root parent %s: %v", parent, err)
	}
	root, err := os.MkdirTemp(parent, proof.WorkRootPattern("chaos-"+name))
	if err != nil {
		t.Fatalf("create work root under %s: %v", parent, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// ── the API ─────────────────────────────────────────────────────────────────

// chaosAPI is the production Connect API and the await callback door over
// the stack: its starter, its control reader and its archive.
type chaosAPI struct {
	svc     *api.Service
	baseURL string
	http    *http.Client
	runs    orkstackv1connect.RunServiceClient
}

// tcAPIConfig is the API configured with the table TC's settings for the
// C1–C12 rows: a 1,000-event history limit, continuation enabled with a
// 120-event, 1 MiB budget and a 5-minute restore timeout.
func tcAPIConfig() api.Config {
	return api.Config{
		HistoryMaxEvents: 1000, ContinuationEnabled: true, ContinuationEvents: 120,
		ContinuationBytes: 1 << 20, ContinuationRestoreTimeout: 5 * time.Minute,
	}
}

// serveAPI serves the API, configured as cfg over this stack's starter,
// reader, archive and stores.
func (s *chaosStack) serveAPI(cfg api.Config) *chaosAPI {
	s.t.Helper()
	store, err := artifacts.New(s.cfg)
	if err != nil {
		s.t.Fatalf("open artifact store: %v", err)
	}
	if cfg.Orkflows == nil {
		cfg.Orkflows = engine.NewMemoryOrkflowStore()
	}
	if cfg.Reader == nil {
		cfg.Reader = s.reader
	}
	cfg.Starter, cfg.RawQueue = s.starter, s.orkerQueue
	cfg.Runs, cfg.Bus, cfg.Archive = s.runs, s.bus, s.archive
	cfg.Artifacts, cfg.ArtifactStore, cfg.Limits = s.cfg, store, engine.DefaultLimits()
	if cfg.Attempts == nil {
		if cfg.Attempts, err = artifacts.NewAttemptStore(s.cfg); err != nil {
			s.t.Fatalf("open attempt store: %v", err)
		}
	}
	if cfg.Commits == nil {
		if cfg.Commits, err = artifacts.NewCommitStore(s.ctx, s.cfg); err != nil {
			s.t.Fatalf("open commit store: %v", err)
		}
	}
	if cfg.Logs == nil {
		if cfg.Logs, err = artifacts.NewLogStore(s.cfg); err != nil {
			s.t.Fatalf("open log store: %v", err)
		}
	}
	svc := api.NewService(cfg)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	transport.Protocols.SetHTTP1(true)
	s.t.Cleanup(func() {
		transport.CloseIdleConnections()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	})
	a := &chaosAPI{svc: svc, baseURL: "http://" + listener.Addr().String(), http: &http.Client{Transport: transport}}
	a.runs = orkstackv1connect.NewRunServiceClient(a.http, a.baseURL)
	s.transcript.step("API serving at %s", a.baseURL)
	return a
}

// apply applies yaml through the API.
func (a *chaosAPI) apply(t *testing.T, ctx context.Context, yaml string) {
	t.Helper()
	if _, err := a.svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
}

// submit submits name through the API.
func (a *chaosAPI) submit(t *testing.T, ctx context.Context, name string) *orkstackv1.SubmitRunResponse {
	t.Helper()
	resp, err := a.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
	if err != nil {
		t.Fatalf("SubmitRun %s: %v", name, err)
	}
	return resp.Msg
}

// post sends body to the await callback door at path with token.
func (a *chaosAPI) post(t *testing.T, ctx context.Context, path, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

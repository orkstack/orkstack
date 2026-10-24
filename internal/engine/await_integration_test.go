//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// awaitStack is a real engine on a real cluster and bucket: the run worker,
// the projection worker, a stand-in Orker activity worker, and the Connect
// API with the callback door, all in-process, in a private Temporal
// namespace (AGENTS.md §11.1).
type awaitStack struct {
	t          *testing.T
	ctx        context.Context
	client     client.Client
	namespace  string
	runQueue   string
	orkerQueue string
	snapshots  *testSnapshotStore
	store      artifacts.Store
	starter    *engine.TemporalStarter
	reader     *engine.TemporalRunReader
	svc        *api.Service
	baseURL    string
	http       *http.Client
	runWorker  worker.Worker

	mu     sync.Mutex
	inputs map[string]*orkstackv1.ExecuteTaskInput
}

type awaitStackOptions struct {
	// noOrker starts no Orker activity worker at all.
	noOrker bool
}

func newAwaitStack(t *testing.T, opts awaitStackOptions) *awaitStack {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	t.Cleanup(temporalClient.Close)
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)

	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	s := &awaitStack{
		t: t, ctx: ctx, client: temporalClient, namespace: namespace,
		runQueue:   "ork-await-run-" + token,
		orkerQueue: "ork-await-tasks-" + token,
		snapshots:  newTestSnapshotStore(),
		store:      store,
		inputs:     map[string]*orkstackv1.ExecuteTaskInput{},
	}
	s.startRunWorker()

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(s.snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := eventWorker.Start(); err != nil {
		t.Fatalf("start event worker: %v", err)
	}
	t.Cleanup(eventWorker.Stop)

	if !opts.noOrker {
		orkerWorker := worker.New(temporalClient, s.orkerQueue, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
		})
		orkerWorker.RegisterActivityWithOptions(
			func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				s.mu.Lock()
				s.inputs[in.GetRunId()+"/"+in.GetTaskId()] = in
				s.mu.Unlock()
				return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
			},
			activity.RegisterOptions{Name: task.ActivityExecuteTask})
		if err := orkerWorker.Start(); err != nil {
			t.Fatalf("start orker worker: %v", err)
		}
		t.Cleanup(orkerWorker.Stop)
	}

	s.starter = engine.NewTemporalStarterWithOrkerQueue(temporalClient, s.runQueue, s.orkerQueue)
	s.reader = engine.NewTemporalRunReader(temporalClient).WithSnapshotStore(s.snapshots)
	s.svc = api.NewService(api.Config{
		Orkflows:      engine.NewMemoryOrkflowStore(),
		Starter:       s.starter,
		Reader:        s.reader,
		RawQueue:      s.orkerQueue,
		Artifacts:     cfg,
		ArtifactStore: store,
		Limits:        engine.DefaultLimits(),
	})
	s.serve()
	return s
}

func (s *awaitStack) startRunWorker() {
	w := worker.New(s.client, s.runQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(w)
	w.RegisterActivityWithOptions(engine.NewSnapshotActivities(s.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	if err := w.Start(); err != nil {
		s.t.Fatalf("start run worker: %v", err)
	}
	s.runWorker = w
	s.t.Cleanup(func() {
		if s.runWorker != nil {
			s.runWorker.Stop()
			s.runWorker = nil
		}
	})
}

func (s *awaitStack) stopRunWorker() {
	s.runWorker.Stop()
	s.runWorker = nil
}

func (s *awaitStack) serve() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(s.svc, api.MuxOptions{}))
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
	s.baseURL = "http://" + listener.Addr().String()
	s.http = &http.Client{Transport: transport}
}

func (s *awaitStack) runs() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(s.http, s.baseURL)
}

func (s *awaitStack) apply(yaml string) {
	s.t.Helper()
	if _, err := s.svc.ApplyOrkflow(s.ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		s.t.Fatalf("ApplyOrkflow: %v", err)
	}
}

func (s *awaitStack) submit(name string) *orkstackv1.SubmitRunResponse {
	s.t.Helper()
	resp, err := s.runs().SubmitRun(s.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
	if err != nil {
		s.t.Fatalf("SubmitRun: %v", err)
	}
	s.t.Cleanup(func() {
		_ = s.client.TerminateWorkflow(context.Background(), resp.Msg.GetRun().GetId(), "", "await integration cleanup")
	})
	return resp.Msg
}

// waitTask polls the record until taskID reaches want, against a deadline.
func (s *awaitStack) waitTask(runID, taskID string, want orkstackv1.Status) *orkstackv1.Run {
	s.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	var last *orkstackv1.Run
	for {
		run, err := s.reader.GetRun(s.ctx, runID)
		if err == nil {
			last = run
			for _, tr := range run.GetTaskRuns() {
				if tr.GetTaskId() == taskID && tr.GetStatus() == want {
					return run
				}
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s task %s never reached %v (last %v, err %v)", runID, taskID, want, last, err)
		}
		<-tick.C
	}
}

// waitRun waits for the run's execution to close and returns its record.
func (s *awaitStack) waitRun(runID string) *orkstackv1.Run {
	s.t.Helper()
	_ = s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil)
	run, err := s.reader.GetRun(s.ctx, runID)
	if err != nil {
		s.t.Fatalf("read run %s: %v", runID, err)
	}
	return run
}

func (s *awaitStack) post(path, token, body string) (int, []byte) {
	s.t.Helper()
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, s.baseURL+path, strings.NewReader(body))
	if err != nil {
		s.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		s.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func taskRunNamed(run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	return nil
}

func eventsOfRun(t *testing.T, s *awaitStack, runID string) []*orkstackv1.Event {
	t.Helper()
	events, err := s.reader.Events(s.ctx, runID, 0)
	if err != nil {
		t.Fatalf("read events of %s: %v", runID, err)
	}
	return events
}

func countEvents(events []*orkstackv1.Event, taskID string, kind orkstackv1.EventType) int {
	n := 0
	for _, e := range events {
		if e.GetTaskId() == taskID && e.GetType() == kind {
			n++
		}
	}
	return n
}

func writeAwaitProof(t *testing.T, name, body string) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err == nil {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644)
	}
}

const awaitIntegrationYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: await-integration
version: "1.0.0"
tasks:
  - id: submit-training
    uses: shell
    with: {op: exec, command: ["echo", "submit"]}
  - id: training
    needs: [submit-training]
    await:
      timeout_seconds: 3600
      callback: true
      schema:
        model_uri: {type: string, required: true}
  - id: evaluate
    uses: shell
    needs: [training]
    with: {op: exec, command: ["echo", "{{ .tasks.training.output.model_uri }}"]}
`

// The callback door, with the token SubmitRun returned; the downstream
// activity's input carries the ref; the object in MinIO is byte-identical.
func TestIntegration_AwaitResolvedFeedsItsValuesDownstream(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{})
	s.apply(awaitIntegrationYAML)
	submitted := s.submit("await-integration")
	runID := submitted.GetRun().GetId()
	if len(submitted.GetAwaitCallbacks()) != 1 {
		t.Fatalf("callbacks = %v", submitted.GetAwaitCallbacks())
	}
	callback := submitted.GetAwaitCallbacks()[0]
	s.waitTask(runID, "training", orkstackv1.Status_STATUS_AWAITING)

	body := "{\"model_uri\": \"s3://models/88421\",  \"auc\": 0.91, \"n\": 12345678901234567890}\n"
	status, resp := s.post(callback.GetPath(), callback.GetToken(), body)
	if status != http.StatusAccepted {
		t.Fatalf("callback door answered %d: %s", status, resp)
	}
	run := s.waitRun(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	training := taskRunNamed(run, "training")
	if training.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK {
		t.Fatalf("training await = %v", training.GetAwait())
	}
	s.mu.Lock()
	in := s.inputs[runID+"/evaluate"]
	s.mu.Unlock()
	var ref *orkstackv1.UpstreamRef
	for _, up := range in.GetUpstream() {
		if up.GetTaskId() == "training" {
			ref = up
		}
	}
	if ref == nil || ref.GetValues().GetUri() != training.GetValuesRef().GetUri() {
		t.Fatalf("evaluate's upstream for training = %v, want %v", ref, training.GetValuesRef())
	}
	reader, err := s.store.Get(s.ctx, training.GetValuesRef().GetUri())
	if err != nil {
		t.Fatalf("read the stored values: %v", err)
	}
	stored, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Equal(stored, []byte(body)) {
		t.Fatalf("stored values %q, want the posted bytes %q", stored, body)
	}
	writeAwaitProof(t, "await-I1.txt", fmt.Sprintf("run %s: posted %d bytes, stored %s (sha256 %s), evaluate upstream ref %s\n",
		runID, len(body), training.GetValuesRef().GetUri(), training.GetValuesRef().GetSha256(), ref.GetValues().GetUri()))
}

// A deadline, two seconds.
func TestIntegration_AwaitTimesOutAndFailsTheRunExplicitly(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	wf := &orkstackv1.Orkflow{Name: "await-timeout", Version: "1.0.0", Tasks: []*orkstackv1.Task{
		{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 2}},
	}}
	runID := engine.NewRunID()
	if err := s.starter.StartRun(s.ctx, runID, wf, nil, engine.RunPlan{Namespace: "default", Routes: map[string]string{}}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	run := s.waitRun(runID)
	gate := taskRunNamed(run, "gate")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || gate.GetError().GetCode() != engine.AwaitTimedOutCode ||
		gate.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT {
		t.Fatalf("run %v, gate %v", run.GetStatus(), gate.GetError())
	}
	writeAwaitProof(t, "await-I2.txt", fmt.Sprintf("run %s: %s %s\n", runID, gate.GetError().GetCode(), gate.GetError().GetMessage()))
}

// A rejection over the Connect RPC.
func TestIntegration_AwaitRejectionFailsTheRunWithItsReason(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{})
	s.apply(awaitIntegrationYAML)
	runID := s.submit("await-integration").GetRun().GetId()
	s.waitTask(runID, "training", orkstackv1.Status_STATUS_AWAITING)
	req := connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
		RunId: runID, TaskId: "training", Reject: true, Reason: "databricks job cancelled",
	})
	req.Header().Set(api.AuditPrincipalHeader, "alice")
	if _, err := s.runs().ResolveAwait(s.ctx, req); err != nil {
		t.Fatalf("ResolveAwait: %v", err)
	}
	run := s.waitRun(runID)
	training := taskRunNamed(run, "training")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || training.GetError().GetCode() != engine.AwaitRejectedCode ||
		!strings.Contains(training.GetError().GetMessage(), "databricks job cancelled") ||
		training.GetAwait().GetResolvedBy() != "alice" {
		t.Fatalf("run %v, training %v", run.GetStatus(), training)
	}
}

// The run is cancelled while it awaits.
func TestIntegration_RunCanceledWhileAwaitingEndsCanceled(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	wf := &orkstackv1.Orkflow{Name: "await-cancel", Version: "1.0.0", Tasks: []*orkstackv1.Task{
		{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 3600}},
	}}
	runID := engine.NewRunID()
	if err := s.starter.StartRun(s.ctx, runID, wf, nil, engine.RunPlan{Namespace: "default", Routes: map[string]string{}}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	s.waitTask(runID, "gate", orkstackv1.Status_STATUS_AWAITING)
	if err := s.starter.CancelRun(s.ctx, runID, "operator"); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	run := s.waitRun(runID)
	gate := taskRunNamed(run, "gate")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || gate.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		gate.GetFinishedAt() == nil {
		t.Fatalf("run %v, gate %v", run.GetStatus(), gate)
	}
}

// AGENTS.md §15 "an accepted execution cannot disappear" — the run worker is gone
// while the resolution is delivered, and a fresh worker applies it.
func TestIntegration_ResolutionDeliveredWithNoEngineWorkerIsAppliedAfterRestart(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{})
	s.apply(awaitIntegrationYAML)
	submitted := s.submit("await-integration")
	runID := submitted.GetRun().GetId()
	s.waitTask(runID, "training", orkstackv1.Status_STATUS_AWAITING)

	s.stopRunWorker()
	callback := submitted.GetAwaitCallbacks()[0]
	status, body := s.post(callback.GetPath(), callback.GetToken(), `{"model_uri":"s3://after-restart"}`)
	if status != http.StatusAccepted {
		t.Fatalf("with no workflow worker polling, the door answered %d: %s", status, body)
	}
	s.startRunWorker()
	run := s.waitRun(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	writeAwaitProof(t, "await-I5.txt", fmt.Sprintf("run %s: delivered with no run worker (202), applied after restart: %v\n",
		runID, run.GetStatus()))
}

// Two different completions at once — exactly one applies.
func TestIntegration_ConcurrentResolutionsApplyExactlyOne(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{})
	s.apply(awaitIntegrationYAML)
	submitted := s.submit("await-integration")
	runID := submitted.GetRun().GetId()
	s.waitTask(runID, "training", orkstackv1.Status_STATUS_AWAITING)
	callback := submitted.GetAwaitCallbacks()[0]

	bodies := []string{`{"model_uri":"s3://one"}`, `{"model_uri":"s3://two"}`}
	statuses := make([]int, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func(i int, body string) {
			defer wg.Done()
			statuses[i], _ = s.post(callback.GetPath(), callback.GetToken(), body)
		}(i, body)
	}
	wg.Wait()
	run := s.waitRun(runID)
	events := eventsOfRun(t, s, runID)
	if n := countEvents(events, "training", orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED); n != 1 {
		t.Fatalf("%d TASK_RESOLVED events, want exactly one", n)
	}
	ignored := 0
	for _, e := range events {
		if e.GetTaskId() == "training" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG &&
			strings.HasPrefix(e.GetMessage(), "resolution ignored") {
			ignored++
		}
	}
	conflicts := 0
	for _, status := range statuses {
		if status == http.StatusConflict {
			conflicts++
		}
	}
	if conflicts+ignored != 1 {
		t.Fatalf("statuses %v, ignored %d: the losing resolution must be a 409 or a recorded ignore, exactly once",
			statuses, ignored)
	}
	got := taskRunNamed(run, "training").GetOutput().GetData().GetFields()["model_uri"].GetStringValue()
	if got != "s3://one" && got != "s3://two" {
		t.Fatalf("training output %q is neither body", got)
	}
}

// No Orker polls anything; an await-only orkflow still submits and runs.
func TestIntegration_AwaitOnlyOrkflowRunsWithNoOrker(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	s.apply(`
apiVersion: orkstack/v1
kind: Orkflow
name: await-only-integration
version: "1.0.0"
tasks:
  - id: gate
    await: {timeout_seconds: 600}
`)
	runID := s.submit("await-only-integration").GetRun().GetId()
	s.waitTask(runID, "gate", orkstackv1.Status_STATUS_AWAITING)
	if _, err := s.runs().ResolveAwait(s.ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
		RunId: runID, TaskId: "gate", ValuesJson: `{"ok":true}`,
	})); err != nil {
		t.Fatalf("ResolveAwait: %v", err)
	}
	if run := s.waitRun(runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
}

const (
	awaitedResolvedFixture = "testdata/history/run-awaited-resolved-v1.json"
	awaitedTimeoutFixture  = "testdata/history/run-awaited-timeout-v1.json"
)

// Records the two awaited replay fixtures from REAL runs on a versioned
// worker. Opt-in, exactly as TestIntegration_RecordGoldenRunHistory: the
// fixtures are recorded once and then age.
func TestIntegration_RecordAwaitedRunHistories(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + awaitedResolvedFixture + " and " +
			awaitedTimeoutFixture + " — the committed fixtures are meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-awaited-run-" + suffix
	orkerQueue := "ork-awaited-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-awaited-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	orkerWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId, "upstream_count": float64(len(in.Upstream))}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	reader := engine.NewTemporalRunReader(temporalClient).WithSnapshotStore(snapshots)

	record := func(path, runID string) {
		recorded := &history.History{}
		iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				t.Fatalf("read history: %v", err)
			}
			recorded.Events = append(recorded.Events, event)
		}
		encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
		if err != nil {
			t.Fatalf("encode history: %v", err)
		}
		if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
			len(recorded.Events), runID, path, deployment)
	}

	// The resolved run: a → await b (callback, schema) → c, completed with a
	// small inline body.
	resolvedRunID := fmt.Sprintf("ork-awaited-resolved-%d", time.Now().UnixNano())
	if err := starter.StartRun(ctx, resolvedRunID, awaitedFixtureOrkflow(3600), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		run, err := reader.GetRun(ctx, resolvedRunID)
		if err == nil && taskRunNamed(run, "b").GetStatus() == orkstackv1.Status_STATUS_AWAITING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the resolved fixture's await never opened: %v", err)
		}
		<-tick.C
	}
	body := []byte(`{"model_uri":"s3://models/fixture"}`)
	if err := starter.SignalAwait(ctx, resolvedRunID, &orkstackv1.AwaitSignal{
		TaskId: "b",
		Values: &orkstackv1.ArtifactRef{
			Name: "values.json", Uri: "file:///tmp/" + resolvedRunID + "/b/values.json", SizeBytes: int64(len(body)),
		},
		ValuesJson: body,
		Via:        orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		Principal:  "fixture-recorder",
	}); err != nil {
		t.Fatalf("SignalAwait: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, resolvedRunID, "").Get(ctx, nil); err != nil {
		t.Fatalf("resolved run %s did not succeed: %v", resolvedRunID, err)
	}
	record(awaitedResolvedFixture, resolvedRunID)

	// The timeout run: the same shape, a two-second deadline, never resolved.
	timeoutRunID := fmt.Sprintf("ork-awaited-timeout-%d", time.Now().UnixNano())
	if err := starter.StartRun(ctx, timeoutRunID, awaitedFixtureOrkflow(2), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	err := temporalClient.GetWorkflow(ctx, timeoutRunID, "").Get(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), engine.AwaitTimedOutCode) {
		t.Fatalf("timeout run %s ended with %v, want %s", timeoutRunID, err, engine.AwaitTimedOutCode)
	}
	record(awaitedTimeoutFixture, timeoutRunID)
}

func awaitedFixtureOrkflow(timeoutSeconds int32) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "awaited-replay",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "a", Uses: "shell"},
			{Id: "b", Needs: []string{"a"}, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: timeoutSeconds, Callback: true}},
			{Id: "c", Uses: "shell", Needs: []string{"b"}},
		},
	}
}

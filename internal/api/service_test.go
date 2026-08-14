package api_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// fakeStarter records started runs instead of talking to Temporal.
type fakeStarter struct {
	mu       sync.Mutex
	started  []string
	inputs   []map[string]any
	canceled []string
	err      error
}

func (f *fakeStarter) StartRun(_ context.Context, runID string, _ *orkstackv1.Workflow, input *orkstackv1.Payload) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.started = append(f.started, runID)
	f.inputs = append(f.inputs, input.GetData().AsMap())
	return nil
}

func (f *fakeStarter) CancelRun(_ context.Context, runID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, runID)
	return nil
}

func (f *fakeStarter) startedRuns() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.started...)
}

func (f *fakeStarter) startedInputs() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.inputs...)
}

type fixture struct {
	svc     *api.Service
	bus     *engine.EventBus
	runs    *engine.RunStore
	starter *fakeStarter
	baseURL string
	client  *http.Client
}

// newFixture starts the real h2c server on a loopback port and returns
// connect-go clients wired to it over unencrypted HTTP/2.
func newFixture(t *testing.T) *fixture {
	t.Helper()

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	starter := &fakeStarter{}
	var counter int
	svc := api.NewService(api.Config{
		Workflows: engine.NewWorkflowStore(),
		Runs:      runs,
		Bus:       bus,
		Starter:   starter,
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

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)

	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	return &fixture{
		svc:     svc,
		bus:     bus,
		runs:    runs,
		starter: starter,
		baseURL: "http://" + listener.Addr().String(),
		client:  &http.Client{Transport: transport},
	}
}

func (f *fixture) workflowClient() orkstackv1connect.WorkflowServiceClient {
	return orkstackv1connect.NewWorkflowServiceClient(f.client, f.baseURL)
}

func (f *fixture) runClient() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(f.client, f.baseURL)
}

func (f *fixture) pluginClient() orkstackv1connect.PluginServiceClient {
	return orkstackv1connect.NewPluginServiceClient(f.client, f.baseURL)
}

func helloYAML(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	return string(source)
}

func TestApplyThenGetWorkflowRoundTrips(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	client := f.workflowClient()

	applied, err := client.Apply(ctx, connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	wf := applied.Msg.GetWorkflow()
	if wf.GetName() != "hello-ork" || wf.GetVersion() != "0.1.0" {
		t.Fatalf("applied workflow = %s@%s, want hello-ork@0.1.0", wf.GetName(), wf.GetVersion())
	}
	if len(wf.GetTasks()) != 3 {
		t.Fatalf("applied workflow has %d tasks, want 3", len(wf.GetTasks()))
	}

	// Exact version.
	got, err := client.GetWorkflow(ctx, connect.NewRequest(&orkstackv1.GetWorkflowRequest{
		Name: "hello-ork", Version: "0.1.0",
	}))
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.Msg.GetWorkflow().GetSourceYaml() != wf.GetSourceYaml() {
		t.Error("round-tripped source_yaml differs from the applied document")
	}
	if len(got.Msg.GetWorkflow().GetTasks()) != 3 {
		t.Errorf("round-tripped workflow has %d tasks, want 3", len(got.Msg.GetWorkflow().GetTasks()))
	}
	if got.Msg.GetWorkflow().GetTasks()[0].GetWith().AsMap()["url"] != "{{ .inputs.url }}" {
		t.Error("round-tripped task `with` lost its raw template")
	}

	// Empty version means latest.
	latest, err := client.GetWorkflow(ctx, connect.NewRequest(&orkstackv1.GetWorkflowRequest{Name: "hello-ork"}))
	if err != nil {
		t.Fatalf("GetWorkflow(latest): %v", err)
	}
	if latest.Msg.GetWorkflow().GetVersion() != "0.1.0" {
		t.Errorf("latest version = %q, want 0.1.0", latest.Msg.GetWorkflow().GetVersion())
	}

	listed, err := client.ListWorkflows(ctx, connect.NewRequest(&orkstackv1.ListWorkflowsRequest{}))
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(listed.Msg.GetWorkflows()) != 1 {
		t.Errorf("ListWorkflows returned %d workflows, want 1", len(listed.Msg.GetWorkflows()))
	}
}

func TestApplyRejectsInvalidDocument(t *testing.T) {
	f := newFixture(t)
	_, err := f.workflowClient().Apply(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: "name: Bad_Name\ntasks: []\n"}))
	if err == nil {
		t.Fatal("Apply(invalid) = nil error, want InvalidArgument")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("Apply(invalid) code = %v, want InvalidArgument", connect.CodeOf(err))
	}
}

func TestGetWorkflowUnknownReturnsNotFound(t *testing.T) {
	f := newFixture(t)
	_, err := f.workflowClient().GetWorkflow(context.Background(),
		connect.NewRequest(&orkstackv1.GetWorkflowRequest{Name: "ghost"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetWorkflow(ghost) code = %v, want NotFound", connect.CodeOf(err))
	}
}

func TestSubmitRunStartsRunAndGetRunReturnsIt(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	submitted, err := f.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	run := submitted.Msg.GetRun()
	if run.GetId() != "run-001" {
		t.Errorf("run id = %q, want run-001", run.GetId())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Errorf("submitted run status = %v, want PENDING", run.GetStatus())
	}
	if run.GetWorkflowName() != "hello-ork" || run.GetWorkflowVersion() != "0.1.0" {
		t.Errorf("run pins %s@%s, want hello-ork@0.1.0", run.GetWorkflowName(), run.GetWorkflowVersion())
	}
	if started := f.starter.startedRuns(); len(started) != 1 || started[0] != "run-001" {
		t.Errorf("started runs = %v, want [run-001]", started)
	}

	got, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "run-001"}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Msg.GetRun().GetId() != "run-001" {
		t.Errorf("GetRun id = %q, want run-001", got.Msg.GetRun().GetId())
	}

	listed, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 1 {
		t.Errorf("ListRuns returned %d runs, want 1", len(listed.Msg.GetRuns()))
	}
}

func TestSubmitRunUnknownWorkflowReturnsNotFound(t *testing.T) {
	f := newFixture(t)
	_, err := f.runClient().SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{WorkflowName: "ghost"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("SubmitRun(ghost) code = %v, want NotFound", connect.CodeOf(err))
	}
}

func TestSubmitRunReportsStarterFailure(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	f.starter.mu.Lock()
	f.starter.err = errors.New("temporal is down")
	f.starter.mu.Unlock()

	_, err := f.runClient().SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{WorkflowName: "hello-ork"}))
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("SubmitRun code = %v, want Internal", connect.CodeOf(err))
	}
	run, ok := f.runs.Get("run-001")
	if !ok {
		t.Fatal("run was not recorded")
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED after a failed start", run.GetStatus())
	}
}

func TestSubmitRunIsIdempotentPerKey(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	req := func() *connect.Request[orkstackv1.SubmitRunRequest] {
		return connect.NewRequest(&orkstackv1.SubmitRunRequest{
			WorkflowName:   "hello-ork",
			IdempotencyKey: "abc",
		})
	}
	first, err := f.runClient().SubmitRun(ctx, req())
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	second, err := f.runClient().SubmitRun(ctx, req())
	if err != nil {
		t.Fatalf("SubmitRun (repeat): %v", err)
	}
	if first.Msg.GetRun().GetId() != second.Msg.GetRun().GetId() {
		t.Errorf("idempotency key produced two runs: %q and %q",
			first.Msg.GetRun().GetId(), second.Msg.GetRun().GetId())
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("started runs = %v, want exactly one", started)
	}
}

func TestCancelRunForwardsToStarter(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := f.runClient().SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{WorkflowName: "hello-ork"})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	if _, err := f.runClient().CancelRun(ctx,
		connect.NewRequest(&orkstackv1.CancelRunRequest{RunId: "run-001", Reason: "stop"})); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.canceled) != 1 || f.starter.canceled[0] != "run-001" {
		t.Errorf("canceled runs = %v, want [run-001]", f.starter.canceled)
	}
}

// seedRun registers a run and publishes a lifecycle sequence to the bus.
func (f *fixture) seedRun(t *testing.T, runID string) []*orkstackv1.Event {
	t.Helper()
	f.runs.Save(&orkstackv1.Run{
		Id:           runID,
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_RUNNING,
	})
	published := []*orkstackv1.Event{}
	stages := []struct {
		eventType orkstackv1.EventType
		taskID    string
		status    orkstackv1.Status
		message   string
	}{
		{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, "", orkstackv1.Status_STATUS_RUNNING, "run started"},
		{orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, "fetch", orkstackv1.Status_STATUS_RUNNING, "scheduled"},
		{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, "fetch", orkstackv1.Status_STATUS_SUCCEEDED, "done"},
		{orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "", orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded"},
	}
	for i, stage := range stages {
		event, _ := f.bus.Publish(&orkstackv1.Event{
			RunId:     runID,
			Type:      stage.eventType,
			TaskId:    stage.taskID,
			Status:    stage.status,
			Message:   stage.message,
			Timestamp: timestamppb.New(time.Unix(int64(1700000000+i), 0).UTC()),
		}, fmt.Sprintf("%s#%d", runID, i))
		published = append(published, event)
	}
	return published
}

func TestStreamEventsDeliversOrderedEvents(t *testing.T) {
	f := newFixture(t)
	want := f.seedRun(t, "run-stream")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: "run-stream",
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var got []*orkstackv1.Event
	for stream.Receive() {
		got = append(got, stream.Msg())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("received %d events, want %d", len(got), len(want))
	}
	var previous int64
	for i, event := range got {
		if event.GetSequence() != want[i].GetSequence() {
			t.Errorf("event[%d] sequence = %d, want %d", i, event.GetSequence(), want[i].GetSequence())
		}
		if event.GetType() != want[i].GetType() {
			t.Errorf("event[%d] type = %v, want %v", i, event.GetType(), want[i].GetType())
		}
		if event.GetSequence() <= previous {
			t.Fatalf("event sequences are not increasing: %d after %d", event.GetSequence(), previous)
		}
		previous = event.GetSequence()
	}
	if got[len(got)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Errorf("last event = %v, want RUN_SUCCEEDED", got[len(got)-1].GetType())
	}
}

func TestStreamEventsResumesFromSequence(t *testing.T) {
	f := newFixture(t)
	f.seedRun(t, "run-resume")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId:        "run-resume",
		FromSequence: 2,
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var sequences []int64
	for stream.Receive() {
		sequences = append(sequences, stream.Msg().GetSequence())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	if fmt.Sprint(sequences) != fmt.Sprint([]int64{3, 4}) {
		t.Errorf("resumed sequences = %v, want [3 4] (from_sequence is the last seen cursor)", sequences)
	}
}

func TestStreamEventsUnknownRunReturnsNotFound(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := f.runClient().StreamEvents(ctx,
		connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: "ghost"}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatalf("StreamEvents(ghost) code = %v, want NotFound", connect.CodeOf(err))
		}
		return
	}
	defer stream.Close()
	for stream.Receive() {
	}
	if connect.CodeOf(stream.Err()) != connect.CodeNotFound {
		t.Fatalf("StreamEvents(ghost) code = %v, want NotFound", connect.CodeOf(stream.Err()))
	}
}

func TestStreamEventsDeliversLiveEvents(t *testing.T) {
	f := newFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "run-live", Status: orkstackv1.Status_STATUS_RUNNING})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// One event before the call so the stream's response headers flush; the
	// rest are published afterwards and must arrive live.
	f.bus.Publish(&orkstackv1.Event{
		RunId:   "run-live",
		Type:    orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		Message: "line 0",
	}, "live#start")

	stream, err := f.runClient().StreamEvents(ctx,
		connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: "run-live"}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	if !stream.Receive() {
		t.Fatalf("first receive failed: %v", stream.Err())
	}
	messages := []string{stream.Msg().GetMessage()}

	for i := 1; i < 3; i++ {
		f.bus.Publish(&orkstackv1.Event{
			RunId:   "run-live",
			Type:    orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
			Message: fmt.Sprintf("line %d", i),
		}, fmt.Sprintf("live#%d", i))
		if !stream.Receive() {
			t.Fatalf("live receive %d failed: %v", i, stream.Err())
		}
		messages = append(messages, stream.Msg().GetMessage())
	}

	f.bus.Publish(&orkstackv1.Event{
		RunId: "run-live",
		Type:  orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
	}, "live#done")

	for stream.Receive() {
		messages = append(messages, stream.Msg().GetMessage())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	if len(messages) != 4 {
		t.Fatalf("received %d events, want 4: %v", len(messages), messages)
	}
	if messages[0] != "line 0" || messages[1] != "line 1" || messages[2] != "line 2" {
		t.Errorf("live events out of order: %v", messages)
	}
}

func TestWebSocketBridgeDeliversSameEventsAsStreamEvents(t *testing.T) {
	f := newFixture(t)
	want := f.seedRun(t, "run-ws")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(f.baseURL, "http") + "/ws/runs/run-ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.CloseNow()

	var got []*orkstackv1.Event
	for range want {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("websocket read: %v", err)
		}
		if typ != websocket.MessageText {
			t.Fatalf("frame type = %v, want text", typ)
		}
		var event orkstackv1.Event
		if err := protojson.Unmarshal(data, &event); err != nil {
			t.Fatalf("decode event %q: %v", string(data), err)
		}
		got = append(got, &event)
	}

	for i := range want {
		if got[i].GetSequence() != want[i].GetSequence() ||
			got[i].GetType() != want[i].GetType() ||
			got[i].GetTaskId() != want[i].GetTaskId() ||
			got[i].GetStatus() != want[i].GetStatus() {
			t.Errorf("ws event[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if got[len(got)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Errorf("last ws event = %v, want RUN_SUCCEEDED", got[len(got)-1].GetType())
	}
}

func TestWebSocketBridgeRejectsUnknownRun(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(f.baseURL, "http")+"/ws/runs/ghost", nil)
	if err == nil {
		t.Fatal("dial(ghost) succeeded, want rejection")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("dial(ghost) status = %v, want 404", resp)
	}
}

func TestListPluginsReturnsConfiguredSet(t *testing.T) {
	svc := api.NewService(api.Config{
		Plugins: []*orkstackv1.PluginInfo{
			{Name: "shell"},
			{Name: "file"},
		},
	})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	defer server.Close()

	client := orkstackv1connect.NewPluginServiceClient(server.Client(), server.URL)
	resp, err := client.ListPlugins(context.Background(), connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	names := []string{}
	for _, p := range resp.Msg.GetPlugins() {
		names = append(names, p.GetName())
	}
	if fmt.Sprint(names) != fmt.Sprint([]string{"file", "shell"}) {
		t.Errorf("plugins = %v, want [file shell] sorted by name", names)
	}
}

func TestListPluginsEmptyByDefault(t *testing.T) {
	f := newFixture(t)
	resp, err := f.pluginClient().ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	if len(resp.Msg.GetPlugins()) != 0 {
		t.Errorf("plugins = %v, want empty in M2", resp.Msg.GetPlugins())
	}
}

func TestServerSpeaksH2CToConnectClients(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply over h2c: %v", err)
	}

	// Assert the transport really negotiated HTTP/2 without TLS.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		f.baseURL+"/orkstack.v1.WorkflowService/ListWorkflows", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("ListWorkflows over h2c: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("negotiated HTTP/%d.%d, want HTTP/2 (h2c)", resp.ProtoMajor, resp.ProtoMinor)
	}
}

func TestSubmitRun_MergesDeclaredInputDefaultsBeforeSavingAndStarting(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.workflowClient().Apply(ctx,
		connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	omitted, err := f.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
	}))
	if err != nil {
		t.Fatalf("SubmitRun omitted input: %v", err)
	}
	wantDefault := "https://httpbin.org/json"
	if got := omitted.Msg.GetRun().GetInput().GetData().AsMap()["url"]; got != wantDefault {
		t.Errorf("stored run input url = %v, want default %q", got, wantDefault)
	}
	if got := f.starter.startedInputs()[0]["url"]; got != wantDefault {
		t.Errorf("starter input url = %v, want default %q", got, wantDefault)
	}

	providedData, err := structpb.NewStruct(map[string]any{"url": "https://caller.test"})
	if err != nil {
		t.Fatalf("provided input: %v", err)
	}
	provided, err := f.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		WorkflowName: "hello-ork",
		Input:        &orkstackv1.Payload{Data: providedData},
	}))
	if err != nil {
		t.Fatalf("SubmitRun provided input: %v", err)
	}
	if got := provided.Msg.GetRun().GetInput().GetData().AsMap()["url"]; got != "https://caller.test" {
		t.Errorf("caller-provided url = %v, want preserved value", got)
	}
}

func TestSubmitRun_ConcurrentIdempotencyKeyStartsExactlyOneRun(t *testing.T) {
	const requests = 32

	workflows := engine.NewWorkflowStore()
	runs := engine.NewRunStore()
	starter := &fakeStarter{}
	allocations := make(chan int, requests)
	releaseFirst := make(chan struct{})
	var allocationMu sync.Mutex
	allocationCount := 0
	svc := api.NewService(api.Config{
		Workflows: workflows,
		Runs:      runs,
		Bus:       engine.NewEventBus(),
		Starter:   starter,
		NewRunID: func() string {
			allocationMu.Lock()
			allocationCount++
			n := allocationCount
			allocationMu.Unlock()
			allocations <- n
			if n == 1 {
				<-releaseFirst
			}
			return fmt.Sprintf("concurrent-%d", n)
		},
	})
	if _, err := svc.Apply(context.Background(), connect.NewRequest(&orkstackv1.ApplyWorkflowRequest{
		Yaml: helloYAML(t),
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	start := make(chan struct{})
	results := make(chan *orkstackv1.Run, requests)
	errors := make(chan error, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			<-start
			response, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
				WorkflowName:   "hello-ork",
				IdempotencyKey: "shared-key",
			}))
			if err != nil {
				errors <- err
				return
			}
			results <- response.Msg.GetRun()
		}()
	}
	close(start)
	if got := <-allocations; got != 1 {
		t.Fatalf("first allocation = %d, want 1", got)
	}

	var racedAllocation int
	select {
	case racedAllocation = <-allocations:
		// Before the fix, another request reaches ID allocation while the first
		// request is blocked before it can reserve the key.
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseFirst)
	wg.Wait()
	close(results)
	close(errors)

	if racedAllocation != 0 {
		t.Fatalf("concurrent request allocated run %d before the key was reserved", racedAllocation)
	}
	for err := range errors {
		t.Errorf("SubmitRun: %v", err)
	}
	var runID string
	count := 0
	for run := range results {
		count++
		if runID == "" {
			runID = run.GetId()
		}
		if run.GetId() != runID {
			t.Errorf("received run id %q, want one shared id %q", run.GetId(), runID)
		}
	}
	if count != requests {
		t.Errorf("responses = %d, want %d", count, requests)
	}
	if started := starter.startedRuns(); len(started) != 1 {
		t.Errorf("starter calls = %v, want one", started)
	}
	if stored := runs.List(engine.ListFilter{}); len(stored) != 1 {
		t.Errorf("stored runs = %d, want one", len(stored))
	}
}

func TestStreamEvents_TerminalCursorEndsWithoutMessages(t *testing.T) {
	f := newFixture(t)
	f.seedRun(t, "run-terminal-cursor")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId:        "run-terminal-cursor",
		FromSequence: 4,
	}))
	if err != nil {
		t.Fatalf("StreamEvents at terminal cursor: %v", err)
	}
	defer stream.Close()
	var received int
	for stream.Receive() {
		received++
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error = %v, want clean completion", err)
	}
	if received != 0 {
		t.Errorf("received %d events, want zero after terminal cursor", received)
	}
}

func TestWebSocketBridge_TerminalCursorClosesNormally(t *testing.T) {
	f := newFixture(t)
	f.seedRun(t, "run-ws-terminal-cursor")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(f.baseURL, "http") +
		"/ws/runs/run-ws-terminal-cursor?from_sequence=4"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.CloseNow()
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("websocket read error = %v (status %v), want normal closure",
			err, websocket.CloseStatus(err))
	}
}

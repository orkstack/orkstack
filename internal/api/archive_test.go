package api_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// archiveHarness is one engine process: a run store and event bus that die with
// it, plus the archive that does not. Restart means building a second harness
// over the same archive.
type archiveHarness struct {
	svc        *api.Service
	runs       *engine.RunStore
	bus        *engine.EventBus
	activities *engine.EventActivities
	starter    *fakeStarter
}

func newArchiver(t *testing.T) *engine.RunArchiver {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return engine.NewRunArchiver(store)
}

// boot starts an engine process over an existing archive: fresh in-process
// state, same durable history. The run-id prefix stands in for the wall clock a
// real engine uses, so two processes never mint the same id.
func boot(t *testing.T, archive *engine.RunArchiver, prefix string, now func() time.Time) *archiveHarness {
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
		Archive:   archive,
		Now:       now,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("ork-%s-%d", prefix, counter)
		},
	})
	return &archiveHarness{
		svc:        svc,
		runs:       runs,
		bus:        bus,
		activities: engine.NewArchivingEventActivities(bus, runs, archive),
		starter:    starter,
	}
}

func (h *archiveHarness) apply(t *testing.T, yaml string) {
	t.Helper()
	if _, err := h.svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
}

// submit submits a run and plays the workflow's first act: RUN_CREATED, event
// 1, published by RunWorkflow through the projection activity on a real
// cluster. The API publishes nothing of its own on the happy path, so a
// harness that skipped this would be modelling an engine that does not exist.
func (h *archiveHarness) submit(t *testing.T, name string) string {
	t.Helper()
	resp, err := h.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{WorkflowName: name}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	run := resp.Msg.GetRun()
	if err := h.activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId:     run.GetId(),
		Sequence:  1,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:    orkstackv1.Status_STATUS_PENDING,
		Message:   "run created for orkflow " + run.GetWorkflowName(),
		Timestamp: run.GetCreatedAt(),
	}, run, run.GetId()+"#1"); err != nil {
		t.Fatalf("PublishEvent(created): %v", err)
	}
	return run.GetId()
}

// finish plays the engine's terminal projection: the same activity the real
// RunWorkflow calls, which is what writes the archive.
func (h *archiveHarness) finish(t *testing.T, runID string, status orkstackv1.Status, finishedAt time.Time) {
	t.Helper()
	eventType := orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	if status == orkstackv1.Status_STATUS_FAILED {
		eventType = orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	}
	stored, ok := h.runs.Get(runID)
	if !ok {
		t.Fatalf("run %q is not in the live store", runID)
	}
	snapshot := &orkstackv1.Run{
		Id:              runID,
		WorkflowName:    stored.GetWorkflowName(),
		WorkflowVersion: stored.GetWorkflowVersion(),
		Status:          status,
		StartedAt:       timestamppb.New(finishedAt.Add(-time.Second)),
		FinishedAt:      timestamppb.New(finishedAt),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fetch", Plugin: "http", Status: status},
		},
	}
	event := &orkstackv1.Event{
		RunId: runID, Type: eventType, Status: status,
		Message: "run " + status.String(), Timestamp: timestamppb.New(finishedAt),
	}
	if err := h.activities.PublishEvent(context.Background(), event, snapshot, runID+"#terminal"); err != nil {
		t.Fatalf("PublishEvent(terminal): %v", err)
	}
}

func (h *archiveHarness) get(t *testing.T, runID string) (*orkstackv1.Run, error) {
	t.Helper()
	resp, err := h.svc.GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

func (h *archiveHarness) list(t *testing.T, req *orkstackv1.ListRunsRequest) []*orkstackv1.Run {
	t.Helper()
	resp, err := h.svc.ListRuns(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	return resp.Msg.GetRuns()
}

func listedIDs(runs []*orkstackv1.Run) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		out = append(out, run.GetId())
	}
	return out
}

func sameIDs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// clock returns a Now function that advances a fixed step per call, so run ids
// and created_at timestamps are deterministic and strictly ordered.
func clock(start time.Time, step time.Duration) func() time.Time {
	current := start.Add(-step)
	return func() time.Time {
		current = current.Add(step)
		return current
	}
}

// TestEngineRestart_RunHistorySurvivesANewProcess is the durability claim: a
// second engine, with an empty read model, still answers `run get` and
// `run list` for runs the first engine executed.
func TestEngineRestart_RunHistorySurvivesANewProcess(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	succeeded := first.submit(t, "hello-ork")
	failed := first.submit(t, "hello-ork")
	first.finish(t, succeeded, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(time.Hour))
	first.finish(t, failed, orkstackv1.Status_STATUS_FAILED, start.Add(2*time.Hour))

	// The process dies. Everything in RunStore and EventBus goes with it.
	second := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	if _, ok := second.runs.Get(succeeded); ok {
		t.Fatal("the restarted engine started with a populated read model; the test proves nothing")
	}

	recovered, err := second.get(t, succeeded)
	if err != nil {
		t.Fatalf("GetRun after restart: %v", err)
	}
	if recovered.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("recovered status = %s, want SUCCEEDED", recovered.GetStatus())
	}
	if recovered.GetWorkflowName() != "hello-ork" || len(recovered.GetTaskRuns()) != 1 {
		t.Fatalf("recovered run lost detail: %+v", recovered)
	}
	if recovered.GetCreatedAt() == nil {
		t.Fatal("recovered run lost its created_at, so date-range queries would be blind")
	}

	all := second.list(t, &orkstackv1.ListRunsRequest{})
	if got := listedIDs(all); !sameIDs(got, failed, succeeded) {
		t.Fatalf("listed %v after restart, want %v newest first", got, []string{failed, succeeded})
	}

	onlySucceeded := second.list(t, &orkstackv1.ListRunsRequest{
		Status: orkstackv1.Status_STATUS_SUCCEEDED,
	})
	if got := listedIDs(onlySucceeded); !sameIDs(got, succeeded) {
		t.Fatalf("--status SUCCEEDED listed %v after restart, want %v", got, []string{succeeded})
	}
}

// TestListRuns_StatusAndOrkflowFiltersReachTheArchive is the `orkctl run list
// --status SUCCEEDED --orkflow hello-ork` path with nothing left in memory.
func TestListRuns_StatusAndOrkflowFiltersReachTheArchive(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	ids := make([]string, 0, 3)
	for i, status := range []orkstackv1.Status{
		orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SUCCEEDED,
	} {
		id := first.submit(t, "hello-ork")
		first.finish(t, id, status, start.Add(time.Duration(i+1)*time.Hour))
		ids = append(ids, id)
	}

	restarted := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	tests := []struct {
		name string
		req  *orkstackv1.ListRunsRequest
		want []string
	}{
		{
			name: "status only",
			req:  &orkstackv1.ListRunsRequest{Status: orkstackv1.Status_STATUS_SUCCEEDED},
			want: []string{ids[2], ids[0]},
		},
		{
			name: "orkflow only",
			req:  &orkstackv1.ListRunsRequest{WorkflowName: "hello-ork"},
			want: []string{ids[2], ids[1], ids[0]},
		},
		{
			name: "orkflow and status",
			req: &orkstackv1.ListRunsRequest{
				WorkflowName: "hello-ork",
				Status:       orkstackv1.Status_STATUS_FAILED,
			},
			want: []string{ids[1]},
		},
		{
			name: "another orkflow matches nothing",
			req:  &orkstackv1.ListRunsRequest{WorkflowName: "not-applied"},
			want: nil,
		},
		{
			name: "page size keeps the newest",
			req:  &orkstackv1.ListRunsRequest{PageSize: 1},
			want: []string{ids[2]},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := listedIDs(restarted.list(t, tc.req)); !sameIDs(got, tc.want...) {
				t.Fatalf("listed %v, want %v", got, tc.want)
			}
		})
	}
}

// TestListRuns_MergesLiveAndArchivedRunsNewestFirst keeps a run that is still
// executing visible alongside history, without ever listing it twice.
func TestListRuns_MergesLiveAndArchivedRunsNewestFirst(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	archived := first.submit(t, "hello-ork")
	first.finish(t, archived, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(time.Hour))

	second := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	second.apply(t, helloYAML(t))
	live := second.submit(t, "hello-ork")

	all := second.list(t, &orkstackv1.ListRunsRequest{})
	if got := listedIDs(all); !sameIDs(got, live, archived) {
		t.Fatalf("listed %v, want the live run %s before the archived run %s", got, live, archived)
	}

	// The live run finishes: it is now in both places, and must be listed once.
	second.finish(t, live, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(25*time.Hour))
	all = second.list(t, &orkstackv1.ListRunsRequest{})
	if got := listedIDs(all); !sameIDs(got, live, archived) {
		t.Fatalf("listed %v after the live run was archived, want each run once", got)
	}
}

// TestGetRun_PrefersTheLiveSnapshotOverTheArchive keeps `run get` truthful
// while a run is still executing: the archive is history, not the present.
func TestGetRun_PrefersTheLiveSnapshotOverTheArchive(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	harness := boot(t, archive, "a", clock(start, time.Minute))
	harness.apply(t, helloYAML(t))
	runID := harness.submit(t, "hello-ork")
	harness.finish(t, runID, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(time.Hour))

	// A later live update the archive will never see.
	harness.runs.Apply(&orkstackv1.Run{
		Id: runID, WorkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_CANCELED,
	})
	run, err := harness.get(t, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("GetRun returned %s, want the live snapshot CANCELED", run.GetStatus())
	}
}

// TestGetRun_UnknownRunIsStillNotFound proves the archive fallback did not turn
// every missing id into an internal error.
func TestGetRun_UnknownRunIsStillNotFound(t *testing.T) {
	harness := boot(t, newArchiver(t), "a", clock(time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC), time.Minute))
	_, err := harness.get(t, "ork-never-existed-1")
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetRun error = %v (code %s), want NotFound", err, connect.CodeOf(err))
	}
}

// TestSubmitRun_StarterFailureIsArchived covers the one terminal run the engine
// never sees: it failed before Temporal ever accepted it.
func TestSubmitRun_StarterFailureIsArchived(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	first.starter.err = fmt.Errorf("temporal is down")
	if _, err := first.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{WorkflowName: "hello-ork"})); err == nil {
		t.Fatal("SubmitRun succeeded despite a failing starter")
	}

	restarted := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	runs := restarted.list(t, &orkstackv1.ListRunsRequest{Status: orkstackv1.Status_STATUS_FAILED})
	if len(runs) != 1 {
		t.Fatalf("archived %d failed runs after restart, want 1", len(runs))
	}
	if runs[0].GetError().GetCode() != "RUN_START_FAILED" {
		t.Fatalf("archived error code = %q, want RUN_START_FAILED", runs[0].GetError().GetCode())
	}
}

// TestListRuns_WithoutAnArchiveIsUnchanged keeps the in-process-only wiring
// working exactly as before, so the archive is additive.
func TestListRuns_WithoutAnArchiveIsUnchanged(t *testing.T) {
	runs := engine.NewRunStore()
	svc := api.NewService(api.Config{Runs: runs, Starter: &fakeStarter{}})
	runs.Save(&orkstackv1.Run{
		Id: "ork-1", WorkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED,
	})
	resp, err := svc.ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{Status: orkstackv1.Status_STATUS_SUCCEEDED}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if got := listedIDs(resp.Msg.GetRuns()); !sameIDs(got, "ork-1") {
		t.Fatalf("listed %v, want [ork-1]", got)
	}
	if _, err := svc.GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-missing"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetRun error = %v, want NotFound without an archive", err)
	}
}

// serve puts a harness behind the real h2c server plus the WebSocket bridge,
// so the stream paths are exercised over the wire the CLI and UI use.
func serve(t *testing.T, h *archiveHarness) (string, *http.Client) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(h.svc, api.MuxOptions{}))
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
	return "http://" + listener.Addr().String(), &http.Client{Transport: transport}
}

// TestStreamEvents_ReplaysAnArchivedRunAfterRestart keeps `orkctl run watch`
// and the UI working for a run this process never executed: the archived event
// log is replayed and the stream ends, rather than answering NotFound.
func TestStreamEvents_ReplaysAnArchivedRunAfterRestart(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	runID := first.submit(t, "hello-ork")
	first.finish(t, runID, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(time.Hour))

	second := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	baseURL, httpClient := serve(t, second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)
	stream, err := client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID,
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var replayed []*orkstackv1.Event
	for stream.Receive() {
		replayed = append(replayed, stream.Msg())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %d events, want the archived RUN_CREATED and RUN_SUCCEEDED", len(replayed))
	}
	if replayed[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED ||
		replayed[1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("replayed %s then %s, want RUN_CREATED then RUN_SUCCEEDED",
			replayed[0].GetType(), replayed[1].GetType())
	}

	// from_sequence resumes inside the archived log, exactly as it does live.
	resumed, err := client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID, FromSequence: 1,
	}))
	if err != nil {
		t.Fatalf("StreamEvents(from_sequence=1): %v", err)
	}
	defer resumed.Close()
	var tail []*orkstackv1.Event
	for resumed.Receive() {
		tail = append(tail, resumed.Msg())
	}
	if len(tail) != 1 || tail[0].GetSequence() != 2 {
		t.Fatalf("resumed stream = %v, want only sequence 2", tail)
	}

	// A run nobody ever ran is still NotFound.
	ghost, err := client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: "ork-ghost-1",
	}))
	if err == nil {
		for ghost.Receive() {
		}
		err = ghost.Err()
		ghost.Close()
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("StreamEvents(ghost) error = %v (code %s), want NotFound", err, connect.CodeOf(err))
	}
}

// TestWebSocketBridge_ReplaysAnArchivedRunAfterRestart is the same guarantee for
// the browser bridge: history, then a normal close.
func TestWebSocketBridge_ReplaysAnArchivedRunAfterRestart(t *testing.T) {
	archive := newArchiver(t)
	start := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	first := boot(t, archive, "a", clock(start, time.Minute))
	first.apply(t, helloYAML(t))
	runID := first.submit(t, "hello-ork")
	first.finish(t, runID, orkstackv1.Status_STATUS_SUCCEEDED, start.Add(time.Hour))

	second := boot(t, archive, "b", clock(start.Add(24*time.Hour), time.Minute))
	baseURL, _ := serve(t, second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(baseURL, "http")+"/ws/runs/"+runID, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.CloseNow()

	var replayed []*orkstackv1.Event
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			break // the bridge closed after replaying the archived log
		}
		if typ != websocket.MessageText {
			t.Fatalf("frame type = %v, want text", typ)
		}
		var event orkstackv1.Event
		if err := api.UnmarshalJSON(data, &event); err != nil {
			t.Fatalf("decode frame %q: %v", data, err)
		}
		replayed = append(replayed, &event)
	}
	if len(replayed) != 2 {
		t.Fatalf("bridge replayed %d events, want 2", len(replayed))
	}
	if replayed[1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("last frame = %s, want RUN_SUCCEEDED", replayed[1].GetType())
	}

	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(baseURL, "http")+"/ws/runs/ork-ghost-1", nil)
	if err == nil {
		t.Fatal("dial(ghost) succeeded, want rejection")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("dial(ghost) status = %v, want 404", resp)
	}
}

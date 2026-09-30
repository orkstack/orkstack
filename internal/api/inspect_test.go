package api_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// liveFakeReader is a fakeReader that can also describe a live execution:
// the optional engine.LiveAttemptReader half, answered from a stored LiveRun
// or error, with every call counted. The plain fakeReader stays as it is, so
// a Service wired with it is a reader WITHOUT live support.
type liveFakeReader struct {
	*fakeReader
	liveMu sync.Mutex
	live   engine.LiveRun
	err    error
	calls  int
	nows   []time.Time
	// derive, when set, builds the answer from the clock the handler passed.
	derive func(now time.Time) engine.LiveRun
}

func (l *liveFakeReader) LiveAttempts(_ context.Context, _ string, now time.Time) (engine.LiveRun, error) {
	l.liveMu.Lock()
	defer l.liveMu.Unlock()
	l.calls++
	l.nows = append(l.nows, now)
	if l.err != nil {
		return engine.LiveRun{}, l.err
	}
	if l.derive != nil {
		return l.derive(now), nil
	}
	return l.live, nil
}

func (l *liveFakeReader) liveCalls() int {
	l.liveMu.Lock()
	defer l.liveMu.Unlock()
	return l.calls
}

var inspectNow = time.Date(2026, 9, 23, 9, 30, 0, 0, time.UTC)

func inspectRun(id string, status orkstackv1.Status, tasks ...string) *orkstackv1.Run {
	run := &orkstackv1.Run{Id: id, OrkflowName: "train-model", Status: status}
	for _, task := range tasks {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: task, Status: orkstackv1.Status_STATUS_RUNNING})
	}
	return run
}

type inspectFixture struct {
	*readerFixture
	live *liveFakeReader
}

func newInspectFixture(t *testing.T, run *orkstackv1.Run, live engine.LiveRun, liveErr error) *inspectFixture {
	t.Helper()
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	liveReader := &liveFakeReader{fakeReader: reader, live: live, err: liveErr}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Reader = liveReader
		cfg.Now = func() time.Time { return inspectNow }
	})
	return &inspectFixture{readerFixture: fixture, live: liveReader}
}

func (f *inspectFixture) inspect(t *testing.T, req *orkstackv1.InspectRunRequest) (*orkstackv1.InspectRunResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := f.runClient().InspectRun(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func taskIDsOf(attempts []*orkstackv1.LiveAttempt) []string {
	ids := make([]string, 0, len(attempts))
	for _, a := range attempts {
		ids = append(ids, a.GetTaskId())
	}
	return ids
}

func requireInspectCode(t *testing.T, err error, want connect.Code) *connect.Error {
	t.Helper()
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != want {
		t.Fatalf("err = %v, want %s", err, want)
	}
	return connectErr
}

// A1
func TestInspectRun_ReturnsLiveAttemptsInTaskRunOrder(t *testing.T) {
	run := inspectRun("ork-inspect-1", orkstackv1.Status_STATUS_RUNNING, "fetch", "transform", "load")
	f := newInspectFixture(t, run, engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{
		{TaskId: "load", State: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED},
		{TaskId: "fetch", State: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED},
	}}, nil)
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if got := fmt.Sprint(taskIDsOf(resp.GetAttempts())); got != "[fetch load]" {
		t.Fatalf("attempt order = %s, want [fetch load] (Run.task_runs order)", got)
	}
	if resp.GetRunStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("run_status = %s, want RUNNING", resp.GetRunStatus())
	}
	if !resp.GetComputedAt().AsTime().Equal(inspectNow) {
		t.Fatalf("computed_at = %s, want the fixed clock %s", resp.GetComputedAt().AsTime(), inspectNow)
	}
}

// A2
func TestInspectRun_TaskFilterNarrowsAndUnknownTaskIsNotFound(t *testing.T) {
	run := inspectRun("ork-inspect-2", orkstackv1.Status_STATUS_RUNNING, "fetch", "load")
	f := newInspectFixture(t, run, engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{
		{TaskId: "fetch"}, {TaskId: "load"},
	}}, nil)
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId(), TaskId: "load"})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if got := fmt.Sprint(taskIDsOf(resp.GetAttempts())); got != "[load]" {
		t.Fatalf("attempts = %s, want [load]", got)
	}
	_, err = f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId(), TaskId: "nope"})
	connectErr := requireInspectCode(t, err, connect.CodeNotFound)
	if want := `task "nope" not found in run "ork-inspect-2"`; connectErr.Message() != want {
		t.Fatalf("message = %q, want %q", connectErr.Message(), want)
	}
}

// A3
func TestInspectRun_ATerminalRunAnswersFromTheSnapshotWithoutTheCluster(t *testing.T) {
	run := inspectRun("ork-inspect-3", orkstackv1.Status_STATUS_SUCCEEDED, "fetch")
	f := newInspectFixture(t, run, engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{{TaskId: "fetch"}}}, nil)
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if len(resp.GetAttempts()) != 0 || resp.GetRunStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("response = %v, want SUCCEEDED and no attempts", resp)
	}
	if calls := f.live.liveCalls(); calls != 0 {
		t.Fatalf("LiveAttempts called %d times for a terminal run, want 0", calls)
	}
}

// A4
func TestInspectRun_AnotherNamespacesRunIsNotFound(t *testing.T) {
	run := inspectRun("ork-inspect-4", orkstackv1.Status_STATUS_RUNNING, "fetch")
	run.Namespace = "payments"
	f := newInspectFixture(t, run, engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{{TaskId: "fetch"}}}, nil)
	_, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	requireInspectCode(t, err, connect.CodeNotFound)
	if calls := f.live.liveCalls(); calls != 0 {
		t.Fatalf("LiveAttempts called %d times across namespaces, want 0 (nothing is described before the check)", calls)
	}
}

// A5: a reader without the live half, and no reader at all.
func TestInspectRun_AReaderWithoutLiveSupportIsFailedPrecondition(t *testing.T) {
	const want = "live attempt inspection is not available: this engine does not read runs from the cluster"
	run := inspectRun("ork-inspect-5", orkstackv1.Status_STATUS_RUNNING, "fetch")

	plain := newReaderFixture(t, &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := plain.runClient().InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{RunId: run.GetId()}))
	if connectErr := requireInspectCode(t, err, connect.CodeFailedPrecondition); connectErr.Message() != want {
		t.Fatalf("plain reader message = %q, want %q", connectErr.Message(), want)
	}

	runs := engine.NewRunStore()
	runs.Save(run)
	client := inMemoryRunClient(t, runs)
	_, err = client.InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{RunId: run.GetId()}))
	if connectErr := requireInspectCode(t, err, connect.CodeFailedPrecondition); connectErr.Message() != want {
		t.Fatalf("in-process read model message = %q, want %q", connectErr.Message(), want)
	}
}

// inMemoryRunClient serves a Service whose read model is its own RunStore
// (Config.Reader nil), the unit-test and in-process shape.
func inMemoryRunClient(t *testing.T, runs *engine.RunStore) orkstackv1connect.RunServiceClient {
	t.Helper()
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
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
	return orkstackv1connect.NewRunServiceClient(&http.Client{Transport: transport}, "http://"+listener.Addr().String())
}

// A6
func TestInspectRun_ClusterFailureIsUnavailableNeverEmpty(t *testing.T) {
	run := inspectRun("ork-inspect-6", orkstackv1.Status_STATUS_RUNNING, "fetch")
	f := newInspectFixture(t, run, engine.LiveRun{}, fmt.Errorf("temporal is unreachable"))
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err == nil {
		t.Fatalf("response = %v, want Unavailable — a cluster outage is never an empty list", resp)
	}
	requireInspectCode(t, err, connect.CodeUnavailable)
}

// A7
func TestInspectRun_ARunThatLeftTheClusterAnswersTheSnapshotStatus(t *testing.T) {
	run := inspectRun("ork-inspect-7", orkstackv1.Status_STATUS_RUNNING, "fetch")
	f := newInspectFixture(t, run, engine.LiveRun{}, fmt.Errorf("gone: %w", engine.ErrRunNotFound))
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if len(resp.GetAttempts()) != 0 || resp.GetRunStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("response = %v, want RUNNING (the snapshot's) and no attempts", resp)
	}
}

// A8: the reader is handed the handler's clock, and the flag it derives
// against that clock is what the response carries beside computed_at. The
// fake derives staleness from task.HeartbeatStaleAfter against the clock it
// was HANDED (engine's TestLiveAttempts_DerivesEveryScenarioOfTheTable L6/L11
// holds the real derivation), so a handler that passed any other clock, or
// stamped computed_at from a second reading, fails here.
func TestInspectRun_StalenessIsComputedAgainstTheHandlerClock(t *testing.T) {
	lastBeat := inspectNow.Add(-24 * time.Second)
	for _, tc := range []struct {
		now   time.Time
		stale bool
	}{{inspectNow, true}, {lastBeat.Add(3 * time.Second), false}} {
		run := inspectRun("ork-inspect-8", orkstackv1.Status_STATUS_RUNNING, "fit")
		reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
		live := &liveFakeReader{fakeReader: reader, derive: func(now time.Time) engine.LiveRun {
			return engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{{
				TaskId:             "fit",
				HeartbeatAvailable: true,
				LastHeartbeatAt:    timestamppb.New(lastBeat),
				HeartbeatStale:     now.Sub(lastBeat) > task.HeartbeatStaleAfter,
			}}}
		}}
		now := tc.now
		fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
			cfg.Reader = live
			cfg.Now = func() time.Time { return now }
		})
		f := &inspectFixture{readerFixture: fixture, live: live}
		resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
		if err != nil {
			t.Fatalf("InspectRun: %v", err)
		}
		if got := resp.GetAttempts()[0].GetHeartbeatStale(); got != tc.stale {
			t.Fatalf("Now %s after the last beat: heartbeat_stale = %v, want %v",
				now.Sub(lastBeat), got, tc.stale)
		}
		live.liveMu.Lock()
		nows := append([]time.Time(nil), live.nows...)
		live.liveMu.Unlock()
		if !resp.GetComputedAt().AsTime().Equal(now) || len(nows) != 1 || !nows[0].Equal(now) {
			t.Fatalf("computed_at %s, reader clock %v: want both %s", resp.GetComputedAt().AsTime(), nows, now)
		}
		t.Logf("Now fixed %s after last_heartbeat_at → heartbeat_stale=%v, computed_at=%s",
			now.Sub(lastBeat), tc.stale, resp.GetComputedAt().AsTime().Format(time.RFC3339))
	}
}

// A9
func TestInspectRun_APendingTaskMissingFromTheSnapshotIsKeptLast(t *testing.T) {
	run := inspectRun("ork-inspect-9", orkstackv1.Status_STATUS_RUNNING, "fetch", "load")
	f := newInspectFixture(t, run, engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{
		{TaskId: "zeta"}, {TaskId: "load"}, {TaskId: "alpha"}, {TaskId: "fetch"},
	}}, nil)
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if got := fmt.Sprint(taskIDsOf(resp.GetAttempts())); got != "[fetch load alpha zeta]" {
		t.Fatalf("attempts = %s, want [fetch load alpha zeta]", got)
	}
}

// A10
func TestInspectRun_MissingRunIDIsInvalidArgument(t *testing.T) {
	run := inspectRun("ork-inspect-10", orkstackv1.Status_STATUS_RUNNING, "fetch")
	f := newInspectFixture(t, run, engine.LiveRun{}, nil)
	_, err := f.inspect(t, &orkstackv1.InspectRunRequest{})
	if connectErr := requireInspectCode(t, err, connect.CodeInvalidArgument); connectErr.Message() != "run_id is required" {
		t.Fatalf("message = %q", connectErr.Message())
	}
}

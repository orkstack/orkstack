package api_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
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

// These tests describe the replica that did not start the run.
//
// Its RunStore is empty and its EventBus is empty, because it accepted no
// submission and executed none of the run's projection activities — which is
// the ordinary state of a replica behind a load balancer. Every answer below
// therefore has to come from the system of record, and every one of these
// tests fails the moment a read reaches for process-local state again.

// fakeReader stands in for engine.TemporalRunReader: the run and the ordered
// stream that Temporal holds for it, with nothing of either in this process.
type fakeReader struct {
	// mu guards runs, so a test can move a run to a terminal state while a
	// stream is following it — which is the whole shape of a live log follow.
	mu     sync.Mutex
	runs   map[string]*orkstackv1.Run
	events map[string][]*orkstackv1.Event
	// live blocks the stream after the events already recorded, so a test can
	// assert on a run that has not finished yet. Closing it ends the stream.
	live chan struct{}
	// listErr and getErr make the record unreachable rather than empty.
	listErr error
	getErr  error
	// runReadCount counts GetRun, which is one whole-history replay apiece.
	runReadCount int
	// executions is the aggregate half of the same record: what the cluster's
	// index says about a whole namespace rather than about one run. It is
	// keyed by namespace, because a namespace is a tenancy boundary and a
	// double that answered the same thing for every name could not fail the
	// test that says one namespace's numbers exclude another's.
	executions    map[string]engine.Executions
	executionsErr error
	// executionWindows records every window the aggregate half was asked
	// about, so a test can prove that an answer served from the cache
	// consulted the record no further.
	executionWindows []engine.StatsWindow
}

// Executions answers the aggregate question. It records the window first, so
// a call that fails is still a call that happened.
func (f *fakeReader) Executions(
	_ context.Context,
	w engine.StatsWindow,
) (engine.Executions, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executionWindows = append(f.executionWindows, w)
	if f.executionsErr != nil {
		return engine.Executions{}, f.executionsErr
	}
	return f.executions[w.Namespace], nil
}

// executionCalls is how many times the aggregate half was consulted.
func (f *fakeReader) executionCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.executionWindows)
}

// setRun replaces what the record answers for a run, as the cluster would when
// the run moves on.
func (f *fakeReader) setRun(run *orkstackv1.Run) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runs == nil {
		f.runs = map[string]*orkstackv1.Run{}
	}
	f.runs[run.GetId()] = run
}

func (f *fakeReader) GetRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Counted, because reading a run is the expensive answer in this system:
	// engine.TemporalRunReader.GetRun replays the run's whole history. A poll
	// loop that asks it per tick is a cost no test would otherwise see.
	f.runReadCount++
	run, ok := f.runs[runID]
	if !ok {
		return nil, fmt.Errorf("fake reader: %s: %w", runID, engine.ErrRunNotFound)
	}
	return run, nil
}

// runReads is how many whole-run reads this reader has served.
func (f *fakeReader) runReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runReadCount
}

func (f *fakeReader) ListRuns(_ context.Context, q engine.RunQuery) ([]*orkstackv1.Run, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*orkstackv1.Run, 0, len(f.runs))
	for _, run := range f.runs {
		if q.Namespace != "" && artifacts.ResolveNamespace(run.GetNamespace()) != artifacts.ResolveNamespace(q.Namespace) {
			continue
		}
		if q.OrkflowName != "" && run.GetOrkflowName() != q.OrkflowName {
			continue
		}
		if q.Status != orkstackv1.Status_STATUS_UNSPECIFIED && run.GetStatus() != q.Status {
			continue
		}
		out = append(out, run)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := engine.RunIndexTime(out[i]), engine.RunIndexTime(out[j])
		if !a.Equal(b) {
			return a.After(b)
		}
		return out[i].GetId() > out[j].GetId()
	})
	// Mirror TemporalRunReader exactly: Exhaustive changes how completely the
	// index is walked, not how much comes back, and only an exhaustive caller
	// that also leaves Limit at 0 receives everything. A double that returned
	// more than the production reader would hide a pagination bug.
	limit := q.Limit
	if limit <= 0 && !q.Exhaustive {
		limit = engine.DefaultRunPageSize
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeReader) StreamEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	events, ok := f.events[runID]
	if !ok {
		return fmt.Errorf("fake reader: %s: %w", runID, engine.ErrRunNotFound)
	}
	for _, event := range events {
		if event.GetSequence() <= fromSequence {
			continue
		}
		if err := send(event); err != nil {
			return err
		}
		if api.IsTerminalEvent(event) {
			return nil
		}
	}
	if f.live == nil {
		return nil
	}
	select {
	case <-f.live:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readerFixture is a service whose only source of run truth is the reader.
type readerFixture struct {
	svc     *api.Service
	runs    *engine.RunStore
	reader  *fakeReader
	baseURL string
	client  *http.Client
}

func newReaderFixture(t *testing.T, reader *fakeReader) *readerFixture {
	t.Helper()
	return newReaderFixtureWith(t, reader, nil)
}

// newReaderFixtureWith is newReaderFixture with the config left open for one
// more seam, so a test about logs does not need a second copy of the server
// wiring.
func newReaderFixtureWith(
	t *testing.T,
	reader *fakeReader,
	configure func(*api.Config),
) *readerFixture {
	t.Helper()
	runs := engine.NewRunStore()
	cfg := api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Reader:   reader,
		Runs:     runs,
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
	}
	if configure != nil {
		configure(&cfg)
	}
	svc := api.NewService(cfg)

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

	return &readerFixture{
		svc:     svc,
		runs:    runs,
		reader:  reader,
		baseURL: "http://" + listener.Addr().String(),
		client:  &http.Client{Transport: transport},
	}
}

func (f *readerFixture) runClient() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(f.client, f.baseURL)
}

// foreignRun is a run started elsewhere: it is live, it is not in this
// process's store, and its stream is 1..4 ending on RUN_SUCCEEDED.
func foreignRun() (*orkstackv1.Run, []*orkstackv1.Event) {
	at := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	run := &orkstackv1.Run{
		Id:             "ork-foreign-1",
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt:      timestamppb.New(at),
		StartedAt:      timestamppb.New(at.Add(time.Second)),
		FinishedAt:     timestamppb.New(at.Add(4 * time.Second)),
	}
	types := []orkstackv1.EventType{
		orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
	}
	events := make([]*orkstackv1.Event, 0, len(types))
	for i, eventType := range types {
		events = append(events, &orkstackv1.Event{
			RunId:     run.GetId(),
			Sequence:  int64(i + 1),
			Type:      eventType,
			Message:   eventType.String(),
			Timestamp: timestamppb.New(at.Add(time.Duration(i) * time.Second)),
		})
	}
	return run, events
}

func newForeignFixture(t *testing.T) *readerFixture {
	t.Helper()
	run, events := foreignRun()
	return newReaderFixture(t, &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): events},
	})
}

func TestGetRun_AnswersForARunThisReplicaNeverSaw(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, local := f.runs.Get("ork-foreign-1"); local {
		t.Fatal("the fixture's local store already knows the run; the test would prove nothing")
	}
	got, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: "ork-foreign-1",
	}))
	if err != nil {
		t.Fatalf("GetRun on a replica that did not start the run: %v", err)
	}
	run := got.Msg.GetRun()
	if run.GetId() != "ork-foreign-1" || run.GetOrkflowName() != "hello-ork" {
		t.Fatalf("GetRun returned %s@%s, want ork-foreign-1 / hello-ork",
			run.GetId(), run.GetOrkflowName())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("GetRun reports %s, want the record's SUCCEEDED", run.GetStatus())
	}
}

func TestListRuns_AnswersForRunsThisReplicaNeverSaw(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listed, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 1 || listed.Msg.GetRuns()[0].GetId() != "ork-foreign-1" {
		t.Fatalf("ListRuns returned %v, want the one run the record holds",
			listedIDs(listed.Msg.GetRuns()))
	}

	// The filters have to reach the record too, not a local index of them.
	filtered, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		OrkflowName: "something-else",
	}))
	if err != nil {
		t.Fatalf("ListRuns(orkflow_name): %v", err)
	}
	if len(filtered.Msg.GetRuns()) != 0 {
		t.Fatalf("filtering by another orkflow returned %v, want nothing",
			listedIDs(filtered.Msg.GetRuns()))
	}
}

// TestListRuns_PageSizeBoundsASafetyShapedQuery pins the public pagination
// contract against the exhaustive walk the deletion gate needs.
//
// A request naming an orkflow AND a non-terminal status is answered by reading
// visibility to the end, so an older live run cannot hide behind a page of
// newer ones. That is about how completely the index is WALKED. It must not
// change how much is RETURNED: page_size still caps the response, and a
// request that names no page_size still gets one default page rather than
// every run the orkflow ever had.
func TestListRuns_PageSizeBoundsASafetyShapedQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// One more than a default page, so "bounded" and "everything" differ.
	total := engine.DefaultRunPageSize + 50
	runs := make(map[string]*orkstackv1.Run, total)
	base := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("ork-running-%03d", i)
		runs[id] = &orkstackv1.Run{
			Id:          id,
			OrkflowName: "deploy",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			CreatedAt:   timestamppb.New(base.Add(time.Duration(i) * time.Second)),
		}
	}
	f := newReaderFixture(t, &fakeReader{runs: runs})

	asked, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_RUNNING,
		PageSize:    10,
	}))
	if err != nil {
		t.Fatalf("ListRuns(page_size=10): %v", err)
	}
	if got := len(asked.Msg.GetRuns()); got != 10 {
		t.Errorf("ListRuns(page_size=10) returned %d runs, want the 10 that were asked for", got)
	}

	// page_size 0 has always meant "one default page", never "all of them".
	unstated, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_RUNNING,
	}))
	if err != nil {
		t.Fatalf("ListRuns(no page_size): %v", err)
	}
	if got := len(unstated.Msg.GetRuns()); got != engine.DefaultRunPageSize {
		t.Errorf("ListRuns without page_size returned %d runs, want one default page of %d; "+
			"an exhaustive walk must not turn a public list into an unbounded dump",
			got, engine.DefaultRunPageSize)
	}
}

func TestStreamEvents_ReplaysTheWholeOrderedStreamOnAReplicaThatNeverSawTheRun(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: "ork-foreign-1",
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	var received []*orkstackv1.Event
	for stream.Receive() {
		received = append(received, stream.Msg())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	assertStreamContract(t, received, 0)

	// A cursor resumes inside the record's stream, not inside a local log.
	resumed, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: "ork-foreign-1", FromSequence: 2,
	}))
	if err != nil {
		t.Fatalf("StreamEvents(from_sequence=2): %v", err)
	}
	defer resumed.Close()
	var tail []*orkstackv1.Event
	for resumed.Receive() {
		tail = append(tail, resumed.Msg())
	}
	assertStreamContract(t, tail, 2)
}

func TestWebSocketBridge_ReplaysTheWholeOrderedStreamOnAReplicaThatNeverSawTheRun(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx,
		strings.Replace(f.baseURL, "http://", "ws://", 1)+"/ws/runs/ork-foreign-1", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.CloseNow()

	var received []*orkstackv1.Event
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		event := &orkstackv1.Event{}
		if err := api.UnmarshalJSON(data, event); err != nil {
			t.Fatalf("decode frame %q: %v", data, err)
		}
		received = append(received, event)
	}
	assertStreamContract(t, received, 0)
}

// TestWebSocketBridge_RejectsARunTheRecordDoesNotHold keeps the 404: the bridge
// resolves the run before upgrading, so a browser gets a status code rather
// than a socket that opens and dies.
func TestWebSocketBridge_RejectsARunTheRecordDoesNotHold(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx,
		strings.Replace(f.baseURL, "http://", "ws://", 1)+"/ws/runs/ork-nope-1", nil)
	if err == nil {
		t.Fatal("dial succeeded for a run the record does not hold")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bridge answered %v, want 404", resp)
	}
}

// TestReads_IgnoreAStaleProcessLocalSnapshot is the cache rule, stated as a
// test. The local store holds an older, contradictory copy of the same run;
// the answer must still be the record's, or the store is not a cache.
func TestReads_IgnoreAStaleProcessLocalSnapshot(t *testing.T) {
	f := newForeignFixture(t)
	f.runs.Save(&orkstackv1.Run{
		Id:             "ork-foreign-1",
		OrkflowName:    "stale-local-copy",
		OrkflowVersion: "0.0.1",
		Status:         orkstackv1.Status_STATUS_RUNNING,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: "ork-foreign-1",
	}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if name := got.Msg.GetRun().GetOrkflowName(); name != "hello-ork" {
		t.Fatalf("GetRun answered from the process-local store (%s), not the record", name)
	}
	if status := got.Msg.GetRun().GetStatus(); status != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun reports %s, want the record's SUCCEEDED", status)
	}
}

// TestGetRun_ReportsAnUnreachableRecordRatherThanAnswering is the other half of
// the cache rule: when the record cannot be read, the answer is an error, not
// whatever this process happens to remember.
func TestGetRun_ReportsAnUnreachableRecordRatherThanAnswering(t *testing.T) {
	run, _ := foreignRun()
	f := newReaderFixture(t, &fakeReader{getErr: fmt.Errorf("temporal is unreachable")})
	f.runs.Save(run)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: run.GetId(),
	}))
	if err == nil {
		t.Fatal("GetRun answered from memory while the record was unreachable")
	}
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("GetRun failed with %s, want Unavailable", got)
	}
}

// TestStreamEvents_EndsALiveRunAtItsTerminalEvent holds the stream contract for
// a run that is still executing when the read starts: the reader blocks, then
// delivers the terminal event and the stream ends.
func TestStreamEvents_EndsALiveRunAtItsTerminalEvent(t *testing.T) {
	run, events := foreignRun()
	live := make(chan struct{})
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): events[:2]},
		live:   live,
	}
	f := newReaderFixture(t, reader)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := f.runClient().StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: run.GetId(),
	}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	for i := 1; i <= 2; i++ {
		if !stream.Receive() {
			t.Fatalf("stream ended before event %d: %v", i, stream.Err())
		}
		if got := stream.Msg().GetSequence(); got != int64(i) {
			t.Fatalf("received sequence %d, want %d", got, i)
		}
	}
	// The run finishes on the cluster; the stream must end, not hang.
	reader.events[run.GetId()] = events
	close(live)
	if stream.Receive() {
		t.Fatalf("stream continued past the blocking point with %v", stream.Msg())
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
}

// assertStreamContract holds acceptance 2: strictly increasing sequences
// starting just after the cursor, no gaps, and a terminal event last.
func assertStreamContract(t *testing.T, events []*orkstackv1.Event, fromSequence int64) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("the stream delivered nothing")
	}
	for i, event := range events {
		if got, want := event.GetSequence(), fromSequence+int64(i)+1; got != want {
			t.Fatalf("event %d carries sequence %d, want %d (stream: %v)",
				i, got, want, sequencesOf(events))
		}
	}
	if !api.IsTerminalEvent(events[len(events)-1]) {
		t.Fatalf("the stream ended on %s, want a terminal event",
			events[len(events)-1].GetType())
	}
	for _, event := range events[:len(events)-1] {
		if api.IsTerminalEvent(event) {
			t.Fatalf("a terminal event (%s) arrived before the end of the stream",
				event.GetType())
		}
	}
}

func sequencesOf(events []*orkstackv1.Event) []int64 {
	out := make([]int64, 0, len(events))
	for _, event := range events {
		out = append(out, event.GetSequence())
	}
	return out
}

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
	// listQueries records every RunQuery this reader was asked, so a test can
	// assert what the handler ASKED for and not only what came back. The wire
	// path setting Exhaustive is invisible in the response — it costs pages,
	// not rows — so it has to be observed here.
	listQueries []engine.RunQuery
	// runningCounts records every namespace RunningCount was asked about, and
	// runningCountErr makes that count unreadable.
	runningCounts []string
	// runningCeilings is the ceiling each RunningCount call passed.
	runningCeilings []int64
	runningCountErr error
	// starts is what each run was started with — its pinned definition and
	// plan, as its start event holds them — and startErr makes that event
	// unreadable rather than absent.
	starts   map[string]*engine.RunStartRecord
	startErr error
	// runStartCount counts RunStart, the one-event read.
	runStartCount int
	// streamCount counts StreamEvents, one history walk apiece.
	streamCount int
	// startless names runs whose start event reads as gone (aged out of
	// history while the run record is still answered elsewhere).
	startless map[string]bool
	// definitions, when set, resolves a start record passed by reference,
	// as TemporalRunReader.RunDefinition does over object storage; unset,
	// RunDefinition returns the record's own definition, as every fake's
	// does. runDefinitionCount counts the calls.
	definitions        func(ctx context.Context, runID string, start *engine.RunStartRecord) (*orkstackv1.Orkflow, error)
	runDefinitionCount int
}

// RunDefinition resolves a start record's definition: through definitions
// when a test set it, otherwise the record's own.
func (f *fakeReader) RunDefinition(ctx context.Context, runID string, start *engine.RunStartRecord) (*orkstackv1.Orkflow, error) {
	f.mu.Lock()
	f.runDefinitionCount++
	resolve := f.definitions
	f.mu.Unlock()
	if resolve != nil {
		return resolve(ctx, runID, start)
	}
	return start.Orkflow, nil
}

// RunStart answers from starts, the way TemporalRunReader answers from a
// run's first history event. A run the record holds but no test gave a start
// has the start every run has: its definition, naming its orkflow and its
// namespace — what the start event of a run in runs would say.
func (f *fakeReader) RunStart(_ context.Context, runID string) (*engine.RunStartRecord, error) {
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runStartCount++
	if start, ok := f.starts[runID]; ok {
		return start, nil
	}
	if run, ok := f.runs[runID]; ok && !f.startless[runID] {
		return &engine.RunStartRecord{Orkflow: &orkstackv1.Orkflow{
			Name: run.GetOrkflowName(), Version: run.GetOrkflowVersion(), Namespace: run.GetNamespace(),
		}}, nil
	}
	return nil, fmt.Errorf("fake reader: %s: %w", runID, engine.ErrRunNotFound)
}

// setStart records what a run was started with.
func (f *fakeReader) setStart(runID string, start *engine.RunStartRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.starts == nil {
		f.starts = map[string]*engine.RunStartRecord{}
	}
	f.starts[runID] = start
}

// RunningCount answers the admission question from the same executions map
// the aggregate half reads, so a test seeds one map for both.
func (f *fakeReader) RunningCount(_ context.Context, namespace string, ceiling int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	namespace = artifacts.ResolveNamespace(namespace)
	f.runningCounts = append(f.runningCounts, namespace)
	f.runningCeilings = append(f.runningCeilings, ceiling)
	if f.runningCountErr != nil {
		return 0, f.runningCountErr
	}
	return int64(len(f.executions[namespace].Running)), nil
}

// runningCountNamespaces is every namespace RunningCount was asked about.
func (f *fakeReader) runningCountNamespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.runningCounts...)
}

// runQueries is every RunQuery this reader has served, in order.
func (f *fakeReader) runQueries() []engine.RunQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]engine.RunQuery(nil), f.listQueries...)
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
	f.mu.Lock()
	f.listQueries = append(f.listQueries, q)
	f.mu.Unlock()
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
		if q.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && run.GetOrigin().GetKind() != q.OriginKind {
			continue
		}
		if q.OriginID != "" && run.GetOrigin().GetId() != q.OriginID {
			continue
		}
		// The residual filters and the cursor, exactly as the production
		// reader's matchesRunQuery applies them: on RunIndexTime, and Keep on
		// the filter view rather than the raw run.
		created := engine.RunIndexTime(run)
		if (!q.Since.IsZero() && created.Before(q.Since)) || (!q.Until.IsZero() && !created.Before(q.Until)) {
			continue
		}
		if !q.BeforeCreated.IsZero() && !(created.Before(q.BeforeCreated) || (created.Equal(q.BeforeCreated) && run.GetId() < q.BeforeID)) {
			continue
		}
		if q.Keep != nil && !q.Keep(engine.RunFilterView(run)) {
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
	f.mu.Lock()
	f.streamCount++
	f.mu.Unlock()
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
		Filters: filters(t, "orkflow_name=something-else"),
	}))
	if err != nil {
		t.Fatalf("ListRuns(orkflow_name): %v", err)
	}
	if len(filtered.Msg.GetRuns()) != 0 {
		t.Fatalf("filtering by another orkflow returned %v, want nothing",
			listedIDs(filtered.Msg.GetRuns()))
	}
}

// TestListRuns_ArchiveStillAnswersAStatusVisibilityCannotDecode is the guard on
// the reader's impossible-status shortcut.
//
// TemporalRunReader skips the visibility walk entirely for STATUS_PENDING and
// STATUS_SKIPPED, because statusFromExecution can never decode a record into
// either, so the walk was 20 round-trips proving a foregone conclusion. Those
// statuses are real on the wire all the same — the ARCHIVE stores the status it
// was given, and a PENDING run lives there.
//
// So the shortcut is only safe while ListRuns keeps merging the archive half
// independently of what the reader returned. A shortcut written one layer up,
// in this handler instead of the reader, would make PENDING runs vanish from
// the wire; this test is what fails in that case.
func TestListRuns_ArchiveStillAnswersAStatusVisibilityCannotDecode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	archive := newArchiver(t)
	pending := &orkstackv1.Run{
		Id:          "ork-archived-pending-1",
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_PENDING,
		CreatedAt:   timestamppb.New(time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)),
	}
	if err := archive.Archive(ctx, pending, nil); err != nil {
		t.Fatalf("archive the pending run: %v", err)
	}

	// The reader holds a RUNNING run of the same orkflow and nothing pending,
	// which is exactly what the real reader can hold: visibility decodes only
	// SUCCEEDED, FAILED, CANCELED and RUNNING.
	f := newReaderFixtureWith(t, &fakeReader{runs: map[string]*orkstackv1.Run{
		"ork-live-1": {
			Id:          "ork-live-1",
			OrkflowName: "deploy",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			CreatedAt:   timestamppb.New(time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)),
		},
	}}, func(cfg *api.Config) { cfg.Archive = archive })

	listed, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: filters(t, "orkflow_name=deploy", "status=PENDING"),
	}))
	if err != nil {
		t.Fatalf("ListRuns(status=PENDING): %v", err)
	}
	ids := make([]string, 0, len(listed.Msg.GetRuns()))
	for _, run := range listed.Msg.GetRuns() {
		ids = append(ids, run.GetId())
	}
	if len(ids) != 1 || ids[0] != pending.GetId() {
		t.Fatalf("ListRuns(status=PENDING) returned %v, want [%s]; the reader's shortcut "+
			"must not take the archive's answer with it", ids, pending.GetId())
	}
}

// TestListRuns_StaysBoundedAndNeverAsksForAnExhaustiveWalk pins what a WIRE
// list may cost.
//
// The deletion gate needs an exhaustive walk, and for a while any request that
// named an orkflow and a non-terminal status was given one — which made
// `orkctl run list --filter orkflow_name=X --filter status=RUNNING` read the whole visibility
// index and, past the scan budget, fail with a sentence about deletion safety.
// The exhaustive walk belongs to Service.completeOrkflowRuns alone. From the
// wire, the status is answered by a visibility PREDICATE instead, so the
// ordinary bounded budget suffices.
//
// Two things are pinned: page_size still caps the response and an unstated
// page_size still means one default page, and no wire request ever sets
// Exhaustive on the query the handler builds.
func TestListRuns_StaysBoundedAndNeverAsksForAnExhaustiveWalk(t *testing.T) {
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
		Filters:  filters(t, "orkflow_name=deploy", "status=RUNNING"),
		PageSize: 10,
	}))
	if err != nil {
		t.Fatalf("ListRuns(page_size=10): %v", err)
	}
	if got := len(asked.Msg.GetRuns()); got != 10 {
		t.Errorf("ListRuns(page_size=10) returned %d runs, want the 10 that were asked for", got)
	}

	// page_size 0 has always meant "one default page", never "all of them".
	unstated, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: filters(t, "orkflow_name=deploy", "status=RUNNING"),
	}))
	if err != nil {
		t.Fatalf("ListRuns(no page_size): %v", err)
	}
	if got := len(unstated.Msg.GetRuns()); got != engine.DefaultRunPageSize {
		t.Errorf("ListRuns without page_size returned %d runs, want one default page of %d; "+
			"an exhaustive walk must not turn a public list into an unbounded dump",
			got, engine.DefaultRunPageSize)
	}

	// The cost, not the response. Exhaustive is invisible in the rows that
	// come back — it buys pages — so it has to be read off the query the
	// handler built. Both requests above are the exact shape that would
	// trigger a whole-index walk.
	queries := f.reader.runQueries()
	if len(queries) != 2 {
		t.Fatalf("reader served %d list queries, want 2", len(queries))
	}
	for i, q := range queries {
		if q.Exhaustive {
			t.Errorf("wire ListRuns[%d] asked for an exhaustive walk (%+v); the whole-index "+
				"walk belongs to the deletion gate, and from the wire it lets a plain "+
				"`run list` scan every run and fail with a deletion-safety error", i, q)
		}
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

// assertStreamContract holds the stream contract: strictly increasing
// sequences starting just after the cursor, no gaps, and a terminal event last.
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

// TestGetRun_MapsAMissingSnapshotShapeByWhoHoldsTheBetterAnswer pins the API
// mapping for the exact error shape engine.TemporalRunReader's loadSnapshot
// produces when a run's snapshot object is gone: it wraps
// artifacts.ErrNotFound, so LookupRun falls through to the archive. For a
// CLOSED run that fallthrough is the point — the archive holds the finished
// run's complete body — and this test holds that path open. For a LIVE run
// there is no archive body, which is why the reader itself must never emit
// this shape for a run that is still executing:
// TestTemporalRunReader_GetRunSurvivesAMissingSnapshotOnALiveRun (engine)
// guarantees the degraded run is returned instead. A transient storage error
// keeps its distinct shape and must stay Unavailable, never a fallthrough.
func TestGetRun_MapsAMissingSnapshotShapeByWhoHoldsTheBetterAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("closed run falls through to the archive body", func(t *testing.T) {
		run, events := foreignRun()
		archive := newArchiver(t)
		if err := archive.Archive(ctx, run, events); err != nil {
			t.Fatalf("archive the finished run: %v", err)
		}
		missing := fmt.Errorf("engine: read snapshot of run %s: %w", run.GetId(), artifacts.ErrNotFound)
		f := newReaderFixtureWith(t, &fakeReader{getErr: missing},
			func(cfg *api.Config) { cfg.Archive = archive })

		got, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: run.GetId(),
		}))
		if err != nil {
			t.Fatalf("GetRun should have answered from the archive: %v", err)
		}
		if got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("archived run status = %s, want SUCCEEDED", got.Msg.GetRun().GetStatus())
		}
	})

	t.Run("transient storage error stays unavailable", func(t *testing.T) {
		run, events := foreignRun()
		archive := newArchiver(t)
		if err := archive.Archive(ctx, run, events); err != nil {
			t.Fatalf("archive the finished run: %v", err)
		}
		transient := fmt.Errorf("engine: read snapshot of run %s: object storage is down", run.GetId())
		f := newReaderFixtureWith(t, &fakeReader{getErr: transient},
			func(cfg *api.Config) { cfg.Archive = archive })

		_, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: run.GetId(),
		}))
		if err == nil {
			t.Fatal("GetRun answered from the archive while storage was down")
		}
		if got := connect.CodeOf(err); got != connect.CodeUnavailable {
			t.Fatalf("GetRun failed with %s, want Unavailable", got)
		}
	})

	t.Run("live run with a degraded reader answer is served, not lost", func(t *testing.T) {
		// After the engine fix a live run whose snapshot is gone comes back
		// from the reader as a degraded run rather than an error; the handler
		// must serve it as an ordinary answer instead of CodeNotFound.
		run, _ := foreignRun()
		run.Status = orkstackv1.Status_STATUS_RUNNING
		run.FinishedAt = nil
		f := newReaderFixture(t, &fakeReader{
			runs: map[string]*orkstackv1.Run{run.GetId(): run},
		})

		got, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: run.GetId(),
		}))
		if err != nil {
			t.Fatalf("GetRun on the live degraded run: %v", err)
		}
		if got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_RUNNING {
			t.Fatalf("live run status = %s, want RUNNING", got.Msg.GetRun().GetStatus())
		}
	})
}

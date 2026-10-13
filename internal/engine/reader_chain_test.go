package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A chain fixture is a run that continued as new: closed generations, each
// with its history ending in CONTINUED_AS_NEW and its event segment in the
// store, and a current generation started from a carry that names them, a
// checkpoint and a switch snapshot. It is what a Stage 2 writer produces,
// built here so the readers can be proven before any writer exists.

const (
	chainRunID     = "ork-1700000000-7"
	chainNamespace = "team-a"
)

var chainCreatedAt = time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)

type chainSpec struct {
	// closed is the number of events each closed generation published.
	closed []int
	// current is the number of events the current generation published.
	current int
	// finish ends the current generation with RUN_SUCCEEDED and a close.
	finish bool
	// closeWith, when set, closes the current generation with this event
	// instead (finish publishes nothing then).
	closeWith *historypb.HistoryEvent
}

type chainFixture struct {
	t        *testing.T
	runID    string
	wf       *orkstackv1.Orkflow
	store    *memSnapshotStore
	client   *fakeReadClient
	reader   *TemporalRunReader
	events   []*orkstackv1.Event // every published event, in sequence order
	segments []*orkstackv1.EventSegmentRef
	carry    *orkstackv1.RunContinuation // the current generation's
	state    *orkstackv1.Run             // the run's state at the last switch
	current  string                      // the current generation's execution id
}

func chainExecution(generation int) string { return fmt.Sprintf("exec-%d", generation) }

// chainState is the run's exact state at a switch: a succeeded task, a
// fan-out whose item 0 succeeded after its last stride snapshot, and a
// pending task.
func chainState() *orkstackv1.Run {
	run := checkpointRunForTest()
	run.Id = chainRunID
	run.Namespace = chainNamespace
	run.OrkflowName = "pipeline"
	run.OrkflowVersion = "v1"
	run.CreatedAt = timestamppb.New(chainCreatedAt)
	run.StartedAt = timestamppb.New(chainCreatedAt.Add(time.Second))
	return run
}

func chainEventType(sequence int64) orkstackv1.EventType {
	switch sequence {
	case 1:
		return orkstackv1.EventType_EVENT_TYPE_RUN_CREATED
	case 2:
		return orkstackv1.EventType_EVENT_TYPE_RUN_STARTED
	}
	return orkstackv1.EventType_EVENT_TYPE_TASK_LOG
}

func newChain(t *testing.T, spec chainSpec) *chainFixture {
	t.Helper()
	f := &chainFixture{
		t:      t,
		runID:  chainRunID,
		wf:     &orkstackv1.Orkflow{Name: "pipeline", Version: "v1", Namespace: chainNamespace},
		store:  newMemSnapshotStore(),
		client: &fakeReadClient{generations: map[string]map[string]*fakeHistory{}, current: map[string]string{}},
		state:  chainState(),
	}
	f.client.generations[f.runID] = map[string]*fakeHistory{}
	f.reader = newRunReader(f.client, converter.GetDefaultDataConverter()).WithSnapshotStore(f.store)

	sequence := int64(0)
	var carry *orkstackv1.RunContinuation
	for generation := 0; generation <= len(spec.closed); generation++ {
		count := spec.current
		if generation < len(spec.closed) {
			count = spec.closed[generation]
		}
		history := []*historypb.HistoryEvent{f.started(carry)}
		first := sequence + 1
		var published []*orkstackv1.Event
		for i := 0; i < count; i++ {
			sequence++
			event := &orkstackv1.Event{
				RunId: f.runID, Sequence: sequence, Type: chainEventType(sequence),
				Status:    orkstackv1.Status_STATUS_RUNNING,
				Message:   fmt.Sprintf("event %d", sequence),
				Timestamp: timestamppb.New(chainCreatedAt.Add(time.Duration(sequence) * time.Second)),
			}
			published = append(published, event)
			history = append(history, f.publish(int64(len(history)+1), event))
		}
		f.events = append(f.events, published...)
		if generation == len(spec.closed) {
			f.carry = carry
			f.current = chainExecution(generation)
			if spec.finish {
				sequence++
				event := &orkstackv1.Event{RunId: f.runID, Sequence: sequence,
					Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED,
					Message: "run succeeded", Timestamp: timestamppb.New(chainCreatedAt.Add(time.Hour))}
				f.events = append(f.events, event)
				history = append(history, f.publish(int64(len(history)+1), event),
					closedEvent(int64(len(history)+2), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, chainCreatedAt.Add(time.Hour)))
			}
			if spec.closeWith != nil {
				history = append(history, spec.closeWith)
			}
			f.client.generations[f.runID][f.current] = &fakeHistory{events: history}
			f.client.current[f.runID] = f.current
			break
		}
		// This generation switches: its segment, the switch snapshot and the
		// checkpoint of the state, then the carry for the next one.
		segment := &orkstackv1.RunEventSegment{RunId: f.runID, Generation: int64(generation),
			FirstSequence: first, LastSequence: sequence, Events: published}
		data, obj := gzipObjectForTest(t, segment, chainNamespace, f.runID, int64(generation), artifacts.ContinuationKindEvents)
		f.store.put(obj.GetUri(), data)
		f.segments = append(f.segments, &orkstackv1.EventSegmentRef{Generation: int64(generation),
			ExecutionId: chainExecution(generation), FirstSequence: first, LastSequence: sequence,
			EventCount: int64(count), Object: obj})
		snapData, snapObj := gzipObjectForTest(t, f.state, chainNamespace, f.runID, int64(generation), artifacts.ContinuationKindSnapshot)
		f.store.put(snapObj.GetUri(), snapData)
		cpData, cpObj := checkpointObjectForTest(t, checkpointPartsForTest(f.state, 2), chainNamespace, f.runID, int64(generation))
		f.store.put(cpObj.GetUri(), cpData)
		carry = &orkstackv1.RunContinuation{
			Version: 1, Generation: int64(generation + 1), EventSequence: sequence,
			CreatedAt: f.state.GetCreatedAt(), StartedAt: f.state.GetStartedAt(),
			Checkpoint: cpObj, Snapshot: snapObj,
			Segments: proto.Clone(&orkstackv1.RunContinuation{Segments: f.segments}).(*orkstackv1.RunContinuation).GetSegments(),
		}
		history = append(history, &historypb.HistoryEvent{
			EventId:   int64(len(history) + 1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId: chainExecution(generation + 1),
				},
			},
		})
		f.client.generations[f.runID][chainExecution(generation)] = &fakeHistory{events: history}
	}
	return f
}

// started is a generation's WorkflowExecutionStarted: generation 0 with the
// four arguments every start passes, a later one with an empty run-id
// argument and the carry as the fifth.
func (f *chainFixture) started(carry *orkstackv1.RunContinuation) *historypb.HistoryEvent {
	at := chainCreatedAt
	args := []any{f.runID, f.wf, &orkstackv1.Payload{}, RunPlan{Namespace: chainNamespace}}
	if carry != nil {
		at = chainCreatedAt.Add(time.Duration(carry.GetGeneration()) * time.Minute)
		args = []any{"", f.wf, &orkstackv1.Payload{}, RunPlan{Namespace: chainNamespace}, carry}
	}
	return &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(at),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input:        payloadsOf(f.t, args...),
			},
		},
	}
}

// publish is a PublishEvent scheduling with the event's own {seq}.pb, whose
// object is the run's state with the event's status.
func (f *chainFixture) publish(eventID int64, event *orkstackv1.Event) *historypb.HistoryEvent {
	key, err := artifacts.RunSnapshotKey(chainNamespace, f.runID, event.GetSequence())
	if err != nil {
		f.t.Fatal(err)
	}
	uri := "s3://test/" + key
	run := proto.Clone(f.state).(*orkstackv1.Run)
	if isTerminalRunEvent(event) {
		run.Status = event.GetStatus()
		for _, row := range run.GetTaskRuns() {
			row.Status = orkstackv1.Status_STATUS_SUCCEEDED
			for _, item := range row.GetItems() {
				item.Status = orkstackv1.Status_STATUS_SUCCEEDED
			}
		}
	}
	f.store.put(uri, mustMarshalRun(f.t, run))
	return publishEventWithURI(f.t, eventID, event, uri)
}

// readFrom makes the fake answer an empty execution id with generation g, as
// a read that began before the chain switched would see it.
func (f *chainFixture) readFrom(generation int) {
	f.client.current[f.runID] = chainExecution(generation)
}

func (f *chainFixture) segmentReads() int {
	n := 0
	for _, uri := range f.store.reads {
		if strings.Contains(uri, "-events-") {
			n++
		}
	}
	return n
}

func sequencesOf(events []*orkstackv1.Event) []int64 {
	out := make([]int64, 0, len(events))
	for _, e := range events {
		out = append(out, e.GetSequence())
	}
	return out
}

func requireContiguous(t *testing.T, events []*orkstackv1.Event, from, to int64) {
	t.Helper()
	got := sequencesOf(events)
	if int64(len(got)) != to-from+1 {
		t.Fatalf("sequences = %v, want %d..%d", got, from, to)
	}
	for i, s := range got {
		if s != from+int64(i) {
			t.Fatalf("sequences = %v, want %d..%d with no gap or duplicate", got, from, to)
		}
	}
}

func TestTemporalRunReader_EventsSpanEveryGeneration(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 3, finish: true})
	events, err := f.reader.Events(context.Background(), f.runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 13)
	for i, event := range events {
		if !proto.Equal(event, f.events[i]) {
			t.Fatalf("event %d = %v, want %v", i+1, event, f.events[i])
		}
	}
	if got := f.client.historyExecutions; len(got) != 1 || got[0] != "" {
		t.Fatalf("history reads = %v, want only the current generation", got)
	}
	if f.segmentReads() != 2 {
		t.Fatalf("segment reads = %d, want one per closed generation", f.segmentReads())
	}
}

func TestTemporalRunReader_StreamFollowsTheSwitch(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 3, finish: true})
	// The stream began on generation 0, before the switch: its long poll
	// ends with CONTINUED_AS_NEW.
	f.readFrom(0)
	var got []*orkstackv1.Event
	err := f.reader.StreamEvents(context.Background(), f.runID, 0, func(e *orkstackv1.Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	requireContiguous(t, got, 1, 9)
	if got[len(got)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("stream ended on %v, want the terminal event", got[len(got)-1])
	}
	if want := []string{"", chainExecution(1)}; strings.Join(f.client.historyExecutions, ",") != strings.Join(want, ",") {
		t.Fatalf("history reads = %v, want %v", f.client.historyExecutions, want)
	}
	if !f.client.longPolled {
		t.Fatal("the successor was not long-polled")
	}
	// The successor's carry names generation 0's segment, but every one of
	// its events was already read from history: nothing is delivered twice.
	if f.segmentReads() > 1 {
		t.Fatalf("segment reads = %d", f.segmentReads())
	}
}

func TestTemporalRunReader_PlainReadFollowsASwitchDuringPagination(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 2}, current: 2})
	f.readFrom(1)
	events, err := f.reader.Events(context.Background(), f.runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 9)
	if want := []string{"", chainExecution(2)}; strings.Join(f.client.historyExecutions, ",") != strings.Join(want, ",") {
		t.Fatalf("history reads = %v, want %v", f.client.historyExecutions, want)
	}
}

func TestTemporalRunReader_ResumesFromASequenceInAnEarlierGeneration(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 3})
	events, err := f.reader.Events(context.Background(), f.runID, 3)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 4, 12)
	// From inside generation 1, generation 0's segment is never read.
	f2 := newChain(t, chainSpec{closed: []int{5, 4}, current: 3})
	var got []*orkstackv1.Event
	if err := f2.reader.StreamEvents(context.Background(), f2.runID, 7, func(e *orkstackv1.Event) error {
		got = append(got, e)
		if e.GetSequence() == 12 {
			return errStopStream
		}
		return nil
	}); !errors.Is(err, errStopStream) {
		t.Fatalf("StreamEvents: %v", err)
	}
	requireContiguous(t, got, 8, 12)
	if f2.segmentReads() != 1 {
		t.Fatalf("segment reads = %d, want only generation 1's", f2.segmentReads())
	}
}

var errStopStream = errors.New("test: stop the stream")

func TestTemporalRunReader_EmptySegmentIsSkipped(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 0}, current: 2, finish: true})
	events, err := f.reader.Events(context.Background(), f.runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 8)
	for _, uri := range f.store.reads {
		if strings.Contains(uri, "/000001-events-") {
			t.Fatalf("the empty segment %s was read", uri)
		}
	}
}

func TestTemporalRunReader_GetRunRightAfterASwitchReturnsTheCheckpoint(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 0})
	run, err := f.reader.GetRun(context.Background(), f.runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !proto.Equal(run.GetTaskRuns()[1], f.state.GetTaskRuns()[1]) || len(run.GetTaskRuns()) != 3 {
		t.Fatalf("GetRun rows = %v, want the checkpoint's", run.GetTaskRuns())
	}
	if !run.GetCreatedAt().AsTime().Equal(chainCreatedAt) || !run.GetStartedAt().AsTime().Equal(chainCreatedAt.Add(time.Second)) {
		t.Fatalf("created %v started %v, want the carried times", run.GetCreatedAt(), run.GetStartedAt())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("status = %v, want RUNNING", run.GetStatus())
	}
	if f.segmentReads() != 0 {
		t.Fatalf("GetRun read %d segments", f.segmentReads())
	}
	if len(f.store.reads) != 1 || !strings.Contains(f.store.reads[0], "-checkpoint-") {
		t.Fatalf("GetRun read %v, want exactly the checkpoint", f.store.reads)
	}
}

func TestTemporalRunReader_MissingCheckpointFallsBackToTheSwitchSnapshot(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 0})
	delete(f.store.objects, f.carry.GetCheckpoint().GetUri())
	run, err := f.reader.GetRun(context.Background(), f.runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	items := run.GetTaskRuns()[1].GetItems()
	if len(items) != 3 || items[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("fan-out items = %v, want item 0 SUCCEEDED from the switch snapshot", items)
	}
	if last := f.store.reads[len(f.store.reads)-1]; last != f.carry.GetSnapshot().GetUri() {
		t.Fatalf("last read %s, want the switch snapshot", last)
	}
}

func TestTemporalRunReader_GetRunNeverReadsSegments(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 3, finish: true})
	if _, err := f.reader.GetRun(context.Background(), f.runID); err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if f.segmentReads() != 0 {
		t.Fatalf("GetRun read %d segments", f.segmentReads())
	}
}

func TestTemporalRunReader_TerminatedGenerationSynthesizesAfterTheCarriedSequence(t *testing.T) {
	terminated := closedEvent(9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt.Add(time.Hour))
	f := newChain(t, chainSpec{closed: []int{5}, current: 0, closeWith: terminated})
	events, err := f.reader.Events(context.Background(), f.runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 6)
	if last := events[5]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("terminal = %v, want a synthesized RUN_CANCELED at 6", last)
	}
}

func TestTemporalRunReader_DeletedSuccessorIsNotFound(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 2})
	f.readFrom(0)
	delete(f.client.generations[f.runID], chainExecution(1))
	if _, err := f.reader.Events(context.Background(), f.runID, 0); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Events = %v, want ErrRunNotFound: an older generation is never served as the run", err)
	}
	if _, err := f.reader.GetRun(context.Background(), f.runID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("GetRun = %v, want ErrRunNotFound", err)
	}
}

func TestTemporalRunReader_SegmentHashMismatchIsUnavailable(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 2})
	uri := f.carry.GetSegments()[0].GetObject().GetUri()
	data, _ := f.store.get(uri)
	altered := append([]byte(nil), data...)
	altered[len(altered)/2] ^= 0xff
	f.store.put(uri, altered)
	_, err := f.reader.Events(context.Background(), f.runID, 0)
	if !errors.Is(err, ErrEventSegmentUnavailable) || errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Events = %v, want ErrEventSegmentUnavailable that is not NotFound", err)
	}
}

func TestTemporalRunReader_MissingSegmentIsUnavailableNotNotFound(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 2})
	delete(f.store.objects, f.carry.GetSegments()[0].GetObject().GetUri())
	_, err := f.reader.Events(context.Background(), f.runID, 0)
	if !errors.Is(err, ErrEventSegmentUnavailable) {
		t.Fatalf("Events = %v, want ErrEventSegmentUnavailable", err)
	}
	if errors.Is(err, artifacts.ErrNotFound) || errors.Is(err, ErrRunNotFound) {
		t.Fatalf("a missing segment of a live run reads as NotFound: %v", err)
	}
	// A transient store failure is unavailable but not lost.
	f2 := newChain(t, chainSpec{closed: []int{5}, current: 2})
	f2.store.fail[f2.carry.GetSegments()[0].GetObject().GetUri()] = errors.New("store is down")
	_, err = f2.reader.Events(context.Background(), f2.runID, 0)
	if !errors.Is(err, ErrEventSegmentUnavailable) || errors.Is(err, ErrEventSegmentLost) {
		t.Fatalf("transient failure = %v, want unavailable and not lost", err)
	}
}

func TestTemporalRunReader_LostSegmentWrapsErrEventSegmentLost(t *testing.T) {
	for name, damage := range map[string]func(f *chainFixture){
		"missing": func(f *chainFixture) { delete(f.store.objects, f.carry.GetSegments()[0].GetObject().GetUri()) },
		"altered": func(f *chainFixture) {
			uri := f.carry.GetSegments()[0].GetObject().GetUri()
			data, _ := f.store.get(uri)
			f.store.put(uri, append(append([]byte(nil), data...), 0))
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newChain(t, chainSpec{closed: []int{5}, current: 2})
			damage(f)
			_, err := f.reader.Events(context.Background(), f.runID, 0)
			if !errors.Is(err, ErrEventSegmentLost) || !errors.Is(err, ErrEventSegmentUnavailable) {
				t.Fatalf("Events = %v, want ErrEventSegmentLost (an ErrEventSegmentUnavailable)", err)
			}
		})
	}
}

func TestTemporalRunReader_CarryNamingAnotherRunsObjectIsRefusedBeforeAnyRead(t *testing.T) {
	retarget := func(uri string) string {
		return strings.Replace(uri, "/"+chainRunID+"/", "/ork-1700000000-8/", 1)
	}
	for name, mutate := range map[string]func(c *orkstackv1.RunContinuation){
		"checkpoint": func(c *orkstackv1.RunContinuation) { c.Checkpoint.Uri = retarget(c.Checkpoint.Uri) },
		"segment": func(c *orkstackv1.RunContinuation) {
			c.Segments[0].Object.Uri = retarget(c.Segments[0].Object.Uri)
		},
		"snapshot": func(c *orkstackv1.RunContinuation) { c.Snapshot.Uri = retarget(c.Snapshot.Uri) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newChain(t, chainSpec{closed: []int{5}, current: 2})
			carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
			mutate(carry)
			history := f.client.generations[f.runID][f.current]
			history.events[0] = f.started(carry)
			if _, err := f.reader.Events(context.Background(), f.runID, 0); !errors.Is(err, ErrEventSegmentUnavailable) ||
				errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("Events = %v, want ErrEventSegmentUnavailable", err)
			}
			if _, err := f.reader.GetRun(context.Background(), f.runID); err == nil || errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("GetRun = %v, want an error that is not NotFound", err)
			}
			if n := f.store.readCount(); n != 0 {
				t.Fatalf("the store was read %d times: %v", n, f.store.reads)
			}
		})
	}
}

func TestTemporalRunReader_CheckpointHashMismatchIsAnError(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 0})
	uri := f.carry.GetCheckpoint().GetUri()
	data, _ := f.store.get(uri)
	altered := append([]byte(nil), data...)
	altered[len(altered)-2] ^= 0xff
	f.store.put(uri, altered)
	_, err := f.reader.GetRun(context.Background(), f.runID)
	if !errors.Is(err, ErrContinuationObjectInvalid) || errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetRun = %v, want ErrContinuationObjectInvalid, never NotFound or a degraded answer", err)
	}
}

func TestTemporalRunReader_SwitchSnapshotHashMismatchIsAnError(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 0})
	delete(f.store.objects, f.carry.GetCheckpoint().GetUri())
	uri := f.carry.GetSnapshot().GetUri()
	data, _ := f.store.get(uri)
	f.store.put(uri, append(append([]byte(nil), data...), 1))
	_, err := f.reader.GetRun(context.Background(), f.runID)
	if !errors.Is(err, ErrContinuationObjectInvalid) || errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetRun = %v, want ErrContinuationObjectInvalid", err)
	}
}

func TestTemporalRunReader_RunStartReadsTheCurrentGeneration(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 2})
	record, err := f.reader.RunStart(context.Background(), f.runID)
	if err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if record.Orkflow.GetName() != "pipeline" || record.Orkflow.GetNamespace() != chainNamespace ||
		record.Plan.Namespace != chainNamespace {
		t.Fatalf("RunStart = %v %v, want the original arguments", record.Orkflow, record.Plan)
	}
	// The current generation's start: payload 0 is empty and it read one
	// history page of the current execution.
	if got := f.client.historyExecutions; len(got) != 1 || got[0] != "" {
		t.Fatalf("history reads = %v", got)
	}
	payloads := f.client.generations[f.runID][f.current].events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	var first string
	if err := converter.GetDefaultDataConverter().FromPayload(payloads[0], &first); err != nil || first != "" || len(payloads) != 5 {
		t.Fatalf("the current start's payload 0 = %q (%v), %d payloads; want empty, 5", first, err, len(payloads))
	}
}

func TestClosedExecution_MapsTheClusterHistoryLimitTermination(t *testing.T) {
	for _, reason := range []string{"Workflow history count exceeds limit.", "Workflow history size exceeds limit.", "WORKFLOW HISTORY COUNT EXCEEDS LIMIT"} {
		event := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
				WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{Reason: reason}}}
		status, failure, closed := closedExecution(event, false)
		if !closed || status != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != RunHistoryLimitCode ||
			failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
			!strings.HasSuffix(failure.GetMessage(), reason) {
			t.Fatalf("termination %q = %v %v, want FAILED RUN_HISTORY_LIMIT", reason, status, failure)
		}
	}
	// An operator's termination is still RUN_TERMINATED.
	event := closedEvent(9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt)
	if status, failure, _ := closedExecution(event, true); status != orkstackv1.Status_STATUS_CANCELED || failure.GetCode() != "RUN_TERMINATED" {
		t.Fatalf("operator termination = %v %v, want CANCELED RUN_TERMINATED", status, failure)
	}
}

func failedClose(failureType string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: "run id is required",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: failureType}}}}}}
}

func TestClosedExecution_CarriedInvalidRunIsStateUnsupported(t *testing.T) {
	status, failure, _ := closedExecution(failedClose("INVALID_RUN"), true)
	if status != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != RunStateUnsupportedCode ||
		failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		!strings.Contains(failure.GetMessage(), "roll the engines forward") {
		t.Fatalf("carried INVALID_RUN = %v %v, want RUN_STATE_UNSUPPORTED", status, failure)
	}
	if _, failure, _ := closedExecution(failedClose("INVALID_RUN"), false); failure.GetCode() != "RUN_FAILED" {
		t.Fatalf("uncarried INVALID_RUN = %v, want RUN_FAILED", failure)
	}
	for _, code := range []string{RunStateUnsupportedCode, RunStateCorruptCode, RunStateUnavailableCode} {
		if _, failure, _ := closedExecution(failedClose(code), true); failure.GetCode() != code ||
			failure.GetCategory() != runStateCategory(code) {
			t.Fatalf("%s close = %v", code, failure)
		}
	}
}

// TestTemporalRunReader_CarriedInvalidRunReadsAsStateUnsupported: the
// rollback shape end to end — a release older than generations ended a
// carried generation INVALID_RUN with no terminal event.
func TestTemporalRunReader_CarriedInvalidRunReadsAsStateUnsupported(t *testing.T) {
	failed := failedClose("INVALID_RUN")
	failed.EventId = 9
	failed.EventTime = timestamppb.New(chainCreatedAt.Add(time.Hour))
	f := newChain(t, chainSpec{closed: []int{5}, current: 0, closeWith: failed})
	events, err := f.reader.Events(context.Background(), f.runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 6)
	if last := events[5]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED ||
		!strings.Contains(last.GetMessage(), "carried forward by a newer release") {
		t.Fatalf("terminal = %v", last)
	}
}

func TestLiveAttempts_ReadsTheDescribedExecution(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 2})
	f.client.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{f.runID: {
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: f.runID, RunId: f.current},
		},
		PendingActivities: []*workflowpb.PendingActivityInfo{{
			ActivityId: "9", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
		}},
	}}
	// A switch lands between the describe and the history read: an empty
	// execution id would now select a newer generation.
	f.client.current[f.runID] = "exec-newer"
	_, err := f.reader.LiveAttempts(context.Background(), f.runID, chainCreatedAt)
	if got := f.client.historyExecutions; len(got) != 1 || got[0] != f.current {
		t.Fatalf("history reads = %v, want the described execution %s", got, f.current)
	}
	// The described execution's history has no ExecuteTask: the invariant
	// error names the activity, never a different generation's row.
	if err == nil || !strings.Contains(err.Error(), "has no scheduling event") {
		t.Fatalf("LiveAttempts = %v", err)
	}
}

func TestTemporalRunReader_EventsForArchiveRecordsALostSegmentAsAGap(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 2, finish: true})
	delete(f.store.objects, f.carry.GetSegments()[0].GetObject().GetUri())
	events, gaps, err := f.reader.EventsForArchive(context.Background(), f.runID)
	if err != nil {
		t.Fatalf("EventsForArchive: %v", err)
	}
	if len(gaps) != 1 || gaps[0].First != 1 || gaps[0].Last != 5 || gaps[0].Reason != "segment object missing" {
		t.Fatalf("gaps = %v, want 1–5 (segment object missing)", gaps)
	}
	requireContiguous(t, events, 6, 12)
	// A transient failure is still an error, so the archive write retries.
	f2 := newChain(t, chainSpec{closed: []int{5}, current: 1, finish: true})
	f2.store.fail[f2.carry.GetSegments()[0].GetObject().GetUri()] = errors.New("store is down")
	if _, _, err := f2.reader.EventsForArchive(context.Background(), f2.runID); !errors.Is(err, ErrEventSegmentUnavailable) {
		t.Fatalf("transient failure = %v, want an error", err)
	}
}

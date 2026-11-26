package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeEndings are a run's earlier endings, by scope.
type fakeEndings struct {
	events []*orkstackv1.Event
	gaps   []EventGap
	err    error
	asked  []uint64
	// marked are the runs a resume marked.
	marked []string
	// resumes are what the earlier ending lists.
	resumes []*orkstackv1.RunResume
	// run is the run as the earlier ending left it.
	run *orkstackv1.Run
}

func (f *fakeEndings) EarlierRun(context.Context, string, uint64) (*orkstackv1.Run, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.run == nil {
		return &orkstackv1.Run{Id: "ork-r", OrkflowName: "wf", Status: orkstackv1.Status_STATUS_FAILED}, nil
	}
	return f.run, nil
}

func (f *fakeEndings) EarlierResumes(context.Context, string, uint64) ([]*orkstackv1.RunResume, error) {
	return f.resumes, f.err
}

func (f *fakeEndings) ResumedRuns(context.Context) (map[string]bool, error) {
	out := map[string]bool{}
	for _, id := range f.marked {
		out[id] = true
	}
	return out, f.err
}

func (f *fakeEndings) EarlierEnding(_ context.Context, _ string, before uint64) ([]*orkstackv1.Event, []EventGap, error) {
	f.asked = append(f.asked, before)
	return f.events, f.gaps, f.err
}

func setAsideEvent(seq int64, typ orkstackv1.EventType, status orkstackv1.Status) *orkstackv1.Event {
	return &orkstackv1.Event{RunId: "ork-r", Sequence: seq, Type: typ, Status: status,
		Timestamp: timestamppb.New(readerEpoch.Add(time.Duration(seq) * time.Second))}
}

// setAside are the events of an ending a resume set aside: 1 to last, the last
// of them the run's failure, without those in skip.
func setAside(last int64, skip ...int64) []*orkstackv1.Event {
	var out []*orkstackv1.Event
next:
	for seq := int64(1); seq <= last; seq++ {
		for _, s := range skip {
			if s == seq {
				continue next
			}
		}
		if seq == last {
			out = append(out, setAsideEvent(seq, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED))
			continue
		}
		out = append(out, setAsideEvent(seq, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_RUNNING))
	}
	return out
}

// resumedHistory is the history of a run resumed under scope: a prefix that
// published 1 and 2, the reset, then own events, then the close.
func resumedHistory(t *testing.T, scope uint64, close enumspb.EventType, own ...*orkstackv1.Event) (*TemporalRunReader, *countingSnapshotStore) {
	t.Helper()
	store := &countingSnapshotStore{objects: map[string][]byte{}}
	history := []*historypb.HistoryEvent{startedEvent(t, "ork-r", &orkstackv1.Orkflow{Name: "wf", Version: "1"})}
	publish := func(e *orkstackv1.Event) {
		uri := snapshotURIFor("ork-r", e.GetSequence())
		store.objects[uri] = mustMarshalRun(t, &orkstackv1.Run{Id: "ork-r", OrkflowName: "wf", Status: e.GetStatus()})
		history = append(history, publishEventWithURI(t, int64(len(history)+1), e, uri))
	}
	for _, e := range setAside(6)[:2] {
		publish(e)
	}
	key := ResumePlanKey{Scope: scope, PlanDigest: strings.Repeat("a", 64)}
	history = append(history, &historypb.HistoryEvent{
		EventId: int64(len(history) + 1), EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		EventTime: timestamppb.New(readerEpoch.Add(10 * time.Second)),
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, BaseRunId: "exec-1", NewRunId: "exec-2",
				Failure: &failurepb.Failure{Message: key.String()},
			}},
	})
	for _, e := range own {
		publish(e)
	}
	history = append(history, closedEvent(int64(len(history)+1), close, readerEpoch.Add(time.Minute)))
	reader, _ := readerOver(history, "ork-r")
	return reader.WithSnapshotStore(store), store
}

// carriedOn are the events of a run resumed under scope from an ending whose
// last event was after, that then succeeded.
func carriedOn(scope, after int64) []*orkstackv1.Event {
	resumed := setAsideEvent(scope, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, orkstackv1.Status_STATUS_RUNNING)
	resumed.Resumed = &orkstackv1.RunResumed{ResumedAfterSequence: after}
	return []*orkstackv1.Event{
		resumed,
		setAsideEvent(scope+1, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT, orkstackv1.Status_STATUS_RUNNING),
		setAsideEvent(scope+2, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED),
	}
}

// TestResumedRun_ALiveReadIsTheWholeStream: a resumed run's stream holds what
// it published before the resume, the ending set aside included, read from
// that ending's body because the run's own history no longer holds it.
func TestResumedRun_ALiveReadIsTheWholeStream(t *testing.T) {
	const completed, failed = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	for name, tc := range map[string]struct {
		scope  uint64
		close  enumspb.EventType
		own    []*orkstackv1.Event
		base   []*orkstackv1.Event
		gaps   []EventGap
		from   int64
		served string
		asked  int
	}{
		"from the start": {scope: 7, close: completed, own: carriedOn(7, 6), base: setAside(6),
			served: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 4:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", asked: 1},
		"from inside it": {scope: 7, close: completed, own: carriedOn(7, 6), base: setAside(6), from: 4,
			served: "[5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", asked: 1},
		// A client that saw the ending goes on from it and reads no body.
		"from the ending": {scope: 7, close: completed, own: carriedOn(7, 6), base: setAside(6), from: 6,
			served: "[7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]"},
		// An accepted gap is a jump right before RUN_RESUMED, not a gap event.
		"a jump": {scope: 20, close: completed, own: carriedOn(20, 6), base: setAside(6), from: 5,
			served: "[6:RUN_FAILED 20:RUN_RESUMED 21:RUN_RESULTS_KEPT 22:RUN_SUCCEEDED]", asked: 1},
		"numbers the base never published": {scope: 7, close: completed, own: carriedOn(7, 6), base: setAside(6, 4), from: 2,
			gaps:   []EventGap{{First: 4, Last: 4, Reason: UnpublishedEventsReason}},
			served: "[3:TASK_LOG 4:STREAM_GAP 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", asked: 1},
		// A plan that could not run ends at its scope, with no RUN_RESUMED.
		"a plan that could not run": {scope: 7, close: failed, base: setAside(6), from: 5,
			own:    []*orkstackv1.Event{setAsideEvent(7, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED)},
			served: "[6:RUN_FAILED 7:RUN_FAILED]", asked: 1},
		// A reset that could not be read ends with no event of its own.
		"an ending with no event": {scope: 7, close: failed, base: setAside(6), from: 5, served: "[6:RUN_FAILED 7:RUN_FAILED]", asked: 1},
	} {
		t.Run(name, func(t *testing.T) {
			reader, _ := resumedHistory(t, tc.scope, tc.close, tc.own...)
			endings := &fakeEndings{events: tc.base, gaps: tc.gaps}
			reader = reader.WithEndings(endings)
			if got := eventTypesOf(liveReadsAgree(t, reader, "ork-r", tc.from)); got != tc.served {
				t.Fatalf("served %s\nwant   %s", got, tc.served)
			}
			if len(endings.asked) != 2*tc.asked { // the stream and the plain read
				t.Fatalf("asked %v, want %d each", endings.asked, tc.asked)
			}
		})
	}
}

// TestResumedRun_AnArchiveIsTheWholeStream: the archive's read names the
// ending's scope and holds the whole stream, with the earlier ending's own
// gaps as they were and an explicit one for what is lost.
func TestResumedRun_AnArchiveIsTheWholeStream(t *testing.T) {
	lost := func(first, last int64) string { return fmt.Sprintf("%d-%d lost", first, last) }
	for name, tc := range map[string]struct {
		base   []*orkstackv1.Event
		gaps   []EventGap
		err    error
		scope  uint64
		after  int64
		events string
		want   []string
	}{
		"whole": {base: setAside(6), scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 4:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]"},
		"its own gaps": {base: setAside(6, 4), gaps: []EventGap{{First: 4, Last: 4, Reason: "a segment is gone"}}, scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]",
			want:   []string{"4-4 a segment is gone"}},
		"its own gaps, cut to what is read of it": {base: setAside(6, 1, 2, 3, 6), scope: 20, after: 5,
			gaps:   []EventGap{{First: 1, Last: 3, Reason: "a segment is gone"}, {First: 6, Last: 30, Reason: "another"}},
			events: "[1:TASK_LOG 2:TASK_LOG 4:TASK_LOG 5:TASK_LOG 20:RUN_RESUMED 21:RUN_RESULTS_KEPT 22:RUN_SUCCEEDED]",
			want:   []string{"3-3 a segment is gone", "6-19 another"}},
		"a hole it says nothing of": {base: setAside(6, 4), scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]",
			want:   []string{lost(4, 4)}},
		"a jump is no gap": {base: setAside(6), scope: 20, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 4:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 20:RUN_RESUMED 21:RUN_RESULTS_KEPT 22:RUN_SUCCEEDED]"},
		"a body that is gone": {err: artifacts.ErrNotFound, scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", want: []string{lost(3, 6)}},
		"a body that does not decode": {err: errEndingDamaged, scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", want: []string{lost(3, 6)}},
		"a body that ends before the one set aside": {base: setAside(4), scope: 7, after: 6,
			events: "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 4:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]", want: []string{lost(5, 6)}},
	} {
		t.Run(name, func(t *testing.T) {
			reader, _ := resumedHistory(t, tc.scope, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(int64(tc.scope), tc.after)...)
			got, err := reader.WithEndings(&fakeEndings{events: tc.base, gaps: tc.gaps, err: tc.err}).EventsForArchive(context.Background(), "ork-r")
			if err != nil || got.Scope != tc.scope || eventTypesOf(got.Events) != tc.events {
				t.Fatalf("= scope %d, %s, %v\nwant %s", got.Scope, eventTypesOf(got.Events), err, tc.events)
			}
			var gaps []string
			for _, g := range got.Gaps {
				reason := g.Reason
				if strings.Contains(reason, "earlier ending") {
					reason = "lost"
				}
				gaps = append(gaps, fmt.Sprintf("%d-%d %s", g.First, g.Last, reason))
			}
			if !reflect.DeepEqual(gaps, tc.want) {
				t.Fatalf("gaps = %q, want %q", gaps, tc.want)
			}
		})
	}
}

// TestResumedRun_UnreadableEndings: an earlier ending that cannot be read now
// fails the read, so nothing is archived or served without it; one that is
// gone for good is said to be lost, never that the run does not exist.
func TestResumedRun_UnreadableEndings(t *testing.T) {
	ctx := context.Background()
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	down := reader.WithEndings(&fakeEndings{err: errors.New("connection refused")})
	for name, read := range map[string]func() error{
		"events":  func() error { _, err := down.Events(ctx, "ork-r", 0); return err },
		"stream":  func() error { return down.StreamEvents(ctx, "ork-r", 0, func(*orkstackv1.Event) error { return nil }) },
		"archive": func() error { _, err := down.EventsForArchive(ctx, "ork-r"); return err },
	} {
		if err := read(); !errors.Is(err, ErrEventSegmentUnavailable) || errors.Is(err, ErrEventSegmentLost) || errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("%s with the store down = %v, want unavailable", name, err)
		}
	}
	gone, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	for name, r := range map[string]*TemporalRunReader{"gone": gone.WithEndings(&fakeEndings{err: artifacts.ErrNotFound}), "no archive": gone.WithEndings(nil)} {
		if _, err := r.Events(ctx, "ork-r", 0); !errors.Is(err, ErrEventSegmentLost) || errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("%s = %v, want lost", name, err)
		}
	}
	// A run read needs no event, so it reads none of an ending.
	endings := &fakeEndings{}
	again, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	run, err := again.WithEndings(endings).GetRun(ctx, "ork-r")
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(endings.asked) != 0 {
		t.Fatalf("GetRun = %v, %v, asked %v", run, err, endings.asked)
	}
}

// TestResumedRun_TheSequenceOfAnEndingWithNoEvent: a run whose resume ended
// with no event ended at the plan's scope, as its stream says.
func TestResumedRun_TheSequenceOfAnEndingWithNoEvent(t *testing.T) {
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	run, sequence, err := reader.WithEndings(&fakeEndings{events: setAside(6)}).GetRunAndSequence(context.Background(), "ork-r")
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_FAILED || sequence != 7 {
		t.Fatalf("= %v at %d, %v; want FAILED at 7", run.GetStatus(), sequence, err)
	}
}

// TestResumedRun_AnEndingWithNoEventKeepsItsBasesRows: a resume that ended
// before it published anything, however it ended, ends at its scope, never at
// a number the ending set aside used, and reads as that ending left it: its
// history holds only the rows of the point the reset went back to.
func TestResumedRun_AnEndingWithNoEventKeepsItsBasesRows(t *testing.T) {
	ctx := context.Background()
	base := &orkstackv1.Run{Id: "ork-r", OrkflowName: "wf", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: "EXIT_1"},
		CanceledBy: "ann", FinishedAt: timestamppb.New(readerEpoch),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}, {TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED}}}
	for name, tc := range map[string]struct {
		close  enumspb.EventType
		status orkstackv1.Status
		last   orkstackv1.EventType
	}{
		"failed":     {enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, orkstackv1.Status_STATUS_FAILED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
		"canceled":   {enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, orkstackv1.Status_STATUS_CANCELED, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED},
		"terminated": {enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, orkstackv1.Status_STATUS_CANCELED, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED},
	} {
		t.Run(name, func(t *testing.T) {
			read := func(e *fakeEndings) *TemporalRunReader {
				reader, _ := resumedHistory(t, 7, tc.close)
				e.events = setAside(6)
				return reader.WithEndings(e)
			}
			ends := func(events []*orkstackv1.Event, where string) {
				t.Helper()
				if last := events[len(events)-1]; last.GetSequence() != 7 || last.GetType() != tc.last {
					t.Errorf("%s ends %v", where, last)
				}
			}
			run, sequence, err := read(&fakeEndings{run: base}).GetRunAndSequence(ctx, "ork-r")
			if err != nil || sequence != 7 || run.GetStatus() != tc.status || len(run.GetTaskRuns()) != 2 || run.GetError().GetCode() == "EXIT_1" ||
				run.GetCanceledBy() != "" || run.GetFinishedAt().AsTime().Equal(readerEpoch) {
				t.Fatalf("= %v at %d, %v", run, sequence, err)
			}
			events, err := read(&fakeEndings{run: base}).Events(ctx, "ork-r", 0)
			if err != nil {
				t.Fatal(err)
			}
			ends(events, "the stream")
			archived, got, err := read(&fakeEndings{run: base}).readForArchive(ctx, "ork-r", false)
			if err != nil || archived.GetStatus() != tc.status || len(archived.GetTaskRuns()) != 2 || archived.GetError().GetDetails() != "" || got.Scope != 7 {
				t.Fatalf("archived %v, %v", archived, err)
			}
			ends(got.Events, "the archive")

			if _, err := read(&fakeEndings{err: errors.New("connection refused")}).GetRun(ctx, "ork-r"); !errors.Is(err, ErrEventSegmentUnavailable) {
				t.Errorf("with the store down = %v", err)
			}
			// The run still reads where that ending is gone: how it ended, and
			// what is missing, with no task of another state.
			for name, lost := range map[string]error{"gone": artifacts.ErrNotFound, "damaged": errEndingDamaged} {
				alive, err := read(&fakeEndings{err: lost}).GetRun(ctx, "ork-r")
				if err != nil {
					t.Fatalf("with the earlier ending %s = %v", name, err)
				}
				archived, _, err := read(&fakeEndings{err: lost}).readForArchive(ctx, "ork-r", false)
				if err != nil {
					t.Fatalf("archived with the earlier ending %s = %v", name, err)
				}
				for where, run := range map[string]*orkstackv1.Run{"live": alive, "archived": archived} {
					says := run.GetError().GetMessage() + run.GetError().GetDetails()
					if run.GetStatus() != tc.status || len(run.GetTaskRuns()) != 0 || !strings.Contains(says, recordSetAsideSentence) {
						t.Errorf("%s with the earlier ending %s = %v", where, name, run)
					}
				}
			}
		})
	}
	if base.GetError().GetCode() != "EXIT_1" || base.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Error("the earlier ending's run was changed")
	}
}

// TestResumedRun_AResetByHandAfterAResume changes nothing of what the resume
// set aside.
func TestResumedRun_AResetByHandAfterAResume(t *testing.T) {
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
	h := reader.client.(*fakeReadClient).history["ork-r"]
	last := len(h.events) - 1
	h.events = append(h.events[:last:last], resetFrom(int64(last), "exec-1", "exec-2", "by hand"), h.events[last])
	_, sequence, err := reader.WithEndings(&fakeEndings{events: setAside(6)}).GetRunAndSequence(context.Background(), "ork-r")
	if err != nil || sequence != 7 {
		t.Fatalf("ends at %d, %v; want 7", sequence, err)
	}
}

// TestResumedRun_AReadPastWhatIsLost: a live read of events an earlier ending
// lost is an error that says so, and one already past them goes on.
func TestResumedRun_AReadPastWhatIsLost(t *testing.T) {
	ctx := context.Background()
	lost := &fakeEndings{events: setAside(6, 4, 5), gaps: []EventGap{{First: 4, Last: 5, Reason: "a segment is gone"}}}
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	reader = reader.WithEndings(lost)
	if _, err := reader.Events(ctx, "ork-r", 2); !errors.Is(err, ErrEventSegmentLost) {
		t.Fatalf("over it = %v", err)
	}
	// A stream is served what comes before it first.
	var sent []*orkstackv1.Event
	err := reader.StreamEvents(ctx, "ork-r", 2, func(e *orkstackv1.Event) error { sent = append(sent, e); return nil })
	if !errors.Is(err, ErrEventSegmentLost) || !strings.Contains(err.Error(), "a segment is gone") || eventTypesOf(sent) != "[3:TASK_LOG]" {
		t.Fatalf("a stream = %s, %v", eventTypesOf(sent), err)
	}
	if _, err := reader.Events(ctx, "ork-r", 4); !errors.Is(err, ErrEventSegmentLost) {
		t.Fatalf("from inside it = %v", err)
	}
	events, err := reader.Events(ctx, "ork-r", 5)
	if err != nil || eventTypesOf(events) != "[6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]" {
		t.Fatalf("past it = %s, %v", eventTypesOf(events), err)
	}
}

// TestResumedRun_AnArchiveIsNotWrittenBlind: a reader with no archive to read
// the earlier ending from archives no resumed run without it.
func TestResumedRun_AnArchiveIsNotWrittenBlind(t *testing.T) {
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	if _, _, err := reader.WithEndings(nil).readForArchive(context.Background(), "ork-r", false); err == nil {
		t.Fatal("archived with no archive to read")
	}
}

// TestResumedRun_NumbersNeverPublishedAtTheEnd: numbers the ending set aside
// used last without publishing an event are no loss of a live read.
func TestResumedRun_NumbersNeverPublishedAtTheEnd(t *testing.T) {
	reader, _ := resumedHistory(t, 9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(9, 8)...)
	events, err := reader.WithEndings(&fakeEndings{events: setAside(6), gaps: []EventGap{{First: 7, Last: 8, Reason: UnpublishedEventsReason}}}).
		Events(context.Background(), "ork-r", 5)
	if err != nil || eventTypesOf(events) != "[6:RUN_FAILED 9:RUN_RESUMED 10:RUN_RESULTS_KEPT 11:RUN_SUCCEEDED]" {
		t.Fatalf("= %s, %v", eventTypesOf(events), err)
	}
}

// TestTemporalRunReader_AResetWhosePlanKeyItCannotRead: a run reset under a
// reason shaped like a plan key this build does not read ends
// RESUME_PLAN_INVALID with no event of its own, as no later sequence is known
// to it. It reads as its base ended it: the base's tasks, the base's events
// and no more, and the reason it ended for.
func TestTemporalRunReader_AResetWhosePlanKeyItCannotRead(t *testing.T) {
	const runID = "ork-r"
	ctx := context.Background()
	base, snapshots := helloRun(t, runID)
	ended := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", OrkflowVersion: "0.1.0", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(readerEpoch), FinishedAt: timestamppb.New(readerEpoch.Add(10 * time.Second)),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}}}
	snapshots.objects[snapshotURIFor(runID, 4)] = mustMarshalRun(t, ended)
	base = append(base, closedEvent(int64(len(base)+1), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, readerEpoch.Add(10*time.Second)))
	reset := append(slices.Clone(base[:3]),
		&historypb.HistoryEvent{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
				BaseRunId: "exec-1", NewRunId: "exec-2", Failure: &failurepb.Failure{Message: "ork-resume:v9:10.000:" + strings.Repeat("a", 64)}}}},
		&historypb.HistoryEvent{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, EventTime: timestamppb.New(readerEpoch.Add(time.Hour)),
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: "a plan key this build does not read",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: ReasonResumePlanInvalid}}}}}})
	fake := &fakeReadClient{generations: map[string]map[string]*fakeHistory{runID: {"exec-1": {events: base}, "exec-2": {events: reset}}},
		current: map[string]string{runID: "exec-2"}}
	reader := newRunReader(fake, converter.GetDefaultDataConverter()).WithSnapshotStore(snapshots).WithEndings(&fakeEndings{err: artifacts.ErrNotFound})

	check := func(where string, run *orkstackv1.Run) {
		t.Helper()
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != ReasonResumePlanInvalid ||
			len(run.GetTaskRuns()) != 1 || run.GetTaskRuns()[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
			!run.GetFinishedAt().AsTime().Equal(readerEpoch.Add(time.Hour)) {
			t.Errorf("%s: %v", where, run)
		}
	}
	run, err := reader.GetRun(ctx, runID)
	must.OK(t, err)
	check("live", run)
	archived, read, err := reader.readForArchive(ctx, runID, false)
	must.OK(t, err)
	check("archived", archived)
	if !read.AsBase || read.Scope != 0 {
		t.Errorf("archived under %d, as its base: %v", read.Scope, read.AsBase)
	}
	events, err := reader.Events(ctx, runID, 0)
	must.OK(t, err)
	for _, got := range [][]*orkstackv1.Event{events, read.Events} {
		if seqs := sequencesOf(got); !slices.Equal(seqs, []int64{1, 2, 3, 4}) || got[3].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
			t.Errorf("its stream = %v; want the base's alone", seqs)
		}
	}
	// Where its base's record is lost, it says so, under its own reason.
	delete(snapshots.objects, snapshotURIFor(runID, 4))
	lost, _, err := reader.readForArchive(ctx, runID, false)
	must.OK(t, err)
	if lost.GetError().GetCode() != ReasonResumePlanInvalid || lost.GetError().GetDetails() == "" {
		t.Errorf("with its base's record lost: %v", lost.GetError())
	}
	snapshots.objects[snapshotURIFor(runID, 4)] = mustMarshalRun(t, ended)
	var streamed []int64
	must.OK(t, reader.StreamEvents(ctx, runID, 0, func(e *orkstackv1.Event) error { streamed = append(streamed, e.GetSequence()); return nil }))
	if !slices.Equal(streamed, []int64{1, 2, 3, 4}) {
		t.Errorf("streamed %v; want the base's alone, each once", streamed)
	}
}

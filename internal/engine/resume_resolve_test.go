package engine

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resolveBase is a base whose history after R opens awaits and receives
// resolutions, built the way RunWorkflow's commands land.
type resolveBase struct {
	*historyBuilder
	batch int64     // the workflow task the next commands are of
	now   time.Time // its start: workflow.Now while it runs
}

func newResolveBase(t *testing.T) *resolveBase {
	b := &resolveBase{historyBuilder: &historyBuilder{t: t}}
	gen0Prefix(b.historyBuilder)
	return b
}

// task completes a workflow task, whose batch the next commands are of.
func (b *resolveBase) task() {
	b.wft()
	b.now = b.events[b.wftSt-1].GetEventTime().AsTime()
	b.batch = b.wftCompleted()
}

// at is the time the next event is written at.
func (b *resolveBase) at() time.Time { return time.Unix(1_800_000_000+int64(len(b.events)), 0) }

// opens publishes id's TASK_AWAITING and starts its deadline timer, in this
// batch, and returns the deadline.
func (b *resolveBase) opens(id string, timeout time.Duration) time.Time {
	b.publishIn(orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, id, orkstackv1.Status_STATUS_AWAITING)
	return b.timer(timeout)
}

func (b *resolveBase) publishIn(typ orkstackv1.EventType, id string, status orkstackv1.Status) {
	sched := b.publish(typ, 10, id)
	ev := b.events[sched-1]
	ev.GetActivityTaskScheduledEventAttributes().WorkflowTaskCompletedEventId = b.batch
	// The published event carries its status.
	ev.GetActivityTaskScheduledEventAttributes().Input = b.payloads(&orkstackv1.Event{Type: typ, Sequence: 10, TaskId: id, Status: status}, "snap")
}

// timer starts a timer in this batch and returns when it fires: its
// workflow task's start, plus after.
func (b *resolveBase) timer(after time.Duration) time.Time {
	fire := b.now.Add(after)
	batch := b.batch
	b.add(enumspb.EVENT_TYPE_TIMER_STARTED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			WorkflowTaskCompletedEventId: batch, StartToFireTimeout: durationpb.New(after)}}
	})
	return fire
}

// fires fires the timer started at event started.
func (b *resolveBase) fires(started int64) {
	b.add(enumspb.EVENT_TYPE_TIMER_FIRED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{StartedEventId: started}}
	})
}

// cancels cancels the timer started at event started.
func (b *resolveBase) cancels(started int64) {
	b.add(enumspb.EVENT_TYPE_TIMER_CANCELED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{StartedEventId: started}}
	})
}

// signal receives a resolution Signal.
func (b *resolveBase) signal(sig *orkstackv1.AwaitSignal) int64 {
	in := b.payloads(sig)
	return b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: ResolveAwaitSignal, Input: in}}
	})
}

func completion(id, uri string) *orkstackv1.AwaitSignal {
	return &orkstackv1.AwaitSignal{TaskId: id, Values: &orkstackv1.ArtifactRef{Uri: uri, Sha256: "aa", SizeBytes: 11}, ValuesJson: []byte(`{"ok":true}`),
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "user:ada"}
}

// scanned is what the resolve scan finds of b, whose awaits' rows hold the
// deadlines given.
func (b *resolveBase) scanned(t *testing.T, deadlines map[string]time.Time) map[string]ResumeResolved {
	t.Helper()
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, nil)
	h, err := ScanResumeBase(b.events, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := &orkstackv1.Run{}
	for id, d := range deadlines {
		base.TaskRuns = append(base.TaskRuns, &orkstackv1.TaskRun{TaskId: id, Status: orkstackv1.Status_STATUS_AWAITING,
			ScheduledAt: timestamppb.New(time.Unix(1_800_000_000, 0)), Await: &orkstackv1.TaskAwait{Deadline: timestamppb.New(d)}})
	}
	return h.ResolutionsAfterR(base)
}

// TestResolutionsAfterR: the first resolution resolve() would have applied
// to an await the base left awaiting is found, and nothing else (design
// §25.7, §26.5).
func TestResolutionsAfterR(t *testing.T) {
	t.Run("a completion before its deadline", func(t *testing.T) {
		b := newResolveBase(t)
		b.task()
		d := b.opens("w", time.Hour)
		at := b.at()
		b.signal(completion("w", "s3://v/1"))
		got := b.scanned(t, map[string]time.Time{"w": d})["w"]
		row := got.Row
		if !got.Answer.Completed || got.Answer.Rejected || row.GetValuesRef().GetUri() != "s3://v/1" || row.GetValuesRef().GetSizeBytes() != 11 ||
			row.GetAwait().GetResolvedBy() != "user:ada" || row.GetAwait().GetResolvedByVerified() ||
			row.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API || !row.GetFinishedAt().AsTime().Equal(at) {
			t.Fatalf("resolved = %+v, row %v", got, row)
		}
		if why := recordedRowFault(kindAwait, cutRecordedRow(kindAwait, row)); why != "" {
			t.Fatalf("its row is no settled await's: %s", why)
		}
	})
	t.Run("a rejection is the first and wins", func(t *testing.T) {
		b := newResolveBase(t)
		b.task()
		d := b.opens("w", time.Hour)
		b.signal(&orkstackv1.AwaitSignal{TaskId: "w", Reject: true, Reason: "no"})
		b.signal(completion("w", "s3://v/1"))
		if got := b.scanned(t, map[string]time.Time{"w": d})["w"]; !got.Answer.Rejected || got.Answer.Completed || got.Reason != "no" {
			t.Fatalf("resolved = %+v, want the rejection", got)
		}
	})
	t.Run("a completion with no values is passed over", func(t *testing.T) {
		b := newResolveBase(t)
		b.task()
		d := b.opens("w", time.Hour)
		b.signal(&orkstackv1.AwaitSignal{TaskId: "w"})
		b.signal(completion("w", "s3://v/2"))
		if got := b.scanned(t, map[string]time.Time{"w": d})["w"]; !got.Answer.Completed || got.Row.GetValuesRef().GetUri() != "s3://v/2" {
			t.Fatalf("resolved = %+v, want the second", got)
		}
	})
	t.Run("a signal between the deadline and its timer firing", func(t *testing.T) {
		b := newResolveBase(t)
		b.task()
		d := b.opens("w", time.Second)
		started := int64(len(b.events))
		for b.at().Before(d.Add(2 * time.Second)) {
			b.add(enumspb.EVENT_TYPE_MARKER_RECORDED, nil)
		}
		b.signal(completion("w", "s3://v/1"))
		b.fires(started)
		if got := b.scanned(t, map[string]time.Time{"w": d})["w"]; !got.Answer.Completed {
			t.Fatalf("resolved = %+v, want it taken: resolve() takes it before the timer", got)
		}
	})
	nothing := map[string]func(*resolveBase) map[string]time.Time{
		"after its timer fired": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Second)
			b.fires(int64(len(b.events)))
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d}
		},
		"before it opened": func(b *resolveBase) map[string]time.Time {
			b.signal(completion("w", "s3://v/1"))
			b.task()
			return map[string]time.Time{"w": b.opens("w", time.Hour)}
		},
		"an await the run closed": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			b.signal(completion("w", "s3://v/1"))
			b.task()
			b.publishIn(orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, "w", orkstackv1.Status_STATUS_SUCCEEDED)
			return map[string]time.Time{"w": d}
		},
		"two awaits whose timers cannot be told apart": func(b *resolveBase) map[string]time.Time {
			b.task()
			b.publishIn(orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, "w", orkstackv1.Status_STATUS_AWAITING)
			b.publishIn(orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, "v", orkstackv1.Status_STATUS_AWAITING)
			d := b.timer(time.Hour)
			b.timer(time.Hour) // both from the task's start
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d, "v": d}
		},
		"after the run was asked to cancel": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, nil)
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d}
		},
		"after its timer was cancelled": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			started := int64(len(b.events))
			b.task()
			b.cancels(started)
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d}
		},
		"a Signal that does not decode": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			in := b.payloads("not a resolution")
			b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, func(ev *historypb.HistoryEvent) {
				ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: ResolveAwaitSignal, Input: in}}
			})
			return map[string]time.Time{"w": d}
		},
		"a Signal that does not decode, then one that does": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			in := b.payloads("not a resolution")
			b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, func(ev *historypb.HistoryEvent) {
				ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: ResolveAwaitSignal, Input: in}}
			})
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d}
		},
		"a completion that names nothing that resolved it": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			c := completion("w", "s3://v/1")
			c.Via = orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_UNSPECIFIED
			b.signal(c)
			b.signal(completion("w", "s3://v/2"))
			return map[string]time.Time{"w": d}
		},
		"a completion whose values name no digest": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			c := completion("w", "s3://v/1")
			c.Values.Sha256 = ""
			b.signal(c)
			b.signal(completion("w", "s3://v/2"))
			return map[string]time.Time{"w": d}
		},
		"a deadline no timer of its batch fires at": func(b *resolveBase) map[string]time.Time {
			b.task()
			d := b.opens("w", time.Hour)
			b.signal(completion("w", "s3://v/1"))
			return map[string]time.Time{"w": d.Add(time.Minute)}
		},
	}
	for name, build := range nothing {
		t.Run(name, func(t *testing.T) {
			b := newResolveBase(t)
			if got := b.scanned(t, build(b)); len(got) != 0 {
				t.Fatalf("resolved = %+v, want none", got)
			}
		})
	}
}

// TestResumePlan_AnAnswerAfterR: in a run without records, an await its base
// left awaiting with an answer received after R and never applied settles
// from that answer once its values object is found whole; a rejection, or
// values that are gone, makes it wait again (design §24.2, §28.H2).
func TestResumePlan_AnAnswerAfterR(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), "values.json", strings.NewReader(`{"ok":true}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := timestamppb.New(time.Unix(1_800_003_600, 0))
	answered := func(uri string) ResumeResolved {
		return ResumeResolved{Answer: ResolutionAnswer{Completed: true}, By: "via api by user:ada", At: time.Unix(1_800_000_024, 0), Row: &orkstackv1.TaskRun{TaskId: "w", Status: sOK,
			ScheduledAt: timestamppb.New(time.Unix(1_800_000_000, 0)), StartedAt: timestamppb.New(time.Unix(1_800_000_000, 0)),
			FinishedAt: timestamppb.New(time.Unix(1_800_000_100, 0)),
			ValuesRef:  &orkstackv1.ArtifactRef{Name: "values.json", Uri: uri, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes},
			Await:      &orkstackv1.TaskAwait{Deadline: deadline, ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, ResolvedBy: "user:ada"}}}
	}
	gone := strings.Replace(ref.URI, ref.SHA256[:8], "00000000", 1)
	for name, tc := range map[string]struct {
		resolved ResumeResolved
		upstream orkstackv1.Status // the await's need's status
		cat      ResumeCategory
		disp     ResumeDisposition
		says     string
	}{
		"a completion":                       {answered(ref.URI), sOK, CategorySettled, DispositionSettle, `answered "via api by user:ada" (asserted) at 2027-01-15T08:00:24Z, which the run had not applied`},
		"a completion whose values are gone": {answered(gone), sOK, CategoryWaitsAgain, DispositionWait, ""},
		"a rejection":                        {ResumeResolved{Answer: ResolutionAnswer{Rejected: true}, By: "via api by user:bob"}, sOK, CategoryRerunFailed, DispositionWait, `rejected "via api by user:bob" (asserted)`},
		"a completion of what runs again":    {answered(ref.URI), sFail, CategoryRerunDownstream, DispositionWait, "the answer is not used, and it waits again"},
	} {
		t.Run(name, func(t *testing.T) {
			w := prow("w", sWait)
			w.Await = &orkstackv1.TaskAwait{Deadline: deadline}
			f := newPlan([]*orkstackv1.Task{ptask("a"), awaitTask("w", "a"), ptask("after", "w"), ptask("x")},
				prow("a", tc.upstream), w, prow("after", sPend), prow("x", sFail))
			f.in.Definition.DefinitionDigest = "sha256:def"
			f.in.Facts.Resolved = map[string]ResumeResolved{"w": tc.resolved}
			vf := &verifyFixture{in: f.in, commits: artifacts.NewMemoryCommitStore(), store: store}
			if tc.upstream == sOK {
				vf.commit(t, nodeUnit("a"), nil) // so that a is kept
			}
			ver := vf.verify(t)
			f.in.Evidence = ver.Evidence
			plan := f.plan(t)
			if u := unitOf(t, plan, "w", -1); u.Category != tc.cat || u.Disposition != tc.disp {
				t.Fatalf("w = %v/%v, want %v/%v (evidence %+v)", u.Category, u.Disposition, tc.cat, tc.disp, f.in.Evidence)
			}
			// The dialog says who answered, as the Signal said, and what of it.
			if u := unitOf(t, plan, "w", -1); tc.says != "" && !strings.Contains(u.Detail, tc.says) {
				t.Fatalf("w's detail = %q, want it to say %q", u.Detail, tc.says)
			}
			if tc.disp != DispositionSettle {
				return
			}
			// The reset run settles it with the answer's row.
			b, err := BuildResumeBlock(ResumeBlockInput{Input: f.in, Plan: plan, Verification: ver, SeqBase: 12, Scope: 12})
			if err != nil {
				t.Fatal(err)
			}
			units, err := DecodeResumeUnits(f.in.Definition, b, false)
			if err != nil {
				t.Fatal(err)
			}
			settled := false
			for _, u := range units {
				if u.unit().Task == "w" {
					settled = u.Row.GetValuesRef().GetUri() == ref.URI && u.Row.GetAwait().GetResolvedBy() == "user:ada"
				}
			}
			if !settled {
				t.Fatalf("units = %v, want w settled with the answer's row", units)
			}
		})
	}
}

// TestSignalText: what a Signal says is quoted and cut to a line, at a rune.
func TestSignalText(t *testing.T) {
	long := strings.Repeat("é", 150) // 300 bytes
	for in, want := range map[string]string{
		"user:ada":     `"user:ada"`,
		"a\nb\x1b[31m": `"a\nb\x1b[31m"`,
		long:           strconv.Quote(strings.Repeat("é", 100) + "…"),
	} {
		if got := signalText(in); got != want {
			t.Errorf("signalText(%q) = %s, want %s", in, got, want)
		}
	}
}

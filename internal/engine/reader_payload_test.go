package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sealedHistory is a cluster holding one sealed run, ork-read: its memo and
// its whole history.
type sealedHistory struct {
	runReadClient
	key    *keyRunClient
	events []*historypb.HistoryEvent
}

func (c *sealedHistory) GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	return &eventsIterator{events: c.events}
}

type eventsIterator struct {
	events []*historypb.HistoryEvent
	next   int
}

func (i *eventsIterator) HasNext() bool { return i.next < len(i.events) }
func (i *eventsIterator) Next() (*historypb.HistoryEvent, error) {
	i.next++
	return i.events[i.next-1], nil
}

// sealedReaderFixture is a reader over a sealed run that was started with a
// canary input, published one event sealed under publish, and closed with
// close, in that order.
func sealedReaderFixture(t *testing.T, publish *payloadcrypt.Scope, close func(own *payloadcrypt.Scope) *failurepb.Failure) (*TemporalRunReader, *RunKeys, *sealedHistory) {
	t.Helper()
	keys, own, memo, args := readerFixture(t)
	if publish == nil {
		publish = own
	}
	wf := &orkstackv1.Orkflow{Name: "flow", Namespace: "default"}
	start := args(own, "ork-read", wf, &orkstackv1.Payload{}, RunPlan{})
	kc := &keyRunClient{memo: memo, start: start}
	at := timestamppb.New(time.Unix(1700000000, 0))
	h := &sealedHistory{key: kc, events: []*historypb.HistoryEvent{
		{EventId: 1, EventTime: at, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: start, Memo: memo}}},
		{EventId: 2, EventTime: at, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent},
				Input: args(publish, &orkstackv1.Event{RunId: "ork-read", Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
					TaskId: "build", Message: Canary}, "")}}},
	}}
	if close != nil {
		h.events = append(h.events, &historypb.HistoryEvent{EventId: 3, EventTime: at, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{Failure: close(own)}}})
	}
	rk := NewRunKeys(kc, keys)
	return newRunReader(h, nil), rk, h
}

// A sealed run reads through an engine that holds its key: its start, its
// events and its failure's own words.
func TestReader_OpensASealedRun(t *testing.T) {
	fc := payloadcrypt.NewFailureConverter(NewDataConverter(nil), true)
	r, rk, _ := sealedReaderFixture(t, nil, func(own *payloadcrypt.Scope) *failurepb.Failure {
		return fc.ErrorToFailure(payloadcrypt.WithKey(temporal.NewNonRetryableApplicationError("the task said "+Canary, RunTimedOutCode, nil), own, RunTimedOutCode))
	})
	r.WithPayloadKeys(rk)
	ctx := context.Background()
	events, err := r.Events(ctx, "ork-read", 0)
	must.OK(t, err)
	if len(events) != 2 || events[0].GetMessage() != Canary || !strings.Contains(events[1].GetMessage(), "the task said "+Canary) {
		t.Fatalf("events = %v, want the run's own words", events)
	}
	run, err := r.GetRun(ctx, "ork-read")
	must.OK(t, err)
	if run.GetOrkflowName() != "flow" || run.GetError().GetMessage() != "the task said "+Canary || run.GetError().GetCode() != RunTimedOutCode {
		t.Fatalf("run = %v, want its Orkflow and its failure's own words", run)
	}
	start, err := r.RunStart(ctx, "ork-read")
	must.OK(t, err)
	if start.Orkflow.GetName() != "flow" {
		t.Fatalf("start = %v", start)
	}
}

// With no key source a sealed run's events do not read, and say so; nothing
// of it is shown.
func TestReader_WithoutKeysShowsNothingOfASealedRun(t *testing.T) {
	r, _, _ := sealedReaderFixture(t, nil, nil)
	events, err := r.Events(context.Background(), "ork-read", 0)
	if err == nil || len(events) != 0 || strings.Contains(err.Error(), Canary) {
		t.Fatalf("events = %v, %v; want an error that quotes nothing", events, err)
	}
}

// A payload of another run, copied into this one's history, is not opened:
// whoever may read this run may not read that one through it.
func TestReader_OpensOnlyTheRunItWasAskedFor(t *testing.T) {
	_, own, _, _ := readerFixture(t)
	r, rk, _ := sealedReaderFixture(t, own.For(payloadcrypt.RunContext("ork-other")), nil)
	asked := &askedKeys{RunKeySource: rk}
	r.WithPayloadKeys(asked)
	events, err := r.Events(context.Background(), "ork-read", 0)
	if !errors.Is(err, payloadcrypt.ErrUnreadable) || len(events) != 0 {
		t.Fatalf("events = %v, %v; want ErrUnreadable", events, err)
	}
	for _, c := range asked.asked {
		if c.ID != "ork-read" {
			t.Fatalf("the key of %s was asked for while reading ork-read", c)
		}
	}
}

// A key that could not be had is an error of the read, never an empty run.
func TestReader_AKeyThatCannotBeHadIsAnError(t *testing.T) {
	r, _, _ := sealedReaderFixture(t, nil, nil)
	down := errors.New("the cluster is down")
	r.WithPayloadKeys(failingKeys{down})
	if events, err := r.Events(context.Background(), "ork-read", 0); !errors.Is(err, down) || len(events) != 0 {
		t.Fatalf("events = %v, %v; want the key source's error", events, err)
	}
}

type askedKeys struct {
	RunKeySource
	asked []payloadcrypt.Context
}

func (a *askedKeys) RunKeyFor(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error) {
	a.asked = append(a.asked, c)
	return a.RunKeySource.RunKeyFor(ctx, c)
}

type failingKeys struct{ err error }

func (f failingKeys) RunKeyFor(context.Context, payloadcrypt.Context) (RunKeyHeld, error) {
	return RunKeyHeld{}, f.err
}

// A close that holds nothing sealed asks for no key: an unsealed run's read
// costs what it did.
func TestReader_AnUnsealedCloseAsksForNoKey(t *testing.T) {
	r, rk, _ := sealedReaderFixture(t, payloadcrypt.Unsealed(), func(*payloadcrypt.Scope) *failurepb.Failure {
		return FailureConverter().ErrorToFailure(errors.New("boom"))
	})
	start := r.client.(*sealedHistory).events[0].GetWorkflowExecutionStartedEventAttributes()
	start.Input = nil
	delete(start.Memo.Fields, payloadcrypt.MemoField)
	asked := &askedKeys{RunKeySource: rk}
	r.WithPayloadKeys(asked)
	events, err := r.Events(context.Background(), "ork-read", 0)
	must.OK(t, err)
	if len(events) != 2 || len(asked.asked) != 0 {
		t.Fatalf("events %v, keys asked %v; want both events and no key", events, asked.asked)
	}
}

// What a read says of a run while its key cannot be had is that it could not
// be read: never a run with no definition, nor one that ended for another
// reason. Each read asks for the key once.
func TestReader_AKeyThatCannotBeHadNeverGivesALesserAnswer(t *testing.T) {
	down := errors.New("the cluster is down")
	sealedCancel := func(own *payloadcrypt.Scope) *historypb.HistoryEvent {
		dc := NewDataConverter(nil)
		details, err := dc.ToPayloads(payloadcrypt.Seal(own, RunSupersededCode), payloadcrypt.Seal(own, "replaced by "+Canary))
		must.OK(t, err)
		return &historypb.HistoryEvent{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
				WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{Details: details}}}
	}
	_, own, _, _ := readerFixture(t)
	for name, events := range map[string]func(h *sealedHistory) []*historypb.HistoryEvent{
		"a start alone": func(h *sealedHistory) []*historypb.HistoryEvent { return h.events[:1] },
		"a close whose start does not seal": func(h *sealedHistory) []*historypb.HistoryEvent {
			plain := &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}}}
			return []*historypb.HistoryEvent{plain, sealedCancel(own)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, _, h := sealedReaderFixture(t, nil, nil)
			h.events = events(h)
			asked := &askedKeys{RunKeySource: failingKeys{down}}
			r.WithPayloadKeys(asked)
			run, err := r.GetRun(context.Background(), "ork-read")
			if !errors.Is(err, down) || run != nil {
				t.Fatalf("GetRun = %v, %v; want the key source's error", run, err)
			}
			if len(asked.asked) != 1 {
				t.Fatalf("one read asked for the key %d times", len(asked.asked))
			}
		})
	}
	t.Run("with the key, a superseded run says so", func(t *testing.T) {
		r, rk, h := sealedReaderFixture(t, nil, nil)
		h.events = append(h.events[:1], sealedCancel(own))
		r.WithPayloadKeys(rk)
		run, err := r.GetRun(context.Background(), "ork-read")
		must.OK(t, err)
		if run.GetError().GetCode() != RunSupersededCode || !strings.Contains(run.GetError().GetMessage(), Canary) {
			t.Fatalf("run error = %v", run.GetError())
		}
	})
	t.Run("a keyless run's close shows its reason code", func(t *testing.T) {
		fc := payloadcrypt.NewFailureConverter(NewDataConverter(nil), true)
		r, _, _ := sealedReaderFixture(t, payloadcrypt.Unsealed(), func(own *payloadcrypt.Scope) *failurepb.Failure {
			return fc.ErrorToFailure(payloadcrypt.WithKey(errors.New(Canary), own, RunTimedOutCode))
		})
		r.WithPayloadKeys(failingKeys{payloadcrypt.ErrKeyless})
		run, err := r.GetRun(context.Background(), "ork-read")
		must.OK(t, err)
		if run.GetError().GetMessage() != RunTimedOutCode {
			t.Fatalf("run error = %v", run.GetError())
		}
	})
}

// A sealed start that does not open still names its run's namespace and
// Orkflow, from the memo a list reads them from: an empty namespace would
// read as the built-in one, and show the run to whoever may read that. One
// whose memo names none is an error.
func TestReader_AStartThatDoesNotOpenKeepsItsNamespace(t *testing.T) {
	dc := NewDataConverter(nil)
	memo := func(fields map[string]string) *commonpb.Memo {
		m := &commonpb.Memo{Fields: map[string]*commonpb.Payload{}}
		for k, v := range fields {
			p, err := dc.ToPayload(v)
			must.OK(t, err)
			m.Fields[k] = p
		}
		return m
	}
	for name, source := range map[string]RunKeySource{
		"a keyless run": failingKeys{payloadcrypt.ErrKeyless}, "a payload that does not open": failingKeys{payloadcrypt.ErrUnreadable}, "no key source": nil,
	} {
		t.Run(name, func(t *testing.T) {
			r, _, h := sealedReaderFixture(t, nil, nil)
			h.events = h.events[:1]
			if source != nil {
				r.WithPayloadKeys(source)
			}
			start := h.events[0].GetWorkflowExecutionStartedEventAttributes()
			start.Memo = memo(map[string]string{MemoNamespace: "payments", MemoOrkflowName: "flow", MemoOrkflowVersion: "7"})
			run, err := r.GetRun(context.Background(), "ork-read")
			must.OK(t, err)
			if run.GetNamespace() != "payments" || run.GetOrkflowName() != "flow" || run.GetOrkflowVersion() != "7" || run.GetInput() != nil {
				t.Fatalf("run = %v, want its memo's namespace and Orkflow, and no input", run)
			}
			for _, fields := range []map[string]string{
				{MemoOrkflowName: "flow"}, {MemoOrkflowName: "flow", MemoNamespace: ""},
			} {
				start.Memo = memo(fields)
				if run, err := r.GetRun(context.Background(), "ork-read"); err == nil || run != nil {
					t.Fatalf("a run whose namespace cannot be read (memo %v) = %v, %v; want an error", fields, run, err)
				}
			}
		})
	}
}

// The input of a run that may hold an identity, which its start's record
// carries, is opened like the rest of its start.
func TestReader_OpensASealedIdentityRunsInput(t *testing.T) {
	r, rk, h := sealedReaderFixture(t, nil, nil)
	r.WithPayloadKeys(rk)
	_, own, _, args := readerFixture(t)
	input, err := structpb.NewStruct(map[string]any{"who": Canary})
	must.OK(t, err)
	start := args(own, "ork-read", &orkstackv1.Orkflow{Name: "flow", Namespace: "default"}, &orkstackv1.Payload{Data: input},
		RunPlan{Identity: &IdentityPlan{Issuer: "https://issuer"}})
	h.key.start = start
	h.events[0].GetWorkflowExecutionStartedEventAttributes().Input = start
	got, err := r.RunStart(context.Background(), "ork-read")
	must.OK(t, err)
	if got.Input.GetData().AsMap()["who"] != Canary {
		t.Fatalf("the start's input reads as %v", got.Input)
	}
}

// The namespace a force cancel is decided by is the run's own, also where its
// key cannot be had or its start never opens: then its memo's.
func TestReader_ARunsNamespaceIsHadWithoutItsKey(t *testing.T) {
	memo := func(attrs *historypb.WorkflowExecutionStartedEventAttributes, namespace string) {
		p, err := NewDataConverter(nil).ToPayload(namespace)
		must.OK(t, err)
		attrs.Memo = proto.Clone(attrs.Memo).(*commonpb.Memo)
		attrs.Memo.Fields[MemoNamespace] = p
	}
	for name, tc := range map[string]struct {
		keys  func(rk *RunKeys) RunKeySource
		spoil func(*historypb.WorkflowExecutionStartedEventAttributes)
		want  string
	}{
		"opened":                   {keys: func(rk *RunKeys) RunKeySource { return rk }, want: "default"},
		"no key source":            {want: "default"},
		"a key that cannot be had": {keys: func(*RunKeys) RunKeySource { return failingKeys{errors.New("down")} }, want: "default"},
		"a keyless run":            {keys: func(rk *RunKeys) RunKeySource { return rk }, spoil: func(a *historypb.WorkflowExecutionStartedEventAttributes) { memo(a, "payments") }, want: "payments"},
		"a keyless run that names none": {keys: func(rk *RunKeys) RunKeySource { return rk }, spoil: func(a *historypb.WorkflowExecutionStartedEventAttributes) {
			memo(a, "")
			a.Input.Payloads = a.Input.Payloads[:2]
		}},
	} {
		t.Run(name, func(t *testing.T) {
			r, rk, h := sealedReaderFixture(t, nil, nil)
			if tc.keys != nil {
				r.WithPayloadKeys(tc.keys(rk))
			}
			attrs := proto.Clone(h.events[0].GetWorkflowExecutionStartedEventAttributes()).(*historypb.WorkflowExecutionStartedEventAttributes)
			if tc.spoil != nil {
				tc.spoil(attrs)
			}
			got, err := r.runNamespace(context.Background(), "ork-read", attrs)
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("= %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// silentKeys never answers.
type silentKeys struct{}

func (silentKeys) RunKeyFor(ctx context.Context, _ payloadcrypt.Context) (RunKeyHeld, error) {
	<-ctx.Done()
	return RunKeyHeld{}, ctx.Err()
}

// A key source that does not answer holds a force cancel's step for less than
// its attempt: the memo is reached.
func TestReader_ARunsNamespaceWaitsLittleForItsKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _, h := sealedReaderFixture(t, nil, nil)
		r.WithPayloadKeys(silentKeys{})
		begun := time.Now()
		got, err := r.runNamespace(context.Background(), "ork-read", h.events[0].GetWorkflowExecutionStartedEventAttributes())
		if err != nil || got != "default" {
			t.Fatalf("= %q, %v", got, err)
		}
		if waited := time.Since(begun); waited != runNamespaceKeyWait || waited >= DefaultForceCancelSettings().RecordTimeout {
			t.Fatalf("waited %s, want %s, under a step's attempt", waited, runNamespaceKeyWait)
		}
	})
}

// What started a run is never guessed: a key that could not be had is said,
// and a start that does not open is an error.
func TestReader_WhatStartedARunIsNotGuessed(t *testing.T) {
	r, rk, h := sealedReaderFixture(t, nil, nil)
	if _, _, err := r.WithPayloadKeys(failingKeys{errors.New("down")}).startedBy(context.Background(), "ork-read", h.events[0]); !errors.Is(err, errPayloadKeyUnavailable) {
		t.Fatalf("= %v, want errPayloadKeyUnavailable", err)
	}
	if _, _, err := r.WithPayloadKeys(failingKeys{payloadcrypt.ErrKeyless}).startedBy(context.Background(), "ork-read", h.events[0]); !errors.Is(err, payloadcrypt.ErrKeyless) {
		t.Fatalf("= %v, want ErrKeyless", err)
	}
	ns, _, err := r.WithPayloadKeys(rk).startedBy(context.Background(), "ork-read", h.events[0])
	if err != nil || ns != "default" {
		t.Fatalf("= %q, %v", ns, err)
	}
}

// A sealed start cut short of its arguments is a keyless run's like any other
// that is not the run's: never a start that was not an engine's, and never a
// run of the built-in namespace.
func TestReader_ASealedStartCutShortIsKeyless(t *testing.T) {
	ctx := context.Background()
	for n := 1; n < 4; n++ {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r, rk, h := sealedReaderFixture(t, nil, nil)
			r.WithPayloadKeys(rk)
			start := proto.Clone(h.events[0]).(*historypb.HistoryEvent)
			attrs := start.GetWorkflowExecutionStartedEventAttributes()
			attrs.Input.Payloads = attrs.Input.Payloads[:n]
			other, err := NewDataConverter(nil).ToPayload("payments")
			must.OK(t, err)
			attrs.Memo.Fields[MemoNamespace] = other
			h.events = []*historypb.HistoryEvent{start}
			if _, engineStart, err := r.startPlan(ctx, "ork-read", "r1"); engineStart || !errors.Is(err, payloadcrypt.ErrKeyless) {
				t.Errorf("startPlan = engine's %v, %v; want ErrKeyless", engineStart, err)
			}
			run, err := r.GetRun(ctx, "ork-read")
			must.OK(t, err)
			if run.GetNamespace() != "payments" {
				t.Errorf("run = %v, want its memo's namespace", run)
			}
		})
	}
}

// A run the cluster retired between a read of its history and the ask for its
// key is not found, which the API answers from the archive: never a key that
// could not be had.
func TestReader_ARunRetiredMeanwhileIsNotFound(t *testing.T) {
	r, _, _ := sealedReaderFixture(t, nil, nil)
	r.WithPayloadKeys(failingKeys{fmt.Errorf("describe run: %w", serviceerror.NewNotFound("gone"))})
	if run, err := r.GetRun(context.Background(), "ork-read"); !errors.Is(err, ErrRunNotFound) || run != nil {
		t.Fatalf("GetRun = %v, %v; want ErrRunNotFound", run, err)
	}
}

// One read asks for its run's key once, however many payloads it opens, and
// keeps the answer: a key that could not be had is not asked for again within
// it, and the next read asks anew.
func TestReader_OneReadAsksForItsKeyOnce(t *testing.T) {
	ctx := context.Background()
	r, rk, h := sealedReaderFixture(t, nil, nil)
	h.events = append(h.events, h.events[1], h.events[1])
	asked := &askedKeys{RunKeySource: rk}
	r.WithPayloadKeys(asked)
	for n := 1; n <= 2; n++ {
		_, err := r.Events(ctx, "ork-read", 0)
		must.OK(t, err)
		if len(asked.asked) != n {
			t.Fatalf("after %d reads the key was asked for %d times", n, len(asked.asked))
		}
	}
	// Its first answer is a failure: a second ask within the read would open.
	flaky := &flakyKeys{then: rk}
	p := (&TemporalRunReader{converter: DataConverter(), payloadKeys: flaky}).runConverter(ctx, "ork-read")
	start := h.events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	for range 2 {
		var wf *orkstackv1.Orkflow
		if err := p.FromPayload(start[1], &wf); !errors.Is(err, errPayloadKeyUnavailable) {
			t.Fatalf("FromPayload = %v, want the read's one answer", err)
		}
	}
	if flaky.asks != 1 {
		t.Fatalf("asked %d times", flaky.asks)
	}
}

// A caller that gave up is told so, and the next caller's read is its own.
func TestReader_ACanceledReadIsNotTheNextReads(t *testing.T) {
	r, rk, _ := sealedReaderFixture(t, nil, nil)
	r.WithPayloadKeys(ctxKeys{rk})
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Events(gone, "ork-read", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Events under a canceled caller = %v", err)
	}
	if events, err := r.Events(context.Background(), "ork-read", 0); err != nil || len(events) != 1 {
		t.Fatalf("the next read = %v, %v", events, err)
	}
}

type flakyKeys struct {
	then RunKeySource
	asks int
}

func (f *flakyKeys) RunKeyFor(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error) {
	f.asks++
	if f.asks == 1 {
		return RunKeyHeld{}, errors.New("a blip")
	}
	return f.then.RunKeyFor(ctx, c)
}

// ctxKeys answers as a source that reads the cluster does: not past its
// caller's end.
type ctxKeys struct{ RunKeySource }

func (k ctxKeys) RunKeyFor(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error) {
	if err := ctx.Err(); err != nil {
		return RunKeyHeld{}, err
	}
	return k.RunKeySource.RunKeyFor(ctx, c)
}

// A start that opens but names another namespace or Orkflow than its memo is
// a keyless run's (rule 6), at every read: also when its key is already held,
// which a copied start under another memo could otherwise use. It is shown as
// its memo's, with nothing of the start.
func TestReader_AStartThatIsNotItsMemosIsNotShown(t *testing.T) {
	dc := NewDataConverter(nil)
	for name, field := range map[string]string{"the namespace": MemoNamespace, "the Orkflow": MemoOrkflowName} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, rk, h := sealedReaderFixture(t, nil, nil)
			h.events = h.events[:1]
			r.WithPayloadKeys(rk)
			run, err := r.GetRun(ctx, "ork-read") // the key is held from here on
			must.OK(t, err)
			if run.GetNamespace() != "default" || run.GetOrkflowName() != "flow" {
				t.Fatalf("the run itself reads as %v", run)
			}
			start := h.events[0].GetWorkflowExecutionStartedEventAttributes()
			start.Memo = proto.Clone(start.Memo).(*commonpb.Memo)
			other, err := dc.ToPayload("other")
			must.OK(t, err)
			start.Memo.Fields[field] = other
			run, err = r.GetRun(ctx, "ork-read")
			must.OK(t, err)
			if run.GetInput() != nil || (field == MemoNamespace) != (run.GetNamespace() == "other") || (field == MemoOrkflowName) != (run.GetOrkflowName() == "other") {
				t.Fatalf("read as %v, want its memo's own words and nothing of the start", run)
			}
			if got, err := r.RunStart(ctx, "ork-read"); !errors.Is(err, payloadcrypt.ErrKeyless) {
				t.Fatalf("RunStart = %v, %v; want ErrKeyless", got, err)
			}
		})
	}
}

// Every read of an execution holds its start to rule 6 before anything of it
// is used, its carry included, also where the run's key is already held: a
// copied start under a memo of another namespace, and a definition in
// plaintext beside arguments that are sealed, are a keyless run's.
func TestReader_EveryReadHoldsTheStartFirst(t *testing.T) {
	ctx := context.Background()
	dc := NewDataConverter(nil)
	spoil := map[string]func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes){
		"another namespace in the memo": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			p, err := dc.ToPayload("other")
			must.OK(t, err)
			start.Memo.Fields[MemoNamespace] = p
		},
		"an unreadable namespace in the memo": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			start.Memo.Fields[MemoNamespace] = &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("{")}
		},
		"a definition in plaintext": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			p, err := dc.ToPayload(&orkstackv1.Orkflow{Name: "flow", Namespace: "default"})
			must.OK(t, err)
			start.Input.Payloads[1] = p
		},
		"another tree's key field": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			field, err := payloadcrypt.ParseKeyMemo(start.Memo.Fields[payloadcrypt.MemoField].GetData())
			must.OK(t, err)
			field.RootRunID = "ork-another"
			p, err := dc.ToPayload(field)
			must.OK(t, err)
			start.Memo.Fields[payloadcrypt.MemoField] = p
		},
		"an argument more": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			start.Input.Payloads = append(start.Input.Payloads, start.Input.Payloads[4])
		},
		"no key field": func(t *testing.T, start *historypb.WorkflowExecutionStartedEventAttributes) {
			delete(start.Memo.Fields, payloadcrypt.MemoField)
		},
	}
	for name, spoil := range spoil {
		t.Run(name, func(t *testing.T) {
			r, rk, h := sealedReaderFixture(t, nil, nil)
			r.WithPayloadKeys(rk)
			_, err := r.Events(ctx, "ork-read", 0) // the key is held from here on
			must.OK(t, err)
			start := proto.Clone(h.events[0]).(*historypb.HistoryEvent)
			h.events[0] = start
			attrs := start.GetWorkflowExecutionStartedEventAttributes()
			// The carry, as a continued execution's, sealed for the run like the rest.
			_, own, _, args := readerFixture(t)
			attrs.ContinuedExecutionRunId = "r0"
			attrs.Input.Payloads = append(attrs.Input.Payloads, args(own, &orkstackv1.RunContinuation{Generation: 1, EventSequence: 7}).Payloads...)
			spoil(t, attrs)
			store := &countingSnapshots{}
			r.WithSnapshotStore(store)
			for read, call := range map[string]func() error{
				"Events":    func() error { _, err := r.Events(ctx, "ork-read", 0); return err },
				"GetRun":    func() error { _, err := r.GetRun(ctx, "ork-read"); return err },
				"RunStart":  func() error { _, err := r.RunStart(ctx, "ork-read"); return err },
				"startPlan": func() error { _, _, err := r.startPlan(ctx, "ork-read", "r1"); return err },
				"carryOf":   func() error { _, err := r.carryOf(ctx, "ork-read", "r1"); return err },
				"live tasks": func() error {
					h.events = append(h.events, &historypb.HistoryEvent{EventId: 9, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: "a", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask}, Input: args(own, &orkstackv1.ExecuteTaskInput{TaskId: "build"})}}})
					_, err := r.scheduledTasks(ctx, r.runConverter(ctx, "ork-read"), "ork-read", "r1", map[string]struct{}{"a": {}})
					return err
				},
			} {
				if err := call(); !errors.Is(err, payloadcrypt.ErrKeyless) {
					t.Errorf("%s = %v, want ErrKeyless", read, err)
				}
			}
			if store.reads != 0 {
				t.Errorf("%d objects were read of a keyless run's carry", store.reads)
			}
		})
	}
}

// countingSnapshots is a snapshot store no keyless run's read may ask.
type countingSnapshots struct {
	SnapshotStore
	reads int
}

func (s *countingSnapshots) ReadSnapshot(context.Context, string) ([]byte, error) {
	s.reads++
	return nil, errors.New("not stored")
}

func (s *countingSnapshots) ReadContinuationRange(context.Context, string, int64, int64, int64) ([]byte, error) {
	s.reads++
	return nil, errors.New("not stored")
}

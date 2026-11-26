package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestConcurrencyReset_RequestTellsWhatItAsksApart: the digest of two requests
// differs where what they ask does, and never with when they were issued.
func TestConcurrencyReset_RequestTellsWhatItAsksApart(t *testing.T) {
	at := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	epoch := func(e int64) *int64 { return &e }
	base := ConcurrencyReset{Key: "k", RequestID: "a", IssuedAt: at}
	bodies := map[string]string{}
	for name, in := range map[string]ConcurrencyReset{
		"plain": base, "forced": {Key: "k", RequestID: "a", IssuedAt: at, ForceUnanswered: true},
		"epoch 0": {Key: "k", RequestID: "a", IssuedAt: at, ExpectedEpoch: epoch(0)},
		"epoch 1": {Key: "k", RequestID: "a", IssuedAt: at, ExpectedEpoch: epoch(1)},
	} {
		r := in.request()
		if r.ID != "a" || !r.IssuedAt.Equal(at) || len(r.Body) != resetBodyMax {
			t.Fatalf("%s: request %+v", name, r)
		}
		for other, body := range bodies {
			if body == r.Body {
				t.Errorf("%s and %s ask the same", name, other)
			}
		}
		bodies[name] = r.Body
	}
	later := base
	later.IssuedAt = at.Add(time.Minute)
	if later.request().Body != base.request().Body || later.request().same(base.request()) {
		t.Fatal("a request issued at another time is another request that asks the same")
	}
	again := base
	if resetStartID(base.request()) == resetStartID(later.request()) || resetStartID(base.request()) != resetStartID(again.request()) {
		t.Fatal("a start's id is the request's, and the same on every attempt")
	}
}

// TestResetReportKnown: how an applied reset stands is told from what the gate
// remembers, or from its memo when it does not answer; one it does not
// remember is not known, and another request under its id is refused.
func TestResetReportKnown(t *testing.T) {
	at := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	req := gateReset{ID: "a", IssuedAt: at, Body: "b"}
	done, other := req, req
	done.Done, other.Body = true, "c"
	state := func(current string, rebuild string, resets ...gateReset) gateLook {
		return gateLook{state: &gateState{Resets: resets, ResetID: current, Epoch: 4, Rebuild: gateRebuild{State: rebuild}}}
	}
	memo := func(open bool, m gateResetMemo) gateLook { return gateLook{open: open, memo: &m} }
	for name, c := range map[string]struct {
		look  gateLook
		want  string
		code  string
		note  bool
		known bool
	}{
		"not remembered":                {look: state("", "", gateReset{ID: "z", IssuedAt: at, Body: "b"})},
		"no gate, no memo":              {look: gateLook{}},
		"rebuilding":                    {look: state("a", gateRebuildListing, req), want: ResetStateRunning, known: true},
		"failing":                       {look: state("a", gateRebuildFailing, req), want: ResetStateFailed, code: ResetFailedCode, known: true},
		"done":                          {look: state("a", gateRebuildSettling, done), want: ResetStateCompleted, known: true},
		"done, then replaced":           {look: state("z", "", done), want: ResetStateCompleted, known: true},
		"replaced before it was done":   {look: state("z", gateRebuildListing, req), want: ResetStateInterrupted, code: ResetInterruptedCode, known: true},
		"its gate does not answer":      {look: memo(true, gateResetMemo{Current: "a", Resets: []gateReset{req}}), want: ResetStateRunning, note: true, known: true},
		"its gate closed before it did": {look: memo(false, gateResetMemo{Current: "a", Resets: []gateReset{req}}), want: ResetStateInterrupted, code: ResetInterruptedCode, known: true},
		"its gate closed, done":         {look: memo(false, gateResetMemo{Current: "a", Resets: []gateReset{done}}), want: ResetStateCompleted, known: true},
		"a later gate does not answer":  {look: memo(true, gateResetMemo{Resets: []gateReset{req}}), want: ResetStateInterrupted, code: ResetInterruptedCode, known: true},
	} {
		t.Run(name, func(t *testing.T) {
			res, known, err := reportKnown(c.look, req)
			must.OK(t, err)
			if known != c.known || res.State != c.want || res.Code != c.code || (res.Note != "") != c.note || (res.Code != "") != (res.Reason != "") {
				t.Fatalf("= %+v, known %v; want %q %q", res, known, c.want, c.code)
			}
		})
	}
	for name, look := range map[string]gateLook{"state": state("a", "", other), "memo": memo(false, gateResetMemo{Resets: []gateReset{other}})} {
		_, known, err := reportKnown(look, req)
		var refused *ConcurrencyResetError
		if !known || !errors.As(err, &refused) || refused.Code != ResetRefusedCode || refused.Retryable {
			t.Errorf("%s: another request under the id = %v, %v; want refused", name, known, err)
		}
	}
}

// TestResetTooLate: by this process's clock, a request no gate could take is
// refused, one whose first action did not come in time never acts, and one the
// limit may have applied and cannot know is never reported as not applied.
func TestResetTooLate(t *testing.T) {
	now := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	r := &TemporalConcurrencyResetter{settings: ConcurrencyResetSettings{Tunables: DefaultGateTunables()}, now: func() time.Time { return now }}
	ask := func(look gateLook, issued time.Time) (ConcurrencyResetResult, bool, error) {
		return r.tooLate(look, gateReset{ID: "a", IssuedAt: issued, Body: "b"})
	}
	if _, done, err := ask(gateLook{}, now.Add(-5*time.Minute)); done || err != nil {
		t.Fatalf("a request in time = %v, %v", done, err)
	}
	for _, issued := range []time.Time{now.Add(-24 * time.Hour), now.Add(resetClockAhead + time.Second), {}} {
		var refused *ConcurrencyResetError
		if _, _, err := ask(gateLook{}, issued); !errors.As(err, &refused) || refused.Code != ResetRefusedCode || refused.Retryable {
			t.Errorf("issued at %v = %v; want refused", issued, err)
		}
	}
	late := now.Add(-10 * time.Minute)
	if res, done, err := ask(gateLook{memo: &gateResetMemo{Resets: []gateReset{{ID: "z"}}}}, late); !done || err != nil || res.State != ResetStateNotApplied || res.Code != ResetNotAppliedCode {
		t.Fatalf("a first action after its time = %+v, %v, %v; want not_applied", res, done, err)
	}
	for name, look := range map[string]gateLook{
		"the gate":      {state: &gateState{ResetsUnknownUntil: now.Add(time.Hour)}},
		"the memo":      {memo: &gateResetMemo{UnknownUntil: now.Add(time.Hour)}},
		"an unreadable": {memo: &gateResetMemo{Unreadable: true}},
	} {
		var refused *ConcurrencyResetError
		if _, _, err := ask(look, late); !errors.As(err, &refused) || refused.Code != ResetRefusedCode {
			t.Errorf("%s cannot know = %v; want refused, never not_applied", name, err)
		}
	}
}

// resetClient is a cluster whose gate answers no query, with the execution
// described as status says. A gate that does not answer, or one that starts
// between two reads, is a race a real cluster does not produce on demand.
type resetClient struct {
	client.Client
	queryErr     error
	status       enumspb.WorkflowExecutionStatus
	closed       time.Time
	memo         *commonpb.Memo
	missing      bool
	describeErr  error
	terminated   []string
	terminateErr error
	startErr     error
	starts       []*workflowservice.StartWorkflowExecutionRequest
}

// startService keeps the starts made through a resetClient.
type startService struct {
	workflowservice.WorkflowServiceClient
	f *resetClient
}

func (f *resetClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return startService{f: f}
}

func (s startService) StartWorkflowExecution(_ context.Context, in *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	s.f.starts = append(s.f.starts, in)
	if s.f.startErr != nil {
		return nil, s.f.startErr
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "new", Started: true}, nil
}

// TestResetStartFresh_IsHeldForAsLongAsItMayBeRepeated: a start with no gate to
// freeze says by when it has to reach the cluster, and its memo holds the key
// for as long, so a forced reset does not end it meanwhile.
func TestResetStartFresh_IsHeldForAsLongAsItMayBeRepeated(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := &resetClient{queryErr: serviceerror.NewNotFound("none"), missing: true}
	r := newTestResetter(f)
	r.now = func() time.Time { return now }
	in := ConcurrencyReset{Key: "k", RequestID: "a", IssuedAt: now}
	_, err := r.ResetConcurrencyLimit(context.Background(), in)
	must.OK(t, err)
	if len(f.starts) != 1 {
		t.Fatalf("%d starts, want one", len(f.starts))
	}
	var start GateStart
	must.OK(t, DataConverter().FromPayloads(f.starts[0].GetInput(), &start))
	by := now.Add(DefaultGateTunables().FreezeTimeout)
	if !start.Reset.StartBy.Equal(by) || !start.Reset.FrozenUntil.IsZero() {
		t.Fatalf("started by %v, frozen until %v; want %v and no freeze", start.Reset.StartBy, start.Reset.FrozenUntil, by)
	}
	silent := &resetClient{queryErr: serviceerror.NewDeadlineExceeded("no worker"), status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, memo: f.starts[0].GetMemo()}
	r = newTestResetter(silent)
	force := ConcurrencyReset{Key: "k", RequestID: "b", IssuedAt: now, ForceUnanswered: true}
	for _, c := range []struct {
		after   time.Duration
		refused bool
	}{{time.Minute + 19*time.Second, true}, {time.Minute + 21*time.Second, false}} {
		r.now = func() time.Time { return now.Add(c.after) }
		_, err := r.ResetConcurrencyLimit(context.Background(), force)
		var busy *ConcurrencyResetError
		if refused := errors.As(err, &busy) && busy.Retryable; refused != c.refused || (len(silent.terminated) == 0) != c.refused {
			t.Fatalf("a forced reset %v later = %v, ended %v; want refused %v", c.after, err, silent.terminated, c.refused)
		}
	}
}

func (f *resetClient) QueryWorkflowWithOptions(context.Context, *client.QueryWorkflowWithOptionsRequest) (*client.QueryWorkflowWithOptionsResponse, error) {
	return nil, f.queryErr
}

func (f *resetClient) DescribeWorkflow(context.Context, string, string) (*client.WorkflowExecutionDescription, error) {
	switch {
	case f.describeErr != nil:
		return nil, f.describeErr
	case f.missing:
		return nil, serviceerror.NewNotFound("none")
	}
	resp := &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: f.status, Memo: f.memo}}
	if !f.closed.IsZero() {
		resp.WorkflowExecutionInfo.CloseTime = timestamppb.New(f.closed)
	}
	return &client.WorkflowExecutionDescription{RawResponse: resp}, nil
}

func (f *resetClient) ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	held, err := converter.GetDefaultDataConverter().ToPayload(gateMemoEntry{Proto: 1, Held: true, Limit: 3, Strategy: gateStrategyQueue, ArrivedAt: time.Unix(0, 0), SeqBase: 1})
	if err != nil {
		return nil, err
	}
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoKey(GateID("default", "", "k"), gateRunLevel): held}}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{{Memo: memo}}}, nil
}

func (f *resetClient) TerminateWorkflow(_ context.Context, _ string, runID, _ string, _ ...any) error {
	f.terminated = append(f.terminated, runID)
	return f.terminateErr
}

func newTestResetter(f *resetClient) *TemporalConcurrencyResetter {
	return NewTemporalConcurrencyResetter(f, "ns", "id", "ork-tasks",
		ConcurrencyResetSettings{Tunables: DefaultGateTunables(), FreezeMargin: 20 * time.Second}, nil, nil)
}

// TestReset_AStartTheClusterRefusesIsNotAskedAgain, and one whose answer is not
// known is; neither says what the cluster said.
func TestReset_AStartTheClusterRefusesIsNotAskedAgain(t *testing.T) {
	for _, c := range []struct {
		err   error
		retry bool
	}{{serviceerror.NewInvalidArgument("raw cluster words"), false}, {serviceerror.NewUnavailable("raw cluster words"), true}} {
		f := &resetClient{queryErr: serviceerror.NewNotFound("none"), missing: true, startErr: c.err}
		_, err := newTestResetter(f).ResetConcurrencyLimit(context.Background(), ConcurrencyReset{Key: "k", RequestID: "a", IssuedAt: time.Now()})
		var refused *ConcurrencyResetError
		if !errors.As(err, &refused) || refused.Code != ResetRefusedCode || refused.Retryable != c.retry || strings.Contains(refused.Reason, "raw cluster words") {
			t.Errorf("a start answered %v = %v; want refused, asked again %v", c.err, err, c.retry)
		}
	}
}

// TestResetUnanswered: a limit that does not answer is reset only when told
// to, and is never taken for one that is absent, or the other way round, or
// for a cluster that cannot be read. A dry run ends nothing.
func TestResetUnanswered(t *testing.T) {
	running := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	in := ConcurrencyReset{Key: "k", RequestID: "a", IssuedAt: time.Now()}
	dry := ConcurrencyReset{Key: "k", DryRun: true}
	refusal := func(err error) (string, bool) {
		var refused *ConcurrencyResetError
		if !errors.As(err, &refused) {
			return "", false
		}
		return refused.Code, refused.Retryable
	}
	down := serviceerror.NewDeadlineExceeded("no worker")
	for name, req := range map[string]ConcurrencyReset{"reset": in, "dry run": dry} {
		f := &resetClient{queryErr: down, status: running}
		_, err := newTestResetter(f).ResetConcurrencyLimit(context.Background(), req)
		if code, retry := refusal(err); code != ResetUnansweredCode || retry || len(f.terminated) != 0 {
			t.Errorf("%s of a limit that does not answer = %v; want %s and nothing ended", name, err, ResetUnansweredCode)
		}
		if want := "a forced reset accepts up to 3 extra runs"; err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v; want %q", name, err, want)
		}
	}
	forced := dry
	forced.ForceUnanswered = true
	f := &resetClient{queryErr: down, status: running}
	res, err := newTestResetter(f).ResetConcurrencyLimit(context.Background(), forced)
	must.OK(t, err)
	if res.State != ResetStateDryRun || res.ConsequenceCode != ResetUnansweredForced || len(f.terminated) != 0 || !strings.Contains(res.Consequence, "up to 3 more runs") {
		t.Fatalf("a forced dry run = %+v, ended %v", res, f.terminated)
	}
	absent := &resetClient{queryErr: serviceerror.NewNotFound("none"), missing: true}
	res, err = newTestResetter(absent).ResetConcurrencyLimit(context.Background(), dry)
	must.OK(t, err)
	if res.State != ResetStateDryRun || res.ConsequenceCode != ResetAbsent || res.Consequence == "" {
		t.Fatalf("a dry run of an absent limit = %+v", res)
	}
	epoch := int64(2)
	dry.ExpectedEpoch = &epoch
	if _, err = newTestResetter(absent).ResetConcurrencyLimit(context.Background(), dry); err == nil {
		t.Fatal("an absent limit was reset at an epoch it does not have")
	} else if code, _ := refusal(err); code != ResetConflictCode {
		t.Fatalf("= %v; want %s", err, ResetConflictCode)
	}
	// One that starts between the two reads was never asked.
	started := &resetClient{queryErr: serviceerror.NewNotFound("none"), status: running}
	force := in
	force.ForceUnanswered = true
	_, err = newTestResetter(started).ResetConcurrencyLimit(context.Background(), force)
	if code, retry := refusal(err); code != ResetRefusedCode || !retry || len(started.terminated) != 0 {
		t.Fatalf("a limit that started meanwhile = %v, ended %v; want asked again, nothing ended", err, started.terminated)
	}
	// A cluster that cannot be read says nothing of the limit.
	unread := &resetClient{queryErr: down, describeErr: serviceerror.NewUnavailable("frontend down")}
	_, err = newTestResetter(unread).ResetConcurrencyLimit(context.Background(), force)
	if code, retry := refusal(err); code != ResetRefusedCode || !retry || len(unread.terminated) != 0 {
		t.Fatalf("a cluster that cannot be read = %v, ended %v; want asked again, nothing ended", err, unread.terminated)
	}
	// One another reset holds is left to it.
	held, err := DataConverter().ToPayload(gateResetMemo{FrozenUntil: time.Now().Add(time.Minute)})
	must.OK(t, err)
	busy := &resetClient{queryErr: down, status: running, memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateResetsMemoKey: held}}}
	_, err = newTestResetter(busy).ResetConcurrencyLimit(context.Background(), force)
	if code, retry := refusal(err); code != ResetRefusedCode || !retry || len(busy.terminated) != 0 {
		t.Fatalf("a limit another reset holds = %v, ended %v; want asked again, nothing ended", err, busy.terminated)
	}
	// One that could not be ended is not started again.
	stuck := &resetClient{queryErr: down, status: running, terminateErr: serviceerror.NewUnavailable("frontend down")}
	_, err = newTestResetter(stuck).ResetConcurrencyLimit(context.Background(), force)
	if code, retry := refusal(err); code != ResetRefusedCode || !retry || len(stuck.terminated) != 1 {
		t.Fatalf("a limit that could not be ended = %v; want asked again", err)
	}
}

// TestCensus_CountsResetsThatDoNotFinish: a gate that began with a reset and
// is still rebuilding past the setting is counted; one that finished, or is
// within it, or rebuilds for another reason, is not.
func TestCensus_CountsResetsThatDoNotFinish(t *testing.T) {
	now := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	c := newCensusAggregator(now)
	c.resetIncompleteAfter = 15 * time.Minute
	for _, g := range []gateState{
		{ResetID: "a", ResetAt: now.Add(-15 * time.Minute), Rebuild: gateRebuild{State: gateRebuildFailing}},
		{ResetID: "b", ResetAt: now.Add(-time.Hour), Rebuild: gateRebuild{State: gateRebuildListing}},
		{ResetID: "c", ResetAt: now.Add(-14 * time.Minute), Rebuild: gateRebuild{State: gateRebuildListing}},
		{ResetID: "d", ResetAt: now.Add(-time.Hour)},
		{Rebuild: gateRebuild{State: gateRebuildFailing}},
	} {
		c.add(GateStateView{Namespace: "payments", State: g})
	}
	if got := c.finish().ResetsIncomplete; got != 2 {
		t.Fatalf("%d resets incomplete, want 2", got)
	}
}

// TestResetLook_AnUnreadableMemoryIsForgottenAfterItsWindow: what a closed
// limit remembered and this release cannot read refuses a reset until a fixed
// time after it closed, and none after; an open one's refuses for as long.
func TestResetLook_AnUnreadableMemoryIsForgottenAfterItsWindow(t *testing.T) {
	life := ResetMemoryLife(DefaultGateTunables())
	unreadable := &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateResetsMemoKey: {Data: []byte("?")}}}
	now := time.Now()
	for name, c := range map[string]struct {
		f       *resetClient
		forgets bool
	}{
		"closed within it": {&resetClient{queryErr: serviceerror.NewNotFound("none"), memo: unreadable, closed: now.Add(-life + time.Hour),
			status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true},
		"closed before it": {&resetClient{queryErr: serviceerror.NewNotFound("none"), memo: unreadable, closed: now.Add(-life - time.Hour),
			status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED}, false},
		"open": {&resetClient{queryErr: serviceerror.NewDeadlineExceeded("no worker"), memo: unreadable,
			status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, true},
	} {
		r := newTestResetter(c.f)
		look, err := r.look(context.Background(), "g")
		must.OKf(t, err, "%s", name)
		if look.forgets(now) != c.forgets {
			t.Errorf("%s: memory unknown = %v; want %v", name, look.forgets(now), c.forgets)
		}
		err = r.remembers(look.memo, gateReset{ID: "a", IssuedAt: now, Body: "b"})
		if (err != nil) != c.forgets {
			t.Errorf("%s: a new reset = %v; want refused %v", name, err, c.forgets)
		}
	}
}

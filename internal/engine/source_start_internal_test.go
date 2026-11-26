package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
)

// sourceCluster is the cluster as a start meets it, one execution after
// another: what a real one does between two calls only when it is raced.
type sourceCluster struct {
	mu   sync.Mutex
	last *workflowpb.WorkflowExecutionInfo
	// by are the latest executions of other sources than the one.
	by        map[string]*workflowpb.WorkflowExecutionInfo
	pendingOf map[string][]*workflowpb.PendingActivityInfo
	tries     map[string]int32
	runs      int
	signals   []string
	started   []client.StartWorkflowOptions
	inputs    []SourceInput
	calls     []string
	before    map[string]func() // what happens before the nth call of a kind
	count     map[string]int
	fail      map[string]error
	tombs     map[string]bool
	tombErr   error
	ns        *orkstackv1.Namespace
	nsErr     error
	pending   []*workflowpb.PendingActivityInfo
	task      bool
}

func (c *sourceCluster) call(kind string) error {
	c.count[kind]++
	key := fmt.Sprintf("%s%d", kind, c.count[kind])
	c.calls = append(c.calls, kind)
	if f := c.before[key]; f != nil {
		f()
	}
	return c.fail[key]
}

func (c *sourceCluster) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.call("describe"); err != nil {
		return nil, err
	}
	if c.by != nil {
		info, ok := c.by[id]
		if !ok {
			return nil, serviceerror.NewNotFound("no such workflow")
		}
		resp := &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info, PendingActivities: c.pendingOf[id]}
		if c.tries[id] > 0 {
			resp.PendingWorkflowTask = &workflowpb.PendingWorkflowTaskInfo{Attempt: c.tries[id]}
		}
		// A cluster answers each call with messages of its own, which a test
		// that changes its state later does not touch.
		return proto.Clone(resp).(*workflowservice.DescribeWorkflowExecutionResponse), nil
	}
	if c.last == nil {
		return nil, serviceerror.NewNotFound("no such workflow")
	}
	resp := &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: c.last, PendingActivities: c.pending}
	if c.task {
		resp.PendingWorkflowTask = &workflowpb.PendingWorkflowTaskInfo{}
	}
	return proto.Clone(resp).(*workflowservice.DescribeWorkflowExecutionResponse), nil
}

func (c *sourceCluster) SignalWorkflow(_ context.Context, id, runID, name string, arg any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.call("signal"); err != nil {
		return err
	}
	if c.by == nil && c.last.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return serviceerror.NewNotFound("workflow execution already completed")
	}
	c.signals = append(c.signals, fmt.Sprintf("%s %s %s %+v", id, runID, name, arg))
	return nil
}

func (c *sourceCluster) SignalWithStartWorkflow(ctx context.Context, id, name string, _ any, o client.StartWorkflowOptions, wf any, args ...any) (client.WorkflowRun, error) {
	c.mu.Lock()
	err := c.call("start")
	c.mu.Unlock()
	if errors.Is(err, context.DeadlineExceeded) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if wf != SourceWorkflowType || id != o.ID || name != SignalSourceSync {
		return nil, fmt.Errorf("%v %v %v", wf, id, name)
	}
	if c.by != nil {
		c.by[id] = &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "started"}, Status: running,
			Type: &commonpb.WorkflowType{Name: SourceWorkflowType}}
	}
	c.started, c.inputs = append(c.started, o), append(c.inputs, args[0].(SourceInput))
	c.runs++
	return nil, nil
}

func (c *sourceCluster) Get(context.Context, string) (*orkstackv1.Namespace, error) {
	return c.ns, c.nsErr
}

func newStarter(t *testing.T) (*SourceStarter, *sourceCluster) {
	t.Helper()
	c := &sourceCluster{before: map[string]func(){}, count: map[string]int{}, fail: map[string]error{}, tombs: map[string]bool{}, ns: &orkstackv1.Namespace{Name: "team"}}
	return &SourceStarter{Client: c, Tombstoned: func(_ context.Context, id string) (bool, error) { return c.tombs[id], c.tombErr }, Namespaces: c,
		Pin: sourcePinned(), Versioned: true, Runs: true, Logger: slog.New(slog.DiscardHandler)}, c
}

// is makes the latest execution one of that run and status, with a carry and
// a now.
func (c *sourceCluster) is(run string, status enumspb.WorkflowExecutionStatus, carry, now string) {
	c.last = &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: workSource, RunId: run}, Status: status,
		Type: &commonpb.WorkflowType{Name: SourceWorkflowType}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{}}}
	if carry != "" {
		c.last.Memo.Fields[sourceMemoCarry] = jsonPayload(carry)
	}
	if now != "" {
		c.last.Memo.Fields[sourceMemoNow] = jsonPayload(now)
	}
}

const sourceTerminated = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED

func startSource() *orkstackv1.EventSource {
	return &orkstackv1.EventSource{Id: workSource, Namespace: "team", Name: "orders", Pool: "gpu", Instances: 2}
}

// carried is a carry with a field this release does not know.
func carried(t *testing.T, how string) string {
	t.Helper()
	c := eventsource.FirstCarry(sourcePinned())
	c.Good = &eventsource.Spec{Revision: 3, Digest: "d3"}
	if how != "" {
		c.Continued = &eventsource.Continued{When: workNow, How: how}
	}
	body, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return strings.Replace(string(body), "{", `{"ofALaterRelease":[1, 2],`, 1)
}

// TestSourceStarter_AFirstStart: a source that has no execution starts from
// its document alone, with what is pinned today, and its memo has a carry
// from its first event.
func TestSourceStarter_AFirstStart(t *testing.T) {
	s, c := newStarter(t)
	if got, err := s.Start(t.Context(), startSource()); got != SourceStarted || err != nil {
		t.Fatalf("%s, %v", got, err)
	}
	o, in := c.started[0], c.inputs[0]
	first, err := eventsource.FirstCarry(sourcePinned()).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if o.ID != workSource || o.TaskQueue != task.DefaultQueue || o.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE ||
		o.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING || o.WorkflowExecutionTimeout != 0 || o.WorkflowRunTimeout != 0 || o.RetryPolicy != nil {
		t.Errorf("started with %+v", o)
	}
	if want := (SourceInput{Namespace: "team", Name: "orders", ID: workSource, Pool: "gpu", Instances: 2, Carry: first}); !reflect.DeepEqual(in, want) {
		t.Errorf("given %+v; want %+v", in, want)
	}
	now, err := eventsource.DecodeNow(o.Memo[sourceMemoNow].(jsontext.Value))
	if err != nil || !reflect.DeepEqual(now.Source, eventsource.Source{Namespace: "team", Name: "orders", Pool: "gpu", Instances: 2}) || now.StartedAfter != "" || now.Load != "failing" {
		t.Errorf("its first memo says %+v, %v", now, err)
	}
	if string(o.Memo[sourceMemoCarry].(jsontext.Value)) != string(first) {
		t.Errorf("its first memo carries %s", o.Memo[sourceMemoCarry])
	}
	if want := []string{"describe", "describe", "start"}; !slices.Equal(c.calls, want) {
		t.Errorf("calls %v; want %v", c.calls, want)
	}
}

// TestSourceStarter_FromTheLastExecution: what is started goes on from the
// bytes the last execution left, and says how that one ended where it did not
// end itself.
func TestSourceStarter_FromTheLastExecution(t *testing.T) {
	for name, tc := range map[string]struct {
		status enumspb.WorkflowExecutionStatus
		now    string
		after  string
	}{
		"it ended itself":              {status: completed},
		"it was terminated":            {status: sourceTerminated, now: `{"events":19900,"bytes":100}`, after: eventsource.EndedAbandoned},
		"it was cancelled":             {status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, after: eventsource.EndedAbandoned},
		"it timed out":                 {status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, after: eventsource.EndedAbandoned},
		"at its history's events":      {status: sourceTerminated, now: `{"events":20000}`, after: eventsource.EndedLimit},
		"at its history's bytes":       {status: sourceTerminated, now: `{"bytes":10485760}`, after: eventsource.EndedLimit},
		"it ended itself with as many": {status: completed, now: `{"events":20000}`},
		"a now of a later release":     {status: completed, now: `{"load":"ok","later":{"x":[1]}}`},
	} {
		t.Run(name, func(t *testing.T) {
			s, c := newStarter(t)
			carry := carried(t, eventsource.EndedHandover)
			c.is("r1", tc.status, carry, tc.now)
			if got, err := s.Start(t.Context(), startSource()); got != SourceStarted || err != nil {
				t.Fatalf("%s, %v", got, err)
			}
			now, err := eventsource.DecodeNow(c.started[0].Memo[sourceMemoNow].(jsontext.Value))
			if err != nil || now.StartedAfter != tc.after || c.inputs[0].StartedAfter != tc.after {
				t.Errorf("after %q, %q, %v; want %q", now.StartedAfter, c.inputs[0].StartedAfter, err, tc.after)
			}
			if string(c.inputs[0].Carry) != carry || string(c.started[0].Memo[sourceMemoCarry].(jsontext.Value)) != carry {
				t.Errorf("the carry it is started with is not the one that was there:\n%s\n%s", c.inputs[0].Carry, carry)
			}
		})
	}

	// One that was ended before it wrote a memo has none to go on from.
	s, c := newStarter(t)
	c.is("r1", sourceTerminated, "", "")
	if got, err := s.Start(t.Context(), startSource()); got != SourceStarted || err != nil || len(c.inputs[0].Carry) == 0 {
		t.Fatalf("%s, %v", got, err)
	}
}

// TestSourceStarter_StartsNothing: what starts nothing, and what a read that
// failed is not.
func TestSourceStarter_StartsNothing(t *testing.T) {
	marked := func(src *orkstackv1.EventSource) { src.DeletingSince = timestamppb.New(workNow) }
	for name, tc := range map[string]struct {
		world func(*SourceStarter, *sourceCluster)
		src   func(*orkstackv1.EventSource)
		want  SourceStart
		fails bool
	}{
		"a copy of a deleted source": {world: func(_ *SourceStarter, c *sourceCluster) { c.tombs[workSource] = true }, want: SourceDeletedCopy},
		"a copy, whatever its last execution": {world: func(_ *SourceStarter, c *sourceCluster) {
			c.tombs[workSource] = true
			c.is("r1", completed, carried(t, eventsource.EndedHandover), "")
		}, want: SourceDeletedCopy},
		"a tombstone that cannot be read":              {world: func(_ *SourceStarter, c *sourceCluster) { c.tombErr = errWorkStore }, fails: true},
		"a marked source's tombstone is not asked for": {world: func(_ *SourceStarter, c *sourceCluster) { c.tombErr = errWorkStore }, src: marked, want: SourceStarted},
		"a marked source with a tombstone":             {world: func(_ *SourceStarter, c *sourceCluster) { c.tombs[workSource] = true }, src: marked, want: SourceStarted},
		"a marked source that was purged": {world: func(_ *SourceStarter, c *sourceCluster) {
			c.is("r1", completed, carried(t, eventsource.EndedPurged), "")
		}, src: marked, want: SourcePurged},
		"a newer release's carry":            {world: func(_ *SourceStarter, c *sourceCluster) { c.is("r1", completed, `{"format":2}`, "") }, want: SourceNewerCarry},
		"a damaged carry":                    {world: func(_ *SourceStarter, c *sourceCluster) { c.is("r1", completed, `{"format":1,"good":7}`, "") }, fails: true},
		"a marked source's damaged carry":    {world: func(_ *SourceStarter, c *sourceCluster) { c.is("r1", completed, `{"format":1,"good":7}`, "") }, src: marked, want: SourceStarted},
		"a deployment that runs none":        {world: func(s *SourceStarter, c *sourceCluster) { s.Runs = false }, want: SourceNotRun},
		"a deployment that is not versioned": {world: func(s *SourceStarter, c *sourceCluster) { s.Versioned = false }, src: marked, want: SourceNotRun},
		"a marked source there":              {world: func(s *SourceStarter, c *sourceCluster) { s.Runs = false }, src: marked, want: SourceStarted},
		"a describe that fails":              {world: func(_ *SourceStarter, c *sourceCluster) { c.fail["describe1"] = errWorkStore }, fails: true},
		"a second describe that fails":       {world: func(_ *SourceStarter, c *sourceCluster) { c.fail["describe2"] = errWorkStore }, fails: true},
		"another kind's workflow": {world: func(_ *SourceStarter, c *sourceCluster) {
			c.is("r1", running, "", "")
			c.last.Type.Name = RunWorkflowType
		}, fails: true},
		"a memo that does not decode":     {world: func(_ *SourceStarter, c *sourceCluster) { c.is("r1", completed, "{}", `{"slot":[`) }, fails: true},
		"a namespace that cannot be read": {world: func(_ *SourceStarter, c *sourceCluster) { c.nsErr = errWorkStore }, fails: true},
		"a start the cluster refuses":     {world: func(_ *SourceStarter, c *sourceCluster) { c.fail["start1"] = serviceerror.NewInvalidArgument("x") }, fails: true},
		"a signal that fails": {world: func(_ *SourceStarter, c *sourceCluster) {
			c.is("r1", running, "", "")
			c.fail["signal1"] = serviceerror.NewUnavailable("x")
		}, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			s, c := newStarter(t)
			tc.world(s, c)
			src := startSource()
			if tc.src != nil {
				tc.src(src)
			}
			got, err := s.Start(t.Context(), src)
			if got != tc.want || (err != nil) != tc.fails || (len(c.started) > 0) != (tc.want == SourceStarted) {
				t.Errorf("%q, %v, %d started", got, err, len(c.started))
			}
		})
	}
}

// TestSourceStarter_OpenIsToldAndRacesBeginAgain: a workflow that is open is
// told and nothing is started; what ran between two calls begins it again.
func TestSourceStarter_OpenIsToldAndRacesBeginAgain(t *testing.T) {
	s, c := newStarter(t)
	c.is("r1", running, "{}", "")
	if got, err := s.Start(t.Context(), startSource()); got != SourceSignalled || err != nil || len(c.signals) != 1 || !strings.Contains(c.signals[0], " sync ") || len(c.started) != 0 {
		t.Fatalf("%s, %v, %v", got, err, c.signals)
	}

	// It closes between the describe and the signal.
	s, c = newStarter(t)
	c.is("r1", running, "", "")
	c.before["signal1"] = func() { c.is("r1", completed, carried(t, ""), "") }
	if got, err := s.Start(t.Context(), startSource()); got != SourceStarted || err != nil || len(c.signals) != 0 {
		t.Fatalf("%s, %v, %v", got, err, c.calls)
	}

	// Another replica started one, which ended, between the two describes: the
	// carry that was read is one before the last.
	s, c = newStarter(t)
	c.is("r1", completed, carried(t, ""), "")
	newer := strings.Replace(carried(t, ""), `"d3"`, `"d4"`, 1)
	c.before["describe2"] = func() { c.is("r2", completed, newer, "") }
	if got, err := s.Start(t.Context(), startSource()); got != SourceStarted || err != nil || string(c.inputs[0].Carry) != newer {
		t.Fatalf("%s, %v, %v", got, err, c.calls)
	}

	// One that is closed, open and closed again for ever is begun again three
	// times.
	s, c = newStarter(t)
	for i := 1; i <= 20; i++ {
		run := fmt.Sprint("r", i)
		c.before[fmt.Sprint("describe", i)] = func() { c.is(run, completed, carried(t, ""), "") }
	}
	if got, err := s.Start(t.Context(), startSource()); got != SourceUnanswered || err != nil || len(c.started) != 0 || c.count["describe"] != 2*sourceStartTries {
		t.Fatalf("%s, %v, %v", got, err, c.calls)
	}

	// A start that is not answered within its bound is begun again, and may
	// have been made: what begins again finds it open.
	synctest.Test(t, func(t *testing.T) {
		s, c := newStarter(t)
		c.fail["start1"] = context.DeadlineExceeded
		c.before["describe3"] = func() { c.is("r1", running, "", "") }
		began := time.Now()
		if got, err := s.Start(t.Context(), startSource()); got != SourceSignalled || err != nil || time.Since(began) != sourceStartBound {
			t.Errorf("%s, %v after %s", got, err, time.Since(began))
		}
		s, c = newStarter(t)
		for i := 1; i <= sourceStartTries; i++ {
			c.fail[fmt.Sprint("start", i)] = context.DeadlineExceeded
		}
		if got, err := s.Start(t.Context(), startSource()); got != SourceUnanswered || err != nil || c.count["start"] != sourceStartTries {
			t.Errorf("%s, %v, %v", got, err, c.calls)
		}
	})

	// A caller that gives up is not the start's bound.
	s, c = newStarter(t)
	c.fail["start1"] = context.DeadlineExceeded
	ctx, cancel := context.WithCancel(t.Context())
	c.before["start1"] = cancel
	if got, err := s.Start(ctx, startSource()); !errors.Is(err, context.Canceled) || got != "" {
		t.Fatalf("%s, %v", got, err)
	}
}

// What the memo says the source asks for is what its caller read of it.
func TestSourceStarter_FirstMemoSaysWhatIsAskedFor(t *testing.T) {
	for name, tc := range map[string]struct {
		src  func(*orkstackv1.EventSource)
		held bool
		pool string
		want eventsource.Source
	}{
		"as it is":             {want: eventsource.Source{Pool: "gpu", Instances: 2}},
		"paused":               {src: func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{} }},
		"marked":               {src: func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) }},
		"its namespace held":   {held: true},
		"its namespace's pool": {src: func(s *orkstackv1.EventSource) { s.Pool, s.Instances = "", 0 }, pool: "arm", want: eventsource.Source{Pool: "arm", Instances: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			s, c := newStarter(t)
			c.ns.DefaultPool = tc.pool
			if tc.held {
				c.ns.Hold = &orkstackv1.NamespaceHold{}
			}
			src := startSource()
			if tc.src != nil {
				tc.src(src)
			}
			if got, err := s.Start(t.Context(), src); got != SourceStarted || err != nil {
				t.Fatalf("%s, %v", got, err)
			}
			now, err := eventsource.DecodeNow(c.started[0].Memo[sourceMemoNow].(jsontext.Value))
			tc.want.Namespace, tc.want.Name = "team", "orders"
			if err != nil || now.Source != tc.want || c.inputs[0].Pool != tc.want.Pool || c.inputs[0].Instances != tc.want.Instances {
				t.Errorf("%+v, %v; want %+v", now.Source, err, tc.want)
			}
		})
	}
}

// TestSourceStarter_RelaysWhatIsWaitedFor: a report is a signal only where
// the workflow waits for it and was not told.
func TestSourceStarter_RelaysWhatIsWaitedFor(t *testing.T) {
	const now = `{"slot":{"0":{"current":{"epoch":7,"digest":"d","phase":"scheduled"},"awaits":{"7":["started","ready","healthy"],"8":["invalid"]},"had":{"7":{"ready":""}}}}}`
	ready, invalid, healthy := orkstackv1.InstanceReport_INSTANCE_REPORT_READY, orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID, orkstackv1.InstanceReport_INSTANCE_REPORT_HEALTHY
	for name, tc := range map[string]struct {
		index   int32
		epoch   int64
		attempt int32
		what    orkstackv1.InstanceReport
		status  enumspb.WorkflowExecutionStatus
		want    string
	}{
		"started":                  {epoch: 7, attempt: 1, want: "started"},
		"started, not waited for":  {epoch: 8, attempt: 1},
		"ready, had before":        {epoch: 7, attempt: 1, what: ready},
		"invalid":                  {epoch: 8, attempt: 1, what: invalid, want: "invalid"},
		"invalid, not waited for":  {epoch: 7, attempt: 1, what: invalid},
		"healthy":                  {epoch: 7, attempt: 2, what: healthy, want: "healthy"},
		"healthy, a first attempt": {epoch: 7, attempt: 1, what: healthy},
		"another index":            {index: 1, epoch: 7, attempt: 1},
		"another epoch":            {epoch: 9, attempt: 1},
		"a workflow that closed":   {epoch: 7, attempt: 1, status: completed},
	} {
		t.Run(name, func(t *testing.T) {
			s, c := newStarter(t)
			status := tc.status
			if status == 0 {
				status = running
			}
			c.is("r1", status, "", now)
			ref := held.Ref{SourceID: workSource, Index: tc.index, Epoch: tc.epoch, Attempt: tc.attempt}
			var err error
			if tc.what == 0 {
				err = s.Started(t.Context(), ref)
			} else {
				err = s.Report(t.Context(), ref, tc.what, "why")
			}
			var want []string
			if tc.want != "" {
				reason := ""
				if tc.what != 0 {
					reason = "why"
				}
				want = []string{fmt.Sprintf("%s r1 report %+v", workSource, SourceReport{Index: tc.index, Epoch: tc.epoch, What: tc.want, Reason: reason})}
			}
			if err != nil || !slices.Equal(c.signals, want) {
				t.Errorf("%v, %v; want %v", c.signals, err, want)
			}
		})
	}
	s, c := newStarter(t)
	if err := s.Report(t.Context(), held.Ref{SourceID: workSource}, 99, ""); err == nil {
		t.Error("a report of no kind was relayed")
	}
	c.fail["describe1"] = errWorkStore
	if err := s.Started(t.Context(), held.Ref{SourceID: workSource}); !errors.Is(err, errWorkStore) {
		t.Errorf("%v", err)
	}
	// What it is told and has closed is told by what starts it again.
	s, c = newStarter(t)
	c.is("r1", completed, "", "")
	if err := s.Sync(t.Context(), workSource); err != nil {
		t.Errorf("%v", err)
	}
	c.fail["signal2"] = serviceerror.NewUnavailable("x")
	if err := s.Sync(t.Context(), workSource); err == nil {
		t.Error("a signal that failed said nothing")
	}
}

package engine

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fcInFlight is one open ForceCancelWorkflow execution as the cluster lists it.
type fcInFlight struct {
	id, exec string
	typ      string // "" is the force cancel's
	started  time.Time
	after    time.Duration // how long after its start it is looked for
	queue    string        // "" is the engines'
	lasted   time.Duration // a closed one's
	unread   bool          // its start cannot be read
	input    any
}

// flightCluster lists executions, answers each one's start, and records what
// is terminated.
type flightCluster struct {
	workflowservice.WorkflowServiceClient
	flights    []fcInFlight
	listErr    error
	queries    []string
	terminated []*workflowservice.TerminateWorkflowExecutionRequest
	endErr     error
	// run is the run as described; attempts, the force cancels a start is
	// refused for, by whether each is still running; started, what started.
	run      *workflowpb.WorkflowExecutionInfo
	reset    *timestamppb.Timestamp
	attempts map[string]bool
	started  []string
	inputs   []ForceCancelInput
	runStart *historypb.HistoryEvent
	more     bool // there is more than was listed
	startErr error
	lasted   map[string]time.Duration    // a closed attempt's
	starts   map[string]ForceCancelInput // what an attempt was started with
	order    []string
	ownStart *fcInFlight // how the request's own running attempt was started
	// closedFlights are those listed as terminated.
	closedFlights []fcInFlight
	// asked is when the run was asked to cancel, zero for never.
	asked time.Time
	// unasked: it never was.
	unasked bool
	// readTo is when the oldest event read happened; cancelErr, what asking
	// it to cancel answers.
	readTo    time.Time
	cancelErr error
	counted   counted
	hard      error // of its history, newest first
	// result is what a closed force cancel answered.
	result ForceCancelResult
}

func (c *flightCluster) GetWorkflowExecutionHistoryReverse(context.Context, *workflowservice.GetWorkflowExecutionHistoryReverseRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	events := []*historypb.HistoryEvent{{EventId: 9, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED}}
	if !c.asked.IsZero() {
		events = append(events, &historypb.HistoryEvent{EventId: 8, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, EventTime: timestamppb.New(c.asked)})
	}
	if c.asked.IsZero() {
		events = append(events, &historypb.HistoryEvent{EventId: 1, EventTime: timestamppb.New(c.readTo)})
	}
	return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: &historypb.History{Events: events}}, c.hard
}

func (c *flightCluster) RequestCancelWorkflowExecution(_ context.Context, in *workflowservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	c.order = append(c.order, "cancel "+in.GetWorkflowExecution().GetWorkflowId())
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, c.cancelErr
}

func (c *flightCluster) GetWorkflowExecutionHistory(_ context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if in.GetExecution().GetWorkflowId() == "ork-r" {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{c.runStart}}}, nil
	}
	all := append(slices.Clone(c.flights), c.closedFlights...)
	if c.ownStart != nil {
		c.ownStart.exec = in.GetExecution().GetRunId()
		all = append(all, *c.ownStart)
	}
	if in, ok := c.starts[in.GetExecution().GetWorkflowId()]; ok {
		all = append(all, fcInFlight{exec: "own", input: in})
	}
	for _, f := range all {
		if f.exec == in.GetExecution().GetRunId() && f.unread {
			return nil, errors.New("history gone")
		}
		if f.exec == in.GetExecution().GetRunId() {
			input, err := DataConverter().ToPayloads(f.input)
			if err != nil {
				return nil, err
			}
			return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: input}}}}}}, nil
		}
	}
	return nil, errors.New("no such execution")
}

func (c *flightCluster) TerminateWorkflowExecution(_ context.Context, in *workflowservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	c.order = append(c.order, "end "+in.GetWorkflowExecution().GetWorkflowId())
	c.terminated = append(c.terminated, in)
	return &workflowservice.TerminateWorkflowExecutionResponse{}, c.endErr
}

func (c *flightCluster) client() *mocks.Client {
	m := &mocks.Client{}
	m.On("WorkflowService").Return(c)
	list := m.On("ListWorkflow", mock.Anything, mock.Anything)
	list.Run(func(args mock.Arguments) {
		q := args.Get(1).(*workflowservice.ListWorkflowExecutionsRequest).GetQuery()
		c.queries = append(c.queries, q)
		flights, out := c.flights, &workflowservice.ListWorkflowExecutionsResponse{}
		if strings.Contains(q, `"Terminated"`) {
			flights = c.closedFlights
		}
		for _, f := range flights {
			typ, queue := f.typ, f.queue
			if typ == "" {
				typ = ForceCancelWorkflowType
			}
			if queue == "" {
				queue = "ork-tasks"
			}
			out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: f.id, RunId: f.exec},
				Type: &commonpb.WorkflowType{Name: typ}, StartTime: timestamppb.New(f.started), CloseTime: timestamppb.New(f.started.Add(f.lasted)), TaskQueue: queue, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING})
		}
		if c.more {
			out.NextPageToken = []byte("more")
		}
		list.ReturnArguments = mock.Arguments{out, c.listErr}
	})
	describe := m.On("DescribeWorkflow", mock.Anything, mock.Anything, mock.Anything)
	describe.Run(func(args mock.Arguments) {
		info := c.run
		if id := args.String(1); id != "ork-r" {
			info = &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "own"}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
			if c.attempts[id] {
				info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}
			at := time.Now()
			info.StartTime, info.CloseTime = timestamppb.New(at), timestamppb.New(at.Add(c.lasted[id]))
		}
		describe.ReturnArguments = mock.Arguments{&client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: info, WorkflowExtendedInfo: &workflowpb.WorkflowExecutionExtendedInfo{LastResetTime: c.reset}}}, nil}
	})
	answered := &mocks.WorkflowRun{}
	answered.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) { *args.Get(1).(*ForceCancelResult) = c.result }).Return(nil)
	m.On("GetWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(answered)
	start := m.On("ExecuteWorkflow", mock.Anything, mock.Anything, ForceCancelWorkflowType, mock.Anything)
	start.Run(func(args mock.Arguments) {
		id := args.Get(1).(client.StartWorkflowOptions).ID
		if _, taken := c.attempts[id]; taken {
			start.ReturnArguments = mock.Arguments{nil, serviceerror.NewWorkflowExecutionAlreadyStarted("taken", "", "")}
			return
		}
		if c.startErr != nil {
			start.ReturnArguments = mock.Arguments{nil, c.startErr}
			return
		}
		c.order = append(c.order, "start")
		c.started, c.inputs = append(c.started, id), append(c.inputs, args.Get(3).(ForceCancelInput))
		start.ReturnArguments = mock.Arguments{nil, nil}
	})
	return m
}

// counted is a ForceCancelCounter.
type counted map[string]int

func (c *counted) CountForceCancel(_ context.Context, outcome string) {
	if *c == nil {
		*c = counted{}
	}
	(*c)[outcome]++
}

// CountForceCancelEnding counts an ending as "<outcome>/<resumed from>".
func (c *counted) CountForceCancelEnding(ctx context.Context, outcome, resumedFrom string) {
	c.CountForceCancel(ctx, outcome+"/"+resumedFrom)
}

// TestForceCancelFind: of the open executions named as a run's force cancels,
// one is joined only when its own decision verifies for that run under the id
// it has and it is not past its time; one past it is superseded; one whose
// decision is bad is nobody's; what cannot be judged is left alone.
func TestForceCancelFind(t *testing.T) {
	keys, other := recordKeys(t), recordKeys(t)
	now := time.Now().UTC()
	signed := func(k ForceCancelKeys, mutate func(*ForceCancelDecision)) (ForceCancelInput, ForceCancelDecision) {
		d := sampleDecision()
		d.IssuedAt, d.ExpiresAt = now.Add(-time.Minute), now.Add(time.Hour)
		if mutate != nil {
			mutate(&d)
		}
		msg, err := SignForceCancelDecision(k, d)
		must.OK(t, err)
		return ForceCancelInput{Decision: msg}, d
	}
	mine, d := signed(keys, nil)
	theirs, _ := signed(other, nil)
	elsewhere, ed := signed(keys, func(d *ForceCancelDecision) { d.RunID = "ork-other" })
	asserted := sampleDecision()
	asserted.PrincipalVerified, asserted.AuthMethod, asserted.PrincipalIssuer, asserted.Rule, asserted.PolicyGeneration = false, "", "", "", 0
	tampered := mine
	tampered.Decision = mine.Decision[:len(mine.Decision)-6] + "AAAAAA"
	deadline := d.Grace() + d.Ceiling() + DefaultForceCancelSettings().AttachProgressSlack
	for _, tt := range []struct {
		name                      string
		flight                    fcInFlight
		join, supersede, squatter bool
	}{
		{"its own", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: mine}, true, false, false},
		{"just inside its time", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, after: deadline - time.Second, input: mine}, true, false, false},
		{"past its time", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, after: deadline, input: mine}, false, true, false},
		{"a genuine decision under another id", fcInFlight{id: d.WorkflowID() + "x", exec: "fc-exec", started: now, input: mine}, false, false, false},
		{"another run's decision under this run's name", fcInFlight{id: ForceCancelWorkflowIDPrefix("payments", "ork-r") + ed.RequestID, exec: "fc-exec", started: now, input: elsewhere}, false, false, false},
		{"a decision that was changed", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: tampered}, false, false, true},
		{"a key nobody published", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: theirs}, false, false, false},
		{"unsigned where callers are verified", fcInFlight{id: asserted.WorkflowID(), exec: "fc-exec", started: now, input: ForceCancelInput{Unsigned: &asserted}}, false, false, true},
		{"no input", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: "?"}, false, false, false},
		{"one whose start cannot be read", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: mine, unread: true}, false, false, false},
		{"a genuine decision on a channel no engine takes work from", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", started: now, input: mine, queue: "ork-dead"}, false, false, false},
		{"another workflow", fcInFlight{id: d.WorkflowID(), exec: "fc-exec", typ: "Other", started: now, input: mine}, false, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &flightCluster{flights: []fcInFlight{tt.flight}}
			s := NewForceCancelStarter(ForceCancelDeps{Client: c.client(), ClusterNamespace: "ork-main", Keys: keys, Mode: func() string { return "enforce" }}, DefaultForceCancelSettings())
			s.now = func() time.Time { return tt.flight.started.Add(tt.flight.after) }
			found, err := s.find(context.Background(), "payments", "ork-r")
			must.OK(t, err)
			if (len(found.join) > 0) != tt.join || (len(found.supersede) > 0) != tt.supersede || (len(found.squatters) > 0) != tt.squatter {
				t.Fatalf("found %+v", found)
			}
			if tt.join && (found.join[0].decision.RequestID != d.RequestID || found.join[0].execution != "fc-exec") {
				t.Errorf("joined %+v", found.join)
			}
			if len(c.queries) != 1 || !strings.Contains(c.queries[0], ForceCancelWorkflowIDPrefix("payments", "ork-r")) || !strings.Contains(c.queries[0], "Running") {
				t.Errorf("asked %q", c.queries)
			}
		})
	}
	t.Run("a cluster that cannot list joins nothing", func(t *testing.T) {
		c := &flightCluster{listErr: errors.New("down")}
		s := NewForceCancelStarter(ForceCancelDeps{Client: c.client(), ClusterNamespace: "ork-main", Keys: keys, Mode: func() string { return "enforce" }}, DefaultForceCancelSettings())
		if found, err := s.find(context.Background(), "payments", "ork-r"); err == nil {
			t.Fatalf("found %+v with no error", found)
		}
	})
}

// TestForceCancelEndForeign: a superseded force cancel and a squatter are ended
// by their own execution, so nothing started since is, and recorded.
func TestForceCancelEndForeign(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewAuditStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	c := &flightCluster{}
	s := NewForceCancelStarter(ForceCancelDeps{Client: c.client(), ClusterNamespace: "ork-main", Audit: store, Keys: recordKeys(t), Mode: func() string { return "enforce" }}, DefaultForceCancelSettings())
	if !s.end(ctx, "ork-r", "payments", fcFlight{workflowID: "ork-force-x-1", execution: "e1", startedAt: time.Now()}, forceCancelSquatter) {
		t.Error("ended, and said not to be")
	}
	s.end(ctx, "ork-r", "payments", fcFlight{workflowID: "ork-force-x-2", execution: "e2", startedAt: time.Now()}, forceCancelSuperseded)
	var ended []string
	for _, r := range c.terminated {
		ended = append(ended, r.GetWorkflowExecution().GetWorkflowId()+" "+r.GetWorkflowExecution().GetRunId()+" "+r.GetReason())
	}
	if !slices.Equal(ended, []string{"ork-force-x-1 e1 orkstack force cancel squatter", "ork-force-x-2 e2 orkstack force cancel superseded"}) {
		t.Errorf("ended %q", ended)
	}
	page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 9})
	must.OK(t, err)
	got := map[string]string{}
	for _, r := range page.Records {
		got[r.GetAction()] = r.GetTarget() + " " + r.GetDetail()
	}
	c.endErr = serviceerror.NewUnavailable("down")
	if s.end(ctx, "ork-r", "payments", fcFlight{workflowID: "ork-force-x-3", execution: "e3"}, forceCancelSquatter) {
		t.Error("not ended, and said to be")
	}
	if len(got) != 2 || !strings.Contains(got[forceCancelSquatter], "ork-force-x-1") || !strings.Contains(got[forceCancelSuperseded], "ork-force-x-2") {
		t.Errorf("recorded %v", got)
	}
}

// TestForceCancelRequest: a request joins the force cancel in flight, or starts
// its own beside nothing: one past its time is superseded, one that is nobody's
// is ended; a run two of them left open is ended here, with a record of this
// request; a request that names itself is one request however often it is sent.
func TestForceCancelRequest(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	now := time.Now().UTC()
	settings := DefaultForceCancelSettings()
	flight := func(id string, started time.Time, target string) fcInFlight {
		d := sampleDecision()
		d.RequestID, d.TargetExecutionRunID, d.IssuedAt, d.ExpiresAt = id, target, now.Add(-time.Minute), now.Add(6*time.Hour)
		msg, err := SignForceCancelDecision(keys, d)
		must.OK(t, err)
		return fcInFlight{id: d.WorkflowID(), exec: "exec-of-" + id, started: started, input: ForceCancelInput{Decision: msg}}
	}
	given := settings.Grace + settings.Ceiling
	whole := func(id string) fcInFlight {
		f := flight(id, now, "exec-7")
		f.lasted = given
		return f
	}
	past := settings.Grace + settings.Ceiling + settings.AttachProgressSlack
	request := func(t *testing.T, c *flightCluster, ask ForceCancelAsk, after time.Duration) (ForceCancelAnswer, error) {
		t.Helper()
		if c.run == nil {
			c.run = openRun()
		}
		c.flights = append([]fcInFlight{}, c.flights...)
		if c.asked.IsZero() && !c.unasked {
			c.asked = now.Add(-time.Hour)
		}
		s := NewForceCancelStarter(ForceCancelDeps{Client: startsOf(t, c), ClusterNamespace: "ork-main", Reader: &TemporalRunReader{converter: DataConverter()}, Counter: &c.counted,
			Starter: NewTemporalStarter(c.client(), "ork-tasks").WithClusterIdentity("ork-main", "engine-1"), Keys: keys, Mode: func() string { return "enforce" }}, settings)
		s.longestGrace = func(context.Context, string) time.Duration { return 0 }
		s.now = func() time.Time { return now.Add(after) }
		return s.Request(ctx, ask)
	}
	t.Run("nothing in flight", func(t *testing.T) {
		c := &flightCluster{}
		got, err := request(t, c, sampleAsk(), 0)
		if err != nil || got.State != FCStateRequested || got.Attached || len(c.started) != 1 || len(c.terminated) != 0 {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("what is found is counted", func(t *testing.T) {
		forged, unread := flight("fc-1", now, "exec-7"), flight("fc-2", now, "exec-7")
		in := forged.input.(ForceCancelInput)
		in.Decision = in.Decision[:len(in.Decision)-6] + "AAAAAA"
		forged.input, unread.unread = in, true
		c := &flightCluster{flights: []fcInFlight{forged, unread, flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-4"), whole("fc-5")}}
		_, err := request(t, c, sampleAsk(), past)
		must.OK(t, err)
		if want := (counted{FCCountSquatter: 1, FCCountUnknown: 1, FCCountSuperseded: 1, FCCountDirect: 1}); !maps.Equal(c.counted, want) {
			t.Errorf("%v, want %v", c.counted, want)
		}
	})
	t.Run("one in flight is joined", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-2", now, "exec-7"), flight("fc-1", now.Add(-time.Second), "exec-7")}}
		c.flights[1].started = now // one clock: keys are published now
		got, err := request(t, c, sampleAsk(), time.Minute)
		if err != nil || got.State != FCStateWaiting || !got.Attached || got.RequestID != "fc-2" || len(c.started) != 0 {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("one made before the run was resumed is not", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-1", now, "exec-7")}, reset: timestamppb.New(now.Add(time.Second))}
		got, err := request(t, c, sampleAsk(), time.Minute)
		if err != nil || got.Attached || len(c.started) != 1 {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("one past its time is superseded", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-1", now, "exec-7")}}
		got, err := request(t, c, sampleAsk(), past)
		if err != nil || got.Attached || got.Direct || len(c.started) != 1 || len(c.terminated) != 1 || c.terminated[0].GetWorkflowExecution().GetRunId() != "exec-of-fc-1" {
			t.Fatalf("%+v, %v; started %v, terminated %v", got, err, c.started, c.terminated)
		}
	})
	t.Run("a run two of them left open is ended here", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}}
		got, err := request(t, c, sampleAsk(), past)
		if err != nil || !got.Direct || len(c.started) != 1 || len(c.terminated) != 2 {
			t.Fatalf("%+v, %v; started %v, terminated %v", got, err, c.started, c.terminated)
		}
		// What finishes the run is started before the run is ended.
		if i, j := slices.Index(c.order, "start"), slices.Index(c.order, "end ork-r"); i < 0 || j < i {
			t.Errorf("order %v", c.order)
		}
		end := c.terminated[1]
		if end.GetWorkflowExecution().GetWorkflowId() != "ork-r" || end.GetWorkflowExecution().GetRunId() != "exec-7" || end.GetReason() != ForceCancelTerminateReason {
			t.Errorf("ended %v", end)
		}
		e := terminated(t, end.GetDetails())
		read := ReadForceCancelClose(ctx, keys, "enforce", "payments", "ork-r", "exec-7", e, time.Now())
		if read.Verification != ForceCancelVerified || read.Record.RequestID != got.RequestID || !read.Record.ProgressingAtCeiling || read.Record.Principal != sampleAsk().Principal {
			t.Errorf("its record reads %+v", read)
		}
	})
	for name, closed := range map[string]func() []fcInFlight{
		"another execution's": func() []fcInFlight {
			f := flight("fc-1", now, "exec-1")
			f.lasted = given
			return []fcInFlight{f, whole("fc-2")}
		},
		"nobody's": func() []fcInFlight {
			f := whole("fc-1")
			f.input = ForceCancelInput{Decision: "x.y.z"}
			return []fcInFlight{f, whole("fc-2")}
		},
		"one ended early": func() []fcInFlight {
			f := whole("fc-1")
			f.lasted = given - time.Second
			return []fcInFlight{f, whole("fc-2")}
		},
		"one counted twice": func() []fcInFlight { f := whole("fc-2"); f.exec = "again"; return []fcInFlight{f, whole("fc-2")} },
	} {
		t.Run("not counted: "+name, func(t *testing.T) {
			c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: closed()}
			got, err := request(t, c, sampleAsk(), past)
			if err != nil || got.Direct || len(c.terminated) != 1 {
				t.Fatalf("%+v, %v; terminated %v", got, err, c.terminated)
			}
		})
	}
	for name, c := range map[string]*flightCluster{
		"never asked to cancel": {unasked: true}, "asked less than its grace ago": {asked: now.Add(past - 30*time.Second)}, "of which that cannot be read": {hard: errors.New("down")},
	} {
		t.Run("a run "+name+" is not ended here", func(t *testing.T) {
			c.flights, c.closedFlights = []fcInFlight{flight("fc-3", now, "exec-7")}, []fcInFlight{whole("fc-1"), whole("fc-2")}
			got, err := request(t, c, sampleAsk(), past)
			if err != nil || got.Direct || len(c.started) != 1 || slices.Contains(c.order, "end ork-r") || slices.Contains(c.order, "cancel ork-r") != c.unasked {
				t.Fatalf("%+v, %v; %v", got, err, c.order)
			}
		})
	}
	t.Run("a run resumed since it was decided is said to be", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}, reset: timestamppb.New(now.Add(past + time.Second))}
		if got, err := request(t, c, sampleAsk(), past); !errors.Is(err, ErrForceCancelStale) || slices.Contains(c.order, "end ork-r") {
			t.Fatalf("%+v, %v; %v", got, err, c.order)
		}
	})
	t.Run("where nothing is signed, nothing is joined, ended or counted", func(t *testing.T) {
		unsigned := func(id string, lasted time.Duration) fcInFlight {
			d := sampleDecision()
			d.claimNothing()
			d.RequestID, d.TargetExecutionRunID, d.IssuedAt, d.ExpiresAt = id, "exec-7", now.Add(-time.Minute), now.Add(6*time.Hour)
			return fcInFlight{id: d.WorkflowID(), exec: "exec-of-" + id, started: now, lasted: lasted, input: ForceCancelInput{Unsigned: &d}}
		}
		for after, closed := range map[time.Duration][]fcInFlight{time.Minute: nil, past: {unsigned("fc-1", given), unsigned("fc-2", given)}} {
			c := &flightCluster{flights: []fcInFlight{unsigned("fc-3", 0)}, closedFlights: closed, run: openRun(), asked: now.Add(-time.Hour)}
			s := NewForceCancelStarter(ForceCancelDeps{Client: startsOf(t, c), ClusterNamespace: "ork-main", Reader: &TemporalRunReader{converter: DataConverter()},
				Starter: NewTemporalStarter(c.client(), "ork-tasks").WithClusterIdentity("ork-main", "engine-1"),
				Keys:    NewKeylessEngine(keysDoc{}, func() string { return "asserted" }), Mode: func() string { return "asserted" }}, settings)
			s.longestGrace = func(context.Context, string) time.Duration { return 0 }
			s.now = func() time.Time { return now.Add(after) }
			got, err := s.Request(ctx, sampleAsk())
			if err != nil || got.Attached || got.Direct || got.UnsignedLeft != 1 || !got.JoinSkipped || len(c.started) != 1 || len(c.terminated) != 0 {
				t.Fatalf("%+v, %v; started %v, ended %v", got, err, c.started, c.terminated)
			}
		}
	})
	t.Run("a cascade", func(t *testing.T) {
		cascading := func(id string, started time.Time) fcInFlight {
			d := sampleDecision()
			d.RequestID, d.Cascade, d.TargetExecutionRunID, d.IssuedAt, d.ExpiresAt = id, true, "exec-7", now.Add(-time.Minute), now.Add(23*time.Hour)
			msg, err := SignForceCancelDecision(keys, d)
			must.OK(t, err)
			return fcInFlight{id: d.WorkflowID(), exec: "exec-of-" + id, started: started, input: ForceCancelInput{Decision: msg}}
		}
		plain, asked := sampleAsk(), sampleAsk()
		asked.Cascade = true
		long := settings.Timeout() + settings.CascadeFloor() + settings.AttachProgressSlack
		for name, tt := range map[string]struct {
			in       fcInFlight
			ask      ForceCancelAsk
			after    time.Duration
			attached bool
			started  int
			ended    int
		}{
			"asked for beside one that does not":     {flight("fc-1", now, "exec-7"), asked, time.Minute, false, 1, 0},
			"asked for while one is":                 {cascading("fc-1", now), asked, time.Minute, true, 0, 0},
			"joined by one that is not":              {cascading("fc-1", now), plain, time.Minute, true, 0, 0},
			"whose run is over, still at it":         {cascading("fc-1", now), asked, past, true, 0, 0},
			"whose run it left open past its time":   {cascading("fc-1", now), asked, past, false, 1, 1},
			"past its time, superseded by its like":  {cascading("fc-1", now), asked, long, false, 1, 1},
			"past its time, and not by less than it": {cascading("fc-1", now), plain, long, true, 0, 0},
		} {
			c := &flightCluster{flights: []fcInFlight{tt.in}, run: openRun()}
			if strings.HasPrefix(name, "whose run is over") {
				c.run.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
			got, err := request(t, c, tt.ask, tt.after)
			if err != nil || got.Attached != tt.attached || len(c.started) != tt.started || len(c.terminated) != tt.ended {
				t.Errorf("%s: %+v, %v; started %v, ended %v", name, got, err, c.started, c.terminated)
				continue
			}
			if tt.started == 1 {
				in := c.inputs[0]
				if (in.Settings != nil) != tt.ask.Cascade || tt.ask.Cascade && in.Settings.Cascade != settings.Cascade {
					t.Errorf("%s: started with %+v", name, in)
				}
			}
		}
	})
	t.Run("a start that cannot be judged is not waited for, and said", func(t *testing.T) {
		unread := flight("fc-1", now, "exec-7")
		unread.unread = true
		c := &flightCluster{flights: []fcInFlight{unread}}
		if got, err := request(t, c, sampleAsk(), 0); err != nil || !got.JoinSkipped || got.Attached || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("those on a channel no engine takes work from do not count", func(t *testing.T) {
		dead := whole("fc-1")
		dead.queue = "ork-dead"
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{dead, whole("fc-2")}}
		if got, err := request(t, c, sampleAsk(), past); err != nil || got.Direct {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("a start that fails leaves the run as it is", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}, startErr: serviceerror.NewUnavailable("down")}
		if _, err := request(t, c, sampleAsk(), past); err == nil || slices.Contains(c.order, "end ork-r") {
			t.Fatalf("%v; %v", err, c.order)
		}
	})
	t.Run("a run that could not be ended here is left to the one started", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}, endErr: serviceerror.NewUnavailable("down")}
		if got, err := request(t, c, sampleAsk(), past); err != nil || got.Direct || got.State != FCStateRequested || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("an earlier one that no longer names the run does not hide a later that does", func(t *testing.T) {
		stale := flight("fc-1", now, "exec-of-another-chain")
		c := &flightCluster{flights: []fcInFlight{stale, flight("fc-2", now.Add(time.Second), "exec-7")}}
		if got, err := request(t, c, sampleAsk(), time.Minute); err != nil || !got.Attached || got.RequestID != "fc-2" || len(c.started) != 0 {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("a list that was not read through joins and starts as it may, and ends nothing here", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}, more: true}
		if got, err := request(t, c, sampleAsk(), past); err != nil || got.Direct || !got.JoinSkipped || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("one that could not be ended is not said to have been", func(t *testing.T) {
		forged := flight("fc-1", now, "exec-7")
		in := forged.input.(ForceCancelInput)
		in.Decision = in.Decision[:len(in.Decision)-6] + "AAAAAA"
		forged.input = in
		c := &flightCluster{flights: []fcInFlight{forged}, endErr: serviceerror.NewUnavailable("down")}
		if got, err := request(t, c, sampleAsk(), 0); err != nil || got.Squatters != 0 || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("a squatter is ended and the request starts", func(t *testing.T) {
		bad := flight("fc-1", now, "exec-7")
		in := bad.input.(ForceCancelInput)
		in.Decision = in.Decision[:len(in.Decision)-6] + "AAAAAA"
		bad.input = in
		c := &flightCluster{flights: []fcInFlight{bad}}
		got, err := request(t, c, sampleAsk(), 0)
		if err != nil || got.Squatters != 1 || got.Attached || len(c.started) != 1 || len(c.terminated) != 1 {
			t.Fatalf("%+v, %v; started %v, terminated %v", got, err, c.started, c.terminated)
		}
	})
	t.Run("a cluster that cannot list starts one all the same", func(t *testing.T) {
		c := &flightCluster{listErr: errors.New("down")}
		if got, err := request(t, c, sampleAsk(), 0); err != nil || got.State != FCStateRequested || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	named := sampleAsk()
	named.ClientRequestID = "retry-1"
	attempt := func(n uint64) string {
		id, err := forceCancelRequestID(keys, named, "exec-7", n)
		must.OK(t, err)
		return ForceCancelWorkflowIDPrefix("payments", "ork-r") + id
	}
	t.Run("the same request again joins itself", func(t *testing.T) {
		c := &flightCluster{attempts: map[string]bool{attempt(0): true}}
		got, err := request(t, c, named, 0)
		if err != nil || !got.Attached || len(c.started) != 0 || !strings.HasSuffix(attempt(0), got.RequestID) {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("one that ended and left the run open is tried again", func(t *testing.T) {
		c := &flightCluster{attempts: map[string]bool{attempt(0): false, attempt(1): false}}
		got, err := request(t, c, named, 0)
		if err != nil || got.Attached || !slices.Equal(c.started, []string{attempt(2)}) {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("one whose run ended answers as itself", func(t *testing.T) {
		run := openRun()
		run.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		c := &flightCluster{run: run, attempts: map[string]bool{attempt(0): false}}
		got, err := request(t, c, named, 0)
		if err != nil || got.State != FCStateAlreadyEnded || !got.Attached || got.Decision.RunID != "ork-r" || got.Decision.RequestID != got.RequestID || len(c.started) != 0 {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	t.Run("a named request ends it under an id no attempt ever had", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")},
			attempts: map[string]bool{attempt(0): false}}
		got, err := request(t, c, named, past)
		if err != nil || !got.Direct || len(c.started) != 1 || strings.HasSuffix(attempt(0), got.RequestID) || !strings.HasSuffix(c.started[0], got.RequestID) {
			t.Fatalf("%+v, %v; started %v", got, err, c.started)
		}
	})
	// attempts is a cluster holding five closed ones, each started with the
	// grace of its own decision, and each lasting lasted.
	attempts := func(grace int64, lasted time.Duration) *flightCluster {
		c := &flightCluster{attempts: map[string]bool{}, lasted: map[string]time.Duration{}, starts: map[string]ForceCancelInput{}}
		for n := range uint64(5) {
			d := sampleDecision()
			d.RequestID, d.TargetExecutionRunID, d.GraceSeconds = strings.TrimPrefix(attempt(n), ForceCancelWorkflowIDPrefix("payments", "ork-r")), "exec-7", grace
			d.CeilingSeconds, d.IssuedAt, d.ExpiresAt = int64(settings.Ceiling/time.Second), now, now.Add(6*time.Hour)
			msg, err := SignForceCancelDecision(keys, d)
			must.OK(t, err)
			c.attempts[attempt(n)], c.lasted[attempt(n)], c.starts[attempt(n)] = false, lasted, ForceCancelInput{Decision: msg}
		}
		return c
	}
	t.Run("one whose run's end it finished is not made again, and one it did not is", func(t *testing.T) {
		run := openRun()
		run.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		for _, tt := range []struct {
			result ForceCancelResult
			again  bool
		}{
			{ForceCancelResult{State: FCStateForceCanceled, Released: true, TerminalPublished: true}, false},
			{ForceCancelResult{State: FCStateCanceled}, false},
			{ForceCancelResult{State: FCStateForceCanceled, Released: true, Cause: FCCauseEventUnpublished}, true},
			{ForceCancelResult{State: FCStateFailed, Cause: FCCauseUnclassified, Terminated: true}, true},
			{ForceCancelResult{State: FCStateFailed, Cause: FCCauseKeysUnavailable}, false},
		} {
			c := attempts(60, given)
			c.run, c.result = run, tt.result
			for n := uint64(1); n < 5; n++ {
				delete(c.attempts, attempt(n))
			}
			got, err := request(t, c, named, 0)
			if err != nil || got.Direct || tt.again != (got.State == FCStateRequested) || tt.again != slices.Equal(c.started, []string{attempt(1)}) {
				t.Errorf("%+v: %+v, %v; started %v", tt.result, got, err, c.started)
			}
		}
		// Every attempt left it unfinished: an ended run is not ended again.
		c := attempts(60, given)
		c.run, c.result = run, ForceCancelResult{State: FCStateForceCanceled}
		if _, err := request(t, c, named, 0); !errors.Is(err, ErrForceCancelRetriesExhausted) || len(c.terminated) != 0 {
			t.Errorf("%v; terminated %v", err, c.terminated)
		}
	})
	t.Run("a run five attempts left open is ended here", func(t *testing.T) {
		c := attempts(60, given)
		got, err := request(t, c, named, 0)
		if err != nil || !got.Direct || len(c.started) != 1 || len(c.terminated) != 1 || c.terminated[0].GetWorkflowExecution().GetRunId() != "exec-7" {
			t.Fatalf("%+v, %v; started %v, terminated %v: want the run ended here", got, err, c.started, c.terminated)
		}
	})
	t.Run("five attempts that ended early end nothing", func(t *testing.T) {
		early := attempts(60, 0)
		early.lasted[attempt(0)] = given
		unread := attempts(60, given)
		unread.starts = nil
		// Each attempt's own time, not the one asked for now, is its whole time.
		for name, c := range map[string]*flightCluster{"ended early": early, "unread": unread, "asked for longer": attempts(600, given)} {
			if _, err := request(t, c, named, 0); !errors.Is(err, ErrForceCancelRetriesExhausted) || len(c.terminated) != 0 {
				t.Fatalf("%s: %v; terminated %v", name, err, c.terminated)
			}
		}
	})
	t.Run("what could not be started ends nothing", func(t *testing.T) {
		ask := sampleAsk()
		ask.Principal = "a\nb"
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}}
		if _, err := request(t, c, ask, past); !errors.Is(err, ErrForceCancelText) || len(c.terminated) != 0 {
			t.Fatalf("%v; terminated %v", err, c.terminated)
		}
	})
	t.Run("a run that closes as it is ended is not said to have been", func(t *testing.T) {
		c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")}, endErr: serviceerror.NewNotFound("closed")}
		if got, err := request(t, c, sampleAsk(), past); err != nil || got.Direct || len(c.started) != 1 {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	t.Run("a run asked before all that was read is ended once that is its grace old", func(t *testing.T) {
		for read, direct := range map[time.Time]bool{now: true, now.Add(past - 30*time.Second): false} {
			c := &flightCluster{flights: []fcInFlight{flight("fc-3", now, "exec-7")}, closedFlights: []fcInFlight{whole("fc-1"), whole("fc-2")},
				unasked: true, readTo: read, cancelErr: serviceerror.NewCancellationAlreadyRequested("asked")}
			if got, err := request(t, c, sampleAsk(), past); err != nil || got.Direct != direct {
				t.Fatalf("%+v, %v; want direct %t", got, err, direct)
			}
		}
	})
	t.Run("joining itself answers the decision in force", func(t *testing.T) {
		f := flight("fc-1", now, "exec-7")
		d := sampleDecision()
		d.RequestID, d.Reason, d.TargetExecutionRunID, d.IssuedAt, d.ExpiresAt = strings.TrimPrefix(attempt(0), ForceCancelWorkflowIDPrefix("payments", "ork-r")), "the first reason", "exec-7", now, now.Add(time.Hour)
		msg, err := SignForceCancelDecision(keys, d)
		must.OK(t, err)
		f.id, f.input = attempt(0), ForceCancelInput{Decision: msg}
		c := &flightCluster{attempts: map[string]bool{attempt(0): true}, ownStart: &f}
		got, err := request(t, c, named, 0)
		if err != nil || got.Decision.Reason != "the first reason" {
			t.Fatalf("%+v, %v", got, err)
		}
	})
}

// startsOf is c's client, whose run starts as a run of its namespace does.
func startsOf(t *testing.T, c *flightCluster) client.Client {
	t.Helper()
	c.runStart = startEvent(t, "payments")
	return c.client()
}

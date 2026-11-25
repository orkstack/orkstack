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

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// asked is a query's answer.
type asked struct{ cascade *ForceCancelCascade }

func (a asked) HasValue() bool { return true }
func (a asked) Get(out any) error {
	*out.(*ForceCancelCascade) = *a.cascade
	return nil
}

// TestForceCancelStatus: a force cancel is what its own verified decision says
// it is, and reports what its result does; the newest is the run's.
func TestForceCancelStatus(t *testing.T) {
	ctx, keys, other := context.Background(), recordKeys(t), recordKeys(t)
	now := time.Now().UTC()
	cascades := false
	signed := func(k ForceCancelKeys, id, run string) ForceCancelInput {
		d := sampleDecision()
		d.Cascade = cascades
		d.RequestID, d.RunID, d.IssuedAt, d.ExpiresAt = id, run, now.Add(-time.Minute), now.Add(time.Hour)
		msg, err := SignForceCancelDecision(k, d)
		must.OK(t, err)
		return ForceCancelInput{Decision: msg}
	}
	var query ForceCancelCascade
	var queryErr error
	var stopped []string
	prefix := ForceCancelWorkflowIDPrefix("payments", "ork-r")
	const running, completed = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	starter := func(execs map[string]ForceCancelInput, at enumspb.WorkflowExecutionStatus, result ForceCancelResult, broken error) *ForceCancelStarter {
		c := &flightCluster{}
		for id, in := range execs {
			started := now
			if id == "fc-old" {
				started = now.Add(-time.Hour)
			}
			c.flights = append(c.flights, fcInFlight{id: prefix + id, exec: "exec-of-" + id, started: started, input: in})
		}
		m := &mocks.Client{}
		m.On("WorkflowService").Return(c)
		out := &workflowservice.ListWorkflowExecutionsResponse{}
		for _, f := range c.flights {
			out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: f.id, RunId: f.exec},
				StartTime: timestamppb.New(f.started), TaskQueue: "ork-tasks"})
		}
		out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: prefix + "elsewhere", RunId: "e"}, StartTime: timestamppb.New(now.Add(time.Hour)), TaskQueue: "ork-dead"})
		m.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(in *workflowservice.ListWorkflowExecutionsRequest) bool {
			return strings.Contains(in.GetQuery(), fmt.Sprintf("WorkflowId STARTS_WITH %q", prefix))
		})).Return(out, broken)
		describe := m.On("DescribeWorkflow", mock.Anything, mock.Anything, mock.Anything)
		describe.Run(func(args mock.Arguments) {
			for _, f := range c.flights {
				if f.id == args.String(1) {
					describe.ReturnArguments = mock.Arguments{&client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{WorkflowId: f.id, RunId: f.exec}, Type: &commonpb.WorkflowType{Name: ForceCancelWorkflowType}, StartTime: timestamppb.New(f.started), Status: at}}}, broken}
					return
				}
			}
			describe.ReturnArguments = mock.Arguments{nil, serviceerror.NewNotFound("none")}
		})
		run := &mocks.WorkflowRun{}
		run.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) { *args.Get(1).(*ForceCancelResult) = result }).Return(nil)
		m.On("GetWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(run)
		m.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, forceCancelCascadeQuery).Return(asked{&query}, func(context.Context, string, string, string, ...interface{}) error { return queryErr })
		m.On("CancelWorkflow", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { stopped = append(stopped, args.String(1)+" "+args.String(2)) }).Return(nil)
		return NewForceCancelStarter(ForceCancelDeps{Client: m, ClusterNamespace: "ork-main", Keys: keys, Mode: func() string { return "enforce" }}, DefaultForceCancelSettings())
	}
	status := func(requestID string, execs map[string]ForceCancelInput, at enumspb.WorkflowExecutionStatus, result ForceCancelResult, broken error) (ForceCancelStatus, error) {
		return starter(execs, at, result, broken).Status(ctx, "payments", "ork-r", requestID)
	}
	mine := map[string]ForceCancelInput{"fc-1": signed(keys, "fc-1", "ork-r")}
	for _, tt := range []struct {
		name   string
		at     enumspb.WorkflowExecutionStatus
		result ForceCancelResult
		want   ForceCancelStatus
	}{
		{"running", running, ForceCancelResult{}, ForceCancelStatus{Running: true, State: FCStateRequested}},
		{"done", completed, ForceCancelResult{State: FCStateForceCanceled, Released: true, TerminalPublished: true}, ForceCancelStatus{State: FCStateForceCanceled}},
		{"not announced", completed, ForceCancelResult{State: FCStateForceCanceled, Released: true, Cause: FCCauseEventUnpublished}, ForceCancelStatus{State: FCStateForceCanceled, Cause: FCCauseEventUnpublished, Incomplete: true}},
		{"not released", completed, ForceCancelResult{State: FCStateForceCanceled, TerminalPublished: true}, ForceCancelStatus{State: FCStateForceCanceled, Incomplete: true}},
		{"a finishing one that did not", completed, ForceCancelResult{State: FCStateAlreadyEnded, Finishing: true, Released: true}, ForceCancelStatus{State: FCStateAlreadyEnded, Incomplete: true}},
		{"the run ended itself", completed, ForceCancelResult{State: FCStateCanceled}, ForceCancelStatus{State: FCStateCanceled}},
		{"a cause no engine wrote", completed, ForceCancelResult{State: FCStateFailed, Cause: "a\nb"}, ForceCancelStatus{State: FCStateFailed}},
		{"out of time", enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, ForceCancelResult{}, ForceCancelStatus{State: FCStateFailed, Cause: FCCauseTimedOut}},
		{"terminated", enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, ForceCancelResult{}, ForceCancelStatus{State: FCStateFailed, Cause: FCCauseStoppedOutside}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, id := range []string{"", "fc-1"} {
				got, err := status(id, mine, tt.at, tt.result, nil)
				must.OK(t, err)
				if got.Decision.RequestID != "fc-1" || got.Decision.Principal != sampleDecision().Principal || !got.StartedAt.Equal(now) {
					t.Errorf("%+v", got)
				}
				got.Decision, got.StartedAt = ForceCancelDecision{}, time.Time{}
				if got != tt.want {
					t.Errorf("%+v, want %+v", got, tt.want)
				}
			}
		})
	}
	t.Run("the newest is the run's", func(t *testing.T) {
		got, err := status("", map[string]ForceCancelInput{"fc-old": signed(keys, "fc-old", "ork-r"), "fc-1": mine["fc-1"]}, running, ForceCancelResult{}, nil)
		if err != nil || got.Decision.RequestID != "fc-1" {
			t.Fatalf("%+v, %v", got, err)
		}
	})
	for name, execs := range map[string]map[string]ForceCancelInput{
		"none": nil, "a forged one": {"fc-1": signed(other, "fc-1", "ork-r")}, "a copy under another id": {"fc-2": mine["fc-1"]}, "another run's": {"fc-1": signed(keys, "fc-1", "ork-s")},
		"one nobody signed": {"fc-1": {}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, id := range []string{"", "fc-1", "fc-2"} {
				// One under a key not known may be judged later, and is no answer now.
				got, err := status(id, execs, running, ForceCancelResult{}, nil)
				if got != (ForceCancelStatus{}) || err == nil || name != "a forged one" && !errors.Is(err, ErrForceCancelNotFound) {
					t.Fatalf("%q: %+v, %v", id, got, err)
				}
			}
		})
	}
	t.Run("a cluster that does not answer is no answer", func(t *testing.T) {
		for _, id := range []string{"", "fc-1"} {
			if _, err := status(id, mine, running, ForceCancelResult{}, errors.New("down")); err == nil || errors.Is(err, ErrForceCancelNotFound) {
				t.Fatalf("%q: %v", id, err)
			}
		}
	})
	if _, err := status("fc\n1", mine, running, ForceCancelResult{}, nil); !errors.Is(err, ErrForceCancelText) {
		t.Errorf("a request id that hides text = %v", err)
	}
	cascades = true
	theirs := map[string]ForceCancelInput{"fc-1": signed(keys, "fc-1", "ork-r")}
	rows := []FCBelowResult{{FCBelow: FCBelow{RunID: "a", ParentRunID: "ork-r", Orkflow: "pay", Depth: 1}, State: FCStateForceCanceled, RequestID: "fc-1-a"}}
	done := ForceCancelCascade{State: FCCascadeComplete, Results: rows, Excluded: map[string]string{"z": FCExcludedNamespace}, After: &FCAfter{RunIDs: []string{"b"}, Count: 1, Unconfirmed: FCCauseTooManyRuns}}
	t.Run("a cascade", func(t *testing.T) {
		query = done
		for name, tt := range map[string]struct {
			at     enumspb.WorkflowExecutionStatus
			result ForceCancelResult
			err    error
			want   ForceCancelCascade
		}{
			"running":                     {running, ForceCancelResult{}, nil, func() ForceCancelCascade { c := done; c.State = FCCascadeRunning; return c }()},
			"running, and not saying":     {running, ForceCancelResult{}, errors.New("no worker"), ForceCancelCascade{State: FCCascadeRunning, Unread: true}},
			"done":                        {completed, ForceCancelResult{State: FCStateForceCanceled, Cascade: &done}, nil, done},
			"over without it":             {completed, ForceCancelResult{State: FCStateFailed}, nil, func() ForceCancelCascade { c := done; c.State = FCCascadeStopped; return c }()},
			"over, and said to run":       {completed, ForceCancelResult{State: FCStateEnded, Cascade: &ForceCancelCascade{State: FCCascadeRunning}}, nil, ForceCancelCascade{State: FCCascadeStopped}},
			"terminated":                  {enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, ForceCancelResult{}, nil, func() ForceCancelCascade { c := done; c.State = FCCascadeStopped; return c }()},
			"terminated, not to be asked": {enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, ForceCancelResult{}, errors.New("no worker"), ForceCancelCascade{State: FCCascadeStopped, Unread: true}},
		} {
			queryErr = tt.err
			got, err := status("fc-1", theirs, tt.at, tt.result, nil)
			if err != nil || got.Cascade == nil || !reflect.DeepEqual(*got.Cascade, tt.want) {
				t.Errorf("%s: %+v, %v; want %+v", name, got.Cascade, err, tt.want)
			}
		}
		queryErr = nil
	})
	t.Run("nothing that hides text is shown of one", func(t *testing.T) {
		odd := ForceCancelCascade{State: FCCascadeComplete, Truncated: "a\nb", Excluded: map[string]string{"z\n": "x"}, After: &FCAfter{RunIDs: []string{"b\n", "c"}, Unconfirmed: "x\ny"},
			Results: append([]FCBelowResult{{FCBelow: FCBelow{RunID: "a\u202e"}}, {FCBelow: FCBelow{Orkflow: "a\n"}}, {Cause: "a\n"}, {RequestID: "a\n"}, {FCBelow: FCBelow{ParentRunID: "a\n"}}}, rows...)}
		for range 2 * forceCancelCascadeMost {
			odd.Results = append(odd.Results, rows...)
		}
		got := odd.shown()
		if got.State != FCCascadePartial || got.Truncated != "" || got.Excluded != nil || !slices.Equal(got.After.RunIDs, []string{"c"}) || got.After.Unconfirmed != "" || len(got.Results) == 0 || len(got.Results) > 2*forceCancelCascadeMost || got.Results[0].RunID != "a" {
			t.Errorf("%d rows, %+v, %+v", len(got.Results), got.After, got.Excluded)
		}
	})
	for name, odd := range map[string]ForceCancelCascade{
		"a row": {Results: []FCBelowResult{{Cause: "a\n"}}}, "one left out": {Excluded: map[string]string{"z\n": "x"}}, "one open": {After: &FCAfter{RunIDs: []string{"b\n"}}},
		"why it is not known": {After: &FCAfter{Unconfirmed: "x\ny"}}, "what cut it short": {Truncated: "a\nb"},
	} {
		odd.State = FCCascadeComplete
		if got := odd.shown(); got.State != FCCascadePartial {
			t.Errorf("%s withheld leaves it %s", name, got.State)
		}
	}
	t.Run("a cascade is stopped", func(t *testing.T) {
		id, err := starter(theirs, running, ForceCancelResult{}, nil).Stop(ctx, "payments", "ork-r")
		if err != nil || id != "fc-1" || !slices.Equal(stopped, []string{prefix + "fc-1 exec-of-fc-1"}) {
			t.Errorf("%q, %v; stopped %v", id, err, stopped)
		}
		for name, s := range map[string]*ForceCancelStarter{"one that is over": starter(theirs, completed, ForceCancelResult{}, nil), "one that does not cascade": starter(mine, running, ForceCancelResult{}, nil)} {
			if id, err := s.Stop(ctx, "payments", "ork-r"); !errors.Is(err, ErrForceCancelNotFound) || len(stopped) != 1 {
				t.Errorf("%s: %q, %v; stopped %v", name, id, err, stopped)
			}
		}
	})
}

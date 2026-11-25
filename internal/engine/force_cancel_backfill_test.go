package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// closedCluster holds a run closed with details and the force cancel that
// record names.
type closedCluster struct {
	workflowservice.WorkflowServiceClient
	details *commonpb.Payloads
	input   any
	status  enumspb.WorkflowExecutionStatus
	gone    bool
	err     error
	at      time.Time
	result  ForceCancelResult
	// ran is how the run closed, terminated when unset; runErr, what
	// describing it answers.
	ran    enumspb.WorkflowExecutionStatus
	runErr error
	// unread is what reading the result answers.
	unread error
}

func (c closedClient) GetWorkflow(context.Context, string, string) client.WorkflowRun {
	return closedResult{result: c.result, err: c.unread}
}

type closedResult struct {
	client.WorkflowRun
	result ForceCancelResult
	err    error
}

func (r closedResult) Get(_ context.Context, out any) error {
	*out.(*ForceCancelResult) = r.result
	return r.err
}

// closedClient is the same cluster as an SDK client.
type closedClient struct {
	client.Client
	*closedCluster
}

func (c closedClient) WorkflowService() workflowservice.WorkflowServiceClient { return c.closedCluster }

func (c closedClient) DescribeWorkflow(_ context.Context, id, _ string) (*client.WorkflowExecutionDescription, error) {
	info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "fc-exec"}, Type: &commonpb.WorkflowType{Name: ForceCancelWorkflowType}, StartTime: timestamppb.New(c.at), Status: c.status}
	if id == "ork-r" {
		info.Execution.RunId, info.Status = "exec-1", enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		if c.ran != 0 {
			info.Status = c.ran
		}
		if c.runErr != nil {
			return nil, c.runErr
		}
	} else if c.gone {
		return nil, serviceerror.NewNotFound("gone")
	}
	return &client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}}, c.err
}

func (c *closedCluster) GetWorkflowExecutionHistoryReverse(context.Context, *workflowservice.GetWorkflowExecutionHistoryReverseRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 9, EventTime: timestamppb.New(c.at), EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{Reason: ForceCancelTerminateReason, Details: c.details}}}}}}, nil
}

func (c *closedCluster) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	input, err := DataConverter().ToPayloads(c.input)
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: input}}}}}}, err
}

// failingAudit refuses every record.
type failingAudit struct{ artifacts.AuditStore }

func (failingAudit) PutAuditRecord(context.Context, *orkstackv1.AuditRecord) error {
	return errors.New("store down")
}

// TestForceCancelBackfill: the records a force cancel could not write are
// written once, as its signed decision and the close's record say, and say that
// they were; and nothing is written from anything else.
func TestForceCancelBackfill(t *testing.T) {
	ctx, keys, other := context.Background(), recordKeys(t), recordKeys(t)
	now := time.Now().UTC()
	d := sampleDecision()
	signed := func(k ForceCancelKeys, d ForceCancelDecision) ForceCancelInput {
		msg, err := SignForceCancelDecision(k, d)
		must.OK(t, err)
		return ForceCancelInput{Decision: msg}
	}
	details := func(k ForceCancelKeys) *commonpb.Payloads {
		p, err := SignForceCancelRecord(k, d.record("exec-1", true, now))
		must.OK(t, err)
		return p
	}
	another := d
	another.RequestID = "fc-9"
	for _, tt := range []struct {
		name    string
		cluster closedCluster
		store   artifacts.AuditStore
		there   []string // written before
		want    []string
		err     bool
		says    string // of the completed one
	}{
		{name: "what it did beyond ending the run is its own result's", cluster: closedCluster{details: details(keys), input: signed(keys, d), result: ForceCancelResult{RequestID: d.RequestID, State: FCStateForceCanceled, Terminated: true, Released: true, TerminalPublished: true}},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: "slots released, ending published"},
		{name: "one that lacks something says so", cluster: closedCluster{details: details(keys), input: signed(keys, d), result: ForceCancelResult{RequestID: d.RequestID, State: FCStateForceCanceled, Cause: FCCauseEventUnpublished}},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseEventUnpublished},
		{name: "another's result is not its own", cluster: closedCluster{details: details(keys), input: signed(keys, d), result: ForceCancelResult{RequestID: "fc-9", State: FCStateForceCanceled, Released: true, TerminalPublished: true}},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseUnclassified},
		{name: "one whose result shows what no engine writes", cluster: closedCluster{details: details(keys), input: signed(keys, d), result: ForceCancelResult{RequestID: d.RequestID, State: FCStateForceCanceled, EndedBy: "a\nb", Released: true, TerminalPublished: true}},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseUnclassified},
		{name: "one whose result no read can read", cluster: closedCluster{details: details(keys), input: signed(keys, d), unread: errors.New("unable to decode")},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseUnclassified},
		{name: "a result the cluster does not answer now", cluster: closedCluster{details: details(keys), input: signed(keys, d), unread: serviceerror.NewUnavailable("down")}, err: true},
		{name: "one stopped from outside", cluster: closedCluster{details: details(keys), input: signed(keys, d), status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseStoppedOutside},
		{name: "one that answered nothing", cluster: closedCluster{details: details(keys), input: signed(keys, d), status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, result: ForceCancelResult{RequestID: d.RequestID, State: FCStateForceCanceled, Released: true, TerminalPublished: true}},
			want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}, says: FCCauseTimedOut},
		{name: "both", cluster: closedCluster{details: details(keys), input: signed(keys, d)}, want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}},
		{name: "one was there", cluster: closedCluster{details: details(keys), input: signed(keys, d)}, there: []string{forceCancelRequested}, want: []string{"run.cancel.force.completed", "run.cancel.force.requested"}},
		{name: "one that still runs writes its own", cluster: closedCluster{details: details(keys), input: signed(keys, d), status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}},
		{name: "one gone from the cluster", cluster: closedCluster{details: details(keys), input: signed(keys, d), gone: true}},
		{name: "a close by nobody's record", cluster: closedCluster{details: details(other), input: signed(keys, d)}},
		{name: "a decision that is no force cancel's", cluster: closedCluster{details: details(keys), input: "?"}},
		{name: "a decision of another request", cluster: closedCluster{details: details(keys), input: signed(keys, another)}},
		{name: "a cluster that does not answer", cluster: closedCluster{details: details(keys), input: signed(keys, d), err: errors.New("down")}, err: true},
		{name: "a store that refuses them", cluster: closedCluster{details: details(keys), input: signed(keys, d)}, store: failingAudit{}, err: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.cluster
			c.at = now
			if c.status == 0 {
				c.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
			memory := artifacts.NewMemoryAuditStore()
			var store artifacts.AuditStore = memory
			if tt.store != nil {
				store = tt.store
			}
			a := NewForceCancelActivities(ForceCancelDeps{Client: closedClient{closedCluster: &c}, ClusterNamespace: "ork-main", Keys: keys, Mode: func() string { return "enforce" }, Audit: store})
			for _, kind := range tt.there {
				rec, err := a.auditRecord(kind, d, ForceCancelResult{})
				must.OK(t, err)
				must.OK(t, a.put(ctx, rec))
			}
			for range 2 {
				if err := a.Backfill(ctx, "payments", "ork-r"); (err != nil) != tt.err {
					t.Fatalf("%v", err)
				}
			}
			page, err := memory.ListAuditRecords(ctx, artifacts.AuditQuery{Since: now.Add(-time.Hour), Until: now.Add(time.Hour), Limit: 10})
			must.OK(t, err)
			var got []string
			for _, rec := range page.Records {
				got = append(got, rec.GetAction())
				if rec.GetPrincipal() != d.Principal || !rec.GetPrincipalVerified() || rec.GetTarget() != "ork-r" ||
					strings.Contains(rec.GetDetail(), "; backfilled ") == (len(tt.there) > 0 && rec.GetAction() == tt.there[0]) {
					t.Errorf("%v", rec)
				}
			}
			for _, rec := range page.Records {
				if rec.GetAction() != forceCancelCompleted {
					continue
				}
				claims := strings.Contains(rec.GetDetail(), "released") || strings.Contains(rec.GetDetail(), "published")
				if whole := strings.Contains(tt.says, "released"); claims != whole || rec.GetSucceeded() != whole || !strings.Contains(rec.GetDetail()+" "+rec.GetErrorCode(), tt.says) {
					t.Errorf("%v", rec)
				}
			}
			if len(got) != len(tt.want) {
				t.Errorf("recorded %v, want %v", got, tt.want)
			}
		})
	}
	if err := NewForceCancelActivities(ForceCancelDeps{}).Backfill(ctx, "payments", "ork-r"); err != nil {
		t.Errorf("with nowhere to record: %v", err)
	}
}

// TestForceCancelStatus_LostOnceItEndedTheRun: a force cancel that ended the
// run and was then lost itself says both, one whose run another ended says so,
// and neither is said to have left it open.
func TestForceCancelStatus_LostOnceItEndedTheRun(t *testing.T) {
	keys, now, d := recordKeys(t), time.Now().UTC(), sampleDecision()
	details := func(k ForceCancelKeys, request string) *commonpb.Payloads {
		ended := d
		ended.RequestID = request
		p, err := SignForceCancelRecord(k, ended.record("exec-1", true, now))
		must.OK(t, err)
		return p
	}
	var noKeys *identity.Issuer
	failed := ForceCancelStatus{State: FCStateFailed, Cause: FCCauseTimedOut}
	ended := ForceCancelStatus{State: FCStateAlreadyEnded, Cause: FCCauseTimedOut}
	for _, tt := range []struct {
		name string
		c    closedCluster
		keys ForceCancelKeys
		want ForceCancelStatus
		err  error
	}{
		{"it ended the run", closedCluster{details: details(keys, d.RequestID)}, keys, ForceCancelStatus{State: FCStateForceCanceled, Cause: FCCauseTimedOut, Incomplete: true, Sent: true}, nil},
		{"another ended it", closedCluster{details: details(keys, "fc-9")}, keys, ended, nil},
		{"nobody's record", closedCluster{details: details(recordKeys(t), d.RequestID)}, keys, failed, nil},
		{"the keys cannot be read", closedCluster{details: details(keys, d.RequestID)}, noKeys, ForceCancelStatus{}, ErrForceCancelUnverifiable},
		{"it ended by itself", closedCluster{ran: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED}, keys, ended, nil},
		{"it is open", closedCluster{ran: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, keys, failed, nil},
		{"it is gone from the cluster", closedCluster{runErr: serviceerror.NewNotFound("gone")}, keys, failed, nil},
		{"the cluster does not answer", closedCluster{runErr: errDown}, keys, ForceCancelStatus{}, errDown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.c
			c.at = now
			s := NewForceCancelActivities(ForceCancelDeps{Client: closedClient{closedCluster: &c}, ClusterNamespace: "ork-main", Keys: tt.keys, Mode: func() string { return "enforce" }}).starting()
			got, err := s.status(context.Background(), fcFlight{workflowID: d.WorkflowID(), execution: "fc-exec"}, d, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT)
			got.Decision = ForceCancelDecision{}
			if !errors.Is(err, tt.err) || got != tt.want {
				t.Errorf("%+v, %v; want %+v, %v", got, err, tt.want, tt.err)
			}
		})
	}
}

// errDown is a cluster that does not answer.
var errDown = errors.New("down")

// TestHistoryArchiver_ForceCancelBackfill: the records are written before the
// run's archive, which waits for them unless there will be no later pass.
func TestHistoryArchiver_ForceCancelBackfill(t *testing.T) {
	for _, tt := range []struct {
		name     string
		code     string
		err      error
		last     bool
		asked    int
		archived bool
	}{
		{"written", RunForceCanceledCode, nil, false, 1, true},
		{"refused", RunForceCanceledCode, errors.New("store down"), false, 1, false},
		{"refused, and no later pass", RunForceCanceledCode, errors.New("store down"), true, 1, true},
		{"a run nobody force-canceled", "RUN_CANCELED", errors.New("store down"), false, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			asked := 0
			h := (&HistoryArchiver{}).WithForceCancelBackfill(func(_ context.Context, namespace, runID string) error {
				asked++
				if namespace != "default" || runID != "ork-r" {
					t.Errorf("asked of %s/%s", namespace, runID)
				}
				return tt.err
			})
			err := h.backfilled(context.Background(), &orkstackv1.Run{Id: "ork-r", Error: &orkstackv1.Error{Code: tt.code}}, tt.last)
			if (err == nil) != tt.archived || asked != tt.asked {
				t.Errorf("%v after %d", err, asked)
			}
		})
	}
}

package engine

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func sampleDecision() ForceCancelDecision {
	now := time.Now().UTC()
	return ForceCancelDecision{RequestID: "fc-1", Namespace: "payments", RunID: "ork-r", TargetExecutionRunID: "exec-1",
		Principal: "user:corp:alice", PrincipalVerified: true, Permission: "run.cancel.force", Scope: "orkflow:payments/pay",
		Reason: "stuck", GraceSeconds: 60, CeilingSeconds: 1800, IssuedAt: now, ExpiresAt: now.Add(time.Hour)}
}

var _ ForceCancelKeys = KeylessEngine{}

// fcActivities are activities running in d's own force cancel, started now.
func fcActivities(t *testing.T, keys ForceCancelKeys, d ForceCancelDecision) (*ForceCancelActivities, artifacts.AuditStore) {
	t.Helper()
	store, err := artifacts.NewAuditStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	a := &ForceCancelActivities{keys: keys, mode: func() string { return "enforce" }, now: time.Now, audit: store}
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: time.Now(), Local: true}, nil
	}
	return a, store
}

// TestForceCancelDecide: a step acts on a decision only when it verifies, in
// the one execution it was made for, while it is fresh; what could not be
// asked is an error and blames no decision.
func TestForceCancelDecide(t *testing.T) {
	keys, other := recordKeys(t), recordKeys(t)
	var noKeys *identity.Issuer
	sign := func(t *testing.T, k *identity.Issuer, d ForceCancelDecision) ForceCancelInput {
		t.Helper()
		msg, err := SignForceCancelDecision(k, d)
		must.OK(t, err)
		return ForceCancelInput{Decision: msg}
	}
	own := forceCancelSelf{WorkflowID: sampleDecision().WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: time.Now(), Local: true}
	unsigned := func(d ForceCancelDecision) ForceCancelInput { return ForceCancelInput{Unsigned: &d} }
	asserted := sampleDecision()
	asserted.PrincipalVerified = false
	braced := sampleDecision()
	braced.Namespace = strings.Repeat("n", '{')
	raw := func(body []byte) string {
		msg, err := keys.SignDecision(jwt.KindForceCancelRoot, body)
		must.OK(t, err)
		return msg
	}
	plain, err := stablejson.Marshal(sampleDecision())
	must.OK(t, err)
	boundElsewhere, cut := raw(append(identity.Bind("payments", "ork-other"), plain...)), raw([]byte{0, 0, 0, 200, 'x'})
	expired := sampleDecision()
	expired.IssuedAt, expired.ExpiresAt = time.Now().Add(-time.Minute), time.Now().Add(time.Minute)
	longGone := sampleDecision()
	longGone.IssuedAt, longGone.ExpiresAt = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
	tests := []struct {
		name  string
		keys  ForceCancelKeys
		mode  string
		in    ForceCancelInput
		self  func(*forceCancelSelf)
		cause string // "" decides
		err   bool
	}{
		{"its own", keys, "enforce", sign(t, keys, sampleDecision()), nil, "", false},
		{"another execution's id", keys, "enforce", sign(t, keys, sampleDecision()), func(s *forceCancelSelf) { s.WorkflowID += "x" }, FCCauseDecisionUnverified, false},
		{"another workflow's type", keys, "enforce", sign(t, keys, sampleDecision()), func(s *forceCancelSelf) { s.Type = "Other" }, FCCauseDecisionUnverified, false},
		{"a key nobody published", keys, "enforce", sign(t, other, sampleDecision()), nil, FCCauseDecisionUnverified, false},
		{"no decision", keys, "enforce", ForceCancelInput{}, nil, FCCauseDecisionUnverified, false},
		{"both", keys, "enforce", ForceCancelInput{Decision: sign(t, keys, sampleDecision()).Decision, Unsigned: &asserted}, nil, FCCauseDecisionUnverified, false},
		{"expired since it started", keys, "enforce", sign(t, keys, expired), nil, "", false},
		{"started after it expired, long ago", keys, "enforce", sign(t, keys, longGone), nil, FCCauseDecisionStale, false},
		{"a namespace whose length reads as a brace", keys, "enforce", sign(t, keys, braced), func(s *forceCancelSelf) { s.WorkflowID = braced.WorkflowID() }, "", false},
		{"a body bound to another run", keys, "enforce", ForceCancelInput{Decision: boundElsewhere}, nil, FCCauseDecisionUnverified, false},
		{"a body cut short", keys, "enforce", ForceCancelInput{Decision: cut}, nil, FCCauseDecisionUnverified, false},
		{"a step no workflow code runs", keys, "enforce", sign(t, keys, sampleDecision()), func(s *forceCancelSelf) { s.Local = false }, FCCauseDecisionUnverified, false},
		{"started after it expired", keys, "enforce", sign(t, keys, sampleDecision()), func(s *forceCancelSelf) { s.StartedAt = time.Now().Add(2 * time.Hour) }, FCCauseDecisionStale, false},
		{"started long after it was decided", keys, "enforce", sign(t, keys, sampleDecision()), func(s *forceCancelSelf) { s.StartedAt = time.Now().Add(forceCancelStartWithin + time.Minute) }, FCCauseDecisionStale, false},
		{"unsigned where callers are verified", keys, "enforce", unsigned(asserted), nil, FCCauseDecisionUnverified, false},
		{"unsigned under observe", keys, "observe", unsigned(asserted), nil, FCCauseDecisionUnverified, false},
		{"unsigned in asserted mode", noKeys, "asserted", unsigned(asserted), nil, "", false},
		{"unsigned, naming someone verified", noKeys, "asserted", unsigned(sampleDecision()), nil, FCCauseDecisionUnverified, false},
		{"unsigned under a mode nobody knows", keys, "", unsigned(asserted), nil, "", true},
		{"keys that cannot be read", noKeys, "enforce", sign(t, keys, sampleDecision()), nil, "", true},
	}
	for name, mutate := range map[string]func(*ForceCancelDecision){
		"no grace":                           func(d *ForceCancelDecision) { d.GraceSeconds = 0 },
		"a grace the clocks' allowance eats": func(d *ForceCancelDecision) { d.GraceSeconds = 29 },
		"a ceiling past a day":               func(d *ForceCancelDecision) { d.GraceSeconds, d.CeilingSeconds = 200, 86401 },
		"more windows than a history holds":  func(d *ForceCancelDecision) { d.GraceSeconds, d.CeilingSeconds = 30, 30*501 },
		"a life past a day":                  func(d *ForceCancelDecision) { d.ExpiresAt = d.IssuedAt.Add(25 * time.Hour) },
		"how nobody signed in":               func(d *ForceCancelDecision) { d.AuthMethod = "oidc" },
		"whose issuer":                       func(d *ForceCancelDecision) { d.PrincipalIssuer = "https://idp" },
		"a rule":                             func(d *ForceCancelDecision) { d.Rule = "admins" },
		"a policy":                           func(d *ForceCancelDecision) { d.PolicyGeneration = 1 },
		"text that hides":                    func(d *ForceCancelDecision) { d.Reason = "stuck\u202e" },
		"a run id that hides text":           func(d *ForceCancelDecision) { d.RunID = "ork-\u202er" },
		"a namespace that hides text":        func(d *ForceCancelDecision) { d.Namespace = "pay\u200bments" },
		"a ceiling below it":                 func(d *ForceCancelDecision) { d.CeilingSeconds = 59 },
		"a ceiling past all":                 func(d *ForceCancelDecision) { d.CeilingSeconds = 1 << 60 },
		"no target":                          func(d *ForceCancelDecision) { d.TargetExecutionRunID = "" },
		"text no one may see":                func(d *ForceCancelDecision) { d.Reason = "a\nb" },
	} {
		d := asserted
		mutate(&d)
		tests = append(tests, struct {
			name  string
			keys  ForceCancelKeys
			mode  string
			in    ForceCancelInput
			self  func(*forceCancelSelf)
			cause string
			err   bool
		}{"unsigned with " + name, noKeys, "asserted", unsigned(d), func(s *forceCancelSelf) { s.WorkflowID = d.WorkflowID() }, FCCauseDecisionUnverified, false})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			self := own
			if tt.self != nil {
				tt.self(&self)
			}
			// Its own clock, hours on, decides nothing: what a force cancel began
			// while its decision was fresh, it finishes.
			a := &ForceCancelActivities{keys: tt.keys, mode: func() string { return tt.mode }, now: func() time.Time { return time.Now().Add(6 * time.Hour) }}
			d, cause, err := a.decide(context.Background(), tt.in, self)
			if (err != nil) != tt.err || cause != tt.cause || (d != nil) != (tt.cause == "" && !tt.err) {
				t.Fatalf("decide = %+v, %q, %v; want cause %q, error %v", d, cause, err, tt.cause, tt.err)
			}
		})
	}
	// A decision whose record would not fit its termination is refused before
	// anything is asked of the run.
	big := sampleDecision()
	big.Reason, big.Principal, big.PrincipalIssuer, big.Scope = strings.Repeat(`"`, 512), strings.Repeat(`"`, 256), strings.Repeat(`"`, 256), strings.Repeat(`"`, 1024)
	if _, err := SignForceCancelDecision(keys, big); !errors.Is(err, ErrForceCancelRecordTooLarge) {
		t.Fatalf("sign = %v, want ErrForceCancelRecordTooLarge", err)
	}
}

// TestForceCancelAudit: the records of a force cancel say what its verified
// decision says, once each; a refused one names nobody, and at most one a
// minute is written however many are refused.
func TestForceCancelAudit(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	d := sampleDecision()
	a, store := fcActivities(t, keys, d)
	started := time.Now()
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: started, Local: true}, nil
	}
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	in := FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: forceCancelRequested}
	must.OK(t, a.Audit(ctx, in))
	must.OK(t, a.Audit(ctx, in)) // a retry, or another engine's
	in.Kind, in.Result = forceCancelCompleted, ForceCancelResult{State: FCStateFailed, Cause: FCCauseRunKeptSwitching, Terminated: true, ProgressingAtCeiling: true}
	must.OK(t, a.Audit(ctx, in))
	in.Kind = "access.role.create"
	if a.Audit(ctx, in) == nil {
		t.Error("a force cancel wrote a record of something else")
	}
	in.Kind, in.Input = forceCancelCompleted, ForceCancelInput{Decision: "x.y.z"}
	if a.Audit(ctx, in) == nil {
		t.Error("an unverified decision wrote a record")
	}
	// A genuine decision started again is the same request: no new record.
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: started.Add(forceCancelStartWithin / 2), Local: true}, nil
	}
	must.OK(t, a.Audit(ctx, FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: forceCancelRequested}))
	// A write that fails is not one of the minute's: the next try writes it.
	a.audit = refuseOnce{store, new(int)}
	if a.Refuse(ctx, FCCauseDecisionStale) == nil {
		t.Error("a refusal that could not be written was said to be")
	}
	must.OK(t, a.Refuse(ctx, FCCauseDecisionStale))
	must.OK(t, a.Refuse(ctx, FCCauseDecisionStale))
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: "anything at all", Type: ForceCancelWorkflowType, StartedAt: started, Local: true}, nil
	}
	a.refusals.last = time.Time{}
	if a.Refuse(ctx, FCCauseDecisionStale) == nil {
		t.Error("an execution named as no force cancel is wrote a refusal")
	}
	a.refusals.last = a.now()
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: "ork-force-forged\x1b\u202e", Type: ForceCancelWorkflowType, StartedAt: started, Local: true}, nil
	}
	must.OK(t, a.Refuse(ctx, "a cause it claims"))
	a.refusals.last = time.Time{}
	must.OK(t, a.Refuse(ctx, "a cause it claims"))

	page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{Since: started.Add(-time.Hour), Until: started.Add(time.Hour), Limit: 20})
	must.OK(t, err)
	byAction := map[string]int{}
	for _, r := range page.Records {
		byAction[r.GetAction()]++
		switch r.GetAction() {
		case forceCancelRefused:
			if r.GetPrincipal() != "" || r.GetPrincipalVerified() || r.GetNamespace() != "" || r.GetSucceeded() || strings.ContainsAny(r.GetTarget(), "\x1b\u202e") || !strings.HasPrefix(r.GetTarget(), "ork-force-") ||
				r.GetErrorCode() != FCCauseDecisionStale && r.GetErrorCode() != FCCauseDecisionUnverified {
				t.Errorf("a refusal says %v", r)
			}
		case forceCancelCompleted:
			if r.GetSucceeded() || r.GetErrorCode() != FCCauseRunKeptSwitching || !strings.Contains(r.GetDetail(), "terminated") || !strings.Contains(r.GetDetail(), "ceiling") {
				t.Errorf("a failed force cancel is recorded %v", r)
			}
			fallthrough
		default:
			if r.GetPrincipal() != d.Principal || !r.GetPrincipalVerified() || r.GetTarget() != "ork-r" || r.GetNamespace() != "payments" || r.GetScope() != d.Scope {
				t.Errorf("a record says %v", r)
			}
		}
	}
	if byAction[forceCancelRequested] != 1 || byAction[forceCancelCompleted] != 1 || byAction[forceCancelRefused] != 2 || len(page.Records) != 4 {
		t.Errorf("records %v, want one of each and two refusals of three", byAction)
	}
}

// TestForceCancelGates: what a run holds is read from its own record, and what
// cannot be read of it is said.
func TestForceCancelGates(t *testing.T) {
	payload := func(v any) *commonpb.Payload {
		p, err := DataConverter().ToPayload(v)
		must.OK(t, err)
		return p
	}
	attrs := func(held, queued any) *commonpb.SearchAttributes {
		return &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			task.SearchAttrOrkflowConcurrencyHeld: payload(held), task.SearchAttrOrkflowConcurrencyQueued: payload(queued)}}
	}
	entry := func(base int64) *commonpb.Memo {
		return &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoSeqKey: payload(9), gateMemoKey("ork-conc-a", gateRunLevel): payload(gateMemoEntry{SeqBase: base})}}
	}
	tests := []struct {
		name  string
		attrs *commonpb.SearchAttributes
		memo  *commonpb.Memo
		want  FCGates
	}{
		{"nothing held", nil, nil, FCGates{}},
		{"held and queued, once each", attrs([]string{"ork-conc-a"}, []string{"ork-conc-a", "ork-conc-b"}), entry(4), FCGates{Gates: []string{"ork-conc-a", "ork-conc-b"}, SeqBase: 4}},
		{"an attribute that does not decode", attrs("{", []string{"ork-conc-b"}), entry(4), FCGates{Gates: []string{"ork-conc-b"}, SeqBase: 4, Unreadable: true}},
		{"an entry that does not decode", attrs([]string{"ork-conc-a"}, []string{}), &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoKey("ork-conc-a", gateRunLevel): payload("{")}},
			FCGates{Gates: []string{"ork-conc-a"}, Unreadable: true}},
		{"one entry that does not decode beside one that does", attrs([]string{"ork-conc-a"}, []string{}), &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			gateMemoKey("ork-conc-a", gateRunLevel): payload(gateMemoEntry{SeqBase: 4}), gateMemoKey("ork-conc-a", 3): payload("{")}},
			FCGates{Gates: []string{"ork-conc-a"}, SeqBase: 4, Unreadable: true}},
		{"a gate with no entry", attrs([]string{"ork-conc-a"}, []string{}), nil, FCGates{Gates: []string{"ork-conc-a"}, Unreadable: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := forceCancelGates(tt.attrs, tt.memo)
			if !equalStrings(got.Gates, tt.want.Gates) || got.SeqBase != tt.want.SeqBase || got.Unreadable != tt.want.Unreadable {
				t.Fatalf("gates = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestForceCancelClassify: how a close reads to the force cancel fc-1.
func TestForceCancelClassify(t *testing.T) {
	terminated := enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	mine := ForceCancelClose{Verification: ForceCancelVerified, Record: ForceCancelRecord{RequestID: "fc-1"}}
	theirs := mine
	theirs.Record.RequestID = "fc-2"
	tests := []struct {
		status enumspb.WorkflowExecutionStatus
		closed ForceCancelClose
		want   FCClassified
	}{
		{enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, ForceCancelClose{}, FCClassified{Open: true}},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, ForceCancelClose{}, FCClassified{State: FCStateCanceled}},
		{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, ForceCancelClose{}, FCClassified{State: FCStateEnded}},
		{enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, ForceCancelClose{}, FCClassified{State: FCStateEnded}},
		{enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, ForceCancelClose{}, FCClassified{Open: true}},
		{terminated, mine, FCClassified{State: FCStateForceCanceled}},
		{terminated, theirs, FCClassified{State: FCStateAlreadyEnded, Request: "fc-2"}},
		{terminated, ForceCancelClose{}, FCClassified{State: FCStateAlreadyEnded}},
		{terminated, ForceCancelClose{Verification: ForceCancelBad}, FCClassified{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord}},
		{terminated, ForceCancelClose{Verification: ForceCancelNotThisRun}, FCClassified{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord}},
		{terminated, ForceCancelClose{Verification: ForceCancelUnknownKey}, FCClassified{State: FCStateFailed, Cause: FCCauseUnknownKey, Retry: true}},
		{terminated, ForceCancelClose{Verification: ForceCancelUnavailable}, FCClassified{State: FCStateFailed, Cause: FCCauseKeysUnavailable, Retry: true}},
	}
	for _, tt := range tests {
		if got := classifyForceCancel(tt.status, tt.closed, "fc-1"); got != tt.want {
			t.Errorf("%v %v = %+v, want %+v", tt.status, tt.closed.Verification, got, tt.want)
		}
	}
}

// fcExecution is one execution of a run's chain, for the lineage it is asked of.
type fcExecution struct {
	id, continued, namespace, kind string
	reset                          time.Duration // after the decision; 0 never
}

// TestForceCancelTargets: a decision names the run as it is now only when the
// current execution carries on from the one it was made for, in its namespace,
// with no reset since. A resume is a reset; carrying on is not.
func TestForceCancelTargets(t *testing.T) {
	d := sampleDecision()
	chain := func(n int) []fcExecution {
		out := []fcExecution{}
		for i := n; i > 1; i-- {
			out = append(out, fcExecution{id: execID(i), continued: execID(i - 1)})
		}
		return append(out, fcExecution{id: "exec-1"})
	}
	tests := []struct {
		name  string
		chain []fcExecution // newest first
		cause string
	}{
		{"the execution it was made for", chain(1), ""},
		{"carried on since", chain(3), ""},
		{"carried on as far as is walked", chain(forceCancelLineageHops), ""},
		{"carried on further than is walked", chain(forceCancelLineageHops + 1), FCCauseDecisionStale},
		{"resumed since", []fcExecution{{id: "exec-9", reset: time.Minute}}, FCCauseDecisionStale},
		{"carried on, then an execution on the way was reset since", []fcExecution{{id: "exec-2", continued: "exec-1", reset: time.Minute}, {id: "exec-1"}}, FCCauseDecisionStale},
		{"reset before it was made", []fcExecution{{id: "exec-1", reset: -time.Minute}}, ""},
		{"another chain", []fcExecution{{id: "exec-7"}}, FCCauseDecisionStale},
		{"gone", nil, FCCauseDecisionStale},
		{"another namespace's run", []fcExecution{{id: "exec-1", namespace: "billing"}}, FCCauseDecisionUnverified},
		{"no run at all", []fcExecution{{id: "exec-1", kind: "ConcurrencyGateWorkflow"}}, FCCauseDecisionUnverified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, starts := &mocks.Client{}, map[string]*historypb.HistoryEvent{}
			for i, e := range tt.chain {
				namespace, kind := e.namespace, e.kind
				if namespace == "" {
					namespace = "payments"
				}
				if kind == "" {
					kind = RunWorkflowType
				}
				ext := &workflowpb.WorkflowExecutionExtendedInfo{}
				if e.reset != 0 {
					ext.LastResetTime = timestamppb.New(d.IssuedAt.Add(e.reset))
				}
				asked := e.id
				if i == 0 {
					asked = ""
				}
				c.On("DescribeWorkflow", mock.Anything, "ork-r", asked).Return(&client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{RunId: e.id}, Type: &commonpb.WorkflowType{Name: kind}},
					WorkflowExtendedInfo:  ext}}, nil)
				start := startEvent(t, namespace)
				start.GetWorkflowExecutionStartedEventAttributes().ContinuedExecutionRunId = e.continued
				starts[e.id] = start
			}
			c.On("WorkflowService").Return(startsService{starts: starts})
			c.On("DescribeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil, serviceerror.NewNotFound("gone"))
			a := &ForceCancelActivities{client: c, reader: &TemporalRunReader{converter: DataConverter()}}
			cause, err := a.targets(context.Background(), d)
			if err != nil || cause != tt.cause {
				t.Fatalf("targets = %q, %v; want %q", cause, err, tt.cause)
			}
		})
	}
	t.Run("a cluster that cannot be read is no answer", func(t *testing.T) {
		c := &mocks.Client{}
		c.On("DescribeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil, serviceerror.NewUnavailable("down"))
		a := &ForceCancelActivities{client: c, reader: &TemporalRunReader{converter: DataConverter()}}
		if cause, err := a.targets(context.Background(), d); err == nil {
			t.Fatalf("targets = %q with no error", cause)
		}
	})
}

// startsService answers the first event of each execution, and no more of it:
// a chain of executions is walked without reading any through.
type startsService struct {
	workflowservice.WorkflowServiceClient
	starts map[string]*historypb.HistoryEvent
}

func (s startsService) GetWorkflowExecutionHistory(_ context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if in.GetMaximumPageSize() != 1 || s.starts[in.GetExecution().GetRunId()] == nil {
		return nil, errors.New("not the start of a known execution")
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{s.starts[in.GetExecution().GetRunId()]}}}, nil
}

func execID(i int) string {
	if i == 1 {
		return "exec-1"
	}
	return "exec-" + time.Duration(i).String()
}

func equalStrings(a, b []string) bool {
	return len(a) == len(b) && strings.Join(a, "\x00") == strings.Join(b, "\x00")
}

// TestForceCancelWorkflow_ARunThatCarriedOnHealthyIsNotEnded: the run was asked
// to cancel at event 5000 of one execution, carried on in another whose events
// are numbered from 1 again, and works there. It is judged there by its own
// events, from its own cancel, and ends on its own.
func TestForceCancelWorkflow_ARunThatCarriedOnHealthyIsNotEnded(t *testing.T) {
	f := newFCTarget()
	f.l0s = map[string]int64{"exec-1": 5000, "exec-2": 150}
	windows := 0
	f.scan = func(in FCStep) FCScanned {
		windows++
		if in.ExecutionRunID == "exec-1" {
			return FCScanned{Stall: ForceCancelStall{Progressing: true}, ExecutionRunID: "exec-2", Open: true}
		}
		if windows == 6 {
			f.closed = FCClassified{State: FCStateCanceled}
			return FCScanned{ExecutionRunID: "exec-2"}
		}
		var events []*historypb.HistoryEvent
		for id := int64(200 + 10*windows); id > in.L0; id-- {
			events = append(events, ev(id, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED))
		}
		return FCScanned{Stall: ForceCancelProgress(events, in.L0), ExecutionRunID: "exec-2", Open: true}
	}
	got := runFC(t, f, fcInput())
	if got.State != FCStateCanceled || len(f.terminated) != 0 || len(f.canceled) != 2 {
		t.Fatalf("result %+v, terminated %v, canceled %v after %d windows", got, f.terminated, f.canceled, windows)
	}
}

// refuseOnce fails its first write: a fault no store produces on demand.
type refuseOnce struct {
	artifacts.AuditStore
	writes *int
}

func (r refuseOnce) PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	if *r.writes++; *r.writes == 1 {
		return errors.New("store down")
	}
	return r.AuditStore.PutAuditRecord(ctx, rec)
}

// TestForceCancelAudit_SucceededMeansAllOfIt: a record says succeeded only
// when the result misses nothing.
func TestForceCancelAudit_SucceededMeansAllOfIt(t *testing.T) {
	for _, tt := range []struct {
		result ForceCancelResult
		want   bool
	}{
		{ForceCancelResult{State: FCStateForceCanceled, Terminated: true, Released: true, TerminalPublished: true}, true},
		{ForceCancelResult{State: FCStateForceCanceled, Terminated: true, Released: true, Cause: FCCauseEventUnpublished}, false},
		{ForceCancelResult{State: FCStateForceCanceled, Terminated: true, TerminalPublished: true}, false},
		{ForceCancelResult{State: FCStateForceCanceled, Terminated: true, Released: true}, false},
		{ForceCancelResult{State: FCStateAlreadyEnded, EndedBy: "fc-0"}, true}, // left to that one
		{ForceCancelResult{State: FCStateAlreadyEnded, EndedBy: "fc-0", Finishing: true, Released: true}, false},
		{ForceCancelResult{State: FCStateAlreadyEnded, EndedBy: "fc-0", Finishing: true, TerminalPublished: true}, false},
		{ForceCancelResult{State: FCStateAlreadyEnded, EndedBy: "fc-0", Finishing: true, Released: true, TerminalPublished: true}, true},
		{ForceCancelResult{State: FCStateCanceled}, true},
		{ForceCancelResult{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord, Terminated: true}, false},
		{ForceCancelResult{State: FCStateFailed, Cause: FCCauseStopped}, false},
	} {
		if got := tt.result.succeeded(); got != tt.want {
			t.Errorf("%+v succeeded = %v", tt.result, got)
		}
	}
}

// fcCluster is a cluster holding one run, for the steps that act on it: a test
// double for the answers and refusals a real one cannot give on demand.
type fcCluster struct {
	runID string // ork-r, where empty
	workflowservice.WorkflowServiceClient
	start      *historypb.HistoryEvent
	events     []*historypb.HistoryEvent // newest first
	historyErr error
	cancelErr  error
	status     enumspb.WorkflowExecutionStatus // unset: open
	endErr     error
	cancels    int
	ended      []*workflowservice.TerminateWorkflowExecutionRequest
}

func (c *fcCluster) GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{c.start}}}, nil
}

func (c *fcCluster) GetWorkflowExecutionHistoryReverse(context.Context, *workflowservice.GetWorkflowExecutionHistoryReverseRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: &historypb.History{Events: c.events}}, c.historyErr
}

func (c *fcCluster) RequestCancelWorkflowExecution(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest, ...grpc.CallOption) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	c.cancels++
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, c.cancelErr
}

func (c *fcCluster) TerminateWorkflowExecution(_ context.Context, in *workflowservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	c.ended = append(c.ended, in)
	return &workflowservice.TerminateWorkflowExecutionResponse{}, c.endErr
}

// fcCancelAt is the cancel request a stuck run was sent, ago, and a task of
// it that failed since.
func fcCancelAt(ago time.Duration) []*historypb.HistoryEvent {
	out := []*historypb.HistoryEvent{ev(11, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED), ev(10, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED)}
	out[1].EventTime = timestamppb.New(time.Now().Add(-ago))
	return out
}

// on is a over cluster, where the run is exec-1, open, reset at reset.
func (c *fcCluster) on(t *testing.T, a *ForceCancelActivities, reset *timestamppb.Timestamp) {
	t.Helper()
	c.start = startEvent(t, "payments")
	if c.events == nil {
		c.events = fcCancelAt(time.Hour)
	}
	if c.status == 0 {
		c.status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	m := &mocks.Client{}
	m.On("WorkflowService").Return(c)
	m.On("DescribeWorkflow", mock.Anything, cmp.Or(c.runID, "ork-r"), mock.Anything).Return(&client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{RunId: "exec-1"}, Type: &commonpb.WorkflowType{Name: RunWorkflowType}, Status: c.status},
		WorkflowExtendedInfo:  &workflowpb.WorkflowExecutionExtendedInfo{LastResetTime: reset}}}, nil)
	a.client, a.namespace, a.reader = m, "ork-main", &TemporalRunReader{converter: DataConverter()}
	a.starter = NewTemporalStarter(m, "").WithClusterIdentity("ork-main", "engine-a")
}

// TestForceCancelTerminate: a run is terminated only as the decision, the
// cluster's own history and this engine's clock allow at that moment; a call
// that was not made is said not to be, and one that may have been is an error.
func TestForceCancelTerminate(t *testing.T) {
	keys := recordKeys(t)
	d := sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	step := func() FCStep {
		return FCStep{Input: ForceCancelInput{Decision: msg, ScanEvents: 100, PageSize: 10}, ExecutionRunID: "exec-1", L0: 10}
	}
	tests := []struct {
		name    string
		cluster *fcCluster
		do      func(*ForceCancelActivities, *FCStep)
		reset   *timestamppb.Timestamp
		want    FCTerminated
		unsent  bool
		err     bool
		sent    int
	}{
		{"stalled since its cancel", &fcCluster{}, nil, nil, FCTerminated{Sent: true}, false, false, 1},
		{"closed already", &fcCluster{endErr: serviceerror.NewNotFound("closed")}, nil, nil, FCTerminated{}, false, false, 1},
		{"an answer that was lost", &fcCluster{endErr: serviceerror.NewUnavailable("down")}, nil, nil, FCTerminated{}, false, true, 1},
		{"a task completed since the stall test", &fcCluster{events: append([]*historypb.HistoryEvent{ev(12, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)}, fcCancelAt(time.Hour)...)}, nil, nil, FCTerminated{Progressing: true}, false, false, 0},
		{"its cancel is younger than the grace", &fcCluster{events: fcCancelAt(30 * time.Second)}, nil, nil, FCTerminated{Cause: FCCauseClockSkew}, false, false, 0},
		{"its cancel is as old as clocks may differ by", &fcCluster{events: fcCancelAt(57 * time.Second)}, nil, nil, FCTerminated{Sent: true}, false, false, 1},
		{"exactly as much happened since as is read", &fcCluster{}, func(_ *ForceCancelActivities, in *FCStep) { in.Input.ScanEvents = 1 }, nil, FCTerminated{Sent: true}, false, false, 1},
		{"a history that cannot be read", &fcCluster{historyErr: serviceerror.NewUnavailable("down")}, nil, nil, FCTerminated{}, true, false, 0},
		{"resumed since", &fcCluster{}, nil, timestamppb.New(d.IssuedAt.Add(time.Minute)), FCTerminated{Cause: FCCauseDecisionStale}, false, false, 0},
		{"a decision that does not verify", &fcCluster{}, func(_ *ForceCancelActivities, in *FCStep) { in.Input.Decision = "x.y.z" }, nil, FCTerminated{}, false, true, 0},
		{"a cluster over its limits", &fcCluster{endErr: serviceerror.NewResourceExhausted(0, "busy")}, nil, nil, FCTerminated{}, true, false, 1},
		{"a cluster that does not know the caller", &fcCluster{endErr: status.Error(codes.Unauthenticated, "who")}, nil, nil, FCTerminated{}, true, false, 1},
		{"a namespace that is not there", &fcCluster{endErr: serviceerror.NewNamespaceNotFound("ork-main")}, nil, nil, FCTerminated{}, true, false, 1},
		{"a call the cluster has not", &fcCluster{endErr: serviceerror.NewUnimplemented("no")}, nil, nil, FCTerminated{}, true, false, 1},
		{"a deadline", &fcCluster{endErr: context.DeadlineExceeded}, nil, nil, FCTerminated{}, false, true, 1},
		{"a failure inside the cluster", &fcCluster{endErr: serviceerror.NewInternal("?")}, nil, nil, FCTerminated{}, false, true, 1},
		{"a failure nobody names", &fcCluster{endErr: errors.New("reset by peer")}, nil, nil, FCTerminated{}, false, true, 1},
		{"a run in no state to be ended", &fcCluster{endErr: serviceerror.NewFailedPrecondition("no")}, nil, nil, FCTerminated{}, true, false, 1},
		{"at its ceiling, by this engine's clock not yet", &fcCluster{}, func(_ *ForceCancelActivities, in *FCStep) { in.ProgressingAtCeiling = true }, nil, FCTerminated{Cause: FCCauseClockSkew}, false, false, 0},
		{"at its ceiling", &fcCluster{events: []*historypb.HistoryEvent{ev(99, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)}}, func(a *ForceCancelActivities, in *FCStep) {
			in.ProgressingAtCeiling = true
			a.now = func() time.Time { return time.Now().Add(d.Ceiling()) }
		}, nil, FCTerminated{Sent: true}, false, false, 1},
		{"refused by the cluster", &fcCluster{endErr: serviceerror.NewPermissionDenied("no", "")}, nil, nil, FCTerminated{}, true, false, 1},
		{"a request the cluster does not take", &fcCluster{endErr: serviceerror.NewInvalidArgument("no")}, nil, nil, FCTerminated{}, true, false, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _ := fcActivities(t, keys, d)
			tt.cluster.on(t, a, tt.reset)
			in := step()
			if tt.do != nil {
				tt.do(a, &in)
			}
			got, err := a.Terminate(context.Background(), in)
			unsent := got.Unsent != ""
			got.Unsent = ""
			if (err != nil) != tt.err || unsent != tt.unsent || got != tt.want || len(tt.cluster.ended) != tt.sent {
				t.Fatalf("terminate = %+v (unsent %v), %v after %d calls; want %+v (unsent %v), error %v, %d calls", got, unsent, err, len(tt.cluster.ended), tt.want, tt.unsent, tt.err, tt.sent)
			}
			if tt.sent == 1 {
				if sent := tt.cluster.ended[0]; sent.GetWorkflowExecution().GetRunId() != "exec-1" || sent.GetReason() != ForceCancelTerminateReason || len(sent.GetDetails().GetPayloads()) != 1 {
					t.Errorf("terminated with %v", sent)
				}
			}
		})
	}
}

// TestForceCancelCancel: the run is asked to cancel only while it is the one
// decided on, and one anyone asked before was asked.
func TestForceCancelCancel(t *testing.T) {
	keys := recordKeys(t)
	d := sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	tests := []struct {
		name    string
		cluster *fcCluster
		reset   *timestamppb.Timestamp
		want    FCCanceled
		err     bool
		asked   int
	}{
		{"asked", &fcCluster{}, nil, FCCanceled{L0: 10, ExecutionRunID: "exec-1", Asked: true}, false, 1},
		{"asked by someone before", &fcCluster{cancelErr: serviceerror.NewCancellationAlreadyRequested("already")}, nil, FCCanceled{L0: 10, ExecutionRunID: "exec-1", Asked: true}, false, 1},
		{"refused", &fcCluster{cancelErr: serviceerror.NewPermissionDenied("no", "")}, nil, FCCanceled{}, true, 1},
		{"resumed since", &fcCluster{}, timestamppb.New(d.IssuedAt.Add(time.Minute)), FCCanceled{Cause: FCCauseDecisionStale}, false, 0},
		{"closed on it", &fcCluster{status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED}, nil, FCCanceled{Closed: true, Asked: true, ExecutionRunID: "exec-1"}, false, 1},
		{"closed before it", &fcCluster{status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, cancelErr: serviceerror.NewNotFound("closed")}, nil, FCCanceled{Closed: true, ExecutionRunID: "exec-1"}, false, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _ := fcActivities(t, keys, d)
			tt.cluster.on(t, a, tt.reset)
			got, err := a.Cancel(context.Background(), FCStep{Input: ForceCancelInput{Decision: msg, ScanEvents: 100, PageSize: 10}})
			if (err != nil) != tt.err || got != tt.want || tt.cluster.cancels != tt.asked {
				t.Fatalf("cancel = %+v, %v after %d requests; want %+v, error %v, %d", got, err, tt.cluster.cancels, tt.want, tt.err, tt.asked)
			}
		})
	}
}

// TestForceCancelPublish_OnlyFromItsOwnCode: the one step scheduled on a
// channel builds an ending only for a force cancel the engines run, and no
// other step acts from there.
func TestForceCancelPublish_OnlyFromItsOwnCode(t *testing.T) {
	keys := recordKeys(t)
	d := sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	in := FCStep{Input: ForceCancelInput{Decision: msg, ScanEvents: 100, PageSize: 10}, ExecutionRunID: "exec-1", L0: 10}
	for _, tt := range []struct {
		name    string
		self    forceCancelSelf
		publish bool
	}{
		{"the engines' code", forceCancelSelf{TaskQueue: task.DefaultQueue}, true},
		{"code on another channel", forceCancelSelf{TaskQueue: "ork-pool-gpu"}, false},
		{"inside workflow code", forceCancelSelf{Local: true, TaskQueue: task.DefaultQueue}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, _ := fcActivities(t, keys, d)
			cluster := &fcCluster{}
			cluster.on(t, a, nil)
			a.self = func(context.Context) (forceCancelSelf, error) {
				tt.self.WorkflowID, tt.self.Type, tt.self.StartedAt = d.WorkflowID(), ForceCancelWorkflowType, time.Now()
				return tt.self, nil
			}
			// The run is open, so what it may build is nothing: a cause, where
			// the step is refused with an error.
			got, err := a.Publish(context.Background(), in)
			if (err == nil) != tt.publish || tt.publish && got.Cause != FCCauseUnverifiedRecord {
				t.Errorf("publish = %+v, %v; want it to act: %v", got, err, tt.publish)
			}
			if !tt.self.Local {
				if out, err := a.Terminate(context.Background(), in); !refused(err) || len(cluster.ended) != 0 {
					t.Errorf("terminate from a channel = %+v, %v", out, err)
				}
			}
		})
	}
}

// recordsUnreadable verifies decisions and cannot read the keys of a record: a
// store that went away between the two.
type recordsUnreadable struct{ ForceCancelKeys }

func (k recordsUnreadable) VerifyDecision(ctx context.Context, msg string, kind jwt.DecisionKind, at time.Time, bind []byte) ([]byte, string, identity.Verdict) {
	if kind != jwt.KindForceCancelRoot {
		return nil, "", identity.VerdictUnavailable
	}
	return k.ForceCancelKeys.VerifyDecision(ctx, msg, kind, at, bind)
}

// TestForceCancelPublish_ACloseThatCannotBeReadNowIsAskedAgain: it is not
// taken for a close that is another's, which would never be announced.
func TestForceCancelPublish_ACloseThatCannotBeReadNowIsAskedAgain(t *testing.T) {
	keys := recordKeys(t)
	d := sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	details, err := SignForceCancelRecord(keys, d.record("exec-1", false, time.Now().UTC()))
	must.OK(t, err)
	closed := ev(12, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
	closed.EventTime = timestamppb.Now()
	closed.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
		Reason: ForceCancelTerminateReason, Details: details}}
	a, _ := fcActivities(t, recordsUnreadable{keys}, d)
	(&fcCluster{status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, events: []*historypb.HistoryEvent{closed}}).on(t, a, nil)
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: time.Now(), TaskQueue: task.DefaultQueue}, nil
	}
	if got, err := a.Publish(context.Background(), FCStep{Input: ForceCancelInput{Decision: msg, ScanEvents: 100, PageSize: 10}}); err == nil {
		t.Fatalf("publish = %+v with no error", got)
	}
}

// TestForceCancelPublish_AnnouncesTheEndWithEarlierEventsGone: the end of a
// force-canceled run is announced, the same, when what the run recorded before
// it is gone for good: a carried-on run's earlier events, or the record of its
// tasks, which no retry brings back. What may come back is an error, and
// nothing is announced from it.
func TestForceCancelPublish_AnnouncesTheEndWithEarlierEventsGone(t *testing.T) {
	ctx := context.Background()
	keys, d := recordKeys(t), sampleDecision()
	d.Namespace, d.RunID = chainNamespace, chainRunID
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	details, err := SignForceCancelRecord(keys, d.record("exec-1", false, time.Now().UTC()))
	must.OK(t, err)
	end := terminated(t, details)
	end.EventId, end.EventType, end.EventTime = 9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, timestamppb.Now()
	build := func(t *testing.T, spec chainSpec) *chainFixture {
		f := newChain(t, spec)
		f.client.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{d.RunID: {
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
				Execution: &commonpb.WorkflowExecution{WorkflowId: d.RunID, RunId: "exec-1"}}}}
		f.reader.WithForceCancelVerifier(keys, func() string { return "enforce" })
		return f
	}
	publish := func(t *testing.T, f *chainFixture) (FCPublished, *memSnapshotStore, error) {
		a, _ := fcActivities(t, keys, d)
		(&fcCluster{runID: d.RunID, status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, events: []*historypb.HistoryEvent{end}}).on(t, a, nil)
		written := newMemSnapshotStore()
		a.reader, a.snapshots = f.reader, written
		a.self = func(context.Context) (forceCancelSelf, error) {
			return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: time.Now(), TaskQueue: task.DefaultQueue}, nil
		}
		got, err := a.Publish(ctx, FCStep{Input: ForceCancelInput{Decision: msg, ScanEvents: 100, PageSize: 10}, ExecutionRunID: "exec-1"})
		return got, written, err
	}
	two, none := chainSpec{closed: []int{4, 3}, current: 2, closeWith: end}, chainSpec{closed: []int{4}, closeWith: end}
	segment := func(f *chainFixture) string { return f.segments[0].GetObject().GetUri() }
	records := func(f *chainFixture, do func(uri string)) {
		for uri := range f.store.objects {
			if !slices.ContainsFunc(f.segments, func(s *orkstackv1.EventSegmentRef) bool { return s.GetObject().GetUri() == uri }) {
				do(uri)
			}
		}
	}
	for _, tt := range []struct {
		name   string
		spec   chainSpec
		damage func(*chainFixture)
		ends   int64 // 0: an error
		rows   bool
	}{
		{"nothing is gone", two, func(*chainFixture) {}, 10, true},
		{"one earlier part's events", two, func(f *chainFixture) { delete(f.store.objects, segment(f)) }, 10, true},
		{"one earlier part's events, altered", two, func(f *chainFixture) { f.store.put(segment(f), []byte("other")) }, 10, true},
		{"the record of its tasks", two, func(f *chainFixture) { records(f, func(uri string) { delete(f.store.objects, uri) }) }, 10, false},
		{"the saved state it carried on from", none, func(f *chainFixture) { records(f, func(uri string) { delete(f.store.objects, uri) }) }, 5, false},
		{"everything it recorded", two, func(f *chainFixture) { clear(f.store.objects) }, 10, false},
		{"what its saved state names is not its own", none, func(f *chainFixture) {
			carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
			carry.Checkpoint.Uri = strings.Replace(carry.GetCheckpoint().GetUri(), "/"+chainRunID+"/", "/ork-1700000000-8/", 1)
			f.client.generations[f.runID][f.current].events[0] = f.started(carry)
		}, 5, false},
		{"an earlier part's events do not answer", two, func(f *chainFixture) { f.store.fail[segment(f)] = errors.New("store is down") }, 0, false},
		{"the record of its tasks does not answer", two, func(f *chainFixture) {
			records(f, func(uri string) { f.store.fail[uri] = errors.New("store is down") })
		}, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			whole, f := build(t, tt.spec), build(t, tt.spec)
			want, err := whole.reader.Events(ctx, d.RunID, 0)
			must.OK(t, err)
			tt.damage(f)

			got, written, err := publish(t, f)
			if tt.ends == 0 {
				if err == nil || !strings.Contains(err.Error(), "store is down") || len(written.objects) != 0 {
					t.Fatalf("= %+v, %v, %d written; want an error", got, err, len(written.objects))
				}
				return
			}
			must.OK(t, err)
			event, err := got.event()
			must.OK(t, err)
			if !proto.Equal(event, want[len(want)-1]) || event.GetSequence() != tt.ends || event.GetRequestId() != d.RequestID {
				t.Errorf("announced %v; want %v at %d", event, want[len(want)-1], tt.ends)
			}
			var run orkstackv1.Run
			must.OK(t, proto.Unmarshal(written.objects[got.SnapshotURI], &run))
			if run.GetError().GetCode() != RunForceCanceledCode || run.GetForceCancelRecord() == "" || (len(run.GetTaskRuns()) > 0) != tt.rows ||
				strings.Contains(run.GetError().GetDetails(), recordLostSentence) == tt.rows {
				t.Errorf("the ended run written at %q: %v", got.SnapshotURI, &run)
			}
		})
	}
	t.Run("it announced its own end", func(t *testing.T) {
		f := build(t, chainSpec{closed: []int{4}, current: 2, finish: true, closeWith: end})
		h := f.client.generations[f.runID][f.current]
		h.events = slices.Delete(h.events, len(h.events)-2, len(h.events)-1) // it was terminated, not completed
		clear(f.store.objects)
		if got, _, err := publish(t, f); err != nil || !got.Ended || got.Cause != "" {
			t.Errorf("= %+v, %v; want its own end", got, err)
		}
		run, _, err := f.reader.readForArchive(ctx, d.RunID, false)
		must.OK(t, err)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || run.GetError().GetCode() != RunRecordIncompleteCode || run.GetForceCancelRecord() != "" {
			t.Errorf("its archive says %v", run)
		}
	})
	t.Run("it was resumed since and goes on", func(t *testing.T) {
		f := build(t, chainSpec{closed: []int{4}, current: 2})
		if got, _, err := publish(t, f); err != nil || got.Cause != FCCauseEventSequence || got.Event != nil {
			t.Errorf("= %+v, %v; want nothing to announce", got, err)
		}
	})
}

// TestForceCancelEnding: what there is to announce of a run as its readers
// show it: this request's ending, a run's own, or nothing.
func TestForceCancelEnding(t *testing.T) {
	forced := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED, Error: &orkstackv1.Error{Code: RunForceCanceledCode}}
	own := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}
	end := func(request string) []*orkstackv1.Event {
		return []*orkstackv1.Event{{Sequence: 3}, terminalEvent(request)}
	}
	for _, tt := range []struct {
		name         string
		run          *orkstackv1.Run
		events       []*orkstackv1.Event
		event, ended bool
	}{
		{"this request's", forced, end("fc-1"), true, false},
		{"another request's", forced, end("fc-2"), false, false},
		{"the run's own", own, end(""), false, true},
		{"not ended", forced, end("fc-1")[:1], false, false},
		{"no events", forced, nil, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := forceCancelEnding(tt.run, tt.events, "fc-1")
			must.OK(t, err)
			if (got.Event != nil) != tt.event || got.Ended != tt.ended || (got.Cause != "") != (!tt.event && !tt.ended) {
				t.Fatalf("ending = %+v", got)
			}
		})
	}
}

func terminalEvent(request string) *orkstackv1.Event {
	return &orkstackv1.Event{RunId: "ork-r", Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, Sequence: 4, RequestId: request}
}

// TestForceCancelDecide_ARefusalIsFinal: what a step refuses stays refused, so
// the workflow is told not to ask again; what could not be asked is not.
func TestForceCancelDecide_ARefusalIsFinal(t *testing.T) {
	keys, d := recordKeys(t), sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	a, _ := fcActivities(t, keys, d)
	for _, err := range []error{
		func() error {
			_, err := a.Classify(context.Background(), FCStep{Input: ForceCancelInput{Decision: "x.y.z"}})
			return err
		}(),
		a.Audit(context.Background(), FCAuditInput{Input: ForceCancelInput{Decision: "x.y.z"}, Kind: forceCancelCompleted}),
	} {
		if !refused(err) {
			t.Errorf("a decision that does not verify = %v, want a refusal", err)
		}
	}
	var none *identity.Issuer
	a.keys = none
	if err := a.Audit(context.Background(), FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: forceCancelCompleted}); err == nil || refused(err) {
		t.Errorf("keys that cannot be read = %v, want an error asked again", err)
	}
}

// TestForceCancelOwnEnding: a run's own end is announced again only when its
// announcement never finished, and under the run's own key.
func TestForceCancelOwnEnding(t *testing.T) {
	keys, d := recordKeys(t), sampleDecision()
	publish := func(id int64, e *orkstackv1.Event) *historypb.HistoryEvent {
		input, err := DataConverter().ToPayloads(e, "s3://own", "own-key")
		must.OK(t, err)
		return &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent}, Input: input}}}
	}
	done := func(id, of int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: of}}}
	}
	end := terminalEvent("")
	for _, tt := range []struct {
		name   string
		events []*historypb.HistoryEvent // newest first
		again  bool
	}{
		{"never taken", []*historypb.HistoryEvent{ev(12, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), publish(11, end)}, true},
		{"finished", []*historypb.HistoryEvent{ev(14, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), done(13, 11), publish(11, end)}, false},
		{"another publish finished", []*historypb.HistoryEvent{ev(14, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), done(13, 5), publish(11, end)}, true},
		{"another run's", []*historypb.HistoryEvent{ev(14, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), publish(11, &orkstackv1.Event{RunId: "ork-other", Type: end.GetType()})}, false},
		{"not an end", []*historypb.HistoryEvent{ev(14, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), publish(11, &orkstackv1.Event{RunId: "ork-r", Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG})}, false},
		{"none read", []*historypb.HistoryEvent{ev(14, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, _ := fcActivities(t, keys, d)
			(&fcCluster{events: tt.events}).on(t, a, nil)
			got, err := a.ownEnding(context.Background(), d, FCStep{Input: ForceCancelInput{ScanEvents: 100, PageSize: 10}, ExecutionRunID: "exec-1"})
			must.OK(t, err)
			event, err := got.event()
			must.OK(t, err)
			if !got.Ended || (got.Event != nil) != tt.again || tt.again && (got.DedupeKey != "own-key" || got.SnapshotURI != "s3://own" || event.GetSequence() != end.GetSequence()) {
				t.Fatalf("own ending = %+v", got)
			}
		})
	}
}

// TestRefusalCap: of many refusals at once, one is written, none waits for
// it, and one that fails gives its minute back.
func TestRefusalCap(t *testing.T) {
	var c refusalCap
	now := time.Now()
	writing, finish := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := c.write(now, func() error { close(writing); <-finish; return errors.New("store down") })
		first <- err
	}()
	<-writing
	if written, err := c.write(now, func() error { t.Error("wrote beside another"); return nil }); written || err != nil {
		t.Errorf("a second at once = %v, %v", written, err)
	}
	close(finish)
	if <-first == nil {
		t.Error("a write that failed was said to be one")
	}
	if written, err := c.write(now, func() error { return nil }); !written || err != nil {
		t.Errorf("the next after a failure = %v, %v", written, err)
	}
	if written, _ := c.write(now.Add(forceCancelRefusalEvery), func() error { return nil }); !written {
		t.Error("a minute later none is written")
	}
}

// FuzzForceCancelUnbound: whatever a decision's body is, reading past its
// binding never panics and never reads a binding that is not whole: what is
// read is its end, and what comes before it is exactly two parts, each as
// long as it says.
func FuzzForceCancelUnbound(f *testing.F) {
	f.Add(append(identity.Bind("payments", "ork-r"), '{', '}'))
	f.Add([]byte{0, 0, 0, 200, 'x'})
	f.Add(identity.Bind("payments", "ork-r")[:18])
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		plain, ok := unbound(body, 2)
		if !ok {
			return
		}
		rest, parts := body[:len(body)-len(plain)], 0
		for ; len(rest) > 0; parts++ {
			n := binary.BigEndian.Uint32(rest)
			rest = rest[4+n:]
		}
		if parts != 2 || !bytes.HasSuffix(body, plain) {
			t.Fatalf("%x: %d parts before %x", body, parts, plain)
		}
	})
}

// TestForceCancelAudit_Counts: a force cancel is counted once, as it ends, by a
// state of its own and nothing else.
func TestForceCancelAudit_Counts(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	d := sampleDecision()
	a, _ := fcActivities(t, keys, d)
	started := time.Now()
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), Type: ForceCancelWorkflowType, StartedAt: started, Local: true}, nil
	}
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	var got counted
	a.counter = &got
	record := func(kind, state string) {
		t.Helper()
		must.OK(t, a.Audit(ctx, FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: kind, Result: ForceCancelResult{State: state}}))
	}
	record(forceCancelRequested, "")
	record(forceCancelCompleted, FCStateForceCanceled)
	record(forceCancelCompleted, FCStateForceCanceled)
	if len(got) != 1 || got[FCStateForceCanceled] != 1 {
		t.Errorf("%v", got)
	}
	b, _ := fcActivities(t, keys, d)
	b.self, got = a.self, nil
	b.counter = &got
	must.OK(t, b.Audit(ctx, FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: forceCancelCompleted, Result: ForceCancelResult{State: "made up"}}))
	if len(got) != 0 {
		t.Errorf("%v", got)
	}
}

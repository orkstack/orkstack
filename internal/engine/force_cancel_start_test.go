package engine

import (
	"context"
	"errors"
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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func sampleAsk() ForceCancelAsk {
	return ForceCancelAsk{Namespace: "payments", RunID: "ork-r", Reason: "stuck", Principal: "user:corp:alice", PrincipalVerified: true,
		AuthMethod: "oidc", PrincipalIssuer: "https://idp", Permission: "run.cancel.force", Scope: "orkflow:payments/pay", Rule: "admins", PolicyGeneration: 7}
}

// startCluster answers the run's description and records what was started.
func startCluster(run *workflowpb.WorkflowExecutionInfo, started *[]client.StartWorkflowOptions, inputs *[]ForceCancelInput, refuse func(id string) error) *mocks.Client {
	c := &mocks.Client{}
	var err error
	if run == nil {
		err = serviceerror.NewNotFound("no such run")
	}
	c.On("DescribeWorkflow", mock.Anything, "ork-r", "").Return(&client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: run}}, err)
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)
	call := c.On("ExecuteWorkflow", mock.Anything, mock.Anything, ForceCancelWorkflowType, mock.Anything)
	call.Run(func(args mock.Arguments) {
		opts := args.Get(1).(client.StartWorkflowOptions)
		call.ReturnArguments = mock.Arguments{nil, refuse(opts.ID)}
		if call.ReturnArguments.Error(1) == nil {
			*started, *inputs = append(*started, opts), append(*inputs, args.Get(3).(ForceCancelInput))
		}
	})
	return c
}

func openRun() *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "ork-r", RunId: "exec-7"},
		Type: &commonpb.WorkflowType{Name: RunWorkflowType}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
}

// TestForceCancelStart: a force cancel is started under an id its decision
// makes, once, with the settings pinned; what it decides is the caller's and
// the deployment's, signed where the engine signs.
func TestForceCancelStart(t *testing.T) {
	keys := recordKeys(t)
	settings := DefaultForceCancelSettings()
	now := time.Now().UTC()
	starter := func(c client.Client, k ForceCancelKeys, mode string) *ForceCancelStarter {
		s := NewForceCancelStarter(ForceCancelDeps{Client: c, ClusterNamespace: "ork-main", Reader: &TemporalRunReader{converter: DataConverter()}, Keys: k, Mode: func() string { return mode }}, settings)
		s.longestGrace = func(context.Context, string) time.Duration { return 0 }
		s.now = func() time.Time { return now }
		return s
	}
	t.Run("signed", func(t *testing.T) {
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		got, err := starter(startCluster(openRun(), &started, &inputs, func(string) error { return nil }), keys, "enforce").Request(context.Background(), sampleAsk())
		must.OK(t, err)
		if len(started) != 1 {
			t.Fatalf("started %d", len(started))
		}
		o, in := started[0], inputs[0]
		if o.ID != ForceCancelWorkflowIDPrefix("payments", "ork-r")+got.RequestID || o.TaskQueue != "ork-tasks" ||
			o.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE || o.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL ||
			!o.WorkflowExecutionErrorWhenAlreadyStarted || o.WorkflowExecutionTimeout != settings.Timeout() {
			t.Errorf("started with %+v", o)
		}
		if in.Unsigned != nil || in.ScanEvents != settings.ScanEvents || in.PageSize != settings.PageSize || in.RecordTimeout != settings.RecordTimeout {
			t.Errorf("input %+v", in)
		}
		a := &ForceCancelActivities{keys: keys, mode: func() string { return "enforce" }}
		d, cause, err := a.decide(context.Background(), in, forceCancelSelf{WorkflowID: o.ID, Type: ForceCancelWorkflowType, StartedAt: now, Local: true})
		if err != nil || d == nil {
			t.Fatalf("its own decision is refused: %q, %v", cause, err)
		}
		want := sampleAsk()
		if d.TargetExecutionRunID != "exec-7" || d.Principal != want.Principal || !d.PrincipalVerified || d.AuthMethod != want.AuthMethod || d.PrincipalIssuer != want.PrincipalIssuer ||
			d.Permission != want.Permission || d.Scope != want.Scope || d.Rule != want.Rule || d.PolicyGeneration != want.PolicyGeneration || d.Reason != want.Reason ||
			d.Grace() != settings.Grace || d.Ceiling() != settings.Ceiling || !d.IssuedAt.Equal(now) || !d.ExpiresAt.Equal(now.Add(settings.Timeout())) || d.RequestID != got.RequestID {
			t.Errorf("decision %+v", d)
		}
	})
	t.Run("unsigned, where the deployment signs nothing", func(t *testing.T) {
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		keyless := NewKeylessEngine(keysDoc{}, func() string { return "asserted" })
		ask := sampleAsk()
		_, err := starter(startCluster(openRun(), &started, &inputs, func(string) error { return nil }), keyless, "asserted").Request(context.Background(), ask)
		must.OK(t, err)
		d := inputs[0].Unsigned
		if inputs[0].Decision != "" || d == nil || d.Principal != ask.Principal || d.PrincipalVerified || d.AuthMethod != "" || d.PrincipalIssuer != "" || d.Rule != "" || d.PolicyGeneration != 0 {
			t.Errorf("an unsigned decision claims %+v", d)
		}
	})
	for name, tt := range map[string]struct {
		run  *workflowpb.WorkflowExecutionInfo
		keys ForceCancelKeys
		mode string
		ask  func(*ForceCancelAsk)
		is   error
		says string
	}{
		"a run that is not there":             {nil, keys, "enforce", nil, ErrRunNotFound, ""},
		"a workflow that is no run":           {&workflowpb.WorkflowExecutionInfo{Execution: openRun().Execution, Type: &commonpb.WorkflowType{Name: "Other"}}, keys, "enforce", nil, ErrRunNotFound, ""},
		"no reason":                           {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.Reason = "" }, ErrForceCancelText, "reason"},
		"a reason too long":                   {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.Reason = strings.Repeat("x", 513) }, ErrForceCancelText, "reason"},
		"a grace below the floor":             {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.Grace = 59 * time.Second }, ErrForceCancelGraceTooShort, "1m0s"},
		"one below its tasks'":                {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.Grace, a.LongestTaskGrace = 2*time.Minute, 2*time.Minute }, ErrForceCancelGraceTooShort, "2m30s"},
		"a grace above the most":              {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.Grace = 11 * time.Minute }, ErrForceCancelGraceTooLong, "10m0s"},
		"a run id that hides text":            {openRun(), keys, "enforce", func(a *ForceCancelAsk) { a.RunID = "ork-\u202er" }, ErrForceCancelText, "run_id"},
		"an engine that must sign and cannot": {openRun(), NewKeylessEngine(keysDoc{}, func() string { return "enforce" }), "enforce", nil, ErrForceCancelNoKey, "identity.key_files"},
	} {
		t.Run(name, func(t *testing.T) {
			var started []client.StartWorkflowOptions
			var inputs []ForceCancelInput
			ask := sampleAsk()
			if tt.ask != nil {
				tt.ask(&ask)
			}
			_, err := starter(startCluster(tt.run, &started, &inputs, func(string) error { return nil }), tt.keys, tt.mode).Request(context.Background(), ask)
			if !errors.Is(err, tt.is) || !strings.Contains(err.Error(), tt.says) || len(started) != 0 {
				t.Fatalf("start = %v after %d starts, want %v naming %q", err, len(started), tt.is, tt.says)
			}
		})
	}
	t.Run("a task grace no decision can hold asks for the most one can", func(t *testing.T) {
		for _, g := range []time.Duration{25 * time.Hour, 1<<63 - 1} {
			var started []client.StartWorkflowOptions
			var inputs []ForceCancelInput
			ask := sampleAsk()
			ask.LongestTaskGrace = g
			_, err := starter(startCluster(openRun(), &started, &inputs, func(string) error { return nil }), keys, "enforce").Request(context.Background(), ask)
			must.OK(t, err)
			a := &ForceCancelActivities{keys: keys, mode: func() string { return "enforce" }}
			d, _, _ := a.decide(context.Background(), inputs[0], forceCancelSelf{WorkflowID: started[0].ID, Type: ForceCancelWorkflowType, StartedAt: now, Local: true})
			if d == nil || d.Grace() != forceCancelMaxCeiling || d.ExpiresAt.Sub(d.IssuedAt) != forceCancelMaxLife {
				t.Errorf("%v: decision %+v", g, d)
			}
		}
	})
	t.Run("a start the cluster refuses", func(t *testing.T) {
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		_, err := starter(startCluster(openRun(), &started, &inputs, func(string) error { return serviceerror.NewUnavailable("down") }), keys, "enforce").Request(context.Background(), sampleAsk())
		var down *serviceerror.Unavailable
		if !errors.As(err, &down) {
			t.Fatalf("start = %v", err)
		}
	})
	t.Run("a task that asks for no grace still has the margin", func(t *testing.T) {
		low := settings
		low.Grace = 30 * time.Second
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		s := NewForceCancelStarter(ForceCancelDeps{Client: startCluster(openRun(), &started, &inputs, func(string) error { return nil }), Keys: keys, Mode: func() string { return "enforce" }}, low)
		ask := sampleAsk()
		ask.LongestTaskGrace, ask.Grace = 5*time.Second, 34*time.Second
		if _, err := s.Request(context.Background(), ask); !errors.Is(err, ErrForceCancelGraceTooShort) {
			t.Fatalf("34s for a task of 5s = %v, want too short", err)
		}
	})
	t.Run("a run whose definition cannot be read has the most", func(t *testing.T) {
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		ask := sampleAsk()
		ask.LongestTaskGrace = LongestTaskGraceUnknown
		s := starter(startCluster(openRun(), &started, &inputs, func(string) error { return nil }), keys, "enforce")
		got, err := s.Request(context.Background(), ask)
		must.OK(t, err)
		if got.Decision.Grace() != settings.MaxGrace {
			t.Errorf("grace %s, want %s", got.Decision.Grace(), settings.MaxGrace)
		}
		ask.Grace = settings.MaxGrace - time.Second
		if _, err := s.Request(context.Background(), ask); !errors.Is(err, ErrForceCancelGraceTooShort) {
			t.Errorf("a shorter one = %v, want too short", err)
		}
	})
	t.Run("a run's tasks may need longer than any request may ask", func(t *testing.T) {
		var started []client.StartWorkflowOptions
		var inputs []ForceCancelInput
		ask := sampleAsk()
		ask.LongestTaskGrace = 50 * time.Minute
		_, err := starter(startCluster(openRun(), &started, &inputs, func(string) error { return nil }), keys, "enforce").Request(context.Background(), ask)
		must.OK(t, err)
		a := &ForceCancelActivities{keys: keys, mode: func() string { return "enforce" }}
		d, cause, _ := a.decide(context.Background(), inputs[0], forceCancelSelf{WorkflowID: started[0].ID, Type: ForceCancelWorkflowType, StartedAt: now, Local: true})
		if d == nil || d.Grace() != 50*time.Minute+30*time.Second || d.Ceiling() < d.Grace() || started[0].WorkflowExecutionTimeout < 2*d.Grace()+2*d.Ceiling()+time.Hour {
			t.Errorf("decision %+v (%s), timeout %v", d, cause, started[0].WorkflowExecutionTimeout)
		}
	})
}

// TestForceCancelRequestID: the same request of the same caller about the same
// execution is the same id, whoever answers it; another's is another, and one
// that names itself nothing is its own.
func TestForceCancelRequestID(t *testing.T) {
	keys := recordKeys(t)
	ask := sampleAsk()
	ask.ClientRequestID = "retry-1"
	first, err := forceCancelRequestID(keys, ask, "exec-1", 0)
	must.OK(t, err)
	if again, err := forceCancelRequestID(keys, ask, "exec-1", 0); err != nil || again != first || len(first) != 36 || first[14] != '8' {
		t.Fatalf("again = %q, %v; want %q, a version 8", again, err, first)
	}
	other := ask
	other.Principal = "user:corp:bob"
	for name, id := range map[string]func() (string, error){
		"another caller's":     func() (string, error) { return forceCancelRequestID(keys, other, "exec-1", 0) },
		"another execution's":  func() (string, error) { return forceCancelRequestID(keys, ask, "exec-2", 0) },
		"the next attempt's":   func() (string, error) { return forceCancelRequestID(keys, ask, "exec-1", 1) },
		"another engine's key": func() (string, error) { return forceCancelRequestID(recordKeys(t), ask, "exec-1", 0) },
	} {
		if got, err := id(); err != nil || got == first {
			t.Errorf("%s = %q, %v", name, got, err)
		}
	}
	ask.ClientRequestID = ""
	a, err := forceCancelRequestID(keys, ask, "exec-1", 0)
	must.OK(t, err)
	b, err := forceCancelRequestID(keys, ask, "exec-1", 0)
	must.OK(t, err)
	if a == b || a[14] != '7' {
		t.Errorf("without a name: %q and %q", a, b)
	}
	keyless := NewKeylessEngine(keysDoc{}, func() string { return "asserted" })
	ask.ClientRequestID = "retry-1"
	if id, err := forceCancelRequestID(keyless, ask, "exec-1", 0); err != nil || id[14] != '7' {
		t.Errorf("an engine with no key = %q, %v", id, err)
	}
	for _, bad := range []string{strings.Repeat("x", 129), "a\nb", "caf\u00e9"} {
		ask.ClientRequestID = bad
		if _, err := forceCancelRequestID(keys, ask, "exec-1", 0); !errors.Is(err, ErrForceCancelText) {
			t.Errorf("%q = %v", bad, err)
		}
	}
	_ = jwt.DeriveForceCancelRequestID
}

// TestLongestTaskGrace: the longest of the run's own tasks, one that names none
// having the default.
func TestLongestTaskGrace(t *testing.T) {
	for want, graces := range map[time.Duration][]int32{0: nil, 30 * time.Second: {0, 5}, 4 * time.Minute: {240, 0, 60}} {
		wf := &orkstackv1.Orkflow{}
		for _, g := range graces {
			wf.Tasks = append(wf.Tasks, &orkstackv1.Task{CancellationGracePeriodSeconds: g})
		}
		if got := LongestTaskGrace(wf); got != want {
			t.Errorf("%v: %s, want %s", graces, got, want)
		}
	}
}

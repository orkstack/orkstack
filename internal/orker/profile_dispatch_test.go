package orker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// labelRuntime is a runtime whose pulled image carries the given interface
// label (ok false: none), or whose label read fails with err; it counts the
// reads. Every other call reaches the embedded runtime. A hand-written fake
// for the read's fault (a daemon that cannot answer an inspect), which no
// real daemon produces on demand, and for labels a test sets per case.
type labelRuntime struct {
	ContainerRuntime
	label string
	ok    bool
	err   error
	reads int
}

func (r *labelRuntime) ImageLabel(_ context.Context, _, key string) (string, bool, error) {
	r.reads++
	if key != dsl.InterfaceLabel {
		return "", false, nil
	}
	return r.label, r.ok, r.err
}

// interfaceWithDevices is an interface label declaring devices.
func interfaceWithDevices(devices string) string {
	if devices == "" {
		return `{"schemaVersion":1}`
	}
	return `{"schemaVersion":1,"devices":` + devices + `}`
}

func declaredSensor(label string) []*orkstackv1.ProfileRef {
	return []*orkstackv1.ProfileRef{{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin(label, "d"+label)}}}
}

func undeclaredSensor(labels ...string) []*orkstackv1.ProfileRef {
	ref := &orkstackv1.ProfileRef{Name: "sensor", IdentityDigest: "d1"}
	for _, l := range labels {
		ref.Contracts = append(ref.Contracts, pin(l, "d"+l))
	}
	return []*orkstackv1.ProfileRef{ref}
}

func declarationTask(refs []*orkstackv1.ProfileRef) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("read")
	in.Devices = refs
	return in
}

const declarationImage = "example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// TestProfileContractFaults_EmittedOnlyWhenListed: PROFILE_CHANGED and
// PROFILE_CONTRACT_NOT_SERVED found before any container are interruptions
// the Orker emits typed, but only for an attempt marked never started and
// only when its retry policy lists them; otherwise they stay category-typed
// and spend an own attempt.
func TestProfileContractFaults_EmittedOnlyWhenListed(t *testing.T) {
	for _, reason := range []string{ReasonProfileChanged, ReasonProfileContractNotServed} {
		for _, tc := range []struct {
			name         string
			neverStarted bool
			listed       bool
			wantType     string
		}{
			{"never started, listed", true, true, InterruptionType(reason)},
			{"never started, not listed", true, false, CategoryInfrastructureError},
			{"not marked, listed", false, true, CategoryInfrastructureError},
		} {
			t.Run(reason+"/"+tc.name, func(t *testing.T) {
				f := Fail(reason, "profile %q: test", "sensor")
				f.NeverStarted = tc.neverStarted
				var listed []string
				if tc.listed {
					listed = []string{InterruptionType(reason)}
				}
				var app *temporal.ApplicationError
				if err := f.EmittedError(listed, false, nil); !errors.As(err, &app) || app.Type() != tc.wantType {
					t.Fatalf("EmittedError = %v; want type %s", err, tc.wantType)
				}
			})
		}
	}
}

// TestProfileContractReasons_HaveCategories: the contract reasons each have a
// row of their own in the category table, never the default for a code
// nobody classified.
func TestProfileContractReasons_HaveCategories(t *testing.T) {
	for reason, want := range map[string]string{
		ReasonProfileContractNotServed:   CategoryInfrastructureError,
		ReasonProfileDeclarationMismatch: CategoryContractViolation,
	} {
		if got, ok := reasonCategory[reason]; !ok || got != want {
			t.Errorf("reasonCategory[%s] = %q, %v; want %q", reason, got, ok, want)
		}
	}
}

// TestDispatch_DeclarationMismatchAfterPull: after the pull, an image whose
// own label declares a profile the task names must declare the contract the
// run pinned as declared; a different label, or a run that pinned the
// profile undeclared, fails PROFILE_DECLARATION_MISMATCH, a ContractViolation
// the attempt never retries.
func TestDispatch_DeclarationMismatchAfterPull(t *testing.T) {
	for name, tc := range map[string]struct {
		refs    []*orkstackv1.ProfileRef
		devices string
		refused bool
	}{
		"declared, the same label":                {refs: declaredSensor("2"), devices: `{"sensor":"2"}`},
		"declared, another label":                 {refs: declaredSensor("2"), devices: `{"sensor":"3"}`, refused: true},
		"undeclared, but the image declares":      {refs: undeclaredSensor("1"), devices: `{"sensor":"1"}`, refused: true},
		"the image declares another profile only": {refs: declaredSensor("2"), devices: `{"other":"5"}`},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := &labelRuntime{label: interfaceWithDevices(tc.devices), ok: true}
			d := &OCIDispatcher{Runtime: runtime}
			failure := d.checkDeclarations(context.Background(), declarationTask(tc.refs), declarationImage)
			if !tc.refused {
				if failure != nil {
					t.Fatalf("checkDeclarations = %v; want it to pass", failure)
				}
				return
			}
			if failure == nil || failure.Reason != ReasonProfileDeclarationMismatch || failure.NeverStarted {
				t.Fatalf("checkDeclarations = %+v; want PROFILE_DECLARATION_MISMATCH, not never-started", failure)
			}
			if failure.Category() != CategoryContractViolation {
				t.Fatalf("category = %s, want ContractViolation", failure.Category())
			}
			if !strings.Contains(failure.Message, `"sensor"`) {
				t.Fatalf("the failure %q does not name the profile", failure.Message)
			}
		})
	}
}

// TestDispatch_OverrideOnASilentImageRuns: an image that says nothing about
// the profile (no interface label, or one without devices:) leaves the run's
// pin standing, as an `@label` override on a third-party image needs. A
// label that does not read declares nothing, as create judged it.
func TestDispatch_OverrideOnASilentImageRuns(t *testing.T) {
	for name, tc := range map[string]struct {
		runtime *labelRuntime
		refs    []*orkstackv1.ProfileRef
		refused bool
	}{
		"no interface label":                  {runtime: &labelRuntime{ok: false}, refs: declaredSensor("2")},
		"a label without devices":             {runtime: &labelRuntime{label: interfaceWithDevices(""), ok: true}, refs: declaredSensor("2")},
		"an unparsable label, undeclared run": {runtime: &labelRuntime{label: "not json", ok: true}, refs: undeclaredSensor("1")},
		"an unparsable label, an override":    {runtime: &labelRuntime{label: "not json", ok: true}, refs: declaredSensor("2")},
	} {
		t.Run(name, func(t *testing.T) {
			d := &OCIDispatcher{Runtime: tc.runtime}
			failure := d.checkDeclarations(context.Background(), declarationTask(tc.refs), declarationImage)
			if tc.refused != (failure != nil) {
				t.Fatalf("checkDeclarations = %v; want refused %v", failure, tc.refused)
			}
			if tc.refused && failure.Reason != ReasonProfileDeclarationMismatch {
				t.Fatalf("reason = %s, want PROFILE_DECLARATION_MISMATCH", failure.Reason)
			}
			if tc.runtime.reads != 1 {
				t.Fatalf("the label was read %d times, want once", tc.runtime.reads)
			}
		})
	}
}

// TestDispatch_ImageLabelErrorIsNeverStarted: a label the runtime cannot read
// is no pass and no ContractViolation: it is a never-started infrastructure
// fault, re-dispatched within the run's never-started budget.
func TestDispatch_ImageLabelErrorIsNeverStarted(t *testing.T) {
	d := &OCIDispatcher{Runtime: &labelRuntime{err: errors.New("cannot connect to the container daemon")}}
	failure := d.checkDeclarations(context.Background(), declarationTask(declaredSensor("2")), declarationImage)
	if failure == nil || failure.Reason != ReasonRuntimeStateUnknown || !failure.NeverStarted {
		t.Fatalf("checkDeclarations = %+v; want a never-started RUNTIME_STATE_UNKNOWN", failure)
	}
	if !failure.mayEmit(false) {
		t.Fatal("the fault is not one the Orker may emit typed")
	}
}

// TestDispatch_RefWithoutContractsSkipsTheCheck: a task naming no profile,
// or only refs that carry no contracts, has no declaration to compare and
// reads no label.
func TestDispatch_RefWithoutContractsSkipsTheCheck(t *testing.T) {
	for name, refs := range map[string][]*orkstackv1.ProfileRef{
		"no profile":             nil,
		"a ref with no contract": {{Name: "sensor", IdentityDigest: "d1"}},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := &labelRuntime{label: interfaceWithDevices(`{"sensor":"9"}`), ok: true}
			d := &OCIDispatcher{Runtime: runtime}
			if failure := d.checkDeclarations(context.Background(), declarationTask(refs), declarationImage); failure != nil {
				t.Fatalf("checkDeclarations = %v; want no check", failure)
			}
			if runtime.reads != 0 {
				t.Fatalf("the label was read %d times, want none", runtime.reads)
			}
		})
	}
}

// TestDispatch_TwoContractArgvPassesOnlyTheSelected: step 3c's selection is
// what reaches the container: a task declaring contract 2 of a two-contract
// profile is created with contract 2's device alone.
func TestDispatch_TwoContractArgvPassesOnlyTheSelected(t *testing.T) {
	d := &OCIDispatcher{profiles: contractState(t), inspectDevice: usbNodes}
	devices, failure := d.profileGate(declarationTask(declaredSensor("2")), &Heartbeater{})
	if failure != nil {
		t.Fatalf("profileGate = %v", failure)
	}
	spec := validSpec()
	spec.Devices = devices
	got := argsString(t, spec)
	if !strings.Contains(got, "--device /dev/ttyUSB1:/dev/ttySensor:rw ") {
		t.Errorf("args lack contract 2's device: %s", got)
	}
	if strings.Contains(got, "/dev/ttyUSB0") || strings.Count(got, "--device ") != 1 {
		t.Errorf("args carry another contract's device: %s", got)
	}
}

// TestAttemptRecord_CarriesTheContractItRanUnder: the attempt record says,
// per profile, which contract the attempt ran under, the labels its run
// pinned, and whether the image chose it.
func TestAttemptRecord_CarriesTheContractItRanUnder(t *testing.T) {
	for name, tc := range map[string]struct {
		refs     []*orkstackv1.ProfileRef
		accepts  []string
		contract string
		run      []string
		declared bool
	}{
		"declared, owned":          {refs: declaredSensor("2"), contract: "2", run: []string{"2"}, declared: true},
		"undeclared, the default":  {refs: undeclaredSensor("1", "2"), contract: "1", run: []string{"1", "2"}},
		"undeclared, accepted":     {refs: undeclaredSensor("0", "4"), accepts: []string{"0"}, contract: "1", run: []string{"0", "4"}},
		"no contracts, its digest": {refs: []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: "d2"}}, contract: "2"},
	} {
		t.Run(name, func(t *testing.T) {
			s := contractState(t)
			s.declared = Profiles{List: []Profile{{Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}}}
			if tc.accepts != nil {
				a := s.audits["sensor"]
				def := a.contracts["1"]
				def.accepts = tc.accepts
				a.contracts = map[string]contractAudit{"1": def, "2": a.contracts["2"]}
				s.audits["sensor"] = a
			}
			d := &OCIDispatcher{profiles: s, inspectDevice: usbNodes}
			in := declarationTask(tc.refs)
			beat := &Heartbeater{}
			if _, failure := d.profileGate(in, beat); failure != nil {
				t.Fatalf("profileGate = %v", failure)
			}
			got := d.attemptRecord(in, 1, nil, beat, nil, nil, AttemptOutcome{}).GetProfiles()
			if len(got) != 1 {
				t.Fatalf("record profiles = %v; want one", got)
			}
			p := got[0]
			if p.GetName() != "sensor" || p.GetKind() != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES || p.GetContract() != tc.contract ||
				!slices.Equal(p.GetRunContracts(), tc.run) || p.GetDeclared() != tc.declared || !p.GetIdentityPinned() {
				t.Fatalf("record profile = %v; want contract %s of %v, declared %v, pinned", p, tc.contract, tc.run, tc.declared)
			}
		})
	}

	d := &OCIDispatcher{profiles: contractState(t), inspectDevice: usbNodes}
	if got := d.attemptRecord(declarationTask(declaredSensor("2")), 1, nil, &Heartbeater{}, nil, nil, AttemptOutcome{}).GetProfiles(); len(got) != 0 {
		t.Fatalf("a record that never reached step 3c carries %v", got)
	}
}

// TestExecute_TheDeclarationCheckRunsAfterThePullAndCreatesNothing: through
// Execute, an image declaring another contract than its run pinned ends the
// attempt PROFILE_DECLARATION_MISMATCH with no container created, and a
// label the runtime cannot read ends it a never-started RUNTIME_STATE_UNKNOWN;
// both attempt records say which contract step 3c selected.
func TestExecute_TheDeclarationCheckRunsAfterThePullAndCreatesNothing(t *testing.T) {
	for _, tc := range []struct {
		name         string
		label        string
		err          error
		wantReason   string
		neverStarted bool
	}{
		{"another contract", interfaceWithDevices(`{"sensor":"3"}`), nil, ReasonProfileDeclarationMismatch, false},
		{"a label read that fails", "", errors.New("cannot connect to the container daemon"), ReasonRuntimeStateUnknown, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creating := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
			runtime := &labelRuntime{ContainerRuntime: creating, label: tc.label, ok: tc.err == nil, err: tc.err}
			attempts := artifacts.NewMemoryAttemptStore()
			d := newAttemptDispatcher(t, runtime, attempts)
			d.heartbeaters = sdkRouteHeartbeater
			d.profiles = contractState(t)
			d.inspectDevice = usbNodes
			probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: time.Minute,
					RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1, InitialInterval: time.Millisecond},
				})
				return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetTestTimeout(60 * time.Second)
			env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
			env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
			env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
			in := declarationTask(declaredSensor("2"))
			env.ExecuteWorkflow("probe", in)

			var app *temporal.ApplicationError
			if err := env.GetWorkflowError(); !errors.As(err, &app) {
				t.Fatalf("workflow error = %v, want the attempt's ApplicationError", err)
			}
			var detail *orkstackv1.ActivityFailureDetail
			if err := app.Details(&detail); err != nil || detail.GetReasonCode() != tc.wantReason {
				t.Fatalf("detail = %v, %v; want reason %s", detail, err, tc.wantReason)
			}
			if runtime.reads != 1 {
				t.Fatalf("the label was read %d times, want once, after the pull", runtime.reads)
			}
			if got := len(creating.creates()); got != 0 {
				t.Fatalf("%d containers created; want none", got)
			}
			records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if err != nil || len(records) != 1 {
				t.Fatalf("ListAttempts = %d records, %v; want one", len(records), err)
			}
			if p := records[0].GetProfiles(); len(p) != 1 || p[0].GetContract() != "2" || !p[0].GetDeclared() {
				t.Fatalf("record profiles = %v; want contract 2, declared", p)
			}
		})
	}
}

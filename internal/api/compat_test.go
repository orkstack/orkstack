package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// The shared fixture of interface-compatibility-gate's test plan: 3.0.0 adds
// a required region and a defaulted timeout, drops output url, adds output
// sbom, and rewrites every task — 7 findings, 2 BREAKING.
const compatBlock210 = `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.publish.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: publish
    uses: shell
    needs: [sign]
    with: { op: eval, command: ["true"] }
`

const compatBlock300 = `
name: release-block
version: "3.0.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
  region: { type: string, required: true }
  timeout: { type: number, default: 300 }
outputs:
  digest: '{{ .tasks.attest.output.value }}'
  sbom: '{{ .tasks.sbom.output.value }}'
tasks:
  - id: attest
    uses: shell
    with: { op: eval, command: ["echo", "{{ .inputs.region }}"] }
  - id: sbom
    uses: shell
    needs: [attest]
    with: { op: eval, command: ["true"] }
`

// compatBlock203 has 2.1.0's inputs and no outputs: against 2.1.0 it removes
// both outputs (BREAKING); against a version with no outputs it is UNKNOWN.
const compatBlock203 = `
name: release-block
version: "2.0.3"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

// The three WHY-section warning lines, verbatim.
var compatApplyWarnings = []string{
	`inputs.region: input "region" is added and required with no default: a run that does not supply it no longer satisfies the declaration — breaking against release-block@2.1.0`,
	`outputs.url: output "url" is removed: an Orkflow composing this one that reads .tasks.<id>.output.url is refused at its next apply against this version, and a reader of the run's output no longer finds it — breaking against release-block@2.1.0`,
	`version: release-block@3.0.0 breaks the declared interface of release-block@2.1.0, the current version (2 breaking changes). Applying it changes nothing that runs; promoting it is refused until acknowledged (orkctl orkflow promote release-block 3.0.0 --acknowledge-breaking). What starts or composes release-block: orkctl orkflow callers release-block`,
}

// compatService wires a Service over the given store (a fresh memory store
// when nil) with namespaces team-a and team-b registered and an optional
// schedule controller.
func compatService(t *testing.T, store engine.OrkflowStore, schedules api.ScheduleController) *api.Service {
	t.Helper()
	if store == nil {
		store = engine.NewMemoryOrkflowStore()
	}
	counter := 0
	return api.NewService(api.Config{
		Orkflows:   store,
		Namespaces: newFakeRegistry(&orkstackv1.Namespace{Name: "team-a"}, &orkstackv1.Namespace{Name: "team-b"}),
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
		Schedules:  schedules,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
}

func applyIn(t *testing.T, svc *api.Service, namespace, document string) *orkstackv1.ApplyOrkflowResponse {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: document, Namespace: namespace}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.Msg
}

func promoteVersion(svc *api.Service, namespace, name, version string, ack bool) (*orkstackv1.SetCurrentOrkflowVersionResponse, error) {
	resp, err := svc.SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: version, Namespace: namespace,
			AcknowledgeBreaking: ack}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func mustPromote(t *testing.T, svc *api.Service, namespace, name, version string, ack bool) *orkstackv1.SetCurrentOrkflowVersionResponse {
	t.Helper()
	resp, err := promoteVersion(svc, namespace, name, version, ack)
	if err != nil {
		t.Fatalf("promote %s@%s (ack=%v): %v", name, version, ack, err)
	}
	return resp
}

func connectMessage(err error) string {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Message()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestCompareInterfaces_ReadsTwoStoredVersions(t *testing.T) {
	svc := compatService(t, nil, nil)
	ctx := context.Background()
	applyIn(t, svc, "team-a", compatBlock210)
	mustPromote(t, svc, "team-a", "release-block", "2.1.0", false)
	applyIn(t, svc, "team-a", compatBlock300)

	resp, err := svc.CompareInterfaces(ctx, connect.NewRequest(&orkstackv1.CompareInterfacesRequest{
		Name: "release-block", FromVersion: "2.1.0", ToVersion: "3.0.0", Namespace: "team-a"}))
	if err != nil {
		t.Fatalf("CompareInterfaces: %v", err)
	}
	if resp.Msg.GetVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING ||
		len(resp.Msg.GetFindings()) != 7 || resp.Msg.GetFromVersion() != "2.1.0" || resp.Msg.GetToVersion() != "3.0.0" {
		t.Fatalf("CompareInterfaces = %v", resp.Msg)
	}

	// Empty from resolves to the current version.
	resp, err = svc.CompareInterfaces(ctx, connect.NewRequest(&orkstackv1.CompareInterfacesRequest{
		Name: "release-block", ToVersion: "3.0.0", Namespace: "team-a"}))
	if err != nil || resp.Msg.GetFromVersion() != "2.1.0" {
		t.Fatalf("empty from: %v, %v; want from 2.1.0", resp, err)
	}

	// from == to is answered, not refused.
	resp, err = svc.CompareInterfaces(ctx, connect.NewRequest(&orkstackv1.CompareInterfacesRequest{
		Name: "release-block", Namespace: "team-a"}))
	if err != nil || resp.Msg.GetVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE {
		t.Fatalf("current against itself: %v, %v; want COMPATIBLE", resp, err)
	}

	for _, tc := range []struct {
		name string
		req  *orkstackv1.CompareInterfacesRequest
		code connect.Code
	}{
		{"unknown version", &orkstackv1.CompareInterfacesRequest{Name: "release-block", ToVersion: "9.9.9", Namespace: "team-a"}, connect.CodeNotFound},
		{"unknown orkflow", &orkstackv1.CompareInterfacesRequest{Name: "nope", Namespace: "team-a"}, connect.CodeNotFound},
		{"no name", &orkstackv1.CompareInterfacesRequest{Namespace: "team-a"}, connect.CodeInvalidArgument},
		{"another namespace", &orkstackv1.CompareInterfacesRequest{Name: "release-block", Namespace: "team-b"}, connect.CodeNotFound},
	} {
		if _, err := svc.CompareInterfaces(ctx, connect.NewRequest(tc.req)); connect.CodeOf(err) != tc.code {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.code)
		}
	}
}

func TestSetCurrentOrkflowVersion_RefusesABreakingPromotionUntilAcknowledged(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	applyIn(t, svc, "", compatBlock300)

	_, err := promoteVersion(svc, "", "release-block", "3.0.0", false)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("unacknowledged breaking promotion = %v, want FailedPrecondition", err)
	}
	msg := connectMessage(err)
	for _, want := range []string{"inputs.region", "outputs.url", "--acknowledge-breaking",
		"promoting release-block to 3.0.0 breaks the declared interface of 2.1.0, the version a run that names no version executes now (2 breaking changes)",
		"orkctl orkflow compat release-block 2.1.0 3.0.0", "orkctl orkflow callers release-block"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not contain %q", msg, want)
		}
	}
	requireResolvedVersion(t, svc, "release-block", "2.1.0", "after the refusal")

	resp := mustPromote(t, svc, "", "release-block", "3.0.0", true)
	if resp.GetInterfaceVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING ||
		resp.GetComparedVersion() != "2.1.0" || resp.GetPreviousVersion() != "2.1.0" {
		t.Fatalf("acknowledged promotion = %v, want BREAKING vs 2.1.0", resp)
	}
	requireResolvedVersion(t, svc, "release-block", "3.0.0", "after the acknowledged promotion")
}

func TestSetCurrentOrkflowVersion_CompatibleAndUnknownNeedNoAcknowledgement(t *testing.T) {
	svc := compatService(t, nil, nil)
	// Compatible: identical inputs and declared outputs, rewritten tasks.
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	applyIn(t, svc, "", strings.Replace(strings.Replace(compatBlock210, `"2.1.0"`, `"2.2.0"`, 1),
		`command: ["true"]`, `command: ["echo", "rewritten"]`, -1))
	resp := mustPromote(t, svc, "", "release-block", "2.2.0", false)
	if resp.GetInterfaceVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE ||
		resp.GetComparedVersion() != "2.1.0" {
		t.Fatalf("compatible promotion = %v", resp)
	}

	// Unknown: identical inputs, no outputs on either side.
	applyIn(t, svc, "", versionedDocument("deploy", "1.0.0", "one"))
	mustPromote(t, svc, "", "deploy", "1.0.0", false)
	applyIn(t, svc, "", versionedDocument("deploy", "2.0.0", "two"))
	resp = mustPromote(t, svc, "", "deploy", "2.0.0", false)
	if resp.GetInterfaceVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN {
		t.Fatalf("unknown promotion = %v", resp)
	}
}

func TestSetCurrentOrkflowVersion_ComparesAgainstTheHeadWhenNeverPromoted(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	applyIn(t, svc, "", compatBlock203) // head: no outputs, never promoted
	// 2.0.3 runs (newest applied); promoting 2.1.0 back declares outputs:
	// UNKNOWN, never refused.
	_, err := promoteVersion(svc, "", "release-block", "2.1.0", false)
	if err != nil {
		t.Fatalf("2.0.3 -> 2.1.0: %v", err)
	}

	svc = compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	applyIn(t, svc, "", compatBlock300) // head, never promoted
	applyIn(t, svc, "", compatBlock203)
	// Head is 2.0.3; promoting 3.0.0 adds region (BREAKING) against the head.
	_, err = promoteVersion(svc, "", "release-block", "3.0.0", false)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("promotion against the unpromoted head = %v, want FailedPrecondition", err)
	}
	if msg := connectMessage(err); !strings.Contains(msg,
		"breaks the declared interface of 2.0.3, its newest applied version, which runs because it was never promoted") {
		t.Fatalf("refusal %q lacks the never-promoted wording", msg)
	}
}

func TestSetCurrentOrkflowVersion_RollbackIsGatedLikeAnyPromotion(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	applyIn(t, svc, "", compatBlock300)
	mustPromote(t, svc, "", "release-block", "3.0.0", true)

	_, err := promoteVersion(svc, "", "release-block", "2.1.0", false)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(connectMessage(err), "inputs.region") {
		t.Fatalf("rollback = %v, want FailedPrecondition naming inputs.region", err)
	}
	requireResolvedVersion(t, svc, "release-block", "3.0.0", "after the refused rollback")
	mustPromote(t, svc, "", "release-block", "2.1.0", true)
	requireResolvedVersion(t, svc, "release-block", "2.1.0", "after the acknowledged rollback")
}

func TestSetCurrentOrkflowVersion_PromotingWhatRunsComparesNothing(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	resp := mustPromote(t, svc, "", "release-block", "2.1.0", false)
	if resp.GetInterfaceVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED || resp.GetComparedVersion() != "" {
		t.Fatalf("promoting the head that already runs = %v, want UNSPECIFIED and no compared version", resp)
	}
	resp = mustPromote(t, svc, "", "release-block", "2.1.0", false)
	if resp.GetInterfaceVerdict() != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED {
		t.Fatalf("re-promoting the current version = %v, want UNSPECIFIED", resp)
	}
}

// racingStore promotes a third version through the inner store the moment a
// promotion reaches the swap — the window between the gate's read and its
// write — then lets the promotion continue with its own expectation.
type racingStore struct {
	*engine.MemoryOrkflowStore
	racer string
	once  sync.Once
}

func (r *racingStore) SetCurrentVersion(ctx context.Context, namespace, name, version string,
	expected artifacts.CurrentExpectation) (string, error) {
	r.once.Do(func() {
		if _, err := r.MemoryOrkflowStore.SetCurrentVersion(ctx, namespace, name, r.racer,
			artifacts.NoCurrentExpectation()); err != nil {
			panic(err)
		}
	})
	return r.MemoryOrkflowStore.SetCurrentVersion(ctx, namespace, name, version, expected)
}

func TestSetCurrentOrkflowVersion_RefusesWhenThePointerMovedAfterTheComparison(t *testing.T) {
	inner := engine.NewMemoryOrkflowStore()
	setup := compatService(t, inner, nil)
	applyIn(t, setup, "", compatBlock210)
	mustPromote(t, setup, "", "release-block", "2.1.0", false)
	applyIn(t, setup, "", strings.Replace(compatBlock210, `"2.1.0"`, `"2.2.0"`, 1))
	applyIn(t, setup, "", strings.Replace(compatBlock210, `"2.1.0"`, `"2.3.0"`, 1))

	svc := compatService(t, &racingStore{MemoryOrkflowStore: inner, racer: "2.2.0"}, nil)
	_, err := promoteVersion(svc, "", "release-block", "2.3.0", false)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("raced promotion = %v, want FailedPrecondition", err)
	}
	want := `orkflow "release-block" in namespace "default" was promoted to "2.2.0" by somebody else after this promotion compared release-block@2.3.0 against "2.1.0"; read the versions again and retry`
	if msg := connectMessage(err); msg != want {
		t.Fatalf("refusal\n got  %q\n want %q", msg, want)
	}
	requireResolvedVersion(t, setup, "release-block", "2.2.0", "after the raced promotion")
}

func TestSetCurrentOrkflowVersion_StoreOutageIsUnavailableAndMovesNothing(t *testing.T) {
	svc := compatService(t, downOrkflowStore{err: errors.New("dial tcp: connection refused")}, nil)
	_, err := promoteVersion(svc, "", "release-block", "3.0.0", true)
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("promotion during an outage = %v, want Unavailable", err)
	}
}

func TestListOrkflowVersions_MarksEachVersionAgainstTheCurrentOne(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	applyIn(t, svc, "", compatBlock300)
	applyIn(t, svc, "", compatBlock203)
	applyIn(t, svc, "", strings.Replace(compatBlock210, `"2.1.0"`, `"2.2.0"`, 1))
	listed, err := svc.ListOrkflowVersions(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "release-block"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	want := map[string]orkstackv1.InterfaceVerdict{
		"2.1.0": orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED,
		"3.0.0": orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING,
		// 2.0.3 drops both outputs 2.1.0 declares: BREAKING.
		"2.0.3": orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING,
		"2.2.0": orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE,
	}
	for _, row := range listed.Msg.GetVersions() {
		if got := row.GetInterfaceVerdict(); got != want[row.GetVersion()] {
			t.Errorf("%s: %v, want %v", row.GetVersion(), got, want[row.GetVersion()])
		}
	}

	// A version with 2.1.0's inputs and no outputs, against a current that
	// declares none either, is UNKNOWN.
	svc = compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock203)
	mustPromote(t, svc, "", "release-block", "2.0.3", false)
	applyIn(t, svc, "", strings.Replace(compatBlock203, `"2.0.3"`, `"2.0.4"`, 1))
	listed, err = svc.ListOrkflowVersions(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "release-block"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	for _, row := range listed.Msg.GetVersions() {
		wantV := orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN
		if row.GetIsCurrent() {
			wantV = orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED
		}
		if row.GetInterfaceVerdict() != wantV {
			t.Errorf("%s: %v, want %v", row.GetVersion(), row.GetInterfaceVerdict(), wantV)
		}
	}
}

func TestApplyOrkflow_StoresABreakingVersionAndWarns(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	resp := applyIn(t, svc, "", compatBlock300)
	if !resp.GetCreated() {
		t.Fatal("a breaking version must be stored")
	}
	if got := strings.Join(resp.GetWarnings(), "\n"); got != strings.Join(compatApplyWarnings, "\n") {
		t.Fatalf("warnings\n got:\n%s\nwant:\n%s", got, strings.Join(compatApplyWarnings, "\n"))
	}
	requireResolvedVersion(t, svc, "release-block", "2.1.0", "after applying a breaking version")
}

func TestApplyOrkflow_NeverPromotedWarningSaysApplyingChangesWhatRuns(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	resp := applyIn(t, svc, "", compatBlock300)
	want := "version: release-block@3.0.0 breaks the declared interface of release-block@2.1.0 (2 breaking changes), " +
		"and release-block was never promoted, so a run that names no version executes its newest applied version: " +
		"applying 3.0.0 makes it what runs. To keep 2.1.0 running, promote it before applying: orkctl orkflow promote release-block 2.1.0"
	warnings := resp.GetWarnings()
	if len(warnings) != 3 || warnings[2] != want {
		t.Fatalf("warnings = %q\nwant last = %q", warnings, want)
	}
}

func TestApplyOrkflow_RepeatApplyOfABreakingVersionSaysItIsAlreadyStored(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock300)
	applyIn(t, svc, "", compatBlock210) // head: 2.1.0, never promoted
	resp := applyIn(t, svc, "", compatBlock300)
	if resp.GetCreated() {
		t.Fatal("a repeat apply must not create")
	}
	want := "version: release-block@3.0.0 is already stored and breaks the declared interface of release-block@2.1.0, " +
		"which runs when no version is named (2 breaking changes); promoting it is refused until acknowledged"
	warnings := resp.GetWarnings()
	if len(warnings) == 0 || warnings[len(warnings)-1] != want {
		t.Fatalf("warnings = %q\nwant last = %q", warnings, want)
	}
}

func TestApplyOrkflow_CompatibleOrUnknownChangesAddNoWarning(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", versionedDocument("deploy", "1.0.0", "one"))
	mustPromote(t, svc, "", "deploy", "1.0.0", false)
	if resp := applyIn(t, svc, "", versionedDocument("deploy", "2.0.0", "two")); len(resp.GetWarnings()) != 0 {
		t.Fatalf("an UNKNOWN change warned: %q", resp.GetWarnings())
	}
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	if resp := applyIn(t, svc, "", strings.Replace(compatBlock210, `"2.1.0"`, `"2.2.0"`, 1)); len(resp.GetWarnings()) != 0 {
		t.Fatalf("a COMPATIBLE change warned: %q", resp.GetWarnings())
	}
}

func TestValidateOrkflow_ReturnsInterfaceWarningsAndWritesNothing(t *testing.T) {
	svc := compatService(t, nil, nil)
	applyIn(t, svc, "", compatBlock210)
	mustPromote(t, svc, "", "release-block", "2.1.0", false)
	// A pin whose members leave run_as_orker_user false adds the pin warning,
	// which must come after the interface warnings.
	pinned := strings.Replace(compatBlock300, "tasks:\n", "pins:\n  build: {}\ntasks:\n", 1)
	pinned = strings.Replace(pinned, "  - id: attest\n    uses: shell\n", "  - id: attest\n    uses: shell\n    pin: build\n", 1)
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: pinned}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	var fields []string
	for _, issue := range resp.Msg.GetIssues() {
		if issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
			t.Fatalf("non-warning issue: %v", issue)
		}
		fields = append(fields, issue.GetField())
	}
	joined := strings.Join(fields, ",")
	if !strings.Contains(joined, "inputs.region,outputs.url,version") {
		t.Fatalf("issue fields = %v, want inputs.region, outputs.url, version in order", fields)
	}
	if last := resp.Msg.GetIssues()[len(fields)-1]; !strings.Contains(last.GetMessage(), "pin") {
		t.Fatalf("the pin warning must come last: %v", fields)
	}
	listed, _ := svc.ListOrkflowVersions(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "release-block"}))
	if len(listed.Msg.GetVersions()) != 1 {
		t.Fatalf("validate stored a version: %v", versionRows(listed.Msg.GetVersions()))
	}
}

// versionsFailingStore answers everything but the version catalog.
type versionsFailingStore struct {
	*engine.MemoryOrkflowStore
}

func (versionsFailingStore) Versions(context.Context, string, string) (artifacts.OrkflowVersions, error) {
	return artifacts.OrkflowVersions{}, errors.New("dial tcp: connection refused")
}

func TestValidateOrkflow_UnreadableCurrentVersionIsAWarning(t *testing.T) {
	svc := compatService(t, versionsFailingStore{engine.NewMemoryOrkflowStore()}, nil)
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: compatBlock300}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	if resp.Msg.GetOrkflow() == nil {
		t.Fatal("validation must otherwise succeed")
	}
	issues := resp.Msg.GetIssues()
	if len(issues) != 1 || issues[0].GetField() != "version" ||
		issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		!strings.HasPrefix(issues[0].GetMessage(), "could not compare with the version that runs now: ") ||
		!strings.HasSuffix(issues[0].GetMessage(), "orkctl orkflow compat release-block 3.0.0") {
		t.Fatalf("issues = %v", issues)
	}
}

// countingOrkflowStore counts the calls a read must or must not make, and every
// write, so a test can say "this wrote nothing".
type countingOrkflowStore struct {
	*engine.MemoryOrkflowStore
	mu                       sync.Mutex
	versions, puts, sets, rm int
}

func (c *countingOrkflowStore) Versions(ctx context.Context, namespace, name string) (artifacts.OrkflowVersions, error) {
	c.mu.Lock()
	c.versions++
	c.mu.Unlock()
	return c.MemoryOrkflowStore.Versions(ctx, namespace, name)
}

func (c *countingOrkflowStore) Put(ctx context.Context, wf *orkstackv1.Orkflow, expected artifacts.HeadExpectation) (*orkstackv1.Orkflow, bool, error) {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	return c.MemoryOrkflowStore.Put(ctx, wf, expected)
}

func (c *countingOrkflowStore) SetCurrentVersion(ctx context.Context, namespace, name, version string, expected artifacts.CurrentExpectation) (string, error) {
	c.mu.Lock()
	c.sets++
	c.mu.Unlock()
	return c.MemoryOrkflowStore.SetCurrentVersion(ctx, namespace, name, version, expected)
}

func (c *countingOrkflowStore) Delete(ctx context.Context, namespace, name string) error {
	c.mu.Lock()
	c.rm++
	c.mu.Unlock()
	return c.MemoryOrkflowStore.Delete(ctx, namespace, name)
}

func (c *countingOrkflowStore) writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts + c.sets + c.rm
}

func TestPlanRun_YAMLFormReadsNoVersions(t *testing.T) {
	store := &countingOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := compatService(t, store, nil)
	if _, err := svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{
		Yaml: compatBlock210,
		Input: &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"image": structpb.NewStringValue("ghcr.io/acme/app:1"),
		}}},
	})); err != nil {
		t.Fatalf("PlanRun: %v", err)
	}
	if store.versions != 0 {
		t.Fatalf("PlanRun's YAML form read the version catalog %d times; the interface comparison "+
			"must stay out of prepareOrkflow", store.versions)
	}
}

const compatCheckout420 = `
name: checkout-service
version: "4.2.0"
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    with:
      image: ghcr.io/acme/checkout:4.2
      notes: from checkout
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ ne .tasks.release.output.url "" }}'
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

// callersFixture applies release-block 2.1.0 (promoted) and 3.0.0, and
// checkout-service 4.2.0 composing 2.1.0 (promoted) and 4.3.0 composing
// 3.0.0 (its head, never promoted beyond 4.2.0).
func callersFixture(t *testing.T, schedules api.ScheduleController) (*api.Service, *countingOrkflowStore) {
	t.Helper()
	store := &countingOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := compatService(t, store, schedules)
	applyIn(t, svc, "team-a", compatBlock210)
	mustPromote(t, svc, "team-a", "release-block", "2.1.0", false)
	applyIn(t, svc, "team-a", compatBlock300)
	applyIn(t, svc, "team-a", compatCheckout420)
	mustPromote(t, svc, "team-a", "checkout-service", "4.2.0", false)
	applyIn(t, svc, "team-a", strings.NewReplacer(`"4.2.0"`, `"4.3.0"`,
		"release-block@2.1.0", "release-block@3.0.0",
		"notes: from checkout", "region: eu",
		".tasks.release.output.url", ".tasks.release.output.sbom").Replace(compatCheckout420))
	return svc, store
}

func listCallers(t *testing.T, svc *api.Service, namespace, name string, all bool) *orkstackv1.ListOrkflowCallersResponse {
	t.Helper()
	resp, err := svc.ListOrkflowCallers(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowCallersRequest{Name: name, Namespace: namespace, AllVersions: all}))
	if err != nil {
		t.Fatalf("ListOrkflowCallers: %v", err)
	}
	return resp.Msg
}

func callerRow(c *orkstackv1.OrkflowCaller) string {
	kind := strings.ToLower(strings.TrimPrefix(c.GetKind().String(), "ORKFLOW_CALLER_KIND_"))
	pins := c.GetPinnedVersion()
	switch {
	case c.GetUnavailableReason() != "":
		pins = "unavailable"
	case pins == "":
		pins = "current"
	}
	label := c.GetName()
	if label == "" {
		label = c.GetId()
	}
	return fmt.Sprintf("%s %s %s %s %s reads=%s supplies=%s", kind, label, c.GetCallerVersion(),
		c.GetTaskId(), pins, strings.Join(c.GetOutputsRead(), ","), strings.Join(c.GetInputsSupplied(), ","))
}

func TestListOrkflowCallers_FindsComposingTriggersAndSchedules(t *testing.T) {
	schedules := newFakeSchedules()
	svc, _ := callersFixture(t, schedules)
	ctx := context.Background()
	store := svc.Triggers()
	follows, _, err := store.Create(ctx, triggers.Registration{Namespace: "team-a", OrkflowName: "release-block",
		Name: "ci-release", Description: "CI"})
	if err != nil {
		t.Fatal(err)
	}
	pinned, _, err := store.Create(ctx, triggers.Registration{Namespace: "team-a", OrkflowName: "release-block",
		OrkflowVersion: "2.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(ctx, triggers.Registration{Namespace: "team-a", OrkflowName: "checkout-service"}); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []*orkstackv1.ScheduleRecord{
		{ScheduleId: "sch-1", Name: "nightly-release", Namespace: "team-a", OrkflowName: "release-block",
			OrkflowVersion: "2.1.0", Description: "nightly",
			Input: &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{
				"image": structpb.NewStringValue("SCHEDULE-INPUT-SENTINEL")}}}},
		{ScheduleId: "sch-2", Name: "hourly-release", Namespace: "team-a", OrkflowName: "release-block"},
		{ScheduleId: "sch-3", Name: "other", Namespace: "team-a", OrkflowName: "checkout-service"},
	} {
		if _, err := schedules.Create(ctx, rec, true); err != nil {
			t.Fatal(err)
		}
	}
	schedules.extra = append(schedules.extra, engine.ScheduleView{ID: "sch-4", Name: "lost-release",
		Namespace: "team-a", OrkflowName: "release-block", Unavailable: "describe: deadline exceeded"})

	resp := listCallers(t, svc, "team-a", "release-block", false)
	var rows []string
	for _, c := range resp.GetCallers() {
		rows = append(rows, callerRow(c))
	}
	want := []string{
		"orkflow checkout-service 4.2.0 release 2.1.0 reads=digest,url supplies=image,notes",
		"trigger ci-release   current reads= supplies=",
		"trigger " + pinned.ID + "   2.1.0 reads= supplies=",
		"schedule hourly-release   current reads= supplies=",
		"schedule lost-release   unavailable reads= supplies=",
		"schedule nightly-release   2.1.0 reads= supplies=",
	}
	if strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Fatalf("callers\n got:\n%s\nwant:\n%s", strings.Join(rows, "\n"), strings.Join(want, "\n"))
	}
	if resp.GetCurrentVersion() != "2.1.0" || resp.GetStoredVersions() != 2 || resp.GetScannedVersions() != 1 {
		t.Fatalf("response = %v", resp)
	}
	first := resp.GetCallers()[0]
	if !first.GetCallerVersionIsCurrent() || first.GetPinnedDefinitionDigest() == "" {
		t.Fatalf("composing row = %v", first)
	}
	if resp.GetCallers()[1].GetId() != follows.ID || resp.GetCallers()[1].GetDescription() != "CI" {
		t.Fatalf("trigger row = %v", resp.GetCallers()[1])
	}
	if lost := resp.GetCallers()[4]; lost.GetUnavailableReason() != "describe: deadline exceeded" || lost.GetPinnedVersion() != "" {
		t.Fatalf("unavailable schedule row = %v", lost)
	}
}

func TestListOrkflowCallers_DefaultScansCurrentVersionsOnly(t *testing.T) {
	svc, _ := callersFixture(t, nil)
	resp := listCallers(t, svc, "team-a", "release-block", false)
	if len(resp.GetCallers()) != 1 || resp.GetCallers()[0].GetPinnedVersion() != "2.1.0" ||
		resp.GetScannedVersions() != 1 || resp.GetAllVersions() {
		t.Fatalf("default scan = %v", resp)
	}
}

func TestListOrkflowCallers_AllVersionsScansEveryStoredVersion(t *testing.T) {
	svc, _ := callersFixture(t, nil)
	resp := listCallers(t, svc, "team-a", "release-block", true)
	var rows []string
	for _, c := range resp.GetCallers() {
		rows = append(rows, fmt.Sprintf("%s@%s current=%v pins=%s supplies=%s", c.GetName(), c.GetCallerVersion(),
			c.GetCallerVersionIsCurrent(), c.GetPinnedVersion(), strings.Join(c.GetInputsSupplied(), ",")))
	}
	want := "checkout-service@4.3.0 current=false pins=3.0.0 supplies=image,region\n" +
		"checkout-service@4.2.0 current=true pins=2.1.0 supplies=image,notes"
	if strings.Join(rows, "\n") != want {
		t.Fatalf("all versions\n got:\n%s\nwant:\n%s", strings.Join(rows, "\n"), want)
	}
	if resp.GetScannedVersions() != 2 || !resp.GetAllVersions() {
		t.Fatalf("scanned = %d, all = %v", resp.GetScannedVersions(), resp.GetAllVersions())
	}
}

func TestListOrkflowCallers_ADeletedOrkflowStillListsItsCallers(t *testing.T) {
	svc, store := callersFixture(t, nil)
	if err := store.MemoryOrkflowStore.Delete(context.Background(), "team-a", "release-block"); err != nil {
		t.Fatal(err)
	}
	resp := listCallers(t, svc, "team-a", "release-block", false)
	if resp.GetStoredVersions() != 0 || resp.GetCurrentVersion() != "" || len(resp.GetCallers()) != 1 {
		t.Fatalf("deleted orkflow = %v", resp)
	}
}

func TestListOrkflowCallers_IsNamespaceScoped(t *testing.T) {
	schedules := newFakeSchedules()
	svc, _ := callersFixture(t, schedules)
	ctx := context.Background()
	if _, _, err := svc.Triggers().Create(ctx, triggers.Registration{Namespace: "team-a", OrkflowName: "release-block"}); err != nil {
		t.Fatal(err)
	}
	if _, err := schedules.Create(ctx, &orkstackv1.ScheduleRecord{ScheduleId: "s", Name: "n", Namespace: "team-a",
		OrkflowName: "release-block"}, true); err != nil {
		t.Fatal(err)
	}
	resp := listCallers(t, svc, "team-b", "release-block", true)
	if len(resp.GetCallers()) != 0 || resp.GetStoredVersions() != 0 || resp.GetScannedVersions() != 0 {
		t.Fatalf("team-b sees team-a's callers: %v", resp)
	}
}

func TestListOrkflowCallers_ScheduleListFailureIsUnavailable(t *testing.T) {
	schedules := newFakeSchedules()
	schedules.listErr = errors.New("cluster unreachable")
	svc, _ := callersFixture(t, schedules)
	_, err := svc.ListOrkflowCallers(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowCallersRequest{Name: "release-block", Namespace: "team-a"}))
	if connect.CodeOf(err) != connect.CodeUnavailable ||
		!strings.Contains(connectMessage(err), `cannot list the schedules of namespace "team-a"`) {
		t.Fatalf("schedule list failure = %v, want Unavailable", err)
	}
}

func TestListOrkflowCallers_NeverCarriesASecretOrAScheduleInput(t *testing.T) {
	schedules := newFakeSchedules()
	svc, _ := callersFixture(t, schedules)
	ctx := context.Background()
	trig, secret, err := svc.Triggers().Create(ctx, triggers.Registration{Namespace: "team-a", OrkflowName: "release-block"})
	if err != nil {
		t.Fatal(err)
	}
	stored, _, _ := svc.Triggers().Get(ctx, trig.ID)
	if _, err := schedules.Create(ctx, &orkstackv1.ScheduleRecord{ScheduleId: "s", Name: "n", Namespace: "team-a",
		OrkflowName: "release-block", Input: &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"image": structpb.NewStringValue("SCHEDULE-INPUT-SENTINEL")}}}}, true); err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(listCallers(t, svc, "team-a", "release-block", true))
	if err != nil {
		t.Fatal(err)
	}
	body := string(encoded)
	for _, forbidden := range []string{secret, "SCHEDULE-INPUT-SENTINEL",
		fmt.Sprintf("%x", stored.SecretSalt), fmt.Sprintf("%x", stored.SecretHash)} {
		if forbidden != "" && strings.Contains(body, forbidden) {
			t.Fatalf("the callers response carries %q: %s", forbidden, body)
		}
	}
}

func TestListOrkflowCallers_WritesNothing(t *testing.T) {
	svc, store := callersFixture(t, newFakeSchedules())
	before := store.writes()
	listCallers(t, svc, "team-a", "release-block", true)
	listCallers(t, svc, "team-a", "release-block", false)
	if _, err := svc.CompareInterfaces(context.Background(), connect.NewRequest(&orkstackv1.CompareInterfacesRequest{
		Name: "release-block", FromVersion: "2.1.0", ToVersion: "3.0.0", Namespace: "team-a"})); err != nil {
		t.Fatal(err)
	}
	if after := store.writes(); after != before {
		t.Fatalf("callers and compare wrote %d times", after-before)
	}
}

func TestListOrkflowCallers_RefusesAnInvalidName(t *testing.T) {
	svc := compatService(t, nil, nil)
	for _, name := range []string{"", "Bad_Name"} {
		_, err := svc.ListOrkflowCallers(context.Background(), connect.NewRequest(
			&orkstackv1.ListOrkflowCallersRequest{Name: name}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%q: %v, want InvalidArgument", name, err)
		}
	}
}

package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Sharing across namespaces (docs/DSL.md): platform owns deploy, search
// composes it.

func sharedDeployYAML(sharedWith string, tasks string) string {
	if tasks == "" {
		tasks = `  - id: ship
    uses: shell
    with: { op: eval, command: ["true"] }
`
	}
	return `apiVersion: orkstack/v1
kind: Orkflow
namespace: platform
name: deploy
version: "1.0.0"
` + sharedWith + `
tasks:
` + tasks
}

const searchReleaseYAML = `apiVersion: orkstack/v1
kind: Orkflow
namespace: search
name: release
version: "1"
tasks:
  - id: deploy
    uses: orkflow:platform/deploy@1.0.0
`

type shareFixture struct {
	*fixture
	store *engine.MemoryOrkflowStore
	reg   *fakeRegistry
}

func newShareFixture(t *testing.T, namespaces ...*orkstackv1.Namespace) *shareFixture {
	t.Helper()
	store := engine.NewMemoryOrkflowStore()
	if len(namespaces) == 0 {
		namespaces = []*orkstackv1.Namespace{{Name: "platform"}, {Name: "search"}, {Name: "payments"}}
	}
	reg := newFakeRegistry(namespaces...)
	starter := &fakeStarter{}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Orkflows = store
		cfg.Namespaces = reg
		cfg.Starter = indexingStarter{starter}
		cfg.Schedules = newFakeSchedules()
	})
	f.starter = starter
	return &shareFixture{fixture: f, store: store, reg: reg}
}

func (f *shareFixture) apply(yaml, namespace string) error {
	_, err := f.orkflowClient().ApplyOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: yaml, Namespace: namespace}))
	return err
}

func (f *shareFixture) mustApply(t *testing.T, yaml, namespace string) {
	t.Helper()
	if err := f.apply(yaml, namespace); err != nil {
		t.Fatalf("ApplyOrkflow into %s: %v", namespace, err)
	}
}

func (f *shareFixture) issues(t *testing.T, yaml, namespace string) []*orkstackv1.ValidationIssue {
	t.Helper()
	resp, err := f.orkflowClient().ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: yaml, Namespace: namespace}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	return resp.Msg.GetIssues()
}

func (f *shareFixture) submit(name, namespace string) (*orkstackv1.Run, error) {
	resp, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: name, Namespace: namespace,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

func (f *shareFixture) plans() []engine.RunPlan {
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	return append([]engine.RunPlan(nil), f.starter.plans...)
}

func (f *shareFixture) requireNothingBehind(t *testing.T) {
	t.Helper()
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submit started %v", started)
	}
	if id := f.svc.NewRunIDForTest(); id != "run-001" {
		t.Fatalf("a refused submit minted ids: next is %s", id)
	}
}

func TestValidateOrkflow_ComposingAnUnsharedOrkflowIsAnError(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, sharedDeployYAML("", ""), "platform")
	requireIssue(t, f.issues(t, searchReleaseYAML, "search"), "tasks[0].uses", authz.ReasonNotShared, errorIssue)
	err := f.apply(searchReleaseYAML, "search")
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), `"platform/deploy@1.0.0" is not shared with "search/release"`) {
		t.Fatalf("apply = %v", err)
	}
	if _, err := f.store.Get(context.Background(), "search", "release", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a refused apply stored something: %v", err)
	}
}

func TestApplyOrkflow_EverySharingLevelAdmitsItsCallers(t *testing.T) {
	cases := []struct {
		name, sharedWith string
		admitted         bool
	}{
		{"every namespace", "shared_with:\n  - namespace: \"*\"", true},
		{"the caller's namespace", "shared_with:\n  - namespace: search", true},
		{"another namespace", "shared_with:\n  - namespace: payments", false},
		{"the calling orkflow", "shared_with:\n  - orkflow: search/release", true},
		{"another orkflow of the caller's namespace", "shared_with:\n  - orkflow: search/nightly", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newShareFixture(t)
			f.mustApply(t, sharedDeployYAML(tc.sharedWith, ""), "platform")
			err := f.apply(searchReleaseYAML, "search")
			if tc.admitted != (err == nil) {
				t.Fatalf("apply = %v, admitted want %v", err, tc.admitted)
			}
			if !tc.admitted && !strings.Contains(err.Error(), authz.ReasonNotShared) {
				t.Fatalf("refusal %v does not carry %s", err, authz.ReasonNotShared)
			}
		})
	}
}

func TestSubmitRun_ASharedChildRunsInTheParentsNamespaceAndRecordsItsOwner(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", ""), "platform")
	f.mustApply(t, searchReleaseYAML, "search")
	if _, err := f.submit("release", "search"); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	child := f.plans()[0].Children["deploy"]
	if child == nil {
		t.Fatal("the shared child is not planned")
	}
	def, err := child.Definition()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.store.Get(context.Background(), "platform", "deploy", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if def.GetNamespace() != "search" || def.GetDefinitionNamespace() != "platform" {
		t.Fatalf("child definition runs in %q, owned by %q; want search, owned by platform",
			def.GetNamespace(), def.GetDefinitionNamespace())
	}
	if def.GetDefinitionDigest() != stored.GetDefinitionDigest() {
		t.Fatalf("child digest %q is not the owner's %q", def.GetDefinitionDigest(), stored.GetDefinitionDigest())
	}
	want := &authz.Decision{Action: authz.ActionRun, Rule: "namespace:search", Callee: "platform/deploy@1.0.0", Caller: "search/release"}
	if got := child.Plan.Shared; got == nil || *got != *want {
		t.Fatalf("child plan decision = %+v, want %+v", got, want)
	}
	if f.plans()[0].Shared != nil {
		t.Fatal("the root plan records a sharing decision; only a shared child has one")
	}
}

func TestSubmitRun_ANestedChildIsCheckedAgainstItsOwnShare(t *testing.T) {
	f := newShareFixture(t)
	helper := `apiVersion: orkstack/v1
kind: Orkflow
namespace: platform
name: helper
version: "1"
tasks:
  - id: t
    uses: shell
    with: { op: eval, command: ["true"] }
`
	f.mustApply(t, helper, "platform")
	// deploy composes its own namespace's helper: shared with search, deploy
	// carries helper along with no entry of helper's own.
	f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", `  - id: help
    uses: orkflow:helper@1
`), "platform")
	f.mustApply(t, searchReleaseYAML, "search")
	if _, err := f.submit("release", "search"); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	nested := f.plans()[0].Children["deploy"].Plan.Children["help"]
	def, err := nested.Definition()
	if err != nil {
		t.Fatal(err)
	}
	if def.GetNamespace() != "search" || def.GetDefinitionNamespace() != "platform" {
		t.Fatalf("nested child runs in %q owned by %q", def.GetNamespace(), def.GetDefinitionNamespace())
	}
	if nested.Plan.Shared != nil {
		t.Fatalf("a same-namespace call recorded a sharing decision: %+v", nested.Plan.Shared)
	}
}

func TestSubmitRun_AParentStoredBeforeTheShareWasWithdrawnIsRefused(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, sharedDeployYAML("", ""), "platform")
	stored, err := f.store.Get(context.Background(), "platform", "deploy", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	// A parent written straight to the store, as one applied by a build that
	// did not check sharing would sit there.
	parent := &orkstackv1.Orkflow{Namespace: "search", Name: "release", Version: "1", DefinitionDigest: "sha256:parent",
		Tasks: []*orkstackv1.Task{{Id: "deploy", Uses: "orkflow:platform/deploy@1.0.0",
			Orkflow: &orkstackv1.OrkflowRef{Namespace: "platform", Name: "deploy", Version: "1.0.0", DefinitionDigest: stored.GetDefinitionDigest()}}}}
	if _, _, err := f.store.Put(context.Background(), parent, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	_, err = f.submit("release", "search")
	requireRefusal(t, err, connect.CodeFailedPrecondition, authz.ReasonNotShared, `"platform/deploy@1.0.0"`, `task "deploy"`)
	f.requireNothingBehind(t)
}

func TestApplyOrkflow_ASharedOrkflowNamingSecretsNeedsTheCallersSecretPolicy(t *testing.T) {
	secrets := `  - id: ship
    uses: shell
    with: { op: eval, command: ["true"] }
    secrets:
      - name: token
        source: env:DEPLOY_TOKEN
`
	t.Run("unrestricted", func(t *testing.T) {
		f := newShareFixture(t)
		f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", secrets), "platform")
		err := f.apply(searchReleaseYAML, "search")
		if err == nil || !strings.Contains(err.Error(), engine.ReasonForeignSecretsUnbounded) ||
			!strings.Contains(err.Error(), "allowed_secrets") {
			t.Fatalf("apply = %v, want %s", err, engine.ReasonForeignSecretsUnbounded)
		}
	})
	t.Run("listed", func(t *testing.T) {
		f := newShareFixture(t, &orkstackv1.Namespace{Name: "platform"},
			&orkstackv1.Namespace{Name: "search", AllowedSecrets: []string{"env:DEPLOY_*"}})
		f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", secrets), "platform")
		f.mustApply(t, searchReleaseYAML, "search")
		if _, err := f.submit("release", "search"); err != nil {
			t.Fatalf("SubmitRun: %v", err)
		}
	})
	t.Run("refused at submit too", func(t *testing.T) {
		f := newShareFixture(t, &orkstackv1.Namespace{Name: "platform"},
			&orkstackv1.Namespace{Name: "search", AllowedSecrets: []string{"env:DEPLOY_*"}})
		f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", secrets), "platform")
		f.mustApply(t, searchReleaseYAML, "search")
		ns, err := f.reg.Get(context.Background(), "search")
		if err != nil {
			t.Fatal(err)
		}
		ns.AllowedSecrets = nil
		if _, err := f.reg.Put(context.Background(), ns, artifacts.PutConditions{}); err != nil {
			t.Fatal(err)
		}
		_, err = f.submit("release", "search")
		requireRefusal(t, err, connect.CodeInvalidArgument, engine.ReasonForeignSecretsUnbounded)
		f.requireNothingBehind(t)
	})
}

func TestValidateOrkflow_AQualifiedRefToItsOwnNamespaceIsAnError(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, sharedDeployYAML("", ""), "platform")
	own := strings.Replace(searchReleaseYAML, "namespace: search", "namespace: platform", 1)
	requireIssue(t, f.issues(t, own, "platform"), "tasks[0].uses", "write orkflow:deploy@1.0.0", errorIssue)
}

func TestValidateOrkflow_SharedWithItsOwnNamespaceIsRefusedWhereItIsApplied(t *testing.T) {
	f := newShareFixture(t)
	doc := strings.Replace(sharedDeployYAML("shared_with:\n  - namespace: platform", ""), "namespace: platform\nname", "name", 1)
	err := f.apply(doc, "platform")
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "needs no entry") {
		t.Fatalf("apply = %v", err)
	}
}

func TestValidateOrkflow_ComposingAMissingNamespacesOrkflowNamesIt(t *testing.T) {
	f := newShareFixture(t)
	requireIssue(t, f.issues(t, searchReleaseYAML, "search"), "tasks[0].uses", `no orkflow "deploy" exists in namespace "platform"`, errorIssue)
}

func TestCreateSchedule_ATargetInAnotherNamespaceIsRefused(t *testing.T) {
	f := newShareFixture(t)
	_, err := f.svc.CreateSchedule(context.Background(), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Namespace: "search", Name: "nightly", OrkflowName: "platform/deploy",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "a schedule runs only its own namespace's Orkflows") {
		t.Fatalf("CreateSchedule = %v", err)
	}
}

func TestCreateTrigger_ATargetInAnotherNamespaceIsRefused(t *testing.T) {
	f := newShareFixture(t)
	_, err := f.triggerClient().CreateTrigger(context.Background(), connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		Namespace: "search", OrkflowName: "platform/deploy",
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "a trigger runs only its own namespace's Orkflows") {
		t.Fatalf("CreateTrigger = %v", err)
	}
}

func TestSubmitRun_RunAgainOfASharedChildIsRefused(t *testing.T) {
	h := newOriginHarness(t)
	h.reader.setRun(&orkstackv1.Run{Id: "ork-shared", Namespace: "default", OrkflowName: "hello-ork",
		OrkflowNamespace: "platform", Status: orkstackv1.Status_STATUS_SUCCEEDED, Origin: &orkstackv1.RunOrigin{
			Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-parent", RootRunId: "ork-root",
		}})
	readsBefore := h.orkflows.reads()
	_, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", SourceRunId: "ork-shared"}, "")
	requireRefusal(t, err, connect.CodeFailedPrecondition, authz.ReasonSharedNotRoot,
		`"platform/hello-ork"`, "ork-root")
	if h.orkflows.reads() != readsBefore {
		t.Fatal("a refused run again read the orkflow store")
	}
	if got := h.starter.startedRuns(); len(got) != 0 {
		t.Fatalf("a refused run again started %v", got)
	}
}

// TestListOrkflowCallers_AQualifiedRefIsNotACallerOfTheSameNameHere: an
// Orkflow composing platform/deploy is no caller of search's own deploy.
func TestListOrkflowCallers_AQualifiedRefIsNotACallerOfTheSameNameHere(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, sharedDeployYAML("shared_with:\n  - namespace: search", ""), "platform")
	f.mustApply(t, strings.Replace(sharedDeployYAML("", ""), "namespace: platform", "namespace: search", 1), "search")
	f.mustApply(t, searchReleaseYAML, "search")
	resp, err := f.orkflowClient().ListOrkflowCallers(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowCallersRequest{Name: "deploy", Namespace: "search"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, caller := range resp.Msg.GetCallers() {
		if strings.Contains(caller.String(), "release") {
			t.Fatalf("search/release, which composes platform/deploy, is listed as a caller of search's deploy: %v", caller)
		}
	}
}

// TestMostFailing_KeepsASharedChildApartFromTheOwnOrkflowOfItsName: the
// tally keys a shared child's runs "<owner>/<name>", and the row names both.
func TestMostFailing_KeepsASharedChildApartFromTheOwnOrkflowOfItsName(t *testing.T) {
	rows := api.MostFailingForTest(engine.RunTally{ByOrkflow: map[string]engine.OrkflowTally{
		"deploy":          {Failed: 1, Total: 2},
		"platform/deploy": {Failed: 1, Total: 2},
	}})
	if len(rows) != 2 || rows[0].GetOrkflowName() != "deploy" || rows[1].GetOrkflowName() != "deploy" ||
		rows[0].GetOrkflowNamespace() != "" || rows[1].GetOrkflowNamespace() != "platform" {
		t.Fatalf("most failing = %v", rows)
	}
}

func (f *shareFixture) seedRun(id, namespace, owner string, status orkstackv1.Status) {
	f.runs.Apply(&orkstackv1.Run{Id: id, Namespace: namespace, OrkflowName: "deploy", OrkflowVersion: "1.0.0",
		OrkflowNamespace: owner, Status: status, CreatedAt: timestamppb.New(time.Now())})
}

func (f *shareFixture) deleteOrkflow(name, namespace string) error {
	_, err := f.orkflowClient().DeleteOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.DeleteOrkflowRequest{Name: name, Namespace: namespace}))
	return err
}

// Deleting a shared Orkflow sees its runs in every namespace it is shared
// with, because they ran its definition there.
func TestDeleteOrkflow_SeesASharedOrkflowsRunsInOtherNamespaces(t *testing.T) {
	for name, sharedWith := range map[string]string{
		"one namespace":   "shared_with:\n  - namespace: search",
		"every namespace": "shared_with:\n  - namespace: \"*\"",
		"one orkflow":     "shared_with:\n  - orkflow: search/release",
	} {
		t.Run(name, func(t *testing.T) {
			f := newShareFixture(t)
			f.mustApply(t, sharedDeployYAML(sharedWith, ""), "platform")
			f.seedRun("ork-shared", "search", "platform", orkstackv1.Status_STATUS_RUNNING)
			err := f.deleteOrkflow("deploy", "platform")
			requireRefusal(t, err, connect.CodeFailedPrecondition, `cannot delete orkflow "deploy" in namespace "platform"`,
				`1 in namespace "search"`)
			if _, err := f.store.Get(context.Background(), "platform", "deploy", ""); err != nil {
				t.Fatalf("a refused delete removed the orkflow: %v", err)
			}
		})
	}
}

// Deleting a namespace's own Orkflow does not count a shared child of
// another namespace's Orkflow that happens to have its name.
func TestDeleteOrkflow_IgnoresASharedChildOfTheSameNameHere(t *testing.T) {
	f := newShareFixture(t)
	f.mustApply(t, strings.Replace(sharedDeployYAML("", ""), "namespace: platform", "namespace: search", 1), "search")
	f.seedRun("ork-shared", "search", "platform", orkstackv1.Status_STATUS_RUNNING)
	if err := f.deleteOrkflow("deploy", "search"); err != nil {
		t.Fatalf("delete of search's own deploy, blocked by platform/deploy's run: %v", err)
	}
	// Its own runs still block it.
	g := newShareFixture(t)
	g.mustApply(t, strings.Replace(sharedDeployYAML("", ""), "namespace: platform", "namespace: search", 1), "search")
	g.seedRun("ork-own", "search", "", orkstackv1.Status_STATUS_RUNNING)
	requireRefusal(t, g.deleteOrkflow("deploy", "search"), connect.CodeFailedPrecondition, "1 active")
}

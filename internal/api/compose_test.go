package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const composeChildYAML = `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
  count: { type: number }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.sign.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

const composeParentYAML = `
name: deploy
version: "1.0.0"
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
    with:
      image: '{{ .tasks.build.output.value }}'
  - id: announce
    uses: shell
    needs: [release]
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

// composeFixture is one replica over a shared orkflow store and a namespace
// registry holding team-a.
type composeFixture struct {
	*fixture
	store *engine.MemoryOrkflowStore
	reg   *fakeRegistry
}

func newComposeFixture(t *testing.T, configure ...func(*api.Config)) *composeFixture {
	t.Helper()
	store := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "team-a"})
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Orkflows = store
		cfg.Namespaces = reg
		for _, edit := range configure {
			edit(cfg)
		}
	})
	return &composeFixture{fixture: f, store: store, reg: reg}
}

func (f *composeFixture) apply(t *testing.T, yaml string) (*orkstackv1.ApplyOrkflowResponse, error) {
	t.Helper()
	resp, err := f.orkflowClient().ApplyOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: yaml, Namespace: "team-a"}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (f *composeFixture) mustApply(t *testing.T, yaml string) *orkstackv1.Orkflow {
	t.Helper()
	resp, err := f.apply(t, yaml)
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.GetOrkflow()
}

func (f *composeFixture) validate(t *testing.T, yaml string) []*orkstackv1.ValidationIssue {
	t.Helper()
	resp, err := f.orkflowClient().ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: yaml, Namespace: "team-a"}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	return resp.Msg.GetIssues()
}

func (f *composeFixture) submit(name string) (*orkstackv1.SubmitRunResponse, error) {
	resp, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: name, Namespace: "team-a",
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (f *composeFixture) plans() []engine.RunPlan {
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	return append([]engine.RunPlan(nil), f.starter.plans...)
}

// putStored writes a hand-built definition straight into the store, as a
// document applied by another release (or deleted and re-applied) would sit
// there. digest is its stamped definition digest.
func (f *composeFixture) putStored(t *testing.T, name, version, digest string, tasks ...*orkstackv1.Task) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Namespace: "team-a", Name: name, Version: version, DefinitionDigest: digest, Tasks: tasks}
	if _, _, err := f.store.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("put %s@%s: %v", name, version, err)
	}
}

func composedTask(id, name, version, digest string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: "orkflow:" + name + "@" + version,
		Orkflow: &orkstackv1.OrkflowRef{Name: name, Version: version, DefinitionDigest: digest}}
}

func shellTask(id string) *orkstackv1.Task {
	with, _ := structpb.NewStruct(map[string]any{"op": "eval", "command": []any{"true"}})
	return &orkstackv1.Task{Id: id, Uses: "shell", With: with, TimeoutSeconds: 300}
}

func findIssue(issues []*orkstackv1.ValidationIssue, field, contains string) *orkstackv1.ValidationIssue {
	for _, issue := range issues {
		if issue.GetField() == field && strings.Contains(issue.GetMessage(), contains) {
			return issue
		}
	}
	return nil
}

func requireIssue(t *testing.T, issues []*orkstackv1.ValidationIssue, field, contains string, severity orkstackv1.ValidationSeverity) {
	t.Helper()
	issue := findIssue(issues, field, contains)
	if issue == nil {
		t.Fatalf("no issue at %s containing %q; issues: %v", field, contains, issues)
	}
	if issue.GetSeverity() != severity {
		t.Fatalf("issue at %s is %v, want %v", field, issue.GetSeverity(), severity)
	}
}

const (
	errorIssue   = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR
	warningIssue = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING
)

func TestApplyOrkflow_StampsTheChildDigest(t *testing.T) {
	f := newComposeFixture(t)
	child := f.mustApply(t, composeChildYAML)
	parent := f.mustApply(t, composeParentYAML)
	ref := parent.GetTasks()[1].GetOrkflow()
	if ref.GetDefinitionDigest() == "" || ref.GetDefinitionDigest() != child.GetDefinitionDigest() {
		t.Fatalf("stored ref digest = %q, child digest %q", ref.GetDefinitionDigest(), child.GetDefinitionDigest())
	}
	unstamped := proto.Clone(parent).(*orkstackv1.Orkflow)
	unstamped.Tasks[1].Orkflow.DefinitionDigest = ""
	if digest, _ := dsl.DefinitionDigest(unstamped); digest == parent.GetDefinitionDigest() {
		t.Fatal("the parent's digest does not cover the child's")
	}
	got, err := f.store.Get(context.Background(), "team-a", "deploy", "1.0.0")
	if err != nil || got.GetTasks()[1].GetOrkflow().GetDefinitionDigest() != child.GetDefinitionDigest() {
		t.Fatalf("stored parent = %v, %v", got, err)
	}
}

func TestValidateOrkflow_UnknownChildVersionNamesTheVersionsThatExist(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, strings.Replace(composeChildYAML, `"2.1.0"`, `"2.0.3"`, 1))
	f.mustApply(t, composeChildYAML)
	issues := f.validate(t, strings.Replace(composeParentYAML, "release-block@2.1.0", "release-block@9.9.9", 1))
	requireIssue(t, issues, "tasks[1].uses",
		`orkflow "release-block" has no version "9.9.9" in namespace "team-a" (versions: 2.1.0 (current), 2.0.3)`, errorIssue)
	issues = f.validate(t, strings.Replace(composeParentYAML, "release-block@2.1.0", "ghost@1", 1))
	requireIssue(t, issues, "tasks[1].uses", `no orkflow "ghost" exists in namespace "team-a"`, errorIssue)
}

// failingVersions is an orkflow store whose catalog read fails.
type failingVersions struct{ *engine.MemoryOrkflowStore }

func (failingVersions) Versions(context.Context, string, string) (artifacts.OrkflowVersions, error) {
	return artifacts.OrkflowVersions{}, errors.New("bucket unreachable")
}

func TestValidateOrkflow_MissingVersionIsEnrichedWithStoredVersions(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	noVersion := strings.Replace(composeParentYAML, "release-block@2.1.0", "release-block", 1)
	issues := f.validate(t, noVersion)
	want := `task "release": uses: orkflow:release-block needs a version — a composed task pins one immutable version and never follows a pointer; list them with orkctl orkflow versions release-block; versions in namespace "team-a": 2.1.0 (current)`
	if len(issues) != 1 || issues[0].GetField() != "tasks[1].uses" || issues[0].GetMessage() != want {
		t.Fatalf("issues = %v\nwant %s", issues, want)
	}
	other := strings.Replace(composeParentYAML, "release-block@2.1.0", "ghost", 1)
	if issues := f.validate(t, other); len(issues) != 1 ||
		!strings.HasSuffix(issues[0].GetMessage(), `; no orkflow "ghost" exists in namespace "team-a"`) {
		t.Fatalf("issues = %v", issues)
	}

	broken := newComposeFixture(t, func(cfg *api.Config) {
		cfg.Orkflows = failingVersions{engine.NewMemoryOrkflowStore()}
	})
	issues = broken.validate(t, noVersion)
	compilerOnly := `task "release": uses: orkflow:release-block needs a version — a composed task pins one immutable version and never follows a pointer; list them with orkctl orkflow versions release-block`
	if len(issues) != 1 || issues[0].GetMessage() != compilerOnly {
		t.Fatalf("a failed catalog read changed the sentence: %v", issues)
	}
}

func TestValidateOrkflow_StaleDescendantDigestIsAnError(t *testing.T) {
	f := newComposeFixture(t)
	f.putStored(t, "notarize", "1.0", "sha256:bbb", shellTask("stamp"))
	f.putStored(t, "release-block", "2.1.0", "sha256:rb", composedTask("notarize", "notarize", "1.0", "sha256:aaa"))
	issues := f.validate(t, composeParentYAMLWithout(t))
	requireIssue(t, issues, "tasks[1].uses",
		"release → release-block@2.1.0 → notarize@1.0: release-block@2.1.0 pins notarize@1.0 as sha256:aaa, but the stored notarize@1.0 is sha256:bbb — it was deleted and re-applied with other content; apply release-block as a new version to re-pin it",
		errorIssue)
}

// composeParentYAMLWithout is the parent with a release task that reads
// nothing of its child, so only the tree is under test.
func composeParentYAMLWithout(t *testing.T) string {
	t.Helper()
	return `
name: deploy
version: "1.0.0"
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
`
}

func TestValidateOrkflow_CompositionCycleIsAnError(t *testing.T) {
	f := newComposeFixture(t)
	f.putStored(t, "release-block", "2.1.0", "sha256:rb", composedTask("notarize", "notarize", "1.0", "sha256:nt"))
	f.putStored(t, "notarize", "1.0", "sha256:nt", composedTask("back", "release-block", "2.1.0", "sha256:rb"))
	issues := f.validate(t, composeParentYAMLWithout(t))
	requireIssue(t, issues, "tasks[1].uses",
		"composition cycle: deploy@1.0.0 → release-block@2.1.0 → notarize@1.0 → release-block@2.1.0", errorIssue)
}

func TestValidateOrkflow_TreeDeeperThanTheCapIsAnError(t *testing.T) {
	f := newComposeFixture(t)
	// release-block → l2 → l3 → l4 → l5: five levels below deploy.
	f.putStored(t, "l5", "1", "sha256:l5", shellTask("x"))
	f.putStored(t, "l4", "1", "sha256:l4", composedTask("x", "l5", "1", "sha256:l5"))
	f.putStored(t, "l3", "1", "sha256:l3", composedTask("x", "l4", "1", "sha256:l4"))
	f.putStored(t, "l2", "1", "sha256:l2", composedTask("x", "l3", "1", "sha256:l3"))
	f.putStored(t, "release-block", "2.1.0", "sha256:rb", composedTask("x", "l2", "1", "sha256:l2"))
	issues := f.validate(t, composeParentYAMLWithout(t))
	requireIssue(t, issues, "tasks[1].uses",
		"composition is 5 levels deep (deploy → release-block → l2 → l3 → l4 → l5); this deployment allows 4 (ORK_COMPOSE_MAX_DEPTH)",
		errorIssue)
}

func TestValidateOrkflow_ChildPoolAndSecretPolicyAreChecked(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, strings.Replace(composeChildYAML, "  - id: sign\n    uses: shell\n",
		"  - id: sign\n    uses: shell\n    pool: gpu\n    secrets: [{name: key, source: \"env:SIGNING_KEY\"}]\n", 1))
	ns, _ := f.reg.Get(context.Background(), "team-a")
	next := proto.Clone(ns).(*orkstackv1.Namespace)
	next.AllowedPools = []string{"main"}
	next.AllowedSecrets = []string{"env:TEAM_A_*"}
	if _, err := f.reg.Put(context.Background(), next, artifacts.PutConditions{}); err != nil {
		t.Fatal(err)
	}
	issues := f.validate(t, composeParentYAML)
	requireIssue(t, issues, "tasks[1].uses", "child orkflow release-block@2.1.0 (task release): ", errorIssue)
	requireIssue(t, issues, "tasks[1].uses", "gpu", errorIssue)
	requireIssue(t, issues, "tasks[1].uses", "env:SIGNING_KEY", errorIssue)
}

func TestValidateOrkflow_ComposedDefinitionsOverThePlanBudget(t *testing.T) {
	f := newComposeFixture(t)
	big, _ := structpb.NewStruct(map[string]any{"op": "eval", "blob": strings.Repeat("x", 300*1024)})
	task := shellTask("sign")
	task.With = big
	f.putStored(t, "release-block", "2.1.0", "sha256:rb", task)
	issues := f.validate(t, composeParentYAMLWithout(t))
	requireIssue(t, issues, "tasks[1].uses", "the Orkflows this document composes total", errorIssue)
	requireIssue(t, issues, "tasks[1].uses", "which may be at most 256 KiB — compose fewer or smaller Orkflows", errorIssue)
}

func TestValidateOrkflow_ChildInputContract(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	missingRequired := strings.Replace(composeParentYAML, "      image: '{{ .tasks.build.output.value }}'\n",
		"      count: '{{ .tasks.build.output.n }}'\n", 1)
	issues := f.validate(t, missingRequired)
	requireIssue(t, issues, "tasks[1].with.image", "with.image is required by release-block@2.1.0 and is not supplied", errorIssue)
	if findIssue(issues, "tasks[1].with.count", "") != nil {
		t.Fatalf("a templated value into a number input was typed: %v", issues)
	}

	warnings := strings.Replace(composeParentYAML, "      image: '{{ .tasks.build.output.value }}'\n",
		"      image: '{{ .tasks.build.output.value }}'\n      count: three\n      extra: 1\n", 1)
	issues = f.validate(t, warnings)
	requireIssue(t, issues, "tasks[1].with.extra",
		"with.extra is not an input of release-block@2.1.0 (inputs: count, image, notes); the child run will carry it, and no template of the child can read it",
		warningIssue)
	requireIssue(t, issues, "tasks[1].with.count", "with.count is a string, but release-block@2.1.0 declares input count as number", warningIssue)
	resp, err := f.apply(t, warnings)
	if err != nil {
		t.Fatalf("warnings refused the apply: %v", err)
	}
	joined := strings.Join(resp.GetWarnings(), "\n")
	if !strings.Contains(joined, "tasks[1].with.extra: with.extra is not an input") || !strings.Contains(joined, "tasks[1].with.count:") {
		t.Fatalf("apply warnings = %v", resp.GetWarnings())
	}
}

func TestValidateOrkflow_OutputReferenceMustBeDeclared(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	doc := `
name: deploy
version: "1.0.0"
outputs:
  out: '{{ .tasks.release.output.urll }}'
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
    with:
      image: x
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ eq .tasks.release.output.ok "true" }}'
    with: { op: exec, body: "{{ .tasks.release.output.digets }}", command: ["true"] }
`
	issues := f.validate(t, doc)
	requireIssue(t, issues, "tasks[2].with.body",
		`.tasks.release.output.digets: release-block@2.1.0 declares no output "digets" (outputs: digest, url)`, errorIssue)
	requireIssue(t, issues, "tasks[2].condition", `declares no output "ok"`, errorIssue)
	requireIssue(t, issues, "outputs.out", `declares no output "urll"`, errorIssue)

	g := newComposeFixture(t)
	g.mustApply(t, strings.Replace(composeChildYAML,
		"outputs:\n  digest: '{{ .tasks.sign.output.value }}'\n  url: '{{ .tasks.sign.output.value }}'\n", "", 1))
	issues = g.validate(t, composeParentYAML)
	requireIssue(t, issues, "tasks[2].with.command[1]",
		`release-block@2.1.0 declares no outputs, so task "release" has no values to read`, errorIssue)
}

func TestValidateOrkflow_ChildCallbackAwaitIsAWarning(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, `
name: approval-gate
version: "1.0"
tasks:
  - id: sign-off
    await: { timeout_seconds: 60, callback: true }
`)
	issues := f.validate(t, `
name: deploy
version: "1.0.0"
tasks:
  - id: approve
    uses: orkflow:approval-gate@1.0
`)
	requireIssue(t, issues, "tasks[0].uses",
		"child orkflow approval-gate@1.0 (task approve) declares callback awaits (sign-off); as a child run its callback tokens are minted and disclosed to no one: resolve them with orkctl run resolve <child-run> --task sign-off or on the child run's page",
		warningIssue)
}

func TestApplyOrkflow_CompositionRefusalWritesNothing(t *testing.T) {
	f := newComposeFixture(t)
	_, err := f.apply(t, composeParentYAML)
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), `no orkflow "release-block" exists`) {
		t.Fatalf("apply = %v", err)
	}
	if _, err := f.store.Get(context.Background(), "team-a", "deploy", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a refused apply stored something: %v", err)
	}
}

// --- submit ------------------------------------------------------------------

const notarizeYAML = `
name: notarize
version: "1.0"
tasks:
  - id: stamp
    uses: shell
    with: { op: eval, command: ["true"] }
`

// indexingStarter is fakeStarter with the TemporalStarter's boot answers.
type indexingStarter struct{ *fakeStarter }

func (indexingStarter) IndexAnswers() (bool, bool) { return true, true }

func TestSubmitRun_PlansEveryChildAndMintsIdsLast(t *testing.T) {
	starter := &fakeStarter{}
	f := newComposeFixture(t, func(cfg *api.Config) { cfg.Starter = indexingStarter{starter} })
	f.starter = starter
	f.mustApply(t, notarizeYAML)
	f.mustApply(t, strings.Replace(composeChildYAML, "tasks:\n", "tasks:\n  - id: notarize\n    uses: orkflow:notarize@1.0\n", 1))
	f.mustApply(t, composeParentYAML)
	resp, err := f.submit("deploy")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	// Child ids are minted after every refusal, depth first; the parent's
	// own id after them.
	if resp.GetRun().GetId() != "run-003" {
		t.Fatalf("parent id = %q, want run-003", resp.GetRun().GetId())
	}
	plans := f.plans()
	release := plans[0].Children["release"]
	if release == nil || release.RunID != "run-001" {
		t.Fatalf("release child = %+v", release)
	}
	notarize := release.Plan.Children["notarize"]
	if notarize == nil || notarize.RunID != "run-002" || len(notarize.Plan.Children) != 0 {
		t.Fatalf("notarize child = %+v", notarize)
	}
	for _, cp := range []*engine.ChildRunPlan{release, notarize} {
		if cp.Plan.Origin != nil {
			t.Fatalf("a child plan carries an origin: %+v", cp.Plan.Origin)
		}
		if !cp.NamespaceIndexed || !cp.OriginIndexed {
			t.Fatalf("a child plan lacks the starter's index answers: %+v", cp)
		}
		if cp.Plan.CommitRoot == "" || cp.Plan.Routes["main"] == "" {
			t.Fatalf("a child plan is not planned: %+v", cp.Plan)
		}
		def, err := cp.Definition()
		if err != nil || def.GetSourceYaml() != "" || def.GetDefinitionDigest() == "" {
			t.Fatalf("child definition = %v, %v", def, err)
		}
		if fmt.Sprint(cp.Plan.ImagePolicy) != fmt.Sprint(plans[0].ImagePolicy) {
			t.Fatalf("a child has its own image policy selection")
		}
	}
}

// TestPrepare_SetsRunTreeLimitsOnTheTopLevelPlanOnly: the history limit is
// pinned once per run tree — on the top-level plan, even when a transfer
// budget is configured (pinDurabilityPlan's early return cannot skip it), at
// the deployment's values — and on no nested child plan: the child's start
// copies it (startChild).
func TestPrepare_SetsRunTreeLimitsOnTheTopLevelPlanOnly(t *testing.T) {
	starter := &fakeStarter{}
	f := newComposeFixture(t, func(cfg *api.Config) {
		cfg.Starter = indexingStarter{starter}
		cfg.TransferBudgetSeconds = 900
		cfg.HistoryMaxEvents = 40_000
		cfg.HistoryMaxBytes = 30 << 20
	})
	f.starter = starter
	f.mustApply(t, notarizeYAML)
	f.mustApply(t, strings.Replace(composeChildYAML, "tasks:\n", "tasks:\n  - id: notarize\n    uses: orkflow:notarize@1.0\n", 1))
	f.mustApply(t, composeParentYAML)
	if _, err := f.submit("deploy"); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	plan := f.plans()[0]
	want := engine.HistoryLimit{MaxEvents: 40_000, MaxBytes: 30 << 20, Reserve: engine.DefaultHistoryReserve}
	if plan.TransferBudgetSeconds != 900 || plan.History == nil || *plan.History != want {
		t.Fatalf("top-level plan: transfer %d, History %+v; want 900 and %+v", plan.TransferBudgetSeconds, plan.History, want)
	}
	release := plan.Children["release"]
	if release.Plan.History != nil || release.Plan.Children["notarize"].Plan.History != nil {
		t.Fatal("a nested child plan carries the run-tree limits")
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(encoded), `"history"`); n != 1 {
		t.Fatalf("the encoded plan carries %d history entries, want exactly one for the tree", n)
	}
}

func TestSubmitRun_ChildVersionDeletedIsFailedPrecondition(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	f.mustApply(t, composeParentYAML)
	if err := f.store.Delete(context.Background(), "team-a", "release-block"); err != nil {
		t.Fatal(err)
	}
	_, err := f.submit("deploy")
	requireRefusal(t, err, connect.CodeFailedPrecondition,
		`child orkflow release-block@2.1.0 of task "release" no longer exists in namespace "team-a"; apply it again, or submit a version of deploy that composes an existing one`)
	assertComposeNothingBehind(t, f)
}

func assertComposeNothingBehind(t *testing.T, f *composeFixture) {
	t.Helper()
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submit started %v", started)
	}
	if recorded := f.runs.List(engine.ListFilter{}); len(recorded) != 0 {
		t.Fatalf("a refused submit recorded %d runs", len(recorded))
	}
	// No id was minted: the next one is still the first.
	if id := f.svc.NewRunIDForTest(); id != "run-001" {
		t.Fatalf("a refused submit minted ids: next is %s", id)
	}
}

func TestSubmitRun_ChildDigestMismatchIsFailedPrecondition(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	f.mustApply(t, composeParentYAML)
	if err := f.store.Delete(context.Background(), "team-a", "release-block"); err != nil {
		t.Fatal(err)
	}
	f.mustApply(t, strings.Replace(composeChildYAML, `command: ["true"]`, `command: ["false"]`, 1))
	_, err := f.submit("deploy")
	requireRefusal(t, err, connect.CodeFailedPrecondition, "deploy@1.0.0 pins release-block@2.1.0 as sha256:",
		"it was deleted and re-applied with other content")
	assertComposeNothingBehind(t, f)
}

func TestSubmitRun_RefusesATreeDeeperThanTheCap(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, notarizeYAML)
	f.mustApply(t, strings.Replace(composeChildYAML, "tasks:\n", "tasks:\n  - id: notarize\n    uses: orkflow:notarize@1.0\n", 1))
	f.mustApply(t, composeParentYAML)
	lowered := api.NewService(api.Config{
		Orkflows: f.store, Namespaces: f.reg, Starter: f.starter, ComposeMaxDepth: 1,
		NewRunID: func() string { return "never" },
	})
	_, err := lowered.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", Namespace: "team-a"}))
	requireRefusal(t, err, connect.CodeFailedPrecondition,
		"composition is 2 levels deep (deploy → release-block → notarize); this deployment now allows 1 (ORK_COMPOSE_MAX_DEPTH was lowered after deploy@1.0.0 was applied)")
	if len(f.starter.startedRuns()) != 0 {
		t.Fatal("a refused submit started a run")
	}
}

// gpuEmpty is a pool probe whose gpu pool has no Orkers.
type gpuEmpty struct{}

func (gpuEmpty) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	if strings.Contains(queue, "gpu") {
		return engine.PoolLiveness{}, nil
	}
	return engine.PoolLiveness{Orkers: 1, LastSeen: time.Now()}, nil
}

func TestSubmitRun_ChildPoolRefusalNamesTheChild(t *testing.T) {
	f := newComposeFixture(t, func(cfg *api.Config) { cfg.Pools = gpuEmpty{} })
	f.mustApply(t, strings.Replace(composeChildYAML, "  - id: sign\n    uses: shell\n", "  - id: sign\n    uses: shell\n    pool: gpu\n", 1))
	f.mustApply(t, composeParentYAML)
	_, err := f.submit("deploy")
	requireRefusal(t, err, connect.CodeFailedPrecondition, `child orkflow release-block@2.1.0 (task "release"): `, "gpu")
	assertComposeNothingBehind(t, f)

	// allow_empty_pool applies to the whole tree.
	resp, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", Namespace: "team-a", AllowEmptyPool: true}))
	if err != nil {
		t.Fatalf("allow_empty_pool did not reach the child: %v", err)
	}
	if resp.Msg.GetRun().GetId() == "" {
		t.Fatal("no run")
	}

	// A pool the namespace no longer allows: the caller's document to fix.
	ns, _ := f.reg.Get(context.Background(), "team-a")
	next := proto.Clone(ns).(*orkstackv1.Namespace)
	next.AllowedPools = []string{"main"}
	if _, err := f.reg.Put(context.Background(), next, artifacts.PutConditions{}); err != nil {
		t.Fatal(err)
	}
	_, err = f.submit("deploy")
	requireRefusal(t, err, connect.CodeInvalidArgument, `child orkflow release-block@2.1.0 (task "release"): `)
}

func TestSubmitRun_ChildImagePolicyRefusalNamesTheChild(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	if err := f.apply(strings.Replace(orkflowUsing("release-block", "docker.io/library/alpine:3.20"),
		"version: 0.1.0", "version: \"2.1.0\"", 1), ""); err != nil {
		t.Fatalf("apply child: %v", err)
	}
	if err := f.apply(`
name: deploy
version: "1.0.0"
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
`, ""); err != nil {
		t.Fatalf("apply parent: %v", err)
	}
	f.floor(t, &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}})
	_, err := f.submit("deploy", "", "", false)
	requirePolicyCode(t, err, connect.CodeFailedPrecondition, `child orkflow release-block@2.1.0 (task "release"): `, alpineRefusal)
	if len(f.starter.startedRuns()) != 0 {
		t.Fatal("a refused submit started a run")
	}
}

func TestSubmitRun_ChildEachCapIsRechecked(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, `
name: release-block
version: "2.1.0"
inputs:
  parts: { type: array }
tasks:
  - id: sign
    uses: shell
    each: { over: "{{ .inputs.parts }}", max_items: 10 }
    with: { op: eval, command: ["true"] }
`)
	f.mustApply(t, composeParentYAMLWithout(t))
	lowered := api.NewService(api.Config{
		Orkflows: f.store, Namespaces: f.reg, Starter: f.starter, EachMaxItems: 5,
		NewRunID: func() string { return "never" },
	})
	_, err := lowered.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", Namespace: "team-a"}))
	requireRefusal(t, err, connect.CodeFailedPrecondition, `child orkflow release-block@2.1.0 (task "release"): `,
		"ORK_EACH_MAX_ITEMS was lowered")
}

func TestSubmitRun_PlanOverBudgetIsFailedPreconditionAndMintsNothing(t *testing.T) {
	f := newComposeFixture(t)
	// Three children of ~100 KiB each: each fits alone, the plan does not.
	big, _ := structpb.NewStruct(map[string]any{"op": "eval", "blob": strings.Repeat("x", 100*1024)})
	task := shellTask("sign")
	task.With = big
	f.putStored(t, "release-block", "2.1.0", "sha256:rb", task)
	f.putStored(t, "deploy", "1.0.0", "sha256:dp",
		composedTask("a", "release-block", "2.1.0", "sha256:rb"),
		composedTask("b", "release-block", "2.1.0", "sha256:rb"),
		composedTask("c", "release-block", "2.1.0", "sha256:rb"))
	_, err := f.submit("deploy")
	requireRefusal(t, err, connect.CodeFailedPrecondition,
		"the run's plan — this definition and the 3 child runs it composes — encodes to",
		"over the 256 KiB a run may carry; compose fewer or smaller Orkflows")
	assertComposeNothingBehind(t, f)
}

func TestSubmitRun_ChildCallbackTokensAreDisclosedToNoOne(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, `
name: approval-gate
version: "1.0"
tasks:
  - id: sign-off
    await: { timeout_seconds: 60, callback: true }
`)
	f.mustApply(t, `
name: deploy
version: "1.0.0"
tasks:
  - id: approve
    uses: orkflow:approval-gate@1.0
`)
	resp, err := f.submit("deploy")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	if len(resp.GetAwaitCallbacks()) != 0 {
		t.Fatalf("the child's callback tokens were disclosed: %v", resp.GetAwaitCallbacks())
	}
	cred := f.plans()[0].Children["approve"].Plan.Awaits["sign-off"]
	if cred.Salt == "" || cred.Digest == "" {
		t.Fatalf("the child plan carries no credential digest: %+v", cred)
	}
}

func TestSubmitRun_KeyedRepeatDiscardsTheCandidatesChildIds(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	f.mustApply(t, composeParentYAML)
	submit := func() *orkstackv1.Run {
		resp, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "deploy", Namespace: "team-a", IdempotencyKey: "once"}))
		if err != nil {
			t.Fatalf("SubmitRun: %v", err)
		}
		return resp.Msg.GetRun()
	}
	first, second := submit(), submit()
	if first.GetId() != second.GetId() {
		t.Fatalf("a keyed repeat started %s after %s", second.GetId(), first.GetId())
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Fatalf("started %v, want the one run", started)
	}
	if child := f.plans()[0].Children["release"].RunID; child == first.GetId() {
		t.Fatalf("the child shares its parent's id %s", child)
	}
}

func TestTriggerInvoke_ChildRefusalIs412(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, composeChildYAML)
	f.mustApply(t, composeParentYAML)
	if err := f.store.Delete(context.Background(), "team-a", "release-block"); err != nil {
		t.Fatal(err)
	}
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "deploy", Namespace: "team-a"})
	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412: %s", status, body)
	}
	if !strings.Contains(string(body), "no longer exists") {
		t.Fatalf("body = %s", body)
	}
	if len(f.starter.startedRuns()) != 0 {
		t.Fatal("a refused invoke started a run")
	}
}

// --- reads -------------------------------------------------------------------

// composedRun is a finished parent whose `release` task ran child run
// ork-child-1 and whose values were stored.
func composedRun(f *evidenceFixture, runID string) *orkstackv1.Run {
	at := evidenceNow.Add(-time.Hour)
	values := f.put("values.json", []byte(`{"digest":"sha256:abc"}`))
	return f.finish(&orkstackv1.Run{Id: runID, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, OrkerId: "orker-1"},
		{
			TaskId: "release", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: values,
			ScheduledAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
			Child: &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0",
				OrkflowDefinitionDigest: "sha256:rb", RunId: "ork-child-1"},
		},
	}})
}

func TestStreamTaskLogs_ComposedTaskIsFailedPrecondition(t *testing.T) {
	logs := &memoryLogStore{}
	f := newEvidenceFixture(t, withConfig(func(cfg *api.Config) { cfg.Logs = logs }))
	run := composedRun(f, "ork-parent-logs")
	stream, err := f.client.StreamTaskLogs(context.Background(), connect.NewRequest(&orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "release"}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	want := `task "release" of run "ork-parent-logs" runs orkflow release-block@2.1.0 as child run ork-child-1 and writes no logs of its own; read the child's: orkctl run logs ork-child-1`
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), want) {
		t.Fatalf("StreamTaskLogs = %v", err)
	}

	// The socket route serves the same refusal.
	err = f.svc.StreamRunTaskLogs(context.Background(), api.TaskLogQuery{RunID: run.GetId(), TaskID: "release"},
		func(*orkstackv1.TaskLogChunk) error { return nil })
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), want) {
		t.Fatalf("the socket's StreamRunTaskLogs = %v", err)
	}
}

func TestListTaskAttempts_ComposedTaskIsEmpty(t *testing.T) {
	f := newEvidenceFixture(t)
	run := composedRun(f, "ork-parent-attempts")
	resp, err := f.client.ListTaskAttempts(context.Background(), connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{
		RunId: run.GetId(), TaskId: "release"}))
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	if len(resp.Msg.GetAttempts()) != 0 {
		t.Fatalf("a composed task has attempts: %v", resp.Msg.GetAttempts())
	}
}

func TestListRunArtifacts_ComposedTaskListsItsValues(t *testing.T) {
	f := newEvidenceFixture(t)
	run := composedRun(f, "ork-parent-artifacts")
	resp, err := f.client.ListRunArtifacts(context.Background(), connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{
		RunId: run.GetId(), TaskId: "release"}))
	if err != nil {
		t.Fatalf("ListRunArtifacts: %v", err)
	}
	found := false
	for _, a := range resp.Msg.GetArtifacts() {
		if a.GetKind() == orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES && a.GetTaskId() == "release" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the composed task's values are not listed: %v", resp.Msg.GetArtifacts())
	}
}

func TestDiffRuns_ComposedRowComparesStatusErrorDuration(t *testing.T) {
	child := func(runID string) func(*orkstackv1.TaskRun) {
		return func(task *orkstackv1.TaskRun) {
			task.Attempt, task.OrkerId, task.Pool, task.ExitCode = 0, "", "", 0
			task.Child = &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0", RunId: runID}
		}
	}
	from := diffRun("ork-mon", nil, diffTask("extract"), diffTask("release", child("ork-child-a")))
	to := diffRun("ork-tue", nil, diffTask("extract"), diffTask("release", child("ork-child-b"), func(task *orkstackv1.TaskRun) {
		task.FinishedAt = timestamppb.New(task.GetFinishedAt().AsTime().Add(time.Minute))
	}))
	f := fullyRecordedFixture(t, from, to)
	res := f.mustDiff(t, from.GetId(), to.GetId())
	var fields []string
	for _, row := range res.GetDifferences() {
		if row.GetTaskId() == "release" {
			fields = append(fields, row.GetField())
		}
	}
	if fmt.Sprint(fields) != "[duration]" {
		t.Fatalf("composed row compared %v, want only [duration]", fields)
	}
	if res.GetEvidenceUnavailable() != 0 {
		t.Fatalf("a composed row reported %d unavailable fields", res.GetEvidenceUnavailable())
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// automationFixture is the CLI fixture with a schedule controller, a pinned
// webhook registry, namespace payments, and orkflows deploy and reconcile
// stored in it.
type automationFixture struct {
	*fixture
	schedules *cliSchedules
	webhooks  *webhooks.Registry
	dir       string
}

func newAutomationFixture(t *testing.T) *automationFixture {
	t.Helper()
	schedules := &cliSchedules{records: map[string]*orkstackv1.ScheduleRecord{}}
	next := 0
	registry := webhooks.NewRegistry(
		webhooks.WithIDGenerator(func() (string, error) {
			next++
			return fmt.Sprintf("wh-%016x", next), nil
		}),
		webhooks.WithSecretGenerator(func() (string, error) { return "generated-webhook-secret", nil }),
		webhooks.WithClock(func() time.Time { return time.Unix(1755302400, 0).UTC() }),
	)
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Schedules = schedules
		cfg.Webhooks = registry
	})
	a := &automationFixture{fixture: f, schedules: schedules, webhooks: registry, dir: t.TempDir()}
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	for _, name := range []string{"deploy", "reconcile"} {
		path := a.write(t, "seed-"+name+".ork.yaml", orkflowSource(t, name, "0.2.0"))
		f.run("apply", path, "-n", "payments").requireExit(t, exitOK)
	}
	return a
}

// orkflowSource is the example orkflow under another name and version.
func orkflowSource(t *testing.T, name, version string) string {
	t.Helper()
	raw, err := os.ReadFile(examplePath(t))
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	source := strings.Replace(string(raw), "name: hello-ork", "name: "+name, 1)
	return strings.Replace(source, `version: "0.2.0"`, `version: "`+version+`"`, 1)
}

// write puts a file in the fixture's directory and returns its path.
func (a *automationFixture) write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(a.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// cliAutomationDoc is the document most tests below apply.
const cliAutomationDoc = `apiVersion: orkstack/v1
kind: Automation
namespace: payments

settings:
  description: card rails
  allowed_pools: [main]

schedules:
  nightly-reconcile:
    orkflow: reconcile
    cron: "0 2 * * *"
    timezone: Europe/Lisbon
    input: '{"mode": "full"}'

triggers:
  github-deploy:
    orkflow: deploy
    description: GitHub Actions deploy hook

webhooks:
  etl-loader:
    url: https://hooks.acme.dev/etl
    orkflow: nightly-etl
    events: [TASK_SUCCEEDED, RUN_FAILED]
`

// writeMethods are the RPC method prefixes that change something.
var writeMethods = []string{"Create", "Update", "Delete", "Register", "Apply", "Pause", "Resume", "Fire", "Backfill", "SetCurrent"}

// writes returns the write RPCs the fixture's server received since mark.
func (a *automationFixture) writes(mark int) []string {
	var out []string
	for _, path := range a.requests()[mark:] {
		method := path[strings.LastIndex(path, "/")+1:]
		for _, prefix := range writeMethods {
			if strings.HasPrefix(method, prefix) {
				out = append(out, strings.TrimPrefix(path, "/orkstack.v1."))
				break
			}
		}
	}
	return out
}

var (
	normalizeIDs     = regexp.MustCompile(`\b(trg|sch|wh)-[0-9a-f]{16}\b`)
	normalizeSecrets = regexp.MustCompile(`\b[0-9a-f]{64}\b`)
	normalizeDigest  = regexp.MustCompile(`sha256:[0-9a-f]{64}`)
	normalizeServer  = regexp.MustCompile(`http://127\.0\.0\.1:[0-9]+`)
	normalizeTimes   = regexp.MustCompile(`\b\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\b`)
)

// normalize replaces what differs between runs — generated ids, tokens, the
// digest that covers them, the fixture's port — so a golden file can hold
// the rest byte for byte.
func normalize(text string) string {
	text = normalizeDigest.ReplaceAllString(text, "sha256:<digest>")
	text = normalizeSecrets.ReplaceAllString(text, "<secret>")
	text = normalizeIDs.ReplaceAllString(text, "$1-<id>")
	text = normalizeTimes.ReplaceAllString(text, "<time>")
	return normalizeServer.ReplaceAllString(text, "http://localhost:8080")
}

// normalize is the package normalize plus the fixture's directory, written
// as the environments/prod/ the docs use.
func (a *automationFixture) normalize(text string) string {
	return normalize(strings.ReplaceAll(text, a.dir+string(filepath.Separator), "environments/prod/"))
}

func TestApply_SingleOrkflowFileIsUnchanged(t *testing.T) {
	f := newFixture(t)
	requireGolden(t, "apply.golden", f.applyExample().requireExit(t, exitOK).stdout)
	out := f.run("--json", "apply", examplePath(t)).requireExit(t, exitOK).stdout
	var resp orkstackv1.ApplyOrkflowResponse
	if err := api.UnmarshalJSON([]byte(out), &resp); err != nil || resp.GetOrkflow().GetName() != "hello-ork" {
		t.Fatalf("single-file --json is not ApplyOrkflowResponse: %v\n%s", err, out)
	}
	if strings.Contains(out, "dryRun") || strings.Contains(out, "automation") {
		t.Fatalf("single-file --json printed an ApplyReport:\n%s", out)
	}
}

// TestApply_ClassifiesDocumentsByKind: kind: decides what a file is, and a
// file with no kind, or another, is refused before anything is sent.
func TestApply_ClassifiesDocumentsByKind(t *testing.T) {
	cases := []struct {
		source  string
		want    documentKind
		wantErr string
	}{
		{"apiVersion: orkstack/v1\nkind: Orkflow\nname: hello\nversion: '1'\ntasks: []\n", orkflowDocument, ""},
		{"apiVersion: orkstack/v2\nkind: Orkflow\nname: hello\n", orkflowDocument, ""},
		{"apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\n", automationDocument, ""},
		{"apiVersion: orkstack/v2\nkind: Automation\nnamespace: payments\n", automationDocument, ""},
		{"name: [unclosed\n", orkflowDocument, ""},
		{"name: hello\ntasks: []\n", orkflowDocument,
			`x.ork.yaml: no apiVersion or kind: an OrkStack document begins with "apiVersion: orkstack/v1" and its kind — "kind: Orkflow" for an Orkflow, "kind: Automation" for an automation document; add the two lines at the top, after any leading comments`},
		{"apiVersion: orkstack/v1\nnamespace: payments\n", orkflowDocument,
			`x.ork.yaml: kind is required: add "kind: Orkflow" (or "kind: Automation") below "apiVersion: orkstack/v1"`},
		{"apiVersion: orkstack/v1\nkind: Workflow\n", orkflowDocument,
			`x.ork.yaml: unknown document kind "Workflow": want kind: Orkflow or kind: Automation`},
		{"kind: orkflow\nname: hello\n", orkflowDocument,
			`x.ork.yaml: unknown document kind "orkflow": want kind: Orkflow or kind: Automation`},
	}
	for _, tc := range cases {
		in, err := classifyDocument("x.ork.yaml", []byte(tc.source))
		switch {
		case tc.wantErr != "":
			if err == nil || err.Error() != tc.wantErr {
				t.Errorf("%q: err = %v, want %q", tc.source, err, tc.wantErr)
			}
		case err != nil:
			t.Errorf("%q: %v", tc.source, err)
		case in.kind != tc.want:
			t.Errorf("%q: kind = %v, want %v", tc.source, in.kind, tc.want)
		}
	}
}

// TestApply_RefusesAFileWithNoKindBeforeAnyRequest: the single-file shortcut
// classifies first, so a header-less file is refused with no RPC.
func TestApply_RefusesAFileWithNoKindBeforeAnyRequest(t *testing.T) {
	f := newFixture(t)
	raw, err := os.ReadFile(examplePath(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "hello.ork.yaml")
	if err := os.WriteFile(path, dsl.VersionedSource(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	mark := len(f.requests())
	out := f.run("apply", path).requireExit(t, exitError)
	if !strings.Contains(out.stderr, path+": no apiVersion or kind: an OrkStack document begins with") {
		t.Fatalf("stderr does not name the file and the header:\n%s", out.stderr)
	}
	if got := f.requests()[mark:]; len(got) != 0 {
		t.Fatalf("a header-less file sent %v", got)
	}
}

// TestApply_AMixedDirectoryWithAHeaderlessFileWritesNothing: one header-less
// file among an Orkflow and an automation document refuses the whole
// directory, names the file, and sends no write.
func TestApply_AMixedDirectoryWithAHeaderlessFileWritesNothing(t *testing.T) {
	a := newAutomationFixture(t)
	dir := filepath.Join(a.dir, "mixed")
	a.write(t, "mixed/deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0"))
	a.write(t, "mixed/automation.ork.yaml", cliAutomationDoc)
	bad := a.write(t, "mixed/legacy.ork.yaml", string(dsl.VersionedSource([]byte(orkflowSource(t, "legacy", "1.0.0")))))
	mark := len(a.requests())
	out := a.run("apply", dir, "-n", "payments").requireExit(t, exitError)
	if !strings.Contains(out.stderr, bad+": no apiVersion or kind") {
		t.Fatalf("stderr does not name the header-less file:\n%s", out.stderr)
	}
	if writes := a.writes(mark); len(writes) != 0 {
		t.Fatalf("a refused directory wrote: %v", writes)
	}
}

// TestApply_AMixedDirectoryAppliesOrkflowsThenAutomation: the same directory
// without the bad file applies its Orkflow first, then the plan's steps.
func TestApply_AMixedDirectoryAppliesOrkflowsThenAutomation(t *testing.T) {
	a := newAutomationFixture(t)
	dir := filepath.Join(a.dir, "mixed")
	a.write(t, "mixed/deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0"))
	a.write(t, "mixed/automation.ork.yaml", strings.Replace(cliAutomationDoc, "    orkflow: deploy\n", "    orkflow: deploy\n    version: 1.4.0\n", 1))
	mark := len(a.requests())
	a.run("apply", dir, "-n", "payments").requireExit(t, exitOK)
	want := []string{
		"OrkflowService/ApplyOrkflow",
		"NamespaceService/UpdateNamespace",
		"ScheduleService/CreateSchedule",
		"TriggerService/CreateTrigger",
		"WebhookService/RegisterWebhook",
	}
	if got := a.writes(mark); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("writes = %v, want %v", got, want)
	}
}

// TestApply_DryRunComparesTheVersionedSource: --dry-run's identical check is
// the store's own source rule, so moving the header among the leading lines
// is identical, and any other header form is a conflict whose refusal says
// where the two lines go.
func TestApply_DryRunComparesTheVersionedSource(t *testing.T) {
	a := newAutomationFixture(t)
	legacy := string(dsl.VersionedSource([]byte(orkflowSource(t, "pinned", "3.0.0"))))
	below, ok := dsl.AddHeader(legacy)
	if !ok {
		t.Fatal("AddHeader refused the example")
	}
	a.run("apply", a.write(t, "below.ork.yaml", below), "-n", "payments").requireExit(t, exitOK)

	onLine1 := a.write(t, "line1.ork.yaml", dsl.OrkflowHeader+legacy)
	out := a.run("apply", onLine1, "-n", "payments", "--dry-run").requireExit(t, exitOK)
	out.requireContains(t, "Dry run: nothing was written.")
	if strings.Contains(out.stdout, "stored with different source") {
		t.Fatalf("the header on line 1 was not identical:\n%s", out.stdout)
	}
	mark := len(a.requests())
	a.run("apply", onLine1, "-n", "payments", "--check").requireExit(t, exitOK)
	a.run("apply", onLine1, "-n", "payments", "--prune").requireExit(t, exitOK)
	if writes := a.writes(mark); len(writes) != 0 {
		t.Fatalf("an identical document wrote: %v", writes)
	}

	afterName := a.write(t, "after.ork.yaml", strings.Replace(legacy, "\nversion:", "\n"+strings.TrimSuffix(dsl.OrkflowHeader, "\n")+"\nversion:", 1))
	refused := a.run("apply", afterName, "-n", "payments", "--dry-run").requireExit(t, exitError)
	refused.requireContains(t, "stored with different source",
		`ApplyOrkflow would refuse it — if only the header changed, write "apiVersion: orkstack/v1" and "kind: Orkflow" as the file's first two lines (after any leading comments); otherwise bump version: or drop it`)

	edited := a.write(t, "edited.ork.yaml", strings.Replace(below, "description:", "description: changed —", 1))
	today := a.run("apply", edited, "-n", "payments", "--dry-run").requireExit(t, exitError)
	today.requireContains(t, "ApplyOrkflow would refuse it — bump version: or drop it")
}

func TestApply_DirectoryExpandsOrkYamlLexicallyAndNonRecursively(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"b.ork.yaml", "a.ork.yml", "c.yaml", "notes.txt", "sub/d.ork.yaml"} {
		path := filepath.Join(dir, name)
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	inputs, err := expandApplyArgs([]string{dir})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	var got []string
	for _, in := range inputs {
		got = append(got, filepath.Base(in.path))
	}
	if strings.Join(got, ",") != "a.ork.yml,b.ork.yaml" {
		t.Errorf("expanded = %v, want [a.ork.yml b.ork.yaml]", got)
	}
	empty := t.TempDir()
	if _, err := expandApplyArgs([]string{empty}); err == nil || !strings.Contains(err.Error(), "no *.ork.yaml or *.ork.yml files") {
		t.Errorf("empty directory = %v", err)
	}
}

func TestApply_DryRunWritesNothing(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	newOrkflow := a.write(t, "deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0"))
	mark := len(a.requests())
	a.run("apply", doc, newOrkflow, "-n", "payments", "--dry-run", "--prune").requireExit(t, exitOK).
		requireContains(t, "Dry run: nothing was written.")
	if writes := a.writes(mark); len(writes) != 0 {
		t.Fatalf("a dry run wrote: %v", writes)
	}
}

func TestApply_RefusesBeforeAnyWrite(t *testing.T) {
	cases := map[string]func(t *testing.T, a *automationFixture) []string{
		"conflicting orkflow": func(t *testing.T, a *automationFixture) []string {
			changed := strings.Replace(orkflowSource(t, "deploy", "0.2.0"), "description:", "description: changed —", 1)
			return []string{a.write(t, "deploy.ork.yaml", changed), a.write(t, "automation.ork.yaml", cliAutomationDoc), "-n", "payments"}
		},
		"plan error": func(t *testing.T, a *automationFixture) []string {
			return []string{a.write(t, "automation.ork.yaml", strings.Replace(cliAutomationDoc, "orkflow: deploy", "orkflow: missing", 1))}
		},
		"missing secret_env": func(t *testing.T, a *automationFixture) []string {
			t.Setenv("ETL_WEBHOOK_SECRET", "")
			doc := strings.Replace(cliAutomationDoc, "events: [TASK_SUCCEEDED, RUN_FAILED]\n", "events: [TASK_SUCCEEDED, RUN_FAILED]\n    secret_env: ETL_WEBHOOK_SECRET\n", 1)
			return []string{a.write(t, "automation.ork.yaml", doc)}
		},
		"existing secrets-out": func(t *testing.T, a *automationFixture) []string {
			existing := a.write(t, "secrets.json", "{}")
			return []string{a.write(t, "automation.ork.yaml", cliAutomationDoc), "--secrets-out", existing}
		},
		"uncreatable secrets-out": func(t *testing.T, a *automationFixture) []string {
			missing := filepath.Join(a.dir, "no-such-dir", "secrets.json")
			return []string{a.write(t, "automation.ork.yaml", cliAutomationDoc), "--secrets-out", missing}
		},
		"secrets-out under a file": func(t *testing.T, a *automationFixture) []string {
			parent := a.write(t, "not-a-dir", "x")
			return []string{a.write(t, "automation.ork.yaml", cliAutomationDoc), "--secrets-out", filepath.Join(parent, "secrets.json")}
		},
		"plan error leaves no secrets-out file": func(t *testing.T, a *automationFixture) []string {
			t.Cleanup(func() {
				if _, err := os.Lstat(filepath.Join(a.dir, "secrets.json")); err == nil {
					t.Error("a refused apply left its --secrets-out file behind")
				}
			})
			doc := strings.Replace(cliAutomationDoc, "orkflow: deploy", "orkflow: missing", 1)
			return []string{a.write(t, "automation.ork.yaml", doc), "--secrets-out", filepath.Join(a.dir, "secrets.json")}
		},
		"expect-plan with two documents": func(t *testing.T, a *automationFixture) []string {
			a.run("namespace", "create", "billing").requireExit(t, exitOK)
			other := strings.Replace(header("billing"), "", "", 1) + "triggers: {}\n"
			return []string{a.write(t, "a.ork.yaml", cliAutomationDoc), a.write(t, "b.ork.yaml", other), "--expect-plan", "sha256:" + strings.Repeat("0", 64)}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			a := newAutomationFixture(t)
			args := build(t, a)
			mark := len(a.requests())
			outcome := a.run(append([]string{"apply"}, args...)...)
			if outcome.code != exitError {
				t.Fatalf("exit = %d, want 1\n%s\n%s", outcome.code, outcome.stdout, outcome.stderr)
			}
			if !strings.Contains(outcome.stdout, "Nothing was written.") {
				t.Errorf("stdout does not say nothing was written:\n%s", outcome.stdout)
			}
			if writes := a.writes(mark); len(writes) != 0 {
				t.Fatalf("a refused apply wrote: %v", writes)
			}
		})
	}
}

func header(ns string) string {
	return "apiVersion: orkstack/v1\nkind: Automation\nnamespace: " + ns + "\n"
}

func TestApply_OrkflowsFirstThenStepsInPlanOrder(t *testing.T) {
	a := newAutomationFixture(t)
	newOrkflow := a.write(t, "deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0"))
	doc := a.write(t, "automation.ork.yaml", strings.Replace(cliAutomationDoc, "    orkflow: deploy\n", "    orkflow: deploy\n    version: 1.4.0\n", 1))
	mark := len(a.requests())
	a.run("apply", doc, newOrkflow, "-n", "payments").requireExit(t, exitOK)
	want := []string{
		"OrkflowService/ApplyOrkflow",
		"NamespaceService/UpdateNamespace",
		"ScheduleService/CreateSchedule",
		"TriggerService/CreateTrigger",
		"WebhookService/RegisterWebhook",
	}
	if got := a.writes(mark); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("writes = %v, want %v", got, want)
	}
}

func TestApply_PendingOrkflowsArePassedToThePlan(t *testing.T) {
	a := newAutomationFixture(t)
	newOrkflow := a.write(t, "rollback.ork.yaml", orkflowSource(t, "rollback", "1.0.0"))
	doc := a.write(t, "automation.ork.yaml", header("payments")+"triggers:\n  undo:\n    orkflow: rollback\n    version: 1.0.0\n")
	out := a.run("apply", a.dir, "-n", "payments", "--dry-run").requireExit(t, exitOK).stdout
	if strings.Contains(out, "ERROR") {
		t.Fatalf("a pending orkflow did not satisfy the reference:\n%s", out)
	}
	var plan orkstackv1.PlanAutomationRequest
	if !a.lastRequest("/orkstack.v1.AutomationService/PlanAutomation", &plan) {
		t.Fatal("no plan request was sent")
	}
	found := false
	for _, pending := range plan.GetPendingOrkflows() {
		if pending.GetName() == "rollback" && pending.GetVersion() == "1.0.0" && pending.GetNamespace() == "payments" {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending_orkflows = %v", plan.GetPendingOrkflows())
	}
	_ = newOrkflow
	_ = doc
}

func TestApply_StopsAtTheFirstFailedStep(t *testing.T) {
	a := newAutomationFixture(t)
	a.schedules.refuseCron = "61 * * * *"
	doc := a.write(t, "automation.ork.yaml", header("payments")+`schedules:
  a-first:
    orkflow: reconcile
    cron: "0 1 * * *"
  b-broken:
    orkflow: reconcile
    cron: "61 * * * *"
triggers:
  github-deploy:
    orkflow: deploy
`)
	outcome := a.run("--json", "apply", doc)
	if outcome.code != exitError {
		t.Fatalf("exit = %d, want 1\n%s%s", outcome.code, outcome.stdout, outcome.stderr)
	}
	var report orkstackv1.ApplyReport
	if err := api.UnmarshalJSON([]byte(outcome.stdout), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, outcome.stdout)
	}
	var outcomes []string
	for _, result := range report.GetAutomation()[0].GetResults() {
		outcomes = append(outcomes, strings.TrimPrefix(result.GetOutcome().String(), "AUTOMATION_STEP_OUTCOME_"))
	}
	if strings.Join(outcomes, ",") != "APPLIED,FAILED,NOT_RUN" {
		t.Fatalf("outcomes = %v", outcomes)
	}
	if failed := report.GetAutomation()[0].GetResults()[1].GetError(); !strings.Contains(failed, "invalid_argument") || !strings.Contains(failed, "61 * * * *") {
		t.Errorf("failed step error = %q", failed)
	}
	requireGolden(t, "apply-result-failed.golden", a.normalize(a.run("apply", doc).stdout))
}

func TestApply_DeleteNotFoundIsAlreadyGone(t *testing.T) {
	a := newAutomationFixture(t)
	root, g := newRootCommand()
	_ = root
	g.server = a.baseURL
	defer g.closeIdleConnections()
	run := &applyRun{g: g, cmd: root, report: &orkstackv1.ApplyReport{Automation: []*orkstackv1.AutomationDocumentReport{{
		File: "automation.ork.yaml",
		Plan: &orkstackv1.PlanAutomationResponse{Namespace: "payments", Steps: []*orkstackv1.AutomationStep{{
			Action: orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE, Kind: orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER,
			Name: "gone", Id: "trg-0000000000000001",
			Request: &orkstackv1.AutomationStep_DeleteTrigger{DeleteTrigger: &orkstackv1.DeleteTriggerRequest{Id: "trg-0000000000000001", Namespace: "payments"}},
		}}},
	}}}}
	root.SetContext(context.Background())
	_, failed := run.executeSteps()
	if failed {
		t.Fatal("a DELETE of an absent object failed the apply")
	}
	if got := run.report.GetAutomation()[0].GetResults()[0].GetOutcome(); got != orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_ALREADY_GONE {
		t.Fatalf("outcome = %v, want ALREADY_GONE", got)
	}
}

func TestApply_ExpectPlanMismatchRefuses(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	var report orkstackv1.ApplyReport
	out := a.run("--json", "apply", doc, "--dry-run").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(out), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	digest := report.GetAutomation()[0].GetPlan().GetPlanDigest()
	// Drift between review and apply: a named trigger appears.
	a.run("trigger", "create", "--orkflow", "deploy", "--name", "hand-made", "-n", "payments").requireExit(t, exitOK)
	mark := len(a.requests())
	outcome := a.run("apply", doc, "--prune", "--expect-plan", digest)
	if outcome.code != exitError || !strings.Contains(outcome.stdout, "the plan differs from the one reviewed") ||
		!strings.Contains(outcome.stdout, "hand-made") {
		t.Fatalf("exit %d:\n%s", outcome.code, outcome.stdout)
	}
	if writes := a.writes(mark); len(writes) != 0 {
		t.Fatalf("a mismatched plan wrote: %v", writes)
	}
	// The digest of the plan that is about to run is accepted.
	out = a.run("--json", "apply", doc, "--prune", "--dry-run").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(out), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	a.run("apply", doc, "--prune", "--expect-plan", report.GetAutomation()[0].GetPlan().GetPlanDigest()).requireExit(t, exitOK)
}

func TestApply_CheckExitCode(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	a.run("apply", doc, "--check").requireExit(t, exitApplyCheckChanges)
	a.run("apply", doc).requireExit(t, exitOK)
	a.run("apply", doc, "--check").requireExit(t, exitOK)
	newOrkflow := a.write(t, "deploy.ork.yaml", orkflowSource(t, "deploy", "9.9.9"))
	a.run("apply", doc, newOrkflow, "-n", "payments", "--check").requireExit(t, exitApplyCheckChanges)
}

func TestApply_SecretsOutWritesAnExclusive0600FileAndRedactsStdout(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	path := filepath.Join(a.dir, "secrets.json")
	out := a.run("apply", doc, "--secrets-out", path).requireExit(t, exitOK).stdout
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", info.Mode().Perm())
	}
	raw, _ := os.ReadFile(path)
	var secrets orkstackv1.DisclosedSecrets
	if err := api.UnmarshalJSON(raw, &secrets); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	kinds := map[string]string{}
	for _, secret := range secrets.GetSecrets() {
		kinds[secret.GetKind().String()] = secret.GetSecret()
		if strings.Contains(out, secret.GetSecret()) {
			t.Errorf("stdout carries a secret written to --secrets-out:\n%s", out)
		}
	}
	if kinds["AUTOMATION_OBJECT_KIND_TRIGGER"] == "" || kinds["AUTOMATION_OBJECT_KIND_WEBHOOK"] != "generated-webhook-secret" {
		t.Errorf("secrets = %v", secrets.GetSecrets())
	}
	if !strings.Contains(out, "secret written to "+path) {
		t.Errorf("stdout does not say where the secret went:\n%s", out)
	}
	if secrets.GetSecrets()[0].GetInvokeUrl() == "" && secrets.GetSecrets()[1].GetInvokeUrl() == "" {
		t.Error("the trigger's invoke URL is missing")
	}
}

// A --secrets-out write that fails after the steps ran must not lose the
// one-time secrets: they are printed, the report carries them, and no
// partial file is left behind.
func TestApply_SecretsOutLateWriteFailurePrintsTheSecrets(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%v", jsonOutput), func(t *testing.T) {
			a := newAutomationFixture(t)
			doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
			path := filepath.Join(a.dir, "secrets.json")
			saved := secretsFileWrite
			secretsFileWrite = func(*os.File, []byte) error { return fmt.Errorf("no space left on device") }
			t.Cleanup(func() { secretsFileWrite = saved })
			args := []string{"apply", doc, "--secrets-out", path}
			if jsonOutput {
				args = append([]string{"--json"}, args...)
			}
			outcome := a.run(args...)
			if outcome.code != exitError {
				t.Fatalf("exit = %d, want 1\n%s\n%s", outcome.code, outcome.stdout, outcome.stderr)
			}
			if !strings.Contains(outcome.stderr, "no space left on device") {
				t.Errorf("stderr does not name the write failure:\n%s", outcome.stderr)
			}
			if _, err := os.Lstat(path); err == nil {
				t.Error("a failed --secrets-out write left a file behind")
			}
			if strings.Contains(outcome.stdout, "secret written to") {
				t.Errorf("stdout claims the secrets were written:\n%s", outcome.stdout)
			}
			if !strings.Contains(outcome.stdout, "generated-webhook-secret") {
				t.Errorf("the webhook secret was not printed:\n%s", outcome.stdout)
			}
			if !jsonOutput {
				if !strings.Contains(outcome.stdout, "Its token is shown once:") {
					t.Errorf("the trigger token was not printed:\n%s", outcome.stdout)
				}
				return
			}
			var report orkstackv1.ApplyReport
			if err := api.UnmarshalJSON([]byte(outcome.stdout), &report); err != nil {
				t.Fatalf("decode report: %v\n%s", err, outcome.stdout)
			}
			secrets := 0
			for _, result := range report.GetAutomation()[0].GetResults() {
				if result.GetSecret() != "" {
					secrets++
				}
			}
			if secrets != 2 {
				t.Errorf("report carries %d secrets, want the trigger's and the webhook's", secrets)
			}
		})
	}
}

func TestApply_AutomationTakesOnlyAnExplicitNamespace(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	t.Setenv(namespaceEnvVar, "billing")
	a.run("apply", doc, "--dry-run").requireExit(t, exitOK)
	var plan orkstackv1.PlanAutomationRequest
	if !a.lastRequest("/orkstack.v1.AutomationService/PlanAutomation", &plan) || plan.GetNamespace() != "" {
		t.Fatalf("ORK_NAMESPACE reached the plan: %q", plan.GetNamespace())
	}
	a.run("apply", doc, "--dry-run", "-n", "payments").requireExit(t, exitOK)
	if !a.lastRequest("/orkstack.v1.AutomationService/PlanAutomation", &plan) || plan.GetNamespace() != "payments" {
		t.Fatalf("explicit -n = %q, want payments", plan.GetNamespace())
	}
	outcome := a.run("apply", doc, "--dry-run", "-n", "billing")
	if outcome.code != exitError || !strings.Contains(outcome.stdout, `the document describes namespace "payments" but this request is for "billing"`) {
		t.Fatalf("mismatched -n: exit %d\n%s", outcome.code, outcome.stdout)
	}
}

func TestApply_ExecutorHandlesEveryStepVariant(t *testing.T) {
	oneof := (&orkstackv1.AutomationStep{}).ProtoReflect().Descriptor().Oneofs().ByName("request")
	fields := oneof.Fields()
	if fields.Len() == 0 {
		t.Fatal("AutomationStep has no request variants")
	}
	seen := map[protoreflect.Name]bool{}
	for i := 0; i < fields.Len(); i++ {
		name := fields.Get(i).Name()
		seen[name] = true
		if _, ok := stepExecutors[name]; !ok {
			t.Errorf("no executor for AutomationStep.%s", name)
		}
	}
	for name := range stepExecutors {
		if !seen[name] {
			t.Errorf("executor %s names no AutomationStep request variant", name)
		}
	}
}

func TestApply_SecretEnvIsReadFromTheEnvironment(t *testing.T) {
	a := newAutomationFixture(t)
	t.Setenv("ETL_WEBHOOK_SECRET", "pre-shared-secret")
	doc := strings.Replace(cliAutomationDoc, "events: [TASK_SUCCEEDED, RUN_FAILED]\n", "events: [TASK_SUCCEEDED, RUN_FAILED]\n    secret_env: ETL_WEBHOOK_SECRET\n", 1)
	out := a.run("apply", a.write(t, "automation.ork.yaml", doc)).requireExit(t, exitOK).stdout
	if strings.Contains(out, "pre-shared-secret") {
		t.Fatalf("the pre-shared secret was printed:\n%s", out)
	}
	hooks, err := a.webhooks.MatchingContext(context.Background(), webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "payments", OrkflowName: "nightly-etl"})
	if err != nil || len(hooks) != 1 || hooks[0].Secret != "pre-shared-secret" {
		t.Fatalf("registered = %+v, %v; want the environment's secret", hooks, err)
	}
}

func TestApply_PlanGoldens(t *testing.T) {
	a := newAutomationFixture(t)
	// State a reviewer would see drift in: an older schedule, a named trigger
	// the document does not declare, an unnamed one, an unscoped webhook.
	a.run("schedule", "create", "nightly-reconcile", "--orkflow", "reconcile", "--cron", "0 3 * * *",
		"--timezone", "Europe/Lisbon", "--input-json", `{"mode": "full"}`, "-n", "payments").requireExit(t, exitOK)
	a.run("trigger", "create", "--orkflow", "deploy", "--name", "manual-hook", "-n", "payments").requireExit(t, exitOK)
	a.run("trigger", "create", "--orkflow", "deploy", "-n", "payments").requireExit(t, exitOK)
	a.run("webhook", "register", "https://hooks.acme.dev/etl", "-n", "payments", "--name", "etl-loader",
		"--event", "TASK_SUCCEEDED", "--event", "RUN_FAILED").requireExit(t, exitOK)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	newOrkflow := a.write(t, "deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0"))

	requireGolden(t, "apply-plan.golden", a.normalize(a.run("apply", newOrkflow, doc, "-n", "payments", "--dry-run").requireExit(t, exitOK).stdout))
	requireGolden(t, "apply-report-dryrun.json.golden", a.normalize(a.run("--json", "apply", doc, "--dry-run").requireExit(t, exitOK).stdout))

	broken := a.write(t, "broken.ork.yaml", strings.Replace(strings.Replace(cliAutomationDoc, "orkflow: deploy", "orkflow: missing", 1),
		"cron: \"0 2 * * *\"", "cron: \"CRON_TZ=UTC 0 2 * * *\"", 1))
	requireGolden(t, "apply-plan-issues.golden", a.normalize(a.run("apply", broken, "--dry-run").stdout))

	requireGolden(t, "apply-result.golden", a.normalize(a.run("apply", doc).requireExit(t, exitOK).stdout))
	requireGolden(t, "apply-report.json.golden", a.normalize(a.run("--json", "apply", doc, "--prune").requireExit(t, exitOK).stdout))
}

func TestApply_ReapplyIsANoOp(t *testing.T) {
	a := newAutomationFixture(t)
	doc := a.write(t, "automation.ork.yaml", cliAutomationDoc)
	a.run("apply", doc).requireExit(t, exitOK)
	mark := len(a.requests())
	out := a.run("--json", "apply", doc).requireExit(t, exitOK).stdout
	if writes := a.writes(mark); len(writes) != 0 {
		t.Fatalf("a re-apply wrote: %v", writes)
	}
	var report map[string]any
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if strings.Contains(out, `"results"`) {
		t.Fatalf("a no-op apply reported results:\n%s", out)
	}
}

func TestExport_PrintsYAMLAndWarnsOnStderr(t *testing.T) {
	a := newAutomationFixture(t)
	a.run("apply", a.write(t, "automation.ork.yaml", cliAutomationDoc)).requireExit(t, exitOK)
	a.run("trigger", "create", "--orkflow", "deploy", "-n", "payments").requireExit(t, exitOK)
	outcome := a.run("export", "-n", "payments").requireExit(t, exitOK)
	if !strings.HasPrefix(outcome.stdout, "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\n") {
		t.Fatalf("export stdout:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "have no name") || !strings.Contains(outcome.stderr, "orkctl trigger update trg-") {
		t.Fatalf("export stderr does not warn about the unnamed trigger:\n%s", outcome.stderr)
	}
	exported := a.write(t, "exported.ork.yaml", outcome.stdout)
	a.run("apply", exported, "--check").requireExit(t, exitOK)
	requireGolden(t, "export.golden", a.normalize(outcome.stdout))
}

func TestTriggerUpdate_ReadModifyWrite(t *testing.T) {
	a := newAutomationFixture(t)
	a.run("apply", a.write(t, "deploy.ork.yaml", orkflowSource(t, "deploy", "1.4.0")), "-n", "payments").requireExit(t, exitOK)
	created := a.run("--json", "trigger", "create", "--orkflow", "deploy", "--description", "kept", "-n", "payments").requireExit(t, exitOK).stdout
	var resp orkstackv1.CreateTriggerResponse
	if err := api.UnmarshalJSON([]byte(created), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	id := resp.GetTrigger().GetId()
	out := a.run("trigger", "update", id, "-n", "payments", "--name", "github-deploy", "--version", "1.4.0").requireExit(t, exitOK).stdout
	if !strings.Contains(out, "DESCRIPTION  kept") || !strings.Contains(out, "The token is unchanged.") {
		t.Errorf("an unflagged field changed, or the token line is missing:\n%s", out)
	}
	requireGolden(t, "trigger-update.golden", a.normalize(out))
	out = a.run("trigger", "update", id, "-n", "payments", "--follow-current", "--name", "").requireExit(t, exitOK).stdout
	if !strings.Contains(out, "VERSION      current") || !strings.Contains(out, "NAME         -") {
		t.Errorf("--follow-current / --name \"\" did not clear:\n%s", out)
	}
	a.run("trigger", "update", "trg-ffffffffffffffff", "-n", "payments", "--name", "x").requireExit(t, exitError).
		requireStderrContains(t, "not_found")
	a.run("trigger", "update", id, "-n", "payments", "--name", "github-deploy").requireExit(t, exitOK)
	requireGolden(t, "trigger-list.golden", a.normalize(a.run("trigger", "list", "-n", "payments").requireExit(t, exitOK).stdout))
}

func TestWebhookUpdate_ReadModifyWrite(t *testing.T) {
	a := newAutomationFixture(t)
	a.run("webhook", "register", "https://example.test/a", "-n", "payments", "--event", "RUN_FAILED").requireExit(t, exitOK)
	id := "wh-0000000000000001"
	out := a.run("webhook", "update", id, "--name", "etl-loader", "--url", "https://example.test/b").requireExit(t, exitOK).stdout
	if !strings.Contains(out, "RUN_FAILED") || !strings.Contains(out, "payments/*/*") || !strings.Contains(out, "The signing secret is unchanged.") {
		t.Errorf("an unflagged field changed:\n%s", out)
	}
	requireGolden(t, "webhook-update.golden", out)
	hooks, _ := a.webhooks.MatchingContext(context.Background(), webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "payments"})
	if len(hooks) != 1 || hooks[0].Secret != "generated-webhook-secret" || hooks[0].URL != "https://example.test/b" {
		t.Fatalf("delivery read = %+v", hooks)
	}
	mark := len(a.requests())
	outcome := a.run("webhook", "update", id, "--deployment-wide")
	if outcome.code != exitError || !strings.Contains(outcome.stderr, "a name requires a namespace scope") {
		t.Fatalf("--deployment-wide with a name: exit %d\n%s", outcome.code, outcome.stderr)
	}
	for _, path := range a.requests()[mark:] {
		if strings.HasSuffix(path, "/UpdateWebhook") {
			t.Fatal("a locally refused update reached the server")
		}
	}
	a.run("webhook", "update", id, "--clear-events", "--orkflow", "nightly-etl").requireExit(t, exitOK).
		requireContains(t, "ALL", "payments/nightly-etl/*")
	a.run("webhook", "update", id, "--name", "", "--deployment-wide").requireExit(t, exitOK).
		requireContains(t, "*/nightly-etl/*")
}

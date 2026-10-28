package automation_test

import (
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
)

// FuzzParseAndPlan reads arbitrary automation documents, the shipped example
// among the seeds. A document is parsed into a document or into issues that
// each say where and why, never a panic; a document without an error plans
// against a deployment where nothing exists by creating what it declares and
// deleting, updating or adopting nothing; and planning is pure: the same
// inputs give the same steps and the same digest.
func FuzzParseAndPlan(f *testing.F) {
	example, err := os.ReadFile("../../examples/automation/examples.ork.yaml")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(example, false)
	f.Add(example, true)
	for _, seed := range []string{
		"", "apiVersion: orkstack/v1\nkind: Automation\nnamespace: default\n",
		"apiVersion: orkstack/v1\nkind: Automation\nnamespace: Bad\n", "apiVersion: orkstack/v1\nkind: Orkflow\n", "---\n---\n",
		"apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\nschedules:\n  s:\n    orkflow: o\n    cron: '* * * * *'\n    input: {a: yes}\n",
		"apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\nwebhooks:\n  w:\n    url: https://example.test/h\n    events: [RUN_FAILED]\n    secret_env: HOOK\n",
		"apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\ntriggers:\n  t:\n    orkflow: o\n    version: '1.0'\n",
		"apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\nsettings: {run_retention: 1h, allowed_pools: [main]}\n", "a: &x [*x]", "{{", "\x00",
	} {
		f.Add([]byte(seed), false)
	}
	f.Fuzz(func(t *testing.T, source []byte, prune bool) {
		doc, issues := automation.Parse(source)
		for _, issue := range issues {
			if issue.GetMessage() == "" {
				t.Fatalf("Parse(%q) raised an issue with no message: %v", source, issue)
			}
		}
		if automation.HasErrors(issues) {
			return
		}
		if doc == nil {
			t.Fatalf("Parse(%q) had no error and no document", source)
		}
		live := automation.Live{Orkflows: map[automation.OrkflowRef]bool{}}
		for _, ref := range doc.References() {
			live.Orkflows[ref] = true
		}
		steps, planIssues := automation.Plan(doc, live, automation.Options{Prune: prune})
		for _, issue := range planIssues {
			if issue.GetMessage() == "" {
				t.Fatalf("Plan(%q) raised an issue with no message: %v", source, issue)
			}
		}
		for _, step := range steps {
			switch step.GetAction() {
			case orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE, orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED:
			default:
				t.Fatalf("Plan(%q) against an empty deployment planned %v for %s %s", source, step.GetAction(), step.GetKind(), step.GetName())
			}
		}
		again, _ := automation.Plan(doc, live, automation.Options{Prune: prune})
		if len(again) != len(steps) {
			t.Fatalf("Plan(%q) is not deterministic: %d steps then %d", source, len(steps), len(again))
		}
		for i := range steps {
			if !proto.Equal(steps[i], again[i]) {
				t.Fatalf("Plan(%q) is not deterministic at step %d:\n%v\n%v", source, i, steps[i], again[i])
			}
		}
		if a, b := automation.Digest(doc.Namespace, steps), automation.Digest(doc.Namespace, again); a != b || !strings.HasPrefix(a, "sha256:") && a != "" {
			t.Fatalf("Plan(%q) digests %q then %q", source, a, b)
		}
	})
}

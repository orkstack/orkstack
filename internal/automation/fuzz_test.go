package automation_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
)

const secondDocumentHeader = "apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\n"

// FuzzParseAndPlan reads arbitrary automation documents, the shipped example
// among the seeds. A document is parsed into a document or into issues that
// each say where and why, never a panic; a document without an error plans
// against a deployment where nothing exists by creating what it declares and
// deleting, updating or adopting nothing; planning is pure: the same inputs
// give the same steps and the same digest; and a file with a second YAML
// document is refused, on the line it begins, whatever that document holds.
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
		secondDocumentHeader + "---\n" + secondDocumentHeader, secondDocumentHeader + "---\nname: [unclosed\n", secondDocumentHeader + "---\n\tname: a\n",
		secondDocumentHeader + "---\n", secondDocumentHeader + "...\nname: b\n",
	} {
		f.Add([]byte(seed), false)
	}
	f.Fuzz(func(t *testing.T, source []byte, prune bool) {
		doc, issues := automation.Parse(source)
		if line, found := dsl.SecondDocumentLine(source); found {
			want := fmt.Sprintf("line %d: %s", line, dsl.SecondDocumentMessage("automation document"))
			if doc != nil || len(issues) != 1 || issues[0].GetMessage() != want {
				t.Fatalf("Parse(%q) with a second document on line %d = %v, %v, want one issue %q", source, line, doc, issues, want)
			}
			return
		}
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
			switch {
			case step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE,
				step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED:
			case step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE &&
				step.GetKind() == orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_NAMESPACE &&
				step.GetName() == "default":
				// `default` always resolves, so settings declared for it
				// update it even on an empty deployment.
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

package automation_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
)

// TestParse_TheShippedAutomationExample is U5: the automation document under
// examples/automation parses with no issue, declares what its header says,
// and names only Orkflows that are shipped examples.
func TestParse_TheShippedAutomationExample(t *testing.T) {
	source, err := os.ReadFile("../../examples/automation/examples.ork.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc, issues := automation.Parse(source)
	if len(issues) != 0 {
		t.Fatalf("Parse issues = %v, want none", issues)
	}
	if doc.Namespace != "examples" {
		t.Errorf("namespace = %q, want examples", doc.Namespace)
	}
	schedule, ok := doc.Schedules["poll-exports"]
	if !ok {
		t.Fatalf("schedules = %v, want poll-exports", doc.Schedules)
	}
	if schedule.Orkflow != "poll-until-ready" {
		t.Errorf("poll-exports orkflow = %q, want poll-until-ready", schedule.Orkflow)
	}
	if got := schedule.Spec.GetCron(); !reflect.DeepEqual(got, []string{"*/15 * * * *"}) {
		t.Errorf("poll-exports cron = %v, want */15 * * * *", got)
	}
	if got := schedule.Spec.GetOverlap(); got != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP {
		t.Errorf("poll-exports overlap = %v, want skip", got)
	}
	trigger, ok := doc.Triggers["change-from-ci"]
	if !ok {
		t.Fatalf("triggers = %v, want change-from-ci", doc.Triggers)
	}
	if trigger.Orkflow != "approval-gate" {
		t.Errorf("change-from-ci orkflow = %q, want approval-gate", trigger.Orkflow)
	}

	shipped := map[string]bool{}
	paths, err := filepath.Glob("../../examples/*.ork.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v (%d found)", err, len(paths))
	}
	for _, path := range paths {
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatalf("compile %s: %v", path, err)
		}
		shipped[wf.GetName()] = true
	}
	for name, s := range doc.Schedules {
		if !shipped[s.Orkflow] {
			t.Errorf("schedule %s names %q, which is no shipped example", name, s.Orkflow)
		}
	}
	for name, tr := range doc.Triggers {
		if !shipped[tr.Orkflow] {
			t.Errorf("trigger %s names %q, which is no shipped example", name, tr.Orkflow)
		}
	}
	for name, w := range doc.Webhooks {
		if w.Orkflow != "" && !shipped[w.Orkflow] {
			t.Errorf("webhook %s names %q, which is no shipped example", name, w.Orkflow)
		}
	}
}

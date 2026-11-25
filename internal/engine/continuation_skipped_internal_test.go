package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A skipped fan-out is taken from a saved state as it was skipped, and refused
// with anything only one that ran holds.
func TestValidRestoredRow_ASkippedFanOut(t *testing.T) {
	fan := eachTask("fan", "build", "build")
	s := buildRunState("ork-r", &orkstackv1.Orkflow{Name: "wf", Tasks: []*orkstackv1.Task{ptask("build"), fan}}, nil, RunPlan{Namespace: "default"})
	skipped := func(change func(*orkstackv1.TaskRun)) *orkstackv1.TaskRun {
		row := fanRow("fan", sSkip)
		row.Each = &orkstackv1.TaskEach{}
		change(row)
		return row
	}
	if what := s.validRestoredRow("fan", skipped(func(*orkstackv1.TaskRun) {})); what != "" {
		t.Fatal(what)
	}
	for name, change := range map[string]func(*orkstackv1.TaskRun){
		"expanded":     func(r *orkstackv1.TaskRun) { r.Each.Expanded = true },
		"a count":      func(r *orkstackv1.TaskRun) { r.Each.Count = 3 },
		"lines":        func(r *orkstackv1.TaskRun) { r.Each.Lines = &orkstackv1.EachLines{} },
		"items":        func(r *orkstackv1.TaskRun) { r.Items = []*orkstackv1.TaskRun{pitem(0, sPend)} },
		"a values_ref": func(r *orkstackv1.TaskRun) { r.ValuesRef = &orkstackv1.ArtifactRef{Uri: "mem://v"} },
		"a commit":     func(r *orkstackv1.TaskRun) { r.Commit = &orkstackv1.CommitRef{Uri: "mem://c"} },
	} {
		if s.validRestoredRow("fan", skipped(change)) == "" {
			t.Errorf("a skipped fan-out with %s was taken", name)
		}
	}
}

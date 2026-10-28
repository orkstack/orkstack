package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The merger reads a part job by the names of its steps (merge.go,
// proofStepNames and setupStepNames), because that is all GitHub's jobs API
// says of a composite action or a step: these tests pin those names to
// .github/workflows/proof.yml, in both directions, so a renamed step cannot
// silently change which attempts may run again.

// stepRunning is the first step of a job whose script contains text.
func stepRunning(t *testing.T, w *wfFile, job, text string) *wfStep {
	t.Helper()
	for i := range w.Jobs[job].Steps {
		if strings.Contains(w.Jobs[job].Steps[i].Run, text) {
			return &w.Jobs[job].Steps[i]
		}
	}
	t.Fatalf("jobs.%s has no step running %q", job, text)
	return nil
}

// proofStepNameProblems holds merge.go's proofStepNames to proof.yml: every
// step that runs `scripts/proof.sh --part` carries a name the merger knows,
// and the merger knows no name that no such step carries. A stale entry
// would misread a renamed step, and with it whether the proof step started.
func proofStepNameProblems(w *wfFile) []string {
	var p []string
	have := map[string]bool{}
	for _, name := range w.jobNames() {
		for i, s := range w.Jobs[name].Steps {
			if !strings.Contains(s.Run, "scripts/proof.sh --part") {
				continue
			}
			if s.Name == "" {
				p = append(p, fmt.Sprintf("jobs.%s %s runs a part under no name", name, s.label(i)))
				continue
			}
			have[s.Name] = true
			if !proofStepNames[s.Name] {
				p = append(p, fmt.Sprintf("jobs.%s %s runs a part under a name the merger does not know", name, s.label(i)))
			}
		}
	}
	if len(have) == 0 {
		p = append(p, "no step runs scripts/proof.sh --part")
	}
	var stale []string
	for name := range proofStepNames {
		if !have[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		p = append(p, fmt.Sprintf("merge.go's proofStepNames names %q, which no step of proof.yml runs a part under", name))
	}
	return p
}

func TestWorkflows_TheMergersProofStepNamesAreTheWorkflowsExactly(t *testing.T) {
	expectEachMutationRefused(t, "proof.yml", proofStepNameProblems, []mutation{
		{"the integration step renamed", func(w *wfFile) {
			stepRunning(t, w, "integration", "scripts/proof.sh --part").Name = "integration shard (renamed)"
		}},
		{"the UI step renamed", func(w *wfFile) { stepRunning(t, w, "ui-e2e", "scripts/proof.sh --part").Name = "ui shard" }},
		{"the plan step unnamed", func(w *wfFile) { stepRunning(t, w, "plan", "scripts/proof.sh --part").Name = "" }},
		{"the observability step renamed", func(w *wfFile) {
			stepRunning(t, w, "observability", "scripts/proof.sh --part").Name = "obs"
		}},
		{"a part step the merger has never heard of", func(w *wfFile) {
			job := w.Jobs["fast"]
			job.Steps = append(job.Steps, wfStep{Name: "another part", Run: `scripts/proof.sh --part fast2`})
		}},
		{"a part step removed", func(w *wfFile) {
			job := w.Jobs["activity-fixture"]
			var kept []wfStep
			for _, s := range job.Steps {
				if !strings.Contains(s.Run, "scripts/proof.sh --part") {
					kept = append(kept, s)
				}
			}
			job.Steps = kept
		}},
	})
	// The pin bites from the map's side as well: an entry no step runs under
	// is refused.
	proofStepNames["a step nobody runs"] = true
	defer delete(proofStepNames, "a step nobody runs")
	if p := proofStepNameProblems(loadWorkflow(t, "proof.yml")); len(p) == 0 {
		t.Error("a proofStepNames entry that no step runs under is accepted")
	}
}

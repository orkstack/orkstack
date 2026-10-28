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

// The proof job's summary says which parts failed in a setup step and which
// ran again, where a reviewer reads it.
func TestWorkflows_TheSummaryNamesASetupFailure(t *testing.T) {
	for _, s := range loadWorkflow(t, "proof.yml").Jobs["proof"].Steps {
		if s.Name != "summary" {
			continue
		}
		if !strings.Contains(s.Run, `.setup_failures[]?`) || !strings.Contains(s.Run, "setup failure: ") {
			t.Error("the summary step does not print proof.json's setup_failures")
		}
		return
	}
	t.Fatal("jobs.proof has no summary step")
}

// proofHostAction is the composite action that turns a runner into a proof
// host (.github/actions/proof-host).
const proofHostAction = "./.github/actions/proof-host"

// setupRunStepMarkers is, for each allowlisted step that runs a script, what
// that script must contain to be the download or install its name says.
var setupRunStepMarkers = map[string]string{
	"bun install":         "bun install",
	"bun install (site)":  "bun install",
	"Chromium (host)":     "playwright install",
	"observability stack": "docker compose",
}

// setupMarkers are the texts that make a script step before the proof step a
// download or an install: such a step must be on the allowlist, so that a
// step added to a job is a decision about its eligibility and not an accident.
var setupMarkers = []string{"bun install", "playwright install", "docker compose", "docker pull"}

// partStepAt is the index of a job's step that runs a part, or -1.
func partStepAt(job *wfJob) int {
	for i, s := range job.Steps {
		if strings.Contains(s.Run, "scripts/proof.sh --part") {
			return i
		}
	}
	return -1
}

// setupStepProblems holds merge.go's setupStepNames to proof.yml in both
// directions. Each allowlisted name is a step of the job of its kind, with
// that exact name (a composite action's steps are only ever reported under
// the name of the step that uses it), before the job's proof step, and is the
// fetch, host setup or install it claims to be; and every fetch, host setup
// or install step before a proof step is on the list. The plan and the build
// have no entry, and every other part job has one.
func setupStepProblems(w *wfFile) []string {
	var p []string
	var kinds []string
	for kind := range setupStepNames {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		job := w.Jobs[kind]
		if job == nil {
			p = append(p, fmt.Sprintf("merge.go's setupStepNames names the job %q, which proof.yml does not have", kind))
			continue
		}
		proofAt := partStepAt(job)
		if proofAt < 0 {
			p = append(p, fmt.Sprintf("jobs.%s has no proof step", kind))
			continue
		}
		byName := map[string]int{}
		for i, s := range job.Steps[:proofAt] {
			if s.Name == "" {
				continue
			}
			if _, dup := byName[s.Name]; dup {
				p = append(p, fmt.Sprintf("jobs.%s has two steps named %q before its proof step", kind, s.Name))
			}
			byName[s.Name] = i
		}
		var names []string
		for name := range setupStepNames[kind] {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			i, ok := byName[name]
			if !ok {
				p = append(p, fmt.Sprintf("merge.go's setupStepNames[%q] names %q, and jobs.%s has no step of that name before its proof step", kind, name, kind))
				continue
			}
			s := job.Steps[i]
			good := false
			switch {
			case s.Uses == proofHostAction:
				good = name == "proof host"
			case s.Uses == proofFetchAction:
				good = strings.HasPrefix(name, "fetch ")
			case s.Uses == "" && s.Run != "":
				marker, known := setupRunStepMarkers[name]
				good = known && strings.Contains(s.Run, marker)
			}
			if !good {
				p = append(p, fmt.Sprintf("jobs.%s %s is allowlisted as a fetch or install step, and is not one", kind, s.label(i)))
			}
		}
	}
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		proofAt := partStepAt(job)
		if proofAt < 0 {
			continue
		}
		list, listed := setupStepNames[name]
		switch {
		case name == "plan" || name == "build":
			if listed {
				p = append(p, fmt.Sprintf("merge.go's setupStepNames lists the %s job, whose setup failure no other part survives", name))
			}
			continue
		case !listed:
			p = append(p, fmt.Sprintf("jobs.%s runs a part and has no entry in merge.go's setupStepNames", name))
			continue
		}
		for i, s := range job.Steps[:proofAt] {
			fetches := s.Uses == proofFetchAction || s.Uses == proofHostAction
			for _, marker := range setupMarkers {
				if strings.Contains(s.Run, marker) {
					fetches = true
				}
			}
			if fetches && !list[s.Name] {
				p = append(p, fmt.Sprintf("jobs.%s %s fetches or installs before the proof step and is not in merge.go's setupStepNames[%q]", name, s.label(i), name))
			}
		}
	}
	return p
}

func TestWorkflows_TheSetupStepsAreTheWorkflowsExactly(t *testing.T) {
	step := func(w *wfFile, job, name string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if w.Jobs[job].Steps[i].Name == name {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no step %q", job, name)
		return nil
	}
	hostStep := func(w *wfFile, job string) *wfStep {
		for i := range w.Jobs[job].Steps {
			if w.Jobs[job].Steps[i].Uses == proofHostAction {
				return &w.Jobs[job].Steps[i]
			}
		}
		t.Fatalf("jobs.%s has no proof-host step", job)
		return nil
	}
	expectEachMutationRefused(t, "proof.yml", setupStepProblems, []mutation{
		{"fetch plan renamed in integration", func(w *wfFile) { step(w, "integration", "fetch plan").Name = "fetch the plan" }},
		{"fetch build renamed in observability", func(w *wfFile) { step(w, "observability", "fetch build").Name = "download build" }},
		{"the host setup unnamed in ui-e2e", func(w *wfFile) { hostStep(w, "ui-e2e").Name = "" }},
		{"the host setup named as the API would", func(w *wfFile) { hostStep(w, "activity-fixture").Name = "Run ./.github/actions/proof-host" }},
		{"the browser image fetch renamed", func(w *wfFile) { step(w, "ui-e2e", "fetch browser image").Name = "pull browser" }},
		{"the host Chromium install renamed", func(w *wfFile) { step(w, "ui-e2e", "Chromium (host)").Name = "Chromium" }},
		{"the observability stack renamed", func(w *wfFile) { step(w, "observability", "observability stack").Name = "stack" }},
		{"bun install no longer installs", func(w *wfFile) { step(w, "ui-e2e", "bun install").Run = "echo skipped" }},
		{"the Chromium install no longer installs", func(w *wfFile) { step(w, "ui-e2e", "Chromium (host)").Run = "echo skipped" }},
		{"an allowlisted fetch swapped for a script", func(w *wfFile) {
			s := step(w, "integration", "fetch build")
			s.Uses, s.Run = "", "gh run download"
		}},
		{"a fetch moved after the proof step", func(w *wfFile) {
			job := w.Jobs["integration"]
			var moved wfStep
			var kept []wfStep
			for _, s := range job.Steps {
				if s.Name == "fetch build" {
					moved = s
					continue
				}
				kept = append(kept, s)
			}
			job.Steps = append(kept, moved)
		}},
		{"a new fetch step nobody listed", func(w *wfFile) {
			job := w.Jobs["observability"]
			job.Steps = append([]wfStep{{Name: "fetch more", Uses: proofFetchAction}}, job.Steps...)
		}},
		{"a new install step nobody listed", func(w *wfFile) {
			job := w.Jobs["integration"]
			job.Steps = append([]wfStep{{Name: "bun install (extra)", Run: "bun install"}}, job.Steps...)
		}},
		{"a pull step nobody listed", func(w *wfFile) {
			job := w.Jobs["activity-fixture"]
			job.Steps = append([]wfStep{{Name: "pull", Run: "docker pull alpine"}}, job.Steps...)
		}},
		{"two steps of one name", func(w *wfFile) {
			job := w.Jobs["fast"]
			job.Steps = append([]wfStep{{Name: "bun install", Run: "bun install"}}, job.Steps...)
		}},
		{"the fast job's installs removed", func(w *wfFile) {
			job := w.Jobs["fast"]
			var kept []wfStep
			for _, s := range job.Steps {
				if !strings.HasPrefix(s.Name, "bun install") {
					kept = append(kept, s)
				}
			}
			job.Steps = kept
		}},
	})

	// The pin bites from the table's side too.
	mutateTable := func(what string, change, restore func()) {
		change()
		defer restore()
		if p := setupStepProblems(loadWorkflow(t, "proof.yml")); len(p) == 0 {
			t.Errorf("the pin accepts %s", what)
		}
	}
	mutateTable("a step the job does not have", func() { setupStepNames["fast"]["a step nobody runs"] = true },
		func() { delete(setupStepNames["fast"], "a step nobody runs") })
	mutateTable("the build job on the list", func() { setupStepNames["build"] = map[string]bool{"fetch plan": true} },
		func() { delete(setupStepNames, "build") })
	mutateTable("the plan job on the list", func() { setupStepNames["plan"] = map[string]bool{"bun install": true} },
		func() { delete(setupStepNames, "plan") })
	saved := setupStepNames["fast"]
	mutateTable("a part job with no entry", func() { delete(setupStepNames, "fast") },
		func() { setupStepNames["fast"] = saved })
	mutateTable("a job proof.yml does not have", func() { setupStepNames["site"] = map[string]bool{"x": true} },
		func() { delete(setupStepNames, "site") })
	mutateTable("a fetch step dropped from the list", func() { delete(setupStepNames["integration"], "fetch build") },
		func() { setupStepNames["integration"]["fetch build"] = true })
}

package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// kind-orker.yml runs scripts/kind-orker.sh, which makes a throwaway kind
// cluster and runs scripts/k8s-orker-check.sh against it (docs/DEPLOY.md
// "Orkers on Kubernetes"): on a change
// to what the bases run, every day, and by dispatch. It is not a required
// check and no proof gate. Its rule returns its problems, so a companion test
// shows that it bites on a weakened copy.

// kindOrkerTriggerPaths are the paths whose change must run the lane: the
// bases, both scripts, the Orker and the builds of its image, and the
// workflow itself.
var kindOrkerTriggerPaths = []string{
	"deploy/kubernetes/**",
	"scripts/kind-orker.sh",
	"scripts/k8s-orker-check.sh",
	"internal/orker/**",
	"cmd/orker/**",
	"deploy/source/**",
	"deploy/Dockerfile",
	".github/workflows/kind-orker.yml",
}

// workflowCrons lists every schedule of a workflow, as its cron strings.
func workflowCrons(w *wfFile) []string {
	var crons []string
	list, _ := w.On["schedule"].([]any)
	for _, entry := range list {
		if m, ok := entry.(map[string]any); ok {
			if c, ok := m["cron"].(string); ok {
				crons = append(crons, c)
			}
		}
	}
	return crons
}

func onlyDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

// kindOrkerProblems: the lane runs the script, with nothing that lets a
// failure pass; it keeps its evidence whatever happened, without the
// throwaway cluster's credentials; and it runs on the changes, the schedule
// and the dispatch it is for, at a time no other workflow is scheduled.
func kindOrkerProblems(others map[string]*wfFile) func(*wfFile) []string {
	return func(w *wfFile) []string {
		const file = "kind-orker.yml"
		job, ok := w.Jobs["kind-orker"]
		if !ok {
			return []string{file + " has no kind-orker job"}
		}
		var p []string
		if job.ContinueOnError != nil {
			p = append(p, file+": jobs.kind-orker continues on error, so a failed scenario would not fail the run")
		}
		ran, kept := false, false
		for i, s := range job.Steps {
			if s.ContinueOnError != nil {
				p = append(p, fmt.Sprintf("%s: jobs.kind-orker %s continues on error", file, s.label(i)))
			}
			if strings.TrimSpace(s.Run) == "scripts/kind-orker.sh" {
				ran = true
				if s.If != "" {
					p = append(p, fmt.Sprintf("%s: jobs.kind-orker %s is conditional, so the proof can be skipped", file, s.label(i)))
				}
			}
			if strings.HasPrefix(s.Uses, "actions/upload-artifact@") {
				kept = true
				if s.If != "always()" {
					p = append(p, fmt.Sprintf("%s: jobs.kind-orker %s runs on %q, not always(), so a failed run keeps no evidence", file, s.label(i), s.If))
				}
				path := withString(s, "path")
				for _, left := range []string{"kubeconfig", "tls/"} {
					if !strings.Contains(path, "!.data/kind-orker/2*/"+left) {
						p = append(p, fmt.Sprintf("%s: jobs.kind-orker %s uploads the throwaway cluster's %s", file, s.label(i), left))
					}
				}
			}
		}
		if !ran {
			p = append(p, file+": jobs.kind-orker does not run scripts/kind-orker.sh")
		}
		if !kept {
			p = append(p, file+": jobs.kind-orker uploads nothing")
		}

		for _, event := range []string{"push", "pull_request"} {
			on, _ := w.On[event].(map[string]any)
			list, _ := on["paths"].([]any)
			var paths []string
			for _, v := range list {
				paths = append(paths, fmt.Sprint(v))
			}
			for _, want := range kindOrkerTriggerPaths {
				if !slices.Contains(paths, want) {
					p = append(p, fmt.Sprintf("%s: a %s that changes %s does not run the lane", file, event, want))
				}
			}
		}
		if _, ok := w.On["workflow_dispatch"]; !ok {
			p = append(p, file+" cannot be dispatched")
		}
		crons := workflowCrons(w)
		if len(crons) != 1 {
			p = append(p, fmt.Sprintf("%s has %d schedules, want one daily run", file, len(crons)))
		}
		for _, cron := range crons {
			fields := strings.Fields(cron)
			if len(fields) != 5 || !onlyDigits(fields[0]) || !onlyDigits(fields[1]) ||
				fields[2] != "*" || fields[3] != "*" || fields[4] != "*" {
				p = append(p, fmt.Sprintf("%s: schedule %q is not once a day", file, cron))
				continue
			}
			for name, other := range others {
				for _, theirs := range workflowCrons(other) {
					if f := strings.Fields(theirs); len(f) >= 2 && f[0] == fields[0] {
						p = append(p, fmt.Sprintf("%s: schedule %q starts at the same minute as %s's %q", file, cron, name, theirs))
					}
				}
			}
		}
		return p
	}
}

func TestWorkflows_TheKubernetesOrkerLaneRunsWhatItIsFor(t *testing.T) {
	others := map[string]*wfFile{}
	for _, f := range allWorkflows {
		if f != "kind-orker.yml" {
			others[f] = loadWorkflow(t, f)
		}
	}
	step := func(w *wfFile, match func(wfStep) bool) *wfStep {
		for i := range w.Jobs["kind-orker"].Steps {
			if match(w.Jobs["kind-orker"].Steps[i]) {
				return &w.Jobs["kind-orker"].Steps[i]
			}
		}
		t.Fatal("jobs.kind-orker has no such step")
		return nil
	}
	script := func(s wfStep) bool { return strings.TrimSpace(s.Run) == "scripts/kind-orker.sh" }
	upload := func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") }
	dropPath := func(event, path string) func(*wfFile) {
		return func(w *wfFile) {
			on := w.On[event].(map[string]any)
			var kept []any
			for _, v := range on["paths"].([]any) {
				if v != path {
					kept = append(kept, v)
				}
			}
			on["paths"] = kept
		}
	}
	expectEachMutationRefused(t, "kind-orker.yml", kindOrkerProblems(others), []mutation{
		{"no kind-orker job", func(w *wfFile) { delete(w.Jobs, "kind-orker") }},
		{"a job that continues on error", func(w *wfFile) { w.Jobs["kind-orker"].ContinueOnError = true }},
		{"the script step continues on error", func(w *wfFile) { step(w, script).ContinueOnError = true }},
		{"a skippable script step", func(w *wfFile) { step(w, script).If = "false" }},
		{"a job that runs something else", func(w *wfFile) { step(w, script).Run = "make check" }},
		{"evidence kept only on success", func(w *wfFile) { step(w, upload).If = "success()" }},
		{"the cluster's credentials uploaded", func(w *wfFile) { step(w, upload).With["path"] = ".data/kind-orker/2*/" }},
		{"no upload", func(w *wfFile) { step(w, upload).Uses = "actions/cache@v5.0.0" }},
		{"a push to the Orker does not run it", dropPath("push", "internal/orker/**")},
		{"a pull request to the bases does not run it", dropPath("pull_request", "deploy/kubernetes/**")},
		{"a change to the script does not run it", dropPath("push", "scripts/kind-orker.sh")},
		{"no dispatch", func(w *wfFile) { delete(w.On, "workflow_dispatch") }},
		{"no schedule", func(w *wfFile) { delete(w.On, "schedule") }},
		{"an hourly schedule", func(w *wfFile) { w.On["schedule"] = []any{map[string]any{"cron": "29 * * * *"}} }},
		{"the nightly's minute", func(w *wfFile) { w.On["schedule"] = []any{map[string]any{"cron": "17 6 * * *"}} }},
	})
}

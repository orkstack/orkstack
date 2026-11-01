package main

import (
	"fmt"
	"strings"
	"testing"
)

// buildKitPins lists the driver-opts of every docker/setup-buildx-action step
// of a workflow, in job order.
func buildKitPins(w *wfFile) []string {
	var pins []string
	for _, job := range w.jobNames() {
		for _, s := range w.Jobs[job].Steps {
			if strings.HasPrefix(s.Uses, "docker/setup-buildx-action@") {
				pins = append(pins, withString(s, "driver-opts"))
			}
		}
	}
	return pins
}

// buildKitPinProblems is the rule: release.yml sets up buildx once, on a
// BuildKit image pinned by tag and digest, and every buildx step of proof.yml
// runs that same image.
func buildKitPinProblems(release, proof *wfFile) []string {
	pins := buildKitPins(release)
	if len(pins) != 1 {
		return []string{fmt.Sprintf("release.yml sets up buildx %d times, want once", len(pins))}
	}
	var problems []string
	image, _ := strings.CutPrefix(pins[0], "image=")
	if !pinnedImage.MatchString(strings.TrimSpace(image)) {
		problems = append(problems, fmt.Sprintf("release.yml runs BuildKit by driver-opts %q, not a pinned image", pins[0]))
	}
	got := buildKitPins(proof)
	if len(got) == 0 {
		problems = append(problems, "proof.yml sets up no buildx")
	}
	for _, p := range got {
		if p != pins[0] {
			problems = append(problems, fmt.Sprintf("proof.yml runs BuildKit by driver-opts %q, release.yml by %q", p, pins[0]))
		}
	}
	return problems
}

// TestWorkflows_TheProofBuildsWithTheReleasesBuildKit: proof.yml's build job
// builds the images every stack part of a CI proof runs, so it runs the
// BuildKit release.yml builds a release with, by the same digest. Dependabot
// sees neither pin (each is a `with:` value), so they are moved by hand, and
// this holds one to the other.
func TestWorkflows_TheProofBuildsWithTheReleasesBuildKit(t *testing.T) {
	expectNoProblems(t, "release.yml and proof.yml", buildKitPinProblems(loadWorkflow(t, "release.yml"), loadWorkflow(t, "proof.yml")))
}

func TestWorkflows_TheBuildKitRuleRefusesEachWeakening(t *testing.T) {
	const other = "image=moby/buildkit:v0.34.1@sha256:0000000000000000000000000000000000000000000000000000000000000000"
	buildx := func(w *wfFile) *wfStep {
		for _, job := range w.jobNames() {
			for i := range w.Jobs[job].Steps {
				if strings.HasPrefix(w.Jobs[job].Steps[i].Uses, "docker/setup-buildx-action@") {
					return &w.Jobs[job].Steps[i]
				}
			}
		}
		panic("the workflow has no buildx step to mutate")
	}
	setPin := func(pin string) func(*wfFile) {
		return func(w *wfFile) { buildx(w).With["driver-opts"] = pin }
	}
	addStep := func(pin string) func(*wfFile) {
		return func(w *wfFile) {
			s := *buildx(w)
			s.With = map[string]any{"driver-opts": pin}
			job := w.Jobs[w.jobNames()[0]]
			job.Steps = append(job.Steps, s)
		}
	}
	release, proof := loadWorkflow(t, "release.yml"), loadWorkflow(t, "proof.yml")
	pin := buildKitPins(release)[0]

	expectEachMutationRefused(t, "proof.yml", func(w *wfFile) []string { return buildKitPinProblems(release, w) }, []mutation{
		{"BuildKit by tag alone", setPin("image=moby/buildkit:v0.34.1")},
		{"BuildKit of another digest", setPin(other)},
		{"no driver-opts, so buildx's own default BuildKit", func(w *wfFile) { delete(buildx(w).With, "driver-opts") }},
		{"no buildx step", func(w *wfFile) { buildx(w).Uses = "docker/setup-qemu-action@v4.0.0" }},
		{"a second buildx step on another BuildKit", addStep(other)},
	})
	expectEachMutationRefused(t, "release.yml", func(w *wfFile) []string { return buildKitPinProblems(w, proof) }, []mutation{
		{"BuildKit of another digest", setPin(other)},
		{"no buildx step", func(w *wfFile) { buildx(w).Uses = "docker/setup-qemu-action@v4.0.0" }},
		{"a second buildx step", addStep(pin)},
	})
	// release.yml held to itself, so only the shape of its own pin can fail it.
	expectEachMutationRefused(t, "release.yml", func(w *wfFile) []string { return buildKitPinProblems(w, w) }, []mutation{
		{"BuildKit by tag alone", setPin("image=moby/buildkit:v0.34.1")},
		{"BuildKit by digest alone", setPin("image=moby/buildkit@" + strings.SplitN(pin, "@", 2)[1])},
		{"BuildKit by a floating tag", setPin("image=moby/buildkit:latest")},
		{"no driver-opts", func(w *wfFile) { delete(buildx(w).With, "driver-opts") }},
	})
}

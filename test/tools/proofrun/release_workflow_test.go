package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// ---- the release's artifacts and its publication ---------------------------------
//
// release.yml builds what a release publishes in `artifacts`, read-only and
// before anything is tagged, and runs the quickstart on it end to end; then
// `publish`, which holds the only package write, pushes the images and creates
// the GitHub release without checking out or running any repository code
// (docs/PROOFS.md §10). Each rule returns its problems, and a companion test
// shows that it bites on a mutated copy.

// releaseImageNames is every image a release publishes.
var releaseImageNames = []string{
	"ork", "orker", "orkctl", "ui",
	"activity-shell", "activity-http", "activity-file", "activity-json", "activity-git", "activity-template",
}

// repoCodeInRun finds a step that runs repository code or a toolchain that
// would: make, go, bun, node, npm, python, or a script of the repository.
// A quote before the command counts, so `bash -c "make dist"` is found too.
var repoCodeInRun = regexp.MustCompile(`(^|[\s;&|("'])(make|go|bun|bunx|node|npm|npx|python3?)(\s|$|["'])|(^|[\s;&|("'])(\./)?scripts/`)

func stepIndex(steps []wfStep, pred func(wfStep) bool) int {
	for i, s := range steps {
		if pred(s) {
			return i
		}
	}
	return -1
}

func holdsToken(s wfStep) bool {
	return envString(s, "GH_TOKEN") != "" || envString(s, "GITHUB_TOKEN") != ""
}

func validatedGroupProblem(name string, job *wfJob) []string {
	group := fmt.Sprint(job.Concurrency["group"])
	if !strings.Contains(group, "needs.validate.outputs.version") || strings.Contains(group, "inputs.") {
		return []string{fmt.Sprintf("jobs.%s's concurrency group %q is not the validated version", name, group)}
	}
	return nil
}

func releaseArtifactsProblems(w *wfFile) []string {
	job, ok := w.Jobs["artifacts"]
	if !ok {
		return []string{"release.yml has no artifacts job"}
	}
	var p []string
	if !contains(job.needs(), "validate") {
		p = append(p, "jobs.artifacts does not need validate")
	}
	if tag, ok := w.Jobs["tag"]; !ok || !contains(tag.needs(), "artifacts") {
		p = append(p, "jobs.tag does not need artifacts: a version could be tagged that never built or ran")
	}
	if len(job.Permissions) != 1 || job.Permissions["contents"] != "read" {
		p = append(p, fmt.Sprintf("jobs.artifacts.permissions = %v, want contents: read alone", job.Permissions))
	}
	p = append(p, validatedGroupProblem("artifacts", job)...)
	smoke := stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "scripts/release-smoke.sh") })
	upload := stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
	switch {
	case smoke < 0:
		p = append(p, "jobs.artifacts never runs scripts/release-smoke.sh")
	case upload < 0:
		p = append(p, "jobs.artifacts uploads nothing for publish")
	case upload < smoke:
		p = append(p, "jobs.artifacts uploads before the smoke ran")
	}
	if smoke >= 0 && !strings.Contains(job.Steps[smoke].Run, "--layouts") {
		p = append(p, "the smoke builds its own images instead of running the layouts publish pushes")
	}
	if stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "deploy/skopeo-image.txt") }) < 0 {
		p = append(p, "jobs.artifacts does not hand publish the repository's skopeo pin")
	}
	for i, s := range job.Steps {
		if isCheckout(s) && (withString(s, "ref") != "${{ github.sha }}" || withString(s, "persist-credentials") != "false") {
			p = append(p, fmt.Sprintf("jobs.artifacts %s does not check out github.sha without credentials", s.label(i)))
		}
		if holdsToken(s) {
			p = append(p, fmt.Sprintf("jobs.artifacts %s is handed a token", s.label(i)))
		}
	}
	return p
}

func TestWorkflows_ReleaseBuildsAndSmokesBeforeAnythingIsTagged(t *testing.T) {
	expectNoProblems(t, "release.yml", releaseArtifactsProblems(loadWorkflow(t, "release.yml")))
	expectEachMutationRefused(t, "release.yml", releaseArtifactsProblems, []mutation{
		{"no artifacts job", func(w *wfFile) { delete(w.Jobs, "artifacts") }},
		{"artifacts can write", func(w *wfFile) { w.Jobs["artifacts"].Permissions["contents"] = "write" }},
		{"the tag does not wait for artifacts", func(w *wfFile) { w.Jobs["tag"].Needs = []any{"validate", "gate"} }},
		{"no smoke", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			for i := range job.Steps {
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, "scripts/release-smoke.sh", "true")
			}
		}},
		{"the upload before the smoke", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			up := stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
			moved := job.Steps[up]
			job.Steps = append([]wfStep{moved}, append(job.Steps[:up:up], job.Steps[up+1:]...)...)
		}},
		{"the smoke on images of its own", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			for i := range job.Steps {
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, "--layouts dist/layouts ", "")
			}
		}},
		{"no skopeo pin handed on", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			for i := range job.Steps {
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, "deploy/skopeo-image.txt", "/dev/null")
			}
		}},
		{"a checkout that keeps the token", func(w *wfFile) {
			steps := w.Jobs["artifacts"].Steps
			i := stepIndex(steps, isCheckout)
			if i < 0 || steps[i].With == nil {
				panic("jobs.artifacts has no checkout step with inputs to mutate")
			}
			steps[i].With["persist-credentials"] = true
		}},
		{"a step handed the token", func(w *wfFile) {
			s := &w.Jobs["artifacts"].Steps[len(w.Jobs["artifacts"].Steps)-2]
			if s.Env == nil {
				s.Env = map[string]any{}
			}
			s.Env["GH_TOKEN"] = "${{ github.token }}"
		}},
		{"concurrency from the raw input", func(w *wfFile) {
			w.Jobs["artifacts"].Concurrency["group"] = "release-${{ inputs.version }}"
		}},
	})
}

// extractRegex returns the first pattern a run line matches a variable
// against, `[[ "$<name>" =~ <pattern> ]]`, compiled.
func extractRegex(run, name string) (*regexp.Regexp, error) {
	m := regexp.MustCompile(`\[\[ "\$` + name + `" =~ (\S+) \]\]`).FindStringSubmatch(run)
	if m == nil {
		return nil, fmt.Errorf("no [[ \"$%s\" =~ … ]] check", name)
	}
	return regexp.Compile(m[1])
}

func publishProblems(w *wfFile) []string {
	job, ok := w.Jobs["publish"]
	if !ok {
		return []string{"release.yml has no publish job"}
	}
	var p []string
	for _, n := range []string{"validate", "artifacts", "tag"} {
		if !contains(job.needs(), n) {
			p = append(p, "jobs.publish does not need "+n)
		}
	}
	cond := normalizeIf(job.If)
	for _, want := range []string{"inputs.tag", "github.ref == 'refs/heads/main'"} {
		if !strings.Contains(cond, want) {
			p = append(p, fmt.Sprintf("jobs.publish.if = %q lacks %q", job.If, want))
		}
	}
	if or, balanced := topLevelOr(cond); or || !balanced {
		p = append(p, fmt.Sprintf("jobs.publish.if = %q has a top-level ||", job.If))
	}
	if got := strings.Join(writePermissions(job.Permissions), ", "); got != "contents: write, packages: write" || len(job.Permissions) != 2 {
		p = append(p, fmt.Sprintf("jobs.publish.permissions = %v, want exactly contents: write and packages: write", job.Permissions))
	}
	p = append(p, validatedGroupProblem("publish", job)...)
	for _, name := range w.jobNames() {
		if name != "publish" && permLevel(w.Jobs[name].Permissions["packages"]) > 1 {
			p = append(p, fmt.Sprintf("jobs.%s writes packages; only publish may", name))
		}
	}
	download, check, push, release := -1, -1, -1, -1
	for i, s := range job.Steps {
		if isCheckout(s) || strings.HasPrefix(s.Uses, "./") {
			p = append(p, fmt.Sprintf("jobs.publish %s checks out or runs repository code", s.label(i)))
		} else if s.Uses != "" && !strings.HasPrefix(s.Uses, "actions/download-artifact@") {
			p = append(p, fmt.Sprintf("jobs.publish %s uses %s; publish uses only download-artifact", s.label(i), s.Uses))
		}
		if m := repoCodeInRun.FindString(s.Run); m != "" {
			p = append(p, fmt.Sprintf("jobs.publish %s runs %q: publish runs no repository code", s.label(i), strings.TrimSpace(m)))
		}
		switch {
		case download < 0 && strings.HasPrefix(s.Uses, "actions/download-artifact@"):
			download = i
		case check < 0 && strings.Contains(s.Run, "sha256sum -c SHA256SUMS"):
			check = i
		case push < 0 && strings.Contains(s.Run, "copy --all --preserve-digests"):
			push = i
		case release < 0 && strings.Contains(s.Run, "gh release create"):
			release = i
		}
		if holdsToken(s) && i != push && i != release {
			p = append(p, fmt.Sprintf("jobs.publish %s is handed a token and neither pushes nor releases", s.label(i)))
		}
	}
	if download < 0 || check <= download || push <= check || release <= push {
		p = append(p, fmt.Sprintf("jobs.publish runs download %d, checksums %d, push %d, release %d: want each, in that order", download, check, push, release))
	}
	if release >= 0 {
		r := job.Steps[release].Run
		// Whether this release is the latest is decided by validate from the
		// repository's tags, so a backport never takes releases/latest.
		if !strings.Contains(r, `--latest="$LATEST"`) || envString(job.Steps[release], "LATEST") != "${{ needs.validate.outputs.latest }}" {
			p = append(p, `gh release create does not pass --latest="$LATEST" from needs.validate.outputs.latest`)
		}
		for _, want := range []string{"--verify-tag", "--latest"} {
			if !strings.Contains(r, want) {
				p = append(p, "gh release create lacks "+want)
			}
		}
		for _, never := range []string{"--prerelease", "--draft"} {
			if strings.Contains(r, never) {
				p = append(p, "gh release create passes "+never+": releases/latest/download would skip the release")
			}
		}
	}
	if push >= 0 {
		r := job.Steps[push].Run
		trap := strings.Index(r, `trap 'rm -f "$RUNNER_TEMP/auth.json"' EXIT`)
		if write := strings.Index(r, `>"$RUNNER_TEMP/auth.json"`); trap < 0 || write < 0 || trap > write {
			p = append(p, "the push writes auth.json without first trapping its removal on every exit")
		}
		if !strings.Contains(r, `[ "$have" = "$digest" ]`) {
			p = append(p, "the push would move a version tag that already holds another digest")
		}
		if !strings.Contains(r, `[ "$got" = "$digest" ]`) {
			p = append(p, "the push never checks the digest each tag holds afterwards")
		}
	}
	if check >= 0 {
		r := job.Steps[check].Run
		if re, err := extractRegex(r, "name"); err != nil {
			p = append(p, "the image list check: "+err.Error())
		} else {
			for _, n := range releaseImageNames {
				if !re.MatchString(n) {
					p = append(p, "the image list check refuses "+n)
				}
			}
			for _, bad := range []string{"activity-evil", "ork orker", "../ork", "ork;true", "", "orkx"} {
				if re.MatchString(bad) {
					p = append(p, fmt.Sprintf("the image list check accepts %q", bad))
				}
			}
		}
		if re, err := extractRegex(r, "digest"); err != nil {
			p = append(p, "the digest check: "+err.Error())
		} else if !re.MatchString("sha256:"+strings.Repeat("a", 64)) || re.MatchString("sha256:abc") || re.MatchString("latest") {
			p = append(p, "the digest check does not hold sha256:<64 hex>")
		}
		if !strings.Contains(r, fmt.Sprintf(`-eq %d`, len(releaseImageNames))) {
			p = append(p, fmt.Sprintf("the image list check does not require exactly %d images", len(releaseImageNames)))
		}
		// Before anything is pushed: each image once, each layout the image
		// its digest names and built for both platforms, the bundle naming
		// exactly those images, and the assets exactly what SHA256SUMS sums.
		for _, want := range []struct{ text, says string }{
			{"uniq -d", "refuses an image listed twice"},
			{`.manifests[0].digest`, "checks that each layout's index names the listed digest"},
			{`blobs/sha256/${digest#sha256:}`, "hashes each layout's manifest blob"},
			{`"linux/amd64,linux/arm64"`, "requires both platforms"},
			{`orkstack-quickstart/.env`, "compares the image list with the quickstart bundle's .env"},
			{`ghcr.io/orkstack/$name:$full@$digest`, "requires the bundle to name each image by its tag and digest"},
			{`cmp -s`, "compares the release assets with SHA256SUMS"},
		} {
			if !strings.Contains(r, want.text) {
				p = append(p, "the check never "+want.says+" ("+want.text+")")
			}
		}
	}
	if download >= 0 && withString(job.Steps[download], "name") != "${{ needs.artifacts.outputs.artifact }}" {
		p = append(p, fmt.Sprintf("jobs.publish downloads %q, not ${{ needs.artifacts.outputs.artifact }}: a re-run would look for its own attempt's",
			withString(job.Steps[download], "name")))
	}
	return p
}

func TestWorkflows_PublishRunsNoRepositoryCodeAndHoldsTheOnlyPackageWrite(t *testing.T) {
	expectNoProblems(t, "release.yml", publishProblems(loadWorkflow(t, "release.yml")))
	steps := func(w *wfFile) []wfStep { return w.Jobs["publish"].Steps }
	edit := func(w *wfFile, find, replace string) {
		for i := range w.Jobs["publish"].Steps {
			w.Jobs["publish"].Steps[i].Run = strings.ReplaceAll(w.Jobs["publish"].Steps[i].Run, find, replace)
		}
	}
	expectEachMutationRefused(t, "release.yml", publishProblems, []mutation{
		{"no publish job", func(w *wfFile) { delete(w.Jobs, "publish") }},
		{"a checkout", func(w *wfFile) {
			w.Jobs["publish"].Steps = append([]wfStep{{Uses: "actions/checkout@v7.0.1"}}, steps(w)...)
		}},
		{"make in a step", func(w *wfFile) { edit(w, "set -euo pipefail\n", "set -euo pipefail\nmake dist\n") }},
		{"a repository script", func(w *wfFile) {
			edit(w, "set -euo pipefail\n", "set -euo pipefail\nscripts/release-smoke.sh v1.0.0\n")
		}},
		{"go run", func(w *wfFile) { edit(w, "set -euo pipefail\n", "set -euo pipefail\ngo run ./x\n") }},
		{"another action", func(w *wfFile) {
			w.Jobs["publish"].Steps = append(steps(w), wfStep{Uses: "docker/login-action@v4.7.0"})
		}},
		{"a pre-release", func(w *wfFile) { edit(w, "--latest", "--latest --prerelease") }},
		{"a draft", func(w *wfFile) { edit(w, "--latest", "--latest --draft") }},
		{"no --verify-tag", func(w *wfFile) { edit(w, "--verify-tag ", "") }},
		{"the release before the push", func(w *wfFile) {
			s := steps(w)
			n := len(s)
			s[n-1], s[n-2] = s[n-2], s[n-1]
		}},
		{"no checksum check", func(w *wfFile) { edit(w, "sha256sum -c SHA256SUMS", "true") }},
		{"publish on any ref", func(w *wfFile) { w.Jobs["publish"].If = "${{ inputs.tag }}" }},
		{"publish without the tag", func(w *wfFile) { w.Jobs["publish"].Needs = []any{"validate", "artifacts"} }},
		{"another job writes packages", func(w *wfFile) { w.Jobs["artifacts"].Permissions["packages"] = "write" }},
		{"publish cannot write packages", func(w *wfFile) { delete(w.Jobs["publish"].Permissions, "packages") }},
		{"a moved version tag", func(w *wfFile) { edit(w, `[ "$have" = "$digest" ]`, "true") }},
		{"no digest check after the push", func(w *wfFile) { edit(w, `[ "$got" = "$digest" ]`, "true") }},
		{"a looser name check", func(w *wfFile) {
			edit(w, `^(ork|orker|orkctl|ui|activity-(shell|http|file|json|git|template))$`, `^[a-z-]+$`)
		}},
		{"a looser digest check", func(w *wfFile) { edit(w, `^sha256:[0-9a-f]{64}$`, `^sha256:`) }},
		{"any number of images", func(w *wfFile) { edit(w, "-eq 10", "-ge 1") }},
		{"an image listed twice", func(w *wfFile) { edit(w, "uniq -d", "uniq -u") }},
		{"no layout digest check", func(w *wfFile) { edit(w, `.manifests[0].digest`, `.manifests[0].size`) }},
		{"no blob hash", func(w *wfFile) { edit(w, `blobs/sha256/${digest#sha256:}`, `index.json`) }},
		{"one platform", func(w *wfFile) { edit(w, `"linux/amd64,linux/arm64"`, `"linux/amd64"`) }},
		{"no bundle cross-check", func(w *wfFile) { edit(w, `orkstack-quickstart/.env`, `orkstack-quickstart/README.txt`) }},
		{"the bundle may name another tag", func(w *wfFile) {
			edit(w, `ghcr.io/orkstack/$name:$full@$digest`, `ghcr.io/orkstack/$name`)
		}},
		{"no asset-set check", func(w *wfFile) { edit(w, "cmp -s", "true") }},
		{"the credential is not trapped", func(w *wfFile) {
			edit(w, `trap 'rm -f "$RUNNER_TEMP/auth.json"' EXIT`, ":")
		}},
		{"always the latest", func(w *wfFile) { edit(w, `--latest="$LATEST"`, "--latest") }},
		{"the download by this attempt's name", func(w *wfFile) {
			s := &w.Jobs["publish"].Steps[stepIndex(steps(w), func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/download-artifact@") })]
			s.With["name"] = "release-${{ needs.validate.outputs.version }}-${{ github.run_id }}-a${{ github.run_attempt }}"
		}},
		{"a token in the check step", func(w *wfFile) {
			for i := range steps(w) {
				if s := &w.Jobs["publish"].Steps[i]; strings.Contains(s.Run, "sha256sum -c SHA256SUMS") {
					s.Env = map[string]any{"GH_TOKEN": "${{ github.token }}"}
				}
			}
		}},
	})
}

// TestWorkflows_TheSkopeoPinIsTheRepositorysPin: the image that copies the
// layouts is one exact release pinned by digest (deploy/skopeo-image.txt),
// the smoke and publish read that file, and publish's check refuses a
// floating one.
func TestWorkflows_TheSkopeoPinIsTheRepositorysPin(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "skopeo-image.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var pin string
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			pin = line
			break
		}
	}
	if !regexp.MustCompile(`^quay\.io/skopeo/stable:v[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`).MatchString(pin) {
		t.Errorf("deploy/skopeo-image.txt names %q, not quay.io/skopeo/stable:vX.Y.Z@sha256:<64 hex>", pin)
	}
	w := loadWorkflow(t, "release.yml")
	publish, ok := w.Jobs["publish"]
	if !ok {
		t.Fatal("release.yml has no publish job")
	}
	at := stepIndex(publish.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "sha256sum -c SHA256SUMS") })
	if at < 0 {
		t.Fatal("jobs.publish has no step that runs sha256sum -c SHA256SUMS")
	}
	check := publish.Steps[at]
	re, err := extractRegex(check.Run, "skopeo")
	if err != nil {
		t.Fatalf("publish's check: %v", err)
	}
	if !re.MatchString(pin) {
		t.Errorf("publish's check refuses the repository's own pin %q", pin)
	}
	for _, bad := range []string{"quay.io/skopeo/stable:latest", "quay.io/skopeo/stable:v1.22.3", "evil.example/skopeo:v1.0.0@sha256:" + strings.Repeat("a", 64)} {
		if re.MatchString(bad) {
			t.Errorf("publish's check accepts %q", bad)
		}
	}
	smoke, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "release-smoke.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(smoke), "deploy/skopeo-image.txt") {
		t.Error("scripts/release-smoke.sh does not read deploy/skopeo-image.txt")
	}
}

// ---- what a re-run finds, and what builds the release ---------------------------
//
// A re-run of one job is attempt n+1 of the run, so an artifact name built from
// github.run_attempt there would miss attempt 1's upload. Every job that
// downloads by name reads the name from the job that uploaded it, as proof.yml's
// shards do (docs/PROOFS.md §8).

func releaseRerunProblems(w *wfFile, proof *wfFile) []string {
	var p []string
	artifacts, ok := w.Jobs["artifacts"]
	if !ok {
		return []string{"release.yml has no artifacts job"}
	}
	upload := stepIndex(artifacts.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
	if upload < 0 {
		p = append(p, "jobs.artifacts uploads nothing")
	} else if got, name := artifacts.Outputs["artifact"], withString(artifacts.Steps[upload], "name"); got == "" || got != name {
		p = append(p, fmt.Sprintf("jobs.artifacts.outputs.artifact = %q, but it uploads %q", got, name))
	}
	for _, job := range w.jobNames() {
		for i, s := range w.Jobs[job].Steps {
			if !strings.HasPrefix(s.Uses, "actions/download-artifact@") {
				continue
			}
			name := withString(s, "name")
			if strings.Contains(name, "github.run_attempt") || !strings.HasPrefix(name, "${{ needs.") {
				p = append(p, fmt.Sprintf("jobs.%s %s downloads %q, not a name from needs.*.outputs", job, s.label(i), name))
			}
		}
	}
	tag, ok := w.Jobs["tag"]
	if !ok {
		return append(p, "release.yml has no tag job")
	}
	if at := stepIndex(tag.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/download-artifact@") }); at < 0 ||
		withString(tag.Steps[at], "name") != "${{ needs.gate.outputs.proof_artifact }}" {
		p = append(p, "jobs.tag does not download the proof by needs.gate.outputs.proof_artifact")
	}
	call, _ := proof.On["workflow_call"].(map[string]any)
	outputs, _ := call["outputs"].(map[string]any)
	out, _ := outputs["proof_artifact"].(map[string]any)
	if fmt.Sprint(out["value"]) != "${{ jobs.proof.outputs.proof_artifact }}" {
		p = append(p, "proof.yml's workflow_call does not output proof_artifact from jobs.proof.outputs.proof_artifact")
	}
	if job, ok := proof.Jobs["proof"]; ok {
		uploads := 0
		for _, s := range job.Steps {
			if strings.HasPrefix(s.Uses, "actions/upload-artifact@") && withString(s, "name") == job.Outputs["proof_artifact"] {
				uploads++
			}
		}
		if job.Outputs["proof_artifact"] == "" || uploads != 1 {
			p = append(p, fmt.Sprintf("proof.yml's jobs.proof.outputs.proof_artifact = %q is not the name its one merged upload uses",
				job.Outputs["proof_artifact"]))
		}
	} else {
		p = append(p, "proof.yml has no proof job")
	}
	return p
}

func TestWorkflows_ReleaseRerunsFindTheirArtifacts(t *testing.T) {
	proof := loadWorkflow(t, "proof.yml")
	rule := func(w *wfFile) []string { return releaseRerunProblems(w, proof) }
	expectNoProblems(t, "release.yml", rule(loadWorkflow(t, "release.yml")))
	expectEachMutationRefused(t, "release.yml", rule, []mutation{
		{"no output for the upload", func(w *wfFile) { delete(w.Jobs["artifacts"].Outputs, "artifact") }},
		{"the output is not the upload's name", func(w *wfFile) { w.Jobs["artifacts"].Outputs["artifact"] = "release-x" }},
		{"the tag reads this attempt's proof", func(w *wfFile) {
			job := w.Jobs["tag"]
			job.Steps[stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/download-artifact@") })].
				With["name"] = "proof-${{ github.run_id }}-a${{ github.run_attempt }}"
		}},
	})
	// And proof.yml's side of it.
	release := loadWorkflow(t, "release.yml")
	for _, m := range []mutation{
		{"no workflow_call output", func(w *wfFile) {
			delete(w.On["workflow_call"].(map[string]any), "outputs")
		}},
		{"the proof job names another artifact", func(w *wfFile) { w.Jobs["proof"].Outputs["proof_artifact"] = "proof-x" }},
	} {
		mutated := loadWorkflow(t, "proof.yml")
		m.mutate(mutated)
		if len(releaseRerunProblems(release, mutated)) == 0 {
			t.Errorf("proof.yml mutation %q is not refused", m.name)
		}
	}
}

// pinnedImage is an image reference by an exact version, such as v0.33.1 or
// qemu-v10.2.3, and a digest.
var pinnedImage = regexp.MustCompile(`^[a-z0-9./-]+:([a-z0-9]+-)?v?[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`)

// releaseToolchainProblems: what builds a release runs nothing a floating tag
// or a restored cache chose. QEMU runs privileged, BuildKit builds every image
// and the Go cache would hold compiled packages, so each is pinned or off
// (AGENTS.md §11.1.3). Dependabot does not see an image inside `with:`; these
// digests are updated by hand.
func releaseToolchainProblems(w *wfFile) []string {
	var p []string
	qemu, buildx := 0, 0
	for _, job := range w.jobNames() {
		for i, s := range w.Jobs[job].Steps {
			switch {
			case strings.HasPrefix(s.Uses, "actions/setup-go@"):
				if withString(s, "cache") != "false" {
					p = append(p, fmt.Sprintf("jobs.%s %s restores a Go cache", job, s.label(i)))
				}
			case strings.HasPrefix(s.Uses, "docker/setup-qemu-action@"):
				qemu++
				if !pinnedImage.MatchString(withString(s, "image")) || withString(s, "cache-image") != "false" {
					p = append(p, fmt.Sprintf("jobs.%s %s runs binfmt %q (cache-image %q): want a pinned image and no cache",
						job, s.label(i), withString(s, "image"), withString(s, "cache-image")))
				}
			case strings.HasPrefix(s.Uses, "docker/setup-buildx-action@"):
				buildx++
				image, _ := strings.CutPrefix(withString(s, "driver-opts"), "image=")
				if !pinnedImage.MatchString(strings.TrimSpace(image)) {
					p = append(p, fmt.Sprintf("jobs.%s %s runs BuildKit from %q: want driver-opts image=<pinned image>",
						job, s.label(i), withString(s, "driver-opts")))
				}
			}
		}
	}
	if qemu == 0 || buildx == 0 {
		p = append(p, "release.yml sets up neither QEMU nor buildx; the arm64 images need both")
	}
	return p
}

func TestWorkflows_ReleaseToolchainIsPinned(t *testing.T) {
	step := func(w *wfFile, prefix string) *wfStep {
		job := w.Jobs["artifacts"]
		i := stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, prefix) })
		if i < 0 {
			panic("jobs.artifacts has no " + prefix + " step")
		}
		if job.Steps[i].With == nil {
			job.Steps[i].With = map[string]any{}
		}
		return &job.Steps[i]
	}
	expectEachMutationRefused(t, "release.yml", releaseToolchainProblems, []mutation{
		{"a Go cache", func(w *wfFile) { step(w, "actions/setup-go@").With["cache"] = true }},
		{"binfmt by tag", func(w *wfFile) { step(w, "docker/setup-qemu-action@").With["image"] = "tonistiigi/binfmt:latest" }},
		{"binfmt from the cache", func(w *wfFile) { delete(step(w, "docker/setup-qemu-action@").With, "cache-image") }},
		{"the default BuildKit", func(w *wfFile) { delete(step(w, "docker/setup-buildx-action@").With, "driver-opts") }},
		{"BuildKit by tag", func(w *wfFile) {
			step(w, "docker/setup-buildx-action@").With["driver-opts"] = "image=moby/buildkit:buildx-stable-1"
		}},
	})
}

// releaseOrderProblems: validate decides whether the release is the latest
// from every tag the repository has, and refuses one below a released tag of
// its own series, before anything is built.
func releaseOrderProblems(w *wfFile) []string {
	job, ok := w.Jobs["validate"]
	if !ok {
		return []string{"release.yml has no validate job"}
	}
	var p []string
	if job.Outputs["latest"] != "${{ steps.order.outputs.latest }}" {
		p = append(p, fmt.Sprintf("jobs.validate.outputs.latest = %q, want ${{ steps.order.outputs.latest }}", job.Outputs["latest"]))
	}
	order := -1
	for i, s := range job.Steps {
		if s.ID == "order" {
			order = i
		}
	}
	switch {
	case order < 0:
		p = append(p, "jobs.validate has no step order")
	case !strings.Contains(job.Steps[order].Run, "git tag --list") ||
		!strings.Contains(job.Steps[order].Run, "dist order -version") ||
		!strings.Contains(job.Steps[order].Run, `>>"$GITHUB_OUTPUT"`):
		p = append(p, "jobs.validate's order step does not hand git tag --list to dist order and its answer to $GITHUB_OUTPUT")
	}
	if at := stepIndex(job.Steps, isCheckout); at < 0 || withString(job.Steps[at], "fetch-depth") != "0" {
		p = append(p, "jobs.validate does not check out with fetch-depth: 0, so it would see no tags")
	}
	return p
}

func TestWorkflows_ReleaseKnowsWhetherItIsTheLatest(t *testing.T) {
	expectEachMutationRefused(t, "release.yml", releaseOrderProblems, []mutation{
		{"no output", func(w *wfFile) { delete(w.Jobs["validate"].Outputs, "latest") }},
		{"no order step", func(w *wfFile) {
			for i := range w.Jobs["validate"].Steps {
				w.Jobs["validate"].Steps[i].ID = strings.ReplaceAll(w.Jobs["validate"].Steps[i].ID, "order", "x")
			}
		}},
		{"no tags", func(w *wfFile) {
			job := w.Jobs["validate"]
			delete(job.Steps[stepIndex(job.Steps, isCheckout)].With, "fetch-depth")
		}},
	})
}

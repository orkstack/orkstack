package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// ---- the release's artifacts and its publication ---------------------------------
//
// release.yml builds what a release publishes in `artifacts`, read-only and
// before anything is tagged, runs the quickstart's compose file on it end to
// end, and runs the README's Quickstart as written on a clone of the commit;
// then
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
	smoke := stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "scripts/quickstart-smoke.sh --release") })
	quickstart := stepIndex(job.Steps, func(s wfStep) bool {
		return strings.TrimSpace(s.Run) == "scripts/quickstart-smoke.sh"
	})
	upload := stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
	switch {
	case smoke < 0:
		p = append(p, "jobs.artifacts never runs scripts/quickstart-smoke.sh --release")
	case quickstart < 0:
		p = append(p, "jobs.artifacts never runs the README's Quickstart (scripts/quickstart-smoke.sh)")
	case upload < 0:
		p = append(p, "jobs.artifacts uploads nothing for publish")
	case upload < smoke || upload < quickstart:
		p = append(p, "jobs.artifacts uploads before the smokes ran")
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
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, "scripts/quickstart-smoke.sh --release", "true")
			}
		}},
		{"no quickstart smoke", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			for i := range job.Steps {
				if strings.TrimSpace(job.Steps[i].Run) == "scripts/quickstart-smoke.sh" {
					job.Steps[i].Run = "true"
				}
			}
		}},
		{"the upload before the quickstart smoke", func(w *wfFile) {
			job := w.Jobs["artifacts"]
			q := stepIndex(job.Steps, func(s wfStep) bool { return strings.TrimSpace(s.Run) == "scripts/quickstart-smoke.sh" })
			up := stepIndex(job.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
			job.Steps[q], job.Steps[up] = job.Steps[up], job.Steps[q]
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
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, " --layouts dist/layouts", "")
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
	download, check, push, sign, release := -1, -1, -1, -1, -1
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
		case sign < 0 && strings.Contains(s.Run, imageSignCommand):
			sign = i
		case release < 0 && strings.Contains(s.Run, "gh release create"):
			release = i
		}
		if holdsToken(s) && i != push && i != sign && i != release {
			p = append(p, fmt.Sprintf("jobs.publish %s is handed a token and neither pushes, signs nor releases", s.label(i)))
		}
	}
	if download < 0 || check <= download || push <= check || sign <= push || release <= sign {
		p = append(p, fmt.Sprintf("jobs.publish runs download %d, checksums %d, push %d, signing %d, release %d: want each, in that order",
			download, check, push, sign, release))
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
		// its digest names and built for both platforms, and the assets
		// exactly what SHA256SUMS sums.
		for _, want := range []struct{ text, says string }{
			{"uniq -d", "refuses an image listed twice"},
			{`.manifests[0].digest`, "checks that each layout's index names the listed digest"},
			{`blobs/sha256/${digest#sha256:}`, "hashes each layout's manifest blob"},
			{`"linux/amd64,linux/arm64"`, "requires both platforms"},
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
			edit(w, "set -euo pipefail\n", "set -euo pipefail\nscripts/quickstart-smoke.sh --release v1.0.0\n")
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
			push := stepIndex(s, func(s wfStep) bool { return strings.Contains(s.Run, "copy --all --preserve-digests") })
			release := stepIndex(s, func(s wfStep) bool { return strings.Contains(s.Run, "gh release create") })
			s[push], s[release] = s[release], s[push]
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
	smoke, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "quickstart-smoke.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(smoke), "deploy/skopeo-image.txt") {
		t.Error("scripts/quickstart-smoke.sh does not read deploy/skopeo-image.txt")
	}
}

// ---- what a release signs -----------------------------------------------------------
//
// publish signs each image by its index digest, and SHA256SUMS, with the key
// the `release` environment holds, in the layout OrkStack's own verifier reads
// (internal/dsl/signature.go). cosign 3 writes a new bundle format by default,
// so every signing command names the flags that make it write the key-based
// `sha256-<hex>.sig` layout, and the release smoke signs with the same image
// and flags and runs an Orkflow under require_signed_by.

// cosignLegacyLayout makes cosign 3 write the `.sig` layout and record nothing
// in a transparency log.
const cosignLegacyLayout = "--new-bundle-format=false --use-signing-config=false --tlog-upload=false"

// The signing commands, each reading the key from the environment, never a file.
const (
	imageSignCommand = "cos sign --key env://COSIGN_PRIVATE_KEY " + cosignLegacyLayout
	blobSignCommand  = "cos sign-blob --key env://COSIGN_PRIVATE_KEY " + cosignLegacyLayout
)

// keyCheck refuses to sign without the key or its password.
const keyCheck = `[ -n "$COSIGN_PRIVATE_KEY" ] && [ -n "$COSIGN_PASSWORD" ] ||`

// keyUse is every way a signing step's script may name the private key:
// testing it, handing it to docker by name, or cosign reading it from the
// environment. Anything else could put it on the disk or in the log.
var keyUse = regexp.MustCompile(`\[ -n "\$COSIGN_PRIVATE_KEY" \]|-e COSIGN_PRIVATE_KEY\b|env://COSIGN_PRIVATE_KEY\b`)

// signedAfter is the line that verifies one image's signature by digest with
// the committed key, after it was signed.
const signedAfter = `cos verify --key /meta/cosign.pub --insecure-ignore-tlog=true "$ref" >/dev/null` + "\n"

func releaseSigningProblems(w *wfFile) []string {
	var p []string
	if job, ok := w.Jobs["validate"]; !ok || stepIndex(job.Steps, func(s wfStep) bool {
		return strings.Contains(s.Run, "[ -s deploy/cosign.pub ]")
	}) < 0 {
		p = append(p, "jobs.validate does not refuse a release whose deploy/cosign.pub is not committed")
	}
	if job, ok := w.Jobs["artifacts"]; ok {
		for _, f := range []string{"deploy/cosign-image.txt", "deploy/cosign.pub"} {
			if stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "cp "+f+" dist/meta/") }) < 0 {
				p = append(p, "jobs.artifacts does not hand publish "+f)
			}
		}
	}
	job, ok := w.Jobs["publish"]
	if !ok {
		return append(p, "release.yml has no publish job")
	}
	if job.environment() != "release" {
		p = append(p, fmt.Sprintf("jobs.publish runs in environment %q, not release, which alone holds the signing key", job.environment()))
	}
	runs := func(text string) int {
		return stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, text) })
	}
	blob, push, sign, release := runs(blobSignCommand), runs("copy --all --preserve-digests"), runs(imageSignCommand), runs("gh release create")
	switch {
	case blob < 0:
		p = append(p, "jobs.publish never signs SHA256SUMS with: "+blobSignCommand)
	case push < 0 || blob > push:
		p = append(p, "jobs.publish signs SHA256SUMS after the push: a missing or wrong key would be found once the images are out")
	}
	switch {
	case sign < 0:
		p = append(p, "jobs.publish never signs the images with: "+imageSignCommand)
	case push < 0 || sign < push:
		p = append(p, "jobs.publish signs the images before it pushes them")
	case release < 0 || release < sign:
		p = append(p, "jobs.publish creates the release before the images are signed")
	}
	for i, s := range job.Steps {
		if i != blob && i != sign {
			if strings.Contains(s.Run, "COSIGN_PRIVATE_KEY") || envString(s, "COSIGN_PRIVATE_KEY") != "" || envString(s, "COSIGN_PASSWORD") != "" {
				p = append(p, fmt.Sprintf("jobs.publish %s is handed the signing key and signs nothing", s.label(i)))
			}
			continue
		}
		if envString(s, "COSIGN_PRIVATE_KEY") != "${{ secrets.COSIGN_PRIVATE_KEY }}" || envString(s, "COSIGN_PASSWORD") != "${{ secrets.COSIGN_PASSWORD }}" {
			p = append(p, fmt.Sprintf("jobs.publish %s does not read the key and its password from the release environment's secrets", s.label(i)))
		}
		if at := strings.Index(s.Run, keyCheck); at < 0 || at > strings.Index(s.Run, "docker run") {
			p = append(p, fmt.Sprintf("jobs.publish %s runs cosign without first refusing a missing key or password", s.label(i)))
		}
		if used, safe := strings.Count(s.Run, "COSIGN_PRIVATE_KEY"), len(keyUse.FindAllString(s.Run, -1)); used != safe {
			p = append(p, fmt.Sprintf("jobs.publish %s names COSIGN_PRIVATE_KEY %d times, %d of them by env:// or docker -e", s.label(i), used, safe))
		}
	}
	if blob >= 0 {
		r := job.Steps[blob].Run
		if !strings.Contains(r, "--output-signature SHA256SUMS.sig SHA256SUMS") {
			p = append(p, "the checksum signing does not write SHA256SUMS.sig over SHA256SUMS")
		}
		if !strings.Contains(r, "verify-blob --key meta/cosign.pub") {
			p = append(p, "the checksum signing never verifies with the committed key, so a key that is not deploy/cosign.pub's would sign")
		}
	}
	if sign >= 0 {
		r := job.Steps[sign].Run
		after := strings.Index(r, imageSignCommand)
		if !strings.Contains(r[after:], signedAfter) {
			p = append(p, "the image signing never verifies each signature by digest with the committed key after signing")
		}
		for _, want := range []struct{ text, says string }{
			{`ref="ghcr.io/orkstack/$name@$digest"`, "names each image by its digest"},
			{`--yes "$ref"`, "signs the digest reference"},
			{`sha256-${digest#sha256:}.sig`, "reads the .sig tag OrkStack's verifier reads"},
			{dslSimpleSigning, "checks that the .sig manifest carries a simple-signing layer"},
			{dslSignatureAnnotation, "checks that the layer carries its signature"},
			{"done <meta/images.tsv", "signs every image the list names"},
		} {
			if !strings.Contains(r, want.text) {
				p = append(p, "the image signing never "+want.says+" ("+want.text+")")
			}
		}
		trap := strings.Index(r, `trap 'rm -f "$RUNNER_TEMP/auth.json"' EXIT`)
		if write := strings.Index(r, `>"$RUNNER_TEMP/auth.json"`); trap < 0 || write < 0 || trap > write {
			p = append(p, "the image signing writes auth.json without first trapping its removal on every exit")
		}
	}
	if release >= 0 && !strings.Contains(job.Steps[release].Run, "SHA256SUMS SHA256SUMS.sig meta/cosign.pub") {
		p = append(p, "the GitHub release does not attach SHA256SUMS.sig and cosign.pub beside SHA256SUMS")
	}
	return p
}

// The layout internal/dsl/signature.go reads, spelled as the publish check
// spells it.
const (
	dslSimpleSigning       = "application/vnd.dev.cosign.simplesigning.v1+json"
	dslSignatureAnnotation = "dev.cosignproject.cosign/signature"
)

func TestWorkflows_ReleaseSignsWhatItPublishes(t *testing.T) {
	expectNoProblems(t, "release.yml", releaseSigningProblems(loadWorkflow(t, "release.yml")))
	steps := func(w *wfFile) []wfStep { return w.Jobs["publish"].Steps }
	edit := func(w *wfFile, find, replace string) {
		for _, job := range w.Jobs {
			for i := range job.Steps {
				job.Steps[i].Run = strings.ReplaceAll(job.Steps[i].Run, find, replace)
			}
		}
	}
	at := func(w *wfFile, text string) int {
		return stepIndex(steps(w), func(s wfStep) bool { return strings.Contains(s.Run, text) })
	}
	swap := func(w *wfFile, a, b string) {
		s := steps(w)
		i, j := at(w, a), at(w, b)
		s[i], s[j] = s[j], s[i]
	}
	expectEachMutationRefused(t, "release.yml", releaseSigningProblems, []mutation{
		{"no release environment", func(w *wfFile) { w.Jobs["publish"].Environment = nil }},
		{"another environment", func(w *wfFile) { w.Jobs["publish"].Environment = map[string]any{"name": "staging"} }},
		{"no committed key required", func(w *wfFile) { edit(w, "[ -s deploy/cosign.pub ]", "true") }},
		{"no public key handed on", func(w *wfFile) { edit(w, "cp deploy/cosign.pub dist/meta/", "true ") }},
		{"no cosign pin handed on", func(w *wfFile) { edit(w, "cp deploy/cosign-image.txt dist/meta/", "true ") }},
		{"SHA256SUMS unsigned", func(w *wfFile) { edit(w, blobSignCommand, "true") }},
		{"SHA256SUMS signed after the push", func(w *wfFile) { swap(w, blobSignCommand, "copy --all --preserve-digests") }},
		{"the images unsigned", func(w *wfFile) { edit(w, imageSignCommand, "true") }},
		{"the images signed before the push", func(w *wfFile) { swap(w, imageSignCommand, "copy --all --preserve-digests") }},
		{"the release before the signing", func(w *wfFile) { swap(w, imageSignCommand, "gh release create") }},
		{"cosign's default bundle format", func(w *wfFile) { edit(w, "--new-bundle-format=false ", "") }},
		{"a transparency-log upload", func(w *wfFile) { edit(w, " --tlog-upload=false", "") }},
		{"a tag signed, not the digest", func(w *wfFile) {
			edit(w, `ref="ghcr.io/orkstack/$name@$digest"`, `ref="ghcr.io/orkstack/$name:${VERSION#v}"`)
		}},
		{"no verify after signing", func(w *wfFile) { edit(w, signedAfter, strings.TrimSuffix(signedAfter, "\n")+" || true\n") }},
		{"no .sig layout check", func(w *wfFile) { edit(w, dslSimpleSigning, "application/json") }},
		{"no verify-blob with the committed key", func(w *wfFile) { edit(w, "verify-blob --key meta/cosign.pub", "version") }},
		{"a missing key not refused", func(w *wfFile) { edit(w, keyCheck, "true ||") }},
		{"the key written to disk", func(w *wfFile) {
			edit(w, "set -euo pipefail\n", "set -euo pipefail\nprintf '%s' \"$COSIGN_PRIVATE_KEY\" >\"$RUNNER_TEMP/cosign.key\"\n")
		}},
		{"the key from a repository variable", func(w *wfFile) {
			steps(w)[at(w, imageSignCommand)].Env["COSIGN_PRIVATE_KEY"] = "${{ vars.COSIGN_PRIVATE_KEY }}"
		}},
		{"the push handed the key", func(w *wfFile) {
			steps(w)[at(w, "copy --all --preserve-digests")].Env["COSIGN_PRIVATE_KEY"] = "${{ secrets.COSIGN_PRIVATE_KEY }}"
		}},
		{"the signing credential not trapped", func(w *wfFile) {
			s := &steps(w)[at(w, imageSignCommand)]
			s.Run = strings.ReplaceAll(s.Run, `trap 'rm -f "$RUNNER_TEMP/auth.json"' EXIT`, ":")
		}},
		{"a release without the signature", func(w *wfFile) { edit(w, "SHA256SUMS SHA256SUMS.sig meta/cosign.pub", "SHA256SUMS") }},
	})

	// The key's two secrets are named only where publish signs: four
	// references in the whole file, at any level a job or step could set env.
	raw := workflowFile(t, "release.yml")
	secrets := regexp.MustCompile(`secrets\.[A-Za-z0-9_]+`).FindAllString(raw, -1)
	if strings.Join(secrets, " ") != "secrets.COSIGN_PRIVATE_KEY secrets.COSIGN_PASSWORD secrets.COSIGN_PRIVATE_KEY secrets.COSIGN_PASSWORD" {
		t.Errorf("release.yml names secrets %v; want the key and its password once in each of publish's two signing steps", secrets)
	}
}

// TestWorkflows_TheCosignPinIsTheRepositorysPin: the image that signs is one
// exact cosign release pinned by digest (deploy/cosign-image.txt); publish's
// check refuses a floating one; the release smoke reads the same pin and signs
// with the same command and flags publish does, then runs an Orkflow under
// require_signed_by; and a committed deploy/cosign.pub is a key the engine
// accepts as a signing key.
func TestWorkflows_TheCosignPinIsTheRepositorysPin(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "cosign-image.txt"))
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
	if !regexp.MustCompile(`^ghcr\.io/sigstore/cosign/cosign:v[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`).MatchString(pin) {
		t.Errorf("deploy/cosign-image.txt names %q, not ghcr.io/sigstore/cosign/cosign:vX.Y.Z@sha256:<64 hex>", pin)
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
	re, err := extractRegex(publish.Steps[at].Run, "cosign")
	if err != nil {
		t.Fatalf("publish's check: %v", err)
	}
	if !re.MatchString(pin) {
		t.Errorf("publish's check refuses the repository's own pin %q", pin)
	}
	for _, bad := range []string{
		"ghcr.io/sigstore/cosign/cosign:latest",
		"ghcr.io/sigstore/cosign/cosign:v3.1.4",
		"ghcr.io/sigstore/cosign/cosign:v3.1@sha256:" + strings.Repeat("a", 64),
		"evil.example/cosign:v3.1.4@sha256:" + strings.Repeat("a", 64),
	} {
		if re.MatchString(bad) {
			t.Errorf("publish's check accepts %q", bad)
		}
	}
	smoke, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "quickstart-smoke.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"deploy/cosign-image.txt", imageSignCommand, "--require-signed-by cosign:"} {
		if !strings.Contains(string(smoke), want) {
			t.Errorf("scripts/quickstart-smoke.sh never says %q: the smoke would not prove what publish signs", want)
		}
	}
	key, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "cosign.pub"))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Not committed yet: release.yml's validate refuses every dispatch
		// until it is (docs/PROOFS.md §10, "Set up release signing").
	case err != nil:
		t.Fatal(err)
	default:
		if _, err := task.ParseSigningKey(key); err != nil {
			t.Errorf("deploy/cosign.pub: %v", err)
		}
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

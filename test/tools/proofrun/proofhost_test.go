package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The proof host's setup is the part of a CI job that leaves the runner: it
// downloads, pulls and installs. Every one of those goes through
// .github/actions/proof-fetch, two attempts under `timeout`, each recorded in the
// setup log, so a stalled mirror or registry costs a retry, and one that
// stalls twice fails the step explicitly and is a setup-failed part the merger
// lets run again. These tests run proof-fetch's own script the way the runner
// does, against fakes, and hold proof-host to the rule.

// ---- proof-fetch's script ------------------------------------------------------

type fetchRun struct {
	code  int
	log   string
	calls string
	dest  string
}

// runFetch runs the "fetch" step of the proof-fetch action as the runner does
// (`bash --noprofile --norc -eo pipefail`), with fake docker, curl and gh first
// on PATH. env sets the step's inputs (ARTIFACT, PATTERN, IMAGE, URL, SHA256,
// DEST); fails says how many calls of each fake fail before one succeeds.
func runFetch(t *testing.T, env map[string]string, curlBody string, fails map[string]int) fetchRun {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	steps := compositeSteps(t, "proof-fetch")
	script := steps[0].Run
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	counts := filepath.Join(dir, "counts")
	if err := os.MkdirAll(counts, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := func(name, body string) {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\necho \""+name+" $*\" >>\"$CALLS\"\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	failing := `n=$(cat "$COUNTS/NAME" 2>/dev/null || echo 0); n=$((n + 1)); echo $n >"$COUNTS/NAME"
if [ "$n" -le "$FAILS_NAME" ]; then exit 22; fi
`
	fake("docker", strings.ReplaceAll(failing, "NAME", "docker"))
	fake("gh", strings.ReplaceAll(failing, "NAME", "gh"))
	fake("curl", strings.ReplaceAll(failing, "NAME", "curl")+`out=""
while [ $# -gt 0 ]; do
  case "$1" in --output) out="$2"; shift ;; esac
  shift
done
cat "$CURL_BODY" >"$out"
`)
	body := filepath.Join(dir, "curl-body")
	if err := os.WriteFile(body, []byte(curlBody), 0o644); err != nil {
		t.Fatal(err)
	}
	scriptFile := filepath.Join(dir, "fetch.sh")
	if err := os.WriteFile(scriptFile, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(dir, "ci", "setup.log")
	calls := filepath.Join(dir, "calls")
	cmd := exec.Command(bash, "--noprofile", "--norc", "-eo", "pipefail", scriptFile)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + dir, "CALLS=" + calls, "COUNTS=" + counts, "CURL_BODY=" + body,
		"GITHUB_RUN_ID=1", "GITHUB_REPOSITORY=o/r", "LOG=" + logFile,
		fmt.Sprintf("FAILS_docker=%d", fails["docker"]), fmt.Sprintf("FAILS_curl=%d", fails["curl"]), fmt.Sprintf("FAILS_gh=%d", fails["gh"]),
	}
	for _, k := range []string{"ARTIFACT", "PATTERN", "IMAGE", "URL", "SHA256", "DEST", "GH_TOKEN"} {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	read := func(p string) string { b, _ := os.ReadFile(p); return string(b) }
	_ = out
	return fetchRun{code: code, log: read(logFile), calls: read(calls), dest: env["DEST"]}
}

const (
	pinA = "registry.example/a@sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pinB = "registry.example/b:3@sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// The loop must survive the shell the runner starts a composite step with:
// `bash -e` ends a script at its first failed command, and the first failed
// attempt of a fetch is exactly what the second attempt is for.
func TestProofFetch_ASecondAttemptFollowsAFailedFirstUnderErrexit(t *testing.T) {
	r := runFetch(t, map[string]string{"IMAGE": pinA}, "", map[string]int{"docker": 1})
	if r.code != 0 {
		t.Fatalf("exit %d, want 0 after the second attempt; log:\n%s", r.code, r.log)
	}
	lines := strings.Split(strings.TrimSpace(r.log), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "fetch image "+pinA+": attempt 1 failed, exit 22 after ") ||
		!strings.HasPrefix(lines[1], "fetch image "+pinA+": attempt 2 ok in ") || !strings.HasSuffix(lines[1], "(limit 360s)") {
		t.Errorf("setup log =\n%s", r.log)
	}
	for _, l := range lines {
		if !setupAttemptRE.MatchString(l) {
			t.Errorf("the merger's setupAttemptRE does not match %q", l)
		}
	}
}

func TestProofFetch_GivesUpAfterTwoAttemptsAndSaysWhatTheMergerDoesWithIt(t *testing.T) {
	r := runFetch(t, map[string]string{"IMAGE": pinA}, "", map[string]int{"docker": 9})
	if r.code != 1 {
		t.Fatalf("exit %d, want 1", r.code)
	}
	if strings.Count(r.calls, "docker pull") != 2 {
		t.Errorf("docker was called\n%s\nwant two pulls", r.calls)
	}
	want := "fetch image " + pinA + ": gave up after 2 attempts; a setup failure in a fetch step is a setup-failed part (docs/PROOFS.md §8)"
	if !strings.Contains(r.log, want) {
		t.Errorf("setup log =\n%s\nwant %q", r.log, want)
	}
}

func TestProofFetch_PullsEveryImageOfAListPinnedByDigest(t *testing.T) {
	r := runFetch(t, map[string]string{"IMAGE": pinA + "\n  " + pinB + "\n"}, "", nil)
	if r.code != 0 {
		t.Fatalf("exit %d; log:\n%s", r.code, r.log)
	}
	if r.calls != "docker pull "+pinA+"\ndocker pull "+pinB+"\n" {
		t.Errorf("calls =\n%s", r.calls)
	}
	if !strings.HasPrefix(r.log, "fetch image "+pinA+" "+pinB+": attempt 1 ok in ") {
		t.Errorf("the setup log names the references on one line:\n%s", r.log)
	}
	// One failing pull of the list is an attempt that failed, and the second
	// attempt pulls the list again.
	r = runFetch(t, map[string]string{"IMAGE": pinA + " " + pinB}, "", map[string]int{"docker": 1})
	if r.code != 0 || strings.Count(r.log, "attempt") != 2 {
		t.Errorf("exit %d; log:\n%s", r.code, r.log)
	}
}

func TestProofFetch_RefusesAnImageThatIsNotPinnedByDigest(t *testing.T) {
	for _, image := range []string{
		"registry:3", "registry:3@sha256:abc", pinA + " registry:3", "registry@sha256:" + strings.Repeat("A", 64),
		"@sha256:" + strings.Repeat("a", 64), "a b@sha256:" + strings.Repeat("a", 64) + " *",
	} {
		r := runFetch(t, map[string]string{"IMAGE": image}, "", nil)
		if r.code != 2 || r.calls != "" || r.log != "" {
			t.Errorf("IMAGE=%q: exit %d, calls %q, log %q; want exit 2 and nothing pulled or logged", image, r.code, r.calls, r.log)
		}
	}
}

func TestProofFetch_DownloadsAFileOnlyIfItHashesToItsPin(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "bin", "tool")
	url := "https://github.com/o/r/releases/download/v1/tool"
	env := func(sum string) map[string]string {
		return map[string]string{"URL": url, "SHA256": sum, "DEST": dest}
	}
	r := runFetch(t, env(sha("payload")), "payload", nil)
	if r.code != 0 {
		t.Fatalf("exit %d; log:\n%s", r.code, r.log)
	}
	if b, err := os.ReadFile(dest); err != nil || string(b) != "payload" {
		t.Errorf("the file = %q, %v", b, err)
	}
	if !strings.HasPrefix(r.log, "fetch url "+url+": attempt 1 ok in ") || !setupAttemptRE.MatchString(strings.TrimSpace(r.log)) {
		t.Errorf("setup log =\n%s", r.log)
	}
	for _, want := range []string{"--fail", "--location", "--proto =https", "--proto-redir =https", "--output " + dest, url} {
		if !strings.Contains(r.calls, want) {
			t.Errorf("curl was called without %q:\n%s", want, r.calls)
		}
	}

	// The wrong bytes are a failed attempt twice over, and no file is kept.
	dest2 := filepath.Join(t.TempDir(), "tool")
	r = runFetch(t, map[string]string{"URL": url, "SHA256": sha("payload"), "DEST": dest2}, "tampered", nil)
	if r.code != 1 || strings.Count(r.log, "failed, exit 1") != 2 || !strings.Contains(r.log, "gave up after 2 attempts") {
		t.Errorf("a download that does not hash to the pin: exit %d; log:\n%s", r.code, r.log)
	}
	if _, err := os.Stat(dest2); err == nil {
		t.Error("a file that does not hash to its pin was kept")
	}

	// A download that stalls once is retried.
	dest3 := filepath.Join(t.TempDir(), "tool")
	r = runFetch(t, map[string]string{"URL": url, "SHA256": sha("payload"), "DEST": dest3}, "payload", map[string]int{"curl": 1})
	if r.code != 0 || !strings.Contains(r.log, "attempt 1 failed, exit 22") || !strings.Contains(r.log, "attempt 2 ok") {
		t.Errorf("a first download that failed: exit %d; log:\n%s", r.code, r.log)
	}
}

func TestProofFetch_RefusesAUrlItCannotTrust(t *testing.T) {
	good := "https://github.com/o/r/releases/download/v1/tool"
	dest := filepath.Join(t.TempDir(), "tool")
	for _, c := range []struct {
		name string
		env  map[string]string
	}{
		{"http", map[string]string{"URL": "http://example.com/tool", "SHA256": sha("x"), "DEST": dest}},
		{"no scheme", map[string]string{"URL": "example.com/tool", "SHA256": sha("x"), "DEST": dest}},
		{"a space", map[string]string{"URL": good + " --output /etc/passwd", "SHA256": sha("x"), "DEST": dest}},
		{"no sha256", map[string]string{"URL": good, "DEST": dest}},
		{"a short sha256", map[string]string{"URL": good, "SHA256": sha("x")[:63], "DEST": dest}},
		{"an uppercase sha256", map[string]string{"URL": good, "SHA256": strings.ToUpper(sha("x")), "DEST": dest}},
		{"no path", map[string]string{"URL": good, "SHA256": sha("x")}},
		{"a url and an image", map[string]string{"URL": good, "SHA256": sha("x"), "DEST": dest, "IMAGE": pinA}},
		{"a url and an artifact", map[string]string{"URL": good, "SHA256": sha("x"), "DEST": dest, "ARTIFACT": "plan-a1"}},
		{"nothing", map[string]string{}},
	} {
		r := runFetch(t, c.env, "x", nil)
		if r.code != 2 || r.calls != "" || r.log != "" {
			t.Errorf("%s: exit %d, calls %q, log %q; want exit 2 and nothing fetched or logged", c.name, r.code, r.calls, r.log)
		}
	}
}

func TestProofFetch_AnArtifactStillRetriesUnderErrexit(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "plan")
	r := runFetch(t, map[string]string{"ARTIFACT": "plan-a1", "DEST": dest, "GH_TOKEN": "t"}, "", map[string]int{"gh": 1})
	if r.code != 0 || !strings.Contains(r.log, "fetch artifact plan-a1: attempt 1 failed, exit 22") || !strings.Contains(r.log, "attempt 2 ok") {
		t.Errorf("exit %d; log:\n%s", r.code, r.log)
	}
	if strings.Count(r.calls, "gh run download") != 2 {
		t.Errorf("calls =\n%s", r.calls)
	}
}

// ---- proof-host ----------------------------------------------------------------

// The cosign CLI the image-policy interop test was proven with, by release and
// by the SHA-256 of its linux/amd64 binary as the release's
// cosign_checksums.txt lists it. Moving it is its own change with its own
// proof: take the new line from the release's checksums, change both here and
// in .github/actions/proof-host/action.yml.
const (
	cosignURL    = "https://github.com/sigstore/cosign/releases/download/v2.6.5/cosign-linux-amd64"
	cosignSHA256 = "c3b4f5410e608af03a5eb0aaac84a4313d8da131248e08ff1759ac70c79d1644"
)

func cloneSteps(steps []wfStep) []wfStep {
	out := make([]wfStep, len(steps))
	copy(out, steps)
	for i := range out {
		if out[i].With != nil {
			w := map[string]any{}
			for k, v := range out[i].With {
				w[k] = v
			}
			out[i].With = w
		}
	}
	return out
}

func namedStepAt(steps []wfStep, name string) int {
	for i, s := range steps {
		if s.Name == name {
			return i
		}
	}
	return -1
}

// proofHostProblems holds the proof-host action's steps to the rule: nothing
// leaves the runner except through proof-fetch.
func proofHostProblems(steps []wfStep) []string {
	var p []string
	for i, s := range steps {
		if s.Uses != "" && !strings.HasPrefix(s.Uses, "actions/cache@") && s.Uses != proofFetchAction {
			p = append(p, fmt.Sprintf("%s uses %q, a download or an install proof-fetch does not bound", s.label(i), s.Uses))
		}
		for _, line := range strings.Split(s.Run, "\n") {
			line = strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(line, "#"):
			case strings.Contains(line, "docker pull"):
				p = append(p, fmt.Sprintf("%s pulls with docker pull; an image goes through proof-fetch: %s", s.label(i), line))
			case strings.Contains(line, "docker run") && !strings.Contains(line, "timeout -k 10 "):
				p = append(p, fmt.Sprintf("%s runs a container that may pull, with no timeout: %s", s.label(i), line))
			case strings.Contains(line, "docker create") && !strings.Contains(line, "--pull never"):
				p = append(p, fmt.Sprintf("%s creates a container that may pull: %s", s.label(i), line))
			case strings.Contains(line, "docker compose") && strings.Contains(line, " up ") && !strings.Contains(line, "--pull never"):
				p = append(p, fmt.Sprintf("%s starts the stack and may pull, with no timeout: %s", s.label(i), line))
			case strings.Contains(line, "curl ") && !strings.Contains(line, "127.0.0.1") && !strings.Contains(line, "ORK_LOCAL_REGISTRY"):
				p = append(p, fmt.Sprintf("%s downloads with curl; a file goes through proof-fetch: %s", s.label(i), line))
			case strings.Contains(line, "apt-get") || strings.Contains(line, "apt install") || strings.Contains(line, "pip install") || strings.Contains(line, "npm install"):
				p = append(p, fmt.Sprintf("%s installs from a mirror: %s", s.label(i), line))
			}
		}
	}

	// cosign: one pinned download, then the install of exactly that file.
	fetch, install := namedStepAt(steps, "fetch cosign"), namedStepAt(steps, "cosign")
	switch {
	case fetch < 0 || install < 0 || fetch >= install:
		p = append(p, "the action has no `fetch cosign` step followed by a `cosign` step")
	default:
		f, in := steps[fetch], steps[install]
		if f.Uses != proofFetchAction {
			p = append(p, "`fetch cosign` is not a proof-fetch step")
		}
		if got := withString(f, "url"); got != cosignURL {
			p = append(p, fmt.Sprintf("`fetch cosign` downloads %q, want %q", got, cosignURL))
		}
		if got := withString(f, "sha256"); got != cosignSHA256 {
			p = append(p, fmt.Sprintf("`fetch cosign` pins sha256 %q, want %q", got, cosignSHA256))
		}
		if got := withString(f, "path"); got != "${{ runner.temp }}/cosign/cosign" {
			p = append(p, fmt.Sprintf("`fetch cosign` saves to %q", got))
		}
		for _, s := range []wfStep{f, in} {
			if normalizeIf(s.If) != "inputs.cosign == 'true'" {
				p = append(p, fmt.Sprintf("%q runs under %q, want only when cosign is asked for", s.Name, s.If))
			}
		}
		if !strings.Contains(in.Run, `sudo install -m 0755 "$RUNNER_TEMP/cosign/cosign" /usr/local/bin/cosign`) || !strings.Contains(in.Run, "cosign version") {
			p = append(p, "`cosign` does not install the downloaded file to /usr/local/bin/cosign and check its version")
		}
	}

	// The stack images: the ones the cache did not load are pulled through
	// proof-fetch, before the stack starts, and saved without a pull.
	names := []string{"Load the stack images", "Missing stack images", "fetch stack images", "SeaweedFS and registry", "Save the stack images"}
	at := make([]int, len(names))
	for i, n := range names {
		at[i] = namedStepAt(steps, n)
		if at[i] < 0 {
			p = append(p, fmt.Sprintf("the action has no %q step", n))
		}
	}
	for i := 1; i < len(at); i++ {
		if at[i] >= 0 && at[i-1] >= 0 && at[i] <= at[i-1] {
			p = append(p, fmt.Sprintf("%q runs before %q", names[i], names[i-1]))
		}
	}
	if at[1] >= 0 && at[2] >= 0 {
		missing, pull := steps[at[1]], steps[at[2]]
		if missing.ID != "missing" || !strings.Contains(missing.Run, "config --images temporal seaweedfs registry") || !strings.Contains(missing.Run, "docker image inspect") ||
			!strings.Contains(missing.Run, `>>"$GITHUB_OUTPUT"`) {
			p = append(p, "`Missing stack images` does not list the images of temporal, seaweedfs and registry that the daemon lacks")
		}
		if pull.Uses != proofFetchAction || withString(pull, "image") != "${{ steps.missing.outputs.images }}" {
			p = append(p, "`fetch stack images` is not a proof-fetch of the missing images")
		}
		if normalizeIf(pull.If) != "steps.missing.outputs.images != ''" {
			p = append(p, fmt.Sprintf("`fetch stack images` runs under %q, want only when an image is missing", pull.If))
		}
	}
	if at[3] >= 0 && !strings.Contains(steps[at[3]].Run, "up -d --wait --pull never seaweedfs registry") {
		p = append(p, "`SeaweedFS and registry` does not start the stack with --pull never")
	}
	if at[4] >= 0 && !strings.Contains(steps[at[4]].Run, "config --images temporal seaweedfs registry") {
		p = append(p, "`Save the stack images` saves other images than the ones the stack starts")
	}
	return p
}

func TestProofHost_NothingLeavesTheRunnerExceptThroughProofFetch(t *testing.T) {
	steps := compositeSteps(t, "proof-host")
	expectNoProblems(t, "the proof-host action", proofHostProblems(steps))
	mut := func(name string, change func(steps []wfStep)) func(t *testing.T) {
		return func(t *testing.T) {
			s := cloneSteps(steps)
			change(s)
			if p := proofHostProblems(s); len(p) == 0 {
				t.Errorf("the rule accepts %s", name)
			}
		}
	}
	by := func(steps []wfStep, name string) *wfStep {
		i := namedStepAt(steps, name)
		if i < 0 {
			t.Fatalf("no step %q", name)
		}
		return &steps[i]
	}
	for name, change := range map[string]func(s []wfStep){
		"the cosign installer action back": func(s []wfStep) {
			c := by(s, "cosign")
			c.Uses, c.Run = "sigstore/cosign-installer@v4.1.2", ""
		},
		"the cosign download unpinned": func(s []wfStep) { by(s, "fetch cosign").With["sha256"] = "" },
		"the cosign download from another release": func(s []wfStep) {
			by(s, "fetch cosign").With["url"] = strings.Replace(cosignURL, "v2.6.5", "v2.6.6", 1)
		},
		"the cosign hash of another file":           func(s []wfStep) { by(s, "fetch cosign").With["sha256"] = strings.Repeat("0", 64) },
		"cosign installed whatever the input says":  func(s []wfStep) { by(s, "cosign").If = "" },
		"cosign downloaded whatever the input says": func(s []wfStep) { by(s, "fetch cosign").If = "" },
		"cosign installed from elsewhere": func(s []wfStep) {
			c := by(s, "cosign")
			c.Run = strings.Replace(c.Run, `"$RUNNER_TEMP/cosign/cosign"`, "/tmp/cosign", 1)
		},
		"cosign downloaded with curl": func(s []wfStep) {
			f := by(s, "fetch cosign")
			f.Uses, f.With, f.Run = "", nil, "curl -fsSL "+cosignURL+" -o /usr/local/bin/cosign"
		},
		"the stack images pulled in the save step": func(s []wfStep) {
			by(s, "Save the stack images").Run += "\ndocker pull --quiet \"$image\""
		},
		"the stack images pulled by compose": func(s []wfStep) {
			c := by(s, "SeaweedFS and registry")
			c.Run = strings.Replace(c.Run, " --pull never", "", 1)
		},
		"the stack images never fetched": func(s []wfStep) {
			f := by(s, "fetch stack images")
			f.Uses, f.With, f.Run = "", nil, "true"
		},
		"the stack images fetched for ever": func(s []wfStep) { by(s, "fetch stack images").If = "" },
		"the stack images fetched of another list": func(s []wfStep) {
			by(s, "fetch stack images").With["image"] = "registry:3"
		},
		"the stack images fetched after the stack starts": func(s []wfStep) {
			i, j := namedStepAt(s, "fetch stack images"), namedStepAt(s, "SeaweedFS and registry")
			s[i], s[j] = s[j], s[i]
		},
		"the missing list not published": func(s []wfStep) {
			m := by(s, "Missing stack images")
			m.Run = strings.Replace(m.Run, `>>"$GITHUB_OUTPUT"`, "", 1)
		},
		"the alpine container unbounded": func(s []wfStep) {
			h := by(s, "host.docker.internal")
			h.Run = strings.Replace(h.Run, "timeout -k 10 360 docker run", "docker run", 1)
		},
		"the temporal CLI's container may pull": func(s []wfStep) {
			c := by(s, "temporal CLI")
			c.Run = strings.Replace(c.Run, "docker create --pull never", "docker create", 1)
		},
		"a file downloaded with curl": func(s []wfStep) {
			by(s, "Disk").Run += "\ncurl -fsSL https://example.com/x -o /tmp/x"
		},
		"a package installed from a mirror": func(s []wfStep) { by(s, "Disk").Run += "\nsudo apt-get install -y something" },
	} {
		t.Run(name, mut(name, change))
	}
}

func TestProofHost_CosignIsPinnedByReleaseAndByHash(t *testing.T) {
	if !regexp.MustCompile(`^https://github\.com/sigstore/cosign/releases/download/v2\.6\.5/cosign-linux-amd64$`).MatchString(cosignURL) {
		t.Errorf("the cosign URL %q is not release v2.6.5's linux/amd64 binary", cosignURL)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(cosignSHA256) {
		t.Errorf("the cosign hash %q is not a SHA-256", cosignSHA256)
	}
	// docs/PROOFS.md says which cosign the proofs run; it names this release.
	proofs := strings.Join(strings.Fields(readRepoFileForPins(t, "docs/PROOFS.md")), " ")
	if !strings.Contains(proofs, "cosign v2.6.5 is downloaded through it") {
		t.Error("docs/PROOFS.md no longer names cosign v2.6.5, the release proof-host downloads")
	}
}

// ---- the development stack -----------------------------------------------------

// proof-fetch pulls an image pinned by digest and refuses any other, so every
// image of the stack proof-host pulls must be named by its digest in the
// compose file: a floating tag is what AGENTS.md §11.1 forbids at runtime.
func TestDevCompose_TheStackImagesProofHostPullsArePinnedByDigest(t *testing.T) {
	var doc struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFileForPins(t, "deploy/docker-compose.yml")), &doc); err != nil {
		t.Fatal(err)
	}
	digested := regexp.MustCompile(`^[^\s@$]+@sha256:[0-9a-f]{64}$`)
	for _, name := range []string{"temporal", "seaweedfs", "registry"} {
		svc, ok := doc.Services[name]
		if !ok {
			t.Errorf("deploy/docker-compose.yml has no %s service", name)
			continue
		}
		if !digested.MatchString(svc.Image) {
			t.Errorf("service %s runs %q, which proof-fetch would refuse: pin it by digest", name, svc.Image)
		}
	}
	if !digested.MatchString("registry:3@sha256:" + strings.Repeat("a", 64)) {
		t.Error("the pin pattern refuses a tag with a digest")
	}
	for _, floating := range []string{"registry:3", "registry", "registry:3@sha256:" + strings.Repeat("a", 63), "registry:3@sha256:" + strings.Repeat("A", 64)} {
		if digested.MatchString(floating) {
			t.Errorf("the pin pattern accepts %q", floating)
		}
	}
}

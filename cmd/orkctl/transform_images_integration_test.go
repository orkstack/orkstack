//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The transform-image scenarios (docs/ACTIVITY_IMAGES.md):
// a real `ork` on S3, a private Temporal namespace and channel, a real Orker
// that runs the activity-json, activity-git and activity-template images in
// real containers, and — for git — a TLS git server built from git's own
// http-backend (gitserver_integration_test.go). Every run is read back
// through the BUILT orkctl, and each scenario writes its transcript to
// <ORK_PROOF_DIR>/orkctl/transform-images-<X>.txt.

// transformStack is the timing stack's engine plus an Orker started with the
// environment the scenarios' secrets name, and the git server when one is
// needed.
type transformStack struct {
	*timingStack
	git *tlsGitServer
}

// transformJSONSecret is a value the json image must refuse to mount.
const transformJSONSecret = "transform-json-secret-value"

func startTransformStack(ctx context.Context, t *testing.T, withGit bool) *transformStack {
	t.Helper()
	var git *tlsGitServer
	extra := []string{"ORK_ORKER_MAX_PINS=1", "TRANSFORM_JSON_SECRET=" + transformJSONSecret}
	if withGit {
		git = startTLSGitServer(t)
		for _, url := range git.HealthURLs() {
			requireContainerCanReachTLS(ctx, t, url)
		}
		extra = append(extra, "TRANSFORM_GIT_TOKEN="+git.Token(), "TRANSFORM_GIT_CA="+git.CA())
	}
	s := newTimingStack(ctx, t, 1)
	startOrkerWithEnv(ctx, t, s.temporal, s.orkerPath, s.address, s.namespace, s.queue, extra...)
	return &transformStack{timingStack: s, git: git}
}

func transformName(scenario string) string {
	return fmt.Sprintf("tx-%s-%d-%d", strings.ToLower(scenario), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// note records a line of evidence in the transcript.
func (x *transformStack) note(format string, args ...any) {
	fmt.Fprintf(&x.transcript, "# "+format+"\n\n", args...)
}

// tokenForms is the minted token in both forms it could leak in: as itself,
// and as the base64 of the Basic credential a header carries.
func tokenForms(token string) []string {
	return []string{token, base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))}
}

// finish writes the transcript, after checking it carries no form of the
// token: the transcript is evidence, and evidence must not leak.
func (x *transformStack) finish(t *testing.T, scenario string) {
	t.Helper()
	body := x.transcript.String()
	if x.git != nil {
		for _, form := range tokenForms(x.git.originalToken()) {
			if strings.Contains(body, form) {
				t.Errorf("the %s transcript carries the git token", scenario)
				body = strings.ReplaceAll(body, form, "<TOKEN REDACTED BY THE TEST>")
			}
		}
	}
	proof.Write(t, "orkctl", "transform-images-"+scenario+".txt", []byte(body))
}

// applyCLI applies a document through the built orkctl and returns what
// apply printed (its warnings included).
func (x *transformStack) applyCLI(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orkflow.ork.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write document: %v", err)
	}
	return x.mustCLI(0, "apply", path)
}

// runToEnd submits name with a JSON input and waits for a terminal status.
func (x *transformStack) runToEnd(ctx context.Context, t *testing.T, name, input string) *orkstackv1.Run {
	t.Helper()
	args := []string{}
	if input != "" {
		args = append(args, "--input-json", input)
	}
	runID := x.submit(name, args...)
	return awaitTerminalOnB(ctx, t, x.replicas[0], runID)
}

func requireRunStatus(t *testing.T, run *orkstackv1.Run, want orkstackv1.Status) {
	t.Helper()
	if run.GetStatus() != want {
		var tasks []string
		for _, tr := range run.GetTaskRuns() {
			tasks = append(tasks, fmt.Sprintf("%s=%s(%s: %s)", tr.GetTaskId(), tr.GetStatus(), tr.GetError().GetCode(), tr.GetError().GetMessage()))
		}
		t.Fatalf("run %s = %s, want %s: %v; tasks %s", run.GetId(), run.GetStatus(), want, run.GetError(), strings.Join(tasks, ", "))
	}
}

func taskRunOf(t *testing.T, run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	t.Helper()
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	t.Fatalf("run %s has no task %q", run.GetId(), id)
	return nil
}

func taskOutput(t *testing.T, run *orkstackv1.Run, id string) map[string]any {
	t.Helper()
	return taskRunOf(t, run, id).GetOutput().GetData().AsMap()
}

func requireTaskStatus(t *testing.T, run *orkstackv1.Run, id string, want orkstackv1.Status) {
	t.Helper()
	if got := taskRunOf(t, run, id).GetStatus(); got != want {
		t.Fatalf("run %s task %s = %s, want %s (%v)", run.GetId(), id, got, want, taskRunOf(t, run, id).GetError())
	}
}

func requireCacheOutcome(t *testing.T, run *orkstackv1.Run, id string, want orkstackv1.CacheOutcome) {
	t.Helper()
	if got := taskRunOf(t, run, id).GetCache().GetOutcome(); got != want {
		t.Fatalf("run %s task %s cache = %s, want %s", run.GetId(), id, got, want)
	}
}

// taskLogs is `orkctl run logs <run> --task <task> [--attempt n]`, both of
// the CLI's streams.
func (x *transformStack) taskLogs(runID, task string, attempt int) string {
	args := []string{"run", "logs", runID, "--task", task}
	if attempt > 0 {
		args = append(args, "--attempt", fmt.Sprint(attempt))
	}
	stdout, stderr, _ := x.cli(0, args...)
	return stdout + stderr
}

func (x *transformStack) download(t *testing.T, runID, task, name string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	x.mustCLI(0, "run", "artifact", "download", runID, "--task", task, "--kind", "output", "--name", name, "--output", path)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded %s: %v", name, err)
	}
	return body
}

func (x *transformStack) attempts(t *testing.T, runID, task string) []*orkstackv1.TaskAttempt {
	t.Helper()
	var resp orkstackv1.ListTaskAttemptsResponse
	if err := api.UnmarshalJSON([]byte(x.mustCLI(0, "--json", "run", "attempts", runID, "--task", task)), &resp); err != nil {
		t.Fatalf("decode run attempts --json: %v", err)
	}
	return resp.GetAttempts()
}

// ============================================================== json

func x1Document(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
inputs:
  env: {type: string, required: true}
  services: {type: array, required: true}
  overrides: {type: object, required: true}
tasks:
  - id: prepare
    uses: json
    needs: []
    network: none
    cache: {enabled: true}
    with:
      op: query
      src: run.json
      filter: '[.services[] | {name, replicas: (.replicas * 2)}]'
  - id: line
    uses: json
    needs: [prepare]
    network: none
    each:
      over: '{{ .tasks.prepare.output.result }}'
      max_items: 10
    with:
      op: query
      src: task.json
      filter: '.each.item | "\(.name)=\(.replicas)"'
  - id: collect
    uses: json
    needs: [line]
    network: none
    with: {op: query, gather: line, filter: 'map(.result) | join(",")'}
  - id: defaults
    uses: json
    needs: []
    network: none
    with: {op: query, nullInput: true, filter: '{tier: {replicas: 1, size: "s"}}'}
  - id: overrides
    uses: json
    needs: []
    network: none
    with: {op: query, src: run.json, filter: .overrides}
  - id: merged
    uses: json
    needs: [defaults, overrides]
    network: none
    with: {op: merge, srcs: [upstream/defaults/values.json, upstream/overrides/values.json]}
  - id: decide
    uses: json
    needs: []
    network: none
    with:
      op: query
      nullInput: true
      vars: {env: '{{ .inputs.env }}'}
      filter: 'if $vars.env == "prod" then "canary" else "direct" end'
  - id: canary
    uses: json
    needs: [decide]
    network: none
    condition: '{{ eq .tasks.decide.output.result "canary" }}'
    with: {op: query, nullInput: true, filter: '"ran"'}
  - id: direct
    uses: json
    needs: [decide]
    network: none
    condition: '{{ eq .tasks.decide.output.result "direct" }}'
    with: {op: query, nullInput: true, filter: '"ran"'}
  - id: big
    uses: json
    needs: []
    network: none
    with: {op: query, nullInput: true, filter: '{id: 12345678901234567890, n: 1000000}'}
  - id: exact
    uses: json
    needs: [big]
    network: none
    with: {op: query, src: upstream/big/values.json, filter: .result.id, as: id.json}
  - id: templated
    uses: json
    needs: [big]
    network: none
    with:
      op: query
      nullInput: true
      vars: {id: '{{ .tasks.big.output.result.id }}', num: '{{ .tasks.big.output.result.n }}'}
      filter: '[$vars.id, $vars.num, ($vars.num | tonumber | floor)]'
      as: templated.json
`, name)
}

const x1Input = `{"env":"prod","services":[{"name":"api","replicas":2},{"name":"web","replicas":1}],"overrides":{"tier":{"replicas":3}}}`

func TestIntegration_TransformImages_JSONFanOutGatherDecide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, false)
	defer x.finish(t, "X1")
	name := transformName("x1")
	x.applyCLI(t, x1Document(name))

	run1 := x.runToEnd(ctx, t, name, x1Input)
	requireRunStatus(t, run1, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := taskOutput(t, run1, "collect")["result"]; got != "api=4,web=2" {
		t.Fatalf("collect result = %#v, want \"api=4,web=2\"", got)
	}
	wantMerged := map[string]any{"result": map[string]any{"tier": map[string]any{"replicas": 3.0, "size": "s"}}}
	if got := taskOutput(t, run1, "merged")["result"]; !reflect.DeepEqual(got, wantMerged) {
		t.Fatalf("merged result = %#v, want %#v", got, wantMerged)
	}
	requireTaskStatus(t, run1, "canary", orkstackv1.Status_STATUS_SUCCEEDED)
	requireTaskStatus(t, run1, "direct", orkstackv1.Status_STATUS_SKIPPED)
	if got := string(x.download(t, run1.GetId(), "exact", "id.json")); got != "12345678901234567890\n" {
		t.Fatalf("id.json = %q: a file must carry the literal through the Orker", got)
	}
	// The README's documented limit: a number through a template is Go's
	// spelling. A future Orker that decodes upstream values exactly fails this
	// line, and updates the json README's Limits in the same change.
	if got := string(x.download(t, run1.GetId(), "templated", "templated.json")); got != "[\"1.2345678901234567e+19\",\"1e+06\",1000000]\n" {
		t.Fatalf("templated.json = %q", got)
	}
	x.note("run 1: collect=api=4,web=2 canary=SUCCEEDED direct=SKIPPED id.json exact, templated.json in Go's spelling")

	run2 := x.runToEnd(ctx, t, name, x1Input)
	requireRunStatus(t, run2, orkstackv1.Status_STATUS_SUCCEEDED)
	requireCacheOutcome(t, run2, "prepare", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT)
	if got := x.mustCLI(0, "run", "get", run2.GetId()); !strings.Contains(got, "SUCCEEDED (cached)") {
		t.Fatalf("run 2's run get lacks SUCCEEDED (cached):\n%s", got)
	}

	run3 := x.runToEnd(ctx, t, name, strings.Replace(x1Input, `"env":"prod"`, `"env":"dev"`, 1))
	requireRunStatus(t, run3, orkstackv1.Status_STATUS_SUCCEEDED)
	requireCacheOutcome(t, run3, "prepare", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS)
	requireTaskStatus(t, run3, "direct", orkstackv1.Status_STATUS_SUCCEEDED)
	requireTaskStatus(t, run3, "canary", orkstackv1.Status_STATUS_SKIPPED)
	x.note("run 2: prepare cache=HIT; run 3 (env dev): prepare cache=MISS direct=SUCCEEDED canary=SKIPPED")
}

// ============================================================== git

func gitSecrets() string {
	return `    secrets:
      - name: git-token
        source: env:TRANSFORM_GIT_TOKEN
      - name: git-ca
        source: env:TRANSFORM_GIT_CA
`
}

func x2Document(name, url string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %[1]s
version: "0.1.0"
inputs:
  env: {type: string, required: true}
tasks:
  - id: resolve
    uses: git
%[3]s    with: {op: resolve, url: "%[2]s", ref: main, tokenSecret: git-token, caFile: secrets/git-ca}
  - id: clone
    uses: git
    needs: [resolve]
%[3]s    with: {op: clone, url: "%[2]s", ref: '{{ .tasks.resolve.output.commit }}', tokenSecret: git-token, caFile: secrets/git-ca}
  - id: tmpl
    uses: file
    needs: [clone]
    network: none
    cache: {enabled: true, ttl_seconds: 86400}
    with: {op: untar, src: '{{ artifact "clone/src.tar.gz" }}', member: templates/app.conf.tmpl}
  - id: defaults
    uses: file
    needs: [clone]
    network: none
    cache: {enabled: true, ttl_seconds: 86400}
    with: {op: untar, src: '{{ artifact "clone/src.tar.gz" }}', member: defaults.yaml}
  - id: render
    uses: template
    needs: [tmpl, defaults]
    network: none
    cache: {enabled: true, ttl_seconds: 86400}
    with:
      op: render
      src: '{{ artifact "tmpl/app.conf.tmpl" }}'
      datasources: {defaults: '{{ artifact "defaults/defaults.yaml" }}'}
      values: {env: '{{ .inputs.env }}'}
      as: app.conf
`, name, url, gitSecrets())
}

const x2Input = `{"env":"prod"}`
const x2AppConf = "env=prod\nport=8080\n"

func TestIntegration_TransformImages_GitResolveCloneRenderCached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X2")
	name := transformName("x2")
	applied := x.applyCLI(t, x2Document(name, x.git.URL("private/app.git")))
	if strings.Contains(applied, ".secrets") {
		t.Fatalf("apply warned at a secrets field; the document follows rule 2 (no cached task mounts a secret):\n%s", applied)
	}

	// Run 1.
	before := x.git.AuthenticatedRequests("private/app.git")
	run1 := x.runToEnd(ctx, t, name, x2Input)
	requireRunStatus(t, run1, orkstackv1.Status_STATUS_SUCCEEDED)
	mainSHA := x.git.Commit("private/app.git", "refs/heads/main")
	resolved := taskOutput(t, run1, "resolve")
	if resolved["commit"] != mainSHA || resolved["ref"] != "refs/heads/main" {
		t.Fatalf("resolve = %v, want commit %s ref refs/heads/main (never feature/main)", resolved, mainSHA)
	}
	if got := string(x.download(t, run1.GetId(), "render", "app.conf")); got != x2AppConf {
		t.Fatalf("app.conf = %q, want %q", got, x2AppConf)
	}
	tar1 := x.download(t, run1.GetId(), "clone", "src.tar.gz")
	sha1 := fmt.Sprint(taskOutput(t, run1, "clone")["sha256"])
	if sha256Hex(tar1) != sha1 {
		t.Fatalf("clone's values sha256 %s does not match the downloaded tarball %s", sha1, sha256Hex(tar1))
	}
	run1Requests := x.git.AuthenticatedRequests("private/app.git") - before
	if run1Requests <= 0 {
		t.Fatalf("run 1 made no authenticated request: the token was never exercised")
	}

	// Run 2, same input — the uncached comparison.
	before = x.git.AuthenticatedRequests("private/app.git")
	run2 := x.runToEnd(ctx, t, name, x2Input)
	requireRunStatus(t, run2, orkstackv1.Status_STATUS_SUCCEEDED)
	for _, id := range []string{"resolve", "clone"} {
		if cache := taskRunOf(t, run2, id).GetCache(); cache != nil {
			t.Fatalf("run 2's %s carries a cache result %v; a task with no cache: never consults a memo", id, cache)
		}
	}
	attempts := x.attempts(t, run2.GetId(), "clone")
	if len(attempts) != 1 {
		t.Fatalf("run 2's clone has %d attempts, want 1", len(attempts))
	}
	attempt := attempts[0]
	var phases []string
	for _, span := range attempt.GetPhases() {
		phases = append(phases, span.GetPhase())
	}
	if attempt.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED || attempt.GetCommitAdopted() ||
		!containsString(phases, "run") || containsString(phases, "memo") || attempt.GetImage().GetPlatformDigest() == "" {
		t.Fatalf("run 2's clone attempt: outcome %s adopted %v phases %v platform %q — want a container that ran in a fresh workspace",
			attempt.GetOutcome(), attempt.GetCommitAdopted(), phases, attempt.GetImage().GetPlatformDigest())
	}
	if grew := x.git.AuthenticatedRequests("private/app.git") - before; grew != run1Requests {
		t.Fatalf("run 2 made %d authenticated requests, run 1 made %d: both git tasks must fetch again", grew, run1Requests)
	}
	sha2 := fmt.Sprint(taskOutput(t, run2, "clone")["sha256"])
	x.note("clone src.tar.gz run1=%s run2=%s equal=%v", sha1, sha2, sha1 == sha2)
	if sha1 != sha2 {
		t.Fatalf("two uncached clones of one commit produced two tarballs: %s, %s", sha1, sha2)
	}
	for _, id := range []string{"tmpl", "defaults", "render"} {
		requireCacheOutcome(t, run2, id, orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT)
		if source := taskRunOf(t, run2, id).GetCache().GetSource().GetRunId(); source != run1.GetId() {
			t.Fatalf("run 2's %s hit names run %s, want run 1 %s", id, source, run1.GetId())
		}
	}
	if got := x.mustCLI(0, "run", "get", run2.GetId()); !strings.Contains(got, "SUCCEEDED (cached)") {
		t.Fatalf("run 2's run get lacks SUCCEEDED (cached):\n%s", got)
	}

	// Run 3: main moves; only VERSION changes.
	newSHA := x.git.CommitVersion("private/app.git", "1.0.1\n")
	run3 := x.runToEnd(ctx, t, name, x2Input)
	requireRunStatus(t, run3, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := taskOutput(t, run3, "resolve")["commit"]; got != newSHA {
		t.Fatalf("run 3 resolve = %v, want the new main %s", got, newSHA)
	}
	if sha3 := fmt.Sprint(taskOutput(t, run3, "clone")["sha256"]); sha3 == sha1 {
		t.Fatalf("run 3's tarball %s equals run 1's for a different commit", sha3)
	}
	for _, id := range []string{"tmpl", "defaults", "render"} {
		requireCacheOutcome(t, run3, id, orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS)
	}
	if got := string(x.download(t, run3.GetId(), "render", "app.conf")); got != x2AppConf {
		t.Fatalf("run 3's app.conf = %q, want the same bytes as run 1", got)
	}
	x.note("run 3: resolve=%s, tmpl/defaults/render cache=MISS, app.conf byte-identical", newSHA)

	// Run 4 — revocation: the token the deployment still mounts is revoked.
	x.git.SetToken(mintGitToken(t))
	run4 := x.runToEnd(ctx, t, name, x2Input)
	requireRunStatus(t, run4, orkstackv1.Status_STATUS_FAILED)
	requireTaskStatus(t, run4, "resolve", orkstackv1.Status_STATUS_FAILED)
	if logs := x.taskLogs(run4.GetId(), "resolve", 0); !strings.Contains(logs, "Authentication failed") {
		t.Fatalf("run 4's resolve log lacks Authentication failed:\n%s", logs)
	}
	for _, tr := range run4.GetTaskRuns() {
		if tr.GetCache().GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			t.Fatalf("run 4's %s is (cached): a revoked credential must take effect on the very next run", tr.GetTaskId())
		}
	}
	x.note("run 4 (token revoked): resolve FAILED, Authentication failed, no task (cached)")
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// ============================================================== refs

func gitResolveDocument(name, url, ref, extra string) string {
	refLine := ""
	if ref != "" {
		refLine = ", ref: " + ref
	}
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
tasks:
  - id: resolve
    uses: git
    timeout_seconds: 300
%s%s    with: {op: resolve, url: "%s"%s, tokenSecret: git-token, caFile: secrets/git-ca}
`, name, gitSecrets(), extra, url, refLine)
}

func TestIntegration_TransformImages_GitRefResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X2b")
	url := x.git.URL("private/app.git")
	resolve := func(ref string) (*orkstackv1.Run, map[string]any) {
		name := transformName("x2b")
		x.applyCLI(t, gitResolveDocument(name, url, ref, ""))
		run := x.runToEnd(ctx, t, name, "")
		return run, taskRunOf(t, run, "resolve").GetOutput().GetData().AsMap()
	}

	run, _ := resolve("release")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	if logs := x.taskLogs(run.GetId(), "resolve", 0); !strings.Contains(logs, "refs/heads/release") || !strings.Contains(logs, "refs/tags/release") {
		t.Fatalf("an ambiguous short name must name both refs:\n%s", logs)
	}
	run, out := resolve("refs/tags/release")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if want := x.git.Commit("private/app.git", "refs/tags/release"); out["commit"] != want {
		t.Fatalf("refs/tags/release = %v, want %s", out, want)
	}
	run, out = resolve("v1.0.0")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	peeled, tagObject := x.git.Commit("private/app.git", "v1.0.0^{commit}"), x.git.Commit("private/app.git", "v1.0.0")
	if out["commit"] != peeled || out["commit"] == tagObject {
		t.Fatalf("v1.0.0 = %v, want the peeled commit %s, never the tag object %s", out, peeled, tagObject)
	}
	run, _ = resolve("nope")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	if logs := x.taskLogs(run.GetId(), "resolve", 0); !strings.Contains(logs, "no branch or tag") {
		t.Fatalf("nope: %s", logs)
	}
	run, out = resolve("")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if out["ref"] != "refs/heads/main" || out["commit"] != x.git.Commit("private/app.git", "refs/heads/main") {
		t.Fatalf("ref absent = %v, want refs/heads/main at main's commit", out)
	}
	x.note("release: ambiguous; refs/tags/release=%s; v1.0.0 peeled=%s (tag object %s); nope: no branch or tag; absent: refs/heads/main",
		x.git.Commit("private/app.git", "refs/tags/release"), peeled, tagObject)
}

// ============================================================== public clone cached

func TestIntegration_TransformImages_PublicCloneByCommitIsCached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X2c")
	name := transformName("x2c")
	applied := x.applyCLI(t, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
inputs:
  commit: {type: string, required: true}
  ca: {type: string, required: true}
tasks:
  - id: ca
    uses: json
    network: none
    cache: {enabled: true, ttl_seconds: 86400}
    with: {op: query, nullInput: true, vars: {pem: '{{ .inputs.ca }}'}, filter: '$vars.pem', as: ca.pem}
  - id: clone
    uses: git
    needs: [ca]
    cache: {enabled: true, ttl_seconds: 86400}
    with: {op: clone, url: "%s", ref: '{{ .inputs.commit }}', caFile: '{{ artifact "ca/ca.pem" }}'}
`, name, x.git.URL("public/lib.git")))
	for _, field := range []string{".secrets", ".cache.ttl_seconds"} {
		if strings.Contains(applied, field) {
			t.Fatalf("apply warned at a %s field:\n%s", field, applied)
		}
	}
	input := fmt.Sprintf(`{"commit":%q,"ca":%q}`, x.git.Commit("public/lib.git", "refs/heads/main"), x.git.CA())

	run1 := x.runToEnd(ctx, t, name, input)
	requireRunStatus(t, run1, orkstackv1.Status_STATUS_SUCCEEDED)
	requireCacheOutcome(t, run1, "ca", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS)
	requireCacheOutcome(t, run1, "clone", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS)
	sha1 := fmt.Sprint(taskOutput(t, run1, "clone")["sha256"])
	requestsBefore := x.git.Requests("public/lib.git")
	if requestsBefore <= 0 {
		t.Fatal("run 1's clone made no request to public/lib.git")
	}

	run2 := x.runToEnd(ctx, t, name, input)
	requireRunStatus(t, run2, orkstackv1.Status_STATUS_SUCCEEDED)
	requireCacheOutcome(t, run2, "clone", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT)
	if source := taskRunOf(t, run2, "clone").GetCache().GetSource().GetRunId(); source != run1.GetId() {
		t.Fatalf("run 2's clone hit names %s, want run 1 %s", source, run1.GetId())
	}
	if sha2 := fmt.Sprint(taskOutput(t, run2, "clone")["sha256"]); sha2 != sha1 {
		t.Fatalf("the cached clone reports sha256 %s, run 1 %s", sha2, sha1)
	}
	requestsAfter := x.git.Requests("public/lib.git")
	x.note("public clone run2 cache=HIT requests_before=%d requests_after=%d", requestsBefore, requestsAfter)
	if requestsAfter != requestsBefore {
		t.Fatalf("the hit fetched: requests %d -> %d", requestsBefore, requestsAfter)
	}
	if got := x.mustCLI(0, "run", "get", run2.GetId()); !strings.Contains(got, "SUCCEEDED (cached)") {
		t.Fatalf("run 2's run get lacks SUCCEEDED (cached):\n%s", got)
	}
}

// ============================================================== pin

func TestIntegration_TransformImages_GitCheckoutIntoAPin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X3")
	name := transformName("x3")
	x.applyCLI(t, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
pins:
  build: {}
tasks:
  - id: checkout
    uses: git
    pin: build
    run_as_orker_user: true
    retry: {maximum_attempts: 2, initial_interval_seconds: 1}
%s    with:
      op: clone
      url: "%s"
      ref: %s
      dest: /orkstack/shared/work/src
      tokenSecret: git-token
      caFile: secrets/git-ca
  - id: read
    uses: shell
    pin: build
    run_as_orker_user: true
    needs: [checkout]
    with:
      op: exec
      command: [sh, -c, "test -d /orkstack/shared/work/src/.git && cat /orkstack/shared/work/src/VERSION | tee /orkstack/output/artifacts/version.txt"]
`, name, gitSecrets(), x.git.URL("private/flaky.git"), x.git.Commit("private/flaky.git", "refs/heads/main")))

	run := x.runToEnd(ctx, t, name, "")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	checkout := taskRunOf(t, run, "checkout")
	if checkout.GetAttempt() != 2 {
		t.Fatalf("checkout succeeded on attempt %d, want 2 (attempt 1 meets the forced 500)", checkout.GetAttempt())
	}
	attempts := x.attempts(t, run.GetId(), "checkout")
	if len(attempts) != 2 || attempts[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED ||
		attempts[0].GetError().GetCode() != "CONTAINER_EXIT_NONZERO" {
		t.Fatalf("checkout attempts = %v, want attempt 1 FAILED CONTAINER_EXIT_NONZERO", attempts)
	}
	if logs := x.taskLogs(run.GetId(), "checkout", 1); !strings.Contains(logs, "returned error: 500") {
		t.Fatalf("attempt 1's log lacks the forced 500 (the token was accepted, the server failed):\n%s", logs)
	}
	if got := x.git.AuthenticatedRequests("private/flaky.git"); got < 2 {
		t.Fatalf("private/flaky.git saw %d authenticated requests, want at least 2", got)
	}
	if got := checkout.GetOutput().GetData().AsMap()["path"]; got != "/orkstack/shared/work/src" {
		t.Fatalf("checkout path = %v", got)
	}
	if got := string(x.download(t, run.GetId(), "read", "version.txt")); got != "flaky-1\n" {
		t.Fatalf("version.txt = %q, want the repository's VERSION", got)
	}
	x.note("checkout: attempt 1 FAILED (500), attempt 2 SUCCEEDED into /orkstack/shared/work/src; read: VERSION=flaky-1")
}

// ============================================================== the token

// requireNoToken checks every place a run is read from for both forms of
// the token: each task's log, run get --json, run attempts --json, and the
// decompressed evidence bundle.
func (x *transformStack) requireNoToken(t *testing.T, run *orkstackv1.Run) {
	t.Helper()
	places := map[string]string{
		"run get --json":      x.mustCLI(0, "--json", "run", "get", run.GetId()),
		"run attempts --json": x.mustCLI(0, "--json", "run", "attempts", run.GetId()),
	}
	for _, tr := range run.GetTaskRuns() {
		places["logs of "+tr.GetTaskId()] = x.taskLogs(run.GetId(), tr.GetTaskId(), 0)
	}
	bundle := filepath.Join(t.TempDir(), "evidence.tar.gz")
	x.mustCLI(0, "run", "evidence", "export", run.GetId(), "-o", bundle)
	raw, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("bundle is not gzip: %v", err)
	}
	decompressed, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompress bundle: %v", err)
	}
	places["the evidence bundle"] = string(decompressed)
	for place, body := range places {
		for _, form := range tokenForms(x.git.originalToken()) {
			if strings.Contains(body, form) {
				t.Errorf("the token occurs in %s of run %s", place, run.GetId())
			}
		}
	}
}

func TestIntegration_TransformImages_GitTokenStaysSecret(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X4")
	url := x.git.URL("private/app.git")

	// (a)
	name := transformName("x4a")
	x.applyCLI(t, x2Document(name, url))
	before := x.git.AuthenticatedRequests("private/app.git")
	run := x.runToEnd(ctx, t, name, x2Input)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if x.git.AuthenticatedRequests("private/app.git") <= before {
		t.Fatal("(a) the token was never used")
	}
	x.requireNoToken(t, run)
	x.note("(a) X2's document: the token occurs in no log, run get, run attempts or evidence bundle")

	// (b) git's traces, honoured and redacted, on the image's own git.
	traces := "    env: {GIT_TRACE_CURL: \"1\", GIT_CURL_VERBOSE: \"1\", GIT_TRACE: \"1\", GIT_TRACE_PACKET: \"1\"%s}\n"
	for _, extra := range []string{"", ", GIT_TRACE_REDACT: \"2\""} {
		name = transformName("x4b")
		x.applyCLI(t, gitResolveDocument(name, url, "main", fmt.Sprintf(traces, extra)))
		run = x.runToEnd(ctx, t, name, "")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		logs := x.taskLogs(run.GetId(), "resolve", 0)
		if !strings.Contains(logs, "Authorization: Basic <redacted>") {
			t.Fatalf("(b%s) the trace lacks the redacted Authorization header:\n%s", extra, logs)
		}
		x.requireNoToken(t, run)
		x.note("(b%s) traced: Authorization: Basic <redacted>; the token occurs nowhere", extra)
	}

	// (c) the one refusal precedes every git command that contacts a remote.
	name = transformName("x4c")
	x.applyCLI(t, gitResolveDocument(name, url, "main", "    env: {GIT_TRACE_CURL: \"1\", GIT_TRACE_REDACT: \"0\"}\n"))
	before = x.git.AuthenticatedRequests("private/app.git")
	run = x.runToEnd(ctx, t, name, "")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	if code := taskRunOf(t, run, "resolve").GetError().GetCode(); code != "CONTAINER_EXIT_NONZERO" {
		t.Fatalf("(c) resolve error = %s, want CONTAINER_EXIT_NONZERO", code)
	}
	if logs := x.taskLogs(run.GetId(), "resolve", 0); !strings.Contains(logs, "GIT_TRACE_REDACT='0'") {
		t.Fatalf("(c) the refusal does not name GIT_TRACE_REDACT:\n%s", logs)
	}
	if after := x.git.AuthenticatedRequests("private/app.git"); after != before {
		t.Fatalf("(c) authenticated requests grew %d -> %d: the refusal must precede every remote contact", before, after)
	}
	x.requireNoToken(t, run)
	x.note("(c) GIT_TRACE_REDACT=0 with tokenSecret: FAILED before any remote contact")

	// (d) no tokenSecret, the CA kept: git meets the 401 and cannot ask.
	name = transformName("x4d")
	x.applyCLI(t, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
tasks:
  - id: resolve
    uses: git
    timeout_seconds: 300
    secrets:
      - name: git-ca
        source: env:TRANSFORM_GIT_CA
    with: {op: resolve, url: "%s", ref: main, caFile: secrets/git-ca}
`, name, url))
	run = x.runToEnd(ctx, t, name, "")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	resolve := taskRunOf(t, run, "resolve")
	if took := resolve.GetFinishedAt().AsTime().Sub(resolve.GetStartedAt().AsTime()); took > 60*time.Second {
		t.Fatalf("(d) the credential-less resolve took %s, want a failure well inside 60s", took)
	}
	if logs := x.taskLogs(run.GetId(), "resolve", 0); !strings.Contains(logs, "terminal prompts disabled") {
		t.Fatalf("(d) the log lacks terminal prompts disabled:\n%s", logs)
	}
	x.note("(d) no tokenSecret: FAILED at once, terminal prompts disabled")
}

// ============================================================== TLS

func TestIntegration_TransformImages_GitVerifiesTLSUnlessTheTaskSaysOtherwise(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X5")
	url := x.git.URL("public/lib.git")
	mainSHA := x.git.Commit("public/lib.git", "refs/heads/main")
	documents := []struct{ label, body string }{
		{"default", "    with: {op: resolve, url: \"" + url + "\", ref: main}\n"},
		{"caFile", "    secrets:\n      - name: git-ca\n        source: env:TRANSFORM_GIT_CA\n" +
			"    with: {op: resolve, url: \"" + url + "\", ref: main, caFile: secrets/git-ca}\n"},
		{"GIT_SSL_NO_VERIFY", "    env: {GIT_SSL_NO_VERIFY: \"true\"}\n    with: {op: resolve, url: \"" + url + "\", ref: main}\n"},
	}
	outcomes := map[string]*orkstackv1.Run{}
	for _, doc := range documents {
		name := transformName("x5")
		x.applyCLI(t, fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"0.1.0\"\ntasks:\n  - id: resolve\n    uses: git\n%s", name, doc.body))
		outcomes[doc.label] = x.runToEnd(ctx, t, name, "")
	}
	x.note("tls default=%s caFile=%s GIT_SSL_NO_VERIFY=%s",
		strings.TrimPrefix(outcomes["default"].GetStatus().String(), "STATUS_"),
		strings.TrimPrefix(outcomes["caFile"].GetStatus().String(), "STATUS_"),
		strings.TrimPrefix(outcomes["GIT_SSL_NO_VERIFY"].GetStatus().String(), "STATUS_"))

	requireRunStatus(t, outcomes["default"], orkstackv1.Status_STATUS_FAILED)
	if logs := x.taskLogs(outcomes["default"].GetId(), "resolve", 0); !strings.Contains(logs, "certificate") {
		t.Fatalf("(a) TLS is verified by default; the failure must name the certificate:\n%s", logs)
	}
	for _, label := range []string{"caFile", "GIT_SSL_NO_VERIFY"} {
		requireRunStatus(t, outcomes[label], orkstackv1.Status_STATUS_SUCCEEDED)
		if got := taskOutput(t, outcomes[label], "resolve")["commit"]; got != mainSHA {
			t.Fatalf("(%s) commit = %v, want %s", label, got, mainSHA)
		}
	}
}

// ============================================================== cancel

func TestIntegration_TransformImages_GitCancellationReachesTheFetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X6")
	name := transformName("x6")
	x.applyCLI(t, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
tasks:
  - id: clone
    uses: git
    cancellation_grace_period_seconds: 20
    secrets:
      - name: git-ca
        source: env:TRANSFORM_GIT_CA
    with: {op: clone, url: "%s", ref: %s, caFile: secrets/git-ca}
`, name, x.git.URL("stall.git"), x.git.Commit("stall.git", "refs/heads/main")))
	runID := x.submit(name)
	select {
	case <-x.git.StallHeld():
	case <-ctx.Done():
		t.Fatal("the fetch never reached the stalling server")
	case <-time.After(3 * time.Minute):
		t.Fatal("the fetch never reached the stalling server within 3 minutes")
	}
	cancelledAt := time.Now()
	x.mustCLI(0, "run", "cancel", runID)
	run := awaitTerminalOnB(ctx, t, x.replicas[0], runID)
	took := time.Since(cancelledAt)
	x.note("cancelled while the fetch was held: run %s after %s", run.GetStatus(), took.Round(time.Millisecond))
	requireRunStatus(t, run, orkstackv1.Status_STATUS_CANCELED)
	if took > 15*time.Second {
		t.Fatalf("the run took %s to cancel, past the 15s bound (SIGKILL would come at 20s)", took)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, err := exec.CommandContext(ctx, "docker", "ps", "-a", "-q", "--filter", "label=ork.run="+runID).Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cancelled run's container is still present: %s (%v)", out, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ============================================================== failures

func TestIntegration_TransformImages_FailuresAreExplicit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, false)
	defer x.finish(t, "X7")
	one := func(body string) *orkstackv1.Run {
		name := transformName("x7")
		x.applyCLI(t, fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"0.1.0\"\ntasks:\n%s", name, body))
		return x.runToEnd(ctx, t, name, "")
	}

	// (a)
	run := one("  - id: q\n    uses: json\n    network: none\n    with: {op: query, nullInput: true, filter: '1,2'}\n")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	failure := taskRunOf(t, run, "q").GetError()
	if failure.GetCode() != "CONTAINER_EXIT_NONZERO" || failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Fatalf("(a) error = %v, want CONTAINER_EXIT_NONZERO / ActivityFailed", failure)
	}
	if logs := x.taskLogs(run.GetId(), "q", 0); !strings.Contains(logs, "2 outputs") {
		t.Fatalf("(a) stderr lacks \"2 outputs\":\n%s", logs)
	}

	// (b) a missing key publishes no artifact.
	run = one(`  - id: tmpl
    uses: json
    network: none
    with: {op: query, nullInput: true, filter: '"x={{ "{{" }} .nope }}"', as: t.tmpl}
  - id: render
    uses: template
    needs: [tmpl]
    network: none
    with: {op: render, src: '{{ artifact "tmpl/t.tmpl" }}'}
`)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	requireTaskStatus(t, run, "render", orkstackv1.Status_STATUS_FAILED)
	x.mustCLI(0, "run", "artifacts", run.GetId(), "--task", "render")
	var listed orkstackv1.ListRunArtifactsResponse
	if err := api.UnmarshalJSON([]byte(x.mustCLI(0, "--json", "run", "artifacts", run.GetId(), "--task", "render")), &listed); err != nil {
		t.Fatalf("(b) decode run artifacts --json: %v", err)
	}
	for _, artifact := range listed.GetArtifacts() {
		if artifact.GetKind() == orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT {
			t.Fatalf("(b) the failed render published an output artifact: %v", artifact)
		}
	}

	// (c) an over-limit result is the Orker's ContractViolation, never retried.
	run = one(`  - id: big
    uses: json
    network: none
    retry: {maximum_attempts: 3, initial_interval_seconds: 1}
    with: {op: query, nullInput: true, filter: '[range(40000)] | map("item-\(.)")'}
`)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	big := taskRunOf(t, run, "big")
	if big.GetError().GetCode() != "VALUES_TOO_LARGE" || big.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION || big.GetAttempt() != 1 {
		t.Fatalf("(c) error = %v after %d attempts, want VALUES_TOO_LARGE / ContractViolation after 1", big.GetError(), big.GetAttempt())
	}
	if logs := x.taskLogs(run.GetId(), "big", 0); !strings.Contains(logs, "write it as an artifact with as:") {
		t.Fatalf("(c) stderr lacks the as: hint:\n%s", logs)
	}

	// (d) a task that mounts a secret is refused.
	run = one(`  - id: q
    uses: json
    network: none
    secrets:
      - name: s
        source: env:TRANSFORM_JSON_SECRET
    with: {op: query, nullInput: true, filter: '1'}
`)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	logs := x.taskLogs(run.GetId(), "q", 0)
	if !strings.Contains(logs, "this image reads no secret, and a task that mounts one is refused") || strings.Contains(logs, transformJSONSecret) {
		t.Fatalf("(d) stderr:\n%s", logs)
	}

	// (e) literal braces, end to end.
	run = one(`  - id: q
    uses: json
    network: none
    with: {op: query, nullInput: true, filter: '"{{ "{{" }}name}}"'}
`)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := taskOutput(t, run, "q")["result"]; got != "{{name}}" {
		t.Fatalf("(e) result = %#v, want \"{{name}}\"", got)
	}
	path := filepath.Join(t.TempDir(), "unescaped.ork.yaml")
	unescaped := "apiVersion: orkstack/v1\nkind: Orkflow\nname: unescaped\nversion: \"0.1.0\"\ntasks:\n  - id: q\n    uses: json\n    network: none\n    with: {op: query, nullInput: true, filter: '\"{{name}}\"'}\n"
	if err := os.WriteFile(path, []byte(unescaped), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := x.cli(0, "orkflow", "validate", path)
	if code == 0 || !strings.Contains(stdout+stderr, "with.filter") {
		t.Fatalf("(e) validate of the unescaped filter exited %d:\n%s%s", code, stdout, stderr)
	}
	x.note("(a) 2 outputs: CONTAINER_EXIT_NONZERO/ActivityFailed; (b) no artifact; (c) VALUES_TOO_LARGE/ContractViolation, 1 attempt; (d) secret refused; (e) {{name}}")
}

// ============================================================== redirect

func TestIntegration_TransformImages_GitTokenNeverFollowsARedirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	x := startTransformStack(ctx, t, true)
	defer x.finish(t, "X9")
	name := transformName("x9")
	x.applyCLI(t, gitResolveDocument(name, x.git.URL("moved/app.git"), "main", ""))
	run := x.runToEnd(ctx, t, name, "")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	if code := taskRunOf(t, run, "resolve").GetError().GetCode(); code != "CONTAINER_EXIT_NONZERO" {
		t.Fatalf("resolve error = %s, want CONTAINER_EXIT_NONZERO", code)
	}
	logs := x.taskLogs(run.GetId(), "resolve", 0)
	prompt := fmt.Sprintf("could not read Username for 'https://host.docker.internal:%d': terminal prompts disabled", x.git.ElsewherePort())
	for _, want := range []string{"a credential was asked for another origin", prompt} {
		if !strings.Contains(logs, want) {
			t.Fatalf("the log lacks %q:\n%s", want, logs)
		}
	}
	if got := x.git.ElsewhereAuthorizations(); len(got) != 0 {
		t.Fatalf("the redirect target received %d credentials", len(got))
	}
	for _, form := range tokenForms(x.git.originalToken()) {
		if strings.Contains(logs, form) {
			t.Fatal("the token occurs in the task log")
		}
	}
	x.note("ElsewhereAuthorizations=%d; %s", len(x.git.ElsewhereAuthorizations()), prompt)
}

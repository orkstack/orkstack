//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// scopeStack is one engine on the deployment's own credential and one real
// Orker whose every storage request goes through s3scope, holding it to a
// policy (docs/DEPLOY.md, "An Orker's own storage credential").
type scopeStack struct {
	t        *testing.T
	ctx      context.Context
	root     string
	orkctl   string
	proxy    *s3scope.Proxy
	temporal client.Client
	queue    string
	// namespace is the Temporal namespace the stack's engine runs in, for
	// a second replica beside it.
	namespace string
	orkerID   string
	engine    *engineReplica
	log       strings.Builder
}

func newScopeStack(t *testing.T, policy s3scope.Policy) *scopeStack {
	t.Helper()
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, cfg)
	proxy := s3scope.New(t, cfg.S3Endpoint, cfg.S3Bucket, policy)
	s := startScopeStack(t, nil, []string{"ORK_S3_ENDPOINT=" + proxy.URL})
	s.proxy = proxy
	fmt.Fprintf(&s.log, "# an Orker behind s3scope (%s)\nengine %s, Orker %s, store %s through %s\n\n",
		time.Now().UTC().Format(time.RFC3339), s.engine.addr, s.orkerID, cfg.S3Endpoint, proxy.URL)
	return s
}

// startScopeStack boots one engine and one Orker, each with extra
// environment over the test's own store settings.
func startScopeStack(t *testing.T, engineEnv, orkerEnv []string) *scopeStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	s := &scopeStack{t: t, ctx: ctx, root: repoRoot(t), queue: "ork-scope-" + stamp, orkerID: "orker-scope-" + stamp}
	namespace := isolatedNamespace()
	s.namespace = namespace
	s.temporal = dialIsolatedNamespace(ctx, t, address, namespace)
	s.orkctl = testbin.Path(t, "./cmd/orkctl")
	s.engine = startReplica(ctx, t, "ork", testbin.Path(t, "./cmd/ork"), address, namespace, s.queue, engineEnv...)
	s.engine.awaitReady(ctx, t)
	startOrkerWithEnv(ctx, t, s.temporal, testbin.Path(t, "./cmd/orker"), address, namespace, s.queue,
		append([]string{
			"ORK_ORKER_ID=" + s.orkerID,
			"ORK_ORKER_MAX_PINS=1",
			"ORK_ORKER_SECRET_ALLOW=env:SCOPE_TOKEN",
			"SCOPE_TOKEN=" + mintHex(t),
		}, orkerEnv...)...,
	)
	return s
}

func (s *scopeStack) cli(args ...string) string {
	s.t.Helper()
	full := append(append([]string(nil), args...), "--server", "http://"+s.engine.addr)
	stdout, stderr, code := runBinary(s.ctx, s.t, s.orkctl, s.root, full...)
	fmt.Fprintf(&s.log, "$ orkctl %s\n%s%s(exit %d)\n\n", strings.Join(args, " "), stdout, stderr, code)
	if code != 0 {
		s.t.Fatalf("orkctl %s exit %d: %s", strings.Join(args, " "), code, stderr)
	}
	return stdout
}

func (s *scopeStack) create(name, source string) {
	s.t.Helper()
	path := filepath.Join(s.t.TempDir(), name+".ork.yaml")
	must.OK(s.t, os.WriteFile(path, []byte(source), 0o644))
	s.cli("orkflow", "create", path)
}

func (s *scopeStack) submit(name string) *orkstackv1.Run {
	s.t.Helper()
	runID := parseRunID(s.t, s.cli("run", "submit", name))
	return awaitTerminalRun(s.ctx, s.t, s.engine, runID)
}

// describe asks the Orker on its own control channel, as the inventory does.
func (s *scopeStack) describe() *orkstackv1.DescribeOrkerResult {
	s.t.Helper()
	starter := engine.NewTemporalStarterWithOrkerQueue(s.temporal, s.queue, s.queue)
	d, err := starter.DescribeOrkerByID(s.ctx, s.orkerID)
	must.OKf(s.t, err, "DescribeOrker %s", s.orkerID)
	return d
}

// awaitScope waits for the Orker's first round to end with a verdict.
func (s *scopeStack) awaitScope(want orkstackv1.StoreScope) *orkstackv1.DescribeOrkerResult {
	s.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var d *orkstackv1.DescribeOrkerResult
	for time.Now().Before(deadline) {
		if d = s.describe(); d.GetStoreScope() == want {
			fmt.Fprintf(&s.log, "DescribeOrker: store_scope=%v detail=%q confirmed_at=%v results_scope=%v\n\n",
				d.GetStoreScope(), d.GetStoreScopeDetail(), d.GetStoreScopeConfirmedAt().AsTime(), d.GetResultsScope())
			return d
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("the Orker never reported %v; last %v (%q)", want, d.GetStoreScope(), d.GetStoreScopeDetail())
	return nil
}

func taskOf(t *testing.T, run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	t.Helper()
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == id {
			return task
		}
	}
	t.Fatalf("run %s has no task %s", run.GetId(), id)
	return nil
}

// everyKindOfTask holds every kind of task an Orker runs: a json task, a
// fan-out, two pinned members sharing /orkstack/shared, a cached task, a
// task with a secret, and a fan-in.
const everyKindOfTask = `apiVersion: orkstack/v1
kind: Orkflow
name: %s
description: Every kind of task, behind the documented Orker storage policy.
pins:
  workspace:
    timeout_seconds: 600
tasks:
  - id: list
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      vars:
        parts:
          - a
          - b
      filter: '$vars.parts'
  - id: convert
    uses: shell
    needs:
      - list
    each:
      over: '{{ .tasks.list.output.result }}'
      as: part
      max_items: 10
    with:
      op: script
      interpreter: sh
      script: |
        printf '%%s\n' {{ sh .each.part }} > /orkstack/output/artifacts/part.txt
  - id: warm
    uses: shell
    pin: workspace
    run_as_orker_user: true
    with:
      op: script
      interpreter: sh
      script: |
        echo warm > /orkstack/shared/warm.txt
  - id: use
    uses: shell
    needs:
      - warm
    pin: workspace
    run_as_orker_user: true
    with:
      op: script
      interpreter: sh
      script: |
        cat /orkstack/shared/warm.txt > /orkstack/output/artifacts/used.txt
  - id: cached
    uses: shell
    network: none
    cache:
      enabled: true
    with:
      op: script
      interpreter: sh
      script: |
        echo cached > /orkstack/output/artifacts/cached.txt
  - id: secret
    uses: shell
    network: none
    secrets:
      - name: scope-token
        source: env:SCOPE_TOKEN
    with:
      op: script
      interpreter: sh
      script: |
        test -s /orkstack/input/secrets/scope-token
  - id: merge
    uses: shell
    needs:
      - convert
      - use
      - cached
      - secret
    with:
      op: script
      interpreter: sh
      script: |
        cat /orkstack/input/artifacts/convert/*/part.txt /orkstack/input/artifacts/use/used.txt
`

// scopeCheckRequest reports a request the scope check itself makes: under a
// checked prefix with its own segment, the sentinel, a one-key listing of a
// checked prefix, or the results request.
func scopeCheckRequest(r s3scope.Request) bool {
	if strings.Contains(r.Key, "/"+artifacts.ScopeResultsSegment+"/") || r.Key == artifacts.ScopeSentinelKey ||
		strings.HasPrefix(r.Key, artifacts.ScopeProbePrefix+"/") {
		return true
	}
	if r.Verb == s3scope.List {
		for _, p := range artifacts.ScopeCheckedPrefixes {
			if r.Key == p+"/" {
				return true
			}
		}
		for _, p := range artifacts.ScopeReadOnlyPrefixes {
			if !p.Listed && r.Key == p.Prefix+"/" {
				return true
			}
		}
	}
	return false
}

// TestIntegration_TheDocumentedOrkerPolicyIsEnough: an Orker holding exactly
// the documented policy runs every kind of task, twice so the second run
// hits the memo, the store refuses none of its requests but the scope
// check's own, and the Orker reports its credential scoped.
func TestIntegration_TheDocumentedOrkerPolicyIsEnough(t *testing.T) {
	s := newScopeStack(t, s3scope.OrkerPolicy())
	scope := s.awaitScope(orkstackv1.StoreScope_STORE_SCOPE_SCOPED)
	if scope.GetStoreScopeConfirmedAt() == nil || scope.GetStoreScopeDetail() != "" {
		t.Fatalf("a scoped verdict says %q, confirmed at %v", scope.GetStoreScopeDetail(), scope.GetStoreScopeConfirmedAt())
	}
	if scope.GetResultsScope() != orkstackv1.ResultsScope_RESULTS_SCOPE_EVERY_NAMESPACE {
		t.Fatalf("results scope = %v; the documented policy covers every namespace's results", scope.GetResultsScope())
	}
	checked := len(s.proxy.Requests())
	if checked == 0 {
		t.Fatal("the scope check made no request through the proxy")
	}

	name := "scope-every-kind-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)
	s.create(name, fmt.Sprintf(everyKindOfTask, name))
	for i := 1; i <= 2; i++ {
		run := s.submit(name)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %d (%s) ended %v: %v\n%s", i, run.GetId(), run.GetStatus(), run.GetError(), s.log.String())
		}
		fmt.Fprintf(&s.log, "run %d %s: %v, cached task %v\n\n", i, run.GetId(), run.GetStatus(),
			taskOf(t, run, "cached").GetCache().GetOutcome())
		if i == 2 && taskOf(t, run, "cached").GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			t.Fatalf("the second run's cached task was %v, want a memo hit through the policy",
				taskOf(t, run, "cached").GetCache().GetOutcome())
		}
	}

	var refused []string
	tasks := 0
	for _, r := range s.proxy.Requests() {
		if scopeCheckRequest(r) {
			continue
		}
		tasks++
		if !r.Allowed {
			refused = append(refused, string(r.Verb)+" "+r.Key)
		}
	}
	if tasks == 0 {
		t.Fatal("no task request went through the proxy, so nothing here was tested")
	}
	// The run-context object was read through the policy, by its key alone.
	contexts := 0
	for _, r := range s.proxy.Requests() {
		if r.Verb == s3scope.Get && strings.HasPrefix(r.Key, artifacts.RunContextPrefix+"/") && r.Allowed {
			contexts++
		}
	}
	if contexts == 0 {
		t.Fatal("no task read its run context through the proxy")
	}
	if len(refused) > 0 {
		t.Fatalf("the documented policy refused %d of the Orker's own requests: %q", len(refused), refused)
	}
	fmt.Fprintf(&s.log, "EVIDENCE: %d task requests through the documented policy, none refused; %d made by the scope check\n",
		tasks, checked)
	t.Logf("EVIDENCE documented Orker policy: two runs of every kind of task succeeded through s3scope with %d requests, none refused; verdict scoped",
		tasks)
	proof.Write(t, "orkctl", "store-scope-documented-policy", []byte(s.log.String()))
}

// TestIntegration_ASharedCredentialIsReportedUnscoped: an Orker whose
// credential reaches everything says so, naming the first prefix it
// reached, and still runs tasks: the check decides nothing.
func TestIntegration_ASharedCredentialIsReportedUnscoped(t *testing.T) {
	s := newScopeStack(t, s3scope.Everything())
	scope := s.awaitScope(orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED)
	if !strings.Contains(scope.GetStoreScopeDetail(), artifacts.IdentityPrefix+"/") {
		t.Fatalf("detail = %q, want it to name %s/", scope.GetStoreScopeDetail(), artifacts.IdentityPrefix)
	}
	name := "scope-shared-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)
	s.create(name, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
description: A plain task on an unscoped Orker.
tasks:
  - id: echo
    uses: shell
    with:
      op: exec
      command:
        - "true"
`, name))
	if run := s.submit(name); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("an unscoped Orker's run ended %v: %v", run.GetStatus(), run.GetError())
	}
	t.Logf("EVIDENCE shared credential: store_scope=%v detail=%q; a plain run still succeeded", scope.GetStoreScope(), scope.GetStoreScopeDetail())
	proof.Write(t, "orkctl", "store-scope-shared-credential", []byte(s.log.String()))
}

// TestIntegration_ACommitPrefixThatCannotBeListedFailsTheTask: without
// listing on metadata/commits/, a store that answers as AWS S3 does says 403
// for a commit that is not there yet, and the first container task fails
// COMMIT_READ_FAILED: the fault the documented policy's listing grant exists
// for. The control is TestIntegration_TheDocumentedOrkerPolicyIsEnough.
func TestIntegration_ACommitPrefixThatCannotBeListedFailsTheTask(t *testing.T) {
	s := newScopeStack(t, s3scope.OrkerPolicy().Without("metadata/commits/", s3scope.List))
	s.awaitScope(orkstackv1.StoreScope_STORE_SCOPE_SCOPED)
	name := "scope-no-list-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)
	s.create(name, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
description: A plain task whose commit check cannot tell missing from forbidden.
tasks:
  - id: echo
    uses: shell
    with:
      op: exec
      command:
        - "true"
`, name))
	run := s.submit(name)
	echo := taskOf(t, run, "echo")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || echo.GetError().GetCode() != "COMMIT_READ_FAILED" {
		t.Fatalf("run ended %v, task %v (%v); want FAILED with COMMIT_READ_FAILED", run.GetStatus(), echo.GetStatus(), echo.GetError())
	}
	t.Logf("EVIDENCE no listing on metadata/commits/: the task failed %s", echo.GetError().GetCode())
	proof.Write(t, "orkctl", "store-scope-no-commit-listing", []byte(s.log.String()))
}

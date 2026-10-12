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
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Cordon and drain's CLI acceptance (docs/DEPLOY.md "Removing an Orker
// safely: cordon and drain"), on the real binaries: `ork` and
// `orker` as built processes against real Temporal (a private namespace) and
// MinIO, and `orkctl` as a built binary whose exit codes are the real ones.

// orkerStack is one engine (or two), one real Orker with a known id, and the
// orkctl binary pointed at them.
type orkerStack struct {
	t         *testing.T
	ctx       context.Context
	root      string
	orkctl    string
	temporal  client.Client
	namespace string
	queue     string
	orkerID   string
	replicas  []*engineReplica
	log       strings.Builder
}

func newOrkerStack(t *testing.T, replicas int) *orkerStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	s := &orkerStack{t: t, ctx: ctx, root: repoRoot(t)}
	s.namespace = isolatedNamespace()
	s.temporal = dialIsolatedNamespace(ctx, t, address, s.namespace)
	s.queue = fmt.Sprintf("ork-drain-cli-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	s.orkerID = fmt.Sprintf("orker-cli-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)

	s.orkctl = testbin.Path(t, "./cmd/orkctl")
	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")
	for i := 0; i < replicas; i++ {
		replica := startReplica(ctx, t, fmt.Sprintf("ork-%c", 'A'+i), enginePath, address, s.namespace, s.queue)
		replica.awaitReady(ctx, t)
		s.replicas = append(s.replicas, replica)
	}
	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+s.namespace,
		"ORK_ORKER_QUEUE="+s.queue,
		"ORK_ORKER_ID="+s.orkerID,
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	)
	awaitActivityPoller(ctx, t, s.temporal, s.queue)
	// The Orker is listed once its presence poller is recorded.
	s.awaitOrker(0, func(row *orkstackv1.Orker) bool { return row != nil && row.GetUnavailableReason() == "" })
	return s
}

// cli runs orkctl against replica i and records the exchange for the proof.
func (s *orkerStack) cli(i int, args ...string) (string, string, int) {
	s.t.Helper()
	full := append(append([]string(nil), args...), "--server", "http://"+s.replicas[i].addr)
	stdout, stderr, code := runBinary(s.ctx, s.t, s.orkctl, s.root, full...)
	fmt.Fprintf(&s.log, "$ orkctl %s   # replica %s\n%s", strings.Join(args, " "), s.replicas[i].name, stdout)
	if stderr != "" {
		fmt.Fprintf(&s.log, "%s", stderr)
	}
	fmt.Fprintf(&s.log, "(exit %d)\n\n", code)
	return stdout, stderr, code
}

// get reads the Orker's row through replica i, nil when nothing polls it.
func (s *orkerStack) get(i int) *orkstackv1.Orker {
	s.t.Helper()
	full := []string{"orker", "get", s.orkerID, "--json", "--server", "http://" + s.replicas[i].addr}
	stdout, _, code := runBinary(s.ctx, s.t, s.orkctl, s.root, full...)
	resp := &orkstackv1.ListOrkersResponse{}
	if err := api.UnmarshalJSON([]byte(stdout), resp); err != nil {
		if code == 0 {
			s.t.Fatalf("orker get --json is not a ListOrkersResponse: %v\n%s", err, stdout)
		}
		return nil
	}
	if len(resp.GetOrkers()) == 0 {
		return nil
	}
	return resp.GetOrkers()[0]
}

func (s *orkerStack) awaitOrker(i int, want func(*orkstackv1.Orker) bool) *orkstackv1.Orker {
	s.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var row *orkstackv1.Orker
	for time.Now().Before(deadline) {
		row = s.get(i)
		if want(row) {
			return row
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("orker %s never reached the wanted state; last row %v", s.orkerID, row)
	return nil
}

// submitSleep applies a one-task orkflow that sleeps, submits it, and returns
// the run id once the Orker reports the task running.
func (s *orkerStack) submitSleep(seconds int) string {
	s.t.Helper()
	name := fmt.Sprintf("sleep-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	source := fmt.Sprintf(`name: %s
description: orker drain probe.
tasks:
  - id: sleep
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "sleep %d"]
`, name, seconds)
	path := filepath.Join(s.t.TempDir(), name+".ork.yaml")
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		s.t.Fatal(err)
	}
	if _, stderr, code := s.cli(0, "apply", path); code != 0 {
		s.t.Fatalf("apply exit %d: %s", code, stderr)
	}
	stdout, stderr, code := s.cli(0, "run", "submit", name)
	if code != 0 {
		s.t.Fatalf("run submit exit %d: %s", code, stderr)
	}
	runID := parseRunID(s.t, stdout)
	s.awaitOrker(0, func(row *orkstackv1.Orker) bool { return row != nil && row.GetRunningTasks() == 1 })
	return runID
}

func (s *orkerStack) writeProof(name string) {
	proof.Write(s.t, "orkctl", name, []byte(s.log.String()))
}

func TestIntegration_DrainWaitsForTheLastTaskThenExitsZero(t *testing.T) {
	s := newOrkerStack(t, 1)
	t.Cleanup(func() { s.writeProof("orker-drain.txt") })
	runID := s.submitSleep(20)

	stdout, stderr, code := s.cli(0, "orker", "drain", s.orkerID, "--timeout", "2m", "--watch")
	t.Logf("EVIDENCE drain transcript (exit %d):\n%s", code, stdout)
	if code != exitOK {
		t.Fatalf("drain exit %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"cordoned " + s.orkerID, "1 task, 0 pins", "0 tasks, 0 pins",
		s.orkerID + " is drained; it is safe to stop this process."} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("the drain transcript lacks %q:\n%s", want, stdout)
		}
	}
	got, _, code := s.cli(0, "run", "get", runID, "--json")
	if code != 0 {
		t.Fatalf("run get exit %d", code)
	}
	resp := &orkstackv1.GetRunResponse{}
	if err := api.UnmarshalJSON([]byte(got), resp); err != nil {
		t.Fatalf("run get --json: %v", err)
	}
	run := resp.GetRun()
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetTaskRuns()) != 1 ||
		run.GetTaskRuns()[0].GetOrkerId() != s.orkerID {
		t.Fatalf("run %s = %v on %v; want SUCCEEDED on the drained Orker %s", runID, run.GetStatus(),
			run.GetTaskRuns(), s.orkerID)
	}
}

func TestIntegration_DrainTimesOutNamingTheRunningTask(t *testing.T) {
	s := newOrkerStack(t, 1)
	t.Cleanup(func() { s.writeProof("orker-drain-timeout.txt") })
	s.submitSleep(60)

	stdout, stderr, code := s.cli(0, "orker", "drain", s.orkerID, "--timeout", "5s")
	t.Logf("EVIDENCE drain timeout transcript (exit %d):\n%s", code, stdout)
	if code != exitDrainTimedOut {
		t.Fatalf("drain exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitDrainTimedOut, stdout, stderr)
	}
	if !strings.Contains(stdout, s.orkerID+" is NOT drained after 5s: 1 task, 0 pins still running") ||
		!strings.Contains(stdout, "stopping the process now interrupts 1 task") {
		t.Fatalf("the timeout does not name what remains:\n%s", stdout)
	}
	// Leave the Orker taking work again, as a lifecycle hook that decided
	// not to stop it would.
	if _, _, code := s.cli(0, "orker", "uncordon", s.orkerID); code != 0 {
		t.Fatalf("uncordon exit %d", code)
	}
}

func TestIntegration_ListOrkersIsAnsweredByAReplicaThatCordonedNothing(t *testing.T) {
	s := newOrkerStack(t, 2)
	t.Cleanup(func() { s.writeProof("orker-two-replicas.txt") })
	const reason = "two-replica cordon"
	if _, stderr, code := s.cli(0, "orker", "cordon", s.orkerID, "--reason", reason); code != 0 {
		t.Fatalf("cordon through A exit %d: %s", code, stderr)
	}
	listed, _, code := s.cli(1, "orker", "list", "--json")
	if code != 0 {
		t.Fatalf("list through B exit %d", code)
	}
	resp := &orkstackv1.ListOrkersResponse{}
	if err := api.UnmarshalJSON([]byte(listed), resp); err != nil {
		t.Fatalf("list --json: %v", err)
	}
	var row *orkstackv1.Orker
	for _, r := range resp.GetOrkers() {
		if r.GetOrkerId() == s.orkerID {
			row = r
		}
	}
	if row == nil || !row.GetCordoned() || row.GetCordonReason() != reason {
		t.Fatalf("replica B lists %v; want %s cordoned with %q", resp.GetOrkers(), s.orkerID, reason)
	}
	t.Logf("EVIDENCE two replicas: cordoned through A, listed through B: %s cordoned=%v reason=%q",
		row.GetOrkerId(), row.GetCordoned(), row.GetCordonReason())

	if _, stderr, code := s.cli(1, "orker", "uncordon", s.orkerID); code != 0 {
		t.Fatalf("uncordon through B exit %d: %s", code, stderr)
	}
	human, _, code := s.cli(0, "orker", "list")
	if code != 0 || !strings.Contains(human, s.orkerID) {
		t.Fatalf("list through A exit %d:\n%s", code, human)
	}
	back := s.get(0)
	if back == nil || back.GetCordoned() {
		t.Fatalf("replica A reads %v after the uncordon through B; want ready", back)
	}
	t.Logf("EVIDENCE two replicas: uncordoned through B, read through A: cordoned=%v", back.GetCordoned())

	// Filters narrow the same inventory on either replica: the Orker is
	// listed under its own pool, and no longer under cordoned=true.
	byPool, stderr, code := s.cli(1, "orker", "list", "--json", "--filter", "pool="+row.GetPool())
	if code != 0 {
		t.Fatalf("list --filter pool=%s through B exit %d: %s", row.GetPool(), code, stderr)
	}
	pooled := &orkstackv1.ListOrkersResponse{}
	if err := api.UnmarshalJSON([]byte(byPool), pooled); err != nil {
		t.Fatalf("list --filter --json: %v", err)
	}
	found := false
	for _, r := range pooled.GetOrkers() {
		found = found || r.GetOrkerId() == s.orkerID
		if r.GetUnavailableReason() == "" && r.GetPool() != row.GetPool() {
			t.Errorf("pool=%s listed %s of pool %s", row.GetPool(), r.GetOrkerId(), r.GetPool())
		}
	}
	if !found {
		t.Fatalf("pool=%s through B does not list %s: %v", row.GetPool(), s.orkerID, pooled.GetOrkers())
	}
	none, _, code := s.cli(0, "orker", "list", "--filter", "orker_id="+s.orkerID, "--filter", "cordoned=true")
	if code != 0 || !strings.Contains(none, "No Orkers match") {
		t.Fatalf("orker_id=%s cordoned=true after the uncordon = exit %d:\n%s", s.orkerID, code, none)
	}
	t.Logf("EVIDENCE filters: pool=%s lists %s through B; cordoned=true lists nothing after the uncordon",
		row.GetPool(), s.orkerID)
}

func TestIntegration_CordonIsAudited(t *testing.T) {
	s := newOrkerStack(t, 1)
	t.Cleanup(func() { s.writeProof("orker-cordon-audit.txt") })
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	const reason = "audited cordon"
	if _, stderr, code := s.cli(0, "orker", "cordon", s.orkerID, "--reason", reason); code != 0 {
		t.Fatalf("cordon exit %d: %s", code, stderr)
	}
	listed, _, code := s.cli(0, "admin", "audit", "--filter", "action^orker.", "--filter", "at>="+since, "--json")
	if code != 0 {
		t.Fatalf("admin audit exit %d", code)
	}
	resp := &orkstackv1.ListAuditRecordsResponse{}
	if err := api.UnmarshalJSON([]byte(listed), resp); err != nil {
		t.Fatalf("admin audit --json: %v", err)
	}
	var rec *orkstackv1.AuditRecord
	for _, r := range resp.GetRecords() {
		if r.GetAction() == "orker.cordon" && r.GetTarget() == s.orkerID {
			rec = r
		}
	}
	if rec == nil || !rec.GetSucceeded() || rec.GetNamespace() != "" || !strings.Contains(rec.GetDetail(), `reason="`+reason+`"`) {
		t.Fatalf("audit records = %v; want an orker.cordon of %s with its reason", resp.GetRecords(), s.orkerID)
	}
	t.Logf("EVIDENCE cordon audited: action=%s target=%s detail=%s", rec.GetAction(), rec.GetTarget(), rec.GetDetail())
	s.cli(0, "orker", "uncordon", s.orkerID)
}

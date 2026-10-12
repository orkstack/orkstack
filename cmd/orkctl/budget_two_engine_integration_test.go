//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests hold namespace budgets to their contract (docs/DEPLOY.md
// "Namespace budgets") against two real `ork`
// replicas sharing one bucket and one private Temporal namespace, and one
// real Orker on a private queue — the admission stack's shape, with both
// replicas measuring on every request (ORK_USAGE_CACHE_TTL=0) so nothing a
// replica remembers can stand in for what the shared store holds. Replica B
// exports metrics on its own admin listener, so the refusal counter can be
// read from the process that refused. Transcripts land under
// ORK_PROOF_DIR/budgets.

type budgetStack struct {
	*admissionStack
	artifactCfg artifacts.Config
	probe       string
}

func newBudgetStack(ctx context.Context, t *testing.T, label string) *budgetStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	partition := fmt.Sprintf("ork-budget-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")

	channel := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, channel)
	noCache := "ORK_USAGE_CACHE_TTL=0"
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, channel, noCache)
	bAdmin := freeAddress(t)
	replicaB, bLog := startObservedReplica(ctx, t, "ork-B", enginePath, address, partition, channel, bAdmin, noCache)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	stamp := time.Now().UnixNano()
	s := &budgetStack{
		admissionStack: &admissionStack{
			partition: partition,
			temporal:  temporalClient,
			a:         replicaA,
			b:         replicaB,
			bAdmin:    bAdmin,
			bLog:      bLog,
			namespace: fmt.Sprintf("analytics-%d-%d", os.Getpid(), stamp%1_000_000_000),
			orkflow:   "reconcile",
			slow:      "reconcile-slow",
		},
		artifactCfg: artifactCfg,
		probe:       "usage-probe",
	}
	fmt.Fprintf(&s.transcript, "# namespace budgets, two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "partition  %s\nnamespace  %s\nreplica A  %s\nreplica B  %s (metrics on %s)\n"+
		"both replicas ORK_USAGE_CACHE_TTL=0\n\n", partition, s.namespace, replicaA.addr, replicaB.addr, bAdmin)

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	for _, yaml := range []string{admissionOrkflowYAML(s.orkflow), admissionSlowOrkflowYAML(s.slow, 25),
		usageProbeYAML(s.probe)} {
		if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: yaml, Namespace: s.namespace,
		})); err != nil {
			t.Fatalf("ApplyOrkflow on replica A: %v", err)
		}
	}
	return s
}

// usageProbeYAML is the three-task usage probe: one task sized below the
// Orker's default, one that takes the default, and one that fails twice.
func usageProbeYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: usage probe.
tasks:
  - id: sized
    uses: shell
    timeout_seconds: 60
    resources:
      cpu: "500m"
      memory: 512Mi
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep 2"]
  - id: defaulted
    uses: shell
    timeout_seconds: 60
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep 1"]
  - id: flaky
    uses: shell
    timeout_seconds: 60
    retry:
      maximum_attempts: 2
      initial_interval_seconds: 1
    with:
      op: exec
      command: ["sh", "-c", "exit 1"]
`, name)
}

// runProbe submits the probe through replica, waits for it to end and be
// archived, and returns its id.
func (s *budgetStack) runProbe(ctx context.Context, t *testing.T, replica *engineReplica, archived int) string {
	t.Helper()
	runID := s.submitID(t, replica, s.probe)
	s.awaitStatus(ctx, t, replica, runID, orkstackv1.Status_STATUS_FAILED)
	awaitArchived(ctx, t, s.artifactCfg, s.namespace, archived)
	return runID
}

func (s *budgetStack) usage(t *testing.T, replica *engineReplica, args ...string) *orkstackv1.GetUsageResponse {
	t.Helper()
	stdout, _ := s.cli(t, replica, exitOK, append([]string{"--json", "-n", s.namespace, "stats", "usage"}, args...)...)
	out := &orkstackv1.GetUsageResponse{}
	if err := api.UnmarshalJSON([]byte(stdout), out); err != nil {
		t.Fatalf("decode stats usage --json: %v\n%s", err, stdout)
	}
	return out
}

func (s *budgetStack) attempts(t *testing.T, replica *engineReplica, runID string) []*orkstackv1.TaskAttempt {
	t.Helper()
	stdout, _ := s.cli(t, replica, exitOK, "--json", "-n", s.namespace, "run", "attempts", runID)
	out := &orkstackv1.ListTaskAttemptsResponse{}
	if err := api.UnmarshalJSON([]byte(stdout), out); err != nil {
		t.Fatalf("decode run attempts --json: %v\n%s", err, stdout)
	}
	return out.GetAttempts()
}

func (s *budgetStack) write(t *testing.T, name string) {
	t.Helper()
	proof.Write(t, "budgets", name, []byte(s.transcript.String()))
}

func closeTo(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func clearUsageComputed(resp *orkstackv1.GetUsageResponse) *orkstackv1.GetUsageResponse {
	out := proto.Clone(resp).(*orkstackv1.GetUsageResponse)
	out.ComputedAt = nil
	return out
}

func TestIntegration_UsageIsTheSumOfEveryAttemptsReservationTimesHeldTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newBudgetStack(ctx, t, "sum")
	defer s.write(t, "usage-sum.txt")

	runID := s.runProbe(ctx, t, s.a, 1)
	records := s.attempts(t, s.a, runID)
	if len(records) != 4 {
		t.Fatalf("run attempts lists %d records, want 4 (sized, defaulted, flaky x2): %v", len(records), records)
	}
	var seconds, cpu, gib float64
	for _, record := range records {
		want := &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1 << 30}
		if record.GetTaskId() == "sized" {
			want = &orkstackv1.ReservedResources{Cpu: 0.5, MemoryBytes: 536870912}
		}
		if !proto.Equal(record.GetResources(), want) {
			t.Errorf("%s attempt %d resources = %v, want %v", record.GetTaskId(), record.GetAttempt(), record.GetResources(), want)
		}
		held := float64(record.GetFinishedAt().AsTime().Sub(record.GetStartedAt().AsTime())) / float64(time.Second)
		seconds += held
		cpu += record.GetResources().GetCpu() * held
		gib += float64(record.GetResources().GetMemoryBytes()) / (1 << 30) * held
		fmt.Fprintf(&s.transcript, "record %s/%d outcome=%s held=%.9fs resources={cpu:%v memory_bytes:%d}\n",
			record.GetTaskId(), record.GetAttempt(), record.GetOutcome(), held,
			record.GetResources().GetCpu(), record.GetResources().GetMemoryBytes())
	}
	got := s.usage(t, s.b, "--since", "1h")
	total := got.GetTotal()
	fmt.Fprintf(&s.transcript, "\nrecomputed from the four records: task_seconds=%.9f cpu_seconds=%.9f gib_seconds=%.9f\n"+
		"stats usage on replica B:         task_seconds=%.9f cpu_seconds=%.9f gib_seconds=%.9f attempts=%d runsEvidenceComplete=%d\n\n",
		seconds, cpu, gib, total.GetTaskSeconds(), total.GetCpuSeconds(), total.GetGibSeconds(),
		total.GetAttempts(), got.GetRunsEvidenceComplete())
	t.Logf("EVIDENCE usage_sum recomputed cpu=%.9f gib=%.9f; replica B cpu=%.9f gib=%.9f attempts=%d",
		cpu, gib, total.GetCpuSeconds(), total.GetGibSeconds(), total.GetAttempts())
	if total.GetAttempts() != 4 || got.GetRunsEvidenceComplete() != 1 {
		t.Fatalf("usage on B: attempts=%d evidence_complete=%d, want 4 and 1", total.GetAttempts(), got.GetRunsEvidenceComplete())
	}
	if !closeTo(total.GetCpuSeconds(), cpu) || !closeTo(total.GetGibSeconds(), gib) || !closeTo(total.GetTaskSeconds(), seconds) {
		t.Fatalf("usage on B = %v / %v / %v, recomputed %v / %v / %v",
			total.GetTaskSeconds(), total.GetCpuSeconds(), total.GetGibSeconds(), seconds, cpu, gib)
	}
}

func TestIntegration_UsageAnswersIdenticallyOnBothReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newBudgetStack(ctx, t, "replicas")
	defer s.write(t, "usage-two-replicas.txt")

	since := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	s.runProbe(ctx, t, s.a, 1)
	until := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	args := []string{"--group-by", "task", "--since", since.Format(time.RFC3339), "--until", until.Format(time.RFC3339)}
	first := s.usage(t, s.a, args...)
	warm := s.usage(t, s.a, args...)
	cold := s.usage(t, s.b, args...)
	equal := proto.Equal(clearUsageComputed(first), clearUsageComputed(warm)) &&
		proto.Equal(clearUsageComputed(first), clearUsageComputed(cold))
	fmt.Fprintf(&s.transcript, "replica A (cold, then memo-warm) and replica B (cold): proto.Equal with computedAt cleared: %t\n\n", equal)
	t.Logf("EVIDENCE usage_two_replicas rows=%d attempts=%d proto_equal=%t", len(first.GetRows()), first.GetTotal().GetAttempts(), equal)
	if !equal {
		t.Fatalf("the replicas disagree:\nA  %v\nA' %v\nB  %v", first, warm, cold)
	}
	if len(first.GetRows()) != 3 {
		t.Fatalf("rows = %d, want one per task (3): %v", len(first.GetRows()), first.GetRows())
	}
}

func TestIntegration_AnEnforcedBudgetSetOnOneReplicaRefusesOnTheOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s := newBudgetStack(ctx, t, "enforce")
	defer s.write(t, "budget-enforced-two-replicas.txt")

	s.runProbe(ctx, t, s.a, 1)
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace,
		"--budget-cpu-seconds", "1", "--budget-period", "1h", "--budget-enforce")

	_, stderr := s.cli(t, s.b, exitError, "run", "submit", s.orkflow, "-n", s.namespace)
	for _, want := range []string{
		"submit run: failed_precondition: ",
		fmt.Sprintf("namespace %q has reserved ", s.namespace),
		" cpu-seconds over the last 1h0m0s, at or above its enforced budget of 1 (measured ",
		"`orkctl namespace update " + s.namespace + " --budget-cpu-seconds N`",
		"`orkctl namespace update " + s.namespace + " --budget-enforce=false`",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("refusal via B %q does not carry %q", stderr, want)
		}
	}
	got := s.usage(t, s.b, "--since", "1h")
	if !got.GetBudgetWindow() || !got.GetBudgetReached() {
		t.Fatalf("usage on B: budgetWindow=%v budgetReached=%v, want both true", got.GetBudgetWindow(), got.GetBudgetReached())
	}
	s.cli(t, s.b, exitOK, "-n", s.namespace, "stats", "usage", "--since", "1h")

	// The refusal is counted on the replica that refused and logged once.
	var refusedLine string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && refusedLine == "" {
		for _, line := range s.metricLines(ctx, t, "ork_runs_refused_total") {
			if strings.Contains(line, `ork_namespace="`+s.namespace+`"`) &&
				strings.Contains(line, `ork_refusal_reason="budget_exceeded"`) {
				refusedLine = line
			}
		}
		if refusedLine == "" {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if refusedLine == "" || !strings.HasSuffix(strings.TrimSpace(refusedLine), " 1") {
		t.Errorf("replica B's /metrics refusal line = %q, want ork_runs_refused_total{...budget_exceeded} 1", refusedLine)
	}
	var logLine string
	for _, line := range strings.Split(s.bLog.String(), "\n") {
		if strings.Contains(line, "run refused") && strings.Contains(line, "BUDGET_EXCEEDED") &&
			strings.Contains(line, s.namespace) {
			logLine = line
		}
	}
	if logLine == "" {
		t.Errorf("replica B logged no `run refused` line with BUDGET_EXCEEDED")
	}
	fmt.Fprintf(&s.transcript, "replica B /metrics (admin listener %s):\n  %s\nreplica B log:\n  %s\n\n",
		s.bAdmin, refusedLine, logLine)
	t.Logf("EVIDENCE budget_refusal_metric %s", refusedLine)
	t.Logf("EVIDENCE budget_refusal_log %s", logLine)

	// Soft through B; a submit through A is accepted and finishes.
	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace, "--budget-enforce=false")
	runID := s.submitID(t, s.a, s.orkflow)
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_SUCCEEDED)

	listed, _ := s.cli(t, s.b, exitOK, "--json", "admin", "audit", "--filter", "namespace="+s.namespace, "--filter", "at>="+since)
	audit := &orkstackv1.ListAuditRecordsResponse{}
	if err := protojson.Unmarshal([]byte(listed), audit); err != nil {
		t.Fatalf("admin audit --json: %v\n%s", err, listed)
	}
	var updates []string
	for _, rec := range audit.GetRecords() {
		if rec.GetAction() == "namespace.update" {
			updates = append(updates, rec.GetDetail())
		}
	}
	if len(updates) != 2 ||
		!strings.Contains(updates[0], "budget={cpu_seconds=1 gib_seconds=0 period=1h0m0s enforce=false}") ||
		!strings.Contains(updates[1], "budget={cpu_seconds=1 gib_seconds=0 period=1h0m0s enforce=true}") {
		t.Fatalf("namespace.update records = %q, want the enforce and the soften, each with budget={…}", updates)
	}
	fmt.Fprintf(&s.transcript, "audit namespace.update details via B, newest first:\n  %s\n  %s\n\n", updates[0], updates[1])
}

func TestIntegration_ABudgetNeverTouchesARunAlreadyAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newBudgetStack(ctx, t, "accepted")
	defer s.write(t, "budget-accepted-run.txt")

	s.runProbe(ctx, t, s.a, 1)
	runID := s.submitID(t, s.a, s.slow)
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_RUNNING)
	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace,
		"--budget-cpu-seconds", "1", "--budget-period", "1h", "--budget-enforce")
	_, stderr := s.cli(t, s.b, exitError, "run", "submit", s.orkflow, "-n", s.namespace)
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, "at or above its enforced budget of 1") {
		t.Errorf("submit past the budget = %q, want the budget refusal", stderr)
	}
	// The run accepted before the budget was reached finishes, untouched.
	s.awaitStatus(ctx, t, s.b, runID, orkstackv1.Status_STATUS_SUCCEEDED)
	s.cli(t, s.a, exitOK, "run", "get", runID, "-n", s.namespace)
	t.Logf("EVIDENCE budget_accepted_run %s SUCCEEDED after the budget refused a later submit", runID)
}

func TestIntegration_ReclaimedAttemptRecordsAreReportedNotCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newBudgetStack(ctx, t, "reclaimed")
	defer s.write(t, "usage-reclaimed.txt")

	since := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	older := s.runProbe(ctx, t, s.a, 1)
	newer := s.runProbe(ctx, t, s.a, 2)
	until := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	args := []string{"--since", since.Format(time.RFC3339), "--until", until.Format(time.RFC3339)}

	beforeA, beforeB := s.usage(t, s.a, args...), s.usage(t, s.b, args...)
	if beforeA.GetRunsEvidenceComplete() != 2 || beforeB.GetRunsEvidenceComplete() != 2 {
		t.Fatalf("before reclamation: evidence complete A=%d B=%d, want 2", beforeA.GetRunsEvidenceComplete(), beforeB.GetRunsEvidenceComplete())
	}
	// The cleanup worker's own call.
	store, err := artifacts.NewRunArchive(s.artifactCfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	if _, err := store.DeleteRunArtifacts(ctx, s.namespace, older); err != nil {
		t.Fatalf("DeleteRunArtifacts(%s): %v", older, err)
	}
	fmt.Fprintf(&s.transcript, "reclaimed run %s's attempt records (DeleteRunArtifacts, the cleanup worker's call)\n\n", older)

	afterA, afterB := s.usage(t, s.a, args...), s.usage(t, s.b, args...)
	for name, got := range map[string]*orkstackv1.GetUsageResponse{"A (memo-warm)": afterA, "B": afterB} {
		if got.GetRunsEvidenceMissing() != 1 || got.GetRunsEvidenceComplete() != 1 || got.GetTotal().GetAttempts() != 4 {
			t.Errorf("%s after reclamation: missing=%d complete=%d attempts=%d, want 1, 1, 4",
				name, got.GetRunsEvidenceMissing(), got.GetRunsEvidenceComplete(), got.GetTotal().GetAttempts())
		}
		if !got.GetCoverageSince().AsTime().After(beforeA.GetCoverageSince().AsTime()) {
			t.Errorf("%s coverage_since %v did not move past the reclaimed run (%v)", name,
				got.GetCoverageSince().AsTime(), beforeA.GetCoverageSince().AsTime())
		}
	}
	equal := proto.Equal(clearUsageComputed(afterA), clearUsageComputed(afterB))
	if !equal {
		t.Errorf("the replicas disagree after reclamation:\nA %v\nB %v", afterA, afterB)
	}
	fmt.Fprintf(&s.transcript, "after reclamation: A missing=%d B missing=%d, coverage %s -> %s, proto.Equal(A, B)=%t (newer run %s)\n\n",
		afterA.GetRunsEvidenceMissing(), afterB.GetRunsEvidenceMissing(), beforeA.GetCoverageSince().AsTime().Format(time.RFC3339Nano),
		afterA.GetCoverageSince().AsTime().Format(time.RFC3339Nano), equal, newer)
	t.Logf("EVIDENCE usage_reclaimed missing_A=%d missing_B=%d proto_equal=%t", afterA.GetRunsEvidenceMissing(),
		afterB.GetRunsEvidenceMissing(), equal)
	s.cli(t, s.b, exitOK, "-n", s.namespace, "stats", "usage", "--since", since.Format(time.RFC3339), "--until", until.Format(time.RFC3339))
}

func TestIntegration_ATriggerInvokeIntoAnExhaustedNamespaceIs412(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newBudgetStack(ctx, t, "trigger")
	defer s.write(t, "budget-trigger-412.txt")

	s.runProbe(ctx, t, s.a, 1)
	triggersA := orkstackv1connect.NewTriggerServiceClient(h2cClient(t), "http://"+s.a.addr)
	created, err := triggersA.CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		Namespace: s.namespace, OrkflowName: s.orkflow, Description: "partner reconcile",
	}))
	if err != nil {
		t.Fatalf("CreateTrigger on replica A: %v", err)
	}
	id, secret := created.Msg.GetTrigger().GetId(), created.Msg.GetSecret()
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace,
		"--budget-cpu-seconds", "1", "--budget-period", "1h", "--budget-enforce")

	path := strings.Replace(api.TriggerInvokePath, "{id}", id, 1)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+s.b.addr+path, strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build invoke: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := h2cClient(t).Do(req)
	if err != nil {
		t.Fatalf("POST %s on replica B: %v", path, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Fprintf(&s.transcript, "$ curl -X POST http://%s%s    # replica B, trigger created on A\n%d %s\n\n",
		s.b.addr, path, resp.StatusCode, body)
	t.Logf("EVIDENCE budget_trigger_invoke %d %s", resp.StatusCode, body)
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("invoke via B = %d, want 412: %s", resp.StatusCode, body)
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode invoke error: %v (%s)", err, body)
	}
	if envelope.Error.Code != "failed_precondition" || !strings.Contains(envelope.Error.Message, "at or above its enforced budget of 1") {
		t.Errorf("error envelope = %+v, want failed_precondition carrying the budget sentence", envelope.Error)
	}
	listed, _ := s.cli(t, s.b, exitOK, "--json", "run", "list", "-n", s.namespace, "--filter", "orkflow_name="+s.orkflow)
	if strings.Contains(listed, `"id"`) {
		t.Errorf("a refused invoke left a run of %s:\n%s", s.orkflow, listed)
	}
}

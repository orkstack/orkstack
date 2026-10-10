//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// admissionHoldReason is the hold reason of the worked incident.
const admissionHoldReason = "partner CI loop, INC-4412"

// admissionStack is two real `ork` replicas sharing one bucket and one
// private Temporal namespace, and one Orker on a private queue — the shape
// of stats_two_engine_integration_test.go. Replica B additionally exports
// metrics through the prometheus exporter on its own admin listener, so the
// refusal counter can be read from the process that refused.
type admissionStack struct {
	partition string
	temporal  client.Client
	a, b      *engineReplica
	bAdmin    string
	bLog      *lockedBuffer
	namespace string
	orkflow   string
	// slow is in flight for a while and then succeeds; crawl stays in flight
	// until it is cancelled.
	slow  string
	crawl string
	// transcript collects what each step said, in the CLI's own words.
	transcript strings.Builder
}

// lockedBuffer is a process log readable while the process still writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newAdmissionStack(ctx context.Context, t *testing.T, label string) *admissionStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	partition := fmt.Sprintf("ork-admission-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	channel := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, channel)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	bAdmin := freeAddress(t)
	replicaB, bLog := startObservedReplica(ctx, t, "ork-B", enginePath, address, partition, channel, bAdmin)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	stamp := time.Now().UnixNano()
	s := &admissionStack{
		partition: partition,
		temporal:  temporalClient,
		a:         replicaA,
		b:         replicaB,
		bAdmin:    bAdmin,
		bLog:      bLog,
		namespace: fmt.Sprintf("payments-%d-%d", os.Getpid(), stamp%1_000_000_000),
		orkflow:   "reconcile",
		slow:      "reconcile-slow",
		crawl:     "reconcile-crawl",
	}
	fmt.Fprintf(&s.transcript, "# namespace admission, two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "partition  %s\nnamespace  %s\nreplica A  %s\nreplica B  %s\n\n",
		partition, s.namespace, replicaA.addr, replicaB.addr)

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	for _, yaml := range []string{admissionOrkflowYAML(s.orkflow), admissionSlowOrkflowYAML(s.slow, 25),
		admissionSlowOrkflowYAML(s.crawl, 200)} {
		if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: yaml, Namespace: s.namespace,
		})); err != nil {
			t.Fatalf("ApplyOrkflow on replica A: %v", err)
		}
	}
	return s
}

// startObservedReplica is startReplica with the prometheus metrics exporter
// on an admin listener, and a log the test can read while it runs.
func startObservedReplica(
	ctx context.Context,
	t *testing.T,
	name, enginePath, temporalAddr, namespace, queue, adminAddr string,
	extraEnv ...string,
) (*engineReplica, *lockedBuffer) {
	t.Helper()
	addr := freeAddress(t)
	cmd := exec.CommandContext(ctx, enginePath)
	cmd.Env = append(os.Environ(),
		"ORK_HTTP_ADDR="+addr,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ARTIFACT_BACKEND=s3",
		"ORK_ADMIN_ADDR="+adminAddr,
		"OTEL_METRICS_EXPORTER=prometheus",
		localActivityCatalogEnv(t),
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	log := &lockedBuffer{}
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		t.Logf("%s log:\n%s", name, log.String())
	})
	httpClient := h2cClient(t)
	baseURL := "http://" + addr
	return &engineReplica{
		name:  name,
		addr:  addr,
		runs:  orkstackv1connect.NewRunServiceClient(httpClient, baseURL),
		flows: orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL),
	}, log
}

// cli runs the real orkctl against one replica, records the command and what
// it printed in the transcript, and requires the exit code.
func (s *admissionStack) cli(t *testing.T, replica *engineReplica, want int, args ...string) (string, string) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	var stdout, stderr bytes.Buffer
	code := Execute(append(append([]string{}, args...), "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n", strings.Join(quoteArgs(args), " "), replica.name)
	if stdout.Len() > 0 {
		s.transcript.WriteString(stdout.String())
	}
	if stderr.Len() > 0 {
		s.transcript.WriteString(stderr.String())
	}
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
	if code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args, replica.name, code, want, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func quoteArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.ContainsAny(arg, " ,") {
			arg = fmt.Sprintf("%q", arg)
		}
		out = append(out, arg)
	}
	return out
}

// submitID submits through the CLI and returns the accepted run's id.
func (s *admissionStack) submitID(t *testing.T, replica *engineReplica, orkflow string) string {
	t.Helper()
	stdout, _ := s.cli(t, replica, exitOK, "--json", "run", "submit", orkflow, "-n", s.namespace)
	var response struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil || response.Run.ID == "" {
		t.Fatalf("decode run submit --json: %v\n%s", err, stdout)
	}
	return response.Run.ID
}

// awaitStatus polls run get on a replica, by deadline, until the run reaches
// one of the given statuses.
func (s *admissionStack) awaitStatus(ctx context.Context, t *testing.T, replica *engineReplica, runID string, want ...orkstackv1.Status) orkstackv1.Status {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	var last orkstackv1.Status
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: runID, Namespace: s.namespace,
		}))
		if err == nil {
			last = got.Msg.GetRun().GetStatus()
			for _, status := range want {
				if last == status {
					fmt.Fprintf(&s.transcript, "run %s reached %s (read via %s)\n\n", runID, last, replica.name)
					return last
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %v (last %s)", runID, want, last)
	return last
}

// awaitRunningIndexed waits, by deadline, until the cluster's execution index
// — the record admission counts from — lists exactly want runs of the
// namespace in flight, as seen from replica.
func (s *admissionStack) awaitRunningIndexed(ctx context.Context, t *testing.T, replica *engineReplica, want int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		listed, err := replica.runs.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
			Namespace: s.namespace,
			Filters:   []*orkstackv1.FieldFilter{equalsFilter("status", "RUNNING")},
		}))
		if err == nil {
			last = len(listed.Msg.GetRuns())
			if last == want {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("the execution index lists %d runs of %s in flight via %s, want %d", last, s.namespace, replica.name, want)
}

func (s *admissionStack) writeTranscript(t *testing.T, name string) {
	t.Helper()
	proof.Write(t, "orkctl", name, []byte(s.transcript.String()))
}

// metricLines is every line of replica B's /metrics that names one of the
// given metric families.
func (s *admissionStack) metricLines(ctx context.Context, t *testing.T, families ...string) []string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.bAdmin+"/metrics", nil)
	if err != nil {
		t.Fatalf("build /metrics request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /metrics on replica B's admin listener: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	var out []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, family := range families {
			if strings.HasPrefix(line, family+"{") || strings.HasPrefix(line, family+" ") {
				out = append(out, line)
			}
		}
	}
	return out
}

func TestIntegration_AHoldSetOnOneReplicaRefusesSubmitsOnTheOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// The audit store is shared by every suite on the stack; bound the read
	// to this test's window so the listing stays within the scan budget.
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s := newAdmissionStack(ctx, t, "hold")
	defer s.writeTranscript(t, "admission-hold-two-replicas.txt")

	// The hold is set through replica A.
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--hold", admissionHoldReason)
	got, _ := s.cli(t, s.b, exitOK, "namespace", "get", s.namespace)
	if !strings.Contains(got, `"`+admissionHoldReason+`"`) {
		t.Fatalf("namespace get via B does not show the hold:\n%s", got)
	}

	runsBefore := s.metricLines(ctx, t, "ork_runs_total")

	// A submit through replica B, which never saw the write, is refused.
	_, stderr := s.cli(t, s.b, exitError, "run", "submit", s.orkflow, "-n", s.namespace)
	for _, want := range []string{
		"submit run: failed_precondition: ",
		fmt.Sprintf("namespace %q is on hold since ", s.namespace),
		`"` + admissionHoldReason + `". Release it with ` + "`orkctl namespace update " + s.namespace + " --release-hold`",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("refusal via B %q does not carry %q", stderr, want)
		}
	}
	// And through A too, for the same reason.
	_, stderr = s.cli(t, s.a, exitError, "run", "submit", s.orkflow, "-n", s.namespace)
	if !strings.Contains(stderr, admissionHoldReason) {
		t.Errorf("refusal via A %q does not carry the reason", stderr)
	}
	listed, _ := s.cli(t, s.b, exitOK, "run", "list", "-n", s.namespace)
	if !strings.Contains(listed, "No runs.") {
		t.Errorf("a refused submit left runs:\n%s", listed)
	}

	// The refusal is counted where counters belong, on the replica that
	// refused, and never on ork.runs.
	var refusedLine string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && refusedLine == "" {
		for _, line := range s.metricLines(ctx, t, "ork_runs_refused_total") {
			if strings.Contains(line, `ork_namespace="`+s.namespace+`"`) &&
				strings.Contains(line, `ork_refusal_reason="namespace_held"`) {
				refusedLine = line
			}
		}
		if refusedLine == "" {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if refusedLine == "" || !strings.HasSuffix(strings.TrimSpace(refusedLine), " 1") {
		t.Errorf("replica B's /metrics refusal line = %q, want ork_runs_refused_total{...namespace_held} 1", refusedLine)
	}
	runsAfter := s.metricLines(ctx, t, "ork_runs_total")
	if strings.Join(runsBefore, "\n") != strings.Join(runsAfter, "\n") {
		t.Errorf("ork_runs_total moved across a refusal:\nbefore %v\nafter  %v", runsBefore, runsAfter)
	}
	var logLine string
	for _, line := range strings.Split(s.bLog.String(), "\n") {
		if strings.Contains(line, "run refused") && strings.Contains(line, "NAMESPACE_HELD") &&
			strings.Contains(line, s.namespace) {
			logLine = line
		}
	}
	if logLine == "" {
		t.Errorf("replica B logged no `run refused` line with NAMESPACE_HELD")
	}
	fmt.Fprintf(&s.transcript, "replica B /metrics (admin listener %s):\n  %s\n", s.bAdmin, refusedLine)
	fmt.Fprintf(&s.transcript, "  ork_runs_total before the refusal: %v\n  ork_runs_total after the refusal:  %v\n",
		runsBefore, runsAfter)
	fmt.Fprintf(&s.transcript, "replica B stderr:\n  %s\n\n", logLine)

	// Released through B; a submit through A is admitted and finishes.
	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace, "--release-hold")

	// The audit trail, read back through B, says why the door was closed and
	// that it was reopened: the hold's namespace.update record names the
	// reason, the release's has no hold= key, newest first.
	listed, _ = s.cli(t, s.b, exitOK, "--json", "admin", "audit", "--filter", "namespace="+s.namespace, "--filter", "at>="+since)
	audit := &orkstackv1.ListAuditRecordsResponse{}
	if err := protojson.Unmarshal([]byte(listed), audit); err != nil {
		t.Fatalf("admin audit --json is not a ListAuditRecordsResponse: %v\n%s", err, listed)
	}
	var updates []*orkstackv1.AuditRecord
	for _, rec := range audit.GetRecords() {
		if rec.GetAction() == "namespace.update" {
			updates = append(updates, rec)
		}
	}
	if len(updates) != 2 {
		t.Fatalf("namespace.update records = %d, want the hold and the release:\n%s", len(updates), listed)
	}
	release, hold := updates[0].GetDetail(), updates[1].GetDetail()
	if strings.Contains(release, "hold=") || !strings.Contains(release, "max_running=0") {
		t.Errorf("release record detail = %q, want no hold= key", release)
	}
	if !strings.Contains(hold, `hold="`+admissionHoldReason+`" max_running=0`) {
		t.Errorf("hold record detail = %q, want hold=%q", hold, admissionHoldReason)
	}
	fmt.Fprintf(&s.transcript, "audit record details via %s, newest first:\n  release: %s\n  hold:    %s\n\n",
		s.b.name, release, hold)
	runID := s.submitID(t, s.a, s.orkflow)
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_SUCCEEDED)
	s.cli(t, s.b, exitOK, "run", "get", runID, "-n", s.namespace)
}

func TestIntegration_MaxRunningCountsRunsTheOtherReplicaStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newAdmissionStack(ctx, t, "cap")
	defer s.writeTranscript(t, "admission-max-running-two-replicas.txt")

	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--max-running", "2")
	first := s.submitID(t, s.a, s.crawl)
	second := s.submitID(t, s.a, s.crawl)
	// Replica B started neither; it counts both from the cluster's index.
	s.awaitRunningIndexed(ctx, t, s.b, 2)

	_, stderr := s.cli(t, s.b, exitError, "run", "submit", s.crawl, "-n", s.namespace)
	want := fmt.Sprintf("submit run: failed_precondition: namespace %q already has 2 runs in flight, "+
		"which is its max_running. Wait for one to finish, or raise the cap with "+
		"`orkctl namespace update %s --max-running N`", s.namespace, s.namespace)
	if !strings.Contains(stderr, want) {
		t.Fatalf("refusal via B =\n%s\nwant it to carry\n%s", stderr, want)
	}

	s.cli(t, s.b, exitOK, "run", "cancel", first, "-n", s.namespace)
	s.awaitStatus(ctx, t, s.b, first, orkstackv1.Status_STATUS_CANCELED)
	s.awaitRunningIndexed(ctx, t, s.b, 1)
	third := s.submitID(t, s.b, s.crawl)

	// Leave nothing executing behind this test.
	for _, id := range []string{second, third} {
		s.cli(t, s.b, exitOK, "run", "cancel", id, "-n", s.namespace)
	}
}

func TestIntegration_AHoldNeverTouchesARunAlreadyAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newAdmissionStack(ctx, t, "accepted")
	defer s.writeTranscript(t, "admission-accepted-run-two-replicas.txt")

	runID := s.submitID(t, s.a, s.slow)
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_RUNNING)
	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace, "--hold", admissionHoldReason)

	// The run accepted before the hold finishes, untouched.
	s.awaitStatus(ctx, t, s.b, runID, orkstackv1.Status_STATUS_SUCCEEDED)
	s.cli(t, s.a, exitOK, "run", "get", runID, "-n", s.namespace)

	// A submit after it is refused.
	_, stderr := s.cli(t, s.a, exitError, "run", "submit", s.orkflow, "-n", s.namespace)
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, admissionHoldReason) {
		t.Errorf("submit after the hold = %q, want the hold refusal", stderr)
	}
}

func TestIntegration_ATriggerInvokeIsRefusedByAHoldWith412(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newAdmissionStack(ctx, t, "trigger")
	defer s.writeTranscript(t, "admission-trigger-two-replicas.txt")

	triggersA := orkstackv1connect.NewTriggerServiceClient(h2cClient(t), "http://"+s.a.addr)
	created, err := triggersA.CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		Namespace: s.namespace, OrkflowName: s.orkflow, Description: "partner reconcile",
	}))
	if err != nil {
		t.Fatalf("CreateTrigger on replica A: %v", err)
	}
	id, secret := created.Msg.GetTrigger().GetId(), created.Msg.GetSecret()
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--hold", admissionHoldReason)

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
	if envelope.Error.Code != "failed_precondition" || !strings.Contains(envelope.Error.Message, admissionHoldReason) {
		t.Errorf("error envelope = %+v, want failed_precondition carrying the reason", envelope.Error)
	}
	listed, _ := s.cli(t, s.b, exitOK, "run", "list", "-n", s.namespace)
	if !strings.Contains(listed, "No runs.") {
		t.Errorf("a refused invoke left runs:\n%s", listed)
	}
}

func admissionOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: admission probe.
tasks:
  - id: work
    uses: shell
    timeout_seconds: 60
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "echo reconciled"]
`, name)
}

// admissionSlowOrkflowYAML runs long enough to be in flight while the test
// holds or caps its namespace, and then succeeds.
func admissionSlowOrkflowYAML(name string, seconds int) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: admission probe, in flight for a while.
tasks:
  - id: crawl
    uses: shell
    timeout_seconds: 240
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep %d"]
`, name, seconds)
}

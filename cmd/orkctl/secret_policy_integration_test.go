//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// secretFloor is the Orker floor every scenario's Orker runs with: the
// sandbox's and analytics' variables, never production's.
const secretFloor = "env:SANDBOX_*,env:ANALYTICS_*"

// secretStack is two real `ork` replicas sharing one bucket and one private
// Temporal namespace, and one real Orker on a private queue whose
// ORK_ORKER_SECRET_ALLOW floor is secretFloor and whose environment holds
// two minted values: SANDBOX_TOKEN (inside the floor) and PROD_DB_PASSWORD
// (outside it) — the shape of stats_two_engine_integration_test.go.
type secretStack struct {
	cfg        artifacts.Config
	temporal   client.Client
	a, b       *engineReplica
	namespace  string
	sandboxTok string
	prodPass   string
	transcript strings.Builder
}

func mintHex(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("mint: %v", err)
	}
	return hex.EncodeToString(raw)
}

func newSecretStack(ctx context.Context, t *testing.T, label string) *secretStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, cfg)

	partition := fmt.Sprintf("ork-secrets-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	s := &secretStack{
		cfg:        cfg,
		temporal:   temporalClient,
		namespace:  fmt.Sprintf("sandbox-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000),
		sandboxTok: mintHex(t),
		prodPass:   mintHex(t),
	}
	channel := idempotencyTaskQueue()
	startOrkerWithEnv(ctx, t, temporalClient, orkerPath, address, partition, channel,
		"ORK_ORKER_SECRET_ALLOW="+secretFloor,
		"SANDBOX_TOKEN="+s.sandboxTok,
		"PROD_DB_PASSWORD="+s.prodPass,
	)
	s.a = startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	s.b = startReplica(ctx, t, "ork-B", enginePath, address, partition, channel)
	s.a.awaitReady(ctx, t)
	s.b.awaitReady(ctx, t)

	fmt.Fprintf(&s.transcript, "# tenant-scoped secret policy, two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "partition  %s\nnamespace  %s\nreplica A  %s\nreplica B  %s\n", partition, s.namespace, s.a.addr, s.b.addr)
	fmt.Fprintf(&s.transcript, "orker      ORK_ORKER_SECRET_ALLOW=%s (SANDBOX_TOKEN and PROD_DB_PASSWORD minted by the test, values never printed)\n\n", secretFloor)
	return s
}

// startOrkerWithEnv is startOrker's sibling for a test that has to set more
// of the Orker's environment than the fixed six variables — here its secret
// floor and the variables a secret source names. It shares the replicas'
// bucket, as startIdempotencyOrker does, and waits for the cluster to see
// its poller.
func startOrkerWithEnv(
	ctx context.Context,
	t *testing.T,
	temporalClient client.Client,
	orkerPath, temporalAddr, temporalNamespace, queue string,
	extra ...string,
) {
	t.Helper()
	env := append([]string{
		"ORK_TEMPORAL_ADDRESS=" + temporalAddr,
		"ORK_TEMPORAL_NAMESPACE=" + temporalNamespace,
		"ORK_ORKER_QUEUE=" + queue,
		"ORK_ORKER_WORKDIR=" + bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	}, extra...)
	startProcess(ctx, t, "orker", orkerPath, env...)
	awaitActivityPoller(ctx, t, temporalClient, queue)
}

// cli runs the real orkctl against one replica, records the command and what
// it printed in the transcript, and requires the exit code.
func (s *secretStack) cli(t *testing.T, replica *engineReplica, want int, args ...string) (string, string) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	var stdout, stderr bytes.Buffer
	code := Execute(append(append([]string{}, args...), "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n", strings.Join(quoteArgs(args), " "), replica.name)
	s.transcript.WriteString(stdout.String())
	s.transcript.WriteString(stderr.String())
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
	if code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args, replica.name, code, want, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func (s *secretStack) note(format string, args ...any) {
	fmt.Fprintf(&s.transcript, format+"\n\n", args...)
}

// writeTranscript records the transcript, after checking it carries neither
// minted value: the transcript is evidence, and evidence must not leak.
func (s *secretStack) writeTranscript(t *testing.T, name string) {
	t.Helper()
	body := s.transcript.String()
	for label, value := range map[string]string{"SANDBOX_TOKEN": s.sandboxTok, "PROD_DB_PASSWORD": s.prodPass} {
		if strings.Contains(body, value) {
			t.Errorf("the transcript carries the value of %s", label)
			body = strings.ReplaceAll(body, value, "<"+label+" REDACTED BY THE TEST>")
		}
	}
	proof.Write(t, "orkctl", name, []byte(body))
}

// document writes an Orkflow to a file for `orkctl orkflow validate`/`apply`.
func (s *secretStack) document(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orkflow.ork.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write orkflow: %v", err)
	}
	return path
}

func (s *secretStack) submitID(t *testing.T, replica *engineReplica, orkflow string) string {
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

func (s *secretStack) awaitStatus(ctx context.Context, t *testing.T, replica *engineReplica, runID string, want ...orkstackv1.Status) orkstackv1.Status {
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
					s.note("run %s reached %s (read via %s)", runID, last, replica.name)
					return last
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %v (last %s)", runID, want, last)
	return last
}

// runObjects reads every object under metadata/ that belongs to one run —
// its archive body, snapshots, log chunks, attempt records, commits and
// object list — straight from the bucket, keyed by object key.
func (s *secretStack) runObjects(ctx context.Context, t *testing.T, runID string) map[string][]byte {
	t.Helper()
	client := auditRawS3Client(t, s.cfg)
	out := map[string][]byte{}
	for _, prefix := range []string{
		"metadata/runs/" + runID,
		"metadata/snapshots/" + s.namespace + "/" + runID + "/",
		"metadata/logs/" + runID + "/",
		"metadata/attempts/" + runID + "/",
		"metadata/commits/" + s.namespace + "/" + runID + "/",
		"metadata/index/run-objects/" + runID,
	} {
		pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.cfg.S3Bucket), Prefix: aws.String(prefix),
		})
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("list %s: %v", prefix, err)
			}
			for _, object := range page.Contents {
				got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.S3Bucket), Key: object.Key})
				if err != nil {
					t.Fatalf("get %s: %v", aws.ToString(object.Key), err)
				}
				body, err := io.ReadAll(got.Body)
				_ = got.Body.Close()
				if err != nil {
					t.Fatalf("read %s: %v", aws.ToString(object.Key), err)
				}
				out[aws.ToString(object.Key)] = body
			}
		}
	}
	return out
}

// requireValueAbsent is the "appears nowhere" assertion: every object of the
// run, read back from the bucket, searched for the value by substring. It
// requires the archive body and at least one attempt record to be among
// them, so an empty search cannot pass for a clean one.
func (s *secretStack) requireValueAbsent(ctx context.Context, t *testing.T, runID, label, value string) {
	t.Helper()
	var objects map[string][]byte
	deadline := time.Now().Add(2 * time.Minute)
	for {
		objects = s.runObjects(ctx, t, runID)
		var archived, attempt bool
		for key := range objects {
			archived = archived || strings.HasPrefix(key, "metadata/runs/")
			attempt = attempt || strings.HasPrefix(key, "metadata/attempts/")
		}
		if archived && attempt {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s: archive body present=%t, attempt record present=%t after 2m", runID, archived, attempt)
		}
		time.Sleep(500 * time.Millisecond)
	}
	var searched []string
	for key, body := range objects {
		searched = append(searched, key)
		if bytes.Contains(body, []byte(value)) {
			t.Fatalf("object %s carries the value of %s", key, label)
		}
	}
	s.note("searched %d objects of run %s in bucket %s for the value of %s: 0 matches\n  %s",
		len(objects), runID, s.cfg.S3Bucket, label, strings.Join(searched, "\n  "))
}

func secretRefusal(namespace, source, task, secret, allowed string) string {
	return fmt.Sprintf("namespace %q may not resolve %q (task %q, secret %q); allowed: %s",
		namespace, source, task, secret, allowed)
}

// secretOrkflowYAML is one task naming one secret, with the shape
// statsOrkflowYAML uses.
func secretOrkflowYAML(name, taskID, secretName, source, command string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: secret policy probe.
tasks:
  - id: %s
    uses: shell
    timeout_seconds: 120
    secrets:
      - name: %s
        source: %s
    with:
      op: exec
      command: ["sh", "-c", %q]
`, name, taskID, secretName, source, command)
}

func exfilYAML() string {
	return secretOrkflowYAML("exfil", "grab", "db", "env:PROD_DB_PASSWORD", "cat /orkstack/input/secrets/db")
}

func fetchYAML(name, command string) string {
	return secretOrkflowYAML(name, "fetch", "token", "env:SANDBOX_TOKEN", command)
}

func TestIntegration_ASecretPolicyRefusesValidateAndApplyWithTheFieldPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newSecretStack(ctx, t, "validate")
	defer s.writeTranscript(t, "secret-policy-validate-apply.txt")

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace, "--allow-secret", "env:SANDBOX_*")
	path := s.document(t, exfilYAML())
	sentence := secretRefusal(s.namespace, "env:PROD_DB_PASSWORD", "grab", "db", "env:SANDBOX_*")

	stdout, _ := s.cli(t, s.b, exitError, "orkflow", "validate", path, "-n", s.namespace)
	if !strings.Contains(stdout, "tasks[0].secrets[0].source: "+sentence) {
		t.Fatalf("validate via B = %q, want the field-anchored sentence", stdout)
	}
	_, stderr := s.cli(t, s.b, exitError, "apply", path, "-n", s.namespace)
	if !strings.Contains(stderr, "invalid_argument: "+sentence) {
		t.Fatalf("apply via B = %q, want invalid_argument with the sentence", stderr)
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		listed, _ := s.cli(t, replica, exitOK, "orkflow", "list", "-n", s.namespace)
		if !strings.Contains(listed, "No orkflows.") {
			t.Fatalf("a refused apply stored something (via %s):\n%s", replica.name, listed)
		}
	}
}

func TestIntegration_AListTightenedOnOneReplicaRefusesASubmitOnTheOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// The audit read below is bounded to this test's own records: the shared
	// bucket's default window can hold more than the scan budget.
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s := newSecretStack(ctx, t, "tighten")
	defer s.writeTranscript(t, "secret-policy-two-replicas.txt")

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	s.cli(t, s.a, exitOK, "apply", s.document(t, exfilYAML()), "-n", s.namespace)
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--allow-secret", "env:SANDBOX_*")

	_, stderr := s.cli(t, s.b, exitError, "run", "submit", "exfil", "-n", s.namespace)
	sentence := secretRefusal(s.namespace, "env:PROD_DB_PASSWORD", "grab", "db", "env:SANDBOX_*")
	if !strings.Contains(stderr, "submit run: invalid_argument: "+sentence) {
		t.Fatalf("submit via B = %q, want invalid_argument with the sentence", stderr)
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		listed, _ := s.cli(t, replica, exitOK, "run", "list", "-n", s.namespace)
		if !strings.Contains(listed, "No runs.") {
			t.Fatalf("a refused submit left a run (via %s):\n%s", replica.name, listed)
		}
	}

	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace, "--clear-allowed-secrets")
	runID := s.submitID(t, s.a, "exfil")
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_FAILED)
	got, _ := s.cli(t, s.b, exitOK, "run", "get", runID, "-n", s.namespace)
	if !strings.Contains(got, "SECRET_NOT_PERMITTED") {
		t.Fatalf("run get via B does not show the Orker's refusal:\n%s", got)
	}

	listed, _ := s.cli(t, s.b, exitOK, "--json", "-n", s.namespace, "admin", "audit", "--since", since)
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
		t.Fatalf("namespace.update records = %d, want the tighten and the clear:\n%s", len(updates), listed)
	}
	cleared, tighten := updates[0].GetDetail(), updates[1].GetDetail()
	if strings.Contains(cleared, "allowed_secrets=") {
		t.Errorf("clear record detail = %q, want no allowed_secrets=", cleared)
	}
	if !strings.Contains(tighten, "allowed_pools=[] allowed_secrets=[env:SANDBOX_*] run_retention=") {
		t.Errorf("tighten record detail = %q, want allowed_secrets=[env:SANDBOX_*] after allowed_pools=", tighten)
	}
	s.note("audit record details via %s, newest first:\n  clear:   %s\n  tighten: %s", s.b.name, cleared, tighten)
}

func TestIntegration_TheOrkerFloorRefusesWhatTheNamespaceAllows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newSecretStack(ctx, t, "floor")
	defer s.writeTranscript(t, "secret-policy-orker-floor.txt")

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	s.cli(t, s.a, exitOK, "apply", s.document(t, exfilYAML()), "-n", s.namespace)
	runID := s.submitID(t, s.a, "exfil")
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_FAILED)

	stdout, _ := s.cli(t, s.b, exitOK, "--json", "run", "get", runID, "-n", s.namespace)
	var run struct {
		Run struct {
			TaskRuns []struct {
				TaskID string `json:"taskId"`
				Status string `json:"status"`
				Error  struct {
					Code     string `json:"code"`
					Message  string `json:"message"`
					Category string `json:"category"`
				} `json:"error"`
			} `json:"taskRuns"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &run); err != nil || len(run.Run.TaskRuns) != 1 {
		t.Fatalf("decode run get --json: %v\n%s", err, stdout)
	}
	task := run.Run.TaskRuns[0]
	if !strings.HasSuffix(task.Status, "FAILED") || task.Error.Code != "SECRET_NOT_PERMITTED" ||
		!strings.HasSuffix(task.Error.Category, "CONTRACT_VIOLATION") {
		t.Fatalf("task = %+v, want FAILED / SECRET_NOT_PERMITTED / CONTRACT_VIOLATION", task)
	}
	want := `secret "db": "env:PROD_DB_PASSWORD" matches none of env:SANDBOX_*, env:ANALYTICS_* (this orker's ORK_ORKER_SECRET_ALLOW)`
	if !strings.Contains(task.Error.Message, want) {
		t.Errorf("task error message = %q, want it to carry %q", task.Error.Message, want)
	}

	attempts, _ := s.cli(t, s.b, exitOK, "--json", "run", "attempts", runID, "--task", "grab", "-n", s.namespace)
	var listed struct {
		Attempts []json.RawMessage `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(attempts), &listed); err != nil || len(listed.Attempts) != 1 {
		t.Fatalf("run attempts --json = %v, %s; want exactly one attempt (a policy refusal is never retried)", err, attempts)
	}
	logs, _ := s.cli(t, s.b, exitOK, "run", "logs", runID, "-n", s.namespace)
	if strings.Contains(logs, s.prodPass) {
		t.Fatal("run logs carry the PROD_DB_PASSWORD value")
	}
	s.requireValueAbsent(ctx, t, runID, "PROD_DB_PASSWORD", s.prodPass)
}

func TestIntegration_APermittedSecretResolvesAndItsValueNeverLeavesTheOrker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newSecretStack(ctx, t, "permitted")
	defer s.writeTranscript(t, "secret-policy-permitted.txt")

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace, "--allow-secret", "env:SANDBOX_*")
	s.cli(t, s.a, exitOK, "apply",
		s.document(t, fetchYAML("fetch", "sha256sum /orkstack/input/secrets/token")), "-n", s.namespace)
	runID := s.submitID(t, s.b, "fetch")
	s.awaitStatus(ctx, t, s.b, runID, orkstackv1.Status_STATUS_SUCCEEDED)

	sum := sha256.Sum256([]byte(s.sandboxTok))
	digest := hex.EncodeToString(sum[:])
	var logs string
	deadline := time.Now().Add(time.Minute)
	for {
		logs, _ = s.cli(t, s.a, exitOK, "run", "logs", runID, "-n", s.namespace)
		if strings.Contains(logs, digest) || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !strings.Contains(logs, digest+"  /orkstack/input/secrets/token") {
		t.Fatalf("run logs = %q, want the sha256sum line of the minted token", logs)
	}
	if strings.Contains(logs, s.sandboxTok) {
		t.Fatal("run logs carry the SANDBOX_TOKEN value")
	}
	s.note("sha256 of the minted SANDBOX_TOKEN computed by the test: %s (the line above is the container's own)", digest)
	s.requireValueAbsent(ctx, t, runID, "SANDBOX_TOKEN", s.sandboxTok)
	s.note("grep -c <minted SANDBOX_TOKEN> on this transcript: %d", strings.Count(s.transcript.String(), s.sandboxTok))
}

func TestIntegration_TraversalIsRefusedAtApplyAndAtSubmit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newSecretStack(ctx, t, "traversal")
	defer s.writeTranscript(t, "secret-policy-traversal.txt")

	const pattern = "file:/etc/orkstack/secrets/sandbox/*"
	const source = "file:/etc/orkstack/secrets/sandbox/../payments/db"
	path := s.document(t, secretOrkflowYAML("climb", "grab", "db", source, "cat /orkstack/input/secrets/db"))
	sentence := fmt.Sprintf(`namespace %q may not resolve %q (task "grab", secret "db"): a "file:" locator with a ".." segment is refused, never normalized; allowed: %s`,
		s.namespace, source, pattern)

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace, "--allow-secret", pattern)
	stdout, _ := s.cli(t, s.b, exitError, "orkflow", "validate", path, "-n", s.namespace)
	if !strings.Contains(stdout, "tasks[0].secrets[0].source: "+sentence) {
		t.Fatalf("validate = %q, want the traversal sentence", stdout)
	}
	_, stderr := s.cli(t, s.b, exitError, "apply", path, "-n", s.namespace)
	if !strings.Contains(stderr, sentence) {
		t.Fatalf("apply = %q, want the traversal sentence", stderr)
	}

	// A definition applied while unrestricted is refused at submit.
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--clear-allowed-secrets")
	s.cli(t, s.a, exitOK, "apply", path, "-n", s.namespace)
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--allow-secret", pattern)
	_, stderr = s.cli(t, s.b, exitError, "run", "submit", "climb", "-n", s.namespace)
	if !strings.Contains(stderr, "invalid_argument: "+sentence) {
		t.Fatalf("submit = %q, want invalid_argument with the traversal sentence", stderr)
	}
}

func TestIntegration_ATightenedListNeverTouchesARunAlreadyAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newSecretStack(ctx, t, "accepted")
	defer s.writeTranscript(t, "secret-policy-accepted-run.txt")

	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace, "--allow-secret", "env:SANDBOX_*")
	s.cli(t, s.a, exitOK, "apply", s.document(t, fetchYAML("slow-fetch",
		"sleep 20; sha256sum /orkstack/input/secrets/token")), "-n", s.namespace)
	runID := s.submitID(t, s.a, "slow-fetch")
	s.awaitStatus(ctx, t, s.a, runID, orkstackv1.Status_STATUS_RUNNING)

	// Tightened to exclude the source while the run is in flight.
	s.cli(t, s.b, exitOK, "namespace", "update", s.namespace, "--allow-secret", "env:ANALYTICS_*")
	got, err := s.a.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: s.namespace}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if status := got.Msg.GetRun().GetStatus(); status != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("the run was %s when the list tightened; the test did not observe an in-flight run", status)
	}
	s.note("list tightened while run %s was %s", runID, got.Msg.GetRun().GetStatus())
	s.awaitStatus(ctx, t, s.b, runID, orkstackv1.Status_STATUS_SUCCEEDED)

	_, stderr := s.cli(t, s.a, exitError, "run", "submit", "slow-fetch", "-n", s.namespace)
	if !strings.Contains(stderr, secretRefusal(s.namespace, "env:SANDBOX_TOKEN", "fetch", "token", "env:ANALYTICS_*")) {
		t.Fatalf("the next submit = %q, want the refusal", stderr)
	}
}

// TestIntegration_AMalformedFloorStopsTheOrkerAtBoot: a floor the Orker
// cannot read stops the process, naming the variable and the entry, rather
// than booting with a floor that silently permits nothing.
func TestIntegration_AMalformedFloorStopsTheOrkerAtBoot(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	partition := fmt.Sprintf("ork-secrets-boot-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	dialIsolatedNamespace(ctx, t, address, partition)
	orkerPath := buildBinary(t, repoRoot(t), t.TempDir(), "orker")

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# the Orker refuses a malformed secret floor at boot (%s)\n\n", time.Now().UTC().Format(time.RFC3339))
	for _, tc := range []struct {
		floor string
		want  []string
	}{
		{"file:relative", []string{"ORK_ORKER_SECRET_ALLOW", "file:relative"}},
		{"env:SANDBOX_*,vault:*", []string{"ORK_ORKER_SECRET_ALLOW", "vault:*", "ORK_ORKER_SECRET_SCHEMES"}},
	} {
		bootCtx, bootCancel := context.WithTimeout(ctx, 90*time.Second)
		cmd := exec.CommandContext(bootCtx, orkerPath)
		cmd.Env = append(os.Environ(),
			"ORK_TEMPORAL_ADDRESS="+address,
			"ORK_TEMPORAL_NAMESPACE="+partition,
			"ORK_ORKER_QUEUE="+idempotencyTaskQueue()+"-boot",
			"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
			"ORK_ARTIFACT_BACKEND=local",
			"ORK_ARTIFACT_DIR="+sharedLocalArtifactDir(t),
			"ORK_ORKER_SECRET_ALLOW="+tc.floor,
		)
		output, err := cmd.CombinedOutput()
		deadlineHit := bootCtx.Err() != nil
		bootCancel()
		fmt.Fprintf(&transcript, "$ ORK_ORKER_SECRET_ALLOW=%q orker\n%s(exit: %v)\n\n", tc.floor, output, err)
		if deadlineHit {
			t.Fatalf("orker with floor %q was still running at the deadline; it booted", tc.floor)
		}
		if err == nil {
			t.Fatalf("orker with floor %q exited 0", tc.floor)
		}
		for _, want := range tc.want {
			if !strings.Contains(string(output), want) {
				t.Errorf("orker output for floor %q does not name %s:\n%s", tc.floor, want, output)
			}
		}
	}
	proof.Write(t, "orkctl", "secret-policy-orker-boot.txt", []byte(transcript.String()))
}

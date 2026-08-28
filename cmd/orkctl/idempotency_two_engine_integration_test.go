//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// idempotencyKey is the key both replicas submit under, and submittedInto is
// the ork namespace they submit it into: neither request names one, and an
// unnamed namespace means the default (M10.3). Together they are the
// coordinate whose hash names the claim workflow, so the test can look that
// workflow up without asking the implementation what it called it.
const (
	idempotencyKey = "m8-2-two-engine-key"
	submittedInto  = artifacts.DefaultNamespace
)

// claimWorkflowIDPrefix, claimScopeSeparator and claimWorkflowIDHexLen restate
// the documented shape of the claim workflow's id: `ork-idem-` + the first 32
// hex digits of sha256 over the coordinate the key occupies. Restating it here
// rather than calling engine.ClaimWorkflowID is deliberate: this test has to
// be able to disagree with the implementation.
//
// That coordinate is `namespace/key`, not the key alone. A key is the caller's
// own word for one submission and two tenants pick the same words, so M10.3
// made it a coordinate inside a namespace exactly as an orkflow name is
// (api.submission.claimKey): one unscoped binding would be shared by the whole
// deployment, and the second tenant would be answered with the first tenant's
// run id while its own work was never started. So the lookup below is live
// evidence for the scope too — drop it from the implementation and the claim
// exists at an id this test never asks about.
const (
	claimWorkflowIDPrefix = "ork-idem-"
	claimScopeSeparator   = "/"
	claimWorkflowIDHexLen = 32
)

// TestIntegration_TwoEnginesDedupeOneIdempotencyKey is the falsifying test for
// M8.2. Two real `ork` processes share one Temporal namespace, one Orker queue
// and one MinIO bucket — the production topology behind a load balancer — and
// receive the same idempotency key at the same moment.
//
// A process-local dedupe map cannot survive this: the replicas hold two maps
// and two run stores, so each one believes it is the first to see the key and
// starts its own run. The assertions below are what a server-side authority
// makes true and a process-local one cannot.
//
// Isolation (AGENTS.md §6): the namespace is unique per test run, because the
// central assertion counts every RunWorkflow execution in the namespace. That
// count is only a statement about this test if nothing else shares it.
func TestIntegration_TwoEnginesDedupeOneIdempotencyKey(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	temporalClient := dialIsolatedNamespace(ctx, t, address, namespace)
	// A claim is held for its TTL by design, so the test terminates its own
	// rather than leaving a 24-hour execution behind on the dev cluster.
	t.Cleanup(func() {
		_ = temporalClient.TerminateWorkflow(context.Background(),
			claimWorkflowIDFor(submittedInto, idempotencyKey), "", "m8.2 integration test cleanup")
	})

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, namespace, queue)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	// The orkflow is applied through A only. B answers for it because both
	// replicas read the same durable store, which is what makes them replicas
	// rather than two independent engines.
	orkflowName := fmt.Sprintf("idem-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: idempotencyOrkflowYAML(orkflowName),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, replicaB.flows, orkflowName)

	// ---- the race: one key, two replicas, released together ------------------
	type submission struct {
		replica string
		run     *orkstackv1.Run
		err     error
	}
	results := make([]submission, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, replica := range []*engineReplica{replicaA, replicaB} {
		wg.Add(1)
		go func(i int, replica *engineReplica) {
			defer wg.Done()
			<-start
			response, err := replica.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName:    orkflowName,
				IdempotencyKey: idempotencyKey,
			}))
			results[i] = submission{replica: replica.name, err: err}
			if err == nil {
				results[i].run = response.Msg.GetRun()
			}
		}(i, replica)
	}
	close(start)
	wg.Wait()

	for _, got := range results {
		if got.err != nil {
			t.Fatalf("SubmitRun on %s: %v", got.replica, got.err)
		}
	}

	// ---- acceptance 1 + 4: both callers see one run -------------------------
	a, b := results[0].run, results[1].run
	t.Logf("%s returned run %s", results[0].replica, a.GetId())
	t.Logf("%s returned run %s", results[1].replica, b.GetId())
	if a.GetId() != b.GetId() {
		t.Fatalf("one idempotency key produced two runs: %s returned %q, %s returned %q",
			results[0].replica, a.GetId(), results[1].replica, b.GetId())
	}
	runID := a.GetId()

	// Both descriptions have to be of the same run. The loser answers from the
	// claim, so the orkflow it reports is the one the winner claimed under.
	for i, run := range []*orkstackv1.Run{a, b} {
		if run.GetOrkflowName() != orkflowName || run.GetOrkflowVersion() != "0.1.0" {
			t.Errorf("%s described the run as %s@%s, want %s@0.1.0",
				results[i].replica, run.GetOrkflowName(), run.GetOrkflowVersion(), orkflowName)
		}
	}

	// ---- the server's own count: exactly one run execution ------------------
	// Held over a settle window so an eventually consistent visibility index
	// cannot report "one" merely because the second run has not landed yet.
	assertSingleRunExecution(ctx, t, temporalClient, namespace, runID)

	// ---- the claim itself: one execution, at the id the coordinate derives --
	claimID := claimWorkflowIDFor(submittedInto, idempotencyKey)
	assertSingleClaimExecution(ctx, t, temporalClient, namespace, claimID)

	// ---- the claimed run is real, and it executes ---------------------------
	// Asked of the server, not of a replica: which replica's projection worker
	// executed a given lifecycle event is arbitrary, so a run's live status in
	// one process's read model is not a fact about the run. (Making every
	// replica answer for every run is M8.3.) What both replicas must do here
	// is resolve the claimed id to the claimed orkflow.
	awaitWorkflowCompleted(ctx, t, temporalClient, runID)
	for _, replica := range []*engineReplica{replicaA, replicaB} {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
		if err != nil {
			t.Fatalf("GetRun(%s) on %s: %v", runID, replica.name, err)
		}
		if got.Msg.GetRun().GetId() != runID ||
			got.Msg.GetRun().GetOrkflowName() != orkflowName {
			t.Errorf("%s answers GetRun(%s) with %s@%s", replica.name, runID,
				got.Msg.GetRun().GetId(), got.Msg.GetRun().GetOrkflowName())
		}
	}

	// ---- the binding outlives the run: a later repeat still dedupes ---------
	for _, replica := range []*engineReplica{replicaA, replicaB} {
		repeat, err := replica.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName:    orkflowName,
			IdempotencyKey: idempotencyKey,
		}))
		if err != nil {
			t.Fatalf("repeat SubmitRun on %s: %v", replica.name, err)
		}
		if got := repeat.Msg.GetRun().GetId(); got != runID {
			t.Errorf("repeat submission on %s returned %q, want the claimed run %q",
				replica.name, got, runID)
		}
	}
	assertSingleRunExecution(ctx, t, temporalClient, namespace, runID)
	writeIdempotencyTranscript(t, namespace, runID, claimID, results[0].replica, results[1].replica)
}

// claimWorkflowIDFor renders the documented claim workflow id for a key
// submitted into a namespace.
func claimWorkflowIDFor(namespace, key string) string {
	sum := sha256.Sum256([]byte(namespace + claimScopeSeparator + key))
	return claimWorkflowIDPrefix + hex.EncodeToString(sum[:])[:claimWorkflowIDHexLen]
}

// assertSingleRunExecution asserts the namespace holds exactly one RunWorkflow
// execution, and that it is the run both callers were told about.
//
// The count is polled over a settle window rather than read once: standard
// visibility is updated asynchronously, so a single read taken immediately
// after the submissions could report one execution while a second one is still
// in flight — the exact failure this test exists to catch.
func assertSingleRunExecution(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	namespace, runID string,
) {
	t.Helper()
	const settle = 8 * time.Second
	deadline := time.Now().Add(settle)
	var ids []string
	for {
		ids = listExecutions(ctx, t, c, namespace,
			fmt.Sprintf("WorkflowType = %q", engine.RunWorkflowType))
		if len(ids) > 1 {
			t.Fatalf("namespace %s holds %d RunWorkflow executions %v, want exactly one: "+
				"the idempotency key started a run on each replica", namespace, len(ids), ids)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(ids) != 1 {
		t.Fatalf("namespace %s holds %d RunWorkflow executions %v, want exactly one",
			namespace, len(ids), ids)
	}
	if ids[0] != runID {
		t.Fatalf("the only run execution is %q, but both callers were told %q", ids[0], runID)
	}
}

// assertSingleClaimExecution asserts the key's claim exists exactly once, at
// the workflow id its coordinate's hash names.
func assertSingleClaimExecution(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	namespace, claimID string,
) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ids := listExecutions(ctx, t, c, namespace,
			fmt.Sprintf("WorkflowId = %q", claimID))
		switch {
		case len(ids) == 1:
			t.Logf("claim workflow %s holds the key", claimID)
			return
		case len(ids) > 1:
			t.Fatalf("claim id %s names %d executions %v, want one", claimID, len(ids), ids)
		}
		if time.Now().After(deadline) {
			t.Fatalf("no claim workflow at %s: the idempotency key is not held on the server",
				claimID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// listExecutions runs a visibility query and returns the matching workflow ids.
func listExecutions(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	namespace, query string,
) []string {
	t.Helper()
	var ids []string
	var token []byte
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			t.Fatalf("ListWorkflow(%s): %v", query, err)
		}
		for _, execution := range resp.GetExecutions() {
			ids = append(ids, execution.GetExecution().GetWorkflowId())
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return ids
		}
	}
}

// awaitWorkflowCompleted waits for the claimed run's Temporal execution to
// close successfully. The status comes from the server, so it is the same
// answer whichever replica started the run and whichever executed its events.
func awaitWorkflowCompleted(ctx context.Context, t *testing.T, c client.Client, runID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last enumspb.WorkflowExecutionStatus
	for time.Now().Before(deadline) {
		described, err := c.DescribeWorkflowExecution(ctx, runID, "")
		if err == nil {
			last = described.GetWorkflowExecutionInfo().GetStatus()
			switch last {
			case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
				t.Logf("run %s completed on the cluster", runID)
				return
			case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
			default:
				t.Fatalf("run %s closed as %s, want COMPLETED", runID, last)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s did not complete within the deadline (last status %s)", runID, last)
}

// engineReplica is one running `ork` process and the Connect clients aimed at
// it.
type engineReplica struct {
	name  string
	addr  string
	runs  orkstackv1connect.RunServiceClient
	flows orkstackv1connect.OrkflowServiceClient
}

func (r *engineReplica) awaitReady(ctx context.Context, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.flows.ListOrkflows(ctx,
			connect.NewRequest(&orkstackv1.ListOrkflowsRequest{})); err == nil {
			t.Logf("%s ready at %s", r.name, r.addr)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s at %s did not become ready", r.name, r.addr)
}

// startReplica boots one real `ork` process on its own port, sharing the
// namespace, the Orker queue and the MinIO bucket with its sibling.
func startReplica(
	ctx context.Context,
	t *testing.T,
	name, enginePath, temporalAddr, namespace, queue string,
) *engineReplica {
	t.Helper()
	addr := freeAddress(t)
	startProcess(ctx, t, name, enginePath,
		"ORK_HTTP_ADDR="+addr,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ARTIFACT_BACKEND=s3",
	)
	httpClient := h2cClient(t)
	baseURL := "http://" + addr
	return &engineReplica{
		name:  name,
		addr:  addr,
		runs:  orkstackv1connect.NewRunServiceClient(httpClient, baseURL),
		flows: orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL),
	}
}

// startIdempotencyOrker boots the one Orker both replicas dispatch to.
func startIdempotencyOrker(
	ctx context.Context,
	t *testing.T,
	temporalClient client.Client,
	orkerPath, temporalAddr, namespace, queue string,
) {
	t.Helper()
	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	)
	awaitActivityPoller(ctx, t, temporalClient, queue)
}

// awaitActivityPoller waits for Temporal's cluster-visible readiness fact.
// Process startup alone is insufficient: submit deliberately refuses a pool
// until its activity poller is registered.
func awaitActivityPoller(ctx context.Context, t *testing.T, temporalClient client.Client, queue string) {
	t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		description, err := temporalClient.DescribeTaskQueueEnhanced(ctx, client.DescribeTaskQueueEnhancedOptions{
			TaskQueue:      queue,
			TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
			ReportPollers:  true,
			Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
		})
		lastErr = err
		if err == nil {
			for _, version := range description.VersionsInfo {
				if len(version.TypesInfo[client.TaskQueueTypeActivity].Pollers) > 0 {
					t.Logf("Orker activity poller ready on %s", queue)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Orker activity poller on %s: %v", queue, ctx.Err())
		case <-deadline.C:
			t.Fatalf("Orker activity poller on %s did not become ready: %v", queue, lastErr)
		case <-ticker.C:
		}
	}
}

// h2cClient dials the engine's h2c Connect server.
func h2cClient(t *testing.T) *http.Client {
	t.Helper()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}

// awaitOrkflowVisible waits until a replica that never applied the orkflow can
// read it, which is the durable store propagating rather than a cache warming.
func awaitOrkflowVisible(
	ctx context.Context,
	t *testing.T,
	flows orkstackv1connect.OrkflowServiceClient,
	name string,
) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := flows.GetOrkflow(ctx,
			connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: name})); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("the second replica never saw orkflow %q in the durable store", name)
}

// idempotencyOrkflowYAML is a one-task orkflow whose work is trivial: this
// test is about which run starts, not about what it does. The task names no
// queue, so the engine binds it to the Orker queue both replicas dispatch to.
func idempotencyOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M8.2 two-replica idempotency probe.
tasks:
  - id: echo
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo idempotent"]
`, name)
}

// isolatedNamespace names a Temporal namespace this test run alone uses. The
// operator's ORK_TEMPORAL_NAMESPACE is kept as the prefix so the isolation
// choice from AGENTS.md §6 stays visible in `temporal workflow list`.
func isolatedNamespace() string {
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = "ork-m8-2-idempotency"
	}
	return fmt.Sprintf("%s-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// dialIsolatedNamespace provisions the test's namespace and returns a client
// bound to it, retrying the first dial because a freshly registered namespace
// is not immediately in the frontend's cache.
func dialIsolatedNamespace(
	ctx context.Context,
	t *testing.T,
	address, namespace string,
) client.Client {
	t.Helper()
	bootstrap, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()

	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			if _, probe := c.DescribeTaskQueue(ctx, idempotencyTaskQueue(), 0); probe == nil {
				t.Cleanup(c.Close)
				return c
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %q did not become usable: %v", namespace, err)
		}
		time.Sleep(time.Second)
	}
}

// idempotencyTaskQueue names the Orker queue for this test process.
func idempotencyTaskQueue() string {
	return fmt.Sprintf("ork-idem-%d", os.Getpid())
}

// requireMinIOForIdempotency skips when the dev object store is absent, so the
// suite reports "skipped", never a false failure, without infrastructure — and
// fails instead under ORK_REQUIRE_INFRA, where the absence IS the failure
// (see infraAbsent).
func requireMinIOForIdempotency(t *testing.T, cfg artifacts.Config) {
	t.Helper()
	base, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		infraAbsent(t, "MinIO endpoint %q is unusable: %v", cfg.S3Endpoint, err)
	}
	health := base.JoinPath("/minio/health/live").String()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, health, nil)
	if err != nil {
		infraAbsent(t, "MinIO health request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		infraAbsent(t, "MinIO not reachable at %s (%v); run `make infra-up`", health, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		infraAbsent(t, "MinIO health %s returned %d; run `make infra-up`", health, resp.StatusCode)
	}
	t.Logf("MinIO health %s -> %d", health, resp.StatusCode)
}

// writeIdempotencyTranscript records the proof artifact when the milestone's
// reproduce.sh asks for one.
func writeIdempotencyTranscript(t *testing.T, namespace, runID, claimID, replicaA, replicaB string) {
	t.Helper()
	path := os.Getenv("ORK_PROOF_IDEMPOTENCY_FILE")
	if path == "" {
		return
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# two engine replicas, one idempotency key (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "namespace           %s\n", namespace)
	fmt.Fprintf(&out, "idempotency key     %s\n", idempotencyKey)
	fmt.Fprintf(&out, "claim coordinate    %s%s%s\n",
		submittedInto, claimScopeSeparator, idempotencyKey)
	fmt.Fprintf(&out, "claim workflow id   %s\n", claimID)
	fmt.Fprintf(&out, "%s returned run    %s\n", replicaA, runID)
	fmt.Fprintf(&out, "%s returned run    %s\n", replicaB, runID)
	fmt.Fprintf(&out, "RunWorkflow executions in the namespace: 1\n")
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		t.Errorf("write idempotency transcript: %v", err)
	}
}

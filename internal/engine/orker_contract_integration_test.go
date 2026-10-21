//go:build integration

package engine_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"archive/tar"
	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Two more §11 claims, proven end to end through a real Orker (AGENTS.md §11,
// docs/PROOFS.md §12):
//
//	3h  an upstream artifact deleted after its producer committed and before
//	    its consumer was dispatched: the task fails INPUT_ARTIFACT_UNAVAILABLE, after exactly the
//	    attempts its policy allows for that reason's category, and the run
//	    ends FAILED with a terminal event;
//	3i  an image the registry cannot serve to this host: IMAGE_PULL_FAILED,
//	    retried exactly as the task's retry: allows.
//
// Neither test needs a store hook, so they live in their own file: the
// validation run's feature push removes only the kill file
// (internal/engine/orker_kill_integration_test.go).

// containerPhases are the §7.1 phases an attempt passes through only once it
// has a container: one created ("create"), run ("run") or exited ("exit").
var containerPhases = []string{"create", "run", "exit"}

// requireNoContainerRan fails unless the attempt's phases are recorded and
// none of them is a container's: the attempt ended before the Orker created
// one. An exit code cannot say this — a record that never ran a container
// and one whose container exited 0 both carry 0.
func requireNoContainerRan(t *testing.T, record *orkstackv1.TaskAttempt) {
	t.Helper()
	if len(record.GetPhases()) == 0 {
		t.Fatalf("attempt %d records no phases; it cannot show that no container ran", record.GetAttempt())
	}
	for _, span := range record.GetPhases() {
		if slices.Contains(containerPhases, span.GetPhase()) {
			t.Fatalf("attempt %d passed through the container phase %q (phases %v); want none to have run",
				record.GetAttempt(), span.GetPhase(), record.GetPhases())
		}
	}
	if record.GetExitCode() != 0 {
		t.Fatalf("attempt %d reports exit code %d with no container", record.GetAttempt(), record.GetExitCode())
	}
}

// TestIntegration_AnUnavailableInputArtifactFailsTheTaskExplicitly: §11 3h.
// Task "produce" writes an artifact. The gate is an await, and while it is
// open the test deletes produce's artifact object from the stack's store —
// after the producer committed and before the consumer was dispatched. The
// resolution then lets
// consume run: its staging finds nothing, it fails
// INPUT_ARTIFACT_UNAVAILABLE, the policy retries it exactly once more, and the
// run ends FAILED with that reason.
func TestIntegration_AnUnavailableInputArtifactFailsTheTaskExplicitly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "orker-contract/unavailable-input")
	stores := s.realStores()
	image := pinnedAlpineForCommitE2E(t)
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startRealOrker("orker-a", "real", realQueue, stores, nil, nil)
	a := s.serveAPI(tcAPIConfig())

	doc := `apiVersion: orkstack/v1
kind: Orkflow
name: unavailable-input
version: "1.0.0"
tasks:
  - id: produce
    uses: shell
    needs: []
    pool: real
    with: {op: exec}
  - id: gate
    needs: []
    await: {timeout_seconds: 3600}
  - id: consume
    uses: shell
    needs: [produce, gate]
    pool: real
    retry: {maximum_attempts: 2}
    with: {op: exec}
`
	wf := compile(t, doc)
	for _, tk := range wf.GetTasks() {
		switch tk.GetId() {
		case "produce":
			tk.Uses, tk.Image = image, &orkstackv1.ImageRef{Reference: image, Pinned: image}
			tk.Command = []string{"/bin/sh", "-c",
				`printf 'payload' > /orkstack/output/artifacts/report.txt; printf '{"ok":true}' > /orkstack/output/values.json`}
			tk.TimeoutSeconds = 120
			tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
		case "consume":
			tk.Uses, tk.Image = image, &orkstackv1.ImageRef{Reference: image, Pinned: image}
			// The artifact is staged from produce's commit; the container
			// reads exactly the staged copy, so a partial input would be read
			// as a partial file.
			tk.Command = []string{"/bin/sh", "-c", `cat /orkstack/input/artifacts/produce/report.txt`}
			tk.TimeoutSeconds = 120
			tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
		}
	}
	plan := realTaskPlan(t, s, realQueue)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, wf, nil, plan)
	stream := s.openStream(s.reader, runID, 0)

	stream.waitFor(t, 3*time.Minute, "produce's success", taskEvent("produce", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED))
	// The artifact the commit references is exactly what consume's staging
	// fetches, so read it from produce's commit — a deterministic key, no
	// dependence on when the projection wrote the snapshot.
	key, err := artifacts.CommitKey("default", runID, "produce")
	if err != nil {
		t.Fatal(err)
	}
	commitStore, err := artifacts.NewCommitStore(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	produced, err := commitStore.Get(ctx, strings.TrimSuffix(plan.CommitRoot, "/")+"/"+key)
	if err != nil {
		t.Fatalf("read produce's commit: %v", err)
	}
	if len(produced.Commit.GetArtifacts()) == 0 {
		t.Fatal("produce's commit references no artifact")
	}
	artifact := produced.Commit.GetArtifacts()[0]
	stream.waitFor(t, 2*time.Minute, "the gate's await", taskEvent("gate", orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING))
	s.transcript.step("produce committed %s; the gate is open, so consume has not staged", artifact.GetUri())

	if err := stores.store.Delete(ctx, artifact.GetUri()); err != nil {
		t.Fatalf("delete the upstream artifact %s: %v", artifact.GetUri(), err)
	}
	if _, err := stores.store.Get(ctx, artifact.GetUri()); err == nil {
		t.Fatalf("the upstream artifact %s still reads after the delete", artifact.GetUri())
	}
	s.transcript.step("the upstream artifact is gone; resolving the gate")

	if _, err := a.runs.ResolveAwait(ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
		RunId: runID, TaskId: "gate", ValuesJson: `{"approved":true}`,
	})); err != nil {
		t.Fatalf("resolve the gate: %v", err)
	}

	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED || err == nil {
		t.Fatalf("the run closed %v (%v), want FAILED", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, "")
	if len(events) == 0 {
		t.Fatal("the run read no events")
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskRunOfRun(run, "consume").GetError().GetCode(); got != "INPUT_ARTIFACT_UNAVAILABLE" {
		t.Fatalf("the run's consume row names %s, want INPUT_ARTIFACT_UNAVAILABLE", got)
	}

	records, err := stores.attempts.ListAttempts(ctx, runID, "consume", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("%d consume attempts, want 2 (the policy's maximum for an InfrastructureError)", len(records))
	}
	for _, record := range records {
		if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED ||
			record.GetError().GetCode() != "INPUT_ARTIFACT_UNAVAILABLE" ||
			record.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
			!record.GetError().GetRetryable() {
			t.Fatalf("consume attempt %d = %v %v (retryable %v), want a retryable INPUT_ARTIFACT_UNAVAILABLE",
				record.GetAttempt(), record.GetOutcome(), record.GetError(), record.GetError().GetRetryable())
		}
		requireNoContainerRan(t, record)
		if !strings.Contains(record.GetError().GetMessage(), "report.txt") {
			t.Fatalf("consume attempt %d's message %q does not name the artifact", record.GetAttempt(), record.GetError().GetMessage())
		}
	}
	s.transcript.figure("consume_attempts", len(records))
	t.Logf("§11 3h: the upstream artifact was deleted while the gate was open; consume failed "+
		"INPUT_ARTIFACT_UNAVAILABLE on both attempts (%d), ran no container, and the run ended FAILED with that reason",
		len(records))
}

// testRegistry is the development registry `make infra-up` runs.
func testRegistry() string {
	if registry := os.Getenv("ORK_TEST_REGISTRY"); registry != "" {
		return registry
	}
	return "localhost:5001"
}

func requireRegistry(t *testing.T, registry string) {
	t.Helper()
	resp, err := http.Get("http://" + registry + "/v2/")
	if err != nil {
		infraAbsent(t, "registry not reachable at %s (%v); run `make infra-up`", registry, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		infraAbsent(t, "registry %s answered %d; run `make infra-up`", registry, resp.StatusCode)
	}
}

// pushForeignPlatformIndex publishes a real, complete image — every blob
// present — whose only platform is one no Orker here runs (linux/s390x),
// behind an OCI index, and returns the tag — v1-<pid>-<n>, this process's
// own — and the index digest. The Orker's
// `docker pull` of that digest finds no manifest for its own platform and
// fails: IMAGE_PULL_FAILED at run time, never a registry that is merely
// unreachable.
//
// It is the same construction as cmd/orkctl's stats test, repeated here
// because a test helper in another package is not importable.
func pushForeignPlatformIndex(ctx context.Context, t *testing.T, registry, repository string) (tag, digest string) {
	t.Helper()
	base := fmt.Sprintf("http://%s/v2/%s", registry, repository)
	do := func(method, url, contentType string, body []byte, want int) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("build %s %s: %v", method, url, err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			var text bytes.Buffer
			_, _ = text.ReadFrom(resp.Body)
			t.Fatalf("%s %s answered %d, want %d: %s", method, url, resp.StatusCode, want, text.String())
		}
		return resp
	}
	pushBlob := func(blob []byte) string {
		t.Helper()
		sum := fmt.Sprintf("sha256:%x", sha256.Sum256(blob))
		started := do(http.MethodPost, base+"/blobs/uploads/", "", nil, http.StatusAccepted)
		location, err := started.Location()
		if err != nil {
			t.Fatalf("blob upload location: %v", err)
		}
		query := location.Query()
		query.Set("digest", sum)
		location.RawQuery = query.Encode()
		do(http.MethodPut, location.String(), "application/octet-stream", blob, http.StatusCreated)
		return sum
	}
	// pushManifest puts a manifest under reference, or under its own digest
	// when reference is empty, so the only tag this test creates is the one
	// carrying its process token.
	pushManifest := func(reference, mediaType string, body []byte) string {
		t.Helper()
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
		if reference == "" {
			reference = digest
		}
		do(http.MethodPut, base+"/manifests/"+reference, mediaType, body, http.StatusCreated)
		return digest
	}

	// An empty tar, gzipped, is a valid layer.
	var tarball, layer bytes.Buffer
	if err := tar.NewWriter(&tarball).Close(); err != nil {
		t.Fatalf("empty tar: %v", err)
	}
	zipper := gzip.NewWriter(&layer)
	_, _ = zipper.Write(tarball.Bytes())
	if err := zipper.Close(); err != nil {
		t.Fatalf("gzip layer: %v", err)
	}
	layerDigest := pushBlob(layer.Bytes())
	config := []byte(fmt.Sprintf(`{"architecture":"s390x","os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:%x"]},"config":{}}`,
		sha256.Sum256(tarball.Bytes())))
	configDigest := pushBlob(config)
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
		`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},`+
		`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":%d}]}`,
		configDigest, len(config), layerDigest, layer.Len()))
	manifestDigest := pushManifest("", "application/vnd.oci.image.manifest.v1+json", manifest)
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[`+
		`{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":%d,"platform":{"architecture":"s390x","os":"linux"}}]}`,
		manifestDigest, len(manifest)))
	// The tag carries this process's token (AGENTS.md §11.1: a test pushes
	// only tags of its own), and a per-process counter.
	reference := fmt.Sprintf("v1-%d-%d", os.Getpid(), foreignIndexPushes.Add(1))
	indexDigest := pushManifest(reference, "application/vnd.oci.image.index.v1+json", index)
	return fmt.Sprintf("%s/%s:%s", registry, repository, reference), indexDigest
}

// foreignIndexPushes numbers this process's foreign-platform index tags.
var foreignIndexPushes atomic.Int64

// TestIntegration_AFailedImagePullFailsTheTaskPerItsPolicy: §11 3i-retry. The
// task's image is a foreign-platform index the registry serves happily and no
// Orker here can run. The pull fails IMAGE_PULL_FAILED — InfrastructureError,
// retryable — so the task runs exactly the two attempts its retry: allows,
// neither creates a container, each attempt's message names the pinned digest
// and the platform mismatch the pull reported, and the run ends FAILED with
// that reason.
func TestIntegration_AFailedImagePullFailsTheTaskPerItsPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	registry := testRegistry()
	requireRegistry(t, registry)
	repository := fmt.Sprintf("orkstack-it/fp-unpullable-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	tag, digest := pushForeignPlatformIndex(ctx, t, registry, repository)
	pinned := fmt.Sprintf("%s/%s@%s", registry, repository, digest)

	s := newChaosStack(t, ctx, "orker-contract/failed-pull")
	stores := s.realStores()
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startRealOrker("orker-a", "real", realQueue, stores, nil, nil)

	wf := realTaskDocument(t, "orker-failed-pull", tag, `printf '{"ok":true}' > /orkstack/output/values.json`,
		"maximum_attempts: 2")
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "work" {
			tk.Image = &orkstackv1.ImageRef{Reference: tag, Pinned: pinned, IndexDigest: digest}
		}
	}
	plan := realTaskPlan(t, s, realQueue)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, wf, nil, plan)
	stream := s.openStream(s.reader, runID, 0)

	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED || err == nil {
		t.Fatalf("the run closed %v (%v), want FAILED", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, "")
	if len(events) == 0 {
		t.Fatal("the run read no events")
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskRunOfRun(run, "work").GetError().GetCode(); got != "IMAGE_PULL_FAILED" {
		t.Fatalf("the run's work row names %s, want IMAGE_PULL_FAILED", got)
	}

	records, err := stores.attempts.ListAttempts(ctx, runID, "work", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("%d attempts, want exactly the 2 the task's retry: allows", len(records))
	}
	for _, record := range records {
		if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED ||
			record.GetError().GetCode() != "IMAGE_PULL_FAILED" ||
			record.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
			!record.GetError().GetRetryable() {
			t.Fatalf("attempt %d = %v %v retryable %t, want IMAGE_PULL_FAILED INFRASTRUCTURE_ERROR retryable",
				record.GetAttempt(), record.GetOutcome(), record.GetError(), record.GetError().GetRetryable())
		}
		requireNoContainerRan(t, record)
		// The pull's own message: the digest the Orker pulled, and the
		// platform it could not find in that index.
		message := record.GetError().GetMessage()
		if !strings.Contains(message, digest) || !(strings.Contains(message, "no matching manifest") ||
			strings.Contains(message, "platform")) {
			t.Fatalf("attempt %d's message %q names neither the pinned digest %s and the platform mismatch",
				record.GetAttempt(), message, digest)
		}
	}
	if n := commitObjectCount(t, s.cfg, runID); n != 0 {
		t.Fatalf("%d commit.json objects exist for a task that never pulled its image, want 0", n)
	}
	s.transcript.figure("attempts", len(records))
	t.Logf("§11 3i-retry: %s could not be served to this host; both attempts failed IMAGE_PULL_FAILED "+
		"(InfrastructureError, retryable) and the run ended FAILED with that reason", pinned)
}

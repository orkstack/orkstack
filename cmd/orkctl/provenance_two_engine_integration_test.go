//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ArtifactProvenanceIsAnsweredByAReplicaThatNeverStartedTheRun
// drives the provenance read against the production topology: two real `ork`
// processes on one Temporal namespace, one Orker, one MinIO bucket.
//
//  1. A run is submitted to replica A only; replica B answers its provenance —
//     HELD, bytes present, one reference — from the shared archive and store,
//     so the answer cannot have come from A's memory.
//  2. The object is deleted straight from the bucket (a lifecycle rule,
//     simulated), and A answers bytes_present=false while the reference
//     still stands.
//  3. The same orkflow runs again and its identical bytes are asked about: on
//     the S3 layout an object key embeds its run id, so the second run's
//     artifact lists exactly one reference, its own — the identity rule
//     observed, not assumed.
func TestIntegration_ArtifactProvenanceIsAnsweredByAReplicaThatNeverStartedTheRun(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	temporalClient := dialIsolatedNamespace(ctx, t, address, namespace)

	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")

	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, namespace, queue)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	orkflowName := fmt.Sprintf("provenance-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: provenanceOrkflowYAML(orkflowName),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, replicaB.flows, orkflowName)

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# artifact provenance across two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "namespace   %s\norkflow     %s@0.1.0\nbackend     s3 (bucket %s)\n\n", namespace, orkflowName, artifactCfg.S3Bucket)

	// ---- 1. submitted to A, asked of B -------------------------------------
	first := submitAndFinish(ctx, t, replicaA, replicaB, orkflowName)
	held := awaitProvenanceHeld(ctx, t, replicaB, first)
	if !held.GetBytesPresent() {
		t.Fatalf("replica B reports the bytes of %s missing right after the run", first)
	}
	requireOnlyReference(t, "replica B, first run", held, first)
	cli := runProvenanceCLI(t, replicaB, first)
	if !strings.Contains(cli, "present in the object store") || !strings.Contains(cli, "REFERENCE  held") {
		t.Fatalf("orkctl against replica B did not render a held, present answer:\n%s", cli)
	}
	fmt.Fprintf(&transcript, "run %s submitted on replica A; provenance asked of replica B:\n", first)
	fmt.Fprintf(&transcript, "  bytes_present=%t own_reference=%s references=%s references_unreadable=%d\n",
		held.GetBytesPresent(), held.GetOwnReference(), referenceIDs(held), held.GetReferencesUnreadable())
	fmt.Fprintf(&transcript, "  uri=%s\n\n$ orkctl --server <replica B> run artifact provenance %s --task completed --kind output --name report.txt\n%s\n",
		held.GetArtifact().GetUri(), first, cli)

	// ---- 2. the bytes vanish; the reference does not -----------------------
	store, err := artifacts.NewS3Store(ctx, artifactCfg)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	if err := store.Delete(ctx, held.GetArtifact().GetUri()); err != nil {
		t.Fatalf("delete %s: %v", held.GetArtifact().GetUri(), err)
	}
	gone, err := askProvenance(ctx, replicaA, first)
	if err != nil {
		t.Fatalf("GetArtifactProvenance on replica A after the delete: %v", err)
	}
	if gone.GetBytesPresent() {
		t.Fatalf("replica A reports bytes present after the object was deleted from the bucket")
	}
	if gone.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_HELD {
		t.Fatalf("deleting the bytes changed the reference to %s; want HELD", gone.GetOwnReference())
	}
	requireOnlyReference(t, "replica A, bytes deleted", gone, first)
	fmt.Fprintf(&transcript, "object deleted straight from the bucket; provenance asked of replica A:\n")
	fmt.Fprintf(&transcript, "  bytes_present=%t own_reference=%s references=%s\n\n",
		gone.GetBytesPresent(), gone.GetOwnReference(), referenceIDs(gone))

	// ---- 3. identical bytes, second run: one object per run on S3 ----------
	second := submitAndFinish(ctx, t, replicaA, replicaB, orkflowName)
	again := awaitProvenanceHeld(ctx, t, replicaB, second)
	if again.GetArtifact().GetSha256() != held.GetArtifact().GetSha256() {
		t.Fatalf("the second run's report.txt digest %s differs from the first's %s; the fixture is not deterministic",
			again.GetArtifact().GetSha256(), held.GetArtifact().GetSha256())
	}
	if again.GetArtifact().GetUri() == held.GetArtifact().GetUri() {
		t.Fatalf("two runs share one S3 object %s; the key is supposed to embed the run id", again.GetArtifact().GetUri())
	}
	requireOnlyReference(t, "replica B, second run", again, second)
	fmt.Fprintf(&transcript, "run %s (same orkflow, identical bytes sha256 %s):\n", second, again.GetArtifact().GetSha256())
	fmt.Fprintf(&transcript, "  bytes_present=%t own_reference=%s references=%s\n  uri=%s\n",
		again.GetBytesPresent(), again.GetOwnReference(), referenceIDs(again), again.GetArtifact().GetUri())

	proof.Write(t, "orkctl", "artifact-provenance-two-replica.txt", []byte(transcript.String()))
	t.Logf("EVIDENCE provenance_two_replica first=%s held_on_B=true bytes_present_after_delete=%t second=%s second_references=%d",
		first, gone.GetBytesPresent(), second, len(again.GetReferences()))
}

// submitAndFinish submits to A and waits until B reports the run SUCCEEDED.
func submitAndFinish(ctx context.Context, t *testing.T, a, b *engineReplica, orkflowName string) string {
	t.Helper()
	submitted, err := a.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: orkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun on replica A: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	finished := awaitTerminalOnB(ctx, t, b, runID)
	if finished.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", runID, finished.GetStatus(), finished.GetError())
	}
	return runID
}

func askProvenance(ctx context.Context, replica *engineReplica, runID string) (*orkstackv1.GetArtifactProvenanceResponse, error) {
	resp, err := replica.runs.GetArtifactProvenance(ctx, connect.NewRequest(&orkstackv1.GetArtifactProvenanceRequest{
		RunId: runID, TaskId: "completed", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "report.txt",
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// awaitProvenanceHeld polls until the run's archive write has published its
// markers. A finished run answers ABSENT until then — which is the documented
// answer for a run whose archive write has not completed, not an error.
func awaitProvenanceHeld(ctx context.Context, t *testing.T, replica *engineReplica, runID string) *orkstackv1.GetArtifactProvenanceResponse {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last *orkstackv1.GetArtifactProvenanceResponse
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = askProvenance(ctx, replica, runID)
		if lastErr == nil && last.GetOwnReference() == orkstackv1.ReferenceState_REFERENCE_STATE_HELD {
			return last
		}
		if lastErr == nil && last.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_ABSENT {
			t.Fatalf("replica %s reports run %s's reference as %s before it was ever held", replica.name, runID, last.GetOwnReference())
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("replica %s never reported run %s's reference HELD: last %v, %v", replica.name, runID, last, lastErr)
	return nil
}

func requireOnlyReference(t *testing.T, where string, res *orkstackv1.GetArtifactProvenanceResponse, runID string) {
	t.Helper()
	refs := res.GetReferences()
	if len(refs) != 1 || refs[0].GetRunId() != runID || refs[0].GetTaskId() != "completed" ||
		refs[0].GetKind() != orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT {
		t.Fatalf("%s: references = %v; want exactly %s/completed/OUTPUT", where, refs, runID)
	}
	if res.GetReferencesUnreadable() != 0 {
		t.Fatalf("%s: references_unreadable = %d", where, res.GetReferencesUnreadable())
	}
}

func referenceIDs(res *orkstackv1.GetArtifactProvenanceResponse) string {
	ids := make([]string, 0, len(res.GetReferences()))
	for _, ref := range res.GetReferences() {
		ids = append(ids, ref.GetRunId()+"/"+ref.GetTaskId())
	}
	return "[" + strings.Join(ids, " ") + "]"
}

// runProvenanceCLI runs the real orkctl command against a replica.
func runProvenanceCLI(t *testing.T, replica *engineReplica, runID string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "artifact", "provenance", runID, "--task", "completed", "--kind", "output",
		"--name", "report.txt", "--server", "http://" + replica.addr}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("orkctl run artifact provenance exited %d: %s", code, stderr.String())
	}
	return stdout.String()
}

// provenanceOrkflowYAML writes one deterministic artifact, so two runs
// produce byte-identical report.txt files.
func provenanceOrkflowYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: artifact provenance probe.
tasks:
  - id: completed
    uses: shell
    timeout_seconds: 60
    with:
      op: script
      interpreter: sh
      script: |
        printf REPORT > /orkstack/output/artifacts/report.txt
        printf '{}' > /orkstack/output/values.json
`, name)
}

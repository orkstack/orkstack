//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run-evidence integration scenarios: two real `ork`
// replicas on one private Temporal namespace, one Orker and the MinIO bucket.
// Every export is asked of replica B, which never started the run, so no byte
// of a bundle can have come from the process that watched it.

// evidenceOrkflowYAML is the completedWorkflow shape (report.txt and
// values.json) plus `retried`, whose first attempt reads its own attempt
// number from /orkstack/input/task.json and exits 1, so attempt 1 fails as
// ACTIVITY_FAILED and attempt 2 succeeds.
func evidenceOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: run evidence probe.
tasks:
  - id: completed
    uses: shell
    timeout_seconds: 60
    with:
      op: script
      interpreter: sh
      script: |
        echo "writing the report"
        printf REPORT > /orkstack/output/artifacts/report.txt
        printf '{"done": true}' > /orkstack/output/values.json
  - id: retried
    uses: shell
    timeout_seconds: 60
    retry:
      maximum_attempts: 2
      initial_interval_seconds: 1
    with:
      op: script
      interpreter: sh
      script: |
        printf '{"retried": true}' > /orkstack/output/values.json
        if grep -q '"attempt": 1,' /orkstack/input/task.json; then
          echo "attempt 1 fails on purpose" >&2
          exit 1
        fi
        echo "attempt 2 succeeds"
`, name)
}

// evidenceSleeperYAML is a task that sleeps long enough to be asked about
// while it runs.
func evidenceSleeperYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: run evidence live-run probe.
tasks:
  - id: sleeper
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        sleep 20
        printf '{}' > /orkstack/output/values.json
`, name)
}

// evidenceStack is a two-replica stack with the probe applied on A, visible
// on B, and one run of it finished.
type evidenceStack struct {
	*timingStack
	name  string
	runID string
	dir   string
}

func newEvidenceStack(t *testing.T, ctx context.Context) *evidenceStack {
	t.Helper()
	s := newTimingStack(ctx, t, 2)
	s.startOrker()
	name := fmt.Sprintf("evidence-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(evidenceOrkflowYAML(name))
	awaitOrkflowVisible(ctx, t, s.replicas[1].flows, name)
	e := &evidenceStack{timingStack: s, name: name, dir: t.TempDir()}
	e.runID = submitOnA(t, s, name, map[string]any{})
	if run := awaitTerminalOnB(ctx, t, s.replicas[1], e.runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", e.runID, run.GetStatus(), run.GetError())
	}
	awaitEvidenceArchived(t, s, 1, e.runID)
	return e
}

// awaitEvidenceArchived polls until the replica answers a manifest: the terminal event
// and the archive write are one step apart, and the export says "not yet
// archived" in between.
func awaitEvidenceArchived(t *testing.T, s *timingStack, replica int, runID string) *evidence.Manifest {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		m, err := manifestOf(s.ctx, s.replicas[replica], runID, "")
		if err == nil {
			return m
		}
		lastErr = err
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "not yet archived") {
			t.Fatalf("ExportRunEvidence(%s) on %s: %v", runID, s.replicas[replica].name, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s was never archived: %v", runID, lastErr)
	return nil
}

// manifestOf is a manifest_only export over the wire.
func manifestOf(ctx context.Context, replica *engineReplica, runID, namespace string) (*evidence.Manifest, error) {
	raw, err := manifestBytes(ctx, replica, runID, namespace)
	if err != nil {
		return nil, err
	}
	return evidence.ParseManifest(raw)
}

func manifestBytes(ctx context.Context, replica *engineReplica, runID, namespace string) ([]byte, error) {
	stream, err := replica.runs.ExportRunEvidence(ctx, connect.NewRequest(&orkstackv1.ExportRunEvidenceRequest{
		RunId: runID, Namespace: namespace, ManifestOnly: true,
	}))
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	var raw bytes.Buffer
	last := false
	for stream.Receive() {
		raw.Write(stream.Msg().GetData())
		last = last || stream.Msg().GetLast()
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	if !last {
		return nil, errors.New("manifest stream ended without last")
	}
	return raw.Bytes(), nil
}

// s3Object reads one object straight from the bucket, by its s3:// uri or key.
func s3Object(t *testing.T, ctx context.Context, cfg artifacts.Config, uriOrKey string) []byte {
	t.Helper()
	key := uriOrKey
	if strings.HasPrefix(uriOrKey, "s3://") {
		parsed, err := url.Parse(uriOrKey)
		if err != nil {
			t.Fatal(err)
		}
		key = strings.TrimPrefix(parsed.Path, "/")
	}
	out, err := auditRawS3Client(t, cfg).GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("get s3 object %s: %v", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// bundleEntries reads every entry of a bundle file.
func bundleEntries(t *testing.T, path string) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	entries := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return entries
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		entries[h.Name] = body
	}
}

func memberOf(t *testing.T, m *evidence.Manifest, path string) evidence.Member {
	t.Helper()
	for _, member := range m.Members {
		if member.Path == path {
			return member
		}
	}
	t.Fatalf("manifest of %s has no member %s", m.RunID, path)
	return evidence.Member{}
}

func TestIntegration_RunEvidenceIsAnsweredByAReplicaThatNeverStartedTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	e := newEvidenceStack(t, ctx)
	defer e.writeTranscript("run-evidence-two-replica.txt")
	fmt.Fprintf(&e.transcript, "# run evidence across two engine replicas (%s)\nrun %s submitted on replica A; every export below asked of replica B\n\n",
		time.Now().UTC().Format(time.RFC3339), e.runID)

	bundle := filepath.Join(e.dir, "bundle.tar.gz")
	e.mustCLI(1, "run", "evidence", "export", e.runID, "--output", bundle)
	verified := e.mustCLI(1, "run", "evidence", "verify", bundle)
	if !strings.HasPrefix(verified, "verified ") || !strings.Contains(verified, "present members against MANIFEST.json") {
		t.Fatalf("verify output:\n%s", verified)
	}

	file, err := os.Open(bundle)
	if err != nil {
		t.Fatal(err)
	}
	m, err := evidence.ReadManifest(file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	var retried *evidence.TaskEntry
	for i := range m.Tasks {
		if m.Tasks[i].TaskID == "retried" {
			retried = &m.Tasks[i]
		}
	}
	if retried == nil || len(retried.Attempts) != 2 || retried.Attempts[0].State != evidence.StatePresent ||
		retried.Attempts[0].Outcome != "FAILED" || retried.Attempts[1].Outcome != "SUCCEEDED" || retried.OutcomeAttempt != 2 {
		t.Fatalf("retried task = %+v, want attempt records FAILED then SUCCEEDED", retried)
	}
	for _, path := range []string{"tasks/retried/attempts/1.json", "tasks/retried/attempts/2.json",
		"tasks/retried/attempts/1/stderr.log", "tasks/completed/artifacts/report.txt", "tasks/completed/commit.json"} {
		if member := memberOf(t, m, path); member.State != evidence.StatePresent {
			t.Fatalf("%s is %s", path, member.State)
		}
	}

	entries := bundleEntries(t, bundle)
	commit := memberOf(t, m, "tasks/completed/commit.json")
	if stored := s3Object(t, ctx, e.artifactCfg, commit.Origin); !bytes.Equal(entries["tasks/completed/commit.json"], stored) {
		t.Fatalf("commit.json in the bundle differs from the bucket object %s", commit.Origin)
	}
	runKey, err := artifacts.RunArchiveKey(e.runID)
	if err != nil {
		t.Fatal(err)
	}
	if stored := s3Object(t, ctx, e.artifactCfg, runKey); !bytes.Equal(entries["run.jsonl"], stored) {
		t.Fatalf("run.jsonl differs from the archive object %s", runKey)
	}
	if string(entries["tasks/completed/artifacts/report.txt"]) != "REPORT" {
		t.Fatalf("report.txt = %q", entries["tasks/completed/artifacts/report.txt"])
	}
	fmt.Fprintf(&e.transcript, "commit.json == s3 %s: true\nrun.jsonl == s3 %s: true\n\n", commit.Origin, runKey)

	onA := e.mustCLI(0, "--json", "run", "evidence", "export", e.runID)
	onB := e.mustCLI(1, "--json", "run", "evidence", "export", e.runID)
	ma, err := evidence.ParseManifest([]byte(onA))
	if err != nil {
		t.Fatal(err)
	}
	mb, err := evidence.ParseManifest([]byte(onB))
	if err != nil {
		t.Fatal(err)
	}
	ma.ExportedAt, mb.ExportedAt = time.Time{}, time.Time{}
	ja, _ := ma.MarshalJSON()
	jb, _ := mb.MarshalJSON()
	if !bytes.Equal(ja, jb) {
		t.Fatalf("replica A and B manifests differ beyond exported_at:\n%s\n---\n%s", ja, jb)
	}
	fmt.Fprintf(&e.transcript, "manifests from replica A and replica B equal except exported_at: true (%d members)\n\n", len(ma.Members))

	e.mustCLI(1, "run", "evidence", "verify", bundle, "--against-store")
	_, stderr, code := e.cli(1, "-n", "other", "run", "evidence", "export", e.runID, "--json")
	if code == 0 || !strings.Contains(stderr, "not_found") {
		t.Fatalf("-n other: exit %d stderr %q", code, stderr)
	}
	t.Logf("EVIDENCE run_evidence_two_replica run=%s %s", e.runID, strings.SplitN(verified, "\n", 2)[0])
}

func TestIntegration_RunEvidenceAndProvenanceAgreeOnAMissingObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	e := newEvidenceStack(t, ctx)
	defer e.writeTranscript("run-evidence-provenance.txt")

	bundle := filepath.Join(e.dir, "bundle.tar.gz")
	e.mustCLI(1, "run", "evidence", "export", e.runID, "--output", bundle)
	awaitProvenanceHeldFor(ctx, t, e.replicas[1], e.runID)

	m := awaitEvidenceArchived(t, e.timingStack, 1, e.runID)
	report := memberOf(t, m, "tasks/completed/artifacts/report.txt")
	store, err := artifacts.NewS3Store(ctx, e.artifactCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, report.Origin); err != nil {
		t.Fatalf("delete %s: %v", report.Origin, err)
	}
	provenance, err := e.replicas[1].runs.GetArtifactProvenance(ctx, connect.NewRequest(&orkstackv1.GetArtifactProvenanceRequest{
		RunId: e.runID, TaskId: "completed", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "report.txt",
	}))
	if err != nil {
		t.Fatal(err)
	}
	after, err := manifestOf(ctx, e.replicas[1], e.runID, "")
	if err != nil {
		t.Fatal(err)
	}
	gone := memberOf(t, after, "tasks/completed/artifacts/report.txt")
	provenanceSHA := evidence.NormalizeDigest(provenance.Msg.GetArtifact().GetSha256())
	if provenance.Msg.GetBytesPresent() || gone.State != evidence.StateReclaimed || gone.Reason != evidence.ReasonObjectMissing ||
		gone.SHA256 != provenanceSHA || gone.SHA256 != report.SHA256 {
		t.Fatalf("provenance bytes_present=%t sha256 %s; manifest member %+v", provenance.Msg.GetBytesPresent(), provenanceSHA, gone)
	}
	line := fmt.Sprintf("provenance bytes_present=%t sha256=%s; evidence state=%s reason=%s sha256=%s",
		provenance.Msg.GetBytesPresent(), provenanceSHA, gone.State, gone.Reason, gone.SHA256)
	fmt.Fprintf(&e.transcript, "%s\n\n", line)

	stdout := e.mustCLI(1, "run", "evidence", "verify", bundle, "--against-store")
	if !strings.Contains(stdout, "tasks/completed/artifacts/report.txt   reclaimed since export") {
		t.Fatalf("verify --against-store:\n%s", stdout)
	}
	t.Logf("EVIDENCE run_evidence_provenance_agree %s", line)
}

// awaitProvenanceHeldFor waits until the run's archive markers are published,
// so a provenance answer after the delete reads the marker, not its absence.
func awaitProvenanceHeldFor(ctx context.Context, t *testing.T, replica *engineReplica, runID string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		res, err := askProvenance(ctx, replica, runID)
		if err == nil && res.GetOwnReference() == orkstackv1.ReferenceState_REFERENCE_STATE_HELD {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s's report.txt reference never became HELD", runID)
}

func TestIntegration_RunEvidenceOfALiveRunIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 2)
	defer s.writeTranscript("run-evidence-live.txt")
	s.startOrker()
	name := fmt.Sprintf("evidence-live-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(evidenceSleeperYAML(name))
	awaitOrkflowVisible(ctx, t, s.replicas[1].flows, name)
	runID := submitOnA(t, s, name, map[string]any{})

	deadline := time.Now().Add(60 * time.Second)
	for {
		got, err := s.replicas[1].runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
		if err == nil && got.Msg.GetRun().GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reported RUNNING", runID)
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, stderr, code := s.cli(1, "--json", "run", "evidence", "export", runID)
	if code == 0 || !strings.Contains(stderr, fmt.Sprintf("run %q is RUNNING; evidence is exported for finished runs only", runID)) {
		t.Fatalf("live export: exit %d stderr %q", code, stderr)
	}
	if run := awaitTerminalOnB(ctx, t, s.replicas[1], runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s", runID, run.GetStatus())
	}
	m := awaitEvidenceArchived(t, s, 1, runID)
	if m.RunStatus != "SUCCEEDED" {
		t.Fatalf("manifest run_status %s", m.RunStatus)
	}
	t.Logf("EVIDENCE run_evidence_live_refused run=%s stderr=%q then exported %d members", runID, strings.TrimSpace(stderr), len(m.Members))
}

func TestIntegration_RunEvidenceTamperedBundleFailsVerifyByName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	e := newEvidenceStack(t, ctx)
	defer e.writeTranscript("run-evidence-tampered.txt")
	bundle := filepath.Join(e.dir, "bundle.tar.gz")
	e.mustCLI(1, "run", "evidence", "export", e.runID, "--output", bundle)
	tampered := filepath.Join(e.dir, "tampered.tar.gz")
	rewriteBundle(t, bundle, tampered, func(name string, body []byte) []byte {
		if name == "tasks/completed/artifacts/report.txt" {
			body = bytes.Clone(body)
			body[0] ^= 0x01
		}
		return body
	})
	stdout, _, code := e.cli(1, "run", "evidence", "verify", tampered)
	if code != exitError {
		t.Fatalf("verify of a tampered bundle exited %d, want %d:\n%s", code, exitError, stdout)
	}
	var line string
	for _, l := range strings.Split(stdout, "\n") {
		if strings.Contains(l, "tasks/completed/artifacts/report.txt") && strings.Contains(l, "digest mismatch") {
			line = strings.TrimSpace(l)
		}
	}
	if line == "" {
		t.Fatalf("verify did not name the tampered member:\n%s", stdout)
	}
	t.Logf("EVIDENCE run_evidence_tampered %s", line)
}

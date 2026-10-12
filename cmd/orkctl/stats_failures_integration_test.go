//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests drive the failure breakdown against real failures: a real
// engine, a real Orker running the first-party shell image (the image
// test/fixtures/bash_activity_contract.sh holds to the ABI), real Temporal,
// real MinIO. Each failure mode is produced the way production produces it —
// a container that exits 1, a container that exits 0 having left a directory
// where an artifact must be a regular file, an image the Orker cannot pull
// for its platform — and the breakdown is asked for
// through the CLI an operator would use.

// failureCorpus is one namespace holding three different kinds of failure.
type failureCorpus struct {
	replicas    []*engineReplica
	namespace   string
	since       time.Time
	until       time.Time
	exitRuns    []string // CONTAINER_EXIT_NONZERO / ACTIVITY_FAILED
	missingRuns []string // ARTIFACT_NOT_A_FILE / CONTRACT_VIOLATION
	pullRuns    []string // IMAGE_PULL_FAILED / INFRASTRUCTURE_ERROR
	succeeded   []string
	artifactCfg artifacts.Config
}

func (c *failureCorpus) failed() int { return len(c.exitRuns) + len(c.missingRuns) + len(c.pullRuns) }

// seedFailureCorpus boots the stack with `replicas` engine processes sharing
// one Temporal namespace and bucket, and submits every run to the first.
func seedFailureCorpus(ctx context.Context, t *testing.T, replicas int) *failureCorpus {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)
	registry := testRegistry()
	requireRegistry(t, registry)

	partition := fmt.Sprintf("ork-fp-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")
	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, queue)

	corpus := &failureCorpus{artifactCfg: artifactCfg}
	for i := 0; i < replicas; i++ {
		replica := startReplica(ctx, t, fmt.Sprintf("ork-%c", 'A'+i), enginePath, address, partition, queue)
		replica.awaitReady(ctx, t)
		corpus.replicas = append(corpus.replicas, replica)
	}
	a := corpus.replicas[0]

	stamp := time.Now().UnixNano()
	corpus.namespace = fmt.Sprintf("fp-%d-%d", os.Getpid(), stamp%1_000_000_000)
	namespaces := orkstackv1connect.NewNamespaceServiceClient(h2cClient(t), "http://"+a.addr)
	if _, err := namespaces.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: corpus.namespace},
	})); err != nil {
		t.Fatalf("CreateNamespace(%s): %v", corpus.namespace, err)
	}

	unpullable := pushForeignPlatformIndex(ctx, t, registry, fmt.Sprintf("orkstack-it/fp-unpullable-%d", stamp))
	apply := func(yaml string) {
		t.Helper()
		if _, err := a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: yaml, Namespace: corpus.namespace,
		})); err != nil {
			t.Fatalf("ApplyOrkflow in %s: %v\n%s", corpus.namespace, err, yaml)
		}
	}
	apply(fingerprintOrkflowYAML("fp-exit", "shell", `["sh", "-c", "exit 1"]`))
	apply(fingerprintOrkflowYAML("fp-dir-artifact", "shell", `["sh", "-c", "mkdir -p /orkstack/output/artifacts/workspace"]`))
	apply(fingerprintOrkflowYAML("fp-pull", unpullable, `["sh", "-c", "exit 0"]`))
	apply(fingerprintOrkflowYAML("fp-ok", "shell", `["sh", "-c", "exit 0"]`))

	corpus.since = time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	submit := func(orkflow string) string {
		t.Helper()
		submitted, err := a.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: orkflow, Namespace: corpus.namespace,
		}))
		if err != nil {
			t.Fatalf("SubmitRun %s: %v", orkflow, err)
		}
		return submitted.Msg.GetRun().GetId()
	}
	corpus.exitRuns = []string{submit("fp-exit"), submit("fp-exit"), submit("fp-exit")}
	corpus.missingRuns = []string{submit("fp-dir-artifact"), submit("fp-dir-artifact")}
	corpus.pullRuns = []string{submit("fp-pull")}
	corpus.succeeded = []string{submit("fp-ok")}

	for _, id := range append(append(append(append([]string(nil), corpus.exitRuns...), corpus.missingRuns...),
		corpus.pullRuns...), corpus.succeeded...) {
		run := awaitTerminalIn(ctx, t, a, corpus.namespace, id)
		t.Logf("run %s (%s) ended %s: %v", id, run.GetOrkflowName(), run.GetStatus(), run.GetError())
	}
	awaitArchived(ctx, t, artifactCfg, corpus.namespace, corpus.failed()+len(corpus.succeeded))
	corpus.until = time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	return corpus
}

// awaitTerminalIn polls GetRun, in the run's own namespace, until the run has
// ended.
func awaitTerminalIn(ctx context.Context, t *testing.T, replica *engineReplica, namespace, runID string) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var (
		last    *orkstackv1.Run
		lastErr error
	)
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: runID, Namespace: namespace,
		}))
		lastErr = err
		if err == nil {
			last = got.Msg.GetRun()
			switch last.GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
				return last
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("replica %s never reported run %s terminal (last %v, %v)", replica.name, runID, last, lastErr)
	return nil
}

// fingerprintOrkflowYAML is one task that does not retry, so each run is one
// attempt and one failure. The shell image's `exec` op runs the argv and
// writes values.json itself, so a failure here is the one the argv causes.
func fingerprintOrkflowYAML(name, uses, command string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: failure fingerprint probe.
tasks:
  - id: work
    uses: %s
    timeout_seconds: 60
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: %s
`, name, uses, command)
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
// behind an OCI index. The engine resolves and pins it at apply, because the
// index is there and hashes to its pin; the Orker's `docker pull` of that
// digest finds no manifest for its own platform and fails. That is
// IMAGE_PULL_FAILED at run time, not at apply — the only way a registry-backed
// apply lets an unpullable image through (a reference to a missing image is
// refused at apply, and the registry refuses an index with missing children).
func pushForeignPlatformIndex(ctx context.Context, t *testing.T, registry, repository string) string {
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
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(blob))
		started := do(http.MethodPost, base+"/blobs/uploads/", "", nil, http.StatusAccepted)
		location, err := started.Location()
		if err != nil {
			t.Fatalf("blob upload location: %v", err)
		}
		query := location.Query()
		query.Set("digest", digest)
		location.RawQuery = query.Encode()
		do(http.MethodPut, location.String(), "application/octet-stream", blob, http.StatusCreated)
		return digest
	}
	pushManifest := func(reference, mediaType string, body []byte) string {
		t.Helper()
		do(http.MethodPut, base+"/manifests/"+reference, mediaType, body, http.StatusCreated)
		return fmt.Sprintf("sha256:%x", sha256.Sum256(body))
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
	manifestDigest := pushManifest("s390x", "application/vnd.oci.image.manifest.v1+json", manifest)
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[`+
		`{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":%d,"platform":{"architecture":"s390x","os":"linux"}}]}`,
		manifestDigest, len(manifest)))
	pushManifest("v1", "application/vnd.oci.image.index.v1+json", index)
	return fmt.Sprintf("%s/%s:v1", registry, repository)
}

// orkctlAgainst runs the CLI in-process against one replica and returns
// stdout, failing the test on a non-zero exit.
func orkctlAgainst(t *testing.T, replica *engineReplica, args ...string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	var stdout, stderr bytes.Buffer
	full := append(append([]string(nil), args...), "--server", "http://"+replica.addr)
	if code := Execute(full, &stdout, &stderr); code != exitOK {
		t.Fatalf("orkctl %s exited %d: %s", strings.Join(args, " "), code, stderr.String())
	}
	return stdout.String()
}

func decodeBreakdown(t *testing.T, raw string) *orkstackv1.GetFailureBreakdownResponse {
	t.Helper()
	out := &orkstackv1.GetFailureBreakdownResponse{}
	if err := api.UnmarshalJSON([]byte(raw), out); err != nil {
		t.Fatalf("decode the breakdown JSON: %v\n%s", err, raw)
	}
	return out
}

func (c *failureCorpus) windowArgs() []string {
	return []string{"--since", c.since.Format(time.RFC3339), "--until", c.until.Format(time.RFC3339)}
}

func (c *failureCorpus) breakdownArgs(extra ...string) []string {
	args := append([]string{"-n", c.namespace, "stats", "failures"}, c.windowArgs()...)
	return append(args, extra...)
}

func groupWithCode(t *testing.T, resp *orkstackv1.GetFailureBreakdownResponse, code string) *orkstackv1.FailureGroup {
	t.Helper()
	var found *orkstackv1.FailureGroup
	for _, group := range resp.GetGroups() {
		if group.GetCode() != code {
			continue
		}
		if found != nil {
			t.Fatalf("code %s is split across two groups: %v and %v", code, found, group)
		}
		found = group
	}
	if found == nil {
		t.Fatalf("no group has code %s: %v", code, resp.GetGroups())
	}
	return found
}

// attemptImage is what `orkctl run attempts --json` reports as the image one
// run's task ran.
func attemptImage(t *testing.T, replica *engineReplica, namespace, runID string) string {
	t.Helper()
	raw := orkctlAgainst(t, replica, "-n", namespace, "--json", "run", "attempts", runID)
	var decoded struct {
		Attempts []struct {
			Image struct {
				Pinned string `json:"pinned"`
			} `json:"image"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("decode run attempts --json: %v\n%s", err, raw)
	}
	if len(decoded.Attempts) == 0 {
		t.Fatalf("run %s lists no attempts:\n%s", runID, raw)
	}
	return decoded.Attempts[len(decoded.Attempts)-1].Image.Pinned
}

func TestIntegration_FailureBreakdownGroupsRealFailuresAndReconcilesWithStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFailureCorpus(ctx, t, 1)
	a := corpus.replicas[0]

	defaultJSON := orkctlAgainst(t, a, corpus.breakdownArgs("--json")...)
	defaultHuman := orkctlAgainst(t, a, corpus.breakdownArgs()...)
	imageJSON := orkctlAgainst(t, a, corpus.breakdownArgs("--group-by", "task,code,image,phase", "--json")...)
	imageHuman := orkctlAgainst(t, a, corpus.breakdownArgs("--group-by", "task,code,image,phase")...)
	statsJSON := orkctlAgainst(t, a, "-n", corpus.namespace, "--json", "stats", "--since", corpus.since.Format(time.RFC3339))

	byDefault := decodeBreakdown(t, defaultJSON)
	byImage := decodeBreakdown(t, imageJSON)
	stats := &orkstackv1.GetStatsResponse{}
	if err := api.UnmarshalJSON([]byte(statsJSON), stats); err != nil {
		t.Fatalf("decode stats --json: %v", err)
	}
	// And the same pinned window asked of GetStats directly, since `orkctl
	// stats` has no --until: the two must agree on both.
	pinned, err := statsClientFor(t, a).GetStats(ctx, connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: corpus.namespace, Since: timestamppb.New(corpus.since), Until: timestamppb.New(corpus.until),
	}))
	if err != nil {
		t.Fatalf("GetStats for the pinned window: %v", err)
	}

	failed := stats.GetRunsByStatus()["failed"]
	if failed != int64(corpus.failed()) || pinned.Msg.GetRunsByStatus()["failed"] != failed {
		t.Fatalf("stats counts %d failed (pinned %d), want the %d seeded",
			failed, pinned.Msg.GetRunsByStatus()["failed"], corpus.failed())
	}
	var sum int64
	for _, group := range byDefault.GetGroups() {
		sum += group.GetRunCount()
	}
	if byDefault.GetFailedRunsInWindow() != failed || byDefault.GetFailedRunsGrouped() != failed || sum != failed {
		t.Fatalf("breakdown in_window=%d grouped=%d sum(run_count)=%d, stats failed=%d",
			byDefault.GetFailedRunsInWindow(), byDefault.GetFailedRunsGrouped(), sum, failed)
	}
	if len(byDefault.GetGroups()) != 3 {
		t.Fatalf("default grouping has %d groups, want 3:\n%s", len(byDefault.GetGroups()), defaultHuman)
	}

	for _, want := range []struct {
		code     string
		category orkstackv1.FailureCategory
		orkflow  string
		runs     []string
	}{
		{"CONTAINER_EXIT_NONZERO", orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, "fp-exit", corpus.exitRuns},
		{"ARTIFACT_NOT_A_FILE", orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, "fp-dir-artifact", corpus.missingRuns},
		{"IMAGE_PULL_FAILED", orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, "fp-pull", corpus.pullRuns},
	} {
		group := groupWithCode(t, byDefault, want.code)
		if group.GetCategory() != want.category || group.GetOrkflowName() != want.orkflow ||
			group.GetTaskId() != "work" || group.GetRunCount() != int64(len(want.runs)) {
			t.Errorf("group %s = %v, want %s/work/%s ×%d", want.code, group, want.orkflow, want.category, len(want.runs))
		}
		imageGroup := groupWithCode(t, byImage, want.code)
		example := imageGroup.GetExampleRunIds()[0]
		if got := attemptImage(t, a, corpus.namespace, example); got != imageGroup.GetImagePinned() {
			t.Errorf("group %s says image %q; `orkctl run attempts %s` says %q",
				want.code, imageGroup.GetImagePinned(), example, got)
		}
		if imageGroup.GetAttemptEvidenceMissing() != 0 {
			t.Errorf("group %s reports %d runs' attempt records missing before any was reclaimed",
				want.code, imageGroup.GetAttemptEvidenceMissing())
		}
	}
	if exit := groupWithCode(t, byImage, "CONTAINER_EXIT_NONZERO"); !strings.Contains(exit.GetImagePinned(), "@sha256:") {
		t.Errorf("the exit-1 group's image %q is not a pinned digest", exit.GetImagePinned())
	}

	proof.Write(t, "stats-failures", "default.json", []byte(defaultJSON))
	proof.Write(t, "stats-failures", "default.txt", []byte(defaultHuman))
	proof.Write(t, "stats-failures", "image-phase.json", []byte(imageJSON))
	proof.Write(t, "stats-failures", "image-phase.txt", []byte(imageHuman))
	proof.Write(t, "stats-failures", "stats.json", []byte(statsJSON))
	t.Logf("EVIDENCE stats_failures namespace=%s stats_runsByStatus_failed=%d breakdown_failedRunsGrouped=%d "+
		"breakdown_failedRunsInWindow=%d groups=%d", corpus.namespace, failed,
		byDefault.GetFailedRunsGrouped(), byDefault.GetFailedRunsInWindow(), len(byDefault.GetGroups()))
}

func TestIntegration_FailureBreakdownSaysUnavailableAfterAttemptRecordsAreReclaimed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFailureCorpus(ctx, t, 1)
	a := corpus.replicas[0]

	before := decodeBreakdown(t, orkctlAgainst(t, a, corpus.breakdownArgs("--json")...))

	// The cleanup worker's own call, on one exit-1 run.
	reclaimed := corpus.exitRuns[0]
	store, err := artifacts.NewRunArchive(corpus.artifactCfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	if _, err := store.DeleteRunArtifacts(ctx, corpus.namespace, reclaimed); err != nil {
		t.Fatalf("DeleteRunArtifacts(%s): %v", reclaimed, err)
	}

	afterJSON := orkctlAgainst(t, a, corpus.breakdownArgs("--group-by", "task,code,image", "--json")...)
	afterHuman := orkctlAgainst(t, a, corpus.breakdownArgs("--group-by", "task,code,image")...)
	after := decodeBreakdown(t, afterJSON)
	if !after.GetAttemptEvidenceRead() || after.GetAttemptEvidenceMissing() != 1 {
		t.Fatalf("after the reclaim: attempt_evidence_read=%v missing=%d, want true and 1",
			after.GetAttemptEvidenceRead(), after.GetAttemptEvidenceMissing())
	}
	var unavailableGroup *orkstackv1.FailureGroup
	for _, group := range after.GetGroups() {
		if group.GetAttemptEvidenceMissing() > 0 {
			unavailableGroup = group
		}
	}
	if unavailableGroup == nil || unavailableGroup.GetAttemptEvidenceMissing() != 1 ||
		unavailableGroup.GetImagePinned() != "" || unavailableGroup.GetCode() != "CONTAINER_EXIT_NONZERO" ||
		len(unavailableGroup.GetExampleRunIds()) != 1 || unavailableGroup.GetExampleRunIds()[0] != reclaimed {
		t.Fatalf("the reclaimed run's group = %v, want CONTAINER_EXIT_NONZERO with an empty image and missing 1 (%s)",
			unavailableGroup, reclaimed)
	}
	if !strings.Contains(afterHuman, unavailable) ||
		!strings.Contains(afterHuman, "attempt records for 1 run were reclaimed or never written") {
		t.Fatalf("the human table does not say unavailable:\n%s", afterHuman)
	}

	// The default grouping reads no attempt record, so a reclaim changes
	// nothing. --limit 50 is the default's own value, spelled out so the
	// question is a different cache key and is computed afresh.
	again := decodeBreakdown(t, orkctlAgainst(t, a, corpus.breakdownArgs("--limit", "50", "--json")...))
	before.ComputedAt, again.ComputedAt = nil, nil
	if !proto.Equal(before, again) {
		t.Fatalf("the default grouping moved after a reclaim:\n before %v\n after  %v", before, again)
	}

	proof.Write(t, "stats-failures", "after-reclaim.json", []byte(afterJSON))
	proof.Write(t, "stats-failures", "after-reclaim.txt", []byte(afterHuman))
	t.Logf("EVIDENCE stats_failures_reclaim run=%s attemptEvidenceMissing=%d", reclaimed, after.GetAttemptEvidenceMissing())
}

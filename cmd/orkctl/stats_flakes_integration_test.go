//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests drive the flake index against real retries: a real engine, a
// real Orker running the first-party shell image, real Temporal, real MinIO.
// The flaky task reads its own attempt number from task.json — the OCI
// contract's §3.2 field — and fails attempt 1, so it retries once and then
// succeeds, deterministically, through the same contract every activity uses.
// Every run SUCCEEDS: the retry is invisible to every other stats number, and
// that is the property under test.

// flakeCorpus is one namespace holding runs of one orkflow whose `flaky` task
// retries once and whose `steady` task never does.
type flakeCorpus struct {
	replicas    []*engineReplica
	namespace   string
	orkflow     string
	since       time.Time
	until       time.Time
	runs        []string
	artifactCfg artifacts.Config
}

// flakeOrkflowYAML is the flake-index fixture: flaky fails on attempt 1 and
// succeeds on attempt 2; steady succeeds first time.
func flakeOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: flake index probe.
tasks:
  - id: flaky
    uses: shell
    timeout_seconds: 30
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
    with:
      op: exec
      command: ["sh", "-c", "[ \"$(jq -r .task.attempt /orkstack/input/task.json)\" -ge 2 ]"]
  - id: steady
    uses: shell
    timeout_seconds: 30
    with:
      op: exec
      command: ["sh", "-c", "true"]
`, name)
}

// seedFlakeCorpus boots the stack with `replicas` engine processes sharing
// one Temporal namespace and bucket, and submits four runs to the first.
func seedFlakeCorpus(ctx context.Context, t *testing.T, replicas int) *flakeCorpus {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	partition := fmt.Sprintf("ork-flake-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")
	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, queue)

	corpus := &flakeCorpus{artifactCfg: artifactCfg}
	for i := 0; i < replicas; i++ {
		replica := startReplica(ctx, t, fmt.Sprintf("ork-%c", 'A'+i), enginePath, address, partition, queue)
		replica.awaitReady(ctx, t)
		corpus.replicas = append(corpus.replicas, replica)
	}
	a := corpus.replicas[0]

	stamp := time.Now().UnixNano()
	corpus.namespace = fmt.Sprintf("flake-%d-%d", os.Getpid(), stamp%1_000_000_000)
	corpus.orkflow = "flake-probe"
	namespaces := orkstackv1connect.NewNamespaceServiceClient(h2cClient(t), "http://"+a.addr)
	if _, err := namespaces.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: corpus.namespace},
	})); err != nil {
		t.Fatalf("CreateNamespace(%s): %v", corpus.namespace, err)
	}
	if _, err := a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: flakeOrkflowYAML(corpus.orkflow), Namespace: corpus.namespace,
	})); err != nil {
		t.Fatalf("ApplyOrkflow in %s: %v", corpus.namespace, err)
	}

	corpus.since = time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	for i := 0; i < 4; i++ {
		submitted, err := a.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: corpus.orkflow, Namespace: corpus.namespace,
		}))
		if err != nil {
			t.Fatalf("SubmitRun %s: %v", corpus.orkflow, err)
		}
		corpus.runs = append(corpus.runs, submitted.Msg.GetRun().GetId())
	}
	for _, id := range corpus.runs {
		run := awaitTerminalIn(ctx, t, a, corpus.namespace, id)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s ended %s (%v); every run of the corpus must succeed", id, run.GetStatus(), run.GetError())
		}
		t.Logf("run %s ended %s", id, run.GetStatus())
	}
	awaitArchived(ctx, t, artifactCfg, corpus.namespace, len(corpus.runs))
	corpus.until = time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	return corpus
}

func (c *flakeCorpus) flakeArgs(extra ...string) []string {
	args := []string{"-n", c.namespace, "stats", "flakes",
		"--since", c.since.Format(time.RFC3339), "--until", c.until.Format(time.RFC3339)}
	return append(args, extra...)
}

func decodeFlakes(t *testing.T, raw string) *orkstackv1.GetFlakeIndexResponse {
	t.Helper()
	out := &orkstackv1.GetFlakeIndexResponse{}
	if err := api.UnmarshalJSON([]byte(raw), out); err != nil {
		t.Fatalf("decode the flake index JSON: %v\n%s", err, raw)
	}
	return out
}

func flakeTaskGroup(t *testing.T, resp *orkstackv1.GetFlakeIndexResponse, task string) *orkstackv1.FlakeGroup {
	t.Helper()
	for _, group := range resp.GetGroups() {
		if group.GetTaskId() == task {
			return group
		}
	}
	t.Fatalf("no group for task %s: %v", task, resp.GetGroups())
	return nil
}

// attemptOrker is the orkerId `orkctl run attempts --json` reports for one
// attempt of one task.
func attemptOrker(t *testing.T, replica *engineReplica, namespace, runID, task string, attempt int32) string {
	t.Helper()
	raw := orkctlAgainst(t, replica, "-n", namespace, "--json", "run", "attempts", runID)
	var decoded struct {
		Attempts []struct {
			TaskID  string `json:"taskId"`
			Attempt int32  `json:"attempt"`
			OrkerID string `json:"orkerId"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("decode run attempts --json: %v\n%s", err, raw)
	}
	for _, record := range decoded.Attempts {
		if record.TaskID == task && record.Attempt == attempt {
			return record.OrkerID
		}
	}
	t.Fatalf("run %s lists no attempt %d of %s:\n%s", runID, attempt, task, raw)
	return ""
}

func TestIntegration_FlakeIndexCountsTheRetriesASucceededRunHides(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFlakeCorpus(ctx, t, 1)
	a := corpus.replicas[0]

	statsJSON := orkctlAgainst(t, a, "-n", corpus.namespace, "--json", "stats", "--since", corpus.since.Format(time.RFC3339))
	flakesJSON := orkctlAgainst(t, a, corpus.flakeArgs("--json")...)
	flakesHuman := orkctlAgainst(t, a, corpus.flakeArgs()...)
	orkerJSON := orkctlAgainst(t, a, corpus.flakeArgs("--group-by", "task,orker", "--json")...)

	stats := &orkstackv1.GetStatsResponse{}
	if err := api.UnmarshalJSON([]byte(statsJSON), stats); err != nil {
		t.Fatalf("decode stats --json: %v", err)
	}
	if stats.GetRunsByStatus()["failed"] != 0 || !stats.GetSuccessRateDefined() || stats.GetSuccessRate() != 1 {
		t.Fatalf("stats sees failed=%d success_rate=%v; the retries must be invisible there",
			stats.GetRunsByStatus()["failed"], stats.GetSuccessRate())
	}
	pinned, err := statsClientFor(t, a).GetStats(ctx, connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: corpus.namespace, Since: timestamppb.New(corpus.since), Until: timestamppb.New(corpus.until),
	}))
	if err != nil {
		t.Fatalf("GetStats for the pinned window: %v", err)
	}
	byStatus := pinned.Msg.GetRunsByStatus()
	finished := byStatus["succeeded"] + byStatus["failed"] + byStatus["canceled"]

	flakes := decodeFlakes(t, flakesJSON)
	if flakes.GetRunsInWindow() != finished || finished != int64(len(corpus.runs)) {
		t.Fatalf("runs_in_window=%d, stats finished=%d, want both %d", flakes.GetRunsInWindow(), finished, len(corpus.runs))
	}
	flaky := flakeTaskGroup(t, flakes, "flaky")
	if flaky.GetRunsObserved() != 4 || flaky.GetRunsRetried() != 4 || flaky.GetRetryRate() != 1 ||
		flaky.GetP50Attempts() != 2 || flaky.GetP95Attempts() != 2 || flaky.GetRetryAttempts() != 4 ||
		flaky.GetDominantCode() != "CONTAINER_EXIT_NONZERO" ||
		flaky.GetDominantCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED ||
		flaky.GetDominantCount() != 4 {
		t.Fatalf("the flaky group = %v", flaky)
	}
	steady := flakeTaskGroup(t, flakes, "steady")
	if steady.GetRunsRetried() != 0 || steady.GetRetryRate() != 0 || !steady.GetRetryRateDefined() || steady.GetRunsObserved() != 4 {
		t.Fatalf("the steady group = %v", steady)
	}
	if flakes.GetRunsEvidenceMissing() != 0 || flakes.GetRunsEvidenceComplete() != 4 {
		t.Fatalf("evidence complete=%d missing=%d, want 4 and 0",
			flakes.GetRunsEvidenceComplete(), flakes.GetRunsEvidenceMissing())
	}

	byOrker := decodeFlakes(t, orkerJSON)
	if byOrker.GetBodyKeyed() {
		t.Fatalf("a grouping by orker answered body_keyed")
	}
	var retriedOn *orkstackv1.FlakeGroup
	for _, group := range byOrker.GetGroups() {
		if group.GetTaskId() == "flaky" && group.GetRunsRetried() > 0 {
			retriedOn = group
		}
	}
	if retriedOn == nil || len(retriedOn.GetExampleRunIds()) == 0 {
		t.Fatalf("no orker group retried flaky: %v", byOrker.GetGroups())
	}
	example := retriedOn.GetExampleRunIds()[0]
	if got := attemptOrker(t, a, corpus.namespace, example, "flaky", 1); got == "" || got != retriedOn.GetOrkerId() {
		t.Fatalf("the orker group says %q; `orkctl run attempts %s` says attempt 1 ran on %q",
			retriedOn.GetOrkerId(), example, got)
	}
	if !strings.Contains(flakesHuman, "flaky") || !strings.Contains(flakesHuman, "100.0%") {
		t.Fatalf("the human table does not show flaky at 100.0%%:\n%s", flakesHuman)
	}

	proof.Write(t, "stats-flakes", "stats.json", []byte(statsJSON))
	proof.Write(t, "stats-flakes", "flakes.json", []byte(flakesJSON))
	proof.Write(t, "stats-flakes", "flakes-orker.json", []byte(orkerJSON))
	proof.Write(t, "stats-flakes", "flakes.txt", []byte(flakesHuman))
	t.Logf("EVIDENCE stats_flakes namespace=%s stats_failed=%d stats_success_rate=%v flaky_runsRetried=%d "+
		"flaky_retryRate=%v flaky_dominant=%s/%s steady_runsRetried=%d runsInWindow=%d orker=%s",
		corpus.namespace, stats.GetRunsByStatus()["failed"], stats.GetSuccessRate(), flaky.GetRunsRetried(),
		flaky.GetRetryRate(), flaky.GetDominantCategory(), flaky.GetDominantCode(), steady.GetRunsRetried(),
		flakes.GetRunsInWindow(), retriedOn.GetOrkerId())
}

func TestIntegration_FlakeIndexExcludesReclaimedRunsRatherThanCountingThemClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFlakeCorpus(ctx, t, 1)
	a := corpus.replicas[0]

	store, err := artifacts.NewRunArchive(corpus.artifactCfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	reclaim := func(ids ...string) {
		t.Helper()
		for _, id := range ids {
			// The cleanup worker's own call.
			if _, err := store.DeleteRunArtifacts(ctx, corpus.namespace, id); err != nil {
				t.Fatalf("DeleteRunArtifacts(%s): %v", id, err)
			}
		}
	}
	reclaim(corpus.runs[0], corpus.runs[1])

	halfJSON := orkctlAgainst(t, a, corpus.flakeArgs("--json")...)
	halfHuman := orkctlAgainst(t, a, corpus.flakeArgs()...)
	half := decodeFlakes(t, halfJSON)
	flaky := flakeTaskGroup(t, half, "flaky")
	if flaky.GetRunsObserved() != 2 || flaky.GetRunsRetried() != 2 || flaky.GetRetryRate() != 1 ||
		flaky.GetRunsEvidenceMissing() != 2 || half.GetRunsEvidenceMissing() != 2 {
		t.Fatalf("after reclaiming two of four: flaky=%v response missing=%d; want observed 2, retried 2, rate 1 (not 0.5), missing 2",
			flaky, half.GetRunsEvidenceMissing())
	}
	const footer = "2 runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean"
	if !strings.Contains(halfHuman, footer) {
		t.Fatalf("the human table does not carry the exclusion footer:\n%s", halfHuman)
	}

	reclaim(corpus.runs[2], corpus.runs[3])
	// A different limit is a different cache key, so this is computed afresh.
	noneJSON := orkctlAgainst(t, a, corpus.flakeArgs("--limit", "49", "--json")...)
	noneHuman := orkctlAgainst(t, a, corpus.flakeArgs("--limit", "48")...)
	none := decodeFlakes(t, noneJSON)
	gone := flakeTaskGroup(t, none, "flaky")
	if gone.GetRetryRateDefined() || gone.GetRunsObserved() != 0 || gone.GetRunsEvidenceMissing() != 4 {
		t.Fatalf("after reclaiming all four: flaky=%v; want retry_rate_defined false, observed 0, missing 4", gone)
	}
	var flakyLine string
	for _, line := range strings.Split(noneHuman, "\n") {
		if fields := strings.Fields(line); len(fields) > 1 && fields[1] == "flaky" {
			flakyLine = line
		}
	}
	if !strings.Contains(flakyLine, unavailable) {
		t.Fatalf("the flaky row does not read %q:\n%s", unavailable, noneHuman)
	}

	proof.Write(t, "stats-flakes", "after-reclaim-two.json", []byte(halfJSON))
	proof.Write(t, "stats-flakes", "after-reclaim-two.txt", []byte(halfHuman))
	proof.Write(t, "stats-flakes", "after-reclaim-all.json", []byte(noneJSON))
	proof.Write(t, "stats-flakes", "after-reclaim-all.txt", []byte(noneHuman))
	t.Logf("EVIDENCE stats_flakes_reclaim runsObserved=%d runsRetried=%d retryRate=%v runsEvidenceMissing=%d "+
		"after_all_retryRateDefined=%t", flaky.GetRunsObserved(), flaky.GetRunsRetried(), flaky.GetRetryRate(),
		flaky.GetRunsEvidenceMissing(), gone.GetRetryRateDefined())
}

// clearComputed returns a copy of a flake index with computed_at cleared, for
// comparing two replicas' answers.
func clearComputed(resp *orkstackv1.GetFlakeIndexResponse) *orkstackv1.GetFlakeIndexResponse {
	out := proto.Clone(resp).(*orkstackv1.GetFlakeIndexResponse)
	out.ComputedAt = nil
	return out
}

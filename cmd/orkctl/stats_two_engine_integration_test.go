//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TwoReplicasReportTheSameStatsForTwoNamespaces is the
// central claim, measured on two real `ork` processes.
//
// Every run below is submitted to replica A: replica B accepts no submission
// and starts no run. What replica B holds in its own RunStore is deliberately
// NOT controlled — both replicas poll the projection channel, and a projection
// activity applies the run it carries to whichever replica happened to execute
// it (EventActivities.PublishEvent). That is the point rather than a gap:
// neither replica's memory is consulted, so what is in it cannot matter, and
// both answer with the SAME numbers because both derive them from the two
// shared records (AGENTS.md §7). The stronger claim — that a run present
// ONLY in a replica's memory is not counted — is proven where it can be
// controlled, in TestGetStats_IgnoresTheRunsThisProcessRemembers.
//
// The window is PINNED for the comparison. An unpinned one ends at each
// replica's own `now`, so a run finishing between the two calls would make
// them differ for a reason that is not the property under test. Pinned, any
// difference is the defect.
//
// Two OrkStack namespaces are seeded with a known set of runs, both holding an
// orkflow of the same name, and each namespace's numbers are asserted exactly
// and asserted to exclude the other's.
//
// Isolation (AGENTS.md §11.1): a unique deployment partition per test run. Note
// the deliberate inversion of the usual rule — the two replicas SHARE it,
// because sharing is the condition under test.
func TestIntegration_TwoReplicasReportTheSameStatsForTwoNamespaces(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	partition := statsPartition()
	t.Logf("partition %s", partition)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")

	channel := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, channel)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, partition, channel)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	statsA := statsClientFor(t, replicaA)
	statsB := statsClientFor(t, replicaB)

	// ---- two namespaces, one orkflow name in each -------------------------
	stamp := time.Now().UnixNano()
	alpha := fmt.Sprintf("alpha-%d-%d", os.Getpid(), stamp)
	beta := fmt.Sprintf("beta-%d-%d", os.Getpid(), stamp)
	namespacesA := orkstackv1connect.NewNamespaceServiceClient(h2cClient(t), "http://"+replicaA.addr)
	for _, name := range []string{alpha, beta} {
		if _, err := namespacesA.CreateNamespace(ctx,
			connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: name},
			})); err != nil {
			t.Fatalf("CreateNamespace(%s) on replica A: %v", name, err)
		}
	}

	// "shipit" exists in both namespaces and is a different orkflow in each,
	// which is what a namespace-blind scan gets wrong.
	apply := func(namespace, yaml string) {
		t.Helper()
		if _, err := replicaA.flows.ApplyOrkflow(ctx,
			connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
				Yaml: yaml, Namespace: namespace,
			})); err != nil {
			t.Fatalf("ApplyOrkflow in %s: %v", namespace, err)
		}
	}
	apply(alpha, statsOrkflowYAML("shipit", "echo shipped"))
	apply(alpha, statsOrkflowYAML("breakit", "exit 3"))
	apply(alpha, statsSlowOrkflowYAML("crawlit"))
	apply(beta, statsOrkflowYAML("shipit", "exit 4"))

	// ---- a known set of runs, all submitted to A ---------------------------
	submit := func(namespace, orkflow string) string {
		t.Helper()
		submitted, err := replicaA.runs.SubmitRun(ctx,
			connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName: orkflow, Namespace: namespace,
			}))
		if err != nil {
			t.Fatalf("SubmitRun %s/%s on replica A: %v", namespace, orkflow, err)
		}
		return submitted.Msg.GetRun().GetId()
	}
	windowSince := time.Now().Add(-time.Hour).UTC()
	finished := map[string][]string{}
	for i := 0; i < 3; i++ {
		finished[alpha] = append(finished[alpha], submit(alpha, "shipit"))
	}
	finished[alpha] = append(finished[alpha], submit(alpha, "breakit"))
	for i := 0; i < 2; i++ {
		finished[beta] = append(finished[beta], submit(beta, "shipit"))
	}

	// One run that is still going while the readings are taken, so the live
	// half of the answer is not trivially empty on both replicas.
	slow := submit(alpha, "crawlit")
	t.Logf("alpha=%s beta=%s slow=%s", alpha, beta, slow)

	// ---- every finished run is in the archive before anything is read ------
	awaitArchived(ctx, t, artifactCfg, alpha, 4)
	awaitArchived(ctx, t, artifactCfg, beta, 2)

	// The slow run has to be genuinely live, or "runs currently running" is a
	// claim about nothing.
	if status := executionStatus(ctx, t, temporalClient, slow); status !=
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the slow run reached %s before the readings; the orkflow is too fast "+
			"for this test to observe a live run", status)
	}

	windowUntil := time.Now().Add(time.Minute).UTC()
	read := func(stats orkstackv1connect.StatsServiceClient, namespace string) *orkstackv1.GetStatsResponse {
		t.Helper()
		got, err := stats.GetStats(ctx, connect.NewRequest(&orkstackv1.GetStatsRequest{
			Namespace: namespace,
			Since:     timestamppb.New(windowSince),
			Until:     timestamppb.New(windowUntil),
		}))
		if err != nil {
			t.Fatalf("GetStats(%s): %v", namespace, err)
		}
		return got.Msg
	}

	// Each reading is named for the replica that PRODUCED it, and the order is
	// deliberately opposite between the two namespaces: alpha is asked of A
	// first, beta of B first. An answer that was really the other replica's —
	// a warmed cache, a result carried across — would have to survive both
	// orders to go unnoticed.
	alphaA, alphaB := read(statsA, alpha), read(statsB, alpha)
	betaB, betaA := read(statsB, beta), read(statsA, beta)

	// The readings were taken while the slow run was still executing, so the
	// live half above is a live half.
	if status := executionStatus(ctx, t, temporalClient, slow); status !=
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the slow run closed (%s) during the readings; they no longer describe "+
			"a namespace with a run in it", status)
	}

	// ---- exact numbers, and A excludes B ----------------------------------
	assertStatsCounts(t, "alpha", alphaA, map[string]int64{
		"running": 1, "succeeded": 3, "failed": 1, "canceled": 0,
	})
	assertStatsRate(t, "alpha", alphaA, 0.75)
	assertStatsFailing(t, "alpha", alphaA, "breakit", 1, 1)
	assertStatsRunning(t, "alpha", alphaA, slow)

	assertStatsCounts(t, "beta", betaA, map[string]int64{
		"running": 0, "succeeded": 0, "failed": 2, "canceled": 0,
	})
	assertStatsRate(t, "beta", betaA, 0)
	// Both namespaces hold "shipit". Alpha's succeeds and beta's fails, so a
	// scan that ignored the namespace would report shipit failing in alpha.
	assertStatsFailing(t, "beta", betaA, "shipit", 2, 2)
	for _, row := range alphaA.GetMostFailing() {
		if row.GetOrkflowName() == "shipit" {
			t.Errorf("alpha reports its own shipit failing %d times; those are beta's",
				row.GetFailed())
		}
	}
	if alphaA.GetDurationSampleCount() != 4 || betaA.GetDurationSampleCount() != 2 {
		t.Errorf("duration samples are alpha=%d beta=%d, want 4 and 2",
			alphaA.GetDurationSampleCount(), betaA.GetDurationSampleCount())
	}
	for _, percentile := range []struct {
		name string
		got  time.Duration
	}{
		{"p50", alphaA.GetP50Duration().AsDuration()},
		{"p95", alphaA.GetP95Duration().AsDuration()},
	} {
		if percentile.got <= 0 {
			t.Errorf("alpha %s is %v; a percentile over four finished runs is a real duration",
				percentile.name, percentile.got)
		}
	}

	// ---- the two replicas agree, number for number ------------------------
	assertSameStats(t, "alpha", alphaA, alphaB)
	assertSameStats(t, "beta", betaA, betaB)

	writeStatsTranscript(t, partition, alpha, beta, slow,
		windowSince, windowUntil, alphaA, alphaB, betaA, betaB)

	// Leave nothing executing behind this test.
	if _, err := replicaB.runs.CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{
		RunId: slow, Namespace: alpha, Reason: "stats proof finished",
	})); err != nil {
		t.Logf("cancel the slow run: %v", err)
	}
}

// statsPartition is this test run's deployment partition, unique to it and
// SHARED by both replicas — AGENTS.md §11.1's deliberate exception, because two
// replicas answering for one set of runs is the condition under test and
// splitting them would make the test fail for a reason unrelated to the
// property.
func statsPartition() string {
	return fmt.Sprintf("ork-stats-%d-%d",
		os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// statsClientFor dials one replica's stats surface.
func statsClientFor(t *testing.T, replica *engineReplica) orkstackv1connect.StatsServiceClient {
	t.Helper()
	return orkstackv1connect.NewStatsServiceClient(h2cClient(t), "http://"+replica.addr)
}

// awaitArchived waits until a namespace's finished runs are in the archive
// INDEX, reading the bucket directly rather than through the surface under
// test — a wait that used GetStats could not tell "not yet archived" from "the
// aggregate is wrong".
func awaitArchived(
	ctx context.Context,
	t *testing.T,
	cfg artifacts.Config,
	namespace string,
	want int,
) {
	t.Helper()
	store, err := artifacts.NewRunArchive(cfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	deadline := time.Now().Add(3 * time.Minute)
	var last int
	for time.Now().Before(deadline) {
		entries, err := store.ListRuns(ctx, artifacts.RunQuery{Namespace: namespace})
		if err != nil {
			t.Fatalf("list the archive index for %s: %v", namespace, err)
		}
		last = len(entries)
		if last >= want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("namespace %s has %d archived runs, want %d", namespace, last, want)
}

func assertStatsCounts(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want map[string]int64,
) {
	t.Helper()
	for status, count := range want {
		if got.GetRunsByStatus()[status] != count {
			t.Errorf("%s: runs_by_status[%q] = %d, want %d (whole map %v)",
				namespace, status, got.GetRunsByStatus()[status], count, got.GetRunsByStatus())
		}
	}
}

func assertStatsRate(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want float64,
) {
	t.Helper()
	if !got.GetSuccessRateDefined() {
		t.Fatalf("%s: success_rate is undefined over finished runs", namespace)
	}
	if diff := got.GetSuccessRate() - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("%s: success_rate = %v, want %v", namespace, got.GetSuccessRate(), want)
	}
}

func assertStatsFailing(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	orkflow string,
	failed, total int64,
) {
	t.Helper()
	for _, row := range got.GetMostFailing() {
		if row.GetOrkflowName() != orkflow {
			continue
		}
		if row.GetFailed() != failed || row.GetTotal() != total {
			t.Errorf("%s: most_failing %s = %d/%d, want %d/%d",
				namespace, orkflow, row.GetFailed(), row.GetTotal(), failed, total)
		}
		return
	}
	t.Errorf("%s: most_failing has no %s row (%v)", namespace, orkflow, got.GetMostFailing())
}

func assertStatsRunning(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want string,
) {
	t.Helper()
	ids := runningIDs(got)
	if strings.Join(ids, ",") != want {
		t.Errorf("%s: running = %v, want just the slow run %s", namespace, ids, want)
	}
}

// assertSameStats: two replicas, one set of numbers.
//
// computed_at and a pool's last_seen are excluded, and only those two: both
// record WHEN a reading was taken rather than what it says, and two readings
// taken a moment apart are supposed to differ there. Everything else — every
// count, the rate, both percentiles, the sample count, the coverage, the
// failing table, the live run ids, the pool names and their Orker counts — is
// compared exactly.
func assertSameStats(t *testing.T, namespace string, a, b *orkstackv1.GetStatsResponse) {
	t.Helper()
	if digestA, digestB := statsDigest(a), statsDigest(b); digestA != digestB {
		t.Errorf("the two replicas disagree about %s:\n replica A %s\n replica B %s",
			namespace, digestA, digestB)
	}
}

// statsDigest renders every number of an answer in a fixed order.
func statsDigest(stats *orkstackv1.GetStatsResponse) string {
	var out strings.Builder
	statuses := make([]string, 0, len(stats.GetRunsByStatus()))
	for status := range stats.GetRunsByStatus() {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		fmt.Fprintf(&out, "%s=%d ", status, stats.GetRunsByStatus()[status])
	}
	fmt.Fprintf(&out, "| rate=%v/%t ", stats.GetSuccessRate(), stats.GetSuccessRateDefined())
	fmt.Fprintf(&out, "p50=%v p95=%v samples=%d coverage=%s ",
		stats.GetP50Duration().AsDuration(), stats.GetP95Duration().AsDuration(),
		stats.GetDurationSampleCount(),
		stats.GetDurationCoverageSince().AsTime().Format(time.RFC3339Nano))
	fmt.Fprintf(&out, "| window=%s..%s ",
		stats.GetWindowSince().AsTime().Format(time.RFC3339Nano),
		stats.GetWindowUntil().AsTime().Format(time.RFC3339Nano))
	for _, row := range stats.GetMostFailing() {
		fmt.Fprintf(&out, "| %s:%d/%d ", row.GetOrkflowName(), row.GetFailed(), row.GetTotal())
	}
	fmt.Fprintf(&out, "| running=%s ", strings.Join(runningIDs(stats), ","))
	// The subject of the answer, compared like everything else: a reply that
	// named the wrong namespace would be identical in every number and wrong.
	fmt.Fprintf(&out, "| ns=%s ", stats.GetNamespace())
	for _, pool := range stats.GetPools() {
		fmt.Fprintf(&out, "| pool %s orkers=%d default=%t ",
			pool.GetName(), pool.GetLiveOrkerCount(), pool.GetIsDefault())
	}
	return strings.TrimSpace(out.String())
}

func runningIDs(stats *orkstackv1.GetStatsResponse) []string {
	ids := make([]string, 0, len(stats.GetRunning()))
	for _, run := range stats.GetRunning() {
		ids = append(ids, run.GetId())
	}
	sort.Strings(ids)
	return ids
}

// statsOrkflowYAML is one task that either succeeds or fails, and does not
// retry: a retried failure would still land as one failed run, but it would
// take a multiple of the backoff to get there and the durations would say
// nothing about how long the work takes.
func statsOrkflowYAML(name, command string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: stats probe.
tasks:
  - id: work
    uses: shell
    timeout_seconds: 30
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "%s"]
`, name, command)
}

// statsSlowOrkflowYAML runs long enough for both replicas to be asked while it
// is still going.
func statsSlowOrkflowYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: stats probe, still running.
tasks:
  - id: crawl
    uses: shell
    timeout_seconds: 240
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep 180"]
`, name)
}

// writeStatsTranscript records the transcript under ORK_PROOF_DIR when it is
// set.
func writeStatsTranscript(
	t *testing.T,
	partition, alpha, beta, slow string,
	since, until time.Time,
	alphaA, alphaB, betaA, betaB *orkstackv1.GetStatsResponse,
) {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "# two engine replicas, one cluster partition, two namespaces (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "partition            %s\n", partition)
	fmt.Fprintf(&out, "window               %s .. %s (pinned)\n",
		since.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano))
	fmt.Fprintf(&out, "every run submitted  replica A (ork-A)\n")
	fmt.Fprintf(&out, "still running        %s\n\n", slow)

	for _, section := range []struct {
		namespace string
		order     string
		a, b      *orkstackv1.GetStatsResponse
	}{
		{alpha, "A first, then B", alphaA, alphaB},
		{beta, "B first, then A", betaA, betaB},
	} {
		fmt.Fprintf(&out, "namespace %s\n", section.namespace)
		// Which replica was asked first, so that the two lines below are read
		// as what each process said rather than as the order they were said in.
		fmt.Fprintf(&out, "  read order %s\n", section.order)
		fmt.Fprintf(&out, "  replica A  %s\n", statsDigest(section.a))
		fmt.Fprintf(&out, "  replica B  %s\n", statsDigest(section.b))
		fmt.Fprintf(&out, "  identical  %t\n",
			statsDigest(section.a) == statsDigest(section.b))
		fmt.Fprintf(&out, "  computed   A %s / B %s (the one field allowed to differ)\n\n",
			section.a.GetComputedAt().AsTime().Format(time.RFC3339Nano),
			section.b.GetComputedAt().AsTime().Format(time.RFC3339Nano))
	}
	fmt.Fprintf(&out, "alpha's numbers exclude beta's: %t\n",
		statsDigest(alphaA) != statsDigest(betaA))
	proof.Write(t, "orkctl", "stats-two-replicas.txt", []byte(out.String()))
}

// TestIntegration_TwoReplicasReportTheSameFailureBreakdown is AGENTS.md §7 for
// the one stats read that opens run bodies: every run is submitted to replica
// A, and replica B — which started nothing — must group the same failed runs
// into the same fingerprints. The window is pinned for the reason the stats
// test above pins it. Only computed_at, the one field that records WHEN, may
// differ; everything else is compared with proto.Equal.
func TestIntegration_TwoReplicasReportTheSameFailureBreakdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFailureCorpus(ctx, t, 2)
	statsA, statsB := statsClientFor(t, corpus.replicas[0]), statsClientFor(t, corpus.replicas[1])

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# two engine replicas, one failure breakdown (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "namespace  %s\nwindow     %s .. %s (pinned)\nevery run submitted to replica A (ork-A)\n\n",
		corpus.namespace, corpus.since.Format(time.RFC3339), corpus.until.Format(time.RFC3339))
	for _, groupBy := range [][]orkstackv1.FailureGroupBy{
		nil,
		{
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER,
		},
	} {
		req := &orkstackv1.GetFailureBreakdownRequest{
			Namespace: corpus.namespace, GroupBy: groupBy,
			Since: timestamppb.New(corpus.since), Until: timestamppb.New(corpus.until),
		}
		// B first: it started nothing, so nothing it could hold would help it.
		fromB, err := statsB.GetFailureBreakdown(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("GetFailureBreakdown on replica B: %v", err)
		}
		fromA, err := statsA.GetFailureBreakdown(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("GetFailureBreakdown on replica A: %v", err)
		}
		a, b := fromA.Msg, fromB.Msg
		if a.GetFailedRunsInWindow() != int64(corpus.failed()) {
			t.Fatalf("replica A counts %d failed runs, want %d", a.GetFailedRunsInWindow(), corpus.failed())
		}
		computedA, computedB := a.GetComputedAt().AsTime(), b.GetComputedAt().AsTime()
		a.ComputedAt, b.ComputedAt = nil, nil
		equal := proto.Equal(a, b)
		if !equal {
			t.Errorf("the replicas disagree (group_by %v):\n replica A %v\n replica B %v", groupBy, a, b)
		}
		encodedA, _ := api.MarshalJSON(a)
		fmt.Fprintf(&transcript, "group_by %v\n  replica A computed %s, replica B computed %s\n  answer %s\n  proto.Equal(A, B) with computed_at cleared: %t\n\n",
			a.GetGroupBy(), computedA.Format(time.RFC3339Nano), computedB.Format(time.RFC3339Nano), encodedA, equal)
		t.Logf("EVIDENCE failure_breakdown_two_replicas group_by=%v groups=%d failed_runs_in_window=%d proto_equal=%t",
			a.GetGroupBy(), len(a.GetGroups()), a.GetFailedRunsInWindow(), equal)
	}
	proof.Write(t, "stats-failures", "two-replicas.txt", []byte(transcript.String()))
}

// TestIntegration_TwoReplicasReportTheSameFlakeIndex holds the flake index to
// AGENTS.md §7: every run submitted to replica A, the pinned window asked of
// both, and the two answers equal once computed_at is cleared — both derive
// them from the shared archive and attempt records, never from memory.
func TestIntegration_TwoReplicasReportTheSameFlakeIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	corpus := seedFlakeCorpus(ctx, t, 2)
	statsA, statsB := statsClientFor(t, corpus.replicas[0]), statsClientFor(t, corpus.replicas[1])

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# two engine replicas, one flake index (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "namespace  %s\nwindow     %s .. %s (pinned)\nevery run submitted to replica A (ork-A)\n\n",
		corpus.namespace, corpus.since.Format(time.RFC3339), corpus.until.Format(time.RFC3339))
	for _, groupBy := range [][]orkstackv1.FailureGroupBy{
		nil,
		{
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE,
		},
	} {
		req := &orkstackv1.GetFlakeIndexRequest{
			Namespace: corpus.namespace, GroupBy: groupBy,
			Since: timestamppb.New(corpus.since), Until: timestamppb.New(corpus.until),
		}
		// B first: it started nothing, so nothing it could hold would help it.
		fromB, err := statsB.GetFlakeIndex(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("GetFlakeIndex on replica B: %v", err)
		}
		fromA, err := statsA.GetFlakeIndex(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("GetFlakeIndex on replica A: %v", err)
		}
		if fromA.Msg.GetRunsInWindow() != int64(len(corpus.runs)) {
			t.Fatalf("replica A counts %d runs, want %d", fromA.Msg.GetRunsInWindow(), len(corpus.runs))
		}
		computedA, computedB := fromA.Msg.GetComputedAt().AsTime(), fromB.Msg.GetComputedAt().AsTime()
		a, b := clearComputed(fromA.Msg), clearComputed(fromB.Msg)
		equal := proto.Equal(a, b)
		if !equal {
			t.Errorf("the replicas disagree (group_by %v):\n replica A %v\n replica B %v", groupBy, a, b)
		}
		encodedA, _ := api.MarshalJSON(a)
		fmt.Fprintf(&transcript, "group_by %v\n  replica A computed %s, replica B computed %s\n  answer %s\n  proto.Equal(A, B) with computed_at cleared: %t\n\n",
			a.GetGroupBy(), computedA.Format(time.RFC3339Nano), computedB.Format(time.RFC3339Nano), encodedA, equal)
		t.Logf("EVIDENCE flake_index_two_replicas group_by=%v groups=%d runs_in_window=%d proto_equal=%t",
			a.GetGroupBy(), len(a.GetGroups()), a.GetRunsInWindow(), equal)
	}
	proof.Write(t, "stats-flakes", "two-replicas.txt", []byte(transcript.String()))
}

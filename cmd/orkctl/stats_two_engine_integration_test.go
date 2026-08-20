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
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TwoReplicasReportTheSameStatsForTwoNamespaces is M10.6's
// central claim, measured on two real `ork` processes.
//
// Every run below is submitted to replica A: replica B accepts no submission
// and starts no run. What replica B holds in its own RunStore is deliberately
// NOT controlled — both replicas poll the projection channel, and a projection
// activity applies the run it carries to whichever replica happened to execute
// it (EventActivities.PublishEvent). That is the point rather than a gap:
// neither replica's memory is consulted, so what is in it cannot matter, and
// both answer with the SAME numbers because both derive them from the two
// shared records (AGENTS.md §1.7). The stronger claim — that a run present
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
// Isolation (AGENTS.md §6): a unique deployment partition per test run. Note
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

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	channel := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, orkerPath, address, partition, channel)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, partition, channel)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	statsA := statsClientFor(t, replicaA)
	statsB := statsClientFor(t, replicaB)

	// ---- two namespaces, one orkflow name in each -------------------------
	stamp := time.Now().UnixNano()
	alpha := fmt.Sprintf("m106-alpha-%d-%d", os.Getpid(), stamp)
	beta := fmt.Sprintf("m106-beta-%d-%d", os.Getpid(), stamp)
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

	// ---- acceptance 5: exact numbers, and A excludes B --------------------
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

	// ---- acceptance 4: the two replicas agree, number for number ----------
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
// SHARED by both replicas — AGENTS.md §6's deliberate exception, because two
// replicas answering for one set of runs is the condition under test and
// splitting them would make the test fail for a reason unrelated to the
// property.
func statsPartition() string {
	return fmt.Sprintf("ork-m10-6-stats-%d-%d",
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

// assertSameStats is acceptance 4: two replicas, one set of numbers.
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
		fmt.Fprintf(&out, "| pool %s orkers=%d default=%t plugins=%s ",
			pool.GetName(), pool.GetLiveOrkerCount(), pool.GetIsDefault(),
			strings.Join(pool.GetPlugins(), "+"))
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
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M10.6 stats probe.
tasks:
  - id: work
    plugin: shell
    timeout_seconds: 30
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: %s
`, name, command)
}

// statsSlowOrkflowYAML runs long enough for both replicas to be asked while it
// is still going.
func statsSlowOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M10.6 stats probe, still running.
tasks:
  - id: crawl
    plugin: shell
    timeout_seconds: 240
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: sleep 180
`, name)
}

// writeStatsTranscript records the proof artifact when reproduce.sh asks.
func writeStatsTranscript(
	t *testing.T,
	partition, alpha, beta, slow string,
	since, until time.Time,
	alphaA, alphaB, betaA, betaB *orkstackv1.GetStatsResponse,
) {
	t.Helper()
	path := os.Getenv("ORK_PROOF_STATS_FILE")
	if path == "" {
		return
	}
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
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		t.Errorf("write stats transcript: %v", err)
	}
}

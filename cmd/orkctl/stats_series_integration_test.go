//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TwoReplicasReportTheSameStatsSeries is the series' claim
// measured on two real `ork` processes over the real archive and cluster.
//
// Leg one is the calendar. Finished runs are written straight into the
// archive the replicas share, at instants on both sides of the 2025 US
// fall-back in Los Angeles — the two 01:30s of Nov 2 among them — and both
// replicas are asked for the seven local days ending at a pinned midnight.
// Every bucket is asserted exactly, Nov 2 is asserted to last 25 hours, and
// the buckets are asserted to sum to the window, from one scan of real index
// keys.
//
// Leg two is the live half. Runs are submitted to replica A — one still
// going — and both replicas are asked for the last two local days, with the
// window's end pinned so the two answer one question. The zone is chosen
// with its midnight more than an hour away, so the runs submitted now are in
// today's bucket however long the test takes.
//
// Replica B starts nothing; both must answer number for number alike
// (AGENTS.md §7).
func TestIntegration_TwoReplicasReportTheSameStatsSeries(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, artifactCfg)

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

	stamp := time.Now().UnixNano()
	calendar := fmt.Sprintf("calendar-%d-%d", os.Getpid(), stamp)
	live := fmt.Sprintf("live-%d-%d", os.Getpid(), stamp)
	namespaces := orkstackv1connect.NewNamespaceServiceClient(h2cClient(t), "http://"+replicaA.addr)
	for _, name := range []string{calendar, live} {
		if _, err := namespaces.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
			Namespace: &orkstackv1.Namespace{Name: name},
		})); err != nil {
			t.Fatalf("CreateNamespace(%s): %v", name, err)
		}
	}

	read := func(stats orkstackv1connect.StatsServiceClient, req *orkstackv1.GetStatsRequest) *orkstackv1.GetStatsResponse {
		t.Helper()
		got, err := stats.GetStats(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("GetStats(%v): %v", req, err)
		}
		return got.Msg
	}

	// ---- leg one: the calendar, from real index keys -------------------------
	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("load America/Los_Angeles: %v", err)
	}
	store, err := artifacts.NewRunArchive(artifactCfg)
	if err != nil {
		t.Fatalf("open the shared run archive: %v", err)
	}
	writer := engine.NewRunArchiver(store)
	seeded := []struct {
		at     string
		status orkstackv1.Status
	}{
		{"2025-10-28T23:59:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // before the window
		{"2025-10-29T00:30:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED},
		{"2025-11-02T00:30:00-07:00", orkstackv1.Status_STATUS_FAILED},
		{"2025-11-02T01:30:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // the first 01:30
		{"2025-11-02T01:30:00-08:00", orkstackv1.Status_STATUS_CANCELED},  // the second
		{"2025-11-02T23:30:00-08:00", orkstackv1.Status_STATUS_SUCCEEDED},
		{"2025-11-03T00:00:00-08:00", orkstackv1.Status_STATUS_FAILED},
		{"2025-11-05T00:00:00-08:00", orkstackv1.Status_STATUS_SUCCEEDED}, // the window's end
	}
	for i, seed := range seeded {
		created, err := time.Parse(time.RFC3339, seed.at)
		if err != nil {
			t.Fatalf("parse %s: %v", seed.at, err)
		}
		if err := writer.Archive(ctx, &orkstackv1.Run{
			Id:          fmt.Sprintf("ork-series-%d-%d", stamp, i),
			Namespace:   calendar,
			OrkflowName: "ledger",
			Status:      seed.status,
			CreatedAt:   timestamppb.New(created),
			// Finished now, whenever it was created: retention counts from a
			// run's end, so a run created in 2025 is not retired mid-test.
			FinishedAt: timestamppb.New(time.Now()),
		}, nil); err != nil {
			t.Fatalf("archive the run created %s: %v", seed.at, err)
		}
	}
	awaitArchived(ctx, t, artifactCfg, calendar, len(seeded))

	windowEnd := time.Date(2025, 11, 5, 0, 0, 0, 0, losAngeles)
	calendarReq := &orkstackv1.GetStatsRequest{
		Namespace:   calendar,
		BucketWidth: orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY,
		Timezone:    "America/Los_Angeles",
		BucketCount: 7,
		Until:       timestamppb.New(windowEnd),
	}
	calendarA, calendarB := read(statsA, calendarReq), read(statsB, calendarReq)
	type bucket struct {
		day                         int
		succeeded, failed, canceled int64
	}
	want := []bucket{{29, 1, 0, 0}, {30, 0, 0, 0}, {31, 0, 0, 0}, {1, 0, 0, 0}, {2, 2, 1, 1}, {3, 0, 1, 0}, {4, 0, 0, 0}}
	if got := len(calendarA.GetBuckets()); got != len(want) {
		t.Fatalf("calendar: %d buckets, want %d: %v", got, len(want), calendarA.GetBuckets())
	}
	for i, w := range want {
		b := calendarA.GetBuckets()[i]
		month := time.November
		if w.day > 4 {
			month = time.October
		}
		if since := b.GetSince().AsTime(); !since.Equal(time.Date(2025, month, w.day, 0, 0, 0, 0, losAngeles)) {
			t.Errorf("calendar bucket %d starts %s, want local %s %d midnight",
				i, since.In(losAngeles), month, w.day)
		}
		counts := b.GetRunsByStatus()
		if counts["succeeded"] != w.succeeded || counts["failed"] != w.failed ||
			counts["canceled"] != w.canceled || counts["running"] != 0 || b.GetPartial() {
			t.Errorf("calendar bucket %s %d = %v partial=%t, want succeeded %d failed %d canceled %d, whole",
				month, w.day, counts, b.GetPartial(), w.succeeded, w.failed, w.canceled)
		}
	}
	if nov2 := calendarA.GetBuckets()[4]; nov2.GetUntil().AsTime().Sub(nov2.GetSince().AsTime()) != 25*time.Hour {
		t.Errorf("Nov 2 lasts %s, want 25h: the fall-back's repeated hour is its own",
			nov2.GetUntil().AsTime().Sub(nov2.GetSince().AsTime()))
	}
	assertSeriesSums(t, "calendar", calendarA, 0)
	if c := calendarA.GetRunsByStatus(); c["succeeded"] != 3 || c["failed"] != 2 || c["canceled"] != 1 {
		t.Errorf("calendar window = %v, want 3 succeeded, 2 failed, 1 canceled", c)
	}
	assertSameSeries(t, "calendar", calendarA, calendarB)

	// ---- leg two: the live half -----------------------------------------------
	zone := zoneAwayFromMidnight(t, time.Now())
	apply := func(yaml string) {
		t.Helper()
		if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: yaml, Namespace: live,
		})); err != nil {
			t.Fatalf("ApplyOrkflow in %s: %v", live, err)
		}
	}
	apply(statsOrkflowYAML("shipit", "echo shipped"))
	apply(statsOrkflowYAML("breakit", "exit 3"))
	apply(statsSlowOrkflowYAML("crawlit"))
	submit := func(orkflow string) string {
		t.Helper()
		submitted, err := replicaA.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: orkflow, Namespace: live,
		}))
		if err != nil {
			t.Fatalf("SubmitRun %s: %v", orkflow, err)
		}
		return submitted.Msg.GetRun().GetId()
	}
	submit("shipit")
	submit("shipit")
	submit("breakit")
	slow := submit("crawlit")
	awaitArchived(ctx, t, artifactCfg, live, 3)
	if status := executionStatus(ctx, t, temporalClient, slow); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the slow run reached %s before the readings", status)
	}
	liveReq := &orkstackv1.GetStatsRequest{
		Namespace:   live,
		BucketWidth: orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY,
		Timezone:    zone.String(),
		BucketCount: 2,
		Until:       timestamppb.New(time.Now().Add(time.Minute)),
	}
	liveA, liveB := read(statsA, liveReq), read(statsB, liveReq)
	if status := executionStatus(ctx, t, temporalClient, slow); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the slow run closed (%s) during the readings", status)
	}
	if got := len(liveA.GetBuckets()); got != 2 {
		t.Fatalf("live: %d buckets, want 2", got)
	}
	yesterday, today := liveA.GetBuckets()[0], liveA.GetBuckets()[1]
	if c := today.GetRunsByStatus(); c["running"] != 1 || c["succeeded"] != 2 || c["failed"] != 1 ||
		c["canceled"] != 0 || !today.GetPartial() {
		t.Errorf("live today (%s) = %v partial=%t, want 1 running, 2 succeeded, 1 failed, partial",
			zone, c, today.GetPartial())
	}
	if c := yesterday.GetRunsByStatus(); c["running"]+c["succeeded"]+c["failed"]+c["canceled"] != 0 || yesterday.GetPartial() {
		t.Errorf("live yesterday = %v partial=%t, want empty and whole", c, yesterday.GetPartial())
	}
	assertSeriesSums(t, "live", liveA, 0)
	assertSameSeries(t, "live", liveA, liveB)

	writeSeriesTranscript(t, partition, zone, calendarReq, liveReq, calendarA, calendarB, liveA, liveB)

	if _, err := replicaB.runs.CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{
		RunId: slow, Namespace: live, Reason: "stats series proof finished",
	})); err != nil {
		t.Logf("cancel the slow run: %v", err)
	}
}

// zoneAwayFromMidnight is a zone whose local time is at least an hour from
// midnight on either side, so runs submitted now stay in today's bucket for
// the length of the test.
func zoneAwayFromMidnight(t *testing.T, now time.Time) *time.Location {
	t.Helper()
	for _, name := range []string{"UTC", "Asia/Tokyo", "America/Los_Angeles", "Asia/Kolkata", "Europe/Lisbon"} {
		zone, err := time.LoadLocation(name)
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if hour := now.In(zone).Hour(); hour >= 1 && hour < 23 {
			return zone
		}
	}
	t.Fatalf("no candidate zone is an hour away from midnight at %s", now)
	return nil
}

// assertSeriesSums: the buckets' finished counts sum exactly to the window's,
// and running falls short by exactly the live runs created before the window.
func assertSeriesSums(t *testing.T, label string, got *orkstackv1.GetStatsResponse, liveBefore int64) {
	t.Helper()
	sum := map[string]int64{}
	for _, b := range got.GetBuckets() {
		for status, count := range b.GetRunsByStatus() {
			sum[status] += count
		}
	}
	for status, count := range got.GetRunsByStatus() {
		short := count - sum[status]
		if status == "running" && short != liveBefore || status != "running" && short != 0 {
			t.Errorf("%s: the buckets count %d %s and the window %d", label, sum[status], status, count)
		}
	}
	if first := got.GetBuckets()[0].GetSince().AsTime(); !first.Equal(got.GetWindowSince().AsTime()) {
		t.Errorf("%s: the series starts %s and the window %s", label, first, got.GetWindowSince().AsTime())
	}
	if last := got.GetBuckets()[len(got.GetBuckets())-1].GetUntil().AsTime(); !last.Equal(got.GetWindowUntil().AsTime()) {
		t.Errorf("%s: the series ends %s and the window %s", label, last, got.GetWindowUntil().AsTime())
	}
}

// seriesDigest is statsDigest with the series appended, bucket by bucket.
//
// duration_coverage_since is left out, beside computed_at: for a window that
// ended before the cluster's retention edge it IS that edge, now minus the
// retention, so it records when each replica answered, as computed_at does,
// and two answers a moment apart differ there by design. Every count, bound
// and bucket is compared.
func seriesDigest(stats *orkstackv1.GetStatsResponse) string {
	stats = proto.Clone(stats).(*orkstackv1.GetStatsResponse)
	stats.DurationCoverageSince = nil
	var out strings.Builder
	out.WriteString(statsDigest(stats))
	fmt.Fprintf(&out, " | series %s %s", stats.GetBucketWidth(), stats.GetTimezone())
	for _, b := range stats.GetBuckets() {
		c := b.GetRunsByStatus()
		fmt.Fprintf(&out, " | %s..%s r%d s%d f%d c%d partial=%t",
			b.GetSince().AsTime().Format(time.RFC3339), b.GetUntil().AsTime().Format(time.RFC3339),
			c["running"], c["succeeded"], c["failed"], c["canceled"], b.GetPartial())
	}
	return out.String()
}

func assertSameSeries(t *testing.T, label string, a, b *orkstackv1.GetStatsResponse) {
	t.Helper()
	if digestA, digestB := seriesDigest(a), seriesDigest(b); digestA != digestB {
		t.Errorf("the two replicas disagree about %s:\n replica A %s\n replica B %s", label, digestA, digestB)
	}
}

// writeSeriesTranscript records both legs under ORK_PROOF_DIR when it is set.
func writeSeriesTranscript(
	t *testing.T,
	partition string,
	zone *time.Location,
	calendarReq, liveReq *orkstackv1.GetStatsRequest,
	calendarA, calendarB, liveA, liveB *orkstackv1.GetStatsResponse,
) {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "# stats series on two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "partition %s\n\n", partition)
	for _, leg := range []struct {
		name string
		req  *orkstackv1.GetStatsRequest
		a, b *orkstackv1.GetStatsResponse
	}{
		{"calendar (archived runs, America/Los_Angeles, the 2025 fall-back)", calendarReq, calendarA, calendarB},
		{"live (submitted runs, " + zone.String() + ")", liveReq, liveA, liveB},
	} {
		fmt.Fprintf(&out, "%s\n  request    %v\n", leg.name, leg.req)
		fmt.Fprintf(&out, "  replica A  %s\n  replica B  %s\n  identical  %t\n\n",
			seriesDigest(leg.a), seriesDigest(leg.b), seriesDigest(leg.a) == seriesDigest(leg.b))
	}
	proof.Write(t, "orkctl", "stats-series-two-replicas.txt", []byte(out.String()))
}

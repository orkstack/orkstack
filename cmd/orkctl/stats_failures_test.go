package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// seedFailures wires a fixture over a real archive and attempt store holding
// the research narrative in miniature: a registry outage on one pool (one of
// whose attempt records was reclaimed), a bad promotion, and a run that never
// started — beside a success nobody should count.
func seedFailures(t *testing.T) *fixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	attempts := artifacts.NewMemoryAttemptStore()

	archive := func(id, orkflow string, status orkstackv1.Status, at time.Time, task *orkstackv1.TaskRun, runErr *orkstackv1.Error) {
		t.Helper()
		run := &orkstackv1.Run{
			Id: id, Namespace: "default", OrkflowName: orkflow, Status: status,
			CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
			Error: runErr,
		}
		if task != nil {
			run.TaskRuns = []*orkstackv1.TaskRun{task}
		}
		if err := archiver.Archive(context.Background(), run, nil); err != nil {
			t.Fatalf("archive %s: %v", id, err)
		}
	}
	failed := func(task, code string, category orkstackv1.FailureCategory, pool, orker string, at time.Time) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{
			TaskId: task, Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1, Pool: pool, OrkerId: orker,
			FinishedAt: timestamppb.New(at.Add(time.Minute)),
			Error:      &orkstackv1.Error{Code: code, Category: category},
		}
	}
	record := func(runID, task, image, phase string) {
		t.Helper()
		if err := attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: task, Attempt: 1,
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, Phase: phase,
			Image: &orkstackv1.ImageRef{Pinned: image},
		}); err != nil {
			t.Fatalf("put attempt: %v", err)
		}
	}

	const ingestImage = "ghcr.io/acme/ingest@sha256:4f0c11"
	const reconcileImage = "ghcr.io/acme/reconcile@sha256:ab12ff"
	infra := orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	activity := orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	for i, id := range []string{"ork-ingest-1", "ork-ingest-2", "ork-ingest-3"} {
		at := fixedNow.Add(-time.Duration(30+i*10) * time.Minute)
		archive(id, "payments", orkstackv1.Status_STATUS_FAILED, at,
			failed("ingest", "IMAGE_PULL_FAILED", infra, "gpu", "orker-g1", at), &orkstackv1.Error{Code: "RUN_FAILED"})
		if id != "ork-ingest-3" { // ork-ingest-3's record was reclaimed
			record(id, "ingest", ingestImage, "image")
		}
	}
	for i, id := range []string{"ork-reconcile-1", "ork-reconcile-2"} {
		at := fixedNow.Add(-time.Duration(2+i) * time.Hour)
		archive(id, "reconcile", orkstackv1.Status_STATUS_FAILED, at,
			failed("reconcile", "CONTAINER_EXIT_NONZERO", activity, "main", "orker-m1", at), &orkstackv1.Error{Code: "RUN_FAILED"})
		record(id, "reconcile", reconcileImage, "exit")
	}
	archive("ork-nightly-1", "nightly", orkstackv1.Status_STATUS_FAILED, fixedNow.Add(-5*time.Hour), nil,
		&orkstackv1.Error{Code: "RUN_START_FAILED"})
	archive("ork-ok-1", "payments", orkstackv1.Status_STATUS_SUCCEEDED, fixedNow.Add(-time.Hour), nil, nil)

	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = statsReader{executions: map[string]engine.Executions{}}
		cfg.Archive = archiver
		cfg.Attempts = attempts
	})
}

func TestStatsFailures_GoldenHumanAndJSON(t *testing.T) {
	f := seedFailures(t)

	human := f.run("stats", "failures").requireExit(t, exitOK)
	requireGolden(t, "stats-failures.golden", human.stdout)

	machine := f.run("--json", "stats", "failures").requireExit(t, exitOK)
	requireGolden(t, "stats-failures.json.golden", machine.stdout)

	image := f.run("stats", "failures", "--group-by", "orkflow,task,code,category,image,pool,phase").requireExit(t, exitOK)
	requireGolden(t, "stats-failures-image.golden", image.stdout)

	empty := f.run("stats", "failures", "--since", "10m").requireExit(t, exitOK)
	requireGolden(t, "stats-failures-empty.golden", empty.stdout)

	category := f.run("stats", "failures", "--category", "activity_failed").requireExit(t, exitOK)
	requireGolden(t, "stats-failures-category.golden", category.stdout)
}

// capturingStats is a StatsService that records the one request it was sent.
type capturingStats struct {
	orkstackv1connect.UnimplementedStatsServiceHandler
	mu   sync.Mutex
	got  *orkstackv1.GetFailureBreakdownRequest
	resp *orkstackv1.GetFailureBreakdownResponse
}

func (c *capturingStats) GetFailureBreakdown(
	_ context.Context,
	req *connect.Request[orkstackv1.GetFailureBreakdownRequest],
) (*connect.Response[orkstackv1.GetFailureBreakdownResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = proto.Clone(req.Msg).(*orkstackv1.GetFailureBreakdownRequest)
	resp := c.resp
	if resp == nil {
		resp = &orkstackv1.GetFailureBreakdownResponse{}
	}
	return connect.NewResponse(resp), nil
}

// serveStats serves one StatsService handler and returns its address.
func serveStats(t *testing.T, handler orkstackv1connect.StatsServiceHandler) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewStatsServiceHandler(handler))
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	return "http://" + listener.Addr().String()
}

func runAgainst(t *testing.T, server string, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	code := Execute(append(append([]string(nil), args...), "--server", server), &stdout, &stderr)
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func TestStatsFailures_GroupByCategoryLimitAndUntilTravelOnTheRequest(t *testing.T) {
	capture := &capturingStats{}
	server := serveStats(t, capture)

	runAgainst(t, server, "-n", "payments", "stats", "failures",
		"--since", "6h", "--until", "2026-09-22T10:51:49Z", "--orkflow", "reconcile",
		"--group-by", "Image, task,PHASE", "--category", "FAILURE_CATEGORY_TIMED_OUT", "--limit", "7",
	).requireExit(t, exitOK)
	got := capture.got
	want := &orkstackv1.GetFailureBreakdownRequest{
		Namespace:   "payments",
		Lookback:    got.GetLookback(),
		Until:       timestamppb.New(time.Date(2026, 9, 22, 10, 51, 49, 0, time.UTC)),
		OrkflowName: "reconcile",
		GroupBy: []orkstackv1.FailureGroupBy{
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE,
		},
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
		Limit:    7,
	}
	if got.GetLookback().AsDuration() != 6*time.Hour || got.GetSince() != nil {
		t.Errorf("--since 6h travelled as since=%v lookback=%v, want lookback 6h only",
			got.GetSince(), got.GetLookback())
	}
	if !proto.Equal(got, want) {
		t.Errorf("wire request = %v\nwant %v", got, want)
	}

	// An instant --since travels as since, and nothing unasked travels at all.
	runAgainst(t, server, "stats", "failures", "--since", "2026-09-22T04:00:00Z").requireExit(t, exitOK)
	if got := capture.got; got.GetSince() == nil || got.GetLookback() != nil || got.GetUntil() != nil ||
		len(got.GetGroupBy()) != 0 || got.GetCategory() != 0 || got.GetLimit() != 0 || got.GetNamespace() != "" {
		t.Errorf("an instant --since sent %v", got)
	}

	// --until must be an instant: a duration there is a question nobody asked.
	bad := runAgainst(t, server, "stats", "failures", "--until", "6h").requireExit(t, exitError)
	if !strings.Contains(bad.stderr, "--until") {
		t.Errorf("a duration --until was not refused by name: %q", bad.stderr)
	}
}

func TestStatsFailures_RendersUnavailableForReclaimedEvidenceAndDashForNone(t *testing.T) {
	f := seedFailures(t)
	out := f.run("stats", "failures", "--group-by", "task,code,image,phase").requireExit(t, exitOK).stdout
	var ingestMissing, nightly string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		switch {
		case fields[0] == "ingest" && fields[2] == unavailable:
			ingestMissing = line
		case fields[0] == dash && fields[1] == "RUN_START_FAILED":
			nightly = line
		}
	}
	if ingestMissing == "" || strings.Count(ingestMissing, unavailable) != 2 {
		t.Errorf("the reclaimed ingest run's image and phase do not both read %q:\n%s", unavailable, out)
	}
	// The run-level failure has no task, image or phase to be missing: dash.
	if nightly == "" || strings.Contains(nightly, unavailable) || strings.Count(nightly, " - ") < 2 {
		t.Errorf("the run-level failure did not render its empty dimensions as %q:\n%s", dash, out)
	}
	if !strings.Contains(out, "image and phase unavailable for 1 group: attempt records for 1 run were reclaimed or never written") {
		t.Errorf("the footer does not count the reclaimed evidence:\n%s", out)
	}
}

func TestStatsFailures_RejectsAnUnknownOrRepeatedGroupBy(t *testing.T) {
	f := seedFailures(t)
	for _, raw := range []string{"orkflow,colour", "task,Task", ","} {
		got := f.run("stats", "failures", "--group-by", raw).requireExit(t, exitError)
		for _, spelling := range []string{"orkflow", "task", "code", "category", "image", "pool", "phase", "orker"} {
			if !strings.Contains(got.stderr, spelling) {
				t.Errorf("--group-by %q: error %q does not name %q", raw, got.stderr, spelling)
			}
		}
	}
}

func TestStatsFailures_RejectsAnUnknownCategoryAndAcceptsBothSpellings(t *testing.T) {
	for _, testCase := range []struct {
		raw  string
		want orkstackv1.FailureCategory
	}{
		{"activity_failed", orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED},
		{"INFRASTRUCTURE_ERROR", orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{"failure_category_contract_violation", orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
		{"Timed_Out", orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT},
		{"cancelled", orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED},
	} {
		got, err := parseFailureCategory(testCase.raw)
		if err != nil || got != testCase.want {
			t.Errorf("parseFailureCategory(%q) = %v, %v; want %v", testCase.raw, got, err, testCase.want)
		}
	}
	f := seedFailures(t)
	for _, raw := range []string{"canceled", "unspecified", "flaky"} {
		got := f.run("stats", "failures", "--category", raw).requireExit(t, exitError)
		for _, spelling := range []string{"activity_failed", "infrastructure_error", "contract_violation", "timed_out", "cancelled"} {
			if !strings.Contains(got.stderr, spelling) {
				t.Errorf("--category %q: error %q does not name %q", raw, got.stderr, spelling)
			}
		}
	}
}

func TestStatsFailures_EveryGroupByEnumValueHasAFlagSpelling(t *testing.T) {
	values := orkstackv1.FailureGroupBy(0).Descriptor().Values()
	seen := map[string]bool{}
	for i := 0; i < values.Len(); i++ {
		dim := orkstackv1.FailureGroupBy(values.Get(i).Number())
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED {
			continue
		}
		spelling := groupBySpelling(dim)
		if spelling == "" || seen[spelling] {
			t.Errorf("%s has no distinct flag spelling (%q)", dim, spelling)
			continue
		}
		seen[spelling] = true
		parsed, err := parseGroupBy(spelling)
		if err != nil || len(parsed) != 1 || parsed[0] != dim {
			t.Errorf("--group-by %q parses to %v, %v; want %s", spelling, parsed, err, dim)
		}
	}
	if len(seen) != 8 {
		t.Errorf("%d dimensions have spellings, want 8", len(seen))
	}
}

func TestStatsFailures_FooterSaysWhenTheListIsShort(t *testing.T) {
	f := seedFailures(t)
	out := f.run("stats", "failures", "--limit", "1").requireExit(t, exitOK).stdout
	if !strings.Contains(out, "showing 1 of 3 groups (raise --limit)") {
		t.Errorf("a truncated list does not say so:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("scanned 6 of 6 failed runs in the window; 6 grouped into 3 groups")) {
		t.Errorf("the totals are not over every group:\n%s", out)
	}
}

package main

import (
	"context"
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

// seedFlakes wires a fixture over a real archive and attempt store holding
// the research narrative in miniature: a task that retried on two of three
// SUCCEEDED runs because one Orker could not pull its image, a task whose
// first attempt left no record, a clean task, and a task whose records were
// all reclaimed.
func seedFlakes(t *testing.T, withAttempts bool) *fixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	attempts := artifacts.NewMemoryAttemptStore()

	const image = "ghcr.io/acme/reconcile@sha256:ab12ff"
	infra := orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	activity := orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	record := func(runID, task string, attempt int32, outcome orkstackv1.AttemptOutcome, orker, phase, code string,
		category orkstackv1.FailureCategory,
	) {
		t.Helper()
		rec := &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: task, Attempt: attempt,
			Outcome: outcome, OrkerId: orker, Phase: phase, Image: &orkstackv1.ImageRef{Pinned: image},
		}
		if code != "" {
			rec.Error = &orkstackv1.Error{Code: code, Category: category}
		}
		if err := attempts.PutAttempt(context.Background(), rec); err != nil {
			t.Fatalf("put attempt: %v", err)
		}
	}
	failed := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	succeeded := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
	archive := func(id, orkflow string, status orkstackv1.Status, minutesAgo int, tasks ...*orkstackv1.TaskRun) {
		t.Helper()
		at := fixedNow.Add(-time.Duration(minutesAgo) * time.Minute)
		run := &orkstackv1.Run{
			Id: id, Namespace: "default", OrkflowName: orkflow, Status: status,
			CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)), TaskRuns: tasks,
		}
		if err := archiver.Archive(context.Background(), run, nil); err != nil {
			t.Fatalf("archive %s: %v", id, err)
		}
	}
	task := func(id, pool string, attempt int32) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: id, Pool: pool, Attempt: attempt, Status: orkstackv1.Status_STATUS_SUCCEEDED}
	}

	// reconcile: fetch-rates retried on two of three runs, always a pull on
	// orker-g1, succeeding on orker-g2.
	archive("ork-reconcile-1", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, 30, task("fetch-rates", "gpu", 3))
	record("ork-reconcile-1", "fetch-rates", 1, failed, "orker-g1", "image", "IMAGE_PULL_FAILED", infra)
	record("ork-reconcile-1", "fetch-rates", 2, failed, "orker-g1", "image", "IMAGE_PULL_FAILED", infra)
	record("ork-reconcile-1", "fetch-rates", 3, succeeded, "orker-g2", "commit", "", 0)
	archive("ork-reconcile-2", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, 90, task("fetch-rates", "gpu", 2))
	record("ork-reconcile-2", "fetch-rates", 1, failed, "orker-g1", "image", "IMAGE_PULL_FAILED", infra)
	record("ork-reconcile-2", "fetch-rates", 2, succeeded, "orker-g2", "commit", "", 0)
	archive("ork-reconcile-3", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, 150, task("fetch-rates", "gpu", 1))
	record("ork-reconcile-3", "fetch-rates", 1, succeeded, "orker-g2", "commit", "", 0)

	// ingest: normalize retried once, its first attempt unreported — and the
	// run FAILED on a later task, which still counts.
	archive("ork-ingest-1", "ingest", orkstackv1.Status_STATUS_FAILED, 60, task("normalize", "main", 2),
		&orkstackv1.TaskRun{TaskId: "load", Status: orkstackv1.Status_STATUS_SKIPPED})
	record("ork-ingest-1", "normalize", 2, succeeded, "orker-m1", "commit", "", 0)
	archive("ork-ingest-2", "ingest", orkstackv1.Status_STATUS_CANCELED, 120, task("normalize", "main", 2))
	record("ork-ingest-2", "normalize", 1, failed, "orker-m1", "exit", "CONTAINER_EXIT_NONZERO", activity)
	record("ork-ingest-2", "normalize", 2, succeeded, "orker-m1", "commit", "", 0)

	// settle: clean.
	archive("ork-settle-1", "settle", orkstackv1.Status_STATUS_SUCCEEDED, 45, task("post", "main", 1))
	record("ork-settle-1", "post", 1, succeeded, "orker-m1", "commit", "", 0)

	// nightly: warm-cache's records were reclaimed.
	archive("ork-nightly-1", "nightly", orkstackv1.Status_STATUS_SUCCEEDED, 300, task("warm-cache", "main", 1))

	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = statsReader{executions: map[string]engine.Executions{}}
		cfg.Archive = archiver
		if withAttempts {
			cfg.Attempts = attempts
		} else {
			cfg.Attempts = nil
		}
	})
}

func TestStatsFlakes_GoldenHumanAndJSON(t *testing.T) {
	f := seedFlakes(t, true)

	human := f.run("stats", "flakes").requireExit(t, exitOK)
	requireGolden(t, "stats-flakes.golden", human.stdout)

	machine := f.run("--json", "stats", "flakes").requireExit(t, exitOK)
	requireGolden(t, "stats-flakes.json.golden", machine.stdout)

	orker := f.run("stats", "flakes", "--group-by", "task,orker").requireExit(t, exitOK)
	requireGolden(t, "stats-flakes-orker.golden", orker.stdout)

	missing := f.run("stats", "flakes", "--orkflow", "nightly").requireExit(t, exitOK)
	requireGolden(t, "stats-flakes-missing.golden", missing.stdout)

	empty := f.run("stats", "flakes", "--since", "10m").requireExit(t, exitOK)
	requireGolden(t, "stats-flakes-empty.golden", empty.stdout)
}

// capturingFlakes is a StatsService that records the one flake request it
// was sent.
type capturingFlakes struct {
	orkstackv1connect.UnimplementedStatsServiceHandler
	mu  sync.Mutex
	got *orkstackv1.GetFlakeIndexRequest
}

func (c *capturingFlakes) GetFlakeIndex(
	_ context.Context,
	req *connect.Request[orkstackv1.GetFlakeIndexRequest],
) (*connect.Response[orkstackv1.GetFlakeIndexResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = proto.Clone(req.Msg).(*orkstackv1.GetFlakeIndexRequest)
	return connect.NewResponse(&orkstackv1.GetFlakeIndexResponse{}), nil
}

func TestStatsFlakes_FlagsTravelOnTheRequest(t *testing.T) {
	capture := &capturingFlakes{}
	server := serveStats(t, capture)

	runAgainst(t, server, "-n", "payments", "stats", "flakes",
		"--since", "168h", "--until", "2026-09-24T10:51:49Z", "--orkflow", "reconcile",
		"--group-by", "Orker, task,POOL", "--limit", "7",
	).requireExit(t, exitOK)
	got := capture.got
	want := &orkstackv1.GetFlakeIndexRequest{
		Namespace:   "payments",
		Lookback:    got.GetLookback(),
		Until:       timestamppb.New(time.Date(2026, 9, 24, 10, 51, 49, 0, time.UTC)),
		OrkflowName: "reconcile",
		GroupBy: []orkstackv1.FailureGroupBy{
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL,
		},
		Limit: 7,
	}
	if got.GetLookback().AsDuration() != 168*time.Hour || got.GetSince() != nil {
		t.Errorf("--since 168h travelled as since=%v lookback=%v, want lookback 168h only",
			got.GetSince(), got.GetLookback())
	}
	if !proto.Equal(got, want) {
		t.Errorf("wire request = %v\nwant %v", got, want)
	}

	runAgainst(t, server, "stats", "flakes", "--since", "2026-09-22T04:00:00Z").requireExit(t, exitOK)
	if got := capture.got; got.GetSince() == nil || got.GetLookback() != nil || got.GetUntil() != nil ||
		len(got.GetGroupBy()) != 0 || got.GetLimit() != 0 || got.GetNamespace() != "" || got.GetOrkflowName() != "" {
		t.Errorf("an instant --since sent %v", got)
	}

	bad := runAgainst(t, server, "stats", "flakes", "--since", "7d").requireExit(t, exitError)
	if !strings.Contains(bad.stderr, "168h") {
		t.Errorf("--since 7d was not refused naming 168h: %q", bad.stderr)
	}
	badUntil := runAgainst(t, server, "stats", "flakes", "--until", "6h").requireExit(t, exitError)
	if !strings.Contains(badUntil.stderr, "--until") {
		t.Errorf("a duration --until was not refused by name: %q", badUntil.stderr)
	}
}

func TestStatsFlakes_RendersUnavailableForAnUndefinedRateAndDashForNoCause(t *testing.T) {
	f := seedFlakes(t, true)
	out := f.run("stats", "flakes").requireExit(t, exitOK).stdout
	var warm, post string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		switch fields[1] {
		case "warm-cache":
			warm = line
		case "post":
			post = line
		}
	}
	if !strings.Contains(warm, unavailable) || strings.Contains(warm, "0.0%") {
		t.Errorf("the unobservable task's rate is not %q:\n%s", unavailable, out)
	}
	if !strings.Contains(post, "0.0%") || strings.Contains(post, unavailable) || strings.Count(post, " - ") < 2 {
		t.Errorf("the clean task is not a defined 0.0%% with dashes for cause and last retried:\n%s", out)
	}
	if !strings.Contains(out, "1 runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean") {
		t.Errorf("the exclusion footer is missing:\n%s", out)
	}
}

func TestStatsFlakes_RejectsCodeAndCategoryAndAnUnknownOrRepeatedDimension(t *testing.T) {
	f := seedFlakes(t, true)
	for _, raw := range []string{"code", "task,Category"} {
		got := f.run("stats", "flakes", "--group-by", raw).requireExit(t, exitError)
		if !strings.Contains(got.stderr, "a flake group reports its dominant code and category; it cannot be keyed by them") {
			t.Errorf("--group-by %q: error %q does not explain the refusal", raw, got.stderr)
		}
	}
	six := []string{"orkflow", "task", "image", "pool", "phase", "orker"}
	for _, raw := range []string{"task,colour", "task,Task", ","} {
		got := f.run("stats", "flakes", "--group-by", raw).requireExit(t, exitError)
		for _, spelling := range six {
			if !strings.Contains(got.stderr, spelling) {
				t.Errorf("--group-by %q: error %q does not name %q", raw, got.stderr, spelling)
			}
		}
		if strings.Contains(got.stderr, "category") || strings.Contains(got.stderr, "code,") {
			t.Errorf("--group-by %q: error %q offers a refused dimension", raw, got.stderr)
		}
	}
}

func TestStatsFlakes_EverySpellingTheServerAcceptsHasAFlagSpelling(t *testing.T) {
	values := orkstackv1.FailureGroupBy(0).Descriptor().Values()
	accepted := 0
	for i := 0; i < values.Len(); i++ {
		dim := orkstackv1.FailureGroupBy(values.Get(i).Number())
		_, serverErr := engine.ResolveFlakeGroupBy([]orkstackv1.FailureGroupBy{dim})
		parsed, clientErr := parseFlakeGroupBy(groupBySpelling(dim))
		switch {
		case serverErr == nil:
			accepted++
			if clientErr != nil || len(parsed) != 1 || parsed[0] != dim {
				t.Errorf("the server accepts %s but --group-by %q parses to %v, %v", dim, groupBySpelling(dim), parsed, clientErr)
			}
		case dim != orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED && clientErr == nil:
			t.Errorf("the server refuses %s but --group-by %q is accepted", dim, groupBySpelling(dim))
		}
	}
	if accepted != 6 || len(flakeGroupBySpellings()) != 6 {
		t.Errorf("accepted=%d spellings=%v, want 6", accepted, flakeGroupBySpellings())
	}
}

func TestStatsFlakes_AnEngineWithoutAnAttemptStoreSaysSoInWords(t *testing.T) {
	f := seedFlakes(t, false)
	got := f.run("stats", "flakes").requireExit(t, exitError)
	if !strings.Contains(got.stderr, "attempt history is not available: this engine has no task attempt store configured") {
		t.Errorf("stderr = %q, want the attemptsError sentence", got.stderr)
	}
	if strings.Contains(got.stderr, "failed_precondition") {
		t.Errorf("stderr leaks the wire code: %q", got.stderr)
	}
}

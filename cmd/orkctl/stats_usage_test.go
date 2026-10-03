package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// brokenBodies is the run archiver with one run whose body cannot be read,
// so `--group-by namespace` has a namespace whose call fails.
type brokenBodies struct {
	*engine.RunArchiver
	broken string
}

func (b brokenBodies) LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	if runID == b.broken {
		return nil, errors.New("bucket unreachable")
	}
	return b.RunArchiver.LoadRun(ctx, runID)
}

// seedUsage wires a fixture over a real archive and attempt store:
//
//   - analytics: a backfill whose train task failed once on gpu and passed,
//     beside a prep task on main, and a report run — 755 reserved cpu-seconds
//     against an enforced budget of 500 per 720h;
//   - default: a load task with an unrecorded middle attempt and a record from
//     before reservations were recorded, and a run whose records are gone;
//   - sandbox: nothing; broken: a run whose body cannot be read.
func seedUsage(t *testing.T) *fixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	attempts := artifacts.NewMemoryAttemptStore()

	record := func(runID, task string, attempt int32, at time.Time, held time.Duration, cpu float64, memory int64) {
		t.Helper()
		rec := &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: task, Attempt: attempt,
			Outcome:   orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
			StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(held)),
		}
		if cpu > 0 {
			rec.Resources = &orkstackv1.ReservedResources{Cpu: cpu, MemoryBytes: memory}
		}
		if err := attempts.PutAttempt(context.Background(), rec); err != nil {
			t.Fatalf("put attempt: %v", err)
		}
	}
	archive := func(id, namespace, orkflow string, at time.Time, tasks ...*orkstackv1.TaskRun) {
		t.Helper()
		run := &orkstackv1.Run{
			Id: id, Namespace: namespace, OrkflowName: orkflow, Status: orkstackv1.Status_STATUS_SUCCEEDED,
			CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Hour)), TaskRuns: tasks,
		}
		if err := archiver.Archive(context.Background(), run, nil); err != nil {
			t.Fatalf("archive %s: %v", id, err)
		}
	}
	task := func(id, pool string, attempt int32) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: id, Pool: pool, Attempt: attempt, Status: orkstackv1.Status_STATUS_SUCCEEDED}
	}
	ago := func(minutes int) time.Time { return fixedNow.Add(-time.Duration(minutes) * time.Minute) }

	archive("ork-backfill-1", "analytics", "backfill", ago(30), task("train", "gpu", 2), task("prep", "main", 1))
	record("ork-backfill-1", "train", 1, ago(30), 60*time.Second, 4, 8<<30)
	record("ork-backfill-1", "train", 2, ago(28), 120*time.Second, 4, 8<<30)
	record("ork-backfill-1", "prep", 1, ago(30), 30*time.Second, 1, 1<<30)
	archive("ork-report-1", "analytics", "report", ago(48*60), task("render", "main", 1))
	record("ork-report-1", "render", 1, ago(48*60), 10*time.Second, 0.5, 512<<20)

	archive("ork-load-1", "default", "ingest", ago(60), task("load", "main", 3),
		&orkstackv1.TaskRun{TaskId: "notify", Status: orkstackv1.Status_STATUS_SKIPPED})
	record("ork-load-1", "load", 1, ago(60), 20*time.Second, 0, 0)
	record("ork-load-1", "load", 3, ago(58), 40*time.Second, 1, 1<<30)
	archive("ork-warm-1", "default", "nightly", ago(300), task("warm-cache", "main", 1))

	archive("ork-broken-1", "broken", "backfill", ago(30), task("train", "gpu", 1))

	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = statsReader{executions: map[string]engine.Executions{}}
		cfg.Archive = brokenBodies{RunArchiver: archiver, broken: "ork-broken-1"}
		cfg.Attempts = attempts
	})
	f.run("namespace", "create", "analytics",
		"--budget-cpu-seconds", "500", "--budget-period", "720h", "--budget-enforce").requireExit(t, exitOK)
	f.run("namespace", "create", "sandbox").requireExit(t, exitOK)
	f.run("namespace", "create", "broken").requireExit(t, exitOK)
	return f
}

func TestStatsUsage_GoldenHumanAndJSON(t *testing.T) {
	f := seedUsage(t)

	human := f.run("-n", "analytics", "stats", "usage").requireExit(t, exitOK)
	requireGolden(t, "stats-usage.golden", human.stdout)

	machine := f.run("--json", "-n", "analytics", "stats", "usage").requireExit(t, exitOK)
	requireGolden(t, "stats-usage.json.golden", machine.stdout)

	pool := f.run("-n", "analytics", "stats", "usage", "--since", "720h", "--group-by", "pool").requireExit(t, exitOK)
	requireGolden(t, "stats-usage-pool.golden", pool.stdout)

	missing := f.run("stats", "usage").requireExit(t, exitOK)
	requireGolden(t, "stats-usage-missing.golden", missing.stdout)

	empty := f.run("-n", "sandbox", "stats", "usage").requireExit(t, exitOK)
	requireGolden(t, "stats-usage-empty.golden", empty.stdout)
}

func TestStatsUsage_GroupByNamespaceFansOutAndMarksAFailedRow(t *testing.T) {
	f := seedUsage(t)
	result := f.run("stats", "usage", "--group-by", "namespace", "--since", "720h")
	if result.code == exitOK {
		t.Fatal("a fan-out with one failed namespace exited 0")
	}
	requireGolden(t, "stats-usage-namespaces.golden", result.stdout)
	result.requireStderrContains(t, "1 of 4 namespaces could not be measured")
	for _, want := range []string{
		"analytics", "151% of 500 cpu-s per 720h0m0s, enforced, reached",
		"error: unavailable: read the archived body of run \"ork-broken-1\"", "default", "sandbox", "unbounded",
	} {
		if !strings.Contains(result.stdout, want) {
			t.Errorf("fan-out output lacks %q:\n%s", want, result.stdout)
		}
	}
}

func TestStatsUsage_GroupByNamespaceRefusesJSONAndAnExplicitNamespace(t *testing.T) {
	f := seedUsage(t)
	before := len(f.requests())
	asJSON := f.run("--json", "stats", "usage", "--group-by", "namespace")
	if asJSON.code == exitOK {
		t.Fatal("--group-by namespace --json succeeded")
	}
	asJSON.requireStderrContains(t, "--group-by namespace renders one GetUsage call per namespace; for JSON, ask each with orkctl -n NS stats usage --json")
	named := f.run("-n", "analytics", "stats", "usage", "--group-by", "namespace")
	if named.code == exitOK {
		t.Fatal("--group-by namespace with --namespace succeeded")
	}
	named.requireStderrContains(t, "--group-by namespace reports every namespace; drop --namespace")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a refused fan-out reached the server: %v", after[before:])
	}
}

func TestStatsUsage_GroupByRefusesFailureDimensions(t *testing.T) {
	f := seedUsage(t)
	before := len(f.requests())
	result := f.run("stats", "usage", "--group-by", "code")
	if result.code == exitOK {
		t.Fatal("--group-by code succeeded")
	}
	result.requireStderrContains(t, "--group-by code: usage is grouped by orkflow, task or pool")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a refused grouping reached the server: %v", after[before:])
	}
}

func TestRunSubmit_RefusedByBudgetNamesTheUsageAndTheFix(t *testing.T) {
	f := seedUsage(t)
	f.run("apply", writeOrkflow(t, "backfill", "train"), "-n", "analytics").requireExit(t, exitOK)
	submit := f.run("run", "submit", "backfill", "-n", "analytics")
	if submit.code == exitOK {
		t.Fatal("a submit into a namespace past its enforced budget succeeded")
	}
	submit.requireStderrContains(t, "submit run: failed_precondition:",
		`namespace "analytics" has reserved 755 cpu-seconds over the last 720h0m0s, at or above its enforced budget of 500 (measured 2026-01-02T03:04:05Z)`,
		"`orkctl namespace update analytics --budget-cpu-seconds N`",
		"`orkctl namespace update analytics --budget-enforce=false`")
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submit started %v", started)
	}
	listed := f.run("run", "list", "-n", "analytics").requireExit(t, exitOK)
	if strings.Contains(listed.stdout, "run-001") {
		t.Fatalf("run list shows the refused submit:\n%s", listed.stdout)
	}
}

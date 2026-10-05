package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	cacheSourceRun = "ork-1758790021000000000-4081623940183294"
	cacheRun       = "ork-1758791221000000000-4081623940183301"
)

func cachedExamplePath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "cached-pipeline.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// planStarter records every plan a submission started a run with.
type planStarter struct {
	*stubStarter
	mu    sync.Mutex
	plans []engine.RunPlan
}

func (p *planStarter) StartRun(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	p.mu.Lock()
	p.plans = append(p.plans, plan)
	p.mu.Unlock()
	return p.stubStarter.StartRun(ctx, runID, wf, input, plan)
}

func (p *planStarter) last(t *testing.T) engine.RunPlan {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.plans) == 0 {
		t.Fatal("no run was started")
	}
	return p.plans[len(p.plans)-1]
}

func newPlanFixture(t *testing.T) (*fixture, *planStarter) {
	t.Helper()
	var starter *planStarter
	f := newFixtureWith(t, func(cfg *api.Config) {
		starter = &planStarter{stubStarter: cfg.Starter.(*stubStarter)}
		cfg.Starter = starter
	})
	return f, starter
}

func TestRunSubmit_NoCacheSetsTheField(t *testing.T) {
	f, starter := newPlanFixture(t)
	f.run("apply", cachedExamplePath(t)).requireExit(t, exitOK)
	f.run("run", "submit", "cached-pipeline").requireExit(t, exitOK)
	if plan := starter.last(t); plan.Cache == nil || plan.Cache.Refresh {
		t.Fatalf("plan.Cache = %+v, want a plan without refresh", plan.Cache)
	}
	f.run("run", "submit", "cached-pipeline", "--no-cache").requireExit(t, exitOK)
	if plan := starter.last(t); plan.Cache == nil || !plan.Cache.Refresh {
		t.Fatalf("plan.Cache = %+v, want refresh", plan.Cache)
	}
}

func TestRunAgain_NoCacheSetsTheField(t *testing.T) {
	f, starter := newPlanFixture(t)
	f.run("apply", cachedExamplePath(t)).requireExit(t, exitOK)
	f.run("run", "submit", "cached-pipeline").requireExit(t, exitOK)
	dry := f.run("run", "again", "run-001", "--no-cache", "--dry-run").requireExit(t, exitOK)
	var request map[string]any
	if err := json.Unmarshal([]byte(dry.stdout), &request); err != nil {
		t.Fatalf("decode: %v (%s)", err, dry.stdout)
	}
	if request["noCache"] != true {
		t.Fatalf("dry-run submission = %v, want noCache true", request)
	}
	f.run("run", "again", "run-001", "--no-cache").requireExit(t, exitOK)
	if plan := starter.last(t); plan.Cache == nil || !plan.Cache.Refresh {
		t.Fatalf("run again --no-cache plan = %+v", plan.Cache)
	}
}

// cachedRunFixture holds run 2 of the cached pipeline: featurize a hit from
// run 1, stamp uncached, train a miss whose memo was written.
func cachedRunFixture(t *testing.T) *fixture {
	t.Helper()
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(fixedNow.Add(time.Duration(seconds) * time.Second))
	}
	f := newFixture(t)
	f.run("namespace", "create", "ml").requireExit(t, exitOK)
	f.runs.Apply(&orkstackv1.Run{
		Id: cacheRun, Namespace: "ml", OrkflowName: "cached-pipeline", OrkflowVersion: "0.1.0",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: at(0), StartedAt: at(0), FinishedAt: at(9),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "featurize", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1",
				Attempt: 1, ScheduledAt: at(1), StartedAt: at(1), FinishedAt: at(2),
				Cache: &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: strings.Repeat("3f2a", 16),
					Source: &orkstackv1.CacheSource{RunId: cacheSourceRun, TaskId: "featurize",
						StartedAt:  timestamppb.New(fixedNow.Add(-27 * time.Minute)),
						FinishedAt: timestamppb.New(fixedNow.Add(-27*time.Minute + 8*time.Minute + 2*time.Second))}}},
			{TaskId: "stamp", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-2",
				Attempt: 1, ScheduledAt: at(1), StartedAt: at(2), FinishedAt: at(4)},
			{TaskId: "train", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1",
				Attempt: 1, ScheduledAt: at(4), StartedAt: at(5), FinishedAt: at(9),
				Cache: &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: strings.Repeat("9b", 32),
					MissReason: "MEMO_ABSENT", MissDetail: "no memo for key 9b9b…", MemoWritten: true}},
		},
	})
	return f
}

func TestRunGetCachedGolden(t *testing.T) {
	f := cachedRunFixture(t)
	got := f.run("-n", "ml", "run", "get", cacheRun).requireExit(t, exitOK)
	requireGolden(t, "run-get-cached.golden", got.stdout)
	got.requireContains(t,
		"featurize  SUCCEEDED (cached)",
		"featurize: cache hit — reused run "+cacheSourceRun+"'s result, committed",
		"(its attempt took 8m02s); no container ran. Open it with: orkctl -n ml run get "+cacheSourceRun,
		"train: cache miss (MEMO_ABSENT: no memo for this key); the container ran and its memo was written")
	if strings.Contains(got.stdout, "stamp:") {
		t.Fatalf("an uncached task got a cache line:\n%s", got.stdout)
	}
	machine := f.run("-n", "ml", "--json", "run", "get", cacheRun).requireExit(t, exitOK)
	machine.requireContains(t, `"outcome": "HIT"`, `"runId": "`+cacheSourceRun+`"`)
}

func TestCacheLine_BypassAndFailedWriteAndAdopted(t *testing.T) {
	bypass := cacheLine("", &orkstackv1.TaskRun{TaskId: "train", Cache: &orkstackv1.CacheResult{
		Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED, MemoWritten: true}})
	if bypass != "train: cache bypassed (submitted with --no-cache); the container ran and its memo was refreshed" {
		t.Fatalf("bypass = %q", bypass)
	}
	failed := cacheLine("", &orkstackv1.TaskRun{TaskId: "train", Cache: &orkstackv1.CacheResult{
		Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "MEMO_EXPIRED", MemoWriteError: "503"}})
	if !strings.HasSuffix(failed, "the container ran but its memo could not be written: 503") {
		t.Fatalf("failed write = %q", failed)
	}
	adopted := cacheLine("", &orkstackv1.TaskRun{TaskId: "train", Cache: &orkstackv1.CacheResult{
		Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "SOMETHING_NEW", MissDetail: "the orker's sentence"}})
	if adopted != "train: cache miss (SOMETHING_NEW: the orker's sentence); the container ran" {
		t.Fatalf("adopted / unknown token = %q", adopted)
	}
}

func TestRunAttemptsCachedGolden(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedAttemptRun(t, f, "run-cached-1", "", map[string]*orkstackv1.TaskRun{
		"featurize": {Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1},
		"train":     {Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1},
		"stamp":     {Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1},
	}, "featurize", "stamp", "train")
	put := func(task string, cache *orkstackv1.CacheResult, phase string) {
		if err := attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
			SchemaVersion: 1, RunId: "run-cached-1", TaskId: task, Attempt: 1,
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, Phase: phase, OrkerId: "orker-a1",
			StartedAt: timestamppb.New(fixedNow), FinishedAt: timestamppb.New(fixedNow.Add(time.Second)), Cache: cache,
		}); err != nil {
			t.Fatal(err)
		}
	}
	put("featurize", &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: "k",
		Source: &orkstackv1.CacheSource{RunId: cacheSourceRun}, KeyComponents: map[string]string{"config": "abc"}}, "memo")
	put("stamp", nil, "commit")
	put("train", &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: "k",
		MissReason: "MEMO_EXPIRED", KeyComponents: map[string]string{"config": "def"}}, "commit")
	listed := f.run("run", "attempts", "run-cached-1").requireExit(t, exitOK)
	requireGolden(t, "run-attempts-cached.golden", listed.stdout)
	listed.requireContains(t, "CACHE", "hit "+cacheSourceRun, "miss MEMO_EXPIRED")
	machine := f.run("--json", "run", "attempts", "run-cached-1").requireExit(t, exitOK)
	machine.requireContains(t, `"keyComponents"`)

	// No record with cache: the table is unchanged (no CACHE column).
	plain, plainAttempts := newAttemptFixture(t)
	seedRetriedRun(t, plain, plainAttempts, "run-attempts-1", "")
	if out := plain.run("run", "attempts", "run-attempts-1").requireExit(t, exitOK); strings.Contains(out.stdout, "CACHE") {
		t.Fatalf("a run with no cached task grew a CACHE column:\n%s", out.stdout)
	}
}

func TestOrkflowGetCachedGolden(t *testing.T) {
	f := newFixture(t)
	f.run("apply", cachedExamplePath(t)).requireExit(t, exitOK)
	got := f.run("orkflow", "get", "cached-pipeline").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-cached.golden", got.stdout)
	got.requireContains(t, "CACHED", "featurize  24h", "train      deployment cap  orkflow")
}

func TestRunPlanCachedGolden(t *testing.T) {
	f := newFixture(t)
	f.run("apply", cachedExamplePath(t)).requireExit(t, exitOK)
	got := f.run("run", "plan", "cached-pipeline").requireExit(t, exitOK)
	requireGolden(t, "run-plan-cached.golden", got.stdout)
	got.requireContains(t,
		"cached across runs (ttl 24h, orkflow scope); whether it reuses a result is decided by the Orker when the run reaches it",
		"cached across runs (ttl: the deployment's cap, orkflow scope)")
}

// cacheStatsFixture is a fixture over a real archive and an in-memory memo
// store holding a small afternoon of the cached pipeline.
func cacheStatsFixture(t *testing.T, seed bool) *fixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	memos := artifacts.NewMemoryMemoStore()
	if seed {
		hit := func(minutes int) *orkstackv1.CacheResult {
			start := fixedNow.Add(-5 * time.Hour)
			return &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Source: &orkstackv1.CacheSource{
				RunId: "ork-src", StartedAt: timestamppb.New(start), FinishedAt: timestamppb.New(start.Add(time.Duration(minutes) * time.Minute))}}
		}
		miss := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "MEMO_ABSENT"}
		archive := func(id, orkflow string, minutesAgo int, tasks map[string]*orkstackv1.CacheResult) {
			at := fixedNow.Add(-time.Duration(minutesAgo) * time.Minute)
			run := &orkstackv1.Run{Id: id, Namespace: "default", OrkflowName: orkflow, Status: orkstackv1.Status_STATUS_SUCCEEDED,
				CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute))}
			for _, task := range []string{"featurize", "train", "warm"} {
				if c, ok := tasks[task]; ok {
					run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: task, Status: orkstackv1.Status_STATUS_SUCCEEDED, Cache: c})
				}
			}
			if err := archiver.Archive(context.Background(), run, nil); err != nil {
				t.Fatal(err)
			}
		}
		archive("ork-c1", "cached-pipeline", 60, map[string]*orkstackv1.CacheResult{"featurize": miss, "train": miss})
		archive("ork-c2", "cached-pipeline", 40, map[string]*orkstackv1.CacheResult{"featurize": hit(8), "train": miss})
		archive("ork-c3", "cached-pipeline", 20, map[string]*orkstackv1.CacheResult{"featurize": hit(8), "train": miss})
		archive("ork-n1", "nightly", 30, map[string]*orkstackv1.CacheResult{
			"warm": {Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED}})
		put := func(scope orkstackv1.CacheScope, orkflow, task string, key byte, minutesAgo int) {
			memos.Now = func() time.Time { return fixedNow.Add(-time.Duration(minutesAgo) * time.Minute) }
			if err := memos.PutMemo(context.Background(), &orkstackv1.CacheMemo{SchemaVersion: 1,
				Key: strings.Repeat(string(key), 64), Namespace: "default", Scope: scope, OrkflowName: orkflow, TaskId: task}); err != nil {
				t.Fatal(err)
			}
		}
		put(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "featurize", '1', 60)
		put(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "train", '2', 60)
		put(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "train", '3', 20)
		put(orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, "other", "toolchain", '4', 5000)
	}
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Archive = archiver
		cfg.Memos = memos
	})
}

func TestCacheStats_GoldenHumanJSONAndEmpty(t *testing.T) {
	f := cacheStatsFixture(t, true)
	human := f.run("cache", "stats").requireExit(t, exitOK)
	requireGolden(t, "cache-stats.golden", human.stdout)
	human.requireContains(t, "cached-pipeline  featurize  2     1       0         66.7%        16m00s",
		"nightly          warm       0     0       1         unavailable  -",
		"scanned 4 of 4 finished runs in the window",
		"REUSED is the producing attempts' duration the hits did not spend again.")
	machine := f.run("--json", "cache", "stats").requireExit(t, exitOK)
	requireGolden(t, "cache-stats.json.golden", machine.stdout)

	empty := cacheStatsFixture(t, false).run("cache", "stats").requireExit(t, exitOK)
	requireGolden(t, "cache-stats-empty.golden", empty.stdout)
	empty.requireContains(t, "No cached task finished in this window.", "No memos stored.")
}

func TestCachePurge_RequiresExactlyOneSelector(t *testing.T) {
	f := cacheStatsFixture(t, true)
	before := len(f.requests())
	for _, args := range [][]string{
		{"cache", "purge"},
		{"cache", "purge", "--orkflow", "p", "--all"},
		{"cache", "purge", "--namespace-scoped", "--all"},
	} {
		f.run(args...).requireExit(t, exitError).
			requireStderrContains(t, "name exactly one of --orkflow, --namespace-scoped or --all")
	}
	if len(f.requests()) != before {
		t.Fatal("a refused purge sent a request")
	}
}

func TestCachePurge_TaskWithAllIsRefused(t *testing.T) {
	f := cacheStatsFixture(t, true)
	before := len(f.requests())
	f.run("cache", "purge", "--all", "--task", "featurize").requireExit(t, exitError).
		requireStderrContains(t, "--task narrows --orkflow or --namespace-scoped")
	if len(f.requests()) != before {
		t.Fatal("a refused purge sent a request")
	}
}

func TestCachePurge_GoldenHumanAndJSON(t *testing.T) {
	f := cacheStatsFixture(t, true)
	human := f.run("cache", "purge", "--orkflow", "cached-pipeline", "--task", "train").requireExit(t, exitOK)
	requireGolden(t, "cache-purge.golden", human.stdout)
	human.requireContains(t, "Purged 2 memos (orkflow cached-pipeline, task train) in namespace default.",
		"No run, commit or artifact was touched")

	g := cacheStatsFixture(t, true)
	machine := g.run("--json", "cache", "purge", "--all", "--older-than", "48h").requireExit(t, exitOK)
	requireGolden(t, "cache-purge.json.golden", machine.stdout)
	machine.requireContains(t, `"deleted": "1"`, `"kept": "3"`)
}

// pagingCache is a CacheService that answers PurgeCache with more: true twice.
type pagingCache struct {
	orkstackv1connect.UnimplementedCacheServiceHandler
	mu    sync.Mutex
	calls int
}

func (p *pagingCache) PurgeCache(context.Context, *connect.Request[orkstackv1.PurgeCacheRequest]) (*connect.Response[orkstackv1.PurgeCacheResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return connect.NewResponse(&orkstackv1.PurgeCacheResponse{Deleted: 10000, More: p.calls < 3, Namespace: "default"}), nil
}

func servePagingCache(t *testing.T) (string, *pagingCache) {
	t.Helper()
	paging := &pagingCache{}
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewCacheServiceHandler(paging))
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
	return "http://" + listener.Addr().String(), paging
}

func TestCachePurge_LoopsWhileMore(t *testing.T) {
	server, paging := servePagingCache(t)
	out := runAgainst(t, server, "cache", "purge", "--all").requireExit(t, exitOK)
	out.requireContains(t, "Purged 30000 memos (all) in namespace default.")
	if paging.calls != 3 {
		t.Fatalf("calls = %d, want 3", paging.calls)
	}
}

func TestCachePurge_JSONMakesOneCall(t *testing.T) {
	server, paging := servePagingCache(t)
	out := runAgainst(t, server, "--json", "cache", "purge", "--all").requireExit(t, exitOK)
	out.requireContains(t, `"more": true`)
	if paging.calls != 1 {
		t.Fatalf("calls = %d, want 1", paging.calls)
	}
}

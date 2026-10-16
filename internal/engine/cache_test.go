package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestEffectiveCacheTTL(t *testing.T) {
	cases := []struct {
		ttl  int32
		cap  int64
		want int64
	}{
		{0, 604800, 604800},
		{3600, 604800, 3600},
		{700000, 604800, 604800},
		{604800, 604800, 604800},
		{-5, 60, 60},
	}
	for _, tc := range cases {
		if got := engine.EffectiveCacheTTL(tc.ttl, tc.cap); got != tc.want {
			t.Fatalf("EffectiveCacheTTL(%d, %d) = %d, want %d", tc.ttl, tc.cap, got, tc.want)
		}
	}
	if engine.DefaultCacheMaxTTL != 168*time.Hour {
		t.Fatalf("DefaultCacheMaxTTL = %s", engine.DefaultCacheMaxTTL)
	}
}

const cachedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: cached
tasks:
  - id: featurize
    uses: shell
    cache: {enabled: true, ttl_seconds: 3600}
    with: {op: exec}
  - id: stamp
    uses: shell
    with: {op: exec}
  - id: train
    uses: shell
    needs: [featurize, stamp]
    cache: {enabled: true, scope: namespace}
    with: {op: exec}
`

// runCached runs document under plan through the fan-out harness, which
// records every input and every published event.
func runCached(t *testing.T, h *eachHarness, runID, document string, plan engine.RunPlan) *orkstackv1.Run {
	t.Helper()
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, document), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run
}

func cachePlan(refresh bool) engine.RunPlan {
	plan := defaultPlan()
	plan.Cache = &engine.CachePlan{MaxTTLSeconds: 604800, Refresh: refresh}
	return plan
}

func plainResult(in *orkstackv1.ExecuteTaskInput) *orkstackv1.ExecuteTaskResult {
	return &orkstackv1.ExecuteTaskResult{
		TaskId:    in.GetTaskId(),
		Attempt:   1,
		OrkerId:   "orker-1",
		ValuesRef: &orkstackv1.ArtifactRef{Uri: "mem://values/" + in.GetTaskId(), Sha256: strings.Repeat("a", 64)},
		Commit:    &orkstackv1.CommitRef{Uri: in.GetCommitUri()},
	}
}

func TestRunWorkflow_CacheContextOnlyForCachedTasks(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return plainResult(in), nil
	})
	run := runCached(t, h, "run-cache-ctx", cachedDocument, cachePlan(false))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	want := map[string]*orkstackv1.CacheContext{
		"featurize": {Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, TtlSeconds: 3600},
		"stamp":     nil,
		"train":     {Scope: orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, TtlSeconds: 604800},
	}
	for id, w := range want {
		calls := h.itemCalls(id)
		if len(calls) != 1 {
			t.Fatalf("%s: %d calls", id, len(calls))
		}
		if !proto.Equal(calls[0].GetCache(), w) {
			t.Fatalf("%s cache = %v, want %v", id, calls[0].GetCache(), w)
		}
	}
}

func TestRunWorkflow_RefreshPlanSendsRefresh(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return plainResult(in), nil
	})
	runCached(t, h, "run-cache-refresh", cachedDocument, cachePlan(true))
	if !h.itemCalls("featurize")[0].GetCache().GetRefresh() || !h.itemCalls("train")[0].GetCache().GetRefresh() {
		t.Fatal("a no_cache plan did not send refresh")
	}
	if h.itemCalls("stamp")[0].GetCache() != nil {
		t.Fatal("an uncached task got a cache context")
	}
}

func TestRunWorkflow_PlanWithoutCacheSendsNone(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return plainResult(in), nil
	})
	runCached(t, h, "run-cache-none", cachedDocument, defaultPlan())
	for _, id := range []string{"featurize", "stamp", "train"} {
		if h.itemCalls(id)[0].GetCache() != nil {
			t.Fatalf("%s got a cache context from a plan with none", id)
		}
	}
}

func hitFrom(runID string, item *int32) *orkstackv1.CacheResult {
	return &orkstackv1.CacheResult{
		Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT,
		Key:     strings.Repeat("f", 64),
		Source: &orkstackv1.CacheSource{
			RunId: runID, TaskId: "featurize", Item: item,
			CommitUri:  "mem://commits/source/commit.json",
			StartedAt:  timestamppb.New(time.Unix(100, 0)),
			FinishedAt: timestamppb.New(time.Unix(160, 0)),
		},
	}
}

func TestRunWorkflow_CachedResultIsRecordedAndAnnounced(t *testing.T) {
	sourceArtifact := &orkstackv1.ArtifactRef{Name: "features.txt", Uri: "s3://b/orkflows/cached/ork-src/featurize/features.txt", Sha256: strings.Repeat("c", 64)}
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := plainResult(in)
		if in.GetTaskId() == "featurize" {
			result.Cache = hitFrom("ork-src", nil)
			result.Artifacts = []*orkstackv1.ArtifactRef{sourceArtifact}
		}
		return result, nil
	})
	run := runCached(t, h, "run-cache-hit", cachedDocument, cachePlan(false))
	featurize := taskRunOf(t, run, "featurize")
	if !proto.Equal(featurize.GetCache(), hitFrom("ork-src", nil)) {
		t.Fatalf("TaskRun.cache = %v", featurize.GetCache())
	}
	if taskRunOf(t, run, "stamp").GetCache() != nil {
		t.Fatal("an uncached task carries cache")
	}
	var message string
	for _, e := range h.eventsFor("run-cache-hit", "featurize") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED {
			message = e.GetMessage()
		}
	}
	if message != "task succeeded (cached from run ork-src)" {
		t.Fatalf("TASK_SUCCEEDED = %q", message)
	}
	// The downstream task stages from this run's commit and the source's
	// references.
	train := h.itemCalls("train")[0]
	var up *orkstackv1.UpstreamRef
	for _, ref := range train.GetUpstream() {
		if ref.GetTaskId() == "featurize" {
			up = ref
		}
	}
	key, _ := artifacts.CommitKey("default", "run-cache-hit", "featurize")
	if up.GetCommitUri() != "mem://commits/"+key || len(up.GetArtifacts()) != 1 || !proto.Equal(up.GetArtifacts()[0], sourceArtifact) {
		t.Fatalf("downstream upstream ref = %v", up)
	}
}

func TestRunWorkflow_MissAndBypassedKeepTheSuccessMessage(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := plainResult(in)
		switch in.GetTaskId() {
		case "featurize":
			result.Cache = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: "k", MissReason: "MEMO_ABSENT"}
		case "train":
			result.Cache = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED, Key: "k", MemoWritten: true}
		}
		return result, nil
	})
	run := runCached(t, h, "run-cache-miss", cachedDocument, cachePlan(false))
	for _, id := range []string{"featurize", "train"} {
		if taskRunOf(t, run, id).GetCache() == nil {
			t.Fatalf("%s carries no cache", id)
		}
		for _, e := range h.eventsFor("run-cache-miss", id) {
			if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED && e.GetMessage() != "task succeeded" {
				t.Fatalf("%s TASK_SUCCEEDED = %q, want unchanged", id, e.GetMessage())
			}
		}
	}
}

func TestRunWorkflow_CachedFanOutItemsCarryCache(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := listThenItems(h, partsJSON(3))(ctx, in)
		if in.GetTaskId() == "convert" && in.GetEach().GetIndex() == 1 {
			item := int32(1)
			result.Cache = hitFrom("ork-src", &item)
		}
		return result, err
	})
	document := strings.Replace(fmt.Sprintf(eachTaskDocument, ""), "    each:", "    cache: {enabled: true}\n    each:", 1)
	run := runCached(t, h, "run-cache-each", document, cachePlan(false))
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	for _, in := range h.itemCalls("convert") {
		if in.GetCache() == nil {
			t.Fatalf("item %d of a cached fan-out has no cache context", in.GetEach().GetIndex())
		}
	}
	node := taskRunOf(t, run, "convert")
	if node.GetCache() != nil {
		t.Fatal("the fan-out's own row carries cache")
	}
	if node.GetItems()[1].GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || node.GetItems()[0].GetCache() != nil {
		t.Fatalf("items = %v", node.GetItems())
	}
	var found bool
	for _, e := range h.eventsFor("run-cache-each", "convert") {
		if strings.Contains(e.GetMessage(), "item 1 succeeded (cached from run ork-src, item 1)") {
			found = true
		}
	}
	if !found {
		t.Fatal("no cached item event")
	}
}

func TestSimulate_CachedTaskCarriesItsSpec(t *testing.T) {
	wf := compileExample(t, "cached-pipeline.ork.yaml")
	plan := simulate(t, wf, payload(t, nil), assumedPayloads(t, map[string]map[string]any{"stamp": {"value": "1"}}))
	for _, pt := range plan.GetTasks() {
		switch pt.GetTaskId() {
		case "featurize":
			if pt.GetCache().GetTtlSeconds() != 86400 {
				t.Fatalf("featurize plan cache = %v", pt.GetCache())
			}
		case "train":
			if pt.GetCache() == nil {
				t.Fatal("train plan carries no cache")
			}
		case "stamp":
			if pt.GetCache() != nil {
				t.Fatal("stamp plan carries cache")
			}
		}
		if pt.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
			t.Fatalf("%s outcome = %v: a plan never predicts a hit", pt.GetTaskId(), pt.GetOutcome())
		}
	}
}

func TestAggregateCacheUsage(t *testing.T) {
	item := int32(0)
	hit := func(d time.Duration) *orkstackv1.CacheResult {
		return &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Source: &orkstackv1.CacheSource{
			StartedAt: timestamppb.New(time.Unix(1000, 0)), FinishedAt: timestamppb.New(time.Unix(1000, 0).Add(d))}}
	}
	miss := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS}
	bypass := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED}
	backwards := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Source: &orkstackv1.CacheSource{
		StartedAt: timestamppb.New(time.Unix(2000, 0)), FinishedAt: timestamppb.New(time.Unix(1000, 0))}}
	runs := []*orkstackv1.Run{
		{OrkflowName: "p", TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "featurize", Cache: hit(8 * time.Minute)},
			{TaskId: "train", Cache: miss},
			{TaskId: "stamp"},
			{TaskId: "odd", Cache: &orkstackv1.CacheResult{}},
		}},
		{OrkflowName: "p", TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "featurize", Cache: backwards},
			{TaskId: "train", Cache: miss},
			{TaskId: "convert", Each: &orkstackv1.TaskEach{}, Items: []*orkstackv1.TaskRun{
				{TaskId: "convert", Item: &item, Cache: hit(time.Minute)},
				{TaskId: "convert", Item: &item, Cache: miss},
			}},
		}},
		{OrkflowName: "a", TaskRuns: []*orkstackv1.TaskRun{{TaskId: "warm", Cache: bypass}}},
	}
	rows := engine.AggregateCacheUsage(runs)
	got := []string{}
	for _, r := range rows {
		rate := "-"
		if r.GetHitRateDefined() {
			rate = fmt.Sprintf("%.2f", r.GetHitRate())
		}
		got = append(got, fmt.Sprintf("%s/%s %d/%d/%d %s %s", r.GetOrkflowName(), r.GetTaskId(),
			r.GetHits(), r.GetMisses(), r.GetBypassed(), rate, r.GetReused().AsDuration()))
	}
	want := []string{
		"a/warm 0/0/1 - 0s",
		"p/convert 1/1/0 0.50 1m0s",
		"p/featurize 2/0/0 1.00 8m0s",
		"p/train 0/2/0 0.00 0s",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows = %q\nwant  %q", got, want)
	}
	if !proto.Equal(rows[0].GetReused(), durationpb.New(0)) {
		t.Fatal("reused of zero is not a zero duration")
	}
}

func TestGroupMemoInventory(t *testing.T) {
	at := func(s int64) time.Time { return time.Unix(s, 0).UTC() }
	ref := func(orkflow string, ns bool, task, key string) artifacts.MemoRef {
		return artifacts.MemoRef{MemoScopeRef: artifacts.MemoScopeRef{Namespace: "ml", Orkflow: orkflow, NamespaceScoped: ns, TaskID: task}, Key: key}
	}
	entries := []artifacts.MemoEntry{
		{Ref: ref("", true, "toolchain", "1"), LastModified: at(50)},
		{Ref: ref("p", false, "train", "1"), LastModified: at(30)},
		{Ref: ref("p", false, "featurize", "1"), LastModified: at(20)},
		{Ref: ref("p", false, "featurize", "2"), LastModified: at(10)},
		{Ref: ref("p", false, "featurize", "3"), LastModified: at(40)},
		{Ref: ref("b", false, "x", "1"), LastModified: at(5)},
	}
	rows := engine.GroupMemoInventory(entries)
	got := []string{}
	for _, r := range rows {
		got = append(got, fmt.Sprintf("%s %s/%s %d %d-%d", r.GetScope(), r.GetOrkflowName(), r.GetTaskId(),
			r.GetMemos(), r.GetOldest().AsTime().Unix(), r.GetNewest().AsTime().Unix()))
	}
	want := []string{
		"CACHE_SCOPE_ORKFLOW b/x 1 5-5",
		"CACHE_SCOPE_ORKFLOW p/featurize 3 10-40",
		"CACHE_SCOPE_ORKFLOW p/train 1 30-30",
		"CACHE_SCOPE_NAMESPACE /toolchain 1 50-50",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows = %q\nwant  %q", got, want)
	}
}

// ---------------------------------------------------------------- the pool check

const cachePoolsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: cache-pools
tasks:
  - id: fetch
    uses: shell
    pool: cpu
    with: {op: exec}
  - id: featurize
    uses: shell
    pool: gpu
    needs: [fetch]
    cache: {enabled: true}
    with: {op: exec}
  - id: train
    uses: shell
    pool: gpu
    needs: [featurize]
    cache: {enabled: true}
    with: {op: exec}
`

// cacheSupportPools answers DescribeOrker per channel with cache_supported
// as given (each_supported always true), and records every channel asked.
type cacheSupportPools struct {
	eachSupportPools
	cacheSupported map[string]bool
}

func (p *cacheSupportPools) OrkerOnPool(ctx context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	result, err := p.eachSupportPools.OrkerOnPool(ctx, queue)
	if err != nil {
		return nil, err
	}
	result.EachSupported = true
	result.CacheSupported = p.cacheSupported[queue]
	return result, nil
}

func cachePoolsResolver(p *cacheSupportPools) *engine.PoolResolver {
	cpu, _ := task.QueueForPool("cpu", "")
	gpu, _ := task.QueueForPool("gpu", "")
	return &engine.PoolResolver{Probe: livePools{cpu: 1, gpu: 1}, Orkers: p}
}

func TestPoolResolverPlan_RefusesAPoolWhoseOrkerPredatesCaching(t *testing.T) {
	p := &cacheSupportPools{cacheSupported: map[string]bool{}}
	_, _, err := cachePoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, cachePoolsDocument), false)
	refused, ok := err.(*engine.PoolNoCacheSupportError)
	if !ok {
		t.Fatalf("Plan = %v, want PoolNoCacheSupportError", err)
	}
	if refused.Pool != "gpu" || fmt.Sprint(refused.Tasks) != "[featurize train]" {
		t.Fatalf("refusal = %+v", refused)
	}
	want := `pool "gpu" (namespace "ml", tasks: featurize, train) is served by Orker "gpu-3" (build "2026.09.01"), ` +
		`which predates task caching (cache:): it cannot read a cached task's input and would fail it. ` +
		`Upgrade the pool's Orkers before submitting this Orkflow.`
	if err.Error() != want {
		t.Fatalf("message = %q\nwant %q", err.Error(), want)
	}
	gpu, _ := task.QueueForPool("gpu", "")
	p.cacheSupported[gpu] = true
	if _, _, err := cachePoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, cachePoolsDocument), false); err != nil {
		t.Fatalf("a pool whose Orker supports caching = %v", err)
	}
}

func TestPoolResolverPlan_CacheIgnoresADescribeFailure(t *testing.T) {
	p := &cacheSupportPools{}
	p.fail = true
	if _, _, err := cachePoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, cachePoolsDocument), false); err != nil {
		t.Fatalf("a describe failure refused the submit: %v", err)
	}
}

func TestPoolResolverPlan_CacheAllowEmptyPoolDoesNotDowngradeIt(t *testing.T) {
	p := &cacheSupportPools{cacheSupported: map[string]bool{}}
	_, _, err := cachePoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, cachePoolsDocument), true)
	if _, ok := err.(*engine.PoolNoCacheSupportError); !ok {
		t.Fatalf("Plan with allow_empty_pool = %v, want the refusal to stand", err)
	}
}

func TestPoolResolverPlan_AsksOnlyForPoolsOfCachedTasks(t *testing.T) {
	gpu, _ := task.QueueForPool("gpu", "")
	p := &cacheSupportPools{cacheSupported: map[string]bool{gpu: true}}
	if _, _, err := cachePoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, cachePoolsDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if fmt.Sprint(p.asked) != fmt.Sprint([]string{gpu}) {
		t.Fatalf("asked %v, want only gpu", p.asked)
	}
}

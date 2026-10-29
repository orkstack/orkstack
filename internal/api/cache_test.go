package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const cachedYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: cached-pipeline
tasks:
  - id: featurize
    uses: busybox:latest
    cache: {enabled: true, ttl_seconds: 86400}
  - id: train
    uses: busybox:latest
    needs: [featurize]
    cache: {enabled: true}
`

const uncachedYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: plain
tasks:
  - id: work
    uses: busybox:latest
`

func submitPlan(t *testing.T, svc *api.Service, starter *fakeStarter, req *orkstackv1.SubmitRunRequest) engine.RunPlan {
	t.Helper()
	if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(req)); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.plans[len(starter.plans)-1]
}

func applyYAML(t *testing.T, svc *api.Service, yaml string) *orkstackv1.ApplyOrkflowResponse {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.Msg
}

func TestSubmitRun_PlanCarriesCacheOnlyWhenDeclared(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Starter: starter, CacheMaxTTL: 48 * time.Hour})
	applyYAML(t, svc, cachedYAML)
	applyYAML(t, svc, uncachedYAML)
	plan := submitPlan(t, svc, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "cached-pipeline"})
	if plan.Cache == nil || plan.Cache.MaxTTLSeconds != 48*3600 || plan.Cache.Refresh {
		t.Fatalf("plan.Cache = %+v", plan.Cache)
	}
	plain := submitPlan(t, svc, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "plain"})
	if plain.Cache != nil {
		t.Fatalf("an uncached Orkflow's plan carries cache: %+v", plain.Cache)
	}
	encoded, err := json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(`"cache"`)) {
		t.Fatalf("a plan with no cache encodes one: %s", encoded)
	}
	// The default cap when none is configured.
	defaulted := api.NewService(api.Config{Starter: starter})
	applyYAML(t, defaulted, cachedYAML)
	if got := submitPlan(t, defaulted, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "cached-pipeline"}); got.Cache.MaxTTLSeconds != 168*3600 {
		t.Fatalf("default cap = %d", got.Cache.MaxTTLSeconds)
	}
}

func TestSubmitRun_NoCacheSetsRefresh(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Starter: starter})
	applyYAML(t, svc, cachedYAML)
	plan := submitPlan(t, svc, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "cached-pipeline", NoCache: true})
	if plan.Cache == nil || !plan.Cache.Refresh {
		t.Fatalf("plan.Cache = %+v, want refresh", plan.Cache)
	}
}

func TestSubmitRun_NoCacheOnAnUncachedOrkflowChangesNothing(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Starter: starter})
	applyYAML(t, svc, uncachedYAML)
	without := submitPlan(t, svc, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "plain"})
	with := submitPlan(t, svc, starter, &orkstackv1.SubmitRunRequest{OrkflowName: "plain", NoCache: true})
	a, _ := json.Marshal(without)
	b, _ := json.Marshal(with)
	if !bytes.Equal(a, b) {
		t.Fatalf("no_cache changed an uncached plan:\n%s\n%s", a, b)
	}
}

func TestSubmitRun_ComposedChildPlansInheritNoCache(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, strings.Replace(composeChildYAML, "  - id: sign\n    uses: shell\n",
		"  - id: sign\n    uses: shell\n    cache: {enabled: true}\n", 1))
	f.mustApply(t, composeParentYAML)
	if _, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", Namespace: "team-a", NoCache: true,
	})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	plans := f.plans()
	parent := plans[len(plans)-1]
	if parent.Cache != nil {
		t.Fatalf("the uncached parent's plan carries cache: %+v", parent.Cache)
	}
	child := parent.Children["release"]
	if child == nil {
		t.Fatalf("no child plan: %+v", parent.Children)
	}
	if child.Plan.Cache == nil || !child.Plan.Cache.Refresh {
		t.Fatalf("the child plan's cache = %+v, want the parent's no_cache", child.Plan.Cache)
	}
}

// oldGPUOrker is gpu's one Orker, of a build that predates the capability
// without() clears.
func oldGPUOrker(without func(*orkstackv1.DescribeOrkerResult)) *stubFleet {
	orker := currentOrker("gpu-3", "gpu", 0)
	orker.BuildId = "2026.09.01"
	without(orker)
	return fleetOf(orker)
}

func TestSubmitRun_PoolWithoutCacheSupportIsFailedPreconditionAndMintsNothing(t *testing.T) {
	starter := &fakeStarter{}
	gpuQueue := poolQueue(t, "gpu")
	minted := 0
	svc := api.NewService(api.Config{
		Starter: starter, Pools: livePools{gpuQueue: 1}, Fleet: oldGPUOrker(func(o *orkstackv1.DescribeOrkerResult) { o.CacheSupported = false }),
		NewRunID: func() string { minted++; return fmt.Sprintf("run-%d", minted) },
	})
	applyYAML(t, svc, strings.ReplaceAll(cachedYAML, "uses: busybox:latest", "uses: busybox:latest\n    pool: gpu"))
	for _, noCache := range []bool{false, true} {
		_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "cached-pipeline", AllowEmptyPool: true, NoCache: noCache}))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "predates task caching (cache:)") {
			t.Fatalf("no_cache=%v submit = %v", noCache, err)
		}
	}
	if len(starter.startedRuns()) != 0 || minted != 0 {
		t.Fatalf("a refused submission started %v / minted %d ids", starter.startedRuns(), minted)
	}
}

// ---------------------------------------------------------------- apply warnings

func warningsFor(t *testing.T, svc *api.Service, yaml string) []*orkstackv1.ValidationIssue {
	t.Helper()
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: yaml}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	return resp.Msg.GetIssues()
}

func issueAt(issues []*orkstackv1.ValidationIssue, field string) *orkstackv1.ValidationIssue {
	for _, issue := range issues {
		if issue.GetField() == field {
			return issue
		}
	}
	return nil
}

func TestValidateOrkflow_CacheTTLAboveTheCapIsAWarning(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}, CacheMaxTTL: 168 * time.Hour})
	yaml := strings.Replace(cachedYAML, "ttl_seconds: 86400", "ttl_seconds: 9999999", 1)
	issue := issueAt(warningsFor(t, svc, yaml), "tasks[0].cache.ttl_seconds")
	if issue == nil || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		issue.GetMessage() != `task "featurize": memos are reused for at most 168h (ORK_CACHE_MAX_TTL); a longer ttl_seconds has no effect` {
		t.Fatalf("issue = %v", issue)
	}
	// Within the cap: no warning on featurize.
	if issueAt(warningsFor(t, svc, cachedYAML), "tasks[0].cache.ttl_seconds") != nil {
		t.Fatal("a TTL within the cap warned")
	}
	// Apply succeeds and returns the warning.
	resp := applyYAML(t, svc, yaml)
	var found bool
	for _, w := range resp.GetWarnings() {
		if strings.HasPrefix(w, "tasks[0].cache.ttl_seconds: ") && strings.Contains(w, "has no effect") {
			found = true
		}
	}
	if !found {
		t.Fatalf("apply warnings = %v", resp.GetWarnings())
	}
}

func TestValidateOrkflow_CacheZeroTTLWarnsTheCap(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}, CacheMaxTTL: 48 * time.Hour})
	issue := issueAt(warningsFor(t, svc, cachedYAML), "tasks[1].cache.ttl_seconds")
	if issue == nil || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		!strings.Contains(issue.GetMessage(), `task "train"`) || !strings.Contains(issue.GetMessage(), "48h") ||
		!strings.Contains(issue.GetMessage(), "ORK_CACHE_MAX_TTL") {
		t.Fatalf("issue = %v", issue)
	}
}

func TestValidateOrkflow_CachedTaskWithSecretsWarns(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}})
	yaml := strings.Replace(cachedYAML, "    cache: {enabled: true, ttl_seconds: 86400}",
		"    cache: {enabled: true, ttl_seconds: 86400}\n    secrets: [{name: db, source: 'env:DB'}]", 1)
	issue := issueAt(warningsFor(t, svc, yaml), "tasks[0].secrets")
	want := `task "featurize": a cached task's key covers its secrets' names and sources, never their values — ` +
		`rotating a secret does not invalidate its memos; purge them with orkctl cache purge --orkflow cached-pipeline --task featurize`
	if issue == nil || issue.GetMessage() != want || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
		t.Fatalf("issue = %v\nwant %s", issue, want)
	}
	applyYAML(t, svc, yaml)
}

// ---------------------------------------------------------------- GetCacheStats

var cacheNow = time.Date(2026, 9, 25, 10, 20, 0, 0, time.UTC)

func withMemos(store artifacts.MemoStore) breakdownOption {
	return func(cfg *api.Config, _ *breakdownFixture) { cfg.Memos = store }
}

// cacheRun archives one finished run of orkflow whose tasks carry the given
// cache results.
func (f *breakdownFixture) cacheRun(t *testing.T, namespace, orkflow string, at time.Time, tasks map[string]*orkstackv1.CacheResult) string {
	t.Helper()
	f.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), f.seeded)
	run := &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: orkflow, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
	}
	ids := make([]string, 0, len(tasks))
	for task := range tasks {
		ids = append(ids, task)
	}
	sort.Strings(ids)
	for _, task := range ids {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: task, Status: orkstackv1.Status_STATUS_SUCCEEDED, Cache: tasks[task]})
	}
	if err := engine.NewRunArchiver(f.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	return id
}

func cacheHit(source string, took time.Duration) *orkstackv1.CacheResult {
	return &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: strings.Repeat("a", 64),
		Source: &orkstackv1.CacheSource{RunId: source,
			StartedAt: timestamppb.New(cacheNow.Add(-time.Hour)), FinishedAt: timestamppb.New(cacheNow.Add(-time.Hour).Add(took))}}
}

var cacheMiss = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "MEMO_ABSENT"}

func putTestMemo(t *testing.T, store artifacts.MemoStore, scope orkstackv1.CacheScope, orkflow, task string, key byte) {
	t.Helper()
	if err := store.PutMemo(context.Background(), &orkstackv1.CacheMemo{
		SchemaVersion: artifacts.MemoSchemaVersion, Key: strings.Repeat(string(key), 64), Namespace: "ml",
		Scope: scope, OrkflowName: orkflow, TaskId: task,
	}); err != nil {
		t.Fatalf("PutMemo: %v", err)
	}
}

type cacheFixture struct {
	*breakdownFixture
	memos *artifacts.MemoryMemoStore
}

func newCacheFixture(t *testing.T) *cacheFixture {
	t.Helper()
	memos := artifacts.NewMemoryMemoStore()
	memos.Now = func() time.Time { return cacheNow.Add(-time.Hour) }
	f := newBreakdownFixture(t, cacheNow, []string{"ml", "beta"}, withMemos(memos))
	return &cacheFixture{breakdownFixture: f, memos: memos}
}

func (f *cacheFixture) seed(t *testing.T) {
	t.Helper()
	f.cacheRun(t, "ml", "cached-pipeline", cacheNow.Add(-2*time.Hour), map[string]*orkstackv1.CacheResult{
		"featurize": cacheMiss, "train": cacheMiss, "stamp": nil})
	f.cacheRun(t, "ml", "cached-pipeline", cacheNow.Add(-time.Hour), map[string]*orkstackv1.CacheResult{
		"featurize": cacheHit("ork-1", 8*time.Minute), "train": cacheMiss})
	f.cacheRun(t, "ml", "nightly", cacheNow.Add(-time.Hour), map[string]*orkstackv1.CacheResult{
		"warm": {Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED}})
	// Out of the window, and another namespace: not the population.
	f.cacheRun(t, "ml", "cached-pipeline", cacheNow.Add(-72*time.Hour), map[string]*orkstackv1.CacheResult{"featurize": cacheMiss})
	f.cacheRun(t, "beta", "cached-pipeline", cacheNow.Add(-time.Hour), map[string]*orkstackv1.CacheResult{"featurize": cacheMiss})
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "featurize", '1')
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "featurize", '2')
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "cached-pipeline", "train", '3')
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, "other", "toolchain", '4')
}

func cacheStatsReq(orkflow string) *orkstackv1.GetCacheStatsRequest {
	return &orkstackv1.GetCacheStatsRequest{Namespace: "ml", OrkflowName: orkflow, Lookback: durationpb.New(24 * time.Hour)}
}

func (f *cacheFixture) stats(t *testing.T, h *api.CacheHandler, req *orkstackv1.GetCacheStatsRequest) *orkstackv1.GetCacheStatsResponse {
	t.Helper()
	resp, err := h.GetCacheStats(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("GetCacheStats: %v", err)
	}
	return resp.Msg
}

func (f *cacheFixture) handler(ttl time.Duration) *api.CacheHandler {
	return api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, ttl))
}

func usageLines(resp *orkstackv1.GetCacheStatsResponse) []string {
	var out []string
	for _, r := range resp.GetUsage() {
		out = append(out, fmt.Sprintf("%s/%s %d/%d/%d %v %s", r.GetOrkflowName(), r.GetTaskId(), r.GetHits(),
			r.GetMisses(), r.GetBypassed(), r.GetHitRateDefined(), r.GetReused().AsDuration()))
	}
	return out
}

func inventoryLines(resp *orkstackv1.GetCacheStatsResponse) []string {
	var out []string
	for _, r := range resp.GetInventory() {
		out = append(out, fmt.Sprintf("%s %s/%s %d", r.GetScope(), r.GetOrkflowName(), r.GetTaskId(), r.GetMemos()))
	}
	return out
}

func TestGetCacheStats_UsageAndInventory(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	resp := f.stats(t, f.handler(0), cacheStatsReq(""))
	wantUsage := []string{
		"cached-pipeline/featurize 1/1/0 true 8m0s",
		"cached-pipeline/train 0/2/0 true 0s",
		"nightly/warm 0/0/1 false 0s",
	}
	if fmt.Sprint(usageLines(resp)) != fmt.Sprint(wantUsage) {
		t.Fatalf("usage = %q\nwant %q", usageLines(resp), wantUsage)
	}
	wantInventory := []string{
		"CACHE_SCOPE_ORKFLOW cached-pipeline/featurize 2",
		"CACHE_SCOPE_ORKFLOW cached-pipeline/train 1",
		"CACHE_SCOPE_NAMESPACE /toolchain 1",
	}
	if fmt.Sprint(inventoryLines(resp)) != fmt.Sprint(wantInventory) {
		t.Fatalf("inventory = %q\nwant %q", inventoryLines(resp), wantInventory)
	}
	if resp.GetRunsInWindow() != 3 || resp.GetRunsScanned() != 3 || resp.GetNamespace() != "ml" ||
		!resp.GetComputedAt().AsTime().Equal(cacheNow) || resp.GetWindowSince() == nil {
		t.Fatalf("response facts = %v", resp)
	}
}

func TestGetCacheStats_OrkflowFilterOmitsNamespaceScopedMemos(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	resp := f.stats(t, f.handler(0), cacheStatsReq("cached-pipeline"))
	if len(resp.GetUsage()) != 2 || resp.GetRunsInWindow() != 2 {
		t.Fatalf("usage = %q, runs %d", usageLines(resp), resp.GetRunsInWindow())
	}
	for _, line := range inventoryLines(resp) {
		if strings.Contains(line, "NAMESPACE") {
			t.Fatalf("an orkflow filter listed a namespace-scoped memo: %q", inventoryLines(resp))
		}
	}
	if _, err := f.handler(0).GetCacheStats(context.Background(), connect.NewRequest(cacheStatsReq("Bad_Name"))); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a bad slug = %v", err)
	}
}

func TestGetCacheStats_ScanBudgetIsResourceExhausted(t *testing.T) {
	f := newCacheFixture(t)
	entries := make([]artifacts.RunIndexEntry, api.CacheStatsMaxScan+1)
	for i := range entries {
		entries[i] = artifacts.RunIndexEntry{RunID: fmt.Sprintf("ork-%d", i), Namespace: "ml", Orkflow: "p",
			Status: "succeeded", Time: cacheNow.Add(-time.Minute)}
	}
	f.bodies.entries = entries
	_, err := f.handler(0).GetCacheStats(context.Background(), connect.NewRequest(cacheStatsReq("")))
	if connect.CodeOf(err) != connect.CodeResourceExhausted || !strings.Contains(err.Error(), fmt.Sprint(api.CacheStatsMaxScan)) {
		t.Fatalf("err = %v", err)
	}
	if n := f.bodies.totalReads(); n != 0 {
		t.Fatalf("an over-budget read opened %d bodies", n)
	}
}

// pagedMemos lists n synthetic memos, cacheListPage at a time, without
// storing them.
type pagedMemos struct {
	artifacts.MemoStore
	n int
}

func (p pagedMemos) ListMemos(_ context.Context, _ artifacts.MemoScopeRef, after string, limit int) ([]artifacts.MemoEntry, string, error) {
	start := 0
	if after != "" {
		fmt.Sscanf(after, "%d", &start)
	}
	var entries []artifacts.MemoEntry
	i := start
	for ; i < p.n && len(entries) < limit; i++ {
		entries = append(entries, artifacts.MemoEntry{Ref: artifacts.MemoRef{MemoScopeRef: artifacts.MemoScopeRef{
			Namespace: "ml", Orkflow: "p", TaskID: "t"}, Key: fmt.Sprintf("%064d", i)}})
	}
	if i >= p.n {
		return entries, "", nil
	}
	return entries, fmt.Sprint(i), nil
}

func TestGetCacheStats_MemoBudgetIsResourceExhausted(t *testing.T) {
	f := newBreakdownFixture(t, cacheNow, []string{"ml"}, withMemos(pagedMemos{n: api.CacheStatsMaxMemos + 1}))
	h := api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, 0))
	_, err := h.GetCacheStats(context.Background(), connect.NewRequest(cacheStatsReq("")))
	if connect.CodeOf(err) != connect.CodeResourceExhausted ||
		!strings.Contains(err.Error(), `namespace "ml" holds more than 100000 memos`) ||
		!strings.Contains(err.Error(), "orkctl cache purge --all --older-than 168h") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetCacheStats_MissingBodyIsSkippedAndCounted(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	id := f.cacheRun(t, "ml", "cached-pipeline", cacheNow.Add(-30*time.Minute), map[string]*orkstackv1.CacheResult{"featurize": cacheMiss})
	f.bodies.absent[id] = true
	resp := f.stats(t, f.handler(0), cacheStatsReq(""))
	if resp.GetRunsInWindow() != 4 || resp.GetRunsScanned() != 3 {
		t.Fatalf("runs %d/%d, want 3 of 4", resp.GetRunsScanned(), resp.GetRunsInWindow())
	}
}

func TestGetCacheStats_BodyReadErrorIsUnavailable(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	id := f.cacheRun(t, "ml", "cached-pipeline", cacheNow.Add(-30*time.Minute), map[string]*orkstackv1.CacheResult{"featurize": cacheMiss})
	f.bodies.broken[id] = errors.New("bucket timeout")
	_, err := f.handler(0).GetCacheStats(context.Background(), connect.NewRequest(cacheStatsReq("")))
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), id) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetCacheStats_NoMemoStoreIsFailedPrecondition(t *testing.T) {
	f := newBreakdownFixture(t, cacheNow, []string{"ml"})
	h := api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, 0))
	_, err := h.GetCacheStats(context.Background(), connect.NewRequest(cacheStatsReq("")))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "cache memo store is not configured") {
		t.Fatalf("err = %v", err)
	}
	if _, err := h.PurgeCache(context.Background(), connect.NewRequest(&orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("purge err = %v", err)
	}
}

func TestGetCacheStats_NoArchive(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}, Memos: artifacts.NewMemoryMemoStore(),
		Namespaces: registry{"default": {Name: "default"}}})
	h := api.NewCacheHandler(svc, api.NewStatsHandler(svc, 0))
	_, err := h.GetCacheStats(context.Background(), connect.NewRequest(&orkstackv1.GetCacheStatsRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "no run archive") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetCacheStats_UnknownNamespaceIsNotFound(t *testing.T) {
	f := newCacheFixture(t)
	_, err := f.handler(0).GetCacheStats(context.Background(), connect.NewRequest(&orkstackv1.GetCacheStatsRequest{Namespace: "nope"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("err = %v", err)
	}
	_, err = f.handler(0).PurgeCache(context.Background(), connect.NewRequest(&orkstackv1.PurgeCacheRequest{Namespace: "nope", All: true}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("purge err = %v", err)
	}
}

func TestGetCacheStats_ServedFromTheCacheWithinTheTTL(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	h := f.handler(5 * time.Second)
	first := f.stats(t, h, cacheStatsReq(""))
	reads := f.bodies.totalReads()
	second := f.stats(t, h, cacheStatsReq(""))
	if f.bodies.totalReads() != reads || !second.GetComputedAt().AsTime().Equal(first.GetComputedAt().AsTime()) {
		t.Fatal("a second read within the TTL was recomputed")
	}
	f.clock.advance(10 * time.Second)
	f.stats(t, h, cacheStatsReq(""))
	if f.bodies.totalReads() == reads {
		t.Fatal("an expired answer was served")
	}
}

// ---------------------------------------------------------------- PurgeCache

func purge(t *testing.T, h *api.CacheHandler, req *orkstackv1.PurgeCacheRequest) (*orkstackv1.PurgeCacheResponse, error) {
	t.Helper()
	resp, err := h.PurgeCache(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestPurgeCache_Selectors(t *testing.T) {
	f := newCacheFixture(t)
	h := f.handler(0)
	for name, req := range map[string]*orkstackv1.PurgeCacheRequest{
		"none":          {Namespace: "ml"},
		"two":           {Namespace: "ml", OrkflowName: "p", All: true},
		"task with all": {Namespace: "ml", All: true, TaskId: "t"},
		"bad slug":      {Namespace: "ml", OrkflowName: "Not A Slug"},
		"bad task":      {Namespace: "ml", NamespaceScoped: true, TaskId: "../x"},
		"negative":      {Namespace: "ml", All: true, OlderThan: durationpb.New(-time.Hour)},
	} {
		if _, err := purge(t, h, req); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s: err = %v, want InvalidArgument", name, err)
		}
	}
	_, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml"})
	if !strings.Contains(err.Error(), "name exactly one of orkflow_name, namespace_scoped or all — a purge never has a default scope") {
		t.Fatalf("err = %v", err)
	}
	// An Orkflow that is not stored may still be purged.
	f.seed(t)
	resp, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", OrkflowName: "cached-pipeline", TaskId: "featurize"})
	if err != nil || resp.GetDeleted() != 2 || resp.GetNamespace() != "ml" {
		t.Fatalf("purge = %v, %v", resp, err)
	}
	resp, _ = purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", NamespaceScoped: true})
	if resp.GetDeleted() != 1 {
		t.Fatalf("namespace-scoped purge = %v", resp)
	}
	resp, _ = purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true})
	if resp.GetDeleted() != 1 || f.memos.Len() != 0 {
		t.Fatalf("all purge = %v, %d left", resp, f.memos.Len())
	}
}

func TestPurgeCache_OlderThanKeepsNewerMemos(t *testing.T) {
	f := newCacheFixture(t)
	f.memos.Now = func() time.Time { return cacheNow.Add(-100 * time.Hour) }
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", '1')
	f.memos.Now = func() time.Time { return cacheNow.Add(-time.Hour) }
	putTestMemo(t, f.memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", '2')
	resp, err := purge(t, f.handler(0), &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true, OlderThan: durationpb.New(72 * time.Hour)})
	if err != nil || resp.GetDeleted() != 1 || resp.GetKept() != 1 || resp.GetMore() {
		t.Fatalf("purge = %v, %v", resp, err)
	}
}

// deletingPaged is pagedMemos that counts deletions.
type deletingPaged struct {
	pagedMemos
	deleted *int
}

func (d deletingPaged) DeleteMemo(context.Context, artifacts.MemoRef) error {
	*d.deleted++
	return nil
}

func TestPurgeCache_StopsAtTheBoundWithMore(t *testing.T) {
	deleted := 0
	f := newBreakdownFixture(t, cacheNow, []string{"ml"}, withMemos(deletingPaged{pagedMemos{n: api.PurgeCacheMaxDeletes + 5}, &deleted}))
	h := api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, 0))
	resp, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true})
	if err != nil || resp.GetDeleted() != api.PurgeCacheMaxDeletes || !resp.GetMore() || deleted != api.PurgeCacheMaxDeletes {
		t.Fatalf("purge = %v, %v (deleted %d)", resp, err, deleted)
	}
}

func TestPurgeCache_TouchesNothingButMemos(t *testing.T) {
	dir := t.TempDir()
	memos, err := artifacts.NewLocalMemoStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer memos.Close()
	others := map[string]string{
		"metadata/commits/ml/ork-1/featurize/commit.json":   `{"schemaVersion":1}`,
		"metadata/attempts/ork-1/featurize/0000000001.json": `{}`,
		"metadata/runs/ork-1.json":                          `{}`,
		"metadata/logs/ork-1/featurize/x":                   `log`,
		"objects/ab/features.txt":                           `1\n2\n`,
	}
	for path, content := range others {
		full := filepath.Join(dir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	putTestMemo(t, memos, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", '1')
	putTestMemo(t, memos, orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, "p", "u", '2')
	f := newBreakdownFixture(t, cacheNow, []string{"ml"}, withMemos(memos))
	h := api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, 0))
	resp, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true})
	if err != nil || resp.GetDeleted() != 2 {
		t.Fatalf("purge = %v, %v", resp, err)
	}
	for path, content := range others {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(path)))
		if err != nil || string(got) != content {
			t.Fatalf("%s changed: %q, %v", path, got, err)
		}
	}
	left, _, _ := memos.ListMemos(context.Background(), artifacts.MemoScopeRef{Namespace: "ml"}, "", 10)
	if len(left) != 0 {
		t.Fatalf("memos left: %v", left)
	}
}

// failingMemos fails every delete after the first.
type failingMemos struct {
	*artifacts.MemoryMemoStore
	deletes int
}

func (f *failingMemos) DeleteMemo(ctx context.Context, ref artifacts.MemoRef) error {
	f.deletes++
	if f.deletes > 1 {
		return errors.New("bucket says no")
	}
	return f.MemoryMemoStore.DeleteMemo(ctx, ref)
}

func TestPurgeCache_StoreErrorIsUnavailable(t *testing.T) {
	store := &failingMemos{MemoryMemoStore: artifacts.NewMemoryMemoStore()}
	putTestMemo(t, store, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", '1')
	putTestMemo(t, store, orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", '2')
	f := newBreakdownFixture(t, cacheNow, []string{"ml"}, withMemos(store))
	h := api.NewCacheHandler(f.svc, api.NewStatsHandler(f.svc, 0))
	_, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true})
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "deleted 1 memos before") {
		t.Fatalf("err = %v", err)
	}
}

func TestPurgeCache_InvalidatesCachedStats(t *testing.T) {
	f := newCacheFixture(t)
	f.seed(t)
	h := f.handler(time.Hour)
	before := f.stats(t, h, cacheStatsReq(""))
	if len(before.GetInventory()) == 0 {
		t.Fatal("no inventory seeded")
	}
	if _, err := purge(t, h, &orkstackv1.PurgeCacheRequest{Namespace: "ml", All: true}); err != nil {
		t.Fatal(err)
	}
	after := f.stats(t, h, cacheStatsReq(""))
	if len(after.GetInventory()) != 0 {
		t.Fatalf("a purge was not visible to the next stats read here: %q", inventoryLines(after))
	}
}

// ---------------------------------------------------------------- audit and diff

func TestAudit_PurgeCacheIsRecorded(t *testing.T) {
	memos := artifacts.NewMemoryMemoStore()
	f := newAuditFixture(t, auditOptions{memos: memos})
	if err := memos.PutMemo(context.Background(), &orkstackv1.CacheMemo{
		SchemaVersion: 1, Key: strings.Repeat("a", 64), Namespace: "default",
		Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, OrkflowName: "cached-pipeline", TaskId: "featurize",
	}); err != nil {
		t.Fatal(err)
	}
	client := orkstackv1connect.NewCacheServiceClient(f.server.Client(), f.server.URL)
	if _, err := client.GetCacheStats(context.Background(), connect.NewRequest(&orkstackv1.GetCacheStatsRequest{})); err == nil {
		// no archive here: a read is never recorded either way
		t.Log("stats answered")
	}
	if records := f.records(t); len(records) != 0 {
		t.Fatalf("a read was recorded: %v", records)
	}
	if _, err := client.PurgeCache(context.Background(), connect.NewRequest(&orkstackv1.PurgeCacheRequest{
		OrkflowName: "cached-pipeline", TaskId: "featurize", OlderThan: durationpb.New(0)})); err != nil {
		t.Fatalf("PurgeCache: %v", err)
	}
	f.now = f.now.Add(time.Second)
	if _, err := client.PurgeCache(context.Background(), connect.NewRequest(&orkstackv1.PurgeCacheRequest{
		All: true, TaskId: "featurize", OlderThan: durationpb.New(72 * time.Hour)})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("refused purge = %v", err)
	}
	records := f.records(t)
	if len(records) != 2 {
		t.Fatalf("records = %v", records)
	}
	refused, done := records[0], records[1]
	if done.GetAction() != "cache.purge" || done.GetNamespace() != "default" || done.GetTarget() != "orkflow/cached-pipeline/featurize" ||
		done.GetDetail() != "older_than=- deleted=1 kept=0 more=false" || !done.GetSucceeded() {
		t.Fatalf("purge record = %v", done)
	}
	if refused.GetAction() != "cache.purge" || refused.GetSucceeded() || refused.GetTarget() != "all/featurize" ||
		refused.GetDetail() != "older_than=72h0m0s" {
		t.Fatalf("refused record = %v", refused)
	}
}

func TestDiffRuns_ComparesCached(t *testing.T) {
	withCache := func(c *orkstackv1.CacheResult) func(*orkstackv1.TaskRun) {
		return func(task *orkstackv1.TaskRun) { task.Cache = c }
	}
	a := diffRun("ork-cache-a", nil,
		diffTask("featurize", withCache(cacheMiss)),
		diffTask("train", withCache(&orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED})),
		diffTask("plain"))
	b := diffRun("ork-cache-b", nil,
		diffTask("featurize", withCache(cacheHit("ork-cache-a", time.Minute))),
		diffTask("train", withCache(cacheMiss)),
		diffTask("plain"))
	f := newDiffFixture(t, []*orkstackv1.Run{a, b})
	res := f.mustDiff(t, a.GetId(), b.GetId(), "cached")
	requireRows(t, res,
		"featurize cached miss | hit ork-cache-a",
		"train cached bypassed | miss",
	)
}

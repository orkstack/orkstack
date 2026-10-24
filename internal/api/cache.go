package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Task caching on the control plane (docs/DSL.md "Caching"): the submit-time
// cache plan, the apply warnings, and the CacheService pair — a stats read
// derived on read from the run archive and the memo listing, and a purge that
// deletes memos and nothing else.

const (
	// CacheStatsMaxScan bounds the finished runs' bodies one cache read opens,
	// FlakeIndexMaxScan's bound. A larger population is refused, never sampled.
	CacheStatsMaxScan = 5000
	// CacheStatsMaxMemos bounds the memo keys one cache read lists.
	CacheStatsMaxMemos = 100000
	// PurgeCacheMaxDeletes bounds one PurgeCache call; more: true says call
	// again.
	PurgeCacheMaxDeletes = 10000
	// cacheListPage is how many memo keys one listing call returns.
	cacheListPage = 1000
)

// cacheSlugPattern is the slug rule every boundary applies to an Orkflow
// name and a task id.
var cacheSlugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// cacheMaxTTLOf is Config.CacheMaxTTL, defaulted.
func cacheMaxTTLOf(configured time.Duration) time.Duration {
	if configured <= 0 {
		return engine.DefaultCacheMaxTTL
	}
	return configured
}

// cachePlanFor is nil when wf declares no cached task (dsl.DeclaresCache),
// else {MaxTTLSeconds: whole seconds of s.cacheMaxTTL, Refresh: noCache}.
func (s *Service) cachePlanFor(wf *orkstackv1.Orkflow, noCache bool) *engine.CachePlan {
	if !dsl.DeclaresCache(wf) {
		return nil
	}
	seconds := int64(cacheMaxTTLOf(s.cacheMaxTTL) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return &engine.CachePlan{MaxTTLSeconds: seconds, Refresh: noCache}
}

// humanTTL renders a duration the way the configuration reference writes
// one: 168h, 90m, 30s.
func humanTTL(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return d.String()
	}
}

// cacheIssues is caching's apply WARNINGs — never an ERROR, so apply never
// refuses on them: a TTL the deployment's cap overrides, and a cached task
// with secrets (rotating one does not invalidate its memos).
func cacheIssues(wf *orkstackv1.Orkflow, capTTL time.Duration) []*orkstackv1.ValidationIssue {
	capTTL = cacheMaxTTLOf(capTTL)
	capSeconds := int64(capTTL / time.Second)
	var issues []*orkstackv1.ValidationIssue
	warning := orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING
	for i, t := range wf.GetTasks() {
		spec := t.GetCache()
		if spec == nil {
			continue
		}
		switch ttl := int64(spec.GetTtlSeconds()); {
		case ttl == 0:
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].cache.ttl_seconds", i), fmt.Sprintf(
				"task %q: cache.ttl_seconds is not set, so memos are reused for at most %s, the deployment's cap "+
					"(ORK_CACHE_MAX_TTL)", t.GetId(), humanTTL(capTTL)), warning))
		case ttl > capSeconds:
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].cache.ttl_seconds", i), fmt.Sprintf(
				"task %q: memos are reused for at most %s (ORK_CACHE_MAX_TTL); a longer ttl_seconds has no effect",
				t.GetId(), humanTTL(capTTL)), warning))
		}
		if len(t.GetSecrets()) > 0 {
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].secrets", i), fmt.Sprintf(
				"task %q: a cached task's key covers its secrets' names and sources, never their values — "+
					"rotating a secret does not invalidate its memos; purge them with orkctl cache purge "+
					"--orkflow %s --task %s", t.GetId(), wf.GetName(), t.GetId()), warning))
		}
	}
	return issues
}

// CacheHandler serves CacheService. It shares the StatsHandler's window
// resolver and response-cache discipline; it adds no second TTL knob.
type CacheHandler struct {
	svc   *Service
	stats *StatsHandler
}

// NewCacheHandler builds the CacheService over svc, caching its stats answers
// on stats's terms.
func NewCacheHandler(svc *Service, stats *StatsHandler) *CacheHandler {
	return &CacheHandler{svc: svc, stats: stats}
}

// cacheStatsKey identifies one GetCacheStats answer: the window AS SENT
// (statsKey's rule) and the orkflow.
type cacheStatsKey struct {
	statsKey
	orkflow string
}

type cacheStatsEntry struct {
	response *orkstackv1.GetCacheStatsResponse
	expires  time.Time
}

// GetCacheStats reports one namespace's memo inventory and how its cached
// tasks fared over one window of finished runs, in GetFlakeIndex's order:
// namespace, window, validation, preconditions, then the response cache.
func (h *CacheHandler) GetCacheStats(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetCacheStatsRequest],
) (*connect.Response[orkstackv1.GetCacheStatsResponse], error) {
	msg := req.Msg
	ns, err := h.svc.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	window, err := h.stats.windowOf(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback())
	if err != nil {
		return nil, err
	}
	if name := msg.GetOrkflowName(); name != "" {
		if err := checkCacheSlug("orkflow_name", name); err != nil {
			return nil, err
		}
	}
	if h.svc.memos == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("cache memo store is not configured"))
	}
	lister, ok := h.svc.archive.(FinishedRunIndexLister)
	if !ok || h.svc.archive == nil {
		return nil, errNoArchive()
	}

	key := cacheStatsKey{
		statsKey: windowKeyFor(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback()),
		orkflow:  msg.GetOrkflowName(),
	}
	if cached, ok := h.stats.lookupCacheStats(key); ok {
		return connect.NewResponse(cached), nil
	}

	inventory, err := h.inventory(ctx, ns.GetName(), msg.GetOrkflowName())
	if err != nil {
		return nil, err
	}
	entries, err := lister.ListFinishedRunIndex(ctx, engine.ArchiveQuery{
		Namespace:   window.Namespace,
		OrkflowName: msg.GetOrkflowName(),
		Status:      orkstackv1.Status_STATUS_UNSPECIFIED,
		Since:       window.Since,
		Until:       window.Until,
	})
	switch {
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errNoArchive()
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list finished runs in namespace %q: %w", window.Namespace, err))
	}
	if len(entries) > CacheStatsMaxScan {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"cache stats for namespace %q would read %d finished runs' bodies, more than the %d this read "+
				"is bounded to; narrow the window with since/until or name one orkflow",
			window.Namespace, len(entries), CacheStatsMaxScan))
	}
	runs, err := h.loadRuns(ctx, entries)
	if err != nil {
		return nil, err
	}
	computed := &orkstackv1.GetCacheStatsResponse{
		Usage:        engine.AggregateCacheUsage(runs),
		Inventory:    engine.GroupMemoInventory(inventory),
		RunsInWindow: int64(len(entries)),
		RunsScanned:  int64(len(runs)),
		Namespace:    window.Namespace,
		WindowSince:  timestamppb.New(window.Since),
		WindowUntil:  timestamppb.New(window.Until),
		ComputedAt:   timestamppb.New(h.stats.now()),
	}
	h.stats.storeCacheStats(key, computed)
	return connect.NewResponse(proto.Clone(computed).(*orkstackv1.GetCacheStatsResponse)), nil
}

// checkCacheSlug holds an Orkflow name or task id to the slug rule every
// boundary applies.
func checkCacheSlug(field, value string) error {
	if len(value) > 200 || !cacheSlugPattern.MatchString(value) {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"%s %q must match %s (at most 200 bytes)", field, value, cacheSlugPattern))
	}
	return nil
}

// inventory lists the namespace's memos (one Orkflow's orkflow-scoped memos
// when orkflow is set), paging cacheListPage at a time, bounded by
// CacheStatsMaxMemos.
func (h *CacheHandler) inventory(ctx context.Context, namespace, orkflow string) ([]artifacts.MemoEntry, error) {
	scope := artifacts.MemoScopeRef{Namespace: namespace, Orkflow: orkflow}
	var all []artifacts.MemoEntry
	after := ""
	for {
		entries, cursor, err := h.svc.memos.ListMemos(ctx, scope, after, cacheListPage)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("list the memos of namespace %q: %w", namespace, err))
		}
		all = append(all, entries...)
		if len(all) > CacheStatsMaxMemos {
			return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
				"namespace %q holds more than %d memos, more than this read lists; name one orkflow, "+
					"or purge old memos with orkctl cache purge --all --older-than %s",
				namespace, CacheStatsMaxMemos, humanTTL(engine.DefaultCacheMaxTTL)))
		}
		if cursor == "" {
			return all, nil
		}
		after = cursor
	}
}

// loadRuns reads every entry's archived body through a bounded worker pool
// of failureScanConcurrency, in index order. An index row with no body is the
// crash window between the archive's two writes and is skipped; any other
// error fails the whole answer, naming the run — a subset is never reported
// as the whole.
func (h *CacheHandler) loadRuns(parent context.Context, entries []artifacts.RunIndexEntry) ([]*orkstackv1.Run, error) {
	ctx, stop := context.WithCancel(parent)
	defer stop()
	type result struct {
		run *orkstackv1.Run
		err error
	}
	results := make([]result, len(entries))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, failureScanConcurrency)
	)
	for i, entry := range entries {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, entry artifacts.RunIndexEntry) {
			defer wg.Done()
			defer func() { <-slots }()
			run, err := h.svc.archive.LoadRun(ctx, entry.RunID)
			switch {
			case errors.Is(err, artifacts.ErrNotFound):
				return
			case err != nil:
				results[i] = result{err: connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("read the archived body of run %q: %w", entry.RunID, err))}
				stop()
				return
			}
			results[i] = result{run: run}
		}(i, entry)
	}
	wg.Wait()
	if err := parent.Err(); err != nil {
		return nil, connect.NewError(connect.CodeCanceled, err)
	}
	var casualty error
	for _, r := range results {
		if r.err == nil {
			continue
		}
		if errors.Is(r.err, context.Canceled) {
			if casualty == nil {
				casualty = r.err
			}
			continue
		}
		return nil, r.err
	}
	if casualty != nil {
		return nil, casualty
	}
	runs := make([]*orkstackv1.Run, 0, len(entries))
	for _, r := range results {
		if r.run != nil {
			runs = append(runs, r.run)
		}
	}
	return runs, nil
}

// PurgeCache deletes memos — never a commit, run, attempt, log or artifact.
func (h *CacheHandler) PurgeCache(
	ctx context.Context,
	req *connect.Request[orkstackv1.PurgeCacheRequest],
) (*connect.Response[orkstackv1.PurgeCacheResponse], error) {
	msg := req.Msg
	ns, err := h.svc.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	scope, err := purgeScope(ns.GetName(), msg)
	if err != nil {
		return nil, err
	}
	if h.svc.memos == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("cache memo store is not configured"))
	}
	var olderThan time.Duration
	if msg.GetOlderThan() != nil {
		olderThan = msg.GetOlderThan().AsDuration()
	}
	now := h.stats.now()
	response := &orkstackv1.PurgeCacheResponse{Namespace: ns.GetName()}
	defer h.stats.clearCacheStats(ns.GetName())
	after := ""
	for {
		entries, cursor, err := h.svc.memos.ListMemos(ctx, scope, after, cacheListPage)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("deleted %d memos before: %w", response.GetDeleted(), err))
		}
		for _, entry := range entries {
			if olderThan > 0 && now.Sub(entry.LastModified) < olderThan {
				response.Kept++
				continue
			}
			if response.GetDeleted() >= PurgeCacheMaxDeletes {
				response.More = true
				return connect.NewResponse(response), nil
			}
			if err := h.svc.memos.DeleteMemo(ctx, entry.Ref); err != nil {
				return nil, connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("deleted %d memos before: %w", response.GetDeleted(), err))
			}
			response.Deleted++
		}
		if cursor == "" {
			return connect.NewResponse(response), nil
		}
		after = cursor
	}
}

// purgeScope validates a PurgeCacheRequest's selector — exactly one of
// orkflow_name, namespace_scoped and all; task_id only with the first two;
// older_than not negative — and returns the memo family it names.
func purgeScope(namespace string, msg *orkstackv1.PurgeCacheRequest) (artifacts.MemoScopeRef, error) {
	selectors := 0
	if msg.GetOrkflowName() != "" {
		selectors++
	}
	if msg.GetNamespaceScoped() {
		selectors++
	}
	if msg.GetAll() {
		selectors++
	}
	if selectors != 1 {
		return artifacts.MemoScopeRef{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"name exactly one of orkflow_name, namespace_scoped or all — a purge never has a default scope"))
	}
	if msg.GetAll() && msg.GetTaskId() != "" {
		return artifacts.MemoScopeRef{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"task_id narrows orkflow_name or namespace_scoped; it does not apply to all"))
	}
	if d := msg.GetOlderThan(); d != nil && d.AsDuration() < 0 {
		return artifacts.MemoScopeRef{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"older_than %s is negative", d.AsDuration()))
	}
	if name := msg.GetOrkflowName(); name != "" {
		if err := checkCacheSlug("orkflow_name", name); err != nil {
			return artifacts.MemoScopeRef{}, err
		}
	}
	if id := msg.GetTaskId(); id != "" {
		if err := checkCacheSlug("task_id", id); err != nil {
			return artifacts.MemoScopeRef{}, err
		}
	}
	return artifacts.MemoScopeRef{
		Namespace:       namespace,
		Orkflow:         msg.GetOrkflowName(),
		NamespaceScoped: msg.GetNamespaceScoped(),
		TaskID:          msg.GetTaskId(),
	}, nil
}

// lookupCacheStats returns a cached GetCacheStats answer.
func (h *StatsHandler) lookupCacheStats(key cacheStatsKey) (*orkstackv1.GetCacheStatsResponse, bool) {
	if h.ttl <= 0 {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.cacheStats[key]
	if !ok || !h.now().Before(entry.expires) {
		return nil, false
	}
	return proto.Clone(entry.response).(*orkstackv1.GetCacheStatsResponse), true
}

// storeCacheStats keeps one whole answer, on storeFlakes' terms.
func (h *StatsHandler) storeCacheStats(key cacheStatsKey, response *orkstackv1.GetCacheStatsResponse) {
	if h.ttl <= 0 {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for existing, entry := range h.cacheStats {
		if !now.Before(entry.expires) {
			delete(h.cacheStats, existing)
		}
	}
	if len(h.cacheStats) >= statsCacheMaxEntries {
		clear(h.cacheStats)
	}
	h.cacheStats[key] = cacheStatsEntry{
		response: proto.Clone(response).(*orkstackv1.GetCacheStatsResponse),
		expires:  now.Add(h.ttl),
	}
}

// clearCacheStats drops this replica's cached answers for one namespace, so a
// purge is visible to the next stats read here. Other replicas' answers carry
// their computed_at.
func (h *StatsHandler) clearCacheStats(namespace string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key := range h.cacheStats {
		if key.namespace == namespace {
			delete(h.cacheStats, key)
		}
	}
}

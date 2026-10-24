package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// DefaultStatsCacheTTL is how long one computed answer is reused.
	//
	// Five seconds is chosen against the thing the cache exists for: a
	// dashboard open in several tabs, refreshing on a timer. At one refresh a
	// second across ten tabs the archive index would be scanned ten times a
	// second for an answer that cannot have moved much; at five seconds it is
	// scanned once. It is short enough that nobody watches a stale number
	// wondering why it will not move, and every answer carries computed_at, so
	// how old it is never has to be guessed.
	DefaultStatsCacheTTL = 5 * time.Second

	// StatsCacheTTLVar overrides that. Zero or negative switches the cache
	// off, which is what a correctness investigation wants; an unparsable
	// value falls back to the default rather than refusing to boot.
	StatsCacheTTLVar = "ORK_STATS_CACHE_TTL"

	// DefaultStatsWindow is how far back a request that names no window looks.
	DefaultStatsWindow = 24 * time.Hour

	// StatsDurationRetention is how long the cluster keeps an execution's
	// record, and therefore how far back a duration can be known. It mirrors
	// the retention this deployment registers for its own namespaces
	// (internal/task/namespace.go); a shorter real retention only makes
	// duration_coverage_since optimistic by the difference, which is why the
	// coverage is reported rather than assumed.
	//
	// Retention is counted from when a run CLOSED, and a run closes no earlier
	// than it was created, so every run created inside the last
	// StatsDurationRetention that has finished is still in the index. That is
	// what makes the coverage instant below a sound bound on a population
	// selected by creation time.
	StatsDurationRetention = 72 * time.Hour

	// MostFailingLimit caps the most-failing table. A dashboard row a reader
	// scrolls past is not a signal.
	MostFailingLimit = 5

	// statsCacheMaxEntries bounds the cache. One entry per distinct window per
	// namespace is a handful in every real deployment; the bound only stops a
	// client that walks windows from growing the map without end.
	statsCacheMaxEntries = 128
)

// RunIndexTallier is the archive's keys-only half: how many runs finished,
// under which orkflow, ending how — every field read out of an index KEY.
//
// It is a separate, narrower interface from RunArchive on purpose, and the
// narrowness is the point. RunArchive can also fetch a run's BODY, and
// fetching bodies is exactly what a stats answer must never do: the cost of a
// dashboard would then scale with how much output a namespace's tasks
// produced. The stats handler holds a value whose only method is a keys-only
// tally, so "no aggregate is computed from a run body" is a property of the
// type rather than a habit of the code.
type RunIndexTallier interface {
	TallyRuns(ctx context.Context, q engine.ArchiveQuery) (engine.RunTally, error)
}

// RunIndexLister is the archive's keys-only listing: the index rows a
// query matches, every field read out of the KEY. It is how the failure
// breakdown learns WHICH runs failed before it reads any of them.
//
// Implementations answer the FAILED rows of the query's namespace, orkflow and
// window, and every one of them: the query's Status and Limit are not
// honoured, because a population narrowed to another status or stopped at a
// page is not the population (engine.RunArchiver.ListRunIndex).
type RunIndexLister interface {
	ListRunIndex(ctx context.Context, q engine.ArchiveQuery) ([]artifacts.RunIndexEntry, error)
}

// StatsCacheTTLFromEnv reads StatsCacheTTLVar, falling back to
// DefaultStatsCacheTTL. An explicit zero or negative value switches the cache
// off; an unparsable one is ignored, because a typo in an optimisation's knob
// is not a reason to refuse to serve.
func StatsCacheTTLFromEnv() time.Duration {
	raw, ok := os.LookupEnv(StatsCacheTTLVar)
	if !ok || raw == "" {
		return DefaultStatsCacheTTL
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return DefaultStatsCacheTTL
	}
	return parsed
}

// statsKey identifies one answer: a namespace and the window AS REQUESTED.
//
// The window is keyed as it arrived, not as it resolved, and that is what
// makes the cache useful at all. A dashboard polls with an empty window and
// means "the last day, now" — resolved, that is a different pair of instants
// every second and no two requests would ever share a key. Keyed as sent, the
// poll hits, and what it gets is the answer to its own question as of
// computed_at.
//
// A request that PINS both bounds is keyed by those exact instants, so what it
// gets back is the same answer to the same question, not a nearby one. The
// cache never widens, narrows or rounds a window a caller stated.
// Each bound carries whether it was SENT as well as what it was. A bare
// integer cannot tell an absent timestamp from the epoch, and the two resolve
// to different windows — one to "a day before now", the other to 1970 — so a
// key that folded them together would answer one with the other.
type statsKey struct {
	namespace string
	since     int64
	sinceSet  bool
	until     int64
	untilSet  bool
	// lookback is part of the key for the same reason the other two are: it is
	// half of what was asked. "The last hour" and "the last day" are two
	// questions, and a key that dropped the duration would answer the second
	// with the first.
	lookback    int64
	lookbackSet bool
}

// statsKeyFor is the key of one request, in one place, so the lookup and the
// store can never disagree about what was asked.
func statsKeyFor(namespace string, req *orkstackv1.GetStatsRequest) statsKey {
	return windowKeyFor(namespace, req.GetSince(), req.GetUntil(), req.GetLookback())
}

// windowKeyFor is the namespace-and-window half of a key, as sent. Both the
// stats cache and the failure-breakdown cache key their window through it.
func windowKeyFor(
	namespace string,
	since, until *timestamppb.Timestamp,
	lookback *durationpb.Duration,
) statsKey {
	key := statsKey{namespace: namespace}
	if since != nil {
		key.since, key.sinceSet = since.AsTime().UnixNano(), true
	}
	if until != nil {
		key.until, key.untilSet = until.AsTime().UnixNano(), true
	}
	if lookback != nil {
		key.lookback, key.lookbackSet = int64(lookback.AsDuration()), true
	}
	return key
}

type statsEntry struct {
	response *orkstackv1.GetStatsResponse
	expires  time.Time
}

// StatsHandler serves StatsService.
//
// It is its own type rather than another face of Service because it owns
// something Service deliberately does not: state with a lifetime. Every other
// read this server answers is stateless — it leaves the process and comes
// back — and putting a cache on Service would invite the next read to reach
// for it. This one is a cache over an answer that is derived, whole, from two
// shared records, so a second replica with a cold cache computes the identical
// numbers.
type StatsHandler struct {
	svc *Service
	// ttl is taken verbatim. Zero or negative means no cache: the caller
	// resolved the default already (see NewMux), so nothing here can quietly
	// re-enable an optimisation an operator switched off.
	ttl time.Duration

	mu    sync.Mutex
	cache map[statsKey]statsEntry
	// breakdowns is GetFailureBreakdown's cache, under the same mu, TTL and
	// bound: one whole answer per question, never a merge.
	breakdowns map[breakdownKey]breakdownEntry
	// flakes is GetFlakeIndex's cache, on the same terms.
	flakes map[flakeKey]flakeEntry
	// cacheStats is CacheService.GetCacheStats' cache, on the same terms.
	cacheStats map[cacheStatsKey]cacheStatsEntry
}

// NewStatsHandler returns the stats surface over a service. ttl is used as
// given; NewMux is where an unset one becomes the documented default.
func NewStatsHandler(svc *Service, ttl time.Duration) *StatsHandler {
	return &StatsHandler{
		svc:        svc,
		ttl:        ttl,
		cache:      map[statsKey]statsEntry{},
		breakdowns: map[breakdownKey]breakdownEntry{},
		flakes:     map[flakeKey]flakeEntry{},
		cacheStats: map[cacheStatsKey]cacheStatsEntry{},
	}
}

// GetStats reports one namespace's runs over one window.
//
// Both sources are shared: the cluster's execution index and the run archive's
// index keys. Nothing is read from this process's memory, so a replica that
// started nothing answers exactly as one that started everything (AGENTS.md
// §1.7).
func (h *StatsHandler) GetStats(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetStatsRequest],
) (*connect.Response[orkstackv1.GetStatsResponse], error) {
	// The namespace is resolved before anything is cached, so a name that does
	// not exist stays a NotFound however warm the cache is.
	ns, err := h.svc.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	// The window is resolved — and therefore VALIDATED — before the cache is
	// consulted, for the same reason. A cache that answered first would let a
	// refused window collect the previous caller's numbers: the two requests
	// differ in a field the answer does not carry, and one of them is not a
	// question.
	window, err := h.windowOf(ns.GetName(),
		req.Msg.GetSince(), req.Msg.GetUntil(), req.Msg.GetLookback())
	if err != nil {
		return nil, err
	}
	key := statsKeyFor(ns.GetName(), req.Msg)
	if cached, ok := h.lookup(key); ok {
		return connect.NewResponse(cached), nil
	}
	computed, err := h.compute(ctx, ns, window)
	if err != nil {
		return nil, err
	}
	h.store(key, computed)
	return connect.NewResponse(cloneStats(computed)), nil
}

// windowOf resolves a request's bounds against ONE clock: this server's.
//
// An empty `until` is now and an empty `since` is `lookback` before the window
// ends, so the ordinary request — "how are we doing?" — needs no arguments,
// and a relative one needs only a duration. A window that ends at or before it
// starts is refused: answering it with zeros would hand back a set of numbers
// that read exactly like a healthy, idle namespace.
//
// It takes the three fields rather than a request because two RPCs carry them
// — GetStats and GetFailureBreakdown — and they must not drift on what a
// window means: the breakdown's population is GetStats' failed count, and the
// two only reconcile if they are measured over the same instants.
func (h *StatsHandler) windowOf(
	namespace string,
	sinceTS, untilTS *timestamppb.Timestamp,
	lookbackD *durationpb.Duration,
) (engine.StatsWindow, error) {
	now := h.now()
	until := now
	if untilTS != nil {
		until = untilTS.AsTime().UTC()
	}
	// The relative form, resolved against the SAME clock that fixed the end of
	// the window. A negative lookback is refused below with everything else
	// that makes a window empty.
	lookback := DefaultStatsWindow
	if lookbackD != nil {
		lookback = lookbackD.AsDuration()
	}
	since := until.Add(-lookback)
	if sinceTS != nil {
		since = sinceTS.AsTime().UTC()
	}
	if !until.After(since) {
		return engine.StatsWindow{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"stats window ends at or before it starts (since %s, until %s); "+
				"a window with no time in it has no runs in it either",
			since.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano)))
	}
	return engine.StatsWindow{Namespace: namespace, Since: since, Until: until}, nil
}

// compute derives one whole answer from the two shared records.
//
// The two records answer about ONE population: the runs the window created.
// The archive is asked for the ones that ended (its index key is stamped with
// a run's creation time, engine.RunIndexTime) and the cluster for the ones
// still going and for how long the ended ones took
// (engine.windowRunDurations). A handler that merged a count of the window's
// runs with a percentile over the window's CLOSURES would produce one screen
// holding two different sets of runs, and nothing on it would say so.
func (h *StatsHandler) compute(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	window engine.StatsWindow,
) (*orkstackv1.GetStatsResponse, error) {
	binding, err := h.svc.binding(ns.GetName())
	if err != nil {
		return nil, err
	}
	if binding.Reader == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"this engine has no read model wired, so live runs could only be counted "+
				"from its own memory — which would make every replica answer differently"))
	}
	// The seam is asked for, not assumed. An archive that cannot tally is an
	// engine that cannot count a finished run, and a stats answer over live
	// runs alone is not one.
	tallier, ok := h.svc.archive.(RunIndexTallier)
	if !ok || h.svc.archive == nil {
		return nil, errNoArchive()
	}

	tally, err := tallier.TallyRuns(ctx, engine.ArchiveQuery{
		Namespace: window.Namespace,
		Since:     window.Since,
		Until:     window.Until,
	})
	switch {
	// A tallier that exists as a type but holds no store answers this rather
	// than a total of zero. The two render identically and mean the opposite,
	// and zero is the one a reader believes.
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errNoArchive()
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("count finished runs in namespace %q: %w", window.Namespace, err))
	}
	executions, err := binding.Reader.Executions(ctx, window)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read the execution index for namespace %q: %w", window.Namespace, err))
	}
	pools, err := h.pools(ctx, ns, binding)
	if err != nil {
		return nil, err
	}

	now := h.now()
	response := &orkstackv1.GetStatsResponse{
		RunsByStatus: statusCounts(tally, len(executions.Running)),
		MostFailing:  mostFailing(tally),
		Running:      executions.Running,
		Pools:        pools,
		ComputedAt:   timestamppb.New(now),
		Namespace:    window.Namespace,
		WindowSince:  timestamppb.New(window.Since),
		WindowUntil:  timestamppb.New(window.Until),
	}
	if finished := tally.Finished(); finished > 0 {
		response.SuccessRate = float64(tally.Succeeded()) / float64(finished)
		response.SuccessRateDefined = true
	}
	response.DurationSampleCount = int64(len(executions.Durations))
	response.DurationCoverageSince = timestamppb.New(
		durationCoverageSince(window.Since, now))
	if len(executions.Durations) > 0 {
		response.P50Duration = durationpb.New(
			engine.PercentileDuration(executions.Durations, 50))
		response.P95Duration = durationpb.New(
			engine.PercentileDuration(executions.Durations, 95))
	}
	return response, nil
}

// pools is the same report ListPools gives, degrading the same way.
//
// The two halves come from two places — which pools are in scope (the
// namespace document, plus the pools its stored orkflows name) and who is
// serving them (the cluster) — and only the first needs the orkflow store. A
// store that cannot be read narrows the report to the namespace's own pools
// and says so in the log, because pool health during a bucket wobble is
// precisely when somebody is looking at this page.
func (h *StatsHandler) pools(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	binding engine.Binding,
) ([]*orkstackv1.Pool, error) {
	named, err := h.svc.poolsNamedByOrkflows(ctx, ns)
	if err != nil {
		h.svc.logger.Warn("stats: the orkflow store could not be read, so only the "+
			"namespace's own pools are reported",
			"namespace", ns.GetName(), "error", err)
		named = nil
	}
	pools, err := h.svc.resolverFor(binding).Pools(ctx, ns, named)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("report pool health for namespace %q: %w", ns.GetName(), err))
	}
	return pools, nil
}

// statusCounts merges the archive's finished counts with the cluster's live
// one, and fills in an explicit zero for every status either could report.
//
// The zeros matter. A dashboard that renders a missing key as a blank tells a
// reader nothing happened AND that nothing was measured, and those are
// different facts.
func statusCounts(tally engine.RunTally, running int) map[string]int64 {
	counts := map[string]int64{
		engine.StatusKeyRunning:   int64(running),
		engine.StatusKeySucceeded: 0,
		engine.StatusKeyFailed:    0,
		engine.StatusKeyCanceled:  0,
	}
	for status, count := range tally.ByStatus {
		if status == engine.StatusKeyRunning {
			// Nothing finished is running. An archive key that spells it is
			// not a live run and must not be added to the live count.
			continue
		}
		counts[status] += count
	}
	return counts
}

// mostFailing is the worst orkflows of the window, worst first.
//
// Ordering is total and carries no clock: failures desc, then finished desc,
// then name. Two replicas therefore produce the same table from the same keys
// — an order that fell back on map iteration would make "identical numbers"
// depend on which replica was asked.
func mostFailing(tally engine.RunTally) []*orkstackv1.OrkflowFailures {
	rows := make([]*orkstackv1.OrkflowFailures, 0, len(tally.ByOrkflow))
	for name, per := range tally.ByOrkflow {
		if per.Failed == 0 {
			continue
		}
		rows = append(rows, &orkstackv1.OrkflowFailures{
			OrkflowName: name,
			Failed:      per.Failed,
			Total:       per.Total,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].GetFailed() != rows[j].GetFailed() {
			return rows[i].GetFailed() > rows[j].GetFailed()
		}
		if rows[i].GetTotal() != rows[j].GetTotal() {
			return rows[i].GetTotal() > rows[j].GetTotal()
		}
		return rows[i].GetOrkflowName() < rows[j].GetOrkflowName()
	})
	if len(rows) > MostFailingLimit {
		rows = rows[:MostFailingLimit]
	}
	return rows
}

// errNoArchive is the one answer for every way an engine can turn out to have
// no run archive. It names the variable that configures one, because a caller
// who cannot act on an error is left exactly where they were.
func errNoArchive() error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"this engine has no run archive configured (ORK_ARTIFACT_BACKEND), so no "+
			"finished run can be counted; a success rate over live runs alone is not one"))
}

// durationCoverageSince is the instant the percentiles actually cover: the
// window's start, or the edge of what the cluster still remembers, whichever
// is later.
//
// It is the one place the answer's two sources may legitimately disagree about
// the one population they share. The archive counts a run created inside the
// window for ever; the cluster can sample it only while it holds its execution
// record. Before this instant a window's counts are complete and its
// percentiles are not, and saying so is the difference between three days of
// latency and a month of it.
func durationCoverageSince(since, now time.Time) time.Time {
	edge := now.Add(-StatsDurationRetention)
	if edge.After(since) {
		return edge
	}
	return since
}

// lookup returns a cached answer, or reports that there is none.
func (h *StatsHandler) lookup(key statsKey) (*orkstackv1.GetStatsResponse, bool) {
	if h.ttl <= 0 {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.cache[key]
	if !ok || !h.now().Before(entry.expires) {
		return nil, false
	}
	// A clone, so a caller that edits what it was handed cannot edit what the
	// next caller is handed.
	return cloneStats(entry.response), true
}

// store keeps one whole answer.
//
// One WHOLE answer: the cache holds a computed GetStatsResponse and hands it
// back verbatim or not at all. It never merges a fresh half into a stale one,
// which is the only way a cache could ever produce a set of numbers no
// computation ever produced.
func (h *StatsHandler) store(key statsKey, response *orkstackv1.GetStatsResponse) {
	if h.ttl <= 0 {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for existing, entry := range h.cache {
		if !now.Before(entry.expires) {
			delete(h.cache, existing)
		}
	}
	if len(h.cache) >= statsCacheMaxEntries {
		// Every live entry is at most one TTL from being recomputed anyway, so
		// dropping them costs one scan each and bounds the map for good.
		clear(h.cache)
	}
	h.cache[key] = statsEntry{
		response: cloneStats(response),
		expires:  now.Add(h.ttl),
	}
}

func (h *StatsHandler) now() time.Time { return h.svc.now().UTC() }

func cloneStats(response *orkstackv1.GetStatsResponse) *orkstackv1.GetStatsResponse {
	return proto.Clone(response).(*orkstackv1.GetStatsResponse)
}

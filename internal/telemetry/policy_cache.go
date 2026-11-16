package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The engine's policy cache: which sink each namespace's records go to, for
// the namespaced records no run carries a selection for (a continuation, a
// webhook delivery, a schedule fire). A run-bound record goes to the sink its
// run pinned at submit and never asks the cache; the Orker has no cache at
// all (docs/DEPLOY.md, "Where a namespace's records go").
//
// The cache never falls back to the default sink. A record of a namespace it
// cannot route is dropped and counted on the default sink
// (RecordsDroppedMetric): unrouted before the first load or for a namespace
// the registry does not hold, pending while that namespace's one lookup runs,
// sink_unknown for a sink this process does not declare.
const (
	// TelemetryNamespacesMetric counts the cached namespaces by routing.
	TelemetryNamespacesMetric = "ork.telemetry.namespaces"
	// AttrRouting labels TelemetryNamespacesMetric.
	AttrRouting = "routing"

	// RoutingDefault is a namespace whose records go to the default sink.
	RoutingDefault = "default"
	// RoutingNamed is a namespace on a named sink that exports metrics.
	RoutingNamed = "named"
	// RoutingNamedWithoutMetrics is a namespace on a named sink that exports
	// no metrics: its metrics reach no one, by its own policy.
	RoutingNamedWithoutMetrics = "named_without_metrics"
	// RoutingSinkUnknown is a namespace naming a sink this process does not
	// declare: its records are dropped and counted.
	RoutingSinkUnknown = "sink_unknown"

	// DropReasonPending is a record of a namespace the cache does not hold
	// yet, dropped while its lookup runs. It is never alerted on: a namespace
	// an operator just created is pending for a moment.
	DropReasonPending = "pending"
	// DropReasonUnrouted is a record the cache cannot route: it has not
	// loaded yet, or the registry does not hold the namespace.
	DropReasonUnrouted = "unrouted"
	// DropReasonLookupFailed is a record of a namespace whose lookup could
	// not read the registry, until a later lookup or refresh can.
	DropReasonLookupFailed = "lookup_failed"

	// PolicyLoadBound bounds the first load, before the engine serves.
	PolicyLoadBound = 30 * time.Second
	// policyLookupTimeout bounds one namespace's lookup.
	policyLookupTimeout = 10 * time.Second
	// policyLookupRetry is how long a namespace whose lookup failed waits
	// before a miss looks it up again.
	policyLookupRetry = 5 * time.Second
)

// NamespacePolicy is what the cache holds of one namespace.
type NamespacePolicy struct {
	Sink     string
	TaskLogs orkstackv1.TaskLogExport
}

// PolicySource is the namespace registry the cache reads. List reads every
// namespace (the first load); Names lists their names alone, and Get reads
// one (a refresh's batch, a miss's lookup); IsNotFound tells the registry's
// "no such namespace" from a failure to read it.
type PolicySource struct {
	List       func(context.Context) ([]*orkstackv1.Namespace, error)
	Names      func(context.Context) ([]string, error)
	Get        func(context.Context, string) (*orkstackv1.Namespace, error)
	IsNotFound func(error) bool
}

// DefaultPolicyRefreshBatch is how many namespace documents one refresh
// reads when the census budget does not set it: half the default budget.
const DefaultPolicyRefreshBatch = DefaultCensusRequestsPerMinute / 2

// PolicyCache maps each namespace to its policy behind an atomic pointer, so
// a record's routing is one map read. A refresh replaces the whole map; a
// miss's lookup adds one entry. It is safe for concurrent use.
type PolicyCache struct {
	sinks  *Sinks
	src    PolicySource
	logger *slog.Logger
	// bg is the lifetime of the lookups a miss starts: they outlive the
	// record that started them, never the engine.
	bg  context.Context
	now func() time.Time
	// lookupRetry is policyLookupRetry; a test shortens it.
	lookupRetry time.Duration
	// batch is how many documents one refresh reads (SetRefreshBatch).
	batch int
	// cursor is where the next refresh's batch of known namespaces starts,
	// in name order; guarded by mu.
	cursor string

	// policies is nil until the first good refresh.
	policies atomic.Pointer[map[string]NamespacePolicy]
	// refreshReads is the registry reads refreshes made (one per listing,
	// one per namespace it read), which the census budget charges; lookups
	// counts the reads misses made, which are single-flight per namespace
	// and charged to nothing.
	refreshReads atomic.Int64
	lookups      atomic.Int64

	mu sync.Mutex
	// generation counts good refreshes; a lookup that started before one
	// keeps nothing it read, since the refresh read the registry later.
	generation uint64
	// missing is the namespaces a lookup found the registry does not hold,
	// until the next good refresh.
	missing map[string]bool
	// inflight is the namespaces whose lookup is running.
	inflight map[string]bool
	// retryAt is when a namespace whose lookup failed may be looked up again.
	retryAt map[string]time.Time
}

// NewPolicyCache builds an empty cache over src, routing to s. It registers
// TelemetryNamespacesMetric on s's default sink. ctx bounds the lookups it
// starts. Nothing routes until Load or Refresh succeeds.
func NewPolicyCache(ctx context.Context, s *Sinks, src PolicySource, logger *slog.Logger) *PolicyCache {
	c := &PolicyCache{
		sinks: s, src: src, logger: logger, bg: ctx, now: time.Now, lookupRetry: policyLookupRetry,
		batch:   DefaultPolicyRefreshBatch,
		missing: map[string]bool{}, inflight: map[string]bool{}, retryAt: map[string]time.Time{},
	}
	_, err := meterFor(s.Default()).Int64ObservableGauge(TelemetryNamespacesMetric,
		metric.WithDescription("Namespaces this engine's policy cache holds, by where their records go."),
		metric.WithUnit("{namespace}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			m := c.policies.Load()
			if m == nil {
				return nil
			}
			counts := map[string]int64{RoutingDefault: 0, RoutingNamed: 0, RoutingNamedWithoutMetrics: 0, RoutingSinkUnknown: 0}
			for _, p := range *m {
				counts[c.routing(p.Sink)]++
			}
			for routing, n := range counts {
				o.Observe(n, metric.WithAttributes(attribute.String(AttrRouting, routing)))
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: ork.telemetry.namespaces could not be built; it is not exported", "error", err)
	}
	return c
}

// routing classifies a namespace's sink.
func (c *PolicyCache) routing(sink string) string {
	if sink == "" || sink == DefaultSink || c.sinks.anyName {
		return RoutingDefault
	}
	p, ok := c.sinks.byName[sink]
	switch {
	case !ok:
		return RoutingSinkUnknown
	case p.cfg.MetricsEnabled():
		return RoutingNamed
	}
	return RoutingNamedWithoutMetrics
}

// Load is the first refresh, bounded: the engine calls it before it serves,
// and starts anyway once bound passes, its records unrouted until a refresh
// succeeds.
func (c *PolicyCache) Load(ctx context.Context, bound time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	if err := c.Refresh(ctx); err != nil {
		return fmt.Errorf("load the telemetry policy of every namespace: %w", err)
	}
	return nil
}

// Refresh brings the map up to date with the registry. The first one, and
// any before a good one, reads every namespace (List). Every later one lists
// the names, drops the namespaces gone from the registry, and reads at most
// the refresh batch of documents: the namespaces it has never read first,
// then the others in turn, so its cost stays within the census budget
// however many namespaces there are, and every namespace is read again
// within ceil(N / batch) refreshes. A refresh that cannot list the registry
// changes nothing; one that cannot read a document keeps that namespace's
// last good policy and reports the error.
func (c *PolicyCache) Refresh(ctx context.Context) error {
	old := c.policies.Load()
	if old == nil {
		return c.refreshAll(ctx)
	}
	c.refreshReads.Add(1)
	names, err := c.src.Names(ctx)
	if err != nil {
		return err
	}
	sort.Strings(names)
	c.mu.Lock()
	batch, cursor := c.batch, c.cursor
	c.mu.Unlock()
	var unread, known []string
	for _, name := range names {
		if _, ok := (*old)[name]; ok {
			known = append(known, name)
		} else {
			unread = append(unread, name)
		}
	}
	toRead := unread[:min(len(unread), batch)]
	next := cursor
	if room := batch - len(toRead); room > 0 && len(known) > 0 {
		// The known namespaces after the cursor, wrapping around.
		i := sort.SearchStrings(known, cursor)
		if i < len(known) && known[i] == cursor {
			i++
		}
		for n := 0; n < min(room, len(known)); n++ {
			name := known[(i+n)%len(known)]
			toRead = append(toRead, name)
			next = name
		}
	}

	m := make(map[string]NamespacePolicy, len(names))
	for _, name := range known {
		m[name] = (*old)[name]
	}
	var firstErr error
	for _, name := range toRead {
		c.refreshReads.Add(1)
		ns, err := c.readNamespace(ctx, name)
		switch {
		case err != nil && c.src.IsNotFound != nil && c.src.IsNotFound(err):
			delete(m, name) // deleted between the listing and the read
		case err != nil:
			if firstErr == nil {
				firstErr = fmt.Errorf("read the telemetry policy of namespace %q: %w", name, err)
			}
		default:
			m[name] = policyOf(ns)
		}
	}
	c.store(m)
	c.mu.Lock()
	c.cursor = next
	c.mu.Unlock()
	return firstErr
}

// refreshAll reads every namespace.
func (c *PolicyCache) refreshAll(ctx context.Context) error {
	c.refreshReads.Add(1)
	list, err := c.src.List(ctx)
	if err != nil {
		return err
	}
	c.refreshReads.Add(int64(len(list)))
	m := make(map[string]NamespacePolicy, len(list))
	for _, ns := range list {
		m[ns.GetName()] = policyOf(ns)
	}
	c.store(m)
	return nil
}

// store makes m the map, as a good refresh.
func (c *PolicyCache) store(m map[string]NamespacePolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.policies.Store(&m)
	c.generation++
	clear(c.missing)
	clear(c.retryAt)
}

// SetRefreshBatch sets how many namespace documents one refresh reads; n
// below 1 is 1.
func (c *PolicyCache) SetRefreshBatch(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batch = max(1, n)
}

func policyOf(ns *orkstackv1.Namespace) NamespacePolicy {
	return NamespacePolicy{Sink: ns.GetTelemetry().GetSink(), TaskLogs: ns.GetTelemetry().GetTaskLogs()}
}

// For is the Providers a record of namespace goes to, or (nil, false) when
// the record is dropped; it then counts the drop itself, under signal. It
// never blocks on the registry: a namespace it does not hold starts one
// lookup in the background and is dropped as pending meanwhile.
func (c *PolicyCache) For(ctx context.Context, namespace, signal string) (*Providers, bool) {
	if c == nil {
		return nil, false
	}
	m := c.policies.Load()
	if m == nil {
		c.sinks.DropRecords(ctx, signal, DropReasonUnrouted, 1)
		return nil, false
	}
	if pol, ok := (*m)[namespace]; ok {
		p, ok := c.sinks.ForNamespaced(pol.Sink)
		if !ok {
			c.sinks.DropRecords(ctx, signal, DropReasonSinkUnknown, 1)
		}
		return p, ok
	}
	c.sinks.DropRecords(ctx, signal, c.miss(namespace), 1)
	return nil, false
}

// miss is the drop reason for a namespace the map lacks, starting its lookup
// when none runs and none is due to wait.
func (c *PolicyCache) miss(namespace string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.missing[namespace] {
		return DropReasonUnrouted
	}
	if retry, failed := c.retryAt[namespace]; failed && c.now().Before(retry) {
		return DropReasonLookupFailed
	}
	if !c.inflight[namespace] {
		c.inflight[namespace] = true
		go c.lookup(namespace, c.generation)
	}
	if _, failed := c.retryAt[namespace]; failed {
		return DropReasonLookupFailed
	}
	return DropReasonPending
}

// lookup reads one namespace and caches what it finds, unless a refresh
// replaced the map after it began (generation). It runs off every record's
// path, so a panic in the registry read is recovered and logged, never the
// engine's end.
func (c *PolicyCache) lookup(namespace string, generation uint64) {
	ctx, cancel := context.WithTimeout(c.bg, policyLookupTimeout)
	defer cancel()
	c.lookups.Add(1)
	ns, err := c.readNamespace(ctx, namespace)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, namespace)
	if c.generation != generation {
		return
	}
	switch {
	case err != nil && c.src.IsNotFound != nil && c.src.IsNotFound(err):
		c.missing[namespace] = true
	case err != nil:
		_, already := c.retryAt[namespace]
		c.retryAt[namespace] = c.now().Add(c.lookupRetry)
		if !already && c.logger != nil && c.bg.Err() == nil {
			c.logger.Warn("telemetry: a namespace's telemetry policy could not be read; its records are dropped as lookup_failed until it can",
				"namespace", namespace, "error", err)
		}
	default:
		delete(c.retryAt, namespace)
		old := c.policies.Load()
		if old == nil {
			return
		}
		m := make(map[string]NamespacePolicy, len(*old)+1)
		for k, v := range *old {
			m[k] = v
		}
		m[namespace] = policyOf(ns)
		c.policies.Store(&m)
	}
}

// readNamespace is src.Get, with a panic turned into an error.
func (c *PolicyCache) readNamespace(ctx context.Context, namespace string) (ns *orkstackv1.Namespace, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("read namespace %q: panic: %v", namespace, r)
		}
	}()
	return c.src.Get(ctx, namespace)
}

// Policy is the cached policy of namespace, if the cache holds it.
func (c *PolicyCache) Policy(namespace string) (NamespacePolicy, bool) {
	if c == nil {
		return NamespacePolicy{}, false
	}
	m := c.policies.Load()
	if m == nil {
		return NamespacePolicy{}, false
	}
	p, ok := (*m)[namespace]
	return p, ok
}

// Requests is the registry reads the cache's refreshes have made, which the
// census budget charges.
func (c *PolicyCache) Requests() int64 { return c.refreshReads.Load() }

// cost is what the next refresh is admitted for: the listing and its batch
// of documents, or, before a good refresh, the listing and every document.
func (c *PolicyCache) cost() int {
	m := c.policies.Load()
	if m == nil {
		return 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return 1 + min(c.batch, len(*m))
}

func (c *PolicyCache) knownMissing(namespace string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.missing[namespace]
}

func (c *PolicyCache) lookingUp(namespace string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight[namespace]
}

// TenantAttrs is the slog key-value pairs a process log line about namespace
// carries: the namespace always, and authored — names a tenant chose, such as
// an Orkflow's, a schedule's or a webhook's — only when the namespace's
// records go to the default sink, where process logs go too. A namespace on
// another sink, or one the cache cannot route, gets the namespace alone, so
// an operator's logs never carry what a tenant keeps on its own sink. The
// caller adds opaque ids and closed enums itself; they are always safe.
func (c *PolicyCache) TenantAttrs(namespace string, authored ...any) []any {
	if c == nil {
		return tenantAttrs(false, namespace)
	}
	if c.sinks.anyName {
		return tenantAttrs(true, namespace, authored...)
	}
	p, ok := c.Policy(namespace)
	return tenantAttrs(ok && c.routing(p.Sink) == RoutingDefault, namespace, authored...)
}

// tenantAttrs is the namespace, and authored only when onDefault: the rule
// TenantAttrs applies from the cache, and a run-bound line from its run's
// own sink (EngineObserver.log).
func tenantAttrs(onDefault bool, namespace string, authored ...any) []any {
	attrs := []any{"namespace", namespace}
	if onDefault {
		attrs = append(attrs, authored...)
	}
	return attrs
}

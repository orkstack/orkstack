package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The censuses' instruments for webhooks and schedules (docs/DEPLOY.md,
// "Censuses").
const (
	WebhookPendingMetric            = "ork.webhook.deliveries.pending"
	SchedulesMetric                 = "ork.schedules"
	ScheduleNextFireMetric          = "ork.schedule.next_fire"
	ScheduleActionsSkippedMetric    = "ork.schedule.actions.skipped"
	ScheduleCensusCoverageMetric    = "ork.schedule.census.coverage"
	ScheduleCensusUnavailableMetric = "ork.schedule.census.unavailable"
	AttrAgeOver                     = "ork.age_over"
	AttrScheduleState               = "state"
	AttrScheduleSkipReason          = "ork.schedule.skip_reason"
	// MaxScheduleDescribes bounds the describes one schedule census pass
	// makes; a deployment with more schedules is described over passes.
	MaxScheduleDescribes = 200
)

// The ages a pending webhook delivery is counted past.
var webhookPendingAges = []struct {
	label string
	age   time.Duration
}{{"5m", 5 * time.Minute}, {"1h", time.Hour}}

// newWebhookPendingCensus registers ork.webhook.deliveries.pending on p, the
// default sink, and returns the pass that feeds it: count is how many
// deliveries still running started more than an age ago. The gauge reports
// the last good pass, and nothing after one that failed.
func newWebhookPendingCensus(p *Providers, logger *slog.Logger,
	count func(ctx context.Context, olderThan time.Duration) (int64, error),
) func(context.Context) error {
	var mu sync.Mutex
	values := map[string]int64{}
	known := false
	_, err := meterFor(p).Int64ObservableGauge(WebhookPendingMetric,
		metric.WithDescription("Webhook deliveries not yet settled that were queued more than ork.age_over ago, in the deployment."),
		metric.WithUnit("{delivery}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			mu.Lock()
			defer mu.Unlock()
			if !known {
				return nil
			}
			for _, a := range webhookPendingAges {
				o.Observe(values[a.label], metric.WithAttributes(attribute.String(AttrAgeOver, a.label)))
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: "+WebhookPendingMetric+" could not be built; it is not exported", "error", err)
	}
	return func(ctx context.Context) error {
		got := map[string]int64{}
		var errs []error
		for _, a := range webhookPendingAges {
			n, err := count(ctx, a.age)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			got[a.label] = n
		}
		err := errors.Join(errs...)
		mu.Lock()
		defer mu.Unlock()
		values, known = got, err == nil
		return err
	}
}

// ScheduleSample is one schedule as a census pass saw it.
type ScheduleSample struct {
	Namespace, Name string
	Paused          bool
	// NextFire is zero when the schedule has none.
	NextFire time.Time
	// Described says SkippedOverlap and MissedCatchup were read this pass:
	// the cluster's running totals of skipped fires.
	Described                     bool
	SkippedOverlap, MissedCatchup int64
}

// ScheduleCensusSource is what the schedule census reads.
type ScheduleCensusSource struct {
	// Read lists every schedule and describes at most maxDescribes.
	Read func(ctx context.Context, maxDescribes int) ([]ScheduleSample, error)
	// Requests, when set, is the running total of requests Read made.
	Requests func() int64
	// Route is where each namespace's series go; nil sends them to the
	// default sink.
	Route GCRouter
	// ObjectLabels is telemetry.object_labels.
	ObjectLabels bool
}

// scheduleSeries is one next-fire series: a namespace's, or with object
// labels one schedule's.
type scheduleSeries struct{ namespace, schedule string }

// scheduleSinkState is what one sink's schedule gauges report.
type scheduleSinkState struct {
	// counts holds each namespace's schedules: [active, paused].
	counts   map[string][2]int64
	nextFire map[scheduleSeries]time.Time
}

// scheduleSnapshot is what the schedule census's gauges report: the last
// pass, immutable once stored. A pass that could not list the schedules
// stores one with unavailable set and nothing else, so no gauge repeats an
// old reading through an outage.
type scheduleSnapshot struct {
	unavailable bool
	coverage    float64
	bySink      map[*Providers]scheduleSinkState
}

// scheduleCensus turns schedule census passes into metrics. Its gauges read
// the last pass's snapshot and take no lock, so a collection never waits on
// a pass. A schedule that is gone stops reporting rather than reading as due
// at the epoch; skipped fires are a counter of the increase of the
// cluster's own totals.
type scheduleCensus struct {
	source ScheduleCensusSource
	logger *slog.Logger
	sinks  *areaSinks[metric.Int64Counter]
	gauges sync.Map // *Providers -> struct{}: sinks whose gauges are registered
	// maxDescribes is what one pass describes at most: half the census
	// budget, so a routine pass never runs the budget into debt.
	maxDescribes int
	snapshot     atomic.Pointer[scheduleSnapshot]
	// skipped and listed belong to the census goroutine alone: the totals
	// last read, by schedule still listed, and how many the last good pass
	// listed, which prices the next one.
	skipped map[scheduleSeries][2]int64
	listed  atomic.Int64
}

func newScheduleCensus(p *Providers, src ScheduleCensusSource, perMinute int, logger *slog.Logger) *scheduleCensus {
	if logger == nil {
		logger = slog.Default()
	}
	c := &scheduleCensus{source: src, logger: logger, skipped: map[scheduleSeries][2]int64{},
		maxDescribes: min(MaxScheduleDescribes, max(1, perMinute/2))}
	c.sinks = newAreaSinks(p, src.Route, "schedule census", logger, func(sink *Providers) (metric.Int64Counter, error) {
		if err := c.registerSinkGauges(sink); err != nil {
			return nil, err
		}
		return meterFor(sink).Int64Counter(ScheduleActionsSkippedMetric,
			metric.WithDescription("Schedule fires the cluster skipped, by namespace and reason: overlap, or catchup_window when the engine was away past the catch-up window."),
			metric.WithUnit("{fire}"))
	})
	meter := meterFor(p)
	var errs []error
	_, err := meter.Float64ObservableGauge(ScheduleCensusCoverageMetric,
		metric.WithDescription("The share of schedules the last schedule census pass described; below 1, skipped-fire counts lag."),
		metric.WithUnit("1"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			if snap := c.snapshot.Load(); snap != nil && !snap.unavailable {
				o.Observe(snap.coverage)
			}
			return nil
		}))
	errs = append(errs, err)
	_, err = meter.Int64ObservableGauge(ScheduleCensusUnavailableMetric,
		metric.WithDescription("1 when the last schedule census pass could not list the schedules."),
		metric.WithUnit("1"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if snap := c.snapshot.Load(); snap != nil {
				o.Observe(boolGauge(snap.unavailable))
			}
			return nil
		}))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		logger.Warn("telemetry: some schedule census instruments could not be built; the rest still record", "error", err)
	}
	return c
}

// cost is what the next pass is admitted for: the listing's pages and a
// describe for each schedule the last pass listed, up to maxDescribes.
func (c *scheduleCensus) cost() int {
	listed := int(c.listed.Load())
	return 1 + listed/scheduleListPage + min(listed, c.maxDescribes)
}

// scheduleListPage is the schedules one list page holds (the engine's
// scheduleListPageSize), for pricing a pass.
const scheduleListPage = 1000

// registerSinkGauges registers one sink's schedule gauges, which report the
// namespaces routed to that sink.
func (c *scheduleCensus) registerSinkGauges(sink *Providers) error {
	if _, done := c.gauges.LoadOrStore(sink, struct{}{}); done {
		return nil
	}
	meter := meterFor(sink)
	_, err := meter.Int64ObservableGauge(SchedulesMetric,
		metric.WithDescription("Schedules, by namespace and state (active or paused), at the last schedule census."),
		metric.WithUnit("{schedule}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			snap := c.snapshot.Load()
			if snap == nil || snap.unavailable {
				return nil
			}
			for ns, n := range snap.bySink[sink].counts {
				nsAttr := attribute.String(AttrNamespace, ns)
				o.Observe(n[0], metric.WithAttributes(nsAttr, attribute.String(AttrScheduleState, "active")))
				o.Observe(n[1], metric.WithAttributes(nsAttr, attribute.String(AttrScheduleState, "paused")))
			}
			return nil
		}))
	if err != nil {
		return err
	}
	_, err = meter.Float64ObservableGauge(ScheduleNextFireMetric,
		metric.WithDescription("When the next fire is due, by namespace (the soonest of its active schedules), or by schedule with object labels on, at the last schedule census."),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			snap := c.snapshot.Load()
			if snap == nil || snap.unavailable {
				return nil
			}
			for series, at := range snap.bySink[sink].nextFire {
				attrs := []attribute.KeyValue{attribute.String(AttrNamespace, series.namespace)}
				if series.schedule != "" {
					attrs = append(attrs, attribute.String(AttrSchedule, series.schedule))
				}
				o.Observe(float64(at.Unix()), metric.WithAttributes(attrs...))
			}
			return nil
		}))
	return err
}

// censusRoute is where one namespace's schedule series go this pass.
type censusRoute struct {
	sink    *Providers
	counter metric.Int64Counter
	ok      bool
}

// routeFor routes namespace: its sink, or not ok when it is dropped.
func (c *scheduleCensus) routeFor(ctx context.Context, namespace string) censusRoute {
	sink := c.sinks.defaults
	if c.source.Route != nil {
		routed, ok := c.source.Route(context.WithoutCancel(ctx), namespace)
		if !ok || routed == nil {
			return censusRoute{}
		}
		sink = routed
	}
	counter, ok := c.sinks.on(sink)
	return censusRoute{sink: sink, counter: counter, ok: ok}
}

// pass runs one census pass. It routes every namespace, which may build a
// sink's instruments, before it builds the snapshot, and the gauges read
// only the stored snapshot: no lock is ever held across either.
func (c *scheduleCensus) pass(ctx context.Context) error {
	samples, err := c.source.Read(ctx, c.maxDescribes)
	if err != nil {
		c.snapshot.Store(&scheduleSnapshot{unavailable: true})
		return err
	}
	routes := map[string]censusRoute{}
	for _, s := range samples {
		if _, seen := routes[s.Namespace]; !seen {
			routes[s.Namespace] = c.routeFor(ctx, s.Namespace)
		}
	}
	bySink := map[*Providers]scheduleSinkState{}
	skipped := make(map[scheduleSeries][2]int64, len(samples))
	described := 0
	// Recorded with a context the pass's own deadline does not reach: the
	// baseline below has already moved past these increases.
	record := context.WithoutCancel(ctx)
	for _, s := range samples {
		r := routes[s.Namespace]
		key := scheduleSeries{s.Namespace, s.Name}
		prev, known := c.skipped[key]
		if s.Described {
			described++
			now := [2]int64{s.SkippedOverlap, s.MissedCatchup}
			skipped[key] = now
			// The first sighting is the baseline: what the cluster counted
			// before this engine watched is not an increase. A namespace
			// whose records are dropped (its policy not yet loaded) loses
			// the increases seen meanwhile.
			for i, reason := range []string{"overlap", "catchup_window"} {
				d := now[i] - prev[i]
				if d < 0 { // the schedule was recreated: its totals restarted
					d = now[i]
				}
				if !known || d <= 0 || !r.ok {
					continue
				}
				attrs := []attribute.KeyValue{attribute.String(AttrNamespace, s.Namespace), attribute.String(AttrScheduleSkipReason, reason)}
				if c.source.ObjectLabels {
					attrs = append(attrs, attribute.String(AttrSchedule, s.Name))
				}
				r.counter.Add(record, d, metric.WithAttributes(attrs...))
			}
		} else if known {
			// Listed, not described this pass: its baseline carries over.
			skipped[key] = prev
		}
		if !r.ok {
			continue
		}
		st := bySink[r.sink]
		if st.counts == nil {
			st.counts, st.nextFire = map[string][2]int64{}, map[scheduleSeries]time.Time{}
		}
		n := st.counts[s.Namespace]
		if s.Paused {
			n[1]++
		} else {
			n[0]++
		}
		st.counts[s.Namespace] = n
		if !s.Paused && !s.NextFire.IsZero() {
			series := scheduleSeries{namespace: s.Namespace}
			if c.source.ObjectLabels {
				series.schedule = s.Name
			}
			if at, has := st.nextFire[series]; !has || s.NextFire.Before(at) {
				st.nextFire[series] = s.NextFire
			}
		}
		bySink[r.sink] = st
	}
	// A schedule no longer listed leaves no baseline behind.
	c.skipped = skipped
	c.listed.Store(int64(len(samples)))
	coverage := 1.0
	if len(samples) > 0 {
		coverage = float64(described) / float64(len(samples))
	}
	c.snapshot.Store(&scheduleSnapshot{coverage: coverage, bySink: bySink})
	return nil
}

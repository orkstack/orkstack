package telemetry

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The concurrency census's gauges (E88 §9.9.3). The census pass runs on
// whichever replica a fire lands on and hands its snapshot to this replica's
// gauges; each replica reports the snapshot it holds while it is fresh, and
// its age always. Nothing here is a decision: it only reports. A value the
// pass could not read is never exported as a zero: a pass that could not run
// reports only that it was unavailable, and a queued-runs count it could not
// take is left out.

// The concurrency gauges' attributes.
const (
	AttrConcurrencyLimit = "ork.concurrency.limit"
	AttrConcurrencyLevel = "ork.concurrency.level"
	AttrConcurrencyState = "ork.concurrency.state"
)

// ConcurrencyCensusFailuresMetric counts the census passes that could not
// run.
const ConcurrencyCensusFailuresMetric = "ork.concurrency.census.failures"

// ConcurrencyCensus is one census pass, as the engine hands it over.
type ConcurrencyCensus struct {
	At                time.Time
	Duration          time.Duration
	Rows              []ConcurrencyRow
	Keys              []ConcurrencyKeyRow
	Rebuilding        []ConcurrencyRebuilding
	QueuedRuns        map[string]int
	QueuedRunsUnknown bool
	Partial           map[string]bool
	Gates             int
	Unanswered        int
	Truncated         bool
	Unavailable       bool
	ResetsIncomplete  int
}

// ConcurrencyRow is one (namespace, limit, level) aggregate.
type ConcurrencyRow struct {
	Namespace, Limit, Level string
	SlotsUsed, SlotsLimit   int
	QueueDepth              int
	OldestAge               time.Duration
	OldestTimeoutFraction   float64
}

// ConcurrencyKeyRow is one (namespace, limit) aggregate.
type ConcurrencyKeyRow struct {
	Namespace, Limit                                    string
	KeysActive, KeysFull, StalledHolders, FenceDegraded int
}

// ConcurrencyRebuilding counts one namespace's gates rebuilding in a state
// ("running" or "failing").
type ConcurrencyRebuilding struct {
	Namespace, State string
	Gates            int
}

// ConcurrencyGauges hold the latest census this replica received and
// report it.
type ConcurrencyGauges struct {
	interval, timeout time.Duration
	indexUnavailable  bool
	now               func() time.Time
	// started is when the gauges were built: before this replica receives
	// its first census, local_age counts from it, so a census that never
	// runs shows as an age that grows, not as an absent series.
	started time.Time
	// failures is nil when it could not be built.
	failures metric.Int64Counter

	mu   sync.Mutex
	last ConcurrencyCensus
	have bool
}

// NewConcurrencyGauges builds the gauges on p's meter. interval and timeout
// are the census's own settings: a snapshot older than 2 × interval +
// timeout is no longer reported as current. indexUnavailable is the boot's
// answer for the concurrency search attributes. A gauge that cannot be
// built is logged and left out; telemetry never fails the engine.
func NewConcurrencyGauges(p *Providers, interval, timeout time.Duration, indexUnavailable bool, logger *slog.Logger) *ConcurrencyGauges {
	g := &ConcurrencyGauges{interval: interval, timeout: timeout, indexUnavailable: indexUnavailable, now: time.Now}
	g.started = g.now()
	meter := meterFor(p)
	ints := map[string]metric.Int64ObservableGauge{}
	floats := map[string]metric.Float64ObservableGauge{}
	var errs []error
	var observables []metric.Observable
	intGauge := func(name, unit, desc string) {
		gauge, err := meter.Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, err)
			return
		}
		ints[name] = gauge
		observables = append(observables, gauge)
	}
	floatGauge := func(name, unit, desc string) {
		gauge, err := meter.Float64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, err)
			return
		}
		floats[name] = gauge
		observables = append(observables, gauge)
	}
	intGauge("ork.concurrency.slots.used", "{slot}", "Slots held, by namespace, limit and level.")
	intGauge("ork.concurrency.slots.limit", "{slot}", "Slots the active keys admit, by namespace, limit and level.")
	intGauge("ork.concurrency.keys.active", "{key}", "Keys holding or queueing anything, by namespace and limit.")
	intGauge("ork.concurrency.keys.full", "{key}", "Active keys with every slot held, by namespace and limit.")
	intGauge("ork.concurrency.queue.depth", "{entry}", "Runs and tasks queued, by namespace, limit and level.")
	floatGauge("ork.concurrency.queue.oldest_age", "s", "How long the oldest queued entry has waited, by namespace, limit and level.")
	floatGauge("ork.concurrency.queue.oldest_timeout_fraction", "{fraction}", "The oldest queued entry's wait as a share of its bound.")
	intGauge("ork.concurrency.stalled_holders", "{holder}", "Holders whose runs keep failing their workflow tasks.")
	intGauge("ork.concurrency.fence_degraded", "{gate}", "Gates checking new holders because they dropped a live tombstone.")
	intGauge("ork.concurrency.rebuilding", "{gate}", "Gates rebuilding their record, by namespace and state.")
	intGauge("ork.concurrency.queued_runs", "{run}", "Runs queued for a slot, by namespace.")
	intGauge("ork.concurrency.resets_incomplete", "{reset}", "Resets of a concurrency limit that have run past reset_incomplete_after.")
	intGauge("ork.concurrency.census.partial", "{flag}", "1 when a gate of the namespace did not answer the census.")
	intGauge("ork.concurrency.census.gates", "{gate}", "Gates the census read.")
	intGauge("ork.concurrency.census.unanswered", "{gate}", "Gates that did not answer the census.")
	intGauge("ork.concurrency.census.truncated", "{gate}", "1 when the census stopped at census_max_gates gates.")
	intGauge("ork.concurrency.census.unavailable", "{flag}", "1 when the census could not run its pass.")
	floatGauge("ork.concurrency.census.duration", "s", "How long the census's last pass took.")
	floatGauge("ork.concurrency.census.local_age", "s", "How old this replica's census snapshot is.")
	floatGauge("ork.concurrency.census.interval", "s", "The census's interval.")
	intGauge("ork.concurrency.index_unavailable", "{flag}", "1 when the cluster does not index concurrency limits.")
	failures, err := meter.Int64Counter(ConcurrencyCensusFailuresMetric, metric.WithUnit("{pass}"),
		metric.WithDescription("Census passes that could not run."))
	if err != nil {
		errs = append(errs, err)
	} else {
		g.failures = failures
	}
	if len(observables) > 0 {
		if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			g.observe(o, ints, floats)
			return nil
		}, observables...); err != nil {
			errs = append(errs, err)
		}
	}
	for _, err := range errs {
		if logger != nil {
			logger.Warn("telemetry: a concurrency gauge could not be built; it is not exported", "error", err)
		}
	}
	return g
}

// Record keeps c as this replica's latest snapshot, and counts it when its
// pass could not run.
func (g *ConcurrencyGauges) Record(c ConcurrencyCensus) {
	if c.Unavailable && g.failures != nil {
		g.failures.Add(context.Background(), 1)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.last, g.have = c, true
}

func (g *ConcurrencyGauges) observe(o metric.Observer, ints map[string]metric.Int64ObservableGauge, floats map[string]metric.Float64ObservableGauge) {
	g.mu.Lock()
	c, have := g.last, g.have
	g.mu.Unlock()
	now := g.now()
	obsInt := func(name string, v int, attrs ...attribute.KeyValue) {
		if gauge := ints[name]; gauge != nil {
			o.ObserveInt64(gauge, int64(v), metric.WithAttributes(attrs...))
		}
	}
	obsFloat := func(name string, v float64, attrs ...attribute.KeyValue) {
		if gauge := floats[name]; gauge != nil {
			o.ObserveFloat64(gauge, v, metric.WithAttributes(attrs...))
		}
	}
	flag := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	obsInt("ork.concurrency.index_unavailable", flag(g.indexUnavailable))
	obsFloat("ork.concurrency.census.interval", g.interval.Seconds())
	if !have {
		obsFloat("ork.concurrency.census.local_age", now.Sub(g.started).Seconds())
		return
	}
	obsFloat("ork.concurrency.census.local_age", now.Sub(c.At).Seconds())
	if now.Sub(c.At) > 2*g.interval+g.timeout {
		return // stale: nothing below is current
	}
	obsInt("ork.concurrency.census.unavailable", flag(c.Unavailable))
	obsFloat("ork.concurrency.census.duration", c.Duration.Seconds())
	if c.Unavailable {
		return // the pass could not run: everything below is unknown
	}
	ns := func(n string) attribute.KeyValue { return attribute.String(AttrNamespace, n) }
	for _, r := range c.Rows {
		attrs := []attribute.KeyValue{ns(r.Namespace), attribute.String(AttrConcurrencyLimit, r.Limit), attribute.String(AttrConcurrencyLevel, r.Level)}
		obsInt("ork.concurrency.slots.used", r.SlotsUsed, attrs...)
		obsInt("ork.concurrency.slots.limit", r.SlotsLimit, attrs...)
		obsInt("ork.concurrency.queue.depth", r.QueueDepth, attrs...)
		obsFloat("ork.concurrency.queue.oldest_age", r.OldestAge.Seconds(), attrs...)
		if f := r.OldestTimeoutFraction; !math.IsNaN(f) && !math.IsInf(f, 0) {
			obsFloat("ork.concurrency.queue.oldest_timeout_fraction", f, attrs...)
		}
	}
	for _, k := range c.Keys {
		attrs := []attribute.KeyValue{ns(k.Namespace), attribute.String(AttrConcurrencyLimit, k.Limit)}
		obsInt("ork.concurrency.keys.active", k.KeysActive, attrs...)
		obsInt("ork.concurrency.keys.full", k.KeysFull, attrs...)
		obsInt("ork.concurrency.stalled_holders", k.StalledHolders, attrs...)
		obsInt("ork.concurrency.fence_degraded", k.FenceDegraded, attrs...)
	}
	for _, r := range c.Rebuilding {
		obsInt("ork.concurrency.rebuilding", r.Gates, ns(r.Namespace), attribute.String(AttrConcurrencyState, r.State))
	}
	if !c.QueuedRunsUnknown {
		for n, count := range c.QueuedRuns {
			obsInt("ork.concurrency.queued_runs", count, ns(n))
		}
	}
	for n, partial := range c.Partial {
		obsInt("ork.concurrency.census.partial", flag(partial), ns(n))
	}
	obsInt("ork.concurrency.resets_incomplete", c.ResetsIncomplete)
	obsInt("ork.concurrency.census.gates", c.Gates)
	obsInt("ork.concurrency.census.unanswered", c.Unanswered)
	obsInt("ork.concurrency.census.truncated", flag(c.Truncated))
}

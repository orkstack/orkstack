package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// A pool's backlog, as metrics (docs/DEPLOY.md, "A pool's backlog, as
// metrics"): what waits on each pool, read from the cluster by every engine
// replica, so an autoscaler or an alert can act on a pool's demand. Each
// series is labelled with the pool alone, never the channel it maps to
// (AGENTS.md §7.1). The Prometheus names are the contract dashboards are
// written against: ork_pool_backlog_tasks, ork_pool_backlog_age_seconds,
// ork_pool_tasks_add_rate_per_second, ork_pool_tasks_dispatch_rate_per_second
// and ork_pool_pollers.
const (
	PoolBacklogTasksMetric      = "ork.pool.backlog.tasks"
	PoolBacklogAgeMetric        = "ork.pool.backlog.age"
	PoolTasksAddRateMetric      = "ork.pool.tasks.add_rate"
	PoolTasksDispatchRateMetric = "ork.pool.tasks.dispatch_rate"
	PoolPollersMetric           = "ork.pool.pollers"

	// poolBacklogCensus is the census that reads them.
	poolBacklogCensus = "pool_backlog"
	// poolBacklogStaleAfter is how many intervals a pool's last good reading
	// is reported for after its reads start failing; past it the pool's
	// series are dropped, so a reading nobody refreshed is never shown as
	// the pool's state.
	poolBacklogStaleAfter = 3
	// poolReadTimeout bounds one pool's read inside its pass, below the
	// census pass's own bound (censusPassTimeout).
	poolReadTimeout = 20 * time.Second
)

// PoolReading is one read of one pool's channel.
type PoolReading struct {
	// Backlog is the cluster's approximate count of tasks waiting.
	Backlog int64
	// BacklogAge is how long the oldest waiting task has waited.
	BacklogAge time.Duration
	// AddRate and DispatchRate are the cluster's own rates, tasks per second.
	AddRate, DispatchRate float64
	// Pollers is the Orkers that polled within the freshness window.
	Pollers int64
}

// PoolBacklogSource is what the pool backlog census reads: Read answers one
// pool, every Interval, for each of Pools.
type PoolBacklogSource struct {
	Pools    []string
	Read     func(ctx context.Context, pool string) (PoolReading, error)
	Interval time.Duration
}

type poolSample struct {
	reading PoolReading
	at      time.Time
}

// newPoolBacklogGauges registers the pool gauges on p and returns the pass
// that feeds them: one read per pool, all at once, each bounded by the
// interval. A pool is reported from its last good reading, and dropped once
// that reading is older than poolBacklogStaleAfter intervals: not measured is
// absent, never 0.
func newPoolBacklogGauges(p *Providers, logger *slog.Logger, src PoolBacklogSource, now func() time.Time) func(context.Context) error {
	var mu sync.Mutex
	last := map[string]poolSample{}
	staleAfter := time.Duration(poolBacklogStaleAfter) * src.Interval

	meter := meterFor(p)
	tasks, err1 := meter.Int64ObservableGauge(PoolBacklogTasksMetric,
		metric.WithDescription("Approximate tasks waiting for an Orker of the pool."),
		metric.WithUnit("{task}"))
	age, err2 := meter.Float64ObservableGauge(PoolBacklogAgeMetric,
		metric.WithDescription("How long the pool's oldest waiting task has waited."),
		metric.WithUnit("s"))
	added, err3 := meter.Float64ObservableGauge(PoolTasksAddRateMetric,
		metric.WithDescription("Tasks the pool received per second, the cluster's own recent rate."),
		metric.WithUnit("{task}/s"))
	dispatched, err4 := meter.Float64ObservableGauge(PoolTasksDispatchRateMetric,
		metric.WithDescription("Tasks handed to the pool's Orkers per second, the cluster's own recent rate."),
		metric.WithUnit("{task}/s"))
	pollers, err5 := meter.Int64ObservableGauge(PoolPollersMetric,
		metric.WithDescription("Orkers of the pool that asked for work recently. A saturated, cordoned or stopping Orker asks for none, so this is not a count of the pool's Orkers."),
		metric.WithUnit("{poller}"))
	unavailable := errors.Join(err1, err2, err3, err4, err5)
	if unavailable == nil {
		_, unavailable = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			mu.Lock()
			defer mu.Unlock()
			at := now()
			for pool, s := range last {
				if at.Sub(s.at) > staleAfter {
					continue
				}
				attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String(AttrPool, pool)))
				o.ObserveInt64(tasks, s.reading.Backlog, attrs)
				o.ObserveFloat64(age, s.reading.BacklogAge.Seconds(), attrs)
				o.ObserveFloat64(added, s.reading.AddRate, attrs)
				o.ObserveFloat64(dispatched, s.reading.DispatchRate, attrs)
				o.ObserveInt64(pollers, s.reading.Pollers, attrs)
			}
			return nil
		}, tasks, age, added, dispatched, pollers)
	}
	if unavailable != nil {
		// Nothing would observe a reading, so no pool is read: every pass
		// fails, which ork.census.failed and its Warn show.
		unavailable = fmt.Errorf("the pool backlog gauges could not be set up; ork.pool.* is not exported: %w", unavailable)
		return func(context.Context) error { return unavailable }
	}
	readTimeout := min(src.Interval, poolReadTimeout)
	failing := map[string]bool{}

	return func(ctx context.Context) error {
		readings := make([]PoolReading, len(src.Pools))
		failures := make([]error, len(src.Pools))
		var wg sync.WaitGroup
		for i, pool := range src.Pools {
			wg.Go(func() {
				// A read runs on a goroutine of its own, outside the census's
				// recover: a panic in it fails this pool's read, never the
				// engine.
				defer func() {
					if r := recover(); r != nil {
						failures[i] = fmt.Errorf("the read panicked: %v", r)
					}
				}()
				readCtx, cancel := context.WithTimeout(ctx, readTimeout)
				defer cancel()
				readings[i], failures[i] = src.Read(readCtx, pool)
			})
		}
		wg.Wait()
		at := now()
		var errs []error
		mu.Lock()
		defer mu.Unlock()
		for i, pool := range src.Pools {
			if failures[i] != nil {
				errs = append(errs, fmt.Errorf("pool %q: %w", pool, failures[i]))
				// The census logs the first failure of a streak of passes;
				// each pool that starts failing is logged here, so a second
				// pool failing within that streak is not lost.
				if !failing[pool] && logger != nil && ctx.Err() == nil {
					logger.Warn("cannot read a pool's backlog; its ork.pool.* series are dropped after three intervals",
						"pool", pool, "error", failures[i])
				}
				failing[pool] = true
				if s, ok := last[pool]; ok && at.Sub(s.at) > staleAfter {
					delete(last, pool)
				}
				continue
			}
			if failing[pool] && logger != nil {
				logger.Info("reads a pool's backlog again", "pool", pool)
			}
			failing[pool] = false
			last[pool] = poolSample{reading: readings[i], at: at}
		}
		return errors.Join(errs...)
	}
}

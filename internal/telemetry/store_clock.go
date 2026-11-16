package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// StoreClockSkewMetric is this engine's clock minus the object store's, in
// seconds (pkg/artifacts StoreClock).
const StoreClockSkewMetric = "ork.store.clock_skew"

// storeClockProbeTimeout bounds one probe: a write and a read of one tiny
// object.
const storeClockProbeTimeout = 30 * time.Second

// storeClockFallbackInterval is the probe's interval when the one it is
// given is not positive, rather than a ticker that cannot exist.
const storeClockFallbackInterval = time.Hour

// StoreClockGauge reports the last good store-clock reading. While the last
// probe failed it reports nothing, so no dashboard reads a stale skew as
// current; the failure is logged once per run of failures.
type StoreClockGauge struct {
	logger *slog.Logger

	mu      sync.Mutex
	skew    time.Duration
	known   bool
	failing bool
}

// NewStoreClockGauge registers ork.store.clock_skew on p. It works, as a
// no-op, on nil or disabled Providers, and an instrument that cannot be built
// is logged and exports nothing.
func NewStoreClockGauge(p *Providers, logger *slog.Logger) *StoreClockGauge {
	g := &StoreClockGauge{logger: logger}
	meter := meterFor(p)
	_, err := meter.Float64ObservableGauge(StoreClockSkewMetric,
		metric.WithDescription("This engine's clock minus the object store's, from the last good probe; absent while the probe fails."),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.known {
				o.Observe(g.skew.Seconds())
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: the store clock skew gauge could not be built; it is not exported", "error", err)
	}
	return g
}

// Record takes one probe's result.
func (g *StoreClockGauge) Record(skew time.Duration, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err != nil {
		g.known = false
		if !g.failing && g.logger != nil {
			g.logger.Warn("the store clock probe failed; ork.store.clock_skew is not reported until it succeeds", "error", err)
		}
		g.failing = true
		return
	}
	if g.failing && g.logger != nil {
		g.logger.Info("the store clock probe succeeds again", "skew", skew)
	}
	g.skew, g.known, g.failing = skew, true, false
}

// RunStoreClockProbe probes the store's clock at once and then every
// interval until ctx ends, recording each result on g. It never stops on a
// failed probe, never retries inline, and never touches a run.
func RunStoreClockProbe(ctx context.Context, interval time.Duration, probe func(context.Context) (time.Duration, error), g *StoreClockGauge) {
	if interval <= 0 {
		interval = storeClockFallbackInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	runStoreClockProbe(ctx, ticker.C, interval, probe, g)
}

func runStoreClockProbe(ctx context.Context, ticks <-chan time.Time, interval time.Duration,
	probe func(context.Context) (time.Duration, error), g *StoreClockGauge,
) {
	bound := min(interval, storeClockProbeTimeout)
	once := func() {
		probeCtx, cancel := context.WithTimeout(ctx, bound)
		defer cancel()
		skew, err := probe(probeCtx)
		if ctx.Err() != nil {
			return // shutting down: a canceled probe is not a failure
		}
		g.Record(skew, err)
	}
	once()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			once()
		}
	}
}

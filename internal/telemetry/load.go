package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrCordonCause labels ork.orker.cordoned with one cause of an Orker's
// cordon: "operator", "store_outage" or "stopping" (AGENTS.md §7, Cordon). A
// bounded label: one series per cause, each 0 or 1.
const AttrCordonCause = "ork.cordon.cause"

// cordonCauses is every cause ork.orker.cordoned reports, in its order.
var cordonCauses = []struct {
	name string
	held func(orker.LoadReading) bool
}{
	{"operator", func(r orker.LoadReading) bool { return r.Operator }},
	{"store_outage", func(r orker.LoadReading) bool { return r.StoreOutage }},
	{"stopping", func(r orker.LoadReading) bool { return r.Stopping }},
}

// WatchOrkerLoad registers the Orker's load gauges (docs/DEPLOY.md
// "Metrics"), read from read when the meter collects and never a stored copy
// (AGENTS.md §7). A process-level signal, so the default sink's:
//
//   - ork.orker.running_tasks: the tasks it runs now;
//   - ork.orker.open_pins: the pins it holds open;
//   - ork.orker.max_concurrent: how many tasks it runs at once;
//   - ork.orker.cordoned: by ork.cordon.cause, 1 while that cause holds,
//     else 0.
//
// Every series carries ork.orker_id and ork.pool, so a dashboard sums them by
// pool. It works, as a no-op, on nil or disabled Providers, and never fails
// the Orker: an instrument that cannot be built is logged and exports nothing.
func WatchOrkerLoad(p *Providers, orkerID, pool string, read func() orker.LoadReading, logger *slog.Logger) {
	meter := meterFor(p)
	running, err1 := meter.Int64ObservableGauge("ork.orker.running_tasks",
		metric.WithDescription("The tasks this Orker runs now."),
		metric.WithUnit("{task}"))
	pins, err2 := meter.Int64ObservableGauge("ork.orker.open_pins",
		metric.WithDescription("The pins this Orker holds open."),
		metric.WithUnit("{pin}"))
	maxConcurrent, err3 := meter.Int64ObservableGauge("ork.orker.max_concurrent",
		metric.WithDescription("How many tasks this Orker runs at once (orker.max_concurrent)."),
		metric.WithUnit("{task}"))
	cordoned, err4 := meter.Int64ObservableGauge("ork.orker.cordoned",
		metric.WithDescription("1 while this cause of a cordon holds on this Orker, else 0; any cause stops it taking new work."),
		metric.WithUnit("{cordon}"))
	for name, err := range map[string]error{
		"ork.orker.running_tasks": err1, "ork.orker.open_pins": err2,
		"ork.orker.max_concurrent": err3, "ork.orker.cordoned": err4,
	} {
		if err != nil {
			warnLoad(logger, "telemetry: the Orker's load gauge "+name+" could not be built; its load is not exported", err)
			return
		}
	}
	base := []attribute.KeyValue{attribute.String(AttrOrkerID, orkerID), attribute.String(AttrPool, pool)}
	orkerSet := attribute.NewSet(base...)
	causeSets := make([]attribute.Set, len(cordonCauses))
	for i, cause := range cordonCauses {
		causeSets[i] = attribute.NewSet(append(base[:len(base):len(base)], attribute.String(AttrCordonCause, cause.name))...)
	}
	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r := read()
		attrs := metric.WithAttributeSet(orkerSet)
		o.ObserveInt64(running, r.RunningTasks, attrs)
		o.ObserveInt64(pins, r.OpenPins, attrs)
		o.ObserveInt64(maxConcurrent, r.MaxConcurrent, attrs)
		for i, cause := range cordonCauses {
			var held int64
			if cause.held(r) {
				held = 1
			}
			o.ObserveInt64(cordoned, held, metric.WithAttributeSet(causeSets[i]))
		}
		return nil
	}, running, pins, maxConcurrent, cordoned)
	if err != nil {
		warnLoad(logger, "telemetry: the Orker's load gauges could not be registered; its load is not exported", err)
	}
}

func warnLoad(logger *slog.Logger, msg string, err error) {
	if logger != nil {
		logger.Warn(msg, "error", err)
	}
}

// The Orker's presence gauges: ork.orker.present is 1 for as long as the
// process exports, so a pool's Orkers are counted from metrics alone, and
// ork.orker.task_log.export_enabled is this Orker's default for exporting
// task logs (ORK_ORKER_TASK_LOG_EXPORT), which a namespace may override.
const (
	OrkerPresentMetric       = "ork.orker.present"
	OrkerTaskLogExportMetric = "ork.orker.task_log.export_enabled"
)

// WatchOrkerPresence registers the presence gauges on p, each labelled with
// the Orker id and pool and reported at every collection.
func WatchOrkerPresence(p *Providers, orkerID, pool string, taskLogExport bool, logger *slog.Logger) {
	meter := meterFor(p)
	set := attribute.NewSet(attribute.String(AttrOrkerID, orkerID), attribute.String(AttrPool, pool))
	export := int64(0)
	if taskLogExport {
		export = 1
	}
	for _, g := range []struct {
		name, description string
		value             int64
	}{
		{OrkerPresentMetric, "1 for each Orker exporting metrics, by pool.", 1},
		{OrkerTaskLogExportMetric, "1 when this Orker exports task logs unless a namespace says otherwise, else 0.", export},
	} {
		value := g.value
		if _, err := meter.Int64ObservableGauge(g.name, metric.WithDescription(g.description),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(value, metric.WithAttributeSet(set))
				return nil
			})); err != nil {
			warnLoad(logger, "telemetry: the Orker gauge "+g.name+" could not be built; it is not exported", err)
		}
	}
}

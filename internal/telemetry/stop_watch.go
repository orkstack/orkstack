package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrStopWatchReason labels ork.orker.stop_watch.failures with the step that
// failed: "anchor", "pipe", "start" or "exit" (orker.StopWatchAnchor and its
// siblings), a closed set.
const AttrStopWatchReason = "reason"

// StopWatchMetrics is ork.orker.stop_watch.failures: each attempt whose
// container's event stream the Orker could not open or follow to its end, so whether the runtime
// stopped the container is unknown (orker.ExitStatus.StopWatchFailure). A
// process-level signal, so the default sink's, labelled with the Orker.
// Recording fails nothing.
type StopWatchMetrics struct {
	failures metric.Int64Counter
	orker    attribute.KeyValue
}

// stopWatchReasons is the label's closed set.
var stopWatchReasons = map[string]bool{
	orker.StopWatchAnchor: true,
	orker.StopWatchPipe:   true,
	orker.StopWatchStart:  true,
	orker.StopWatchExit:   true,
}

// NewStopWatchMetrics builds the counter from p's meter provider; a no-op on
// nil or disabled Providers.
func NewStopWatchMetrics(p *Providers, orkerID string) (*StopWatchMetrics, error) {
	failures, err := meterFor(p).Int64Counter("ork.orker.stop_watch.failures",
		metric.WithDescription("Attempts whose container's event stream this Orker could not open or follow to its end, "+
			"so whether the runtime stopped the container is unknown, by the step that failed."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	return &StopWatchMetrics{failures: failures, orker: attribute.String(AttrOrkerID, orkerID)}, nil
}

// StopWatchFailure counts one attempt's stream that could not be followed. A
// reason outside the closed set is not recorded, so the label never grows.
func (m *StopWatchMetrics) StopWatchFailure(reason string) {
	if !stopWatchReasons[reason] {
		return
	}
	m.failures.Add(context.Background(), 1,
		metric.WithAttributes(m.orker, attribute.String(AttrStopWatchReason, reason)))
}

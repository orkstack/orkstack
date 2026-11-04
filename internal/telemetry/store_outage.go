package telemetry

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrStoreOutageTransition labels ork.orker.store_outage.transitions with
// which way an Orker went: "entered" or "left".
const AttrStoreOutageTransition = "ork.transition"

// StoreOutageMetrics is the Orker's store-outage cordon metrics (docs/DEPLOY.md
// "When object storage goes away"), the one implementation of
// orker.StoreOutageObserver. A process-level signal, so the default sink's:
//
//   - ork.orker.store_outage.active: 1 while the cordon holds, else 0;
//   - ork.orker.store_outage.transitions: each entry and each exit.
//
// Every series carries ork.orker_id. Recording can fail nothing: a measurement
// the SDK cannot take is dropped, and the watch never hears of it.
type StoreOutageMetrics struct {
	transitions metric.Int64Counter
	orker       attribute.KeyValue
}

var _ orker.StoreOutageObserver = (*StoreOutageMetrics)(nil)

// NewStoreOutageMetrics builds both instruments from p's meter provider and
// registers the gauge, read from active at collection. It works, as a no-op,
// on nil or disabled Providers. logger, when set, records what cannot be
// measured; a metric that could not be built leaves the Orker running.
func NewStoreOutageMetrics(p *Providers, orkerID string, active func() bool, logger *slog.Logger) (*StoreOutageMetrics, error) {
	meter := meterFor(p)
	transitions, err := meter.Int64Counter("ork.orker.store_outage.transitions",
		metric.WithDescription("Times this Orker cordoned itself because object storage stopped answering, and times it resumed."),
		metric.WithUnit("{transition}"))
	if err != nil {
		return nil, err
	}
	m := &StoreOutageMetrics{transitions: transitions, orker: attribute.String(AttrOrkerID, orkerID)}
	gauge, err := meter.Int64ObservableGauge("ork.orker.store_outage.active",
		metric.WithDescription("1 while this Orker holds its cordon because object storage does not answer, else 0."),
		metric.WithUnit("{cordon}"))
	if err != nil {
		return nil, err
	}
	set := attribute.NewSet(m.orker)
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var value int64
		if active() {
			value = 1
		}
		o.ObserveInt64(gauge, value, metric.WithAttributeSet(set))
		return nil
	}, gauge); err != nil && logger != nil {
		logger.Warn("telemetry: the Orker's store outage gauge could not be registered; it is not exported", "error", err)
	}
	return m, nil
}

// StoreOutageEntered counts one entry.
func (m *StoreOutageMetrics) StoreOutageEntered() { m.count("entered") }

// StoreOutageLeft counts one exit.
func (m *StoreOutageMetrics) StoreOutageLeft(time.Duration) { m.count("left") }

func (m *StoreOutageMetrics) count(transition string) {
	m.transitions.Add(context.Background(), 1,
		metric.WithAttributes(m.orker, attribute.String(AttrStoreOutageTransition, transition)))
}

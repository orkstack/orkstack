package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/temporalconn"
)

// Labels of the TLS files' metrics.
const (
	// AttrTLSFile is the setting that names the file (temporal.tls.cert_file,
	// …), never its path.
	AttrTLSFile = "ork.file"
	// AttrTLSOutcome is a reload's outcome: "adopted" or "refused".
	AttrTLSOutcome = "ork.outcome"
)

// TLSMetrics are the Temporal connection's TLS metrics (docs/DEPLOY.md,
// "Temporal over TLS"), the one implementation of temporalconn.Observer. A
// process-level signal, so the default sink's:
//
//   - ork.tls.reload{ork.file, ork.outcome}: each re-read of a changed file,
//     adopted or refused;
//   - ork.tls.cert.not_after{ork.file}: the end, in Unix seconds, of the
//     certificates a file holds (the earliest of them);
//   - ork.temporal.auth_failures: calls the cluster refused as
//     unauthenticated.
//
// Recording can fail nothing: a measurement the SDK cannot take is dropped.
type TLSMetrics struct {
	reloads metric.Int64Counter
	auth    metric.Int64Counter

	mu   sync.Mutex
	ends map[string]time.Time
}

var _ temporalconn.Observer = (*TLSMetrics)(nil)

// NewTLSMetrics builds the instruments from p's meter provider. It works, as
// a no-op, on nil or disabled Providers; logger, when set, records a gauge
// that could not be registered.
func NewTLSMetrics(p *Providers, logger *slog.Logger) (*TLSMetrics, error) {
	meter := meterFor(p)
	reloads, err := meter.Int64Counter("ork.tls.reload",
		metric.WithDescription("Re-reads of a changed Temporal TLS or API key file, adopted or refused (a refused one leaves the last good value in use)."),
		metric.WithUnit("{reload}"))
	if err != nil {
		return nil, err
	}
	auth, err := meter.Int64Counter("ork.temporal.auth_failures",
		metric.WithDescription("Calls the Temporal cluster refused as unauthenticated: a revoked key or an expired certificate."),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil, err
	}
	m := &TLSMetrics{reloads: reloads, auth: auth, ends: map[string]time.Time{}}
	gauge, err := meter.Int64ObservableGauge("ork.tls.cert.not_after",
		metric.WithDescription("The end, in Unix seconds, of the certificates a Temporal TLS file holds: the earliest of them."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		for file, end := range m.ends {
			o.ObserveInt64(gauge, end.Unix(), metric.WithAttributes(attribute.String(AttrTLSFile, file)))
		}
		return nil
	}, gauge); err != nil && logger != nil {
		logger.Warn("telemetry: the TLS certificate end gauge could not be registered; it is not exported", "error", err)
	}
	return m, nil
}

// FileReloaded counts one re-read.
func (m *TLSMetrics) FileReloaded(setting string, ok bool) {
	outcome := "refused"
	if ok {
		outcome = "adopted"
	}
	m.reloads.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String(AttrTLSFile, setting), attribute.String(AttrTLSOutcome, outcome)))
}

// CertificateLoaded records a file's end for the gauge.
func (m *TLSMetrics) CertificateLoaded(setting string, notAfter time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ends[setting] = notAfter
}

// AuthFailure counts one refused call.
func (m *TLSMetrics) AuthFailure() { m.auth.Add(context.Background(), 1) }

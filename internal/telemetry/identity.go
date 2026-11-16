package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/identity"
)

// IdentityMetrics is ork.identity.keys_copy_stale, the one implementation of
// identity.CopyObserver (docs/TASK_IDENTITY.md, "The copy on disk"): each
// refresh, about once a minute, after which this engine's copy of the
// published signing keys was older than identity.keys_copy_max_age, so it
// signed nothing and verified nothing. Recording can fail nothing.
type IdentityMetrics struct {
	stale metric.Int64Counter
}

var _ identity.CopyObserver = (*IdentityMetrics)(nil)

// NewIdentityMetrics builds the counter from p's meter provider. It works,
// as a no-op, on nil or disabled Providers.
func NewIdentityMetrics(p *Providers) (*IdentityMetrics, error) {
	stale, err := meterFor(p).Int64Counter("ork.identity.keys_copy_stale",
		metric.WithDescription("Refreshes after which this engine's copy of the published signing keys was "+
			"older than identity.keys_copy_max_age: it signed nothing and verified nothing."),
		metric.WithUnit("{refresh}"))
	if err != nil {
		return nil, err
	}
	return &IdentityMetrics{stale: stale}, nil
}

// KeysCopyStale counts one refresh with a stale copy.
func (m *IdentityMetrics) KeysCopyStale() {
	m.stale.Add(context.Background(), 1)
}

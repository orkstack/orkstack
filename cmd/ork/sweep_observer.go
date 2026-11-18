package main

import (
	"context"
	"time"

	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// sweepObserver adapts the telemetry observer to archive.SweepObserver: this
// binary imports both, and neither imports the other.
type sweepObserver struct {
	o   *telemetry.EngineObserver
	now func() time.Time
}

var _ archivecleanup.SweepObserver = sweepObserver{}

func (s sweepObserver) ObserveSweep(ctx context.Context, r archivecleanup.SweepResult) {
	now := s.now()
	var seconds float64
	if r.Unconfirmed > 0 && !r.EarliestExpiry.IsZero() {
		seconds = r.EarliestExpiry.Sub(now).Seconds()
	}
	s.o.RecordArchiveSweep(ctx, telemetry.ArchiveSweepPass{
		Archived:        int64(r.Archived),
		Failed:          int64(r.Failed),
		Expired:         int64(r.Expired),
		Unconfirmed:     int64(r.Unconfirmed),
		SecondsToExpiry: seconds,
		ListFailed:      r.ListFailed,
		StoreFailed:     r.StoreFailed,
		IntervalTooLong: r.IntervalTooLong,
		At:              now,
	})
}

// cleanupObserver adapts the telemetry observers to archive.CleanupObserver:
// the engine observer's archive counters, and cleanup's own metrics.
type cleanupObserver struct {
	o       *telemetry.EngineObserver
	metrics *telemetry.CleanupObserver
}

var _ archivecleanup.CleanupObserver = cleanupObserver{}

func (c cleanupObserver) ObserveCleanup(ctx context.Context, p archivecleanup.CleanupPass) {
	s := p.CleanupStats
	c.o.RecordArchiveCleanup(ctx, int64(s.RunsCleaned), int64(s.RunsFailed), int64(s.MissingNotDue), int64(s.MissingHeld))
	c.metrics.ObserveCleanup(ctx, p)
}

// webhookObserver adapts the telemetry webhook metrics to
// engine.WebhookObserver.
type webhookObserver struct{ m *telemetry.WebhookMetrics }

var _ engine.WebhookObserver = webhookObserver{}

func (w webhookObserver) ObserveWebhookAttempt(ctx context.Context, namespace, webhookID string, a webhooks.Attempt, outcome string) {
	w.m.Attempt(ctx, namespace, webhookID, a, outcome)
}

func (w webhookObserver) ObserveWebhookDelivery(ctx context.Context, namespace, webhookID, outcome string, attempts int, queuedAt, settledAt time.Time) {
	w.m.Delivery(ctx, namespace, webhookID, outcome, attempts, queuedAt, settledAt)
}

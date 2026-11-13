package main

import (
	"context"
	"time"

	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/telemetry"
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

// cleanupObserver adapts the telemetry observer to archive.CleanupObserver.
type cleanupObserver struct{ o *telemetry.EngineObserver }

var _ archivecleanup.CleanupObserver = cleanupObserver{}

func (c cleanupObserver) ObserveCleanup(ctx context.Context, s archivecleanup.CleanupStats) {
	c.o.RecordArchiveCleanup(ctx, int64(s.RunsCleaned), int64(s.RunsFailed), int64(s.MissingNotDue), int64(s.MissingHeld))
}

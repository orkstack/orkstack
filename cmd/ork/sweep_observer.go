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
	var seconds float64
	dated := !r.EarliestExpiry.IsZero()
	if dated {
		seconds = r.EarliestExpiry.Sub(s.now()).Seconds()
	}
	s.o.RecordArchiveSweep(ctx, int64(r.Archived), int64(r.Failed), int64(r.Expired), int64(r.Unconfirmed), seconds, dated)
}

// cleanupObserver adapts the telemetry observer to archive.CleanupObserver.
type cleanupObserver struct{ o *telemetry.EngineObserver }

var _ archivecleanup.CleanupObserver = cleanupObserver{}

func (c cleanupObserver) ObserveCleanup(ctx context.Context, s archivecleanup.CleanupStats) {
	c.o.RecordArchiveCleanup(ctx, int64(s.RunsCleaned), int64(s.RunsFailed), int64(s.MissingNotDue), int64(s.MissingHeld))
}

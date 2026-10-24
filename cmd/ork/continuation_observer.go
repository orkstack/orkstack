package main

import (
	"context"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// continuationObserver adapts the telemetry observer to
// engine.ContinuationObserver: internal/telemetry does not import
// internal/engine, so it cannot name the engine's observation types, and
// this binary — which imports both — translates between them.
type continuationObserver struct{ o *telemetry.EngineObserver }

var _ engine.ContinuationObserver = continuationObserver{}

func (c continuationObserver) ObserveContinuationWrite(ctx context.Context, o engine.ContinuationWriteObservation) {
	c.o.RecordContinuationWrite(ctx, o.Namespace, o.Kind, o.Outcome, o.StateBytes, o.Seconds, o.HoldSeconds)
}

func (c continuationObserver) ObserveContinuationRestore(ctx context.Context, o engine.ContinuationRestoreObservation) {
	c.o.RecordContinuationRestore(ctx, o.Namespace, o.Outcome, o.Attempt, o.Final, o.Seconds)
}

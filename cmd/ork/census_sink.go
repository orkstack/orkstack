package main

import (
	"context"
	"maps"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// censusSink hands each concurrency census pass to this replica's gauges,
// so neither the engine nor telemetry imports the other.
type censusSink struct{ gauges *telemetry.ConcurrencyGauges }

func (s censusSink) RecordConcurrencyCensus(snap engine.CensusSnapshot) {
	c := telemetry.ConcurrencyCensus{
		At: snap.At, Duration: snap.Duration, QueuedRuns: maps.Clone(snap.QueuedRuns), QueuedRunsUnknown: snap.QueuedRunsUnknown,
		Partial: maps.Clone(snap.Partial), Gates: snap.Gates, Unanswered: snap.Unanswered, Truncated: snap.Truncated,
		Unavailable: snap.Unavailable,
	}
	for _, r := range snap.Rows {
		c.Rows = append(c.Rows, telemetry.ConcurrencyRow{Namespace: r.Namespace, Limit: r.Limit, Level: r.Level,
			SlotsUsed: r.SlotsUsed, SlotsLimit: r.SlotsLimit, QueueDepth: r.QueueDepth,
			OldestAge: r.OldestAge, OldestTimeoutFraction: r.OldestTimeoutFraction})
	}
	for _, k := range snap.Keys {
		c.Keys = append(c.Keys, telemetry.ConcurrencyKeyRow{Namespace: k.Namespace, Limit: k.Limit, KeysActive: k.KeysActive,
			KeysFull: k.KeysFull, StalledHolders: k.StalledHolders, FenceDegraded: k.FenceDegraded})
	}
	for key, gates := range snap.Rebuilding {
		c.Rebuilding = append(c.Rebuilding, telemetry.ConcurrencyRebuilding{Namespace: key[0], State: key[1], Gates: gates})
	}
	s.gauges.Record(c)
}

// arrivalObserver hands the gates' answered acquires to the arrivals
// metric (E88 §9.9.2).
type arrivalObserver struct{ m *telemetry.ConcurrencyMetrics }

func (o arrivalObserver) ObserveArrival(ctx context.Context, namespace, orkflow, limit, level, strategy, outcome string) {
	o.m.Arrival(ctx, telemetry.ConcurrencyArrival{Namespace: namespace, Orkflow: orkflow, Limit: limit, Level: level,
		Strategy: strategy, Outcome: outcome})
}

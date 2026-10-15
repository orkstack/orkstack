package engine

import "time"

// Every default a run's plan pins lives in this file, and nowhere else in
// internal/engine refers to one. Its only readers are internal/api (at
// submit, onto the plan) and cmd/ork (boot checks): workflow code reads the
// number the plan carries, never the constant, so a release that changes a
// default moves only new runs — on an unversioned fleet as on a pinned one —
// and a recorded history replays its own crossing points.

// DefaultHistoryMaxEvents is engine.history.max_events' default: the
// Temporal server's default per-execution history event limit.
const DefaultHistoryMaxEvents int64 = 51200

// DefaultHistoryMaxBytes is engine.history.max_bytes' default: the Temporal
// server's default per-execution history size limit, 50 MiB.
const DefaultHistoryMaxBytes int64 = 50 << 20

// DefaultHistoryReserve is what a run keeps back below its history limit so
// its own failure path can still be recorded, and the byte charges for
// commands a workflow task issued but has not yet recorded.
// TestIntegration_HistoryGuardHoldsAcrossShapes validates every value; a
// measured fail path above its prediction raises the constant, and the new
// value applies to new runs only.
var DefaultHistoryReserve = HistoryReserve{
	Fixed:                  96,
	PerInFlight:            16,
	PerPending:             8,
	PerAwait:               24,
	PerOpenPin:             12,
	PerFanOut:              8,
	PerHandler:             48,
	BytesPerEvent:          512,
	PerInFlightBytes:       8 << 10,
	CommandBytes:           1 << 10,
	PublishBytes:           4 << 10,
	SessionBytes:           4 << 10,
	ChildStartBytes:        1 << 20,
	AwaitHandlerStartBytes: 64 << 10,
}

// The continuation budget's defaults (engine.continuation.*, docs/DEPLOY.md
// §1). Only enabled, events, bytes and restore_timeout are operator
// settings; the rest are pinned engine defaults, copied into the plan at
// submit like the history reserve.

// DefaultContinuationEnabled is engine.continuation.enabled's default: on,
// since the continuation chaos scenarios (C1–C31) proved the writer, in a
// release after the one that first contained it — the order a rollback
// relies on (docs/DEPLOY.md §4).
const DefaultContinuationEnabled = true

// DefaultContinuationEvents is engine.continuation.events' default: a
// generation that reaches a moment with nothing in flight continues as new
// about this many events into its growth, under Temporal's 10,240-event
// warning.
const DefaultContinuationEvents int64 = 8192

// DefaultContinuationBytes is engine.continuation.bytes' default: the same,
// for history bytes, and the most a run copies into one history: its saved
// state, plus its definition when that is passed by reference.
const DefaultContinuationBytes int64 = 8 << 20

// DefaultContinuationHoldPercent is the hold line: a run holds new dispatch
// once its history plus its failure path's reserve reaches this share of the
// cluster's limit.
const DefaultContinuationHoldPercent int64 = 60

// DefaultContinuationMaxGenerations is how many generations one run may
// have. MaxGenerationsCeiling is the most a plan may pin: 256 event-segment
// references fit the 256 KiB carry with room for the rest of it.
const (
	DefaultContinuationMaxGenerations int64 = 256
	MaxGenerationsCeiling             int64 = 256
)

// DefaultContinuationChunkBytes and DefaultContinuationChunksPerTask are the
// bootstrap's read size and how many reads one workflow task makes before a
// yield: at most 1 MiB of saved state per workflow-task completion.
const (
	DefaultContinuationChunkBytes    int64 = 128 << 10
	DefaultContinuationChunksPerTask int64 = 8
)

// DefaultContinuationRestoreTimeout is engine.continuation.restore_timeout's
// default: how long a generation retries reading its saved state before it
// fails RUN_STATE_UNAVAILABLE.
const DefaultContinuationRestoreTimeout = time.Hour

// The retry policy of one bootstrap read: one attempt's budget, the first
// backoff and the largest.
const (
	DefaultRestoreAttempt        = 30 * time.Second
	DefaultRestoreBackoffInitial = time.Second
	DefaultRestoreBackoffMax     = time.Minute
)

// DefaultRunMaxInFlight is engine.run_max_in_flight's default: the most
// activities, child runs and pins one run keeps pending. More wait PENDING,
// as on a saturated pool. DefaultPendingActivityHardStop is absolute, an open
// pin's closure included, 100 below Temporal's 2,000 pending activities.
const (
	DefaultRunMaxInFlight          int32 = 1000
	DefaultPendingActivityHardStop int32 = 1900
)

// DefaultItemInputFloorBytes is the per-item input floor a run pins with its
// continuation budget (ContinuationBudget.ItemInputFloorBytes): a fan-out of
// up to dsl.MaxEachItems items still gives each item 16 KiB of activity
// input, where eachItemInputBudget alone would leave 1.6 KiB at 10,000. It
// binds only above 1,024 declared items.
const DefaultItemInputFloorBytes int64 = 16 << 10

// DefaultContinuationBudget is the budget pinRunTreeLimits pins on a new
// top-level plan when continuation is enabled: the deployment's events, bytes
// and restore timeout (engine.continuation.*), and every other number at its
// pinned default. The boot check in cmd/ork validates exactly this budget, so
// the bound it checks is the one every run then carries.
func DefaultContinuationBudget(events, bytes int64, restoreTimeout time.Duration) ContinuationBudget {
	return ContinuationBudget{
		Events:              events,
		Bytes:               bytes,
		CheckpointMaxBytes:  bytes,
		RetryEvents:         max(1, events/4),
		RetryBytes:          max(1, bytes/4),
		HoldPercent:         DefaultContinuationHoldPercent,
		MaxGenerations:      DefaultContinuationMaxGenerations,
		ChunkBytes:          DefaultContinuationChunkBytes,
		ChunksPerTask:       DefaultContinuationChunksPerTask,
		ItemInputFloorBytes: DefaultItemInputFloorBytes,
		Restore: RestorePolicy{
			TimeoutSeconds:       int64(restoreTimeout / time.Second),
			AttemptSeconds:       int64(DefaultRestoreAttempt / time.Second),
			BackoffInitialMillis: int64(DefaultRestoreBackoffInitial / time.Millisecond),
			BackoffMaxSeconds:    int64(DefaultRestoreBackoffMax / time.Second),
		},
	}
}

// DefaultInFlightLimit is the cap pinRunTreeLimits pins on every new
// top-level plan: the deployment's engine.run_max_in_flight and the pinned
// hard stop.
func DefaultInFlightLimit(maxInFlight int32) InFlightLimit {
	return InFlightLimit{Max: maxInFlight, HardStop: DefaultPendingActivityHardStop}
}

// DefaultSnapshotOverlays is engine.snapshot_overlays' default: off, so
// every run writes full snapshots, until an operator turns it on once every
// engine process runs a release that reads overlays (docs/DEPLOY.md §4, R4).
const DefaultSnapshotOverlays = false

// The snapshot-overlay thresholds a run pins when the deployment enables
// overlays (OverlayPolicy): a run declaring more than DefaultOverlayMinRows
// rows writes an overlay for a non-terminal event, a full snapshot after
// DefaultOverlayMaxChain overlays in a row, and a full snapshot whenever more
// than DefaultOverlayMaxRows rows differ from the base.
const (
	DefaultOverlayMinRows  int64 = 256
	DefaultOverlayMaxChain int64 = 64
	DefaultOverlayMaxRows  int64 = 256
)

// DefaultOverlayPolicy is the policy pinRunTreeLimits pins on a new
// top-level plan when overlays are enabled and the run tree can use them.
func DefaultOverlayPolicy() OverlayPolicy {
	return OverlayPolicy{MinRows: DefaultOverlayMinRows, MaxChain: DefaultOverlayMaxChain, MaxRows: DefaultOverlayMaxRows}
}

package engine

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

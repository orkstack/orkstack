package engine

import "time"

// The concurrency gate's wire types and memo layout, for the integration
// tests that drive a gate on a real cluster as P3's runs will: they send its
// acquire Update, read its answer, and give stand-in runs the memo entries a
// rebuild and a check pass read.

type (
	GateAcquireForTest   = gateAcquire
	GateAnswerForTest    = gateAnswer
	GateMemoEntryForTest = gateMemoEntry
	GateEntryForTest     = gateEntry
	GateResetForTest     = gateReset
)

const (
	GateProtoForTest        = gateProto
	GateProtoOrdinalForTest = gateProtoOrdinal
	GateRunLevelForTest     = gateRunLevel
	GateMemoSeqKeyForTest   = gateMemoSeqKey

	GateAnswerGrantedForTest     = gateAnswerGranted
	GateAnswerQueuedForTest      = gateAnswerQueued
	GateAnswerFencedForTest      = gateAnswerFenced
	GateAnswerUnavailableForTest = gateAnswerUnavailable
	GateAnswerFullForTest        = gateAnswerFull

	GateRebuildFailingForTest = gateRebuildFailing
	GatePhaseLingeringForTest = gatePhaseLingering
)

// GateMemoKeyForTest is the run memo key of an entry at gateID.
func GateMemoKeyForTest(gateID string, taskIx int) string { return gateMemoKey(gateID, taskIx) }

// GateLegacyTombCarryForTest is a gate's carried state as a release that
// predates slot ordinals wrote it: one slot tombstone for (run, seqBase) at
// run level, with no ordinal, standing until until. The gate reads it as
// fencing n = 0 only (§22.15).
func GateLegacyTombCarryForTest(tun GateTunables, run string, seqBase int64, until time.Time) *gateState {
	return &gateState{Tunables: tun, Phase: gatePhaseActive,
		Tombstones: []gateTomb{{Key: slotTombKey(run, seqBase, gateRunLevel), Until: until}}}
}

package engine

// The concurrency gate's wire types and memo layout, for the integration
// tests that drive a gate on a real cluster as P3's runs will: they send its
// acquire Update, read its answer, and give stand-in runs the memo entries a
// rebuild and a check pass read.

type (
	GateAcquireForTest   = gateAcquire
	GateAnswerForTest    = gateAnswer
	GateMemoEntryForTest = gateMemoEntry
)

const (
	GateProtoForTest      = gateProto
	GateRunLevelForTest   = gateRunLevel
	GateMemoSeqKeyForTest = gateMemoSeqKey

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

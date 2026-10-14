package engine

import "go.temporal.io/sdk/workflow"

// HistoryGuardLookaheadChange is the workflow.GetVersion change id of the
// guard's lookahead at a dispatch.
//
// The guard checks, before each dispatch, that the run's history plus what
// its own failure path would append still fits under the cluster's limit.
// Checked only as it stands, one dispatch could pass and then record a
// command larger than the room left — a task's input is up to the plan's
// values bound, 256 KiB — leaving the failure path less than it keeps back,
// so the cluster terminated the run instead (continuation chaos C24: a
// 128 KiB input per dispatch, terminated at 50 MiB). The lookahead asks the
// same question with the dispatch's own command counted: a run stops
// RUN_HISTORY_LIMIT before a command its failure path could not follow.
//
// It is asked LAZILY, only where the lookahead decides otherwise than the
// check without it: a run never in that gap records no marker, and a history
// recorded before the lookahead replays its DefaultVersion branch, which
// dispatched. A carried generation answers from its carry (version).
const HistoryGuardLookaheadChange = "run-history-guard-lookahead"

// historyGuardLookaheadDisabledForTest is a TEST-ONLY switch, the precedent
// of eachDispatchDescending: when set, the lookahead is skipped WITHOUT
// asking its version, so a negative replay test proves the committed fixture
// can only replay because of it. Never set in production code.
var historyGuardLookaheadDisabledForTest = false

// historyExhaustedBefore is historyExhausted at a dispatch that will record
// a command of up to nextBytes: the run stops RUN_HISTORY_LIMIT now when it
// would not be able to end cleanly once that command is recorded.
func (s *runState) historyExhaustedBefore(ctx workflow.Context, inFlight map[string]workflow.Future, nextBytes int64) error {
	if err := s.historyExhausted(ctx, inFlight); err != nil {
		return err
	}
	if nextBytes <= 0 || s.guardOff || historyGuardDisabledForTest || historyGuardLookaheadDisabledForTest {
		return nil
	}
	limit := s.historyLimit()
	if limit == nil {
		return nil
	}
	var c historyCrossing
	c.events, c.bytes = s.historyNow(ctx)
	c.maxEvents, c.maxBytes = limit.MaxEvents, limit.MaxBytes
	c.reserveEvents, c.reserveBytes = s.quickReserve(inFlight, limit.Reserve)
	if c.bytes+nextBytes+c.reserveBytes < c.maxBytes {
		return nil
	}
	c.reserveEvents, c.reserveBytes = s.reserveNow(inFlight)
	if c.bytes+nextBytes+c.reserveBytes < c.maxBytes {
		return nil
	}
	if s.version(ctx, HistoryGuardLookaheadChange, 1) < 1 {
		return nil
	}
	if !s.guardAdopted(ctx) {
		return nil
	}
	return s.historyLimitError(ctx, inFlight, c, nextBytes)
}

// nextDispatchBytes is the most one dispatch of id can record in a single
// command — exactly what its dispatch notes (noteCommand): a task's input up
// to the values bound, a child run's start, or a pin's session; nothing for
// an await or a fan-out, whose items are checked one by one.
func (s *runState) nextDispatchBytes(id string) int64 {
	limit := s.historyLimit()
	if limit == nil {
		return 0
	}
	switch {
	case s.awaits[id] != nil, s.eaches[id] != nil:
		return 0
	case s.children[id] != nil:
		return limit.Reserve.ChildStartBytes
	}
	n := s.valuesBound() + limit.Reserve.CommandBytes
	if p, err := s.pinFor(id); err == nil && p != nil && !p.opened {
		n = max(n, limit.Reserve.SessionBytes)
	}
	return n
}

// nextItemBytes is what one item dispatch of a fan-out records.
func (s *runState) nextItemBytes() int64 {
	limit := s.historyLimit()
	if limit == nil {
		return 0
	}
	return s.itemInputLimit + limit.Reserve.CommandBytes
}

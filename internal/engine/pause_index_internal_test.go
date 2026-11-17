package engine

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/task"
)

// indexLog records what a run wrote to its paused attribute and its pause
// memo key, in order: "set"/"unset" for the attribute, the expiry or "nil"
// for the memo.
type indexLog struct{ attr, memo []string }

func (h *timeoutHarness) recordPauseIndex() *indexLog {
	log := &indexLog{}
	key := temporal.NewSearchAttributeKeyKeyword(task.SearchAttrOrkflowRunPaused)
	h.env.OnUpsertTypedSearchAttributes(mock.Anything).Run(func(args mock.Arguments) {
		// The SDK hides an unset key from every accessor (it holds nil, which
		// GetUntypedValues filters out), so an upsert that carries no value
		// is the unset: the run issues no other attribute write.
		sa := args.Get(0).(temporal.SearchAttributes)
		v, ok := sa.GetUntypedValues()[key]
		switch {
		case sa.Size() == 0:
			log.attr = append(log.attr, "unset")
		case !ok:
		case v == "true":
			log.attr = append(log.attr, "set")
		default:
			log.attr = append(log.attr, fmt.Sprintf("%v", v))
		}
	}).Return(nil).Maybe()
	h.env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(map[string]any)
		v, ok := m[MemoPauseExpiresAt]
		if !ok {
			return
		}
		if v == nil {
			log.memo = append(log.memo, "nil")
			return
		}
		log.memo = append(log.memo, v.(string))
	}).Return(nil).Maybe()
	return log
}

// An Indexed run sets OrkflowRunPaused and its pause memo when it pauses and
// removes both when it unpauses, so a list can tell it is paused and when its
// pause ends.
func TestPause_IndexedRunUpsertsItsPause(t *testing.T) {
	h := newTimeoutHarness(t)
	log := h.recordPauseIndex()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	plan := pausePlan(t)
	plan.Pause.Indexed = true
	if err := h.run(t, twoStepDocument, plan); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !slices.Equal(log.attr, []string{"set", "unset"}) {
		t.Fatalf("attribute writes %v, want [set unset]", log.attr)
	}
	if len(log.memo) != 2 || log.memo[1] != "nil" {
		t.Fatalf("memo writes %v, want [<expiry> nil]", log.memo)
	}
	expires, err := time.Parse(time.RFC3339Nano, log.memo[0])
	if err != nil || expires.Before(time.Unix(0, 0)) {
		t.Fatalf("memo expiry %q is not an RFC 3339 instant: %v", log.memo[0], err)
	}
	// What a run adds is far under the cluster's limits (2 KiB per attribute
	// value, 2 MiB of memo per event by default), so the cluster can refuse
	// it only for an attribute it does not index.
	if n := len(MemoPauseExpiresAt) + len(log.memo[0]); n > 64 {
		t.Fatalf("the pause memo adds %d bytes, want at most 64", n)
	}
}

// A run whose plan is not Indexed pauses with no attribute and no memo: the
// cluster may refuse an attribute it never accepted.
func TestPause_UnindexedRunWritesNoIndex(t *testing.T) {
	h := newTimeoutHarness(t)
	log := h.recordPauseIndex()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	if err := h.run(t, twoStepDocument, pausePlan(t)); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if len(log.attr) != 0 || len(log.memo) != 0 {
		t.Fatalf("attribute writes %v, memo writes %v; want none", log.attr, log.memo)
	}
}

// A paused-attribute write the cluster refuses is retried at the run's next
// pass, never recorded as written: the index says what the pause is once a
// write lands.
func TestPause_RefusedIndexWriteIsRetried(t *testing.T) {
	refused := 0
	fault := func(what string) error {
		if what == "attribute" && refused == 0 {
			refused++
			return errors.New("refused by the test")
		}
		return nil
	}
	pauseIndexWriteFaultForTest.Store(&fault)
	t.Cleanup(func() { pauseIndexWriteFaultForTest.Store(nil) })
	h := newTimeoutHarness(t)
	log := h.recordPauseIndex()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	plan := pausePlan(t)
	plan.Pause.Indexed = true
	if err := h.run(t, twoStepDocument, plan); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if refused != 1 || !slices.Equal(log.attr, []string{"set", "unset"}) {
		t.Fatalf("%d refused, then attribute writes %v; want 1 refused, then [set unset]", refused, log.attr)
	}
}

// A pause whose expiry ends the run clears the run's index at once: the run
// may still be stopping its work, and it is not paused while it does.
func TestPause_ExpiryEndingTheRunClearsTheIndex(t *testing.T) {
	h := newTimeoutHarness(t)
	log := h.recordPauseIndex()
	h.taskTakes(10*time.Minute, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "60")) }, 5*time.Second)
	plan := pausePlan(t)
	plan.Pause.Indexed = true
	requireCode(t, h.run(t, twoStepDocument, plan), RunPauseTimeoutCode)
	if !slices.Equal(log.attr, []string{"set", "unset"}) || len(log.memo) != 2 || log.memo[1] != "nil" {
		t.Fatalf("attribute writes %v, memo writes %v; want [set unset] and [<expiry> nil]", log.attr, log.memo)
	}
}

// One applied request's reserve covers its index writes too: the attribute
// and the memo, beside the request's own commands and event.
func TestDefaultPausePlan_ReserveCoversTheIndexWrites(t *testing.T) {
	const indexWrites, eventAndTimer = 2, 8
	if p := DefaultPausePlan(1, 1); p.ApplyReserveEvents < indexWrites+eventAndTimer {
		t.Fatalf("ApplyReserveEvents %d < %d", p.ApplyReserveEvents, indexWrites+eventAndTimer)
	}
}

package engine

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var interruptionNow = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

// toWire is an error as the workflow receives it, its details encoded,
// through the SDK's own failure converter (as category_internal_test.go).
func toWire(f *failurepb.Failure) error {
	return temporal.GetDefaultFailureConverter().FailureToError(f)
}

// payloads encodes v as the SDK does; nil is no details. The values these
// tests pass always encode, so a failure is a broken test and panics.
func payloads(v any) *commonpb.Payloads {
	if v == nil {
		return nil
	}
	p, err := converter.GetDefaultDataConverter().ToPayloads(v)
	if err != nil {
		panic(fmt.Sprintf("encoding %v: %v", v, err))
	}
	return p
}

// typedInterruption is the error an Orker reports for a typed interruption.
func typedInterruption(reason string, detail *orkstackv1.ActivityFailureDetail) error {
	if detail == nil {
		detail = &orkstackv1.ActivityFailureDetail{}
	}
	detail.ReasonCode = reason
	return toWire(&failurepb.Failure{Message: reason, FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: interruptionTypePrefix + reason, NonRetryable: true, Details: payloads(detail)},
	}})
}

// heartbeatLoss is the cluster's heartbeat timeout whose last beat was beat
// (nil: no details).
func heartbeatLoss(beat any) error {
	return toWire(&failurepb.Failure{Message: "activity heartbeat timeout", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
		TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, LastHeartbeatDetails: payloads(beat)},
	}})
}

func testBudgets() unitBudgets {
	return unitBudgets{
		lost: 3, lostRunCap: 20, handedBack: 10, neverStarted: 20, faults: 5,
		backoff: 2 * time.Second, faultBackoff: 2 * time.Second,
	}
}

func classify(err error) classifiedInterruption {
	return classifyInterruption(categoryOf(err), true, interruptionNow)
}

// TestClassifyInterruption is §4.7's signal table: a typed interruption by
// its reason and never_started mark, a heartbeat timeout only while lost is
// listed, anything else not an interruption.
func TestClassifyInterruption(t *testing.T) {
	stopping := time.Date(2026, 11, 16, 9, 59, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		err        error
		lostListed bool
		want       interruptionKind
		reason     string
		started    bool
		stopping   bool
	}{
		{"never started stop", typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{NeverStarted: true}), true, neverStartedStop, "ORKER_STOPPED", false, false},
		{"handed back", typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true}), true, handedBack, "ORKER_STOPPED", true, false},
		{"stopped by runtime", typedInterruption("CONTAINER_STOPPED_BY_RUNTIME", &orkstackv1.ActivityFailureDetail{Started: true}), true, handedBack, "CONTAINER_STOPPED_BY_RUNTIME", true, false},
		{"store fault", typedInterruption("ATTEMPT_STORE_UNAVAILABLE", &orkstackv1.ActivityFailureDetail{NeverStarted: true, StoreError: "403"}), true, neverStartedFault, "ATTEMPT_STORE_UNAVAILABLE", false, false},
		{"runtime state unknown", typedInterruption("RUNTIME_STATE_UNKNOWN", nil), true, neverStartedFault, "RUNTIME_STATE_UNKNOWN", false, false},
		{"profile drifted", typedInterruption("PROFILE_DRIFTED", &orkstackv1.ActivityFailureDetail{NeverStarted: true}), true, neverStartedFault, "PROFILE_DRIFTED", false, false},
		{"unknown typed reason", typedInterruption("SOMETHING_ELSE", nil), true, notInterruption, "", false, false},
		{"category typed", temporal.NewApplicationError("x", "InfrastructureError", &orkstackv1.ActivityFailureDetail{ReasonCode: "ORKER_STOPPED"}), true, notInterruption, "", false, false},
		{"heartbeat lost", heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "started": true}), true, lost, orkerHeartbeatLostReason, true, false},
		{"heartbeat lost while stopping", heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": stopping.Format(time.RFC3339Nano)}), true, lost, "ORKER_STOPPED", false, true},
		{"heartbeat run without the started mark", heartbeatLoss(map[string]any{"phase": "run"}), true, lost, orkerHeartbeatLostReason, false, false},
		{"heartbeat with no details", heartbeatLoss(nil), true, lost, orkerHeartbeatLostReason, false, false},
		{"heartbeat not listed", heartbeatLoss(map[string]any{"phase": "run"}), false, notInterruption, "", false, false},
		{"start to close", temporal.NewTimeoutError(0, nil), true, notInterruption, "", false, false},
		{"nil", nil, true, notInterruption, "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyInterruption(categoryOf(tc.err), tc.lostListed, interruptionNow)
			if got.kind != tc.want {
				t.Fatalf("kind = %v, want %v", got.kind, tc.want)
			}
			if got.kind == notInterruption {
				return
			}
			if got.event.GetReason() != tc.reason || got.started != tc.started {
				t.Errorf("reason %q started %v, want %q %v", got.event.GetReason(), got.started, tc.reason, tc.started)
			}
			if (got.event.GetOrkerStoppingSince() != nil) != tc.stopping {
				t.Errorf("orker_stopping_since = %v, want set %v", got.event.GetOrkerStoppingSince(), tc.stopping)
			}
			if !got.event.GetAt().AsTime().Equal(interruptionNow) {
				t.Errorf("at = %v", got.event.GetAt().AsTime())
			}
		})
	}
	store := classify(typedInterruption("ATTEMPT_STORE_UNAVAILABLE",
		&orkstackv1.ActivityFailureDetail{NeverStarted: true, StoreError: "403 Forbidden", OrkerId: "o2", Attempt: 2}))
	if store.event.GetStoreError() != "403 Forbidden" || store.event.GetOrkerId() != "o2" || store.attempt != 2 || !store.event.GetNeverStarted() {
		t.Errorf("store fault event = %v attempt %d", store.event, store.attempt)
	}
	if store.event.GetPhase() != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START {
		t.Errorf("a never-started fault's phase is %v", store.event.GetPhase())
	}
	reported := classify(heartbeatLoss(map[string]any{"phase": "reported", "attempt": 2, "ordinal": 7}))
	if reported.attempt != 3 || reported.event.GetOrdinal() != 7 || reported.event.GetPhase() != orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_REPORTED {
		t.Errorf("a loss after attempt 2's reported beat: attempt %d event %v", reported.attempt, reported.event)
	}
	garbled := classify(heartbeatLoss("not a beat"))
	if garbled.kind != lost || garbled.attempt != 1 || garbled.started {
		t.Errorf("a loss whose details do not decode: %+v", garbled)
	}
}

// TestInterruptionTypes is the list an attempt's retry policy carries: the
// never-started class always (it never closes), CONTAINER_STOPPED_BY_RUNTIME
// while hand-backs have budget, the heartbeat timeout while losses do and
// the run's loss events are under their cap, nothing for an opt-out or a
// run without budgets, in one fixed order.
func TestInterruptionTypes(t *testing.T) {
	all := []string{
		"OrkInterruption:ORKER_STOPPED", "OrkInterruption:ATTEMPT_STORE_UNAVAILABLE",
		"OrkInterruption:RUNTIME_STATE_UNKNOWN", "OrkInterruption:PROFILE_DRIFTED",
		"OrkInterruption:CONTAINER_STOPPED_BY_RUNTIME", heartbeatTimeoutType,
	}
	b := testBudgets()
	for _, tc := range []struct {
		name  string
		b     *unitBudgets
		st    *orkstackv1.TaskInterruptions
		count int32
		want  []string
	}{
		{"fresh", &b, nil, 0, all},
		{"no budgets", nil, nil, 0, nil},
		{"hand-backs spent", &b, &orkstackv1.TaskInterruptions{HandedBack: 10}, 0, slices.Delete(slices.Clone(all), 4, 5)},
		{"losses spent", &b, &orkstackv1.TaskInterruptions{Lost: 3}, 0, all[:5]},
		{"loss cap reached", &b, nil, 20, all[:5]},
		{"never-started past its budget still listed", &b, &orkstackv1.TaskInterruptions{NeverStarted: 500, StoreFaults: 5}, 0, all},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := interruptionTypes(tc.b, tc.st, &orkstackv1.RunLossEvents{Count: tc.count})
			if !slices.Equal(got, tc.want) {
				t.Errorf("types = %v, want %v", got, tc.want)
			}
		})
	}
	zero := testBudgets()
	zero.lost, zero.handedBack = 0, 0
	if got := interruptionTypes(&zero, nil, nil); !slices.Equal(got, all[:4]) {
		t.Errorf("lost and handed back capped at 0 list %v, want the never-started class alone", got)
	}
}

// TestUnitBudgets_MaxInterruptions: absent keeps the plan's budgets; 0 opts
// out (nil, nothing listed); n caps lost and handed back at min(plan, n) and
// never raises them, and leaves the never-started class alone. The fault
// back-off's initial wait is floored at the store-outage detection time.
func TestUnitBudgets_MaxInterruptions(t *testing.T) {
	plan := &InterruptionBudgets{Lost: 3, LostRunCap: 20, HandedBack: 10, NeverStarted: 20, NeverStartedFaults: 5, NeverStartedBackoffMillis: 2000}
	n := func(v int32) *int32 { return &v }
	if got := budgetsForUnit(nil, n(5), 0); got != nil {
		t.Errorf("a run with no budgets gives %+v", got)
	}
	if got := budgetsForUnit(plan, n(0), 0); got != nil {
		t.Errorf("max_interruptions 0 gives %+v, want nil", got)
	}
	got := budgetsForUnit(plan, nil, 45*time.Second)
	if got.lost != 3 || got.lostRunCap != 20 || got.handedBack != 10 || got.neverStarted != 20 || got.faults != 5 ||
		got.backoff != 2*time.Second || got.faultBackoff != 45*time.Second {
		t.Errorf("absent: %+v", got)
	}
	got = budgetsForUnit(plan, n(2), time.Second)
	if got.lost != 2 || got.handedBack != 2 || got.neverStarted != 20 || got.faults != 5 || got.faultBackoff != 2*time.Second {
		t.Errorf("max_interruptions 2: %+v", got)
	}
	if got := budgetsForUnit(plan, n(50), 0); got.lost != 3 || got.handedBack != 10 {
		t.Errorf("max_interruptions 50 raised a budget: %+v", got)
	}
}

func applyN(t *testing.T, st *orkstackv1.TaskInterruptions, b unitBudgets, m int32, runLoss *orkstackv1.RunLossEvents, ev classifiedInterruption, n int) []interruptionDecision {
	t.Helper()
	var out []interruptionDecision
	for range n {
		out = append(out, applyInterruption(st, b, m, time.Minute, runLoss, ev, interruptionNow))
	}
	return out
}

func storeFault() classifiedInterruption {
	return classify(typedInterruption("ATTEMPT_STORE_UNAVAILABLE",
		&orkstackv1.ActivityFailureDetail{NeverStarted: true, StoreError: "403 Forbidden", Attempt: 1}))
}

func startedHandBack() classifiedInterruption {
	return classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true}))
}

func stopBounce() classifiedInterruption {
	return classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{NeverStarted: true}))
}

// TestInterruption_StoreFaultBudgetThenOwnThenRealReason: a task with two
// own attempts survives five consecutive store faults, the first with no
// wait and the rest doubling; each further fault past a fresh budget of five
// spends one own attempt, and the last fails with the real reason and the
// store's error.
func TestInterruption_StoreFaultBudgetThenOwnThenRealReason(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{}
	b := testBudgets()
	ev := storeFault()
	wantWaits := []time.Duration{0, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for i, d := range applyN(t, st, b, 2, nil, ev, 5) {
		if !d.redispatch || d.fail || d.wait != wantWaits[i] {
			t.Fatalf("fault %d: %+v, want a re-dispatch after %s", i+1, d, wantWaits[i])
		}
	}
	d := applyInterruption(st, b, 2, time.Minute, nil, ev, interruptionNow)
	if !d.redispatch || d.fail || st.GetOwnAttemptsUsed() != 1 || st.GetStoreFaults() != 0 {
		t.Fatalf("the sixth fault: %+v, own %d faults %d; want an own attempt spent and the budget fresh", d, st.GetOwnAttemptsUsed(), st.GetStoreFaults())
	}
	for i, d := range applyN(t, st, b, 2, nil, ev, 5) {
		if !d.redispatch {
			t.Fatalf("second round fault %d: %+v", i+1, d)
		}
	}
	d = applyInterruption(st, b, 2, time.Minute, nil, ev, interruptionNow)
	if !d.fail || d.redispatch || d.closedBy != closedByNeverStarted {
		t.Fatalf("the twelfth fault: %+v, want the task failed", d)
	}
	last := st.GetLast()
	if last.GetReason() != "ATTEMPT_STORE_UNAVAILABLE" || last.GetStoreError() != "403 Forbidden" || last.GetClosedBy() != closedByNeverStarted {
		t.Errorf("last = %v", last)
	}
	if d.message == "" {
		t.Error("the failure says nothing of its interruptions")
	}
}

// TestInterruption_ProfileDriftedIsNeverStartedFault: a drift spends the
// fault budget, not the lost one, and never a loss event; a one-attempt task
// fails PROFILE_DRIFTED only after its five.
func TestInterruption_ProfileDriftedIsNeverStartedFault(t *testing.T) {
	ev := classify(typedInterruption("PROFILE_DRIFTED", &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o1"}))
	if ev.kind != neverStartedFault {
		t.Fatalf("PROFILE_DRIFTED classifies as %v", ev.kind)
	}
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	ds := applyN(t, st, testBudgets(), 1, runLoss, ev, 6)
	for i, d := range ds[:5] {
		if !d.redispatch {
			t.Fatalf("drift %d: %+v", i+1, d)
		}
	}
	if !ds[5].fail || st.GetLast().GetReason() != "PROFILE_DRIFTED" {
		t.Fatalf("the sixth drift: %+v last %v", ds[5], st.GetLast())
	}
	if st.GetLost() != 0 || runLoss.GetCount() != 0 || st.GetNeverStarted() != 0 {
		t.Errorf("a drift spent lost %d, loss events %d, never_started %d", st.GetLost(), runLoss.GetCount(), st.GetNeverStarted())
	}
}

// TestInterruption_ResetRuleOnStart: an interruption of an attempt whose
// container started resets never_started and store_faults, never lost or
// handed_back; one that never started resets nothing, and a never-started
// stop bounce neither spends nor resets store_faults.
func TestInterruption_ResetRuleOnStart(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{NeverStarted: 25, StoreFaults: 4, Lost: 1, HandedBack: 2}
	applyInterruption(st, testBudgets(), 1, time.Minute, nil, stopBounce(), interruptionNow)
	if st.GetStoreFaults() != 4 || st.GetNeverStarted() != 26 {
		t.Fatalf("a stop bounce: faults %d never_started %d, want 4 and 26", st.GetStoreFaults(), st.GetNeverStarted())
	}
	applyInterruption(st, testBudgets(), 1, time.Minute, nil, storeFault(), interruptionNow)
	if st.GetNeverStarted() != 26 || st.GetStoreFaults() != 5 {
		t.Fatalf("a fault: never_started %d faults %d, want 26 and 5", st.GetNeverStarted(), st.GetStoreFaults())
	}
	applyInterruption(st, testBudgets(), 1, time.Minute, nil, startedHandBack(), interruptionNow)
	if st.GetNeverStarted() != 0 || st.GetStoreFaults() != 0 || st.GetLost() != 1 || st.GetHandedBack() != 3 {
		t.Errorf("after a started hand-back: %v", st)
	}
	// A loss whose last beat says its container started counts as started.
	st = &orkstackv1.TaskInterruptions{NeverStarted: 3, StoreFaults: 2}
	loss := classify(heartbeatLoss(map[string]any{"phase": "run", "started": true, "orker_id": "o1"}))
	applyInterruption(st, testBudgets(), 1, time.Minute, &orkstackv1.RunLossEvents{}, loss, interruptionNow)
	if st.GetNeverStarted() != 0 || st.GetStoreFaults() != 0 {
		t.Errorf("a loss after a started beat left %v", st)
	}
	// One whose last beat is from before the start resets nothing.
	st = &orkstackv1.TaskInterruptions{NeverStarted: 3, StoreFaults: 2}
	loss = classify(heartbeatLoss(map[string]any{"phase": "image", "orker_id": "o1"}))
	applyInterruption(st, testBudgets(), 1, time.Minute, &orkstackv1.RunLossEvents{}, loss, interruptionNow)
	if st.GetNeverStarted() != 3 || st.GetStoreFaults() != 2 {
		t.Errorf("a loss before the start reset %v", st)
	}
}

// TestInterruption_BackoffExponentClamped: past the never-started budget the
// wait doubles from the plan's initial, its exponent clamped at 6 and the
// wait at 60 s; never_started at 10³ and at int32's maximum stays in
// (0, 60 s] without wrapping, and a stop bounce never fails or spends an own
// attempt.
func TestInterruption_BackoffExponentClamped(t *testing.T) {
	b := testBudgets()
	for _, tc := range []struct {
		before int32
		want   time.Duration
	}{
		{0, 0}, {19, 0}, {20, 2 * time.Second}, {21, 4 * time.Second}, {22, 8 * time.Second},
		{24, 32 * time.Second}, {25, 60 * time.Second}, {1000, 60 * time.Second}, {math.MaxInt32 - 1, 60 * time.Second},
		{math.MaxInt32, 60 * time.Second},
	} {
		st := &orkstackv1.TaskInterruptions{NeverStarted: tc.before}
		d := applyInterruption(st, b, 1, time.Minute, nil, stopBounce(), interruptionNow)
		if !d.redispatch || d.fail || d.wait != tc.want || st.GetOwnAttemptsUsed() != 0 {
			t.Errorf("never_started %d: %+v own %d, want a re-dispatch after %s", tc.before, d, st.GetOwnAttemptsUsed(), tc.want)
		}
		if st.GetNeverStarted() < tc.before {
			t.Errorf("never_started wrapped from %d to %d", tc.before, st.GetNeverStarted())
		}
	}
	for _, n := range []int32{-5, 0, 1, 2, 6, 9, 1000, math.MaxInt32} {
		if w := backoffWait(2*time.Second, n); w <= 0 || w > 60*time.Second {
			t.Errorf("backoffWait(2s, %d) = %s", n, w)
		}
	}
	if w := backoffWait(0, 3); w <= 0 {
		t.Errorf("a zero initial waits %s", w)
	}
	if w := backoffWait(time.Hour, 0); w != 60*time.Second {
		t.Errorf("an initial above the cap waits %s", w)
	}
}

// TestInterruption_InterleavedStartsAndFaultsTerminate: a unit whose faults
// are reset by started attempts still ends, because every reset is paid by
// a hand-back, a loss or an own attempt, none of which resets.
func TestInterruption_InterleavedStartsAndFaultsTerminate(t *testing.T) {
	b := testBudgets()
	const m = 3
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	bound := (b.lost + b.handedBack + m) * (b.faults*m + 1)
	for i := int32(0); ; i++ {
		if i > bound {
			t.Fatalf("still re-dispatching after %d interruptions (bound %d): %v", i, bound, st)
		}
		ev := storeFault()
		if i%b.faults == b.faults-1 {
			ev = startedHandBack()
		}
		if d := applyInterruption(st, b, m, time.Minute, runLoss, ev, interruptionNow); d.fail {
			return
		}
	}
}

// TestInterruption_HandBackThenOwnAttempts: ten hand-backs are free; the
// eleventh spends an own attempt, and with none left the task fails
// ORKER_STOPPED, closed by handed_back.
func TestInterruption_HandBackThenOwnAttempts(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{}
	for i, d := range applyN(t, st, testBudgets(), 1, nil, startedHandBack(), 10) {
		if !d.redispatch || d.wait != 0 {
			t.Fatalf("hand-back %d: %+v", i+1, d)
		}
	}
	d := applyInterruption(st, testBudgets(), 1, time.Minute, nil, startedHandBack(), interruptionNow)
	if !d.fail || d.closedBy != closedByHandedBack || st.GetOwnAttemptsUsed() != 1 {
		t.Fatalf("the eleventh: %+v own %d", d, st.GetOwnAttemptsUsed())
	}
}

// TestInterruption_LossWaitsAndSpendsLost: a loss waits the loss wait and
// spends lost; past three the task spends own attempts, closed by lost.
func TestInterruption_LossWaitsAndSpendsLost(t *testing.T) {
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	for _, orker := range []string{"oa", "ob", "oc"} {
		d := applyInterruption(st, testBudgets(), 2, 90*time.Second, runLoss, classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": orker})), interruptionNow)
		if !d.redispatch || d.wait != 90*time.Second {
			t.Fatalf("loss on %s: %+v", orker, d)
		}
	}
	d := applyInterruption(st, testBudgets(), 2, 90*time.Second, runLoss, classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "od"})), interruptionNow)
	if !d.redispatch || d.wait != 0 || st.GetOwnAttemptsUsed() != 1 || st.GetLost() != 3 || st.GetLast().GetClosedBy() != closedByLost {
		t.Fatalf("the fourth loss: %+v %v", d, st)
	}
	if runLoss.GetCount() != 4 {
		t.Errorf("four Orkers lost count %d events", runLoss.GetCount())
	}
}

// TestInterruption_LossEventsJoinWithinWindow: losses on one Orker within
// the window are one event, whichever units they hit; a loss naming no
// Orker is its own; the cap counts events, and a loss past it closes
// lost_run_cap.
func TestInterruption_LossEventsJoinWithinWindow(t *testing.T) {
	runLoss := &orkstackv1.RunLossEvents{}
	b := testBudgets()
	b.lostRunCap = 2
	loss := func(orker string, at time.Time) classifiedInterruption {
		beat := map[string]any{"phase": "run"}
		if orker != "" {
			beat["orker_id"] = orker
		}
		return classifyInterruption(categoryOf(heartbeatLoss(beat)), true, at)
	}
	for range 3 {
		applyInterruption(&orkstackv1.TaskInterruptions{}, b, 1, time.Minute, runLoss, loss("o1", interruptionNow), interruptionNow)
	}
	if runLoss.GetCount() != 1 || len(runLoss.GetOpen()) != 1 {
		t.Fatalf("three units on one Orker: %v", runLoss)
	}
	later := interruptionNow.Add(2 * time.Minute)
	if d := applyInterruption(&orkstackv1.TaskInterruptions{}, b, 1, time.Minute, runLoss, loss("o1", later), later); !d.redispatch || d.fail {
		t.Fatalf("the loss that brings the events to the cap: %+v, want a re-dispatch", d)
	}
	if runLoss.GetCount() != 2 || len(runLoss.GetOpen()) != 1 || !runLoss.GetOpen()[0].GetFirstAt().AsTime().Equal(later) {
		t.Fatalf("a loss on o1 after the window: %v", runLoss)
	}
	st := &orkstackv1.TaskInterruptions{}
	d := applyInterruption(st, b, 1, time.Minute, runLoss, loss("", later), later)
	if runLoss.GetCount() != 3 || !d.fail || d.closedBy != closedByLostRunCap {
		t.Fatalf("an unnamed loss past the cap: %+v %v", d, runLoss)
	}
}

// TestInterruption_LostWhileStoppingSpendsHandBackBudget (E92-N2): a loss
// whose last beat says the Orker was stopping keeps the loss wait but
// spends handed_back, counts lost_while_stopping and opens no loss event.
func TestInterruption_LostWhileStoppingSpendsHandBackBudget(t *testing.T) {
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	stopping := interruptionNow.Add(-time.Minute).Format(time.RFC3339Nano)
	ev := classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": stopping}))
	d := applyInterruption(st, testBudgets(), 1, 90*time.Second, runLoss, ev, interruptionNow)
	if !d.redispatch || d.wait != 90*time.Second || st.GetHandedBack() != 1 || st.GetLostWhileStopping() != 1 || st.GetLost() != 0 || runLoss.GetCount() != 0 {
		t.Fatalf("%+v %v %v", d, st, runLoss)
	}
}

// TestInterruption_LostWhileStoppingAfterHandBacksSpentFallsToOwn (R11-2):
// with handed_back spent it spends the task's own attempts and fails
// ORKER_STOPPED, closed by handed_back.
func TestInterruption_LostWhileStoppingAfterHandBacksSpentFallsToOwn(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{HandedBack: 10}
	ev := classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": interruptionNow.Format(time.RFC3339Nano)}))
	d := applyInterruption(st, testBudgets(), 1, time.Minute, &orkstackv1.RunLossEvents{}, ev, interruptionNow)
	if !d.fail || d.closedBy != closedByHandedBack || st.GetLast().GetReason() != "ORKER_STOPPED" {
		t.Fatalf("%+v last %v", d, st.GetLast())
	}
}

// TestInterruption_OwnAttemptsBeforeTheInterruption: an interruption at
// attempt k of an execution spends the k−1 own attempts before it, k is
// clamped to what that execution had, and the next execution has what
// remains, never fewer than one.
func TestInterruption_OwnAttemptsBeforeTheInterruption(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{}
	back := classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true, Attempt: 3}))
	applyInterruption(st, testBudgets(), 5, time.Minute, nil, back, interruptionNow)
	if st.GetOwnAttemptsUsed() != 2 || remainingAttempts(5, st) != 3 {
		t.Fatalf("own %d remaining %d", st.GetOwnAttemptsUsed(), remainingAttempts(5, st))
	}
	back.attempt = 9
	applyInterruption(st, testBudgets(), 5, time.Minute, nil, back, interruptionNow)
	if st.GetOwnAttemptsUsed() != 4 || remainingAttempts(5, st) != 1 {
		t.Fatalf("own %d remaining %d", st.GetOwnAttemptsUsed(), remainingAttempts(5, st))
	}
	if remainingAttempts(1, &orkstackv1.TaskInterruptions{OwnAttemptsUsed: 7}) != 1 {
		t.Error("remaining attempts fell below one")
	}
	if remainingAttempts(0, nil) != 1 {
		t.Error("a task without retry: has no attempt")
	}
}

// TestInterruption_NotBeforeIsAbsolute: a decision with a wait records the
// absolute instant on the unit, so a continuation can re-arm it; one with
// none clears it.
func TestInterruption_NotBeforeIsAbsolute(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{}
	loss := classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1"}))
	applyInterruption(st, testBudgets(), 1, 90*time.Second, &orkstackv1.RunLossEvents{}, loss, interruptionNow)
	if !st.GetNotBefore().AsTime().Equal(interruptionNow.Add(90 * time.Second)) {
		t.Fatalf("not_before = %v", st.GetNotBefore())
	}
	applyInterruption(st, testBudgets(), 1, 90*time.Second, nil, startedHandBack(), interruptionNow)
	if st.GetNotBefore() != nil {
		t.Fatalf("a hand-back left not_before %v", st.GetNotBefore())
	}
}

// TestInterruption_FailureMessageNamesTheBudgets: a task failed past its
// interruptions says how many and which budget closed.
func TestInterruption_FailureMessageNamesTheBudgets(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{HandedBack: 10, Lost: 1}
	d := applyInterruption(st, testBudgets(), 1, time.Minute, nil, startedHandBack(), interruptionNow)
	want := "interrupted 12 times (lost 1/3, handed back 10/10, lost Orkers 0/20: handed_back); see orkctl run attempts"
	if d.message != want {
		t.Fatalf("message = %q\nwant      %q", d.message, want)
	}
}

// TestClassifyInterruption_UndecodedDetailsAreMarked: a typed interruption
// with no details, or details that do not decode, never panics; it is
// budgeted conservatively (a hand-back, never the never-fails class) and
// marked undecoded so the interpreter logs it. A heartbeat detail that does
// not decode is marked too; one that is simply absent (a loss before the
// first beat) is not.
func TestClassifyInterruption_UndecodedDetailsAreMarked(t *testing.T) {
	noDetails := toWire(&failurepb.Failure{Message: "x", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: interruptionTypePrefix + orkerStoppedReason, NonRetryable: true},
	}})
	garbledDetails := toWire(&failurepb.Failure{Message: "x", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: interruptionTypePrefix + orkerStoppedReason, NonRetryable: true, Details: payloads("not a detail")},
	}})
	inProcess := temporal.NewNonRetryableApplicationError("x", interruptionTypePrefix+orkerStoppedReason, nil)
	for name, err := range map[string]error{"no details": noDetails, "garbled details": garbledDetails, "in process": inProcess} {
		c := classify(err)
		if c.kind != handedBack || !c.undecoded || c.attempt != 1 || c.event.Ordinal != nil {
			t.Errorf("%s: %+v", name, c)
		}
	}
	if c := classify(heartbeatLoss("not a beat")); !c.undecoded {
		t.Errorf("a garbled beat is not marked: %+v", c)
	}
	if c := classify(heartbeatLoss(nil)); c.undecoded {
		t.Errorf("a loss with no beat is marked undecoded: %+v", c)
	}
	if c := classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true})); c.undecoded {
		t.Errorf("a decoded detail is marked: %+v", c)
	}
	// The event's ordinal is its own, never the decoded detail's pointer.
	seven := int32(7)
	c := classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true, Ordinal: &seven}))
	if c.event.GetOrdinal() != 7 {
		t.Errorf("ordinal %v", c.event.Ordinal)
	}
}

// TestClassifyInterruption_MalformedStoppingSince: a beat that names a stop
// whose instant does not parse is still a loss while stopping (the Orker
// said it was stopping), with no instant recorded, and is marked.
func TestClassifyInterruption_MalformedStoppingSince(t *testing.T) {
	c := classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": "yesterday"}))
	if !c.stopping || !c.undecoded || c.event.GetReason() != orkerStoppedReason || c.event.GetOrkerStoppingSince() != nil {
		t.Fatalf("%+v %v", c, c.event)
	}
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	applyInterruption(st, testBudgets(), 1, time.Minute, runLoss, c, interruptionNow)
	if st.GetHandedBack() != 1 || st.GetLostWhileStopping() != 1 || runLoss.GetCount() != 0 {
		t.Errorf("spent %v, loss events %d", st, runLoss.GetCount())
	}
}

// TestClassifyInterruption_ReportedBeatIsNotTheNextAttemptsStart: a loss
// after a beat that said attempt k reported is attempt k+1's, lost before
// its first beat; that beat's started mark is attempt k's, so it resets
// nothing.
func TestClassifyInterruption_ReportedBeatIsNotTheNextAttemptsStart(t *testing.T) {
	c := classify(heartbeatLoss(map[string]any{"phase": "reported", "attempt": 1, "started": true}))
	if c.started || c.attempt != 2 {
		t.Fatalf("%+v", c)
	}
}

// TestInterruption_LostWhileStoppingCountedEvenWhenSpent (R11-2, R11-3):
// lost_while_stopping counts every such loss, and the failure says the
// Orker was stopping.
func TestInterruption_LostWhileStoppingCountedEvenWhenSpent(t *testing.T) {
	st := &orkstackv1.TaskInterruptions{HandedBack: 10}
	ev := classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": interruptionNow.Format(time.RFC3339Nano)}))
	d := applyInterruption(st, testBudgets(), 1, time.Minute, &orkstackv1.RunLossEvents{}, ev, interruptionNow)
	if st.GetLostWhileStopping() != 1 || !strings.Contains(d.message, "lost while its Orker was stopping") {
		t.Fatalf("lost_while_stopping %d, message %q", st.GetLostWhileStopping(), d.message)
	}
}

// TestUnitBudgets_AllZeroListsNothing: a plan whose budgets are all 0 (a
// mixed fleet) lists nothing, as a run without budgets; negative budgets
// read as 0; an initial wait beyond the cap is clamped before it is
// multiplied.
func TestUnitBudgets_AllZeroListsNothing(t *testing.T) {
	if got := budgetsForUnit(&InterruptionBudgets{}, nil, 0); got != nil {
		t.Errorf("an all-zero plan gives %+v", got)
	}
	got := budgetsForUnit(&InterruptionBudgets{Lost: -1, HandedBack: -4, NeverStarted: 2, NeverStartedFaults: -3, LostRunCap: -1, NeverStartedBackoffMillis: math.MaxInt64}, nil, 0)
	if got.lost != 0 || got.handedBack != 0 || got.faults != 0 || got.lostRunCap != 0 || got.backoff != maxInterruptionBackoff {
		t.Errorf("%+v", got)
	}
}

// TestInterruption_MaxInterruptionsThroughApply: a task capped at one
// hand-back spends an own attempt on its second.
func TestInterruption_MaxInterruptionsThroughApply(t *testing.T) {
	one := int32(1)
	b := budgetsForUnit(&InterruptionBudgets{Lost: 3, LostRunCap: 20, HandedBack: 10, NeverStarted: 20, NeverStartedFaults: 5, NeverStartedBackoffMillis: 2000}, &one, 0)
	st := &orkstackv1.TaskInterruptions{}
	if d := applyInterruption(st, *b, 1, time.Minute, nil, startedHandBack(), interruptionNow); !d.redispatch {
		t.Fatalf("first: %+v", d)
	}
	if d := applyInterruption(st, *b, 1, time.Minute, nil, startedHandBack(), interruptionNow); !d.fail {
		t.Fatalf("second: %+v", d)
	}
}

// TestInterruption_LossEventWindowBoundary: a loss on the same Orker one
// nanosecond inside the window joins its event; one exactly at it opens a
// new one.
func TestInterruption_LossEventWindowBoundary(t *testing.T) {
	runLoss := &orkstackv1.RunLossEvents{}
	countLossEvent(runLoss, "o1", time.Minute, interruptionNow)
	countLossEvent(runLoss, "o1", time.Minute, interruptionNow.Add(time.Minute-time.Nanosecond))
	if runLoss.GetCount() != 1 {
		t.Fatalf("inside the window: %d", runLoss.GetCount())
	}
	countLossEvent(runLoss, "o1", time.Minute, interruptionNow.Add(time.Minute))
	if runLoss.GetCount() != 2 {
		t.Fatalf("at the window: %d", runLoss.GetCount())
	}
}

// TestInterruption_FaultAccountNamesTheReason: a fault's failure names its
// own reason, not a hand-back.
func TestInterruption_FaultAccountNamesTheReason(t *testing.T) {
	ev := classify(typedInterruption("PROFILE_DRIFTED", &orkstackv1.ActivityFailureDetail{NeverStarted: true}))
	st := &orkstackv1.TaskInterruptions{StoreFaults: 5}
	d := applyInterruption(st, testBudgets(), 1, time.Minute, nil, ev, interruptionNow)
	want := "PROFILE_DRIFTED before it started, past its budget of 5 in a row; see orkctl run attempts"
	if d.message != want {
		t.Fatalf("message %q\nwant    %q", d.message, want)
	}
}

// TestRemainingWait: a wait is what is left until its recorded not_before on
// the workflow's clock, and nothing once that instant has passed.
func TestRemainingWait(t *testing.T) {
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name      string
		notBefore time.Time
		want      time.Duration
	}{
		{"ahead", now.Add(31 * time.Second), 31 * time.Second},
		{"partly spent", now.Add(-2 * time.Second).Add(31 * time.Second), 29 * time.Second},
		{"exactly now", now, 0},
		{"passed", now.Add(-time.Second), 0},
	} {
		if got := remainingWait(tt.notBefore, now); got != tt.want {
			t.Errorf("%s: remainingWait = %v, want %v", tt.name, got, tt.want)
		}
	}
}

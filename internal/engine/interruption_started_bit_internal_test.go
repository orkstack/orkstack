package engine

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestEngine_ResetKeysOnStartedBit (E92 R7E92-M1): the never-started counters
// (never_started and store_faults) reset only when the failure's detail or
// the last beat says the container started. It is read from that bit and
// never from the reason string: the same reason with started true resets,
// with started false or unknown does not, and RUNTIME_STATE_UNKNOWN (started
// unknown) never resets, whatever its detail claims. lost and handed_back
// never reset in any case. TestInterruption_ResetRuleOnStart covers one
// reason per bit; this holds each reason to both values of it.
func TestEngine_ResetKeysOnStartedBit(t *testing.T) {
	stopping := interruptionNow.Add(-time.Minute).Format(time.RFC3339Nano)
	for _, tc := range []struct {
		name      string
		ev        classifiedInterruption
		wantReset bool
	}{
		{"ORKER_STOPPED started true",
			classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{Started: true})), true},
		{"ORKER_STOPPED started false, never started",
			classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{NeverStarted: true})), false},
		{"ORKER_STOPPED started unknown (no flags)",
			classify(typedInterruption("ORKER_STOPPED", &orkstackv1.ActivityFailureDetail{})), false},
		{"ORKER_STOPPED with no detail at all",
			classify(typedInterruption("ORKER_STOPPED", nil)), false},
		{"CONTAINER_STOPPED_BY_RUNTIME started true",
			classify(typedInterruption("CONTAINER_STOPPED_BY_RUNTIME", &orkstackv1.ActivityFailureDetail{Started: true})), true},
		{"CONTAINER_STOPPED_BY_RUNTIME started unknown",
			classify(typedInterruption("CONTAINER_STOPPED_BY_RUNTIME", &orkstackv1.ActivityFailureDetail{})), false},
		{"RUNTIME_STATE_UNKNOWN never started",
			classify(typedInterruption("RUNTIME_STATE_UNKNOWN", &orkstackv1.ActivityFailureDetail{NeverStarted: true})), false},
		{"RUNTIME_STATE_UNKNOWN whose detail wrongly says started",
			classify(typedInterruption("RUNTIME_STATE_UNKNOWN", &orkstackv1.ActivityFailureDetail{Started: true})), false},
		{"ATTEMPT_STORE_UNAVAILABLE whose detail wrongly says started",
			classify(typedInterruption("ATTEMPT_STORE_UNAVAILABLE", &orkstackv1.ActivityFailureDetail{Started: true})), false},
		{"loss, last beat started true",
			classify(heartbeatLoss(map[string]any{"phase": "run", "started": true, "orker_id": "o1"})), true},
		{"loss, last beat started false",
			classify(heartbeatLoss(map[string]any{"phase": "run", "started": false, "orker_id": "o1"})), false},
		{"loss, last beat before the start",
			classify(heartbeatLoss(map[string]any{"phase": "image", "orker_id": "o1"})), false},
		{"loss, no beat at all",
			classify(heartbeatLoss(nil)), false},
		{"loss while stopping, last beat started true",
			classify(heartbeatLoss(map[string]any{"phase": "run", "started": true, "orker_id": "o1", "stopping_since": stopping})), true},
		{"loss while stopping, started unknown",
			classify(heartbeatLoss(map[string]any{"phase": "run", "orker_id": "o1", "stopping_since": stopping})), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &orkstackv1.TaskInterruptions{NeverStarted: 3, StoreFaults: 2, Lost: 1, HandedBack: 2}
			applyInterruption(st, testBudgets(), 1, time.Minute, &orkstackv1.RunLossEvents{}, tc.ev, interruptionNow)
			reset := st.GetNeverStarted() == 0 && st.GetStoreFaults() == 0
			kept := st.GetNeverStarted() >= 3 && st.GetStoreFaults() >= 2
			if tc.wantReset && !reset {
				t.Errorf("a started attempt left never_started %d, store_faults %d; want both 0", st.GetNeverStarted(), st.GetStoreFaults())
			}
			if !tc.wantReset && !kept {
				t.Errorf("an attempt not known to have started left never_started %d, store_faults %d; want them kept (3, 2 or more)", st.GetNeverStarted(), st.GetStoreFaults())
			}
			if st.GetLost() < 1 || st.GetHandedBack() < 2 {
				t.Errorf("lost %d and handed_back %d: neither may ever reset (1 and 2 or more)", st.GetLost(), st.GetHandedBack())
			}
		})
	}
}

// TestStages_InspectFailsPermanentlyEndsExplicitly (R8-1, the engine half): a
// task whose attempts keep ending RUNTIME_STATE_UNKNOWN, never started, spends
// the never-started fault budget (5 consecutive), then one own attempt per
// exhausted budget, and fails with its real reason RUNTIME_STATE_UNKNOWN once
// they are spent: (5+1)·m interruptions for m own attempts, never retried
// forever, never counted as a hand-back, and never resetting a counter.
func TestStages_InspectFailsPermanentlyEndsExplicitly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		own    int32
		detail *orkstackv1.ActivityFailureDetail
	}{
		{"one own attempt", 1, &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o1"}},
		{"three own attempts", 3, &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o1"}},
		{"a detail that wrongly says started", 2, &orkstackv1.ActivityFailureDetail{NeverStarted: true, Started: true, OrkerId: "o1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testBudgets()
			st := &orkstackv1.TaskInterruptions{}
			runLoss := &orkstackv1.RunLossEvents{}
			want := int(b.faults+1) * int(tc.own)
			var final interruptionDecision
			n := 0
			for ; n < want+10; n++ {
				ev := classify(typedInterruption("RUNTIME_STATE_UNKNOWN", &orkstackv1.ActivityFailureDetail{
					NeverStarted: tc.detail.NeverStarted, Started: tc.detail.Started, OrkerId: tc.detail.OrkerId}))
				if ev.kind != neverStartedFault {
					t.Fatalf("RUNTIME_STATE_UNKNOWN classifies as %v, want a never-started fault", ev.kind)
				}
				final = applyInterruption(st, b, tc.own, time.Minute, runLoss, ev, interruptionNow)
				if st.GetHandedBack() != 0 || st.GetLost() != 0 || runLoss.GetCount() != 0 || st.GetNeverStarted() != 0 {
					t.Fatalf("interruption %d was charged as a hand-back, loss or bounce: %v", n+1, st)
				}
				if final.fail {
					n++
					break
				}
				if !final.redispatch {
					t.Fatalf("interruption %d neither re-dispatched nor failed: %+v", n+1, final)
				}
			}
			if !final.fail || n != want {
				t.Fatalf("failed=%v after %d interruptions, want the task failed after exactly %d", final.fail, n, want)
			}
			if final.closedBy != closedByNeverStarted || st.GetLast().GetReason() != "RUNTIME_STATE_UNKNOWN" || !st.GetLast().GetNeverStarted() {
				t.Errorf("closed by %q, last %v; want its real reason RUNTIME_STATE_UNKNOWN, never started", final.closedBy, st.GetLast())
			}
			if st.GetOwnAttemptsUsed() != tc.own {
				t.Errorf("own attempts used %d, want %d", st.GetOwnAttemptsUsed(), tc.own)
			}
			if !strings.Contains(final.message, "RUNTIME_STATE_UNKNOWN") || !strings.Contains(final.message, "before it started") {
				t.Errorf("the failure message %q does not name its real reason", final.message)
			}
		})
	}
}

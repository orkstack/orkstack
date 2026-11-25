package engine

import (
	"strings"
	"testing"
	"time"
)

// TestForceCancelSettings_Floor: the least a force cancel may take covers the
// waits and every step's own time, at the defaults and at each setting's most,
// and an execution timeout is accepted at it and refused below it.
func TestForceCancelSettings_Floor(t *testing.T) {
	d := DefaultForceCancelSettings()
	step := forceCancelStepAttempts*d.RecordTimeout + time.Minute
	// What ForceCancelWorkflow may take with everything failing, by phase.
	worst := (forceCancelSettleLeast + forceCancelSettleMax + step) + // verify
		d.Ceiling + 2*d.MaxGrace + // the run's graces, one past the ceiling and one more
		forceCancelTerminateTries*3*(forceCancelPatience+forceCancelSettleMax+step) + // ask, scan, terminate in each execution it moves to
		max(d.Ceiling, forceCancelSettleLeast) + // what an ended run still needs, shared
		5*(forceCancelPatience+forceCancelSettleMax+step) + // each of it once more: classify, slots, build, announce, records
		forceCancelPublishStart + forceCancelPublishTimeout
	if d.Floor() < worst {
		t.Fatalf("floor %v, and it may take %v", d.Floor(), worst)
	}
	slow := d
	slow.RecordTimeout = forceCancelRecordTimeoutMax
	if slow.Floor() <= d.Floor() || slow.Check() != nil {
		t.Errorf("slower steps leave the floor at %v (%v)", slow.Floor(), slow.Check())
	}
	short := d
	short.Grace, short.MaxGrace, short.Ceiling = 30*time.Second, 30*time.Second, 30*time.Second
	if short.Check() != nil || short.Floor() < forceCancelSettleLeast {
		t.Errorf("a short ceiling leaves %v for an ended run (%v)", short.Floor(), short.Check())
	}
	for _, tt := range []struct {
		name string
		set  func(*ForceCancelSettings)
		bad  string
	}{
		{"exactly the floor", func(s *ForceCancelSettings) { s.ExecutionTimeout = s.Floor() }, ""},
		{"just below it", func(s *ForceCancelSettings) { s.ExecutionTimeout = s.Floor() - 1 }, "force_cancel_execution_timeout"},
		{"as many windows as may be", func(s *ForceCancelSettings) {
			s.Grace, s.MaxGrace, s.Ceiling = 30*time.Second, time.Minute, 500*30*time.Second
		}, ""},
		{"one more", func(s *ForceCancelSettings) {
			s.Grace, s.MaxGrace, s.Ceiling = 30*time.Second, time.Minute, 500*30*time.Second+1
		}, "force_cancel_ceiling"},
		{"a step too long", func(s *ForceCancelSettings) { s.RecordTimeout = forceCancelRecordTimeoutMax + 1 }, "force_cancel_record_timeout"},
	} {
		s := d
		tt.set(&s)
		if err := s.Check(); (err != nil) != (tt.bad != "") || err != nil && !strings.Contains(err.Error(), tt.bad) {
			t.Errorf("%s: %v, want %q", tt.name, err, tt.bad)
		}
	}
}

package engine

import (
	"fmt"
	"time"
)

// ForceCancelSettings are engine.runs.force_cancel_*: what a force cancel this
// engine accepts waits and reads, pinned in its decision and its input.
type ForceCancelSettings struct {
	// Grace is the least a run is given to show progress after its cancel,
	// and the default; MaxGrace, the most a request may ask.
	Grace, MaxGrace time.Duration
	// Ceiling is how long a run that keeps progressing is waited for.
	Ceiling time.Duration
	// RecordTimeout bounds one try of a step.
	RecordTimeout time.Duration
	// ScanEvents and PageSize bound one read of a run's history.
	ScanEvents, PageSize int
	// ExecutionTimeout is a force cancel's own; 0 is Floor.
	ExecutionTimeout time.Duration
	// AttachProgressSlack is how long past its deadline another request's
	// force cancel is still attached to rather than superseded.
	AttachProgressSlack time.Duration
	// Cascade bounds one that cascades.
	Cascade ForceCancelCascadeSettings
}

// DefaultForceCancelSettings are the settings' defaults.
func DefaultForceCancelSettings() ForceCancelSettings {
	return ForceCancelSettings{Grace: time.Minute, MaxGrace: 10 * time.Minute, Ceiling: 30 * time.Minute, RecordTimeout: 10 * time.Second,
		ScanEvents: 2000, PageSize: 250, AttachProgressSlack: time.Minute, Cascade: DefaultForceCancelCascadeSettings()}
}

// forceCancelRecordTimeoutMax bounds one try of a step.
const forceCancelRecordTimeoutMax = time.Minute

// ForceCancelTimings space and bound the steps of a force cancel's workflow.
// They are pinned in its input: what a release changes of them moves only the
// force cancels it starts.
type ForceCancelTimings struct {
	StepAttempts   int `json:"step_attempts"`
	TerminateTries int `json:"terminate_tries"`
	DeriveTries    int `json:"derive_tries"`
	// PollSpread: a long wait is looked at again after this part of it.
	PollSpread       int           `json:"poll_spread"`
	SettleFirst      time.Duration `json:"settle_first"`
	SettleMax        time.Duration `json:"settle_max"`
	Patience         time.Duration `json:"patience"`
	SettleLeast      time.Duration `json:"settle_least"`
	PublishTimeout   time.Duration `json:"publish_timeout"`
	PublishStart     time.Duration `json:"publish_start"`
	PublishHeartbeat time.Duration `json:"publish_heartbeat"`
	CascadeReserve   time.Duration `json:"cascade_reserve"`
}

func defaultForceCancelTimings() ForceCancelTimings {
	return ForceCancelTimings{StepAttempts: forceCancelStepAttempts, TerminateTries: forceCancelTerminateTries, DeriveTries: forceCancelDeriveTries,
		PollSpread: forceCancelPollSpread, SettleFirst: forceCancelSettleFirst, SettleMax: forceCancelSettleMax, Patience: forceCancelPatience,
		SettleLeast: forceCancelSettleLeast, PublishTimeout: forceCancelPublishTimeout, PublishStart: forceCancelPublishStart,
		PublishHeartbeat: forceCancelPublishHeartbeat, CascadeReserve: forceCancelCascadeReserve}
}

// timings are in's, held to bounds whatever it says, and the defaults where it
// says none: anyone who can start a workflow on the cluster can write them. The
// bounds do not follow the defaults, or a release that changed one would change
// what an open force cancel was started with. Within them a force cancel may
// give up early or run out of its own time, which whoever wrote them can have
// of any: that credential ends a force cancel outright.
func (in ForceCancelInput) timings() ForceCancelTimings {
	t, d := in.Timings, defaultForceCancelTimings()
	for _, n := range []struct{ v, def *int }{{&t.StepAttempts, &d.StepAttempts}, {&t.TerminateTries, &d.TerminateTries}, {&t.DeriveTries, &d.DeriveTries}} {
		if *n.v < 1 || *n.v > 10 {
			*n.v = *n.def
		}
	}
	if t.PollSpread < 4 || t.PollSpread > 100 {
		t.PollSpread = d.PollSpread
	}
	for _, n := range []struct{ v, def *time.Duration }{{&t.SettleFirst, &d.SettleFirst}, {&t.SettleMax, &d.SettleMax}, {&t.Patience, &d.Patience},
		{&t.SettleLeast, &d.SettleLeast}, {&t.PublishTimeout, &d.PublishTimeout}, {&t.PublishStart, &d.PublishStart},
		{&t.PublishHeartbeat, &d.PublishHeartbeat}, {&t.CascadeReserve, &d.CascadeReserve}} {
		if *n.v < time.Second || *n.v > time.Hour {
			*n.v = *n.def
		}
	}
	t.SettleFirst = min(t.SettleFirst, t.SettleMax)
	return t
}

// Floor is the least time in which a force cancel always ends by itself: its
// waits, and, because each step has its own time after the wait it shares is
// spent, every step once more with all its tries failing.
func (s ForceCancelSettings) Floor() time.Duration {
	// One step that never succeeds: its tries, the spacing after the last
	// wait, and the least it is tried for.
	step := forceCancelStepAttempts*s.RecordTimeout + time.Minute
	failing := forceCancelPatience + forceCancelSettleMax + step
	verify := forceCancelSettleLeast + forceCancelSettleMax + step
	// Its graces: one ends past the ceiling, and a run that carries on as it
	// passes has one more. Then, in each execution it may move to, the
	// cancel, the stall test and the termination.
	before := s.Ceiling + 2*s.MaxGrace + forceCancelTerminateTries*3*failing
	// Its close, its slots, its ending's build and announcement, its records.
	after := max(s.Ceiling, forceCancelSettleLeast) + 7*failing + 3*(forceCancelPublishStart+time.Minute) + forceCancelPublishTimeout
	return verify + before + after
}

// CascadeFloor is what a cascade may take beyond its force cancel's own time:
// twice as long as the longest of those it starts is waited for, what is left
// to the parents' cancel, and its searches.
func (s ForceCancelSettings) CascadeFloor() time.Duration {
	c := s.Cascade.bounded()
	return 2*(s.Ceiling+2*s.MaxGrace+forceCancelSettleLeast) + s.MaxGrace + c.SettleSlack +
		5*(forceCancelStepAttempts*c.ListTimeout+time.Minute) + 2*time.Duration(c.Max/c.StartsPerTask+1)*(forceCancelStepAttempts*s.RecordTimeout+time.Minute)
}

// Timeout is a force cancel's execution timeout.
func (s ForceCancelSettings) Timeout() time.Duration {
	if s.ExecutionTimeout > 0 {
		return s.ExecutionTimeout
	}
	return s.Floor()
}

// Check refuses settings no force cancel could be decided with, naming the
// one that is wrong.
func (s ForceCancelSettings) Check() error {
	bad := func(name string, value any, want string) error {
		return fmt.Errorf("engine.runs.%s is %v; it must be %s", name, value, want)
	}
	switch {
	case s.Grace < forceCancelMinGrace:
		return bad("force_cancel_grace", s.Grace, "at least "+forceCancelMinGrace.String())
	case s.MaxGrace < s.Grace:
		return bad("max_force_cancel_grace", s.MaxGrace, "at least force_cancel_grace")
	case s.Ceiling < s.MaxGrace || s.Ceiling > forceCancelMaxCeiling || s.Ceiling > forceCancelMaxWindows*s.Grace:
		return bad("force_cancel_ceiling", s.Ceiling, fmt.Sprintf("at least max_force_cancel_grace, and at most %s and %d times force_cancel_grace", forceCancelMaxCeiling, forceCancelMaxWindows))
	case s.RecordTimeout < time.Second || s.RecordTimeout > forceCancelRecordTimeoutMax:
		return bad("force_cancel_record_timeout", s.RecordTimeout, "between 1s and "+forceCancelRecordTimeoutMax.String())
	case s.ScanEvents < 1 || s.ScanEvents > forceCancelScanMax:
		return bad("force_cancel_scan_events", s.ScanEvents, fmt.Sprintf("between 1 and %d", forceCancelScanMax))
	case s.PageSize < 1 || s.PageSize > forceCancelPageMax:
		return bad("force_cancel_page_size", s.PageSize, fmt.Sprintf("between 1 and %d", forceCancelPageMax))
	case s.ExecutionTimeout != 0 && s.ExecutionTimeout < s.Floor():
		return bad("force_cancel_execution_timeout", s.ExecutionTimeout, "unset, or at least "+s.Floor().String()+" with these settings")
	case s.AttachProgressSlack < 0:
		return bad("force_cancel_attach_progress_slack", s.AttachProgressSlack, "at least 0")
	case s.Cascade.Max < 1 || s.Cascade.Max > forceCancelCascadeMost:
		return bad("force_cancel_cascade_max", s.Cascade.Max, fmt.Sprintf("between 1 and %d", forceCancelCascadeMost))
	}
	if held := s.Cascade.bounded(); held != s.Cascade {
		for _, c := range []struct {
			name      string
			set, held any
		}{{"scan", s.Cascade.Scan, held.Scan}, {"walk_scan", s.Cascade.WalkScan, held.WalkScan}, {"batch", s.Cascade.Batch, held.Batch},
			{"starts_per_task", s.Cascade.StartsPerTask, held.StartsPerTask}, {"poll", s.Cascade.Poll, held.Poll},
			{"list_timeout", s.Cascade.ListTimeout, held.ListTimeout}, {"settle_slack", s.Cascade.SettleSlack, held.SettleSlack}} {
			if c.set != c.held {
				return bad("force_cancel_cascade_"+c.name, c.set, fmt.Sprintf("within its bounds (%v is the nearest)", c.held))
			}
		}
	}
	return nil
}

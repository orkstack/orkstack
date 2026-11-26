package eventsource

import "time"

// FirstCarry is the carry a source's first execution starts with.
func FirstCarry(pin Pinned) Carry { return Carry{Format: CarryFormat, Pinned: pin} }

// Pin is what a source's first execution is pinned with today, and one whose
// source was restarted again: three numbers are the deployment's, and the
// rest this release's.
func Pin(maxAge, restartMax, readmit time.Duration) Pinned {
	return Pinned{
		RollWait: 5 * time.Minute, CancelWait: 2 * time.Minute,
		LoadTimeout: 30 * time.Second, LoadBackoff: 5 * time.Second, LoadBackoffMax: 15 * time.Minute, DailyLoad: 24 * time.Hour, PurgeBatch: 1000,
		FreshStarts: 6, FreshWindow: 24 * time.Hour,
		MaxAge: maxAge, RotateWithin: time.Hour,
		RestartBase: time.Second, RestartMax: restartMax, Healthy: 10 * time.Minute,
		RevokeHold: readmit, RevokeUnavailableMax: time.Hour, CheckTimeout: 30 * time.Second, CheckTries: 3, CheckBackoff: time.Second,
		ProviderRefusedMax: time.Hour, ConfirmUnavailableMax: time.Hour, JudgmentPauseMax: 24 * time.Hour,
		BudgetEvents: 2000, BudgetBytes: 1 << 20, ForcedTimes: 10, LimitPercent: 80, MemoEvents: 100, MemoBytes: 100 << 10,
		Heartbeat: 30 * time.Second, AgeBound: 4, StopGrace: 5 * time.Minute,
	}
}

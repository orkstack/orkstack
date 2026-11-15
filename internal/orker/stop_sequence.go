package orker

import (
	"context"
	"fmt"
	"time"
)

// An Orker's stop runs in two phases (docs/OCI_ACTIVITY_CONTRACT.md §6.3,
// docs/DEPLOY.md "Removing an Orker safely"):
//
//	t0  BeginStop: the stopping cordon is set, so the Orker asks for no new task
//	    and opens no new pin. Running tasks, pins it opened before the signal,
//	    and the at most two tasks its polls still open deliver, carry on: those
//	    run in flight, as for any cordon, because handing one back would spend
//	    one of the task's own attempts for nothing.
//	t1  Finish, once nothing is in flight, the grace has ended, or a second
//	    signal ended it: the sweep takes back every attempt not committing its
//	    own result, the task worker is stopped at once, and the stop waits until
//	    every handed-back attempt has reported itself.
//
// The worker is stopped right after the sweep, not after the hand-backs have
// reported (the design's order, which waits first for the polls still open to
// end): a poll left open would deliver a retry of a task just handed back to
// this same Orker, only to be handed back again before it starts, and until
// such a hand-back spends no attempt of the task's own that costs one. Each
// handed-back attempt is detached from the worker's stop and reports itself.
// With a grace longer than a long poll the two orders are the same, since no
// poll is still open at t1.

// DefaultStopGrace is orker.stop.grace's default: how long a stopping Orker
// lets its running tasks and open pins finish before it hands back what
// remains.
const DefaultStopGrace = 24 * time.Hour

// MaxStopGrace is orker.stop.grace's bound: 31 days, written in hours because
// configuration durations are Go durations.
const MaxStopGrace = 744 * time.Hour

// StopTimeoutTail is what a stop can take past its grace, for tasks within the
// default cancellation grace: the cancellation sequence, a handed-back
// attempt's report deadline (the commit tail's cap), the longest poll the
// cluster may hold open, and a margin. A process manager's stop timeout must
// be at least the grace plus this, or running tasks are lost rather than
// handed back.
const StopTimeoutTail = DefaultCancellationGrace + handBackReportDeadline + 75*time.Second + 10*time.Second

// GraceEnd is why a stop's grace ended.
type GraceEnd int

const (
	// GraceEndedIdle: nothing was in flight any more.
	GraceEndedIdle GraceEnd = iota + 1
	// GraceEndedTimeout: the grace ran out.
	GraceEndedTimeout
	// GraceEndedBySignal: a second stop signal ended it.
	GraceEndedBySignal
)

func (e GraceEnd) String() string {
	switch e {
	case GraceEndedIdle:
		return "idle"
	case GraceEndedTimeout:
		return "grace ended"
	case GraceEndedBySignal:
		return "second signal"
	}
	return "unknown"
}

// StopSequence is how one stop runs.
type StopSequence struct {
	// Grace is orker.stop.grace.
	Grace time.Duration
	// EndGrace is closed by a second stop signal. Nil never ends the grace.
	EndGrace <-chan struct{}
	// StopWorker stops the task worker: its polls, its session workers and
	// with them every open pin. It is called once, at t1.
	StopWorker func()
	// ReportEvery is how often the stop says what it still waits for. Zero
	// is 30 s.
	ReportEvery time.Duration
	// Warnings are the boot's warnings about the platform's stop timeout
	// (StopTimeoutWarnings), said again when the stop begins.
	Warnings []string
}

// StopOutcome is what one stop did.
type StopOutcome struct {
	Ended GraceEnd
	// HandedBack is how many attempts the sweep took back at t1.
	HandedBack int
}

// Stop is one stop in progress, between t0 and its end.
type Stop struct {
	a     *Activities
	seq   StopSequence
	since time.Time
	ends  time.Time
}

// BeginStop is t0: it sets the stopping cordon, and says when the grace ends
// and what the platform's stop timeout must be, before it returns.
func (a *Activities) BeginStop(seq StopSequence) *Stop {
	since := time.Now()
	ends := since.Add(seq.Grace)
	a.cordon.SetStopping(since, ends)
	// The t0 beat, once the cordon's lock is released: every running unpinned
	// attempt tells the cluster its Orker is stopping, so a loss after it
	// can be told from one with no stop behind it.
	a.stops.beatStopping(since, func(err error) {
		a.dispatcher.logger().Warn("a running task's stopping beat was not acknowledged; "+
			"a loss before its next beat reads as one with no stop behind it", "error", err)
	})
	a.dispatcher.logger().Info("stopping: taking no new task or pin; running tasks and open pins carry on until the grace ends",
		"grace", seq.Grace, "grace_ends_at", ends.UTC().Format(time.RFC3339),
		"running_tasks", a.running.load(), "open_pins", a.pins.count())
	a.dispatcher.logger().Warn("running tasks are lost, not handed back, if this process is killed before the grace ends; "+
		"the platform's stop timeout must be at least the grace plus the stop's tail",
		"grace", seq.Grace, "tail", StopTimeoutTail, "stop_timeout_at_least", seq.Grace+StopTimeoutTail)
	for _, warning := range seq.Warnings {
		a.dispatcher.logger().Warn(warning)
	}
	return &Stop{a: a, seq: seq, since: since, ends: ends}
}

// Finish waits out the grace, then is t1: the sweep, the worker's stop, and the
// wait for every handed-back attempt's report.
func (s *Stop) Finish() StopOutcome {
	a := s.a
	logger := a.dispatcher.logger()
	ended := s.awaitGrace()
	running := a.running.load()
	handedBack := a.HandBack()
	logger.Info("stop grace over: handing back what still runs",
		"why", ended.String(), "running_tasks", running, "handed_back", handedBack,
		"stopping_for", time.Since(s.since).Round(time.Second))
	if s.seq.StopWorker != nil {
		s.seq.StopWorker()
	}
	every := s.seq.ReportEvery
	if every <= 0 {
		every = 30 * time.Second
	}
	started := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), every)
		err := a.AwaitHandedBack(ctx)
		cancel()
		if err == nil {
			break
		}
		logger.Info("still waiting for handed-back tasks to report: each finishes its cancellation sequence, "+
			"then retries its report for up to "+handBackReportDeadline.String(),
			"running_tasks", a.running.load(), "waiting_for", a.stops.owedLabels(10))
	}
	if handedBack > 0 {
		logger.Info("every handed-back task has reported", "took", time.Since(started).Round(time.Millisecond))
	}
	return StopOutcome{Ended: ended, HandedBack: handedBack}
}

// awaitGrace returns once nothing is in flight, the grace has ended, or a
// second signal ended it, whichever is first.
func (s *Stop) awaitGrace() GraceEnd {
	ctx, cancel := context.WithDeadline(context.Background(), s.ends)
	defer cancel()
	idle := make(chan error, 1)
	go func() { idle <- s.a.AwaitIdle(ctx) }()
	select {
	case err := <-idle:
		if err == nil {
			return GraceEndedIdle
		}
		return GraceEndedTimeout
	case <-s.seq.EndGrace:
		return GraceEndedBySignal
	}
}

// StopGraceVar is orker.stop.grace's environment variable.
const StopGraceVar = "ORK_ORKER_STOP_GRACE"

// ParseStopGrace reads orker.stop.grace: empty is the default, and anything
// that is not a duration from 0 to MaxStopGrace is refused, naming the bound,
// so `31d` is told to write `744h` rather than silently becoming something
// else.
func ParseStopGrace(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultStopGrace, nil
	}
	grace, err := time.ParseDuration(raw)
	if err != nil {
		return DefaultStopGrace, fmt.Errorf("%s=%q is not a duration from 0s to %s (write days as hours: 31 days is 744h)",
			StopGraceVar, raw, maxStopGraceText)
	}
	if grace < 0 || grace > MaxStopGrace {
		return DefaultStopGrace, fmt.Errorf("%s=%q is outside 0s to %s", StopGraceVar, raw, maxStopGraceText)
	}
	return grace, nil
}

// maxStopGraceText is MaxStopGrace as an operator writes it.
const maxStopGraceText = "744h"

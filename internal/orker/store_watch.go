package orker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// An Orker that cannot reach object storage cannot finish a task: an attempt
// handed to it fails InfrastructureError and uses one of the task's retries,
// so a long outage would fail runs that only had to wait. The watch stops that
// without any new state: it checks the store on a cadence, and while the
// checks keep failing it holds the Orker's cordon (cordon.go). A cordoned
// Orker polls for nothing, so tasks stay queued with no attempt started, and
// everything in flight is left alone. Good checks end it, with no operator.
//
// The result is one more cause of a process-local flag: nothing stored,
// nothing shared, gone with the process (AGENTS.md §7, Cordon).

const (
	// StoreCheckEnabledVar turns the watch off (orker.store_check.enabled).
	StoreCheckEnabledVar = "ORK_ORKER_STORE_CHECK_ENABLED"
	// StoreCheckIntervalVar is how often the store is checked.
	StoreCheckIntervalVar = "ORK_ORKER_STORE_CHECK_INTERVAL"
	// StoreCheckTimeoutVar bounds one check.
	StoreCheckTimeoutVar = "ORK_ORKER_STORE_CHECK_TIMEOUT"
	// StoreCheckFailuresVar is how many failed checks in a row cordon the Orker.
	StoreCheckFailuresVar = "ORK_ORKER_STORE_CHECK_FAILURES"
	// StoreCheckRecoveriesVar is how many good checks in a row end the cordon.
	StoreCheckRecoveriesVar = "ORK_ORKER_STORE_CHECK_RECOVERIES"

	// DefaultStoreCheckEnabled, with the three defaults below it, rides out a
	// blip of half a minute and cordons within about three quarters of one.
	DefaultStoreCheckEnabled = true
	// DefaultStoreCheckInterval is orker.store_check.interval's default.
	DefaultStoreCheckInterval = 15 * time.Second
	// DefaultStoreCheckTimeout is orker.store_check.timeout's default.
	DefaultStoreCheckTimeout = 5 * time.Second
	// DefaultStoreCheckFailures is orker.store_check.failures' default.
	DefaultStoreCheckFailures = 3
	// DefaultStoreCheckRecoveries is orker.store_check.recoveries' default.
	DefaultStoreCheckRecoveries = 2

	// MinStoreCheckInterval and MaxStoreCheckInterval bound the cadence.
	MinStoreCheckInterval = time.Second
	MaxStoreCheckInterval = time.Hour

	// storeOutageReason opens every reason the inventory shows.
	storeOutageReason = "object storage unreachable"
)

// StoreCheck asks the store one bounded question; nil means it answered. It
// must honor ctx where it can, and the watch survives one that cannot.
type StoreCheck func(ctx context.Context) error

// StoreWatchConfig is the orker.store_check settings.
type StoreWatchConfig struct {
	Enabled           bool
	Interval          time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
}

// DefaultStoreWatchConfig is the settings an Orker that sets none runs with.
func DefaultStoreWatchConfig() StoreWatchConfig {
	return StoreWatchConfig{
		Enabled:           DefaultStoreCheckEnabled,
		Interval:          DefaultStoreCheckInterval,
		Timeout:           DefaultStoreCheckTimeout,
		FailureThreshold:  DefaultStoreCheckFailures,
		RecoveryThreshold: DefaultStoreCheckRecoveries,
	}
}

// Validate refuses settings the watch could not honor, naming the variable. A
// watch that is off has nothing to honor.
func (c StoreWatchConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Interval < MinStoreCheckInterval || c.Interval > MaxStoreCheckInterval {
		return fmt.Errorf("%s=%s must be between %s and %s",
			StoreCheckIntervalVar, c.Interval, MinStoreCheckInterval, MaxStoreCheckInterval)
	}
	if c.Timeout <= 0 || c.Timeout > c.Interval {
		return fmt.Errorf("%s=%s must be above zero and at most %s=%s, so one check ends before the next is due",
			StoreCheckTimeoutVar, c.Timeout, StoreCheckIntervalVar, c.Interval)
	}
	if c.FailureThreshold < 1 {
		return fmt.Errorf("%s=%d must be 1 or more", StoreCheckFailuresVar, c.FailureThreshold)
	}
	if c.RecoveryThreshold < 1 {
		return fmt.Errorf("%s=%d must be 1 or more", StoreCheckRecoveriesVar, c.RecoveryThreshold)
	}
	return nil
}

// StoreOutageObserver is told when an outage starts and ends, for the Orker's
// metrics (internal/telemetry, StoreOutageMetrics). It returns nothing, so
// telemetry can never change what the watch does.
type StoreOutageObserver interface {
	StoreOutageEntered()
	StoreOutageLeft(lasted time.Duration)
}

// StoreWatch is the loop and its two counters. Only Run's goroutine touches
// the counters.
type StoreWatch struct {
	cordon   *Cordon
	check    StoreCheck
	cfg      StoreWatchConfig
	now      func() time.Time
	logger   *slog.Logger
	observer StoreOutageObserver
	load     func() (tasks int32, pins int)

	failures  int
	successes int
	// pending is the check still running past its timeout, if any. While it is
	// there no second one starts, so a hung store never stacks up checks.
	pending chan error
}

// NewStoreWatch builds this Orker's watch over check. Options.Logger is its
// log; a nil observer exports nothing.
func (a *Activities) NewStoreWatch(check StoreCheck, cfg StoreWatchConfig, observer StoreOutageObserver) *StoreWatch {
	return &StoreWatch{
		cordon:   a.cordon,
		check:    check,
		cfg:      cfg,
		now:      time.Now,
		logger:   a.dispatcher.logger(),
		observer: observer,
		load: func() (int32, int) {
			return a.running.load(), len(a.pins.snapshot())
		},
	}
}

// Run checks the store on its cadence until ctx is done. A boot has already
// proved the store (artifacts.ProbeCommitStore), so the first check is one
// interval in.
func (w *StoreWatch) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Step(ctx)
		}
	}
}

// Step runs one check and counts it. A check cut short because the process is
// stopping says nothing about the store, so it counts for nothing.
func (w *StoreWatch) Step(ctx context.Context) {
	err := w.runCheck(ctx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		w.failed(err)
		return
	}
	w.passed()
}

// errStoreCheckUnanswered is a check that outlasted its timeout.
var errStoreCheckUnanswered = errors.New("the check did not answer")

// runCheck bounds one check by the timeout whether or not the check honors
// its context: a syscall on a hung mount does not, and a watch stuck inside
// one would never report the outage it exists for.
func (w *StoreWatch) runCheck(ctx context.Context) error {
	if w.pending != nil {
		select {
		case <-w.pending:
			w.pending = nil
		default:
			return errStoreCheckUnanswered
		}
	}
	checkCtx, cancel := context.WithTimeout(ctx, w.cfg.Timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.check(checkCtx) }()
	select {
	case err := <-done:
		if err != nil && errors.Is(checkCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", errStoreCheckUnanswered, err)
		}
		return err
	case <-checkCtx.Done():
		// The check's own answer usually follows at once; either way the next
		// step collects it first.
		w.pending = done
		return errStoreCheckUnanswered
	}
}

func (w *StoreWatch) failed(err error) {
	w.successes = 0
	w.failures++
	_, outage := w.cordon.Causes()
	logger := w.log()
	if outage != nil {
		logger.Debug("object storage check failed during an outage", "error", err)
		return
	}
	logger.Warn("object storage check failed",
		"error", err, "failures_in_a_row", w.failures, "cordons_at", w.cfg.FailureThreshold)
	if w.failures < w.cfg.FailureThreshold {
		return
	}
	reason := w.reason(err)
	w.cordon.SetStoreOutage(reason, w.now())
	tasks, pins := w.currentLoad()
	logger.Warn("orker cordoned itself: object storage unreachable; it takes no new task and opens no new pin until the store answers",
		"reason", reason, "error", err, "running_tasks", tasks, "open_pins", pins)
	if w.observer != nil {
		w.observer.StoreOutageEntered()
	}
}

func (w *StoreWatch) passed() {
	w.failures = 0
	operator, outage := w.cordon.Causes()
	if outage == nil {
		w.successes = 0
		return
	}
	w.successes++
	if w.successes < w.cfg.RecoveryThreshold {
		return
	}
	w.successes = 0
	w.cordon.ClearStoreOutage()
	lasted := w.now().Sub(outage.Since)
	w.log().Info("object storage answers again; the orker's store outage cordon is over",
		"lasted", lasted.Round(time.Second), "operator_cordon_holds", operator != nil)
	if w.observer != nil {
		w.observer.StoreOutageLeft(lasted)
	}
}

// reason is what the inventory shows: plain words, never the store's raw error,
// which names endpoints and object keys and belongs in this Orker's own log.
func (w *StoreWatch) reason(err error) string {
	if errors.Is(err, errStoreCheckUnanswered) {
		return fmt.Sprintf("%s: the last check did not answer within %s", storeOutageReason, w.cfg.Timeout)
	}
	return storeOutageReason + ": the last check failed; this Orker's log has the error"
}

func (w *StoreWatch) currentLoad() (int32, int) {
	if w.load == nil {
		return 0, 0
	}
	return w.load()
}

func (w *StoreWatch) log() *slog.Logger {
	if w.logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return w.logger
}

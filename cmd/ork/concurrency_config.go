package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// concurrencySettingsFromEnv reads engine.concurrency.* (E88 P2): the
// concurrency gates' tunables and the census's settings. Each value is
// refused at boot, naming its variable and its key, when it does not parse,
// and the tunables together when a gate could not run on them
// (GateTunables.Check).
func concurrencySettingsFromEnv() (engine.GateTunables, engine.CensusSettings, error) {
	t, c := engine.DefaultGateTunables(), engine.DefaultCensusSettings()
	var errs []error
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_ENTRIES", "engine.concurrency.max_entries", &t.MaxEntries))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_TOMBSTONES", "engine.concurrency.max_tombstones", &t.MaxTombstones))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_LABELS", "engine.concurrency.max_labels", &t.MaxLabels))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_ACQUIRE_TIMEOUT", "engine.concurrency.acquire_timeout", &t.AcquireTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_CLOCK_GRACE", "engine.concurrency.clock_grace", &t.ClockGrace))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_TOMBSTONE_MARGIN", "engine.concurrency.tombstone_margin", &t.TombstoneMargin))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_VERIFY_MIN_AGE", "engine.concurrency.verify_min_age", &t.VerifyMinAge))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_VERIFY_DEBOUNCE", "engine.concurrency.verify_debounce", &t.VerifyDebounce))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_VERIFY_AWAIT", "engine.concurrency.verify_await", &t.VerifyAwait))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_VERIFY_PARALLEL", "engine.concurrency.verify_parallel", &t.VerifyParallel))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_VERIFY_BATCH", "engine.concurrency.verify_batch", &t.VerifyBatch))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_VERIFY_TIMEOUT", "engine.concurrency.verify_timeout", &t.VerifyTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_TASK_ACK_GRACE", "engine.concurrency.task_ack_grace", &t.TaskAckGrace))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_TASK_CONFIRM_BATCH", "engine.concurrency.task_confirm_batch", &t.TaskConfirmBatch))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_STALLED_AFTER", "engine.concurrency.stalled_after", &t.StalledAfter))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_HOLDER_SWEEP_INTERVAL", "engine.concurrency.holder_sweep_interval", &t.HolderSweepInterval))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_GRANT_BACKOFF_MIN", "engine.concurrency.grant_backoff_min", &t.GrantBackoffMin))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_GRANT_BACKOFF_MAX", "engine.concurrency.grant_backoff_max", &t.GrantBackoffMax))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REBUILD_PAGE_TIMEOUT", "engine.concurrency.rebuild_page_timeout", &t.RebuildPageTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REBUILD_SETTLE", "engine.concurrency.rebuild_settle", &t.RebuildSettle))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REBUILD_BACKOFF_MIN", "engine.concurrency.rebuild_backoff_min", &t.RebuildBackoffMin))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REBUILD_BACKOFF_MAX", "engine.concurrency.rebuild_backoff_max", &t.RebuildBackoffMax))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REBUILD_TIMEOUT", "engine.concurrency.rebuild_timeout", &t.RebuildTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_FREEZE_TIMEOUT", "engine.concurrency.freeze_timeout", &t.FreezeTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_DRAIN_MARGIN", "engine.concurrency.drain_margin", &t.DrainMargin))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_GATE_CONTINUE_EVENTS", "engine.concurrency.gate_continue_events", &t.ContinueEvents))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_GATE_CONTINUE_UPDATES", "engine.concurrency.gate_continue_updates", &t.ContinueUpdates))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_GATE_LINGER", "engine.concurrency.gate_linger", &t.Linger))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_CENSUS_INTERVAL", "engine.concurrency.census_interval", &c.Interval))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_CENSUS_TIMEOUT", "engine.concurrency.census_timeout", &c.Timeout))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_CENSUS_MAX_GATES", "engine.concurrency.census_max_gates", &c.MaxGates))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_CENSUS_PARALLEL", "engine.concurrency.census_parallel", &c.Parallel))
	if err := errors.Join(errs...); err != nil {
		return engine.DefaultGateTunables(), engine.DefaultCensusSettings(), err
	}
	if err := t.Check(); err != nil {
		return engine.DefaultGateTunables(), engine.DefaultCensusSettings(), err
	}
	if err := c.Check(); err != nil {
		return engine.DefaultGateTunables(), engine.DefaultCensusSettings(), err
	}
	return t, c, nil
}

// concurrencyDuration reads one duration into dst, leaving it unset when
// the variable is.
func concurrencyDuration(key, configKey string, dst *time.Duration) error {
	raw := env(key, "")
	if raw == "" {
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s=%q (%s) is not a duration (e.g. 30s, 5m): %w", key, raw, configKey, err)
	}
	*dst = parsed
	return nil
}

// concurrencyInt reads one base-10 integer into dst, leaving it unset when
// the variable is.
func concurrencyInt(key, configKey string, dst *int) error {
	raw := env(key, "")
	if raw == "" {
		return nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("%s=%q (%s) is not an integer: %w", key, raw, configKey, err)
	}
	*dst = parsed
	return nil
}

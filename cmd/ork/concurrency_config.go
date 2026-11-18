package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// concurrencySettingsFromEnv reads engine.concurrency.* (E88): the
// concurrency gates' tunables, the census's settings and what a run's submit
// and plan read. Each value is refused at boot, naming its variable and its
// key, when it does not parse, and each set together when no gate, census or
// run could be decided with it (GateTunables.Check, CensusSettings.Check,
// ConcurrencySettings.Check).
func concurrencySettingsFromEnv() (engine.GateTunables, engine.CensusSettings, engine.ConcurrencySettings, error) {
	t, c, r := engine.DefaultGateTunables(), engine.DefaultCensusSettings(), engine.DefaultConcurrencySettings()
	fail := func(err error) (engine.GateTunables, engine.CensusSettings, engine.ConcurrencySettings, error) {
		return engine.DefaultGateTunables(), engine.DefaultCensusSettings(), engine.DefaultConcurrencySettings(), err
	}
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
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_ACTIVE_KEYS_PER_NAMESPACE", "engine.concurrency.max_active_keys_per_namespace", &r.MaxActiveKeysPerNamespace))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_QUEUED_RUNS_PER_NAMESPACE", "engine.concurrency.max_queued_runs_per_namespace", &r.MaxQueuedRunsPerNamespace))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_LIMITS_PER_ORKFLOW", "engine.concurrency.max_limits_per_orkflow", &r.MaxLimitsPerOrkflow))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_MAX_RUN_SLOT_ENTRIES", "engine.concurrency.max_run_slot_entries", &r.MaxRunSlotEntries))
	errs = append(errs, concurrencyInt64("ORK_CONCURRENCY_PER_SLOT_BYTES", "engine.concurrency.per_slot_bytes", &r.PerSlotBytes))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_DEFAULT_QUEUE_TIMEOUT", "engine.concurrency.default_queue_timeout", &r.DefaultQueueTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_MIN_QUEUE_TIMEOUT", "engine.concurrency.min_queue_timeout", &r.MinQueueTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_MAX_QUEUE_TIMEOUT", "engine.concurrency.max_queue_timeout", &r.MaxQueueTimeout))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_ACQUIRE_BACKOFF_MIN", "engine.concurrency.acquire_backoff_min", &r.AcquireBackoffMin))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_ACQUIRE_BACKOFF_MAX", "engine.concurrency.acquire_backoff_max", &r.AcquireBackoffMax))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REQUEUE_INTERVAL", "engine.concurrency.requeue_interval", &r.RequeueInterval))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_REQUEUE_BACKOFF_FACTOR", "engine.concurrency.requeue_backoff_factor", &r.RequeueBackoffFactor))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_REQUEUE_INTERVAL_MAX", "engine.concurrency.requeue_interval_max", &r.RequeueIntervalMax))
	errs = append(errs, concurrencyDuration("ORK_CONCURRENCY_RETRY_AFTER", "engine.concurrency.retry_after", &r.RetryAfter))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_EVENT_HOLDERS_SHOWN", "engine.concurrency.event_holders_shown", &r.EventHoldersShown))
	errs = append(errs, concurrencyInt("ORK_CONCURRENCY_API_HOLDERS_SHOWN", "engine.concurrency.api_holders_shown", &r.APIHoldersShown))
	if err := errors.Join(errs...); err != nil {
		return fail(err)
	}
	if err := t.Check(); err != nil {
		return fail(err)
	}
	if err := c.Check(); err != nil {
		return fail(err)
	}
	if err := r.Check(t); err != nil {
		return fail(err)
	}
	return t, c, r, nil
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

// concurrencyInt64 reads one base-10 integer into dst, leaving it unset when
// the variable is.
func concurrencyInt64(key, configKey string, dst *int64) error {
	raw := env(key, "")
	if raw == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("%s=%q (%s) is not an integer: %w", key, raw, configKey, err)
	}
	*dst = parsed
	return nil
}

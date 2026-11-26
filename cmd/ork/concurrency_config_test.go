package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestConcurrencySettingsFromEnv: unset is the defaults; each setting is
// read; a value that does not parse, that a gate or the census could not
// run on, or that no run could be decided with, is refused at boot naming
// the setting, never clamped.
func TestConcurrencySettingsFromEnv(t *testing.T) {
	gate, census, run, err := concurrencySettingsFromEnv()
	if err != nil || gate != engine.DefaultGateTunables() || census != engine.DefaultCensusSettings() || run != engine.DefaultConcurrencySettings() {
		t.Fatalf("unset = %+v, %+v, %+v, %v; want the defaults", gate, census, run, err)
	}
	t.Setenv("ORK_CONCURRENCY_MAX_ENTRIES", "200")
	t.Setenv("ORK_CONCURRENCY_GATE_LINGER", "10m")
	t.Setenv("ORK_CONCURRENCY_CENSUS_INTERVAL", "2m")
	t.Setenv("ORK_CONCURRENCY_DEFAULT_QUEUE_TIMEOUT", "30m")
	t.Setenv("ORK_CONCURRENCY_PER_SLOT_BYTES", "4096")
	gate, census, run, err = concurrencySettingsFromEnv()
	if err != nil || gate.MaxEntries != 200 || gate.Linger != 10*time.Minute || census.Interval != 2*time.Minute ||
		run.DefaultQueueTimeout != 30*time.Minute || run.PerSlotBytes != 4096 {
		t.Fatalf("set = %+v, %+v, %+v, %v; want the five overrides", gate, census, run, err)
	}
	for _, tc := range []struct{ env, value, names string }{
		{"ORK_CONCURRENCY_MAX_ENTRIES", "lots", "engine.concurrency.max_entries"},
		{"ORK_CONCURRENCY_VERIFY_AWAIT", "soon", "engine.concurrency.verify_await"},
		{"ORK_CONCURRENCY_VERIFY_BATCH", "0", "verify_batch"},
		{"ORK_CONCURRENCY_GATE_LINGER", "1m", "gate_linger"},
		{"ORK_CONCURRENCY_MAX_ENTRIES", "100000", "max_entries"},
		{"ORK_CONCURRENCY_CENSUS_INTERVAL", "100ms", "census_interval"},
		{"ORK_CONCURRENCY_CENSUS_PARALLEL", "0", "census_parallel"},
		{"ORK_CONCURRENCY_PER_SLOT_BYTES", "2KiB", "engine.concurrency.per_slot_bytes"},
		{"ORK_CONCURRENCY_MAX_LIMITS_PER_ORKFLOW", "0", "max_limits_per_orkflow"},
		{"ORK_CONCURRENCY_DEFAULT_QUEUE_TIMEOUT", "1s", "default_queue_timeout"},
		{"ORK_CONCURRENCY_ACQUIRE_BACKOFF_MAX", "1ms", "acquire_backoff_max"},
		{"ORK_CONCURRENCY_MAX_RUN_SLOT_ENTRIES", "600", "max_run_slot_entries"},
		{"ORK_CONCURRENCY_EVENT_HOLDERS_SHOWN", "11", "event_holders_shown"},
		{"ORK_CONCURRENCY_RESET_AUDIT_WINDOW", "11m", "reset_audit_window"},
		{"ORK_CONCURRENCY_FREEZE_MARGIN", "1m", "freeze_margin"},
		{"ORK_CONCURRENCY_RESET_INCOMPLETE_AFTER", "0s", "reset_incomplete_after"},
		{"ORK_CONCURRENCY_RECORD_TIMEOUT", "0s", "record_timeout"},
	} {
		t.Run(tc.env+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			_, _, _, err := concurrencySettingsFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("%s=%s = %v, want a refusal naming %s", tc.env, tc.value, err, tc.names)
			}
		})
	}
}

// TestBackstopLife: the cluster keeps a finished execution for the longer of
// an idempotency key's life and the reset window, each with its own grace.
func TestBackstopLife(t *testing.T) {
	gate := engine.DefaultGateTunables()
	if got, want := backstopLife(time.Hour, gate), gate.ResetWindow+5*time.Minute; got != want {
		t.Errorf("a short key's life = %s; want the reset window's %s", got, want)
	}
	if got, want := backstopLife(72*time.Hour, gate), engine.ClaimLife(72*time.Hour); got != want {
		t.Errorf("a long key's life = %s; want its own %s", got, want)
	}
}

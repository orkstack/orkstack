package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestConcurrencySettingsFromEnv: unset is the defaults; each setting is
// read; a value that does not parse, or that a gate or the census could not
// run on, is refused at boot naming the setting, never clamped.
func TestConcurrencySettingsFromEnv(t *testing.T) {
	gate, census, err := concurrencySettingsFromEnv()
	if err != nil || gate != engine.DefaultGateTunables() || census != engine.DefaultCensusSettings() {
		t.Fatalf("unset = %+v, %+v, %v; want the defaults", gate, census, err)
	}
	t.Setenv("ORK_CONCURRENCY_MAX_ENTRIES", "200")
	t.Setenv("ORK_CONCURRENCY_GATE_LINGER", "10m")
	t.Setenv("ORK_CONCURRENCY_CENSUS_INTERVAL", "2m")
	gate, census, err = concurrencySettingsFromEnv()
	if err != nil || gate.MaxEntries != 200 || gate.Linger != 10*time.Minute || census.Interval != 2*time.Minute {
		t.Fatalf("set = %+v, %+v, %v; want the three overrides", gate, census, err)
	}
	for _, tc := range []struct{ env, value, names string }{
		{"ORK_CONCURRENCY_MAX_ENTRIES", "lots", "engine.concurrency.max_entries"},
		{"ORK_CONCURRENCY_VERIFY_AWAIT", "soon", "engine.concurrency.verify_await"},
		{"ORK_CONCURRENCY_VERIFY_BATCH", "0", "verify_batch"},
		{"ORK_CONCURRENCY_GATE_LINGER", "1m", "gate_linger"},
		{"ORK_CONCURRENCY_MAX_ENTRIES", "100000", "max_entries"},
		{"ORK_CONCURRENCY_CENSUS_INTERVAL", "100ms", "census_interval"},
		{"ORK_CONCURRENCY_CENSUS_PARALLEL", "0", "census_parallel"},
	} {
		t.Run(tc.env+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			_, _, err := concurrencySettingsFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("%s=%s = %v, want a refusal naming %s", tc.env, tc.value, err, tc.names)
			}
		})
	}
}

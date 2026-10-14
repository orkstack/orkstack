package main

import (
	"strings"
	"testing"
)

// TestLoadConfig_InFlightCapBounds: ORK_RUN_MAX_IN_FLIGHT is always
// validated, 16 to 1,800, and a refusal names the variable.
func TestLoadConfig_InFlightCapBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int32
		refused bool
	}{
		{"", 1000, false}, {"16", 16, false}, {"1800", 1800, false}, {"500", 500, false},
		{"15", 0, true}, {"1801", 0, true}, {"0", 0, true}, {"many", 0, true},
	} {
		t.Run("ORK_RUN_MAX_IN_FLIGHT="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_RUN_MAX_IN_FLIGHT", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if tc.refused && !strings.Contains(cfg.limitsErr.Error(), "ORK_RUN_MAX_IN_FLIGHT") {
				t.Fatalf("refusal %v does not name the variable", cfg.limitsErr)
			}
			if !tc.refused && cfg.runMaxInFlight != tc.want {
				t.Fatalf("runMaxInFlight = %d, want %d", cfg.runMaxInFlight, tc.want)
			}
		})
	}
}

// TestLoadConfig_ContinuationValidatedOnlyWhenEnabled: the continuation
// keys are validated only when continuation is on, so a deployment (or a
// test) that tunes engine.history.* low with continuation off is never
// refused over keys it does not use.
func TestLoadConfig_ContinuationValidatedOnlyWhenEnabled(t *testing.T) {
	t.Run("off: a low history limit and out-of-range keys boot", func(t *testing.T) {
		t.Setenv("ORK_HISTORY_MAX_EVENTS", "600")
		t.Setenv("ORK_CONTINUATION_EVENTS", "10")
		t.Setenv("ORK_CONTINUATION_BYTES", "1")
		if err := loadConfig().limitsErr; err != nil {
			t.Fatalf("refused with continuation off: %v", err)
		}
	})
	for _, tc := range []struct {
		name, key, raw, names string
	}{
		{"events below 50", "ORK_CONTINUATION_EVENTS", "49", "ORK_CONTINUATION_EVENTS"},
		{"events above max_events/4", "ORK_CONTINUATION_EVENTS", "12801", "ORK_CONTINUATION_EVENTS"},
		{"bytes below 16 KiB", "ORK_CONTINUATION_BYTES", "16383", "ORK_CONTINUATION_BYTES"},
		{"bytes above max_bytes/5", "ORK_CONTINUATION_BYTES", "10485761", "ORK_CONTINUATION_BYTES"},
		{"restore timeout below 1m", "ORK_CONTINUATION_RESTORE_TIMEOUT", "59s", "ORK_CONTINUATION_RESTORE_TIMEOUT"},
		{"restore timeout above 8h", "ORK_CONTINUATION_RESTORE_TIMEOUT", "8h1m", "ORK_CONTINUATION_RESTORE_TIMEOUT"},
		{"enabled malformed", "ORK_CONTINUATION_ENABLED", "perhaps", "ORK_CONTINUATION_ENABLED"},
	} {
		t.Run("on: "+tc.name, func(t *testing.T) {
			t.Setenv("ORK_CONTINUATION_ENABLED", "true")
			if tc.key == "ORK_CONTINUATION_ENABLED" {
				t.Setenv(tc.key, tc.raw)
			} else {
				t.Setenv(tc.key, tc.raw)
			}
			err := loadConfig().limitsErr
			if err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("refusal = %v, want one naming %s", err, tc.names)
			}
		})
	}
	t.Run("on: the defaults boot", func(t *testing.T) {
		t.Setenv("ORK_CONTINUATION_ENABLED", "true")
		cfg := loadConfig()
		if cfg.limitsErr != nil || !cfg.continuation.enabled || cfg.continuation.events != 8192 {
			t.Fatalf("defaults = %+v (%v)", cfg.continuation, cfg.limitsErr)
		}
	})
}

// TestLoadConfig_RestoreTimeoutMustFitTheHistoryBound: the bootstrap's
// worst-case history (RestoreHistoryBound) must fit a quarter of
// engine.history.max_events: 8 h passes at the defaults, 9 h is refused (it
// is over the 8 h bound too), 1 h is refused under a 1,000-event limit and
// 5 m passes there. The refusal names the timeout, the bytes and the limit.
func TestLoadConfig_RestoreTimeoutMustFitTheHistoryBound(t *testing.T) {
	for _, tc := range []struct {
		maxEvents, events, bytes, timeout string
		refused                           bool
	}{
		{"", "", "", "8h", false},
		{"", "", "", "9h", true},
		{"1000", "120", "1048576", "1h", true},
		{"1000", "120", "1048576", "5m", false},
	} {
		t.Run("max_events="+tc.maxEvents+" timeout="+tc.timeout, func(t *testing.T) {
			t.Setenv("ORK_CONTINUATION_ENABLED", "true")
			t.Setenv("ORK_CONTINUATION_RESTORE_TIMEOUT", tc.timeout)
			for key, value := range map[string]string{"ORK_HISTORY_MAX_EVENTS": tc.maxEvents,
				"ORK_CONTINUATION_EVENTS": tc.events, "ORK_CONTINUATION_BYTES": tc.bytes} {
				if value != "" {
					t.Setenv(key, value)
				}
			}
			err := loadConfig().limitsErr
			if refused := err != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, err, tc.refused)
			}
			if tc.refused && tc.maxEvents != "" {
				for _, name := range []string{"engine.continuation.restore_timeout", "engine.continuation.bytes", "engine.history.max_events"} {
					if !strings.Contains(err.Error(), name) {
						t.Fatalf("refusal %v does not name %s", err, name)
					}
				}
			}
		})
	}
}

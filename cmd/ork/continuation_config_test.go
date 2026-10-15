package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
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
		t.Setenv("ORK_CONTINUATION_ENABLED", "false")
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

// TestLoadConfig_ContinuationIsOnByDefault: with nothing set, runs carry on
// into a fresh history (engine.continuation.enabled defaults to true), with
// a budget the default history limits admit; and a deployment that tunes
// engine.history.* below what that budget needs is refused at boot, naming
// the keys — never a run that quietly cannot carry on.
func TestLoadConfig_ContinuationIsOnByDefault(t *testing.T) {
	cfg := loadConfig()
	if cfg.limitsErr != nil || !cfg.continuation.enabled || cfg.continuation.events != engine.DefaultContinuationEvents {
		t.Fatalf("defaults: enabled %t, events %d (%v); want on at %d", cfg.continuation.enabled,
			cfg.continuation.events, cfg.limitsErr, engine.DefaultContinuationEvents)
	}
	t.Setenv("ORK_HISTORY_MAX_EVENTS", "600")
	err := loadConfig().limitsErr
	if err == nil || !strings.Contains(err.Error(), "ORK_CONTINUATION_EVENTS") || !strings.Contains(err.Error(), "ORK_HISTORY_MAX_EVENTS") {
		t.Fatalf("a 600-event limit with continuation on by default = %v, want a refusal naming both keys", err)
	}
	t.Setenv("ORK_CONTINUATION_ENABLED", "false")
	if err := loadConfig().limitsErr; err != nil {
		t.Fatalf("the same limit with continuation turned off = %v, want it to boot", err)
	}
}

// TestLoadConfig_DefinitionByReferenceNeedsContinuation: a definition passed
// by reference shares the continuation copy's bound, so an explicit true
// without engine.continuation.enabled is refused at boot, naming both keys;
// an unset key boots with continuation off whatever the built-in default
// (today's false, or Stage 7's true), its effective value false; and an
// unset key with continuation on takes the built-in default.
func TestLoadConfig_DefinitionByReferenceNeedsContinuation(t *testing.T) {
	t.Run("an explicit true with continuation off is refused", func(t *testing.T) {
		t.Setenv("ORK_CONTINUATION_ENABLED", "false")
		t.Setenv("ORK_DEFINITION_BY_REFERENCE", "true")
		err := loadConfig().limitsErr
		if err == nil || !strings.Contains(err.Error(), "ORK_DEFINITION_BY_REFERENCE") ||
			!strings.Contains(err.Error(), "ORK_CONTINUATION_ENABLED") ||
			!strings.Contains(err.Error(), "engine.definition_by_reference") || !strings.Contains(err.Error(), "engine.continuation.enabled") {
			t.Fatalf("refusal = %v, want one naming both keys", err)
		}
	})
	t.Run("a malformed value is refused", func(t *testing.T) {
		t.Setenv("ORK_DEFINITION_BY_REFERENCE", "perhaps")
		if err := loadConfig().limitsErr; err == nil || !strings.Contains(err.Error(), "ORK_DEFINITION_BY_REFERENCE") {
			t.Fatalf("refusal = %v, want one naming the key", err)
		}
	})
	for _, builtIn := range []bool{false, true} {
		t.Run(fmt.Sprintf("unset, continuation off, built-in %t", builtIn), func(t *testing.T) {
			enabled, explicit, err := definitionByReferenceFromEnv(false, builtIn)
			if err != nil || enabled || explicit {
				t.Fatalf("= %t (explicit %t, %v), want false, booting", enabled, explicit, err)
			}
		})
		t.Run(fmt.Sprintf("unset, continuation on, built-in %t", builtIn), func(t *testing.T) {
			enabled, explicit, err := definitionByReferenceFromEnv(true, builtIn)
			if err != nil || enabled != builtIn || explicit {
				t.Fatalf("= %t (explicit %t, %v), want the built-in %t", enabled, explicit, err, builtIn)
			}
		})
	}
	t.Run("the boot's effective value", func(t *testing.T) {
		t.Setenv("ORK_CONTINUATION_ENABLED", "false")
		if cfg := loadConfig(); cfg.limitsErr != nil || cfg.definitionByReference || cfg.definitionByReferenceSet {
			t.Fatalf("continuation off, key unset = %t (set %t, %v), want false", cfg.definitionByReference,
				cfg.definitionByReferenceSet, cfg.limitsErr)
		}
		t.Setenv("ORK_CONTINUATION_ENABLED", "true")
		t.Setenv("ORK_DEFINITION_BY_REFERENCE", "true")
		if cfg := loadConfig(); cfg.limitsErr != nil || !cfg.definitionByReference || !cfg.definitionByReferenceSet {
			t.Fatalf("continuation on, key true = %t (set %t, %v), want true", cfg.definitionByReference,
				cfg.definitionByReferenceSet, cfg.limitsErr)
		}
	})
}

// TestLoadConfig_SnapshotOverlays: ORK_SNAPSHOT_OVERLAYS (engine.snapshot_overlays)
// is the built-in default (on) when unset, takes true or false, and refuses anything else at boot,
// naming the variable.
func TestLoadConfig_SnapshotOverlays(t *testing.T) {
	if cfg := loadConfig(); cfg.limitsErr != nil || cfg.snapshotOverlays != engine.DefaultSnapshotOverlays {
		t.Fatalf("unset = %t (%v), want the built-in default %t", cfg.snapshotOverlays, cfg.limitsErr, engine.DefaultSnapshotOverlays)
	}
	t.Setenv("ORK_SNAPSHOT_OVERLAYS", "true")
	if cfg := loadConfig(); cfg.limitsErr != nil || !cfg.snapshotOverlays {
		t.Fatalf("true = %t (%v)", cfg.snapshotOverlays, cfg.limitsErr)
	}
	t.Setenv("ORK_SNAPSHOT_OVERLAYS", "sometimes")
	if err := loadConfig().limitsErr; err == nil || !strings.Contains(err.Error(), "ORK_SNAPSHOT_OVERLAYS") {
		t.Fatalf("a malformed value = %v, want a boot refusal naming ORK_SNAPSHOT_OVERLAYS", err)
	}
}

// TestDefaults_DefinitionByReferenceAndOverlaysOn: from Stage 7 both flags
// are on by default (spec 7.3): an unset deployment boots with a definition
// too large for a start passed by reference (continuation is on by default)
// and with snapshot overlays, and the generated defaults say so.
func TestDefaults_DefinitionByReferenceAndOverlaysOn(t *testing.T) {
	if !engine.DefaultDefinitionByReference || !engine.DefaultSnapshotOverlays {
		t.Fatalf("built-in defaults: definition_by_reference %t, snapshot_overlays %t; want both on",
			engine.DefaultDefinitionByReference, engine.DefaultSnapshotOverlays)
	}
	cfg := loadConfig()
	if cfg.limitsErr != nil || !cfg.definitionByReference || cfg.definitionByReferenceSet || !cfg.snapshotOverlays {
		t.Fatalf("an unset deployment: definition_by_reference %t (set %t), snapshot_overlays %t (%v); want both on",
			cfg.definitionByReference, cfg.definitionByReferenceSet, cfg.snapshotOverlays, cfg.limitsErr)
	}
	raw, err := os.ReadFile("../../deploy/orkstack.defaults.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"definition_by_reference: true", "snapshot_overlays: true"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("deploy/orkstack.defaults.yaml does not say %q", want)
		}
	}
}

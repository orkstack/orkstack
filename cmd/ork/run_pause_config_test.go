package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestLoadConfig_RunPauseTimeoutBounds: ORK_RUN_PAUSE_TIMEOUT and
// ORK_RUN_PAUSE_TIMEOUT_MAX are always validated — whole seconds, the
// maximum at most 744h, the default from 1m to the maximum — and a refusal
// names the variable it is about.
// configKeyOf is each pause variable's configuration-file key.
var configKeyOf = map[string]string{
	"ORK_RUN_PAUSE_TIMEOUT":     "engine.run_pause_timeout",
	"ORK_RUN_PAUSE_TIMEOUT_MAX": "engine.run_pause_timeout_max",
}

func TestLoadConfig_RunPauseTimeoutBounds(t *testing.T) {
	for _, tc := range []struct {
		name, timeout, max   string
		wantDefault, wantMax time.Duration
		refused, remedy      string
	}{
		{name: "unset", wantDefault: engine.DefaultRunPauseTimeout, wantMax: engine.DefaultRunPauseTimeoutMax},
		{name: "a shorter default", timeout: "1h", wantDefault: time.Hour, wantMax: engine.DefaultRunPauseTimeoutMax},
		{name: "a lower maximum", timeout: "1h", max: "24h", wantDefault: time.Hour, wantMax: 24 * time.Hour},
		{name: "the default at the maximum", timeout: "24h", max: "24h", wantDefault: 24 * time.Hour, wantMax: 24 * time.Hour},
		{name: "the shortest default", timeout: "1m", wantDefault: time.Minute, wantMax: engine.DefaultRunPauseTimeoutMax},
		{name: "a maximum above 744h", max: "745h", refused: "ORK_RUN_PAUSE_TIMEOUT_MAX"},
		{name: "a default above the maximum", max: "24h", refused: "ORK_RUN_PAUSE_TIMEOUT", remedy: "at most 24h0m0s"},
		{name: "a default under a minute", timeout: "59s", refused: "ORK_RUN_PAUSE_TIMEOUT"},
		{name: "a fraction of a second", timeout: "1h0.5s", refused: "ORK_RUN_PAUSE_TIMEOUT"},
		{name: "not a duration", max: "a month", refused: "ORK_RUN_PAUSE_TIMEOUT_MAX"},
		{name: "a maximum under a minute", timeout: "30s", max: "30s", refused: "ORK_RUN_PAUSE_TIMEOUT_MAX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_RUN_PAUSE_TIMEOUT", tc.timeout)
			t.Setenv("ORK_RUN_PAUSE_TIMEOUT_MAX", tc.max)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != (tc.refused != "") {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused != "")
			}
			if tc.refused != "" {
				if !strings.Contains(cfg.limitsErr.Error(), tc.refused+"=") && !strings.Contains(cfg.limitsErr.Error(), tc.refused+" is unset") {
					t.Fatalf("refusal %q does not name %s", cfg.limitsErr, tc.refused)
				}
				// The refusal names the configuration file's key too, so a
				// deployment that sets it there knows what to change.
				if key := configKeyOf[tc.refused]; !strings.Contains(cfg.limitsErr.Error(), "("+key+")") {
					t.Fatalf("refusal %q does not name the key %s", cfg.limitsErr, key)
				}
				if !strings.Contains(cfg.limitsErr.Error(), tc.remedy) {
					t.Fatalf("refusal %q does not say %q", cfg.limitsErr, tc.remedy)
				}
				return
			}
			if cfg.runPauseTimeout != tc.wantDefault || cfg.runPauseTimeoutMax != tc.wantMax {
				t.Fatalf("pause timeouts %s / %s, want %s / %s", cfg.runPauseTimeout, cfg.runPauseTimeoutMax,
					tc.wantDefault, tc.wantMax)
			}
		})
	}
}

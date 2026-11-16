package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestLoadConfig_RunTimeoutHandlerGraceBounds:
// ORK_RUN_TIMEOUT_HANDLER_GRACE is always validated — a duration of whole
// seconds from 0s to 24h — and a refusal names the variable. A malformed
// value is refused rather than replaced with the default, because it bounds
// how long a timed-out run's failure handlers may run.
func TestLoadConfig_RunTimeoutHandlerGraceBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		refused bool
	}{
		{raw: "", want: engine.DefaultRunTimeoutHandlerGrace},
		{raw: "0s", want: 0},
		{raw: "1s", want: time.Second},
		{raw: "10m", want: 10 * time.Minute},
		{raw: "24h", want: 24 * time.Hour},
		{raw: "1500ms", refused: true},
		{raw: "-1s", refused: true},
		{raw: "24h1s", refused: true},
		{raw: "later", refused: true},
		{raw: "600", refused: true},
	} {
		t.Run("ORK_RUN_TIMEOUT_HANDLER_GRACE="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_RUN_TIMEOUT_HANDLER_GRACE", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if tc.refused {
				if !strings.Contains(cfg.limitsErr.Error(), "ORK_RUN_TIMEOUT_HANDLER_GRACE") {
					t.Fatalf("refusal %v does not name the variable", cfg.limitsErr)
				}
				return
			}
			if cfg.runTimeoutHandlerGrace != tc.want {
				t.Fatalf("runTimeoutHandlerGrace = %s, want %s", cfg.runTimeoutHandlerGrace, tc.want)
			}
		})
	}
}

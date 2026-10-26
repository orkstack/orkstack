package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestLoadConfig_PinCreationTimeoutBounds: ORK_PIN_CREATION_TIMEOUT is always
// validated — a duration of whole seconds from 1s to 24h — and a refusal
// names the variable. A malformed value is refused rather than replaced with
// the default, because it bounds how long a saturated pool keeps a run's pin
// waiting before the run fails: silently changing it would change what the
// operator asked for.
func TestLoadConfig_PinCreationTimeoutBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		refused bool
	}{
		{raw: "", want: engine.DefaultPinCreationTimeout},
		{raw: "30s", want: 30 * time.Second},
		{raw: "1s", want: time.Second},
		{raw: "90s", want: 90 * time.Second},
		{raw: "1m30s", want: 90 * time.Second},
		{raw: "5m", want: 5 * time.Minute},
		{raw: "24h", want: 24 * time.Hour},
		{raw: "999ms", refused: true},
		{raw: "1500ms", refused: true},
		{raw: "0s", refused: true},
		{raw: "-1m", refused: true},
		{raw: "24h1s", refused: true},
		{raw: "48h", refused: true},
		{raw: "soon", refused: true},
		{raw: "30", refused: true},
	} {
		t.Run("ORK_PIN_CREATION_TIMEOUT="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_PIN_CREATION_TIMEOUT", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if tc.refused {
				if !strings.Contains(cfg.limitsErr.Error(), "ORK_PIN_CREATION_TIMEOUT") {
					t.Fatalf("refusal %v does not name the variable", cfg.limitsErr)
				}
				return
			}
			if cfg.pinCreationTimeout != tc.want {
				t.Fatalf("pinCreationTimeout = %s, want %s", cfg.pinCreationTimeout, tc.want)
			}
		})
	}
}

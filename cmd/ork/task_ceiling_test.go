package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
)

// requireWarning finds the one boot warning saying every want.
func requireWarning(t *testing.T, cfg config, wants ...string) {
	t.Helper()
	for _, warning := range cfg.taskCeiling.warnings {
		found := true
		for _, want := range wants {
			if !strings.Contains(warning, want) {
				found = false
				break
			}
		}
		if found {
			return
		}
	}
	t.Fatalf("no boot warning says all of %q: %q", wants, cfg.taskCeiling.warnings)
}

// TestLoadConfig_OrkflowMaxTasksAboveTheMeasuredMaximumWarns: an explicit
// engine.orkflow_max_tasks above N, the largest size the task soak measured,
// starts — the user chose warn-only (D2, 2026-10-15) — with a boot warning
// naming the variable, the value and N. At or below N, and unset, nothing
// about the measurement is said.
func TestLoadConfig_OrkflowMaxTasksAboveTheMeasuredMaximumWarns(t *testing.T) {
	n := engine.DefaultOrkflowMaxTasks
	above := fmt.Sprint(n + 1)
	t.Setenv("ORK_ORKFLOW_MAX_TASKS", above)
	cfg := loadConfig()
	if cfg.limitsErr != nil || cfg.orkflowMaxTasks != n+1 {
		t.Fatalf("ORK_ORKFLOW_MAX_TASKS=%s = %d (%v), want it to start at that value", above, cfg.orkflowMaxTasks, cfg.limitsErr)
	}
	requireWarning(t, cfg, "ORK_ORKFLOW_MAX_TASKS="+above, fmt.Sprintf("above the largest size measured (%d)", n),
		"this deployment runs unmeasured")
	for _, raw := range []string{"", fmt.Sprint(n)} {
		t.Setenv("ORK_ORKFLOW_MAX_TASKS", raw)
		cfg := loadConfig()
		for _, warning := range cfg.taskCeiling.warnings {
			if strings.Contains(warning, "measured") {
				t.Fatalf("ORK_ORKFLOW_MAX_TASKS=%q warns %q, want no warning at or below N", raw, warning)
			}
		}
	}
}

// TestLoadConfig_OrkflowMaxTasksAboveTheFlatShapeMaximumWarns: above X, the
// flat-stage bound for the configured settings (engine.ShapeMaxTasks), the
// boot warns naming the six keys, "may stop" and "plain tasks"; at the
// defaults N does not warn (X = 4,222); with restore_timeout 8 h, an
// explicit 4,000 warns (X = 3,041).
func TestLoadConfig_OrkflowMaxTasksAboveTheFlatShapeMaximumWarns(t *testing.T) {
	cfg := loadConfig()
	if cfg.limitsErr != nil || len(cfg.taskCeiling.warnings) != 0 || cfg.taskCeiling.flatBound != 4222 {
		t.Fatalf("the defaults: X %d, warnings %q (%v); want X = 4,222 and no warning at N = %d",
			cfg.taskCeiling.flatBound, cfg.taskCeiling.warnings, cfg.limitsErr, engine.DefaultOrkflowMaxTasks)
	}
	t.Setenv("ORK_CONTINUATION_RESTORE_TIMEOUT", "8h")
	t.Setenv("ORK_ORKFLOW_MAX_TASKS", "4000")
	cfg = loadConfig()
	if cfg.limitsErr != nil || cfg.taskCeiling.flatBound != 3041 {
		t.Fatalf("restore_timeout 8h: X = %d (%v), want 3,041", cfg.taskCeiling.flatBound, cfg.limitsErr)
	}
	requireWarning(t, cfg, "ORK_ORKFLOW_MAX_TASKS=4000", "above 3041", "engine.history.max_events",
		"engine.continuation.events", "engine.continuation.bytes", "engine.continuation.restore_timeout",
		"engine.run_max_in_flight", "a stage of more than 3041 plain tasks", "may stop RUN_HISTORY_LIMIT")
	t.Setenv("ORK_ORKFLOW_MAX_TASKS", "3041")
	cfg = loadConfig()
	if cfg.limitsErr != nil {
		t.Fatal(cfg.limitsErr)
	}
	for _, warning := range cfg.taskCeiling.warnings {
		if strings.Contains(warning, "flat-stage bound") {
			t.Fatalf("exactly X warns %q, want no flat-stage warning", warning)
		}
	}
}

// TestLoadConfig_OrkflowMaxTasksWithoutContinuation: with continuation off
// a run has one history. Unset is min(N, max_events / 50) — 1,024 at the
// default limit, or N when that is smaller — and logged; an explicit 5,000
// is refused, naming the three keys; and accepted at max_events 256,000.
func TestLoadConfig_OrkflowMaxTasksWithoutContinuation(t *testing.T) {
	t.Setenv("ORK_CONTINUATION_ENABLED", "false")
	cfg := loadConfig()
	want := min(engine.DefaultOrkflowMaxTasks, int32(engine.DefaultHistoryMaxEvents/engine.UncontinuedEventsPerTask))
	if cfg.limitsErr != nil || cfg.orkflowMaxTasks != want || cfg.taskCeiling.explicit {
		t.Fatalf("unset with continuation off = %d (explicit %t, %v), want min(N, 1,024) = %d",
			cfg.orkflowMaxTasks, cfg.taskCeiling.explicit, cfg.limitsErr, want)
	}
	if engine.DefaultOrkflowMaxTasks > 1024 && want != 1024 {
		t.Fatalf("unset with continuation off at the default limit = %d, want 1,024", want)
	}
	t.Setenv("ORK_ORKFLOW_MAX_TASKS", "5000")
	err := loadConfig().limitsErr
	if err == nil {
		t.Fatal("an explicit 5,000 with continuation off booted, want a refusal")
	}
	for _, key := range []string{"ORK_ORKFLOW_MAX_TASKS=5000", "engine.history.max_events", "ORK_HISTORY_MAX_EVENTS",
		"engine.continuation.enabled", "ORK_CONTINUATION_ENABLED"} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("refusal %q does not name %s", err, key)
		}
	}
	t.Setenv("ORK_HISTORY_MAX_EVENTS", "256000")
	if cfg := loadConfig(); cfg.limitsErr != nil || cfg.orkflowMaxTasks != 5000 {
		t.Fatalf("an explicit 5,000 at max_events 256,000 = %d (%v), want it accepted", cfg.orkflowMaxTasks, cfg.limitsErr)
	}
}

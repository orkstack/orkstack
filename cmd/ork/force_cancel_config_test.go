package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestForceCancelSettingsFromEnv: unset is the defaults; each setting is read;
// a value that does not parse, or with which no force cancel could be decided,
// is refused at boot naming the setting, never clamped.
func TestForceCancelSettingsFromEnv(t *testing.T) {
	got, err := forceCancelSettingsFromEnv()
	if err != nil || got != engine.DefaultForceCancelSettings() || got.Timeout() != got.Floor() {
		t.Fatalf("unset = %+v (timeout %v), %v; want the defaults", got, got.Timeout(), err)
	}
	t.Run("set", func(t *testing.T) {
		for k, v := range map[string]string{"ORK_FORCE_CANCEL_GRACE": "90s", "ORK_MAX_FORCE_CANCEL_GRACE": "15m", "ORK_FORCE_CANCEL_CEILING": "1h",
			"ORK_FORCE_CANCEL_RECORD_TIMEOUT": "20s", "ORK_FORCE_CANCEL_SCAN_EVENTS": "500", "ORK_FORCE_CANCEL_PAGE_SIZE": "50",
			"ORK_FORCE_CANCEL_EXECUTION_TIMEOUT": "9h", "ORK_FORCE_CANCEL_ATTACH_PROGRESS_SLACK": "2m",
			"ORK_FORCE_CANCEL_CASCADE_MAX": "20", "ORK_FORCE_CANCEL_CASCADE_SCAN": "30", "ORK_FORCE_CANCEL_CASCADE_WALK_SCAN": "40", "ORK_FORCE_CANCEL_CASCADE_BATCH": "5",
			"ORK_FORCE_CANCEL_CASCADE_STARTS_PER_TASK": "6", "ORK_FORCE_CANCEL_CASCADE_POLL": "7s", "ORK_FORCE_CANCEL_CASCADE_LIST_TIMEOUT": "8s", "ORK_FORCE_CANCEL_CASCADE_SETTLE_SLACK": "9s"} {
			t.Setenv(k, v)
		}
		want := engine.ForceCancelSettings{Grace: 90 * time.Second, MaxGrace: 15 * time.Minute, Ceiling: time.Hour, RecordTimeout: 20 * time.Second,
			ScanEvents: 500, PageSize: 50, ExecutionTimeout: 9 * time.Hour, AttachProgressSlack: 2 * time.Minute,
			Cascade: engine.ForceCancelCascadeSettings{Max: 20, Scan: 30, WalkScan: 40, Batch: 5, StartsPerTask: 6, Poll: 7 * time.Second, ListTimeout: 8 * time.Second, SettleSlack: 9 * time.Second}}
		if got, err := forceCancelSettingsFromEnv(); err != nil || got != want || got.Timeout() != 9*time.Hour {
			t.Fatalf("set = %+v, %v", got, err)
		}
	})
	for _, tc := range []struct{ env, value, names string }{
		{"ORK_FORCE_CANCEL_GRACE", "soon", "engine.runs.force_cancel_grace"},
		{"ORK_FORCE_CANCEL_GRACE", "29s", "force_cancel_grace"},
		{"ORK_MAX_FORCE_CANCEL_GRACE", "50s", "max_force_cancel_grace"},
		{"ORK_FORCE_CANCEL_CEILING", "9m", "force_cancel_ceiling"},
		{"ORK_FORCE_CANCEL_CEILING", "25h", "force_cancel_ceiling"},
		{"ORK_FORCE_CANCEL_RECORD_TIMEOUT", "61s", "force_cancel_record_timeout"},
		{"ORK_FORCE_CANCEL_SCAN_EVENTS", "0", "force_cancel_scan_events"},
		{"ORK_FORCE_CANCEL_PAGE_SIZE", "1001", "force_cancel_page_size"},
		{"ORK_FORCE_CANCEL_EXECUTION_TIMEOUT", "2h", "force_cancel_execution_timeout"},
		{"ORK_FORCE_CANCEL_ATTACH_PROGRESS_SLACK", "-1s", "force_cancel_attach_progress_slack"},
		{"ORK_FORCE_CANCEL_CASCADE_MAX", "301", "force_cancel_cascade_max"},
		{"ORK_FORCE_CANCEL_CASCADE_MAX", "0", "force_cancel_cascade_max"},
		{"ORK_FORCE_CANCEL_CASCADE_SCAN", "0", "force_cancel_cascade_scan"},
		{"ORK_FORCE_CANCEL_CASCADE_WALK_SCAN", "20001", "force_cancel_cascade_walk_scan"},
		{"ORK_FORCE_CANCEL_CASCADE_BATCH", "101", "force_cancel_cascade_batch"},
		{"ORK_FORCE_CANCEL_CASCADE_STARTS_PER_TASK", "101", "force_cancel_cascade_starts_per_task"},
		{"ORK_FORCE_CANCEL_CASCADE_POLL", "61s", "force_cancel_cascade_poll"},
		{"ORK_FORCE_CANCEL_CASCADE_LIST_TIMEOUT", "0s", "force_cancel_cascade_list_timeout"},
		{"ORK_FORCE_CANCEL_CASCADE_SETTLE_SLACK", "11m", "force_cancel_cascade_settle_slack"},
	} {
		t.Run(tc.env+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			if _, err := forceCancelSettingsFromEnv(); err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("%s=%s = %v, want a refusal naming %s", tc.env, tc.value, err, tc.names)
			}
		})
	}
}

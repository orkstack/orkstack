package main

import (
	"strings"
	"testing"

	archivecleanup "github.com/orkstack/orkstack/internal/archive"
)

func TestLoadConfig_ArtifactGCSettingsAreRefusedNotGuessed(t *testing.T) {
	tests := []struct {
		name, mode, grace, refusal string
	}{
		{"default", "", "", ""},
		{"delete", "delete", "2h", ""},
		{"misspelled mode", "Delete", "", "ORK_CLEANUP_ARTIFACT_GC"},
		{"grace below the minimum", "delete", "5m", "ORK_CLEANUP_ARTIFACT_GC_GRACE"},
		{"grace that does not parse", "delete", "soon", "ORK_CLEANUP_ARTIFACT_GC_GRACE"},
	}
	t.Setenv("ORK_CLEANUP_ARTIFACT_GC_BATCH", "0")
	if err := loadConfig().limitsErr; err == nil || !strings.Contains(err.Error(), "ORK_CLEANUP_ARTIFACT_GC_BATCH") {
		t.Fatalf("batch 0: %v", err)
	}
	t.Setenv("ORK_CLEANUP_ARTIFACT_GC_BATCH", "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ORK_CLEANUP_ARTIFACT_GC", tt.mode)
			t.Setenv("ORK_CLEANUP_ARTIFACT_GC_GRACE", tt.grace)
			err := loadConfig().limitsErr
			switch {
			case tt.refusal == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tt.refusal != "" && (err == nil || !strings.Contains(err.Error(), tt.refusal)):
				t.Fatalf("err = %v, want a refusal naming %s", err, tt.refusal)
			}
		})
	}
}

func TestGCModeInForce_OffUnlessCleanupRunsOnAReclaimableStore(t *testing.T) {
	for _, tc := range []struct {
		enabled, reclaimable bool
		configured, want     archivecleanup.ReclaimMode
	}{
		{true, true, archivecleanup.ReclaimDelete, archivecleanup.ReclaimDelete},
		{true, true, archivecleanup.ReclaimDryRun, archivecleanup.ReclaimDryRun},
		{false, true, archivecleanup.ReclaimDelete, archivecleanup.ReclaimOff},
		{true, false, archivecleanup.ReclaimDelete, archivecleanup.ReclaimOff},
		{true, true, archivecleanup.ReclaimOff, archivecleanup.ReclaimOff},
	} {
		if got := gcModeInForce(tc.enabled, tc.reclaimable, tc.configured); got != tc.want {
			t.Errorf("gcModeInForce(%v, %v, %s) = %s, want %s", tc.enabled, tc.reclaimable, tc.configured, got, tc.want)
		}
	}
}

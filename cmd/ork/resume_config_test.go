package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
)

// TestResumeSettingsFromEnv: unset is the defaults, a set value is read, and
// one out of its bounds is refused at boot, naming its key.
func TestResumeSettingsFromEnv(t *testing.T) {
	if got, err := resumeSettingsFromEnv(); err != nil || got != api.DefaultResumeSettings() {
		t.Fatalf("unset = %+v, %v; want the defaults", got, err)
	}
	t.Setenv("ORK_RESUME_CONFIRM_TIMEOUT", "45s")
	t.Setenv("ORK_RESUME_VISIBILITY_LAG_FLOOR", "20s")
	t.Setenv("ORK_RESUME_MAX_SEQ_GAP", "10")
	t.Setenv("ORK_RESUME_CONFIRM_PER_RUN", "2")
	t.Setenv("ORK_RESUME_CHILD_WAIT_HINT", "36h")
	want := api.ResumeSettings{ConfirmTimeout: 45 * time.Second, VisibilityLagFloor: 20 * time.Second, ChildWaitHint: 36 * time.Hour,
		MaxSequenceGap: 10, ConfirmsPerMinute: 2}
	if got, err := resumeSettingsFromEnv(); err != nil || got != want {
		t.Fatalf("set = %+v, %v; want %+v", got, err, want)
	}
	for key, bad := range map[string][]string{
		"ORK_RESUME_CONFIRM_TIMEOUT":      {"0s", "500ms", "11m", "soon"},
		"ORK_RESUME_VISIBILITY_LAG_FLOOR": {"-1s", "1h"},
		"ORK_RESUME_MAX_SEQ_GAP":          {"0", "-1", "1000001", "x"},
		"ORK_RESUME_CONFIRM_PER_RUN":      {"0", "601"},
		"ORK_RESUME_CHILD_WAIT_HINT":      {"0s", "30s", "721h", "later"},
	} {
		for _, value := range bad {
			t.Run(key+"="+value, func(t *testing.T) {
				t.Setenv(key, value)
				if _, err := resumeSettingsFromEnv(); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("= %v, want it refused naming %s", err, key)
				}
			})
		}
	}
}

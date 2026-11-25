package orker

import (
	"context"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestCordon_ProfileCauseHoldsAlone: a profile cause holds the cordon by
// itself, so the Orker asks for no new task; clearing it never touches the
// operator's cordon, and clearing the operator's never clears it.
func TestCordon_ProfileCauseHoldsAlone(t *testing.T) {
	c := NewCordon()
	now := time.Unix(1_800_000_000, 0)
	c.SetProfiles("profile sensor drifted", now)
	if cordoned, reason, since := c.State(); !cordoned || reason != "profile sensor drifted" || !since.Equal(now) {
		t.Fatalf("State = %v %q %v", cordoned, reason, since)
	}
	c.Set("patch", now)
	c.Clear()
	if cordoned, _, _ := c.State(); !cordoned {
		t.Fatal("clearing the operator's cordon cleared the profile cause")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Wait(ctx); err == nil {
		t.Fatal("Wait returned while a profile cause held")
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait(context.Background()) }()
	c.ClearProfiles()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("clearing the last cause woke no reservation")
	}
}

// TestProfileState_SyncsTheCordon: a drift or an unavailable profile holds
// the Orker's cordon; the last unavailable cause cleared releases it, and a
// drift never does.
func TestProfileState_SyncsTheCordon(t *testing.T) {
	c := NewCordon()
	s := newProfileState(Profiles{}, "")
	s.attach(c)
	now := time.Unix(1_800_000_000, 0)

	s.setUnavailable("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE, "pull failed", now)
	if cordoned, _, _ := c.State(); !cordoned {
		t.Fatal("an unavailable profile left the Orker uncordoned")
	}
	s.clearUnavailable("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE)
	if cordoned, reason, _ := c.State(); cordoned {
		t.Fatalf("the last cause cleared, still cordoned: %q", reason)
	}
	s.setDrift("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, "188:199 is now 188:0", now)
	s.clearUnavailable("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE)
	if cordoned, reason, _ := c.State(); !cordoned || reason == "" {
		t.Fatalf("a drift did not hold the cordon: %v %q", cordoned, reason)
	}
}

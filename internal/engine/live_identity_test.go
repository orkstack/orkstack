package engine

import (
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// TestLiveIdentities: a beat's identities become the attempt's live
// identities, state by state; one the engine cannot read is left out, never
// guessed; past the bound nothing more is read.
func TestLiveIdentities(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	beat := []task.HeartbeatIdentity{
		{Name: "aws", Kind: "identity", State: task.HeartbeatIdentityFresh, LastRefresh: at.Format(time.RFC3339), ExpiresAt: at.Add(time.Hour).Format(time.RFC3339)},
		{Name: "gcp", Kind: "identity", State: task.HeartbeatIdentityRetrying, LastRefresh: at.Format(time.RFC3339), ExpiresAt: at.Add(time.Hour).Format(time.RFC3339)},
		{Name: "old", Kind: "identity", State: task.HeartbeatIdentityExpired, LastRefresh: at.Format(time.RFC3339), ExpiresAt: at.Format(time.RFC3339)},
		{Name: "odd", Kind: "identity", State: "sleepy", LastRefresh: at.Format(time.RFC3339), ExpiresAt: at.Format(time.RFC3339)},
		{Name: "bad-time", Kind: "identity", State: task.HeartbeatIdentityFresh, LastRefresh: "yesterday", ExpiresAt: at.Format(time.RFC3339)},
	}
	got := liveIdentities(beat, at.Add(time.Minute))
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	want := []orkstackv1.LiveIdentityState{
		orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_FRESH,
		orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_RETRYING,
		orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED,
	}
	for i, l := range got {
		if l.GetState() != want[i] || l.GetKind() != "identity" || !l.GetExpiresAt().IsValid() {
			t.Fatalf("%d: %v", i, l)
		}
	}
	many := make([]task.HeartbeatIdentity, task.HeartbeatIdentitiesMax+5)
	for i := range many {
		many[i] = beat[0]
	}
	if got := liveIdentities(many, at); len(got) != task.HeartbeatIdentitiesMax {
		t.Fatalf("past the bound: %d", len(got))
	}
}

// TestLiveIdentities_PastItsExpiryIsExpired: an Orker that went quiet
// leaves its last beat behind; an identity past its expiry by now is shown
// expired, never fresh.
func TestLiveIdentities_PastItsExpiryIsExpired(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	beat := []task.HeartbeatIdentity{{Name: "aws", Kind: "identity", State: task.HeartbeatIdentityFresh,
		LastRefresh: at.Format(time.RFC3339), ExpiresAt: at.Add(time.Hour).Format(time.RFC3339)}}
	if got := liveIdentities(beat, at.Add(2*time.Hour)); len(got) != 1 || got[0].GetState() != orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED {
		t.Fatalf("got %v", got)
	}
}

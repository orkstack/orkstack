package engine

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// liveIdentities is what an attempt's beat says of its identities, as
// LiveAttempt.identity shows it: at most task.HeartbeatIdentitiesMax, and an
// entry the engine cannot read (an unknown state, a time that is not RFC
// 3339) left out rather than guessed. A beat's state is as of the beat: one
// past its expiry by now is expired, whatever the last beat said.
func liveIdentities(beat []task.HeartbeatIdentity, now time.Time) []*orkstackv1.LiveIdentity {
	var out []*orkstackv1.LiveIdentity
	for _, b := range beat[:min(len(beat), task.HeartbeatIdentitiesMax)] {
		state, ok := map[string]orkstackv1.LiveIdentityState{
			task.HeartbeatIdentityFresh:    orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_FRESH,
			task.HeartbeatIdentityRetrying: orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_RETRYING,
			task.HeartbeatIdentityExpired:  orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED,
		}[b.State]
		refreshed, rerr := time.Parse(time.RFC3339, b.LastRefresh)
		expires, eerr := time.Parse(time.RFC3339, b.ExpiresAt)
		if !ok || rerr != nil || eerr != nil || b.Name == "" {
			continue
		}
		if !now.Before(expires) {
			state = orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED
		}
		out = append(out, &orkstackv1.LiveIdentity{Name: b.Name, Kind: b.Kind, State: state,
			LastRefresh: timestamppb.New(refreshed), ExpiresAt: timestamppb.New(expires)})
	}
	return out
}

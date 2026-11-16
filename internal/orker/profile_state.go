package orker

import (
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ReasonProfileInvalid refuses boot: a declared profile is malformed, of a
// kind this build does not implement, or fails its boot audit.
const ReasonProfileInvalid = "ORKER_PROFILE_INVALID"

// profileState is what this process knows about its profiles: what it
// declared, how it describes them, and the profile causes holding its cordon.
// Like a cordon (cordon.go) it is process-local: never stored, gone with the
// process, so only a restart clears a drift.
type profileState struct {
	declared Profiles

	mu        sync.Mutex
	described []*orkstackv1.OrkerProfile
	// cordons holds at most one cause per profile. A drift latches: no later
	// observation replaces it and nothing clears it. An unavailable cause is
	// replaced by a drift and cleared by consecutive good checks.
	cordons map[string]*orkstackv1.ProfileCordon
}

func newProfileState(declared Profiles) *profileState {
	return &profileState{declared: declared, cordons: map[string]*orkstackv1.ProfileCordon{}}
}

// setDescribed records the profiles as this Orker describes them, once its
// boot audit has computed their identities.
func (s *profileState) setDescribed(described []*orkstackv1.OrkerProfile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.described = slices.Clone(described)
}

// setDrift latches a drift of one profile. The first observation stands:
// what the Orker saw first is what explains the cordon.
func (s *profileState) setDrift(profile string, reason orkstackv1.ProfileCordonReason, detail string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.cordons[profile]; ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT {
		return
	}
	s.cordons[profile] = &orkstackv1.ProfileCordon{
		Cause:   orkstackv1.ProfileCordon_CAUSE_DRIFT,
		Profile: profile,
		Reason:  reason,
		Since:   timestamppb.New(now),
		Detail:  detail,
	}
}

// setUnavailable marks one profile unavailable, keeping the instant it was
// first seen and the latest reason. A drifted profile stays drifted.
func (s *profileState) setUnavailable(profile string, reason orkstackv1.ProfileCordonReason, detail string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.cordons[profile]
	switch {
	case ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT:
		return
	case ok:
		held.Reason, held.Detail = reason, detail
	default:
		s.cordons[profile] = &orkstackv1.ProfileCordon{
			Cause:   orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE,
			Profile: profile,
			Reason:  reason,
			Since:   timestamppb.New(now),
			Detail:  detail,
		}
	}
}

// clearUnavailable ends an unavailable cause. It never clears a drift.
func (s *profileState) clearUnavailable(profile string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.cordons[profile]; ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
		delete(s.cordons, profile)
	}
}

// drifted reports whether a profile has drifted.
func (s *profileState) drifted(profile string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.cordons[profile]
	return ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT
}

// report fills the profile fields of this Orker's answer with copies, sorted
// by profile, and marks it cordoned while any cause holds. With nothing
// declared and nothing held it adds no field, so an engine built before
// profiles decodes the answer unchanged. A nil state reports no profile.
func (s *profileState) report(result *orkstackv1.DescribeOrkerResult) {
	result.ProfileKindsSupported = SupportedProfileKinds()
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.described {
		result.Profiles = append(result.Profiles, proto.CloneOf(p))
	}
	names := make([]string, 0, len(s.cordons))
	for name := range s.cordons {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		result.ProfileCordons = append(result.ProfileCordons, proto.CloneOf(s.cordons[name]))
	}
	if len(result.ProfileCordons) > 0 {
		result.Cordoned = true
	}
}

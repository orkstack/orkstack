package orker

import (
	"slices"
	"sort"
	"strings"
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
	// build is the commit this Orker was built from, which its probe image
	// must carry (host_view_probe.go).
	build string
	// cordon is the Orker's cordon, which holds while any profile cause
	// does (attach). Nil in a state no Orker runs, which then only reports.
	cordon *Cordon

	mu        sync.Mutex
	described []*orkstackv1.OrkerProfile
	// audits is each profile as its boot audit found it (profile_gate.go):
	// what every later check compares with, fixed for the process's life.
	audits map[string]profileAudit
	// cordons holds at most one cause per profile. A drift latches: no later
	// observation replaces it and nothing clears it. An unavailable cause is
	// replaced by a drift and cleared by consecutive good checks.
	cordons map[string]*orkstackv1.ProfileCordon
}

func newProfileState(declared Profiles, build string) *profileState {
	return &profileState{declared: declared, build: build, cordons: map[string]*orkstackv1.ProfileCordon{}}
}

// attach makes the profile causes hold c, the Orker's cordon.
func (s *profileState) attach(c *Cordon) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cordon = c
	s.syncLocked()
}

// syncLocked holds the Orker's cordon while any profile cause does, naming
// them in profile order, and ends it once none does. The caller holds the
// lock.
func (s *profileState) syncLocked() {
	if s.cordon == nil {
		return
	}
	if len(s.cordons) == 0 {
		s.cordon.ClearProfiles()
		return
	}
	names := make([]string, 0, len(s.cordons))
	var since time.Time
	for name, held := range s.cordons {
		names = append(names, name)
		if at := held.GetSince().AsTime(); since.IsZero() || at.Before(since) {
			since = at
		}
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		held := s.cordons[name]
		word := "is unavailable"
		if held.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT {
			word = "drifted"
		}
		parts[i] = "profile " + name + " " + word
	}
	s.cordon.SetProfiles(strings.Join(parts, "; "), since)
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
	s.syncLocked()
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
	s.syncLocked()
}

// clearUnavailable ends an unavailable cause. It never clears a drift.
func (s *profileState) clearUnavailable(profile string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.cordons[profile]; ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
		delete(s.cordons, profile)
		s.syncLocked()
	}
}

// unavailable reports whether a profile is held unavailable.
func (s *profileState) unavailable(profile string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.cordons[profile]
	return ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE
}

// hostViewAudits is every audited profile with devices that has not
// drifted: what the host-view probe passes and judges.
func (s *profileState) hostViewAudits() map[string]profileAudit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]profileAudit{}
	for name, audit := range s.audits {
		if held, ok := s.cordons[name]; ok && held.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT {
			continue
		}
		if len(audit.devices) > 0 {
			out[name] = audit
		}
	}
	return out
}

// nextAuditDelay is how long until the next periodic check: recovery_interval
// while a profile is held unavailable, audit_interval otherwise.
func (s *profileState) nextAuditDelay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, held := range s.cordons {
		if held.GetCause() == orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
			return s.declared.RecoveryInterval
		}
	}
	return s.declared.AuditInterval
}

// cordonDetail is what the Orker observed of a profile's cause, for its own
// log and the control channel only.
func (s *profileState) cordonDetail(profile string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cordons[profile].GetDetail()
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

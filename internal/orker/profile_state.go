package orker

import (
	"log/slog"
	"maps"
	"slices"
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
	// probe is where the profile probes' image comes from (probe_image.go).
	probe    *probeImageSource
	declared Profiles
	// build is the commit this Orker was built from, which its probe image
	// must carry (host_view_probe.go).
	build string
	// cordon is the Orker's cordon, which holds while any profile cause
	// does (attach). Nil in a state no Orker runs, which then only reports.
	cordon *Cordon
	// cdiDirs are the trusted CDI spec directories a gpu profile's audit
	// reads, and cdiSocket the container runtime's socket no unit's group may
	// own (useCDI).
	cdiDirs   []string
	cdiSocket string
	// hookBinary hashes a hook binary again before a container is created;
	// nil is the host's (hashHostHook). A test's seam.
	hookBinary func(path string) (hostHook, error)
	// inspectMount inspects a mount source again before a container is
	// created; nil is the host's (inspectHostMount). A test's seam.
	inspectMount func(path string) (hostMount, error)
	// runtime is the engine's default-runtime question, asked again by the
	// periodic audit and before every create, and bootRuntime what it
	// answered at boot (useRuntime). Nil for an Orker with no profile.
	runtime     defaultRuntimeAsker
	bootRuntime runtimeFacts

	mu        sync.Mutex
	described []*orkstackv1.OrkerProfile
	// audits is each profile as its boot audit found it (profile_gate.go):
	// what every later check compares with, fixed for the process's life.
	audits map[string]profileAudit
	// causes holds what keeps each profile out of service. Only profiles
	// with a cause have an entry.
	causes map[string]*profileCauses
}

// profileCauses is what keeps one profile out of service. A drift latches:
// no later observation replaces it and nothing clears it, and while it
// stands it alone is reported. Otherwise every unavailable reason that
// stands is held, each set and cleared only by its own check, so clearing
// one never ends another (AGENTS.md §7: "clearing one never touches
// another").
type profileCauses struct {
	drift       *orkstackv1.ProfileCordon
	unavailable map[orkstackv1.ProfileCordonReason]*orkstackv1.ProfileCordon
}

// entries are the causes as reported: the drift alone if it stands, else
// every unavailable reason in value order, so the lowest comes first.
func (c *profileCauses) entries() []*orkstackv1.ProfileCordon {
	if c.drift != nil {
		return []*orkstackv1.ProfileCordon{c.drift}
	}
	reasons := slices.Sorted(maps.Keys(c.unavailable))
	out := make([]*orkstackv1.ProfileCordon, len(reasons))
	for i, r := range reasons {
		out[i] = c.unavailable[r]
	}
	return out
}

// since is the instant the earliest standing cause was first seen.
func (c *profileCauses) since() time.Time {
	var since time.Time
	for _, e := range c.entries() {
		if at := e.GetSince().AsTime(); since.IsZero() || at.Before(since) {
			since = at
		}
	}
	return since
}

func newProfileState(declared Profiles, build string) *profileState {
	return &profileState{declared: declared, build: build, causes: map[string]*profileCauses{},
		probe: newProbeImageSource(declared.ProbeImage, nil, declared.RecoveryInterval)}
}

// withProbe gives s release as its probe image when the document pins none,
// and logger to say which digest the release path holds.
func (s *profileState) withProbe(release *ReleaseProbe, logger *slog.Logger) *profileState {
	s.probe = newProbeImageSource(s.declared.ProbeImage, release, s.declared.RecoveryInterval)
	s.probe.logger = logger
	return s
}

// probeSource is where the profile probes' image comes from.
func (s *profileState) probeSource() *probeImageSource { return s.probe }

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
	if len(s.causes) == 0 {
		s.cordon.ClearProfiles()
		return
	}
	names := slices.Sorted(maps.Keys(s.causes))
	var since time.Time
	parts := make([]string, len(names))
	for i, name := range names {
		held := s.causes[name]
		if at := held.since(); since.IsZero() || at.Before(since) {
			since = at
		}
		word := "is unavailable"
		if held.drift != nil {
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
	held := s.causes[profile]
	if held == nil {
		held = &profileCauses{}
		s.causes[profile] = held
	}
	if held.drift != nil {
		return
	}
	held.drift = &orkstackv1.ProfileCordon{
		Cause:   orkstackv1.ProfileCordon_CAUSE_DRIFT,
		Profile: profile,
		Reason:  reason,
		Since:   timestamppb.New(now),
		Detail:  detail,
	}
	s.syncLocked()
}

// setUnavailable holds one profile unavailable for one reason, keeping the
// instant that reason was first seen and its latest detail. A drifted
// profile stays drifted.
func (s *profileState) setUnavailable(profile string, reason orkstackv1.ProfileCordonReason, detail string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	if held == nil {
		held = &profileCauses{}
		s.causes[profile] = held
	}
	if held.drift != nil {
		return
	}
	if held.unavailable == nil {
		held.unavailable = map[orkstackv1.ProfileCordonReason]*orkstackv1.ProfileCordon{}
	}
	if existing, ok := held.unavailable[reason]; ok {
		existing.Detail = detail
	} else {
		held.unavailable[reason] = &orkstackv1.ProfileCordon{
			Cause:   orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE,
			Profile: profile,
			Reason:  reason,
			Since:   timestamppb.New(now),
			Detail:  detail,
		}
	}
	s.syncLocked()
}

// clearUnavailable ends the named unavailable reasons of one profile, and
// only those: a reason another check holds stands. It never clears a drift.
func (s *profileState) clearUnavailable(profile string, reasons ...orkstackv1.ProfileCordonReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	if held == nil || held.drift != nil {
		return
	}
	changed := false
	for _, r := range reasons {
		if _, ok := held.unavailable[r]; ok {
			delete(held.unavailable, r)
			changed = true
		}
	}
	if !changed {
		return
	}
	if len(held.unavailable) == 0 {
		delete(s.causes, profile)
	}
	s.syncLocked()
}

// unavailable reports whether a profile is held unavailable, for any reason.
func (s *profileState) unavailable(profile string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	return held != nil && held.drift == nil && len(held.unavailable) > 0
}

// unavailableFor reports whether a profile is held unavailable for any of
// reasons: what a check that owns those reasons asks before it counts the
// good observations that clear them.
func (s *profileState) unavailableFor(profile string, reasons ...orkstackv1.ProfileCordonReason) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	if held == nil || held.drift != nil {
		return false
	}
	for _, r := range reasons {
		if _, ok := held.unavailable[r]; ok {
			return true
		}
	}
	return false
}

// hostViewAudits is every audited profile with devices that has not
// drifted: what the host-view probe passes and judges.
func (s *profileState) hostViewAudits() map[string]profileAudit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]profileAudit{}
	for name, audit := range s.audits {
		if held := s.causes[name]; held != nil && held.drift != nil {
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
	for _, held := range s.causes {
		if held.drift == nil && len(held.unavailable) > 0 {
			return s.declared.RecoveryInterval
		}
	}
	return s.declared.AuditInterval
}

// cordonDetail is what the Orker observed of a profile's cause, for its own
// log and the control channel only: the drift's, or the first standing
// unavailable reason's.
func (s *profileState) cordonDetail(profile string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	if held == nil {
		return ""
	}
	if entries := held.entries(); len(entries) > 0 {
		return entries[0].GetDetail()
	}
	return ""
}

// drifted reports whether a profile has drifted.
func (s *profileState) drifted(profile string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.causes[profile]
	return held != nil && held.drift != nil
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
	for _, name := range slices.Sorted(maps.Keys(s.causes)) {
		for _, entry := range s.causes[name].entries() {
			result.ProfileCordons = append(result.ProfileCordons, proto.CloneOf(entry))
		}
	}
	if len(result.ProfileCordons) > 0 {
		result.Cordoned = true
	}
}

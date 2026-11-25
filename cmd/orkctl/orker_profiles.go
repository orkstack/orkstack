package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// safeOrker is a copy of an inventory row whose every string — the id, pool,
// build, reasons, profile names, pins, metadata addresses, at any depth — is
// terminalSafe, so no text an Orker reports can drive the terminal or forge a
// line. Human output only: --json prints the response as it came.
func safeOrker(row *orkstackv1.Orker) *orkstackv1.Orker {
	if row == nil {
		return nil
	}
	clone := proto.Clone(row).(*orkstackv1.Orker)
	terminalSafeStrings(clone.ProtoReflect())
	return clone
}

func safeOrkers(rows []*orkstackv1.Orker) []*orkstackv1.Orker {
	out := make([]*orkstackv1.Orker, len(rows))
	for i, row := range rows {
		out[i] = safeOrker(row)
	}
	return out
}

// terminalSafeStrings rewrites every string field of m, and of every message
// it holds, through terminalSafe.
func terminalSafeStrings(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.StringKind:
			list := v.List()
			for i := range list.Len() {
				list.Set(i, protoreflect.ValueOfString(terminalSafe(list.Get(i).String())))
			}
		case fd.IsList() && fd.Message() != nil:
			list := v.List()
			for i := range list.Len() {
				terminalSafeStrings(list.Get(i).Message())
			}
		case fd.IsMap():
			// The inventory row holds no map; a later one is escaped by value.
			mp := v.Map()
			mp.Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				switch {
				case fd.MapValue().Kind() == protoreflect.StringKind:
					mp.Set(k, protoreflect.ValueOfString(terminalSafe(mv.String())))
				case fd.MapValue().Message() != nil:
					terminalSafeStrings(mv.Message())
				}
				return true
			})
		case fd.Kind() == protoreflect.StringKind:
			m.Set(fd, protoreflect.ValueOfString(terminalSafe(v.String())))
		case fd.Message() != nil:
			terminalSafeStrings(v.Message())
		}
		return true
	})
}

// An Orker's profiles and the profile causes that hold its cordon, in the
// words Admin → Pools uses (ui/src/lib/orkers.ts, "Profiles"), so the two
// surfaces say the same thing (docs/UI_CLI_PARITY.md, "Orker inventory").

// profileStateLabel is what the profile causes alone make of an Orker: a
// drift; else a cause this release cannot read, which keeps the Orker out of
// its pool as the engine counts it; else an unavailable profile; "" when no
// profile holds it.
func profileStateLabel(row *orkstackv1.Orker) string {
	causes := row.GetProfileCordons()
	switch {
	case slices.ContainsFunc(causes, func(c *orkstackv1.ProfileCordon) bool {
		return c.GetCause() == orkstackv1.ProfileCordon_CAUSE_DRIFT
	}):
		return "profile drifted"
	case slices.ContainsFunc(causes, func(c *orkstackv1.ProfileCordon) bool {
		return c.GetCause() != orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE
	}):
		return "profile out of service"
	case len(causes) > 0:
		return "profile unavailable"
	default:
		return ""
	}
}

// profileKindLabel is a profile's kind as an operator writes it in
// orker.profiles.
func profileKindLabel(kind orkstackv1.ProfileKind) string {
	switch kind {
	case orkstackv1.ProfileKind_PROFILE_KIND_GPU:
		return "gpu"
	case orkstackv1.ProfileKind_PROFILE_KIND_DEVICES:
		return "devices"
	default:
		return "unknown kind"
	}
}

// profileReasonCode is a cause's reason as the UI prints it: the enum's name
// without its prefix, or REASON_<n> for one this build cannot name.
func profileReasonCode(reason orkstackv1.ProfileCordonReason) string {
	if _, known := orkstackv1.ProfileCordonReason_name[int32(reason)]; !known ||
		reason == orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNSPECIFIED {
		return "REASON_" + strconv.Itoa(int(reason))
	}
	return strings.TrimPrefix(reason.String(), "PROFILE_CORDON_REASON_")
}

// profileReasonWords say what each reason found, word for word the UI's.
var profileReasonWords = map[orkstackv1.ProfileCordonReason]string{
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED:            "its device spec changed",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED:          "a device was replaced, or changed its numbers or group",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOOK_BINARY_CHANGED:     "a hook binary changed",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT:             "its device spec is gone",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD:      "another Orker holds the host’s GPU lock",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_LOCK_MISMATCH:      "the host’s GPU lock is not the one it audited",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE: "its probe image cannot be pulled or run",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED:       "a container no longer sees its devices as the Orker does",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY:     "none of its units is healthy",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH:    "its probe image is not this Orker’s build",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOOK_BUILD_UNKNOWN:      "its hook is a build this release does not list",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD:      "containers it did not start hold its units",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT:           "a device is not there",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNITS_BELOW_CEILING:     "fewer of its units are usable than the per-task gpu cap (max_request.gpu), so the Orker polls for no task",
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_NOT_STARTED:       "the container engine does not start its unit probes",
}

// profileCauses are an Orker's profile causes, those that take it out of its
// pool first (a drift, then a cause this release cannot read), then an
// unavailable profile; each by reason, then profile.
func profileCauses(row *orkstackv1.Orker) []*orkstackv1.ProfileCordon {
	rank := func(c *orkstackv1.ProfileCordon) int {
		switch c.GetCause() {
		case orkstackv1.ProfileCordon_CAUSE_DRIFT:
			return 0
		case orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE:
			return 2
		default:
			return 1
		}
	}
	causes := slices.Clone(row.GetProfileCordons())
	slices.SortStableFunc(causes, func(a, b *orkstackv1.ProfileCordon) int {
		if d := rank(a) - rank(b); d != 0 {
			return d
		}
		if d := int(a.GetReason()) - int(b.GetReason()); d != 0 {
			return d
		}
		return strings.Compare(a.GetProfile(), b.GetProfile())
	})
	return causes
}

// profileCause is the cause that stands on one profile, drift first; nil when
// it is in service.
func profileCause(row *orkstackv1.Orker, profile string) *orkstackv1.ProfileCordon {
	for _, c := range profileCauses(row) {
		if c.GetProfile() == profile {
			return c
		}
	}
	return nil
}

// profileCauseWord is the word a cause puts on its profile.
func profileCauseWord(c *orkstackv1.ProfileCordon) string {
	switch c.GetCause() {
	case orkstackv1.ProfileCordon_CAUSE_DRIFT:
		return "drifted"
	case orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE:
		return "unavailable"
	default:
		return "out of service"
	}
}

// profileCauseNote is one cause in words: what was found, its code, and how
// it ends.
func profileCauseNote(c *orkstackv1.ProfileCordon) string {
	what, ok := profileReasonWords[c.GetReason()]
	if !ok {
		what = "the Orker found a fault"
	}
	since := ""
	if c.GetSince() != nil {
		since = " since " + formatTime(c.GetSince())
	}
	code := profileReasonCode(c.GetReason())
	switch c.GetCause() {
	case orkstackv1.ProfileCordon_CAUSE_DRIFT:
		return fmt.Sprintf("Profile %s drifted%s: %s (%s). It stays out of service on this Orker until the Orker restarts.",
			c.GetProfile(), since, what, code)
	case orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE:
		return fmt.Sprintf("Profile %s unavailable%s: %s (%s). Once that is put right, it returns by itself after consecutive good checks.",
			c.GetProfile(), since, what, code)
	default:
		return fmt.Sprintf("Profile %s out of service%s: %s (%s), for a cause this release cannot read. The Orker takes no work from its pool; see orkctl orker get.",
			c.GetProfile(), since, what, code)
	}
}

// profileHoldsText is the uncordon's account of the first profile cause left
// holding the Orker, and how it ends.
func profileHoldsText(c *orkstackv1.ProfileCordon) string {
	switch c.GetCause() {
	case orkstackv1.ProfileCordon_CAUSE_DRIFT:
		return fmt.Sprintf("profile %s is drifted, which only a restart of the Orker ends", c.GetProfile())
	case orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE:
		return fmt.Sprintf("profile %s is unavailable, and resumes by itself after consecutive good checks", c.GetProfile())
	default:
		return fmt.Sprintf("profile %s is out of service, for a cause this release cannot read; orkctl orker get shows it", c.GetProfile())
	}
}

// profileUnitsText is a gpu profile's units, "1 of 2 usable units in use; 1
// unhealthy", "" for a devices profile or none declared. Counts never go below
// zero or past the total, whatever a skewed answer says.
func profileUnitsText(p *orkstackv1.OrkerProfile) string {
	if p.GetKind() != orkstackv1.ProfileKind_PROFILE_KIND_GPU || p.GetUnits() <= 0 {
		return ""
	}
	total := p.GetUnits()
	usable := min(total, max(0, p.GetUsableUnits()))
	bound := min(usable, max(0, p.GetBoundUnits()))
	text := fmt.Sprintf("%d of %d usable %s in use", bound, usable, pick(int(usable), "unit", "units"))
	if unusable := total - usable; unusable > 0 {
		text += fmt.Sprintf("; %d unhealthy", unusable)
	}
	return text
}

// orkerProfiles is PROFILES: each profile with its kind and the cause on it,
// "a100 (gpu; drifted), fpga (devices)"; - for none or when unmeasured.
func orkerProfiles(row *orkstackv1.Orker) string {
	if row.GetUnavailableReason() != "" || len(row.GetProfiles()) == 0 {
		return dash
	}
	parts := make([]string, 0, len(row.GetProfiles()))
	for _, p := range row.GetProfiles() {
		inside := profileKindLabel(p.GetKind())
		if c := profileCause(row, p.GetName()); c != nil {
			inside += "; " + profileCauseWord(c)
		}
		parts = append(parts, p.GetName()+profileContractsShort(p)+" ("+inside+")")
	}
	return strings.Join(parts, ", ")
}

// contractsPlain says a profile is one contract "1" accepting nothing, the
// short form as most write it, which the list and get leave unsaid.
func contractsPlain(p *orkstackv1.OrkerProfile) bool {
	cs := p.GetContracts()
	return len(cs) == 0 || (len(cs) == 1 && cs[0].GetLabel() == "1" && len(cs[0].GetAccepts()) == 0)
}

// profileContractsShort is a profile's contracts as PROFILES writes them
// after its name: ":1,2*" with the default of several starred, then " +acc
// 0" for what the default accepts; "" for a plain profile.
func profileContractsShort(p *orkstackv1.OrkerProfile) string {
	if contractsPlain(p) {
		return ""
	}
	labels := make([]string, 0, len(p.GetContracts()))
	var accepts []string
	for _, c := range p.GetContracts() {
		label := c.GetLabel()
		if label == p.GetDefaultContract() {
			if len(p.GetContracts()) > 1 {
				label += "*"
			}
			accepts = c.GetAccepts()
		}
		labels = append(labels, label)
	}
	text := ":" + strings.Join(labels, ",")
	if len(accepts) > 0 {
		text += " +acc " + strings.Join(accepts, ",")
	}
	return text
}

// profileContractsLong is a profile's contracts as orkctl orker get writes
// them: "contracts 1, 2 (default; accepts 1)"; "" for a plain profile.
func profileContractsLong(p *orkstackv1.OrkerProfile) string {
	if contractsPlain(p) {
		return ""
	}
	parts := make([]string, 0, len(p.GetContracts()))
	for _, c := range p.GetContracts() {
		var notes []string
		if c.GetLabel() == p.GetDefaultContract() && len(p.GetContracts()) > 1 {
			notes = append(notes, "default")
		}
		if len(c.GetAccepts()) > 0 {
			notes = append(notes, "accepts "+strings.Join(c.GetAccepts(), ", "))
		}
		part := c.GetLabel()
		if len(notes) > 0 {
			part += " (" + strings.Join(notes, "; ") + ")"
		}
		parts = append(parts, part)
	}
	return pick(len(parts), "contract ", "contracts ") + strings.Join(parts, ", ")
}

// attemptProfilesCell is PROFILES of orkctl run attempts: each profile the
// attempt ran with and the contract it ran under, "sensor (contract 2,
// needed by the image)" when its image chose it, "(contract 2, accepting
// 1)" when it ran under a contract that accepts a pinned one; - for none.
// Every word of it is the Orker's, so the cell is made terminal-safe: an
// escape, tab or newline in a profile's name or label is shown, never obeyed.
func attemptProfilesCell(profiles []*orkstackv1.EffectiveProfile) string {
	if len(profiles) == 0 {
		return dash
	}
	parts := make([]string, 0, len(profiles))
	for _, p := range profiles {
		var notes []string
		if c := p.GetContract(); c != "" {
			notes = append(notes, "contract "+c)
			if pinned := p.GetRunContracts(); len(pinned) > 0 && !slices.Contains(pinned, c) {
				notes = append(notes, "accepting "+strings.Join(pinned, ", "))
			}
		}
		if p.GetDeclared() {
			notes = append(notes, "needed by the image")
		}
		part := p.GetName()
		if len(notes) > 0 {
			part += " (" + strings.Join(notes, ", ") + ")"
		}
		parts = append(parts, part)
	}
	return terminalSafe(strings.Join(parts, "; "))
}

// profileLine is one profile's value in orkctl orker get: its kind, the cause
// on it, then a gpu profile's units.
func profileLine(row *orkstackv1.Orker, p *orkstackv1.OrkerProfile) string {
	parts := []string{profileKindLabel(p.GetKind())}
	if contracts := profileContractsLong(p); contracts != "" {
		parts = append(parts, contracts)
	}
	if c := profileCause(row, p.GetName()); c != nil {
		parts = append(parts, profileCauseWord(c))
	}
	if units := profileUnitsText(p); units != "" {
		parts = append(parts, units)
	}
	return strings.Join(parts, ", ")
}

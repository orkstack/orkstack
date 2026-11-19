package orker

import (
	"maps"
	"slices"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Step 3c's contract selection (design rollout-revision §5): which of a
// profile's contracts serves a ref, decided from what the run pinned and
// this Orker's own boot audit alone, never from the host. A ref without
// contracts (an engine built before them) is judged by its digest; a
// declared ref by the label its image named; an undeclared one by the
// default contract only, so an image that declares nothing never meets a
// contract added for images that need one. An owner is never trusted on its
// label alone: a label pinned with no digest is served only through
// accepts:.

// ReasonProfileContractNotServed: this Orker serves none of the contracts the
// run pinned for a profile. The attempt never started, so another Orker of
// the pool that serves them runs it.
const ReasonProfileContractNotServed = "PROFILE_CONTRACT_NOT_SERVED"

// contractSelection is the contract a ref runs under here.
type contractSelection struct {
	Name     string
	Label    string
	Identity string
	// Accepted is whether it serves the run through accepts:, not as the
	// owner of a pinned label.
	Accepted bool
	// Declared is whether the task's image chose the contract.
	Declared bool
	devices  []auditedDevice
}

// contractsOf is an audit's contracts and its default's label: one
// contract, defaultContractLabel, for an audit recorded without contracts.
func contractsOf(a profileAudit) (map[string]contractAudit, string) {
	if len(a.contracts) == 0 {
		return map[string]contractAudit{defaultContractLabel: {identity: a.identity, devices: a.devices}}, defaultContractLabel
	}
	return a.contracts, a.def
}

// selectContract is the contract of audit that serves ref, or why none does.
// Contract faults are never-started: they are found before any container,
// and serving is a property of the run, so another Orker may serve it.
func selectContract(ref *orkstackv1.ProfileRef, audit profileAudit) (contractSelection, *Failure) {
	name := ref.GetName()
	contracts, def := contractsOf(audit)
	picked := func(label string, accepted bool) (contractSelection, *Failure) {
		c := contracts[label]
		return contractSelection{Name: name, Label: label, Identity: c.identity, Accepted: accepted,
			Declared: ref.GetDeclared(), devices: c.devices}, nil
	}
	notServed := func(format string, args ...any) (contractSelection, *Failure) {
		return contractSelection{}, neverStarted(Fail(ReasonProfileContractNotServed, format, args...))
	}
	edited := func(label string) (contractSelection, *Failure) {
		return contractSelection{}, neverStarted(Fail(ReasonProfileChanged,
			"profile %q: this Orker defines its contract %q otherwise than when the run was submitted; "+
				"a contract keeps one definition across a pool, so a changed one needs a new label", name, label))
	}
	pins := ref.GetContracts()
	switch {
	case len(pins) == 0 && !ref.GetDeclared():
		// An engine built before contracts pins a digest: only a contract
		// with that definition serves it, never one on a label.
		if ref.GetIdentityDigest() == "" {
			return contractSelection{}, Fail(ReasonProfileChanged,
				"profile %q: the task carries no identity for it, so it cannot be held to what its run pinned", name)
		}
		if contracts[def].identity == ref.GetIdentityDigest() {
			return picked(def, false)
		}
		for _, label := range slices.Sorted(maps.Keys(contracts)) {
			if contracts[label].identity == ref.GetIdentityDigest() {
				return picked(label, false)
			}
		}
		return contractSelection{}, Fail(ReasonProfileChanged,
			"profile %q is defined on this Orker otherwise than when the run was submitted; "+
				"every Orker of a pool must define it the same way", name)
	case ref.GetDeclared():
		if len(pins) != 1 {
			return notServed("profile %q: the run pinned %d contracts for a task whose image declared one", name, len(pins))
		}
		label, digests := pins[0].GetLabel(), pins[0].GetIdentityDigests()
		if owner, owns := contracts[label]; owns && len(digests) > 0 {
			if slices.Contains(digests, owner.identity) {
				return picked(label, false)
			}
			return edited(label)
		}
		if slices.Contains(contracts[def].accepts, label) {
			return picked(def, true)
		}
		for _, other := range slices.Sorted(maps.Keys(contracts)) {
			if other != def && slices.Contains(contracts[other].accepts, label) {
				return picked(other, true)
			}
		}
		return notServed("profile %q: this Orker serves no contract %q, which the task's image needs; "+
			"the task runs on an Orker of its pool that serves it", name, label)
	default:
		labels := make([]string, 0, len(pins))
		for _, p := range pins {
			labels = append(labels, p.GetLabel())
			if p.GetLabel() != def || len(p.GetIdentityDigests()) == 0 {
				continue
			}
			if slices.Contains(p.GetIdentityDigests(), contracts[def].identity) {
				return picked(def, false)
			}
			return edited(def)
		}
		for _, label := range contracts[def].accepts {
			if slices.Contains(labels, label) {
				return picked(def, true)
			}
		}
		return notServed("profile %q: this Orker's default contract %q serves none of the contracts the run pinned; "+
			"the task runs on an Orker of its pool that serves them", name, def)
	}
}

// gateSelect is step 3c: for each ref, the profile offered, its devices
// unchanged since the boot audit (every contract's: drift latches for the
// whole name, so it is checked before any contract is chosen), then the
// contract that serves it. It returns the selected contracts' devices and
// the selections. inspect is what the host says a path is now.
func (s *profileState) gateSelect(refs []*orkstackv1.ProfileRef, inspect func(string) (hostDevice, error), now time.Time) ([]DeviceMapping, []contractSelection, *Failure) {
	var devices []DeviceMapping
	var picked []contractSelection
	for _, ref := range refs {
		name := ref.GetName()
		audit, ok := s.audit(name)
		if !ok {
			return nil, nil, Fail(ReasonProfileNotOffered,
				"profile %q is not offered by this Orker; the task runs on an Orker of its pool that offers it", name)
		}
		if failure := s.checkDevices(name, audit, inspect, now); failure != nil {
			return nil, nil, failure
		}
		sel, failure := selectContract(ref, audit)
		if failure != nil {
			return nil, nil, failure
		}
		for _, d := range sel.devices {
			devices = append(devices, DeviceMapping{HostPath: d.Host.Resolved, ContainerPath: d.ContainerPath, Permissions: d.Permissions})
		}
		picked = append(picked, sel)
	}
	return devices, picked, nil
}

// memoIdentity is the identity a cached task's memo key carries for ref: that
// of the contract serving it here, or, where none does, the ref's own pin
// (the memo step runs after 3c, so a served attempt always has one).
func (s *profileState) memoIdentity(ref *orkstackv1.ProfileRef) string {
	audit, ok := s.audit(ref.GetName())
	if !ok {
		return ref.GetIdentityDigest()
	}
	sel, failure := selectContract(ref, audit)
	if failure != nil {
		return ref.GetIdentityDigest()
	}
	return sel.Identity
}

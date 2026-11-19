package engine

import (
	"fmt"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ProfileRefsFor is the profiles one task of this run holds, as its
// ExecuteTaskInput carries them: each device or gpu profile it names, with
// the identity the plan pinned for the task's pool. A profile the plan did
// not pin is an error, never an empty digest an Orker could read as "no
// check". A pure function of its arguments, so workflow code may call it.
func (p *RunPlan) ProfileRefsFor(pool string, t *orkstackv1.Task) ([]*orkstackv1.ProfileRef, error) {
	var pinned map[string]string
	if p != nil {
		pinned = p.ProfileIdentities[pool]
	}
	var devices []*orkstackv1.ProfileRef
	for _, name := range t.GetDevices() {
		digest := pinned[name]
		if digest == "" {
			return nil, fmt.Errorf("task %s: the run pinned no identity for profile %s on pool %s", t.GetId(), name, pool)
		}
		devices = append(devices, &orkstackv1.ProfileRef{Name: name, IdentityDigest: digest})
	}
	return devices, nil
}

// ReasonProfileNotPinned refuses a task, before dispatch, that names a
// profile its run's plan pinned no identity for. Submit pins every profile it
// admits (judgeTaskProfiles), so only a defect reaches it; it fails the task
// explicitly rather than sending an identity an Orker could read as no check.
const ReasonProfileNotPinned = "PROFILE_NOT_PINNED"

// deviceRefs is the Devices an ExecuteTaskInput of t carries: nil for a task
// naming no profile, so its payload is byte-identical to one built before
// profiles existed. The path is entered only by a definition declaring
// `devices:`, which no recorded history holds, so it needs no version gate
// (AGENTS.md §7.2). Pure.
//
// Under a run that pinned contracts (profileContractsPath), each ref also
// carries the contracts it may run under: the one its image declared
// (device_contracts) or, for an undeclared profile, every contract the
// pool's Orkers served at submit.
func (s *runState) deviceRefs(pool string, t *orkstackv1.Task) ([]*orkstackv1.ProfileRef, error) {
	if len(t.GetDevices()) == 0 {
		return nil, nil
	}
	refs, err := s.plan.ProfileRefsFor(pool, t)
	if err == nil && s.profileContractsPath(t) {
		err = s.plan.addContracts(pool, t, refs)
	}
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ReasonProfileNotPinned, err)
	}
	return refs, nil
}

// profileContractsPath reports whether t's dispatches carry contracts and
// list the contract faults: it names a profile and its run pinned
// contracts. A definition that declares `devices:` alone enters the path,
// so no version gate guards it.
func (s *runState) profileContractsPath(t *orkstackv1.Task) bool {
	return len(t.GetDevices()) > 0 && len(s.plan.ProfileContracts) > 0
}

// addContracts sets each ref's contracts from the plan: a declared label's
// pinned definitions, or the pool's whole set. A run that pinned contracts
// pins them for every profile its tasks name, on every pool (submit,
// judgeTaskProfiles); a profile it holds none for on this pool, an empty
// set, or a declared label it pinned no definition for is an error, never
// an empty list an Orker could read as no check. Pure.
func (p *RunPlan) addContracts(pool string, t *orkstackv1.Task, refs []*orkstackv1.ProfileRef) error {
	for _, ref := range refs {
		pinned, ok := p.ProfileContracts[pool][ref.GetName()]
		if !ok {
			return fmt.Errorf("task %s: the run pinned no contracts for profile %s on pool %s", t.GetId(), ref.GetName(), pool)
		}
		if label, ok := t.GetDeviceContracts()[ref.GetName()]; ok {
			digests, found := pinned.Declared[label]
			if !found {
				return fmt.Errorf("task %s: the run pinned no definition of contract %s of profile %s on pool %s",
					t.GetId(), label, ref.GetName(), pool)
			}
			ref.Declared = true
			ref.Contracts = []*orkstackv1.ProfileContractPin{{Label: label, IdentityDigests: append([]string(nil), digests...)}}
			continue
		}
		if len(pinned.Set) == 0 {
			return fmt.Errorf("task %s: the run pinned an empty contract set for profile %s on pool %s", t.GetId(), ref.GetName(), pool)
		}
		for _, pin := range pinned.Set {
			ref.Contracts = append(ref.Contracts, &orkstackv1.ProfileContractPin{Label: pin.Label, IdentityDigests: append([]string(nil), pin.Digests...)})
		}
	}
	return nil
}

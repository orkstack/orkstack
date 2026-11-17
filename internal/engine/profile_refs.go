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
func (s *runState) deviceRefs(pool string, t *orkstackv1.Task) ([]*orkstackv1.ProfileRef, error) {
	if len(t.GetDevices()) == 0 {
		return nil, nil
	}
	refs, err := s.plan.ProfileRefsFor(pool, t)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ReasonProfileNotPinned, err)
	}
	return refs, nil
}

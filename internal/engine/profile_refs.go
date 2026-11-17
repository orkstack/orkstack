package engine

import (
	"fmt"

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

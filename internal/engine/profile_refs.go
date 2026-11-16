package engine

import (
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ProfileRefsFor is the profiles one task of this run holds, as its
// ExecuteTaskInput carries them: each device or gpu profile it names and its
// egress profile, with the identity the plan pinned for the task's pool. Only
// an egress profile may be unpinned (no Orker answered for it at submit); a
// device or gpu profile the plan did not pin is an error, never an empty
// digest an Orker could read as "no check". A pure function of its
// arguments, so workflow code may call it.
func (p *RunPlan) ProfileRefsFor(pool string, t *orkstackv1.Task) (devices []*orkstackv1.ProfileRef, egress *orkstackv1.ProfileRef, err error) {
	var pinned map[string]string
	if p != nil {
		pinned = p.ProfileIdentities[pool]
	}
	for _, name := range t.GetDevices() {
		digest := pinned[name]
		if digest == "" {
			return nil, nil, fmt.Errorf("task %s: the run pinned no identity for profile %s on pool %s", t.GetId(), name, pool)
		}
		devices = append(devices, &orkstackv1.ProfileRef{Name: name, IdentityDigest: digest})
	}
	if t.GetNetwork() == orkstackv1.NetworkMode_NETWORK_MODE_EGRESS {
		if t.GetNetworkProfile() == "" {
			return nil, nil, fmt.Errorf("task %s: its network is an egress profile but names none", t.GetId())
		}
		egress = &orkstackv1.ProfileRef{Name: t.GetNetworkProfile(), IdentityDigest: pinned[t.GetNetworkProfile()]}
	}
	return devices, egress, nil
}

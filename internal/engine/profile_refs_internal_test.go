package engine

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

func TestProfileRefsFor(t *testing.T) {
	plan := &RunPlan{ProfileIdentities: map[string]map[string]string{
		"gpu-amd": {"amd": "id-amd", "sensor": "id-sensor", "github-only": "id-egress"},
	}}
	egressTask := &orkstackv1.Task{
		Devices: []string{"amd", "sensor"}, Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS, NetworkProfile: "github-only",
	}
	for name, tc := range map[string]struct {
		plan        *RunPlan
		pool        string
		task        *orkstackv1.Task
		wantDevices []*orkstackv1.ProfileRef
		wantEgress  *orkstackv1.ProfileRef
		wantErr     bool
	}{
		"pinned": {plan, "gpu-amd", egressTask,
			[]*orkstackv1.ProfileRef{{Name: "amd", IdentityDigest: "id-amd"}, {Name: "sensor", IdentityDigest: "id-sensor"}},
			&orkstackv1.ProfileRef{Name: "github-only", IdentityDigest: "id-egress"}, false},
		"an unpinned egress profile travels unpinned": {plan, "other",
			&orkstackv1.Task{Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS, NetworkProfile: "github-only"},
			nil, &orkstackv1.ProfileRef{Name: "github-only"}, false},
		"an unpinned device profile is an error": {plan, "other", egressTask, nil, nil, true},
		"no profiles":                            {plan, "gpu-amd", &orkstackv1.Task{Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE}, nil, nil, false},
		"a profile name without egress mode is not egress": {plan, "gpu-amd",
			&orkstackv1.Task{NetworkProfile: "github-only"}, nil, nil, false},
		"an egress task naming no profile is an error": {plan, "gpu-amd",
			&orkstackv1.Task{Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS}, nil, nil, true},
		"nil plan with a device profile": {nil, "gpu-amd", egressTask, nil, nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			devices, egress, err := tc.plan.ProfileRefsFor(tc.pool, tc.task)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(devices) != len(tc.wantDevices) {
				t.Fatalf("devices = %v, want %v", devices, tc.wantDevices)
			}
			for i := range devices {
				if !proto.Equal(devices[i], tc.wantDevices[i]) {
					t.Errorf("devices[%d] = %v, want %v", i, devices[i], tc.wantDevices[i])
				}
			}
			if !proto.Equal(egress, tc.wantEgress) {
				t.Errorf("egress = %v, want %v", egress, tc.wantEgress)
			}
		})
	}
}

// A plan with no profile identities encodes exactly as one built before the
// field existed, so recorded histories keep matching.
func TestRunPlan_NoProfileIdentitiesEncodesAsBefore(t *testing.T) {
	raw, err := stablejson.Marshal(RunPlan{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "profile_identities") {
		t.Fatalf("an empty plan carries the field: %s", raw)
	}
}

package engine

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

func TestProfileRefsFor(t *testing.T) {
	plan := &RunPlan{ProfileIdentities: map[string]map[string]string{
		"gpu-amd": {"amd": "id-amd", "sensor": "id-sensor"},
	}}
	devicesTask := &orkstackv1.Task{Devices: []string{"amd", "sensor"}}
	for name, tc := range map[string]struct {
		plan        *RunPlan
		pool        string
		task        *orkstackv1.Task
		wantDevices []*orkstackv1.ProfileRef
		wantErr     bool
	}{
		"pinned": {plan, "gpu-amd", devicesTask,
			[]*orkstackv1.ProfileRef{{Name: "amd", IdentityDigest: "id-amd"}, {Name: "sensor", IdentityDigest: "id-sensor"}}, false},
		"an unpinned device profile is an error": {plan, "other", devicesTask, nil, true},
		"no profiles":                            {plan, "gpu-amd", &orkstackv1.Task{Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE}, nil, false},
		"nil plan with a device profile":         {nil, "gpu-amd", devicesTask, nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			devices, err := tc.plan.ProfileRefsFor(tc.pool, tc.task)
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

// TestRunPlan_ProfileContractsEncodeAsBeforeWhenUnset: a plan built before
// profile contracts existed, profiled or not, encodes byte for byte as it did,
// so recorded histories keep matching; one that carries them round-trips with
// its sets and declared pins in order.
func TestRunPlan_ProfileContractsEncodeAsBeforeWhenUnset(t *testing.T) {
	for name, tc := range map[string]struct {
		plan RunPlan
		want string
	}{
		"no profile": {RunPlan{Namespace: "default"}, `"namespace":"default"`},
		"a profile pinned before contracts": {
			RunPlan{Namespace: "default", ProfileIdentities: map[string]map[string]string{"lab": {"sensor": "d1"}}},
			`"profile_identities":{"lab":{"sensor":"d1"}}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := stablejson.Marshal(tc.plan)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "profile_contracts") || !strings.Contains(string(raw), tc.want) {
				t.Fatalf("plan encodes as %s; want %s and no profile_contracts", raw, tc.want)
			}
		})
	}

	plan := RunPlan{Namespace: "default", ProfileContracts: map[string]map[string]ProfileContracts{
		"lab": {"sensor": {
			Set:      []ContractPin{{Label: "1", Digests: []string{"d1"}}, {Label: "2", Digests: []string{"d2", "d3"}}},
			Declared: map[string][]string{"2": {"d2"}},
		}},
	}}
	raw, err := stablejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	const want = `"profile_contracts":{"lab":{"sensor":{"set":[{"label":"1","digests":["d1"]},{"label":"2","digests":["d2","d3"]}],"declared":{"2":["d2"]}}}}`
	if !strings.Contains(string(raw), want) {
		t.Fatalf("plan encodes as %s; want it to hold %s", raw, want)
	}
	// A pinned profile whose set is empty, from a nil or an empty slice,
	// encodes as [], never null.
	for _, set := range [][]ContractPin{nil, {}} {
		empty, err := stablejson.Marshal(ProfileContracts{Set: set})
		if err != nil || string(empty) != `{"set":[]}` {
			t.Fatalf("an empty set (%#v) encodes as %s, %v; want {\"set\":[]}", set, empty, err)
		}
	}
	// The same through the data converter that writes a plan into history.
	payload, err := converter.GetDefaultDataConverter().ToPayload(RunPlan{ProfileContracts: map[string]map[string]ProfileContracts{"lab": {"sensor": {}}}})
	if err != nil || !strings.Contains(string(payload.GetData()), `"sensor":{"set":[]}`) {
		t.Fatalf("the data converter writes an empty set as %s, %v; want []", payload.GetData(), err)
	}

	var back RunPlan
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	again, err := stablejson.Marshal(back)
	if err != nil || string(again) != string(raw) {
		t.Fatalf("the plan does not round-trip: %v\n%s\n%s", err, raw, again)
	}
}

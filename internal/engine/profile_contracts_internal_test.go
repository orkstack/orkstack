package engine

import (
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// contractsPlan pins profile "sensor" on pool "edge": identity "d1", the set
// of contracts 1 and 2, and label 2's own definition for declaring images.
func contractsPlan() RunPlan {
	return RunPlan{
		ProfileIdentities: map[string]map[string]string{"edge": {"sensor": "d1", "lidar": "d9"}},
		ProfileContracts: map[string]map[string]ProfileContracts{"edge": {
			"sensor": {
				Set:      []ContractPin{{Label: "1", Digests: []string{"d1"}}, {Label: "2", Digests: []string{"d2"}}},
				Declared: map[string][]string{"2": {"d2"}},
			},
			"lidar": {Set: []ContractPin{{Label: "1", Digests: []string{"d9"}}}},
		}},
	}
}

// TestDeviceRefs_BuildsDeclaredAndUndeclaredRefs: a profile the task's image
// declared a contract for carries that contract alone; one it did not
// carries the whole set the pool served at submit.
func TestDeviceRefs_BuildsDeclaredAndUndeclaredRefs(t *testing.T) {
	s := &runState{plan: contractsPlan()}
	task := &orkstackv1.Task{Id: "scan", Devices: []string{"sensor", "lidar"}, DeviceContracts: map[string]string{"sensor": "2"}}
	refs, err := s.deviceRefs("edge", task)
	if err != nil {
		t.Fatalf("deviceRefs: %v", err)
	}
	want := []*orkstackv1.ProfileRef{
		{Name: "sensor", IdentityDigest: "d1", Declared: true,
			Contracts: []*orkstackv1.ProfileContractPin{{Label: "2", IdentityDigests: []string{"d2"}}}},
		{Name: "lidar", IdentityDigest: "d9",
			Contracts: []*orkstackv1.ProfileContractPin{{Label: "1", IdentityDigests: []string{"d9"}}}},
	}
	if len(refs) != len(want) {
		t.Fatalf("refs %v", refs)
	}
	for i := range want {
		if !proto.Equal(refs[i], want[i]) {
			t.Errorf("ref %d = %v, want %v", i, refs[i], want[i])
		}
	}
}

// TestDeviceRefs_ADeclaredLabelTheRunDidNotPinIsRefused: a declared label
// the plan holds no definition for fails the task PROFILE_NOT_PINNED, never
// an empty contract an Orker could read as no check.
func TestDeviceRefs_ADeclaredLabelTheRunDidNotPinIsRefused(t *testing.T) {
	s := &runState{plan: contractsPlan()}
	task := &orkstackv1.Task{Id: "scan", Devices: []string{"sensor"}, DeviceContracts: map[string]string{"sensor": "7"}}
	if _, err := s.deviceRefs("edge", task); err == nil || categoryOf(err).appType() != ReasonProfileNotPinned {
		t.Fatalf("deviceRefs = %v, want PROFILE_NOT_PINNED", err)
	}
}

// TestDeviceRefs_AProfileWithoutContractsUnderAContractedRunIsRefused: a
// run that pinned contracts refuses a profile it holds none for on the
// task's pool, or an empty set, never sending an empty contract list.
func TestDeviceRefs_AProfileWithoutContractsUnderAContractedRunIsRefused(t *testing.T) {
	for name, mutate := range map[string]func(*RunPlan){
		"no entry for the pool":    func(p *RunPlan) { p.ProfileIdentities["batch"] = map[string]string{"sensor": "d1"} },
		"no entry for the profile": func(p *RunPlan) { delete(p.ProfileContracts["edge"], "sensor") },
		"an empty set":             func(p *RunPlan) { p.ProfileContracts["edge"]["sensor"] = ProfileContracts{Set: []ContractPin{}} },
	} {
		plan := contractsPlan()
		mutate(&plan)
		pool := "edge"
		if name == "no entry for the pool" {
			pool = "batch"
		}
		s := &runState{plan: plan}
		if _, err := s.deviceRefs(pool, &orkstackv1.Task{Id: "scan", Devices: []string{"sensor"}}); err == nil || categoryOf(err).appType() != ReasonProfileNotPinned {
			t.Errorf("%s: deviceRefs = %v, want PROFILE_NOT_PINNED", name, err)
		}
	}
}

// TestRunWorkflow_PlanWithoutContractsBuildsTheSameInput: a plan that pins
// no contract builds exactly the refs it built before contracts existed.
func TestRunWorkflow_PlanWithoutContractsBuildsTheSameInput(t *testing.T) {
	plan := contractsPlan()
	plan.ProfileContracts = nil
	s := &runState{plan: plan}
	refs, err := s.deviceRefs("edge", &orkstackv1.Task{Id: "scan", Devices: []string{"sensor"}})
	if err != nil || len(refs) != 1 || !proto.Equal(refs[0], &orkstackv1.ProfileRef{Name: "sensor", IdentityDigest: "d1"}) {
		t.Fatalf("refs %v, %v; want the identity alone", refs, err)
	}
}

// TestInterruptionTypes_ProfileFaultsPerUnit: a unit that names a profile
// under a run with contracts lists both contract faults after the three
// existing ones; any other lists exactly what it listed before.
func TestInterruptionTypes_ProfileFaultsPerUnit(t *testing.T) {
	b := testBudgets()
	plain := interruptionTypes(&b, &orkstackv1.TaskInterruptions{}, nil, false)
	profiled := interruptionTypes(&b, &orkstackv1.TaskInterruptions{}, nil, true)
	extra := []string{interruptionTypePrefix + profileChangedReason, interruptionTypePrefix + profileContractNotServedReason}
	if !slices.Equal(profiled[:4], plain[:4]) || !slices.Equal(profiled[4:6], extra) || len(profiled) != len(plain)+2 {
		t.Fatalf("profiled %v, plain %v; want the two contract faults after the existing four", profiled, plain)
	}
	for _, typ := range extra {
		if slices.Contains(plain, typ) {
			t.Fatalf("a unit naming no profile lists %s", typ)
		}
	}
}

// TestInterruption_ContractFaultsShareTheFaultBudget: a changed profile and
// an unserved contract are never-started faults, spending the one fault
// budget together, never the lost one.
func TestInterruption_ContractFaultsShareTheFaultBudget(t *testing.T) {
	changed := classify(typedInterruption(profileChangedReason, &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o1"}))
	notServed := classify(typedInterruption(profileContractNotServedReason, &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o2"}))
	if changed.kind != neverStartedFault || notServed.kind != neverStartedFault {
		t.Fatalf("kinds %v and %v, want never-started faults", changed.kind, notServed.kind)
	}
	b := testBudgets()
	st, runLoss := &orkstackv1.TaskInterruptions{}, &orkstackv1.RunLossEvents{}
	for i := range b.faults {
		ev := changed
		if i%2 == 1 {
			ev = notServed
		}
		if d := applyN(t, st, b, 1, runLoss, ev, 1)[0]; !d.redispatch {
			t.Fatalf("fault %d: %+v", i+1, d)
		}
	}
	if d := applyN(t, st, b, 1, runLoss, notServed, 1)[0]; !d.fail || st.GetLast().GetReason() != profileContractNotServedReason {
		t.Fatalf("the fault past the shared budget: %+v last %v", d, st.GetLast())
	}
	if st.GetLost() != 0 || runLoss.GetCount() != 0 {
		t.Errorf("contract faults spent lost %d, loss events %d", st.GetLost(), runLoss.GetCount())
	}
}

// TestInterruption_ContractFaultCounterResetsOnStart: an attempt that
// started resets the fault budget the contract faults spent.
func TestInterruption_ContractFaultCounterResetsOnStart(t *testing.T) {
	changed := classify(typedInterruption(profileChangedReason, &orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "o1"}))
	b := testBudgets()
	st := &orkstackv1.TaskInterruptions{}
	applyN(t, st, b, 1, nil, changed, int(b.faults)-1)
	applyN(t, st, b, 1, nil, startedHandBack(), 1)
	if st.GetStoreFaults() != 0 {
		t.Fatalf("faults %d after a started attempt, want 0", st.GetStoreFaults())
	}
	for i, d := range applyN(t, st, b, 1, nil, changed, int(b.faults)) {
		if !d.redispatch {
			t.Fatalf("fault %d after the reset: %+v", i+1, d)
		}
	}
}

// TestTaskDispatchInput_CarriesTheContracts: under a run that pinned
// contracts, a profiled task is dispatched with the contracts it may run
// under, and a plain task still carries no profile at all.
func TestTaskDispatchInput_CarriesTheContracts(t *testing.T) {
	profiled := &orkstackv1.Task{Id: "scan", Devices: []string{"sensor"}, DeviceContracts: map[string]string{"sensor": "2"}}
	plain := &orkstackv1.Task{Id: "plain"}
	s := &runState{runID: "ork-c", wf: &orkstackv1.Orkflow{Name: "c", Tasks: []*orkstackv1.Task{profiled, plain}}, plan: contractsPlan()}
	in, err := s.taskDispatchInput(profiled, "scan", "edge", nil)
	if err != nil {
		t.Fatalf("dispatch input: %v", err)
	}
	if d := in.GetDevices(); len(d) != 1 || !d[0].GetDeclared() || len(d[0].GetContracts()) != 1 || d[0].GetContracts()[0].GetLabel() != "2" {
		t.Fatalf("devices = %v, want sensor declared under contract 2", d)
	}
	if in, err := s.taskDispatchInput(plain, "plain", "edge", nil); err != nil || in.GetDevices() != nil {
		t.Fatalf("a plain task's devices = %v, %v", in.GetDevices(), err)
	}
}

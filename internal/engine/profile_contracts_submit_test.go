package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

const (
	digest1 = "1111000000000000000000000000000000000000000000000000000000000000"
	digest2 = "2222000000000000000000000000000000000000000000000000000000000000"
	digest3 = "3333000000000000000000000000000000000000000000000000000000000000"
)

// contract is one contract an Orker reports for a profile.
func contract(label, digest string, accepts ...string) *orkstackv1.OrkerProfileContract {
	return &orkstackv1.OrkerProfileContract{Label: label, IdentityDigest: digest, Accepts: accepts}
}

// sensorOrker is an Orker reporting profile "sensor" (devices) with these
// contracts, the first its default.
func sensorOrker(id string, contracts ...*orkstackv1.OrkerProfileContract) *orkstackv1.DescribeOrkerResult {
	return sensorOrkerDefault(id, contracts[0].GetLabel(), contracts...)
}

// sensorOrkerDefault is sensorOrker with the default contract named.
func sensorOrkerDefault(id, def string, contracts ...*orkstackv1.OrkerProfileContract) *orkstackv1.DescribeOrkerResult {
	var own string
	for _, c := range contracts {
		if c.GetLabel() == def {
			own = c.GetIdentityDigest()
		}
	}
	return deviceOrker(id, devicesKind, &orkstackv1.OrkerProfile{
		Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, IdentityDigest: own,
		DefaultContract: def, Contracts: contracts,
	})
}

var (
	undeclaredTask = &orkstackv1.Task{Id: "read", Devices: []string{"sensor"}}
	declaredTwo    = &orkstackv1.Task{Id: "scan", Devices: []string{"sensor"}, DeviceContracts: map[string]string{"sensor": "2"}}
)

// TestJudgeTaskProfiles_ContractSets holds submit's contract pinning to the
// rollout table (docs/DEPLOY.md, "Changing a profile"): an undeclared task
// pins the labels every serving Orker's default contract serves, a declared
// one the label its image declared, each with the definitions its owners
// gave, and a pool that cannot serve one of them is refused, naming no
// digest.
func TestJudgeTaskProfiles_ContractSets(t *testing.T) {
	a1 := func(id string) *orkstackv1.DescribeOrkerResult { return sensorOrker(id, contract("1", digest1)) }
	b2acc1 := func(id string) *orkstackv1.DescribeOrkerResult {
		return sensorOrker(id, contract("2", digest2, "1"))
	}
	both := func(id string) *orkstackv1.DescribeOrkerResult {
		return sensorOrkerDefault(id, "1", contract("1", digest1), contract("2", digest2))
	}
	for _, tc := range []struct {
		name     string
		tasks    []*orkstackv1.Task
		serving  []*orkstackv1.DescribeOrkerResult
		cordoned []*orkstackv1.DescribeOrkerResult
		want     ProfileContracts
		refused  string
	}{
		{name: "one contract everywhere", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{a1("a"), a1("b")},
			want:    ProfileContracts{Set: []ContractPin{{Label: "1", Digests: []string{digest1}}}}},
		{name: "mid-roll, the new default accepts the old", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{a1("a"), b2acc1("b")},
			want:    ProfileContracts{Set: []ContractPin{{Label: "1", Digests: []string{digest1}}}}},
		{name: "the old Orker operator-cordoned gives its definition", tasks: []*orkstackv1.Task{undeclaredTask},
			serving:  []*orkstackv1.DescribeOrkerResult{b2acc1("b"), b2acc1("c")},
			cordoned: []*orkstackv1.DescribeOrkerResult{a1("a")},
			want: ProfileContracts{Set: []ContractPin{
				{Label: "1", Digests: []string{digest1}}, {Label: "2", Digests: []string{digest2}}}}},
		{name: "the old Orker silent pins its label alone", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{b2acc1("b"), b2acc1("c")},
			want: ProfileContracts{Set: []ContractPin{
				{Label: "1"}, {Label: "2", Digests: []string{digest2}}}}},
		{name: "two new defaults that both accept the old", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{b2acc1("b"), sensorOrker("c", contract("3", digest3, "1"))},
			want:    ProfileContracts{Set: []ContractPin{{Label: "1"}}}},
		{name: "a breaking default change with no accepts", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{a1("a"), sensorOrker("b", contract("2", digest2))},
			refused: `PROFILE_MISMATCH`},
		{name: "a declared label both Orkers own", tasks: []*orkstackv1.Task{declaredTwo},
			serving: []*orkstackv1.DescribeOrkerResult{both("a"), both("b")},
			want: ProfileContracts{
				Declared: map[string][]string{"2": {digest2}}}},
		{name: "a declared label one Orker lacks", tasks: []*orkstackv1.Task{declaredTwo},
			serving: []*orkstackv1.DescribeOrkerResult{both("a"), a1("b")},
			refused: `PROFILE_UNAVAILABLE`},
		{name: "a declared label one Orker accepts", tasks: []*orkstackv1.Task{declaredTwo},
			serving: []*orkstackv1.DescribeOrkerResult{both("a"), sensorOrker("b", contract("3", digest3, "2", "1"))},
			want: ProfileContracts{
				Declared: map[string][]string{"2": {digest2}}}},
		{name: "declared and undeclared tasks of one profile", tasks: []*orkstackv1.Task{undeclaredTask, declaredTwo},
			serving: []*orkstackv1.DescribeOrkerResult{both("a"), both("b")},
			want: ProfileContracts{
				Set:      []ContractPin{{Label: "1", Digests: []string{digest1}}},
				Declared: map[string][]string{"2": {digest2}}}},
		{name: "one label, two definitions", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{a1("a"), sensorOrker("b", contract("1", digest3))},
			refused: `contract "1"`},
		{name: "a profile reported with no default contract", tasks: []*orkstackv1.Task{undeclaredTask},
			serving: []*orkstackv1.DescribeOrkerResult{sensorOrkerDefault("a", "9", contract("1", digest1))},
			refused: `no default contract`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeTaskProfiles("ns", "edge", tc.tasks, tc.serving, tc.cordoned)
			if tc.refused != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refused) {
					t.Fatalf("judge = %+v, %v; want a refusal saying %s", got.contracts, err, tc.refused)
				}
				for _, d := range []string{digest1, digest2, digest3} {
					if strings.Contains(err.Error(), d) {
						t.Fatalf("a refusal names a digest: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("judge: %v", err)
			}
			if !reflect.DeepEqual(got.contracts["sensor"], tc.want) {
				t.Fatalf("pinned %+v, want %+v", got.contracts["sensor"], tc.want)
			}
			if got.identities["sensor"] == "" {
				t.Fatal("field 2 is empty for a pinned profile")
			}
		})
	}
}

// TestJudgeTaskProfiles_NoOwnerSeenIsPinned: a label the serving Orkers only
// accept, and no answering Orker owns, is pinned with no definition, and the
// judgement says a second look may find one.
func TestJudgeTaskProfiles_NoOwnerSeenIsPinned(t *testing.T) {
	got, err := judgeTaskProfiles("ns", "edge", []*orkstackv1.Task{undeclaredTask}, []*orkstackv1.DescribeOrkerResult{
		sensorOrker("b", contract("2", digest2, "1")), sensorOrker("c", contract("3", digest3, "1")),
	}, nil)
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if want := []ContractPin{{Label: "1"}}; !reflect.DeepEqual(got.contracts["sensor"].Set, want) {
		t.Fatalf("set = %+v, want %+v", got.contracts["sensor"].Set, want)
	}
	if !got.unpinnedDigest {
		t.Fatal("a label pinned with no definition does not ask for a second look")
	}
}

// TestJudgeTaskProfiles_FieldTwoNeverEmpty: the identity a ref carries
// beside its contracts is label 1's one definition, else the first pinned
// definition by label, else the first serving Orker's own default, and never
// empty.
func TestJudgeTaskProfiles_FieldTwoNeverEmpty(t *testing.T) {
	for _, tc := range []struct {
		name    string
		tasks   []*orkstackv1.Task
		serving []*orkstackv1.DescribeOrkerResult
		want    string
	}{
		{"label 1's definition", []*orkstackv1.Task{undeclaredTask},
			[]*orkstackv1.DescribeOrkerResult{sensorOrker("a", contract("1", digest1)), sensorOrker("b", contract("2", digest2, "1"))}, digest1},
		{"the first pinned definition by label", []*orkstackv1.Task{declaredTwo},
			[]*orkstackv1.DescribeOrkerResult{sensorOrker("a", contract("3", digest3, "2")),
				sensorOrkerDefault("b", "3", contract("2", digest2), contract("3", digest3, "2"))}, digest2},
		{"the first serving Orker's own default", []*orkstackv1.Task{undeclaredTask},
			[]*orkstackv1.DescribeOrkerResult{sensorOrker("b", contract("2", digest2, "1")), sensorOrker("c", contract("3", digest3, "1"))}, digest2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeTaskProfiles("ns", "edge", tc.tasks, tc.serving, nil)
			if err != nil {
				t.Fatalf("judge: %v", err)
			}
			if got.identities["sensor"] != tc.want {
				t.Fatalf("field 2 = %q, want %q", got.identities["sensor"], tc.want)
			}
		})
	}
}

// twiceFleet is a FleetReader whose silent Orkers answer from their second
// question on: an Orker a census read missed, which a second look finds.
type twiceFleet struct {
	mu     sync.Mutex
	orkers []*orkstackv1.DescribeOrkerResult
	silent map[string]*orkstackv1.DescribeOrkerResult
	asked  map[string]int
}

func (f *twiceFleet) ObserveOrkers(context.Context) ([]OrkerPresence, error) {
	var presence []OrkerPresence
	for _, o := range f.orkers {
		presence = append(presence, OrkerPresence{Identity: o.GetOrkerId(), LastSeen: time.Now()})
	}
	for id := range f.silent {
		presence = append(presence, OrkerPresence{Identity: id, LastSeen: time.Now()})
	}
	return presence, nil
}

func (f *twiceFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked[id]++
	for _, o := range f.orkers {
		if o.GetOrkerId() == id {
			return o, nil
		}
	}
	if o, ok := f.silent[id]; ok && f.asked[id] > 1 {
		return o, nil
	}
	return nil, fmt.Errorf("orker %q did not answer", id)
}

// TestJudgeTaskProfiles_AsksSilentOrkersAgain: a pinned label no answering
// Orker gave a definition for, with an Orker of the census silent, makes the
// census ask the silent Orkers once more; the one that answers gives the
// definition, and nothing is asked a third time.
func TestJudgeTaskProfiles_AsksSilentOrkersAgain(t *testing.T) {
	inPool := func(o *orkstackv1.DescribeOrkerResult) *orkstackv1.DescribeOrkerResult {
		o.Pool = "edge"
		return o
	}
	fleet := &twiceFleet{
		orkers: []*orkstackv1.DescribeOrkerResult{
			inPool(sensorOrker("b", contract("2", digest2, "1"))), inPool(sensorOrker("c", contract("2", digest2, "1")))},
		silent: map[string]*orkstackv1.DescribeOrkerResult{"a": inPool(sensorOrker("a", contract("1", digest1)))},
		asked:  map[string]int{},
	}
	r := &PoolResolver{Fleet: fleet}
	census := newPoolCensus(fleet)
	pins, err := r.checkProfiles(context.Background(), census, "ns", "wf", "edge", []*orkstackv1.Task{undeclaredTask})
	if err != nil {
		t.Fatalf("checkProfiles: %v", err)
	}
	want := []ContractPin{{Label: "1", Digests: []string{digest1}}}
	if got := pins.contracts["sensor"].Set; !reflect.DeepEqual(got, want) {
		t.Fatalf("set = %+v, want %+v (the silent Orker's definition, from the second look)", got, want)
	}
	if fleet.asked["a"] != 2 {
		t.Fatalf("the silent Orker was asked %d times, want 2", fleet.asked["a"])
	}
	if _, err := r.checkProfiles(context.Background(), census, "ns", "wf", "edge", []*orkstackv1.Task{undeclaredTask}); err != nil {
		t.Fatal(err)
	}
	if fleet.asked["a"] != 2 {
		t.Fatalf("a second judgement on one census asked the silent Orker again (%d)", fleet.asked["a"])
	}
}

// TestRunPlan_ProfileCensusReadAtIsTheOldest: the freshness a submit is
// judged by is the oldest census any plan of its run tree pinned contracts
// from, none when none did, and it is never encoded.
func TestRunPlan_ProfileCensusReadAtIsTheOldest(t *testing.T) {
	t0 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	pins := profilePins{identities: map[string]string{"sensor": digest1},
		contracts: map[string]ProfileContracts{"sensor": {Set: []ContractPin{{Label: "1", Digests: []string{digest1}}}}}}
	var child RunPlan
	child.pinProfiles("edge", pins, t0)
	var parent RunPlan
	if !parent.ProfileCensusReadAt().IsZero() {
		t.Fatal("a plan that pinned nothing has a census time")
	}
	parent.pinProfiles("edge", pins, t0.Add(30*time.Second))
	parent.pinProfiles("other", profilePins{}, t0.Add(-time.Hour))
	parent.Children = map[string]*ChildRunPlan{"sub": {Plan: child}}
	if got := parent.ProfileCensusReadAt(); !got.Equal(t0) {
		t.Fatalf("ProfileCensusReadAt = %v, want the child's %v", got, t0)
	}
	raw, err := stablejson.Marshal(parent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "census") || strings.Contains(string(raw), "2026-11-19") {
		t.Fatalf("the plan encodes its census time: %s", raw)
	}
}

// TestPoolCensus_CordonedOwnersAreOperatorCordoned: the Orkers whose
// definitions a profiled submit reads beside the serving ones are those an
// operator cordoned, never one holding a profile drift (alone or beside an
// operator's cordon), nor one that serves.
func TestPoolCensus_CordonedOwnersAreOperatorCordoned(t *testing.T) {
	operator := sensorOrker("a", contract("1", digest1))
	operator.Pool, operator.Cordoned, operator.CordonReason = "edge", true, "decommission"
	drifted := sensorOrker("b", contract("1", digest3))
	drifted.Pool, drifted.Cordoned = "edge", true
	drifted.ProfileCordons = []*orkstackv1.ProfileCordon{{Profile: "sensor", Cause: orkstackv1.ProfileCordon_CAUSE_DRIFT}}
	both := sensorOrker("d", contract("1", digest3))
	both.Pool, both.Cordoned, both.CordonReason = "edge", true, "decommission"
	both.ProfileCordons = []*orkstackv1.ProfileCordon{{Profile: "sensor", Cause: orkstackv1.ProfileCordon_CAUSE_DRIFT}}
	serving := sensorOrker("c", contract("2", digest2, "1"))
	serving.Pool = "edge"
	census := censusOf([]FleetOrker{{Described: operator}, {Described: drifted}, {Described: both}, {Described: serving}})
	owners, err := census.cordonedOwners(context.Background(), "edge")
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 1 || owners[0].Described.GetOrkerId() != "a" {
		t.Fatalf("cordoned owners = %v, want only the operator-cordoned a", owners)
	}
}

// brokenFleet is a FleetReader whose inventory cannot be read.
type brokenFleet struct{}

func (brokenFleet) ObserveOrkers(context.Context) ([]OrkerPresence, error) {
	return nil, fmt.Errorf("the presence channel did not answer")
}

func (brokenFleet) DescribeOrkerByID(context.Context, string) (*orkstackv1.DescribeOrkerResult, error) {
	return nil, fmt.Errorf("unreachable")
}

// TestCheckProfiles_AnUnreadableCensusIsUnreachable: a census that could
// not be read measured nothing, so a profiled submit is refused as an
// unreachable pool (Unavailable, try again), never PROFILE_UNAVAILABLE.
func TestCheckProfiles_AnUnreadableCensusIsUnreachable(t *testing.T) {
	r := &PoolResolver{Fleet: brokenFleet{}}
	_, err := r.checkProfiles(context.Background(), newPoolCensus(brokenFleet{}), "ns", "wf", "edge",
		[]*orkstackv1.Task{undeclaredTask})
	var unreachable *PoolUnreachableError
	if !errors.As(err, &unreachable) || unreachable.Pool != "edge" {
		t.Fatalf("checkProfiles = %v, want PoolUnreachableError for edge", err)
	}
}

package engine_test

import (
	"context"
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

const sensorDigest = "5e5e000000000000000000000000000000000000000000000000000000000000"

const profilePoolsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: profiled
tasks:
  - id: read
    uses: shell
    pool: edge
    devices:
      - sensor
    with: {op: exec}
`

func sensorOrker(id string) *orkstackv1.DescribeOrkerResult {
	o := currentOrker(id, "edge", 0)
	o.ProfileKindsSupported = []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
	o.Profiles = []*orkstackv1.OrkerProfile{{
		Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, IdentityDigest: sensorDigest,
		DefaultContract: "1", Contracts: []*orkstackv1.OrkerProfileContract{{Label: "1", IdentityDigest: sensorDigest}},
	}}
	return o
}

func profileResolver(fleet engine.FleetReader, orkers int) *engine.PoolResolver {
	edge, _ := task.QueueForPool("edge", "")
	return &engine.PoolResolver{Probe: livePools{edge: orkers}, Fleet: fleet}
}

// TestPoolResolverPlan_PinsTheProfileIdentities: a submit whose task names a
// profile pins, per pool, the identity every Orker there offers it with, and
// the contracts they serve it under.
func TestPoolResolverPlan_PinsTheProfileIdentities(t *testing.T) {
	_, plan, err := profileResolver(fleetOf(sensorOrker("e1"), sensorOrker("e2")), 2).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "lab"}, compile(t, profilePoolsDocument), false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.ProfileIdentities["edge"]["sensor"]; got != sensorDigest {
		t.Fatalf("ProfileIdentities = %v", plan.ProfileIdentities)
	}
	set := plan.ProfileContracts["edge"]["sensor"].Set
	if len(set) != 1 || set[0].Label != "1" || len(set[0].Digests) != 1 || set[0].Digests[0] != sensorDigest {
		t.Fatalf("ProfileContracts = %+v", plan.ProfileContracts)
	}
	if plan.ProfileCensusReadAt().IsZero() {
		t.Fatal("a plan that pinned contracts says no census was read")
	}
}

// TestPoolResolverPlan_ProfilesAreNeverAssumed: where nothing says a profile
// is offered — no Orker answered, or the pool has none and allow_empty_pool
// is set — the submit is refused PROFILE_UNAVAILABLE, never passed.
func TestPoolResolverPlan_ProfilesAreNeverAssumed(t *testing.T) {
	for name, resolver := range map[string]*engine.PoolResolver{
		"no Orker answered":            profileResolver(&fakeFleet{silent: []string{"e1"}}, 1),
		"an empty pool, allowed empty": profileResolver(fleetOf(), 0),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "lab"},
				compile(t, profilePoolsDocument), true)
			var u *engine.ProfileUnavailableError
			if !errors.As(err, &u) || u.Profile != "sensor" || u.Pool != "edge" {
				t.Fatalf("Plan = %v, want PROFILE_UNAVAILABLE for sensor on edge", err)
			}
		})
	}
}

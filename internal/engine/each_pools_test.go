package engine_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// eachFleet is one current Orker for each of eachPoolsDocument's pools, with
// gpu's Orker (gpu-3, build 2026.09.01) answering each_supported as given.
func eachFleet(gpuSupported bool) *fakeFleet {
	gpu := currentOrker("gpu-3", "gpu", 0)
	gpu.BuildId = "2026.09.01"
	gpu.EachSupported = gpuSupported
	return fleetOf(currentOrker("cpu-1", "cpu", 0), gpu, currentOrker("io-1", "io", 0))
}

const eachPoolsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: each-pools
tasks:
  - id: list
    uses: shell
    pool: cpu
    with: {op: exec}
  - id: convert
    uses: shell
    pool: gpu
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 10}
    with: {op: exec}
  - id: merge
    uses: shell
    pool: gpu
    needs: [convert]
    with: {op: exec}
  - id: report
    uses: shell
    pool: io
    needs: [merge]
    with: {op: exec}
`

func eachPoolsResolver(fleet engine.FleetReader, logger *slog.Logger) *engine.PoolResolver {
	cpu, _ := task.QueueForPool("cpu", "")
	gpu, _ := task.QueueForPool("gpu", "")
	io, _ := task.QueueForPool("io", "")
	return &engine.PoolResolver{
		Probe:  livePools{cpu: 1, gpu: 1, io: 1},
		Fleet:  fleet,
		Logger: logger,
	}
}

func TestPoolResolverPlan_RefusesAPoolWhoseOrkerPredatesFanOut(t *testing.T) {
	_, _, err := eachPoolsResolver(eachFleet(false), nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false)
	var refused *engine.PoolNoEachSupportError
	if !errors.As(err, &refused) {
		t.Fatalf("Plan = %v, want PoolNoEachSupportError", err)
	}
	if refused.Pool != "gpu" || fmt.Sprint(refused.Tasks) != "[convert merge]" {
		t.Fatalf("refusal = %+v", refused)
	}
	for _, want := range []string{
		`pool "gpu" (namespace "data", tasks: convert, merge)`,
		`Orker "gpu-3" (build "2026.09.01")`, "predates fan-out (each:) support",
		"give merge no items to read", "Upgrade the pool's Orkers",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q lacks %q", err.Error(), want)
		}
	}
	if _, _, err := eachPoolsResolver(eachFleet(true), nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("a pool whose Orker supports fan-out = %v", err)
	}
}

// Any Orker of a pool may take an item, so one Orker that predates fan-out
// refuses the pool even when its sibling answers first and supports it.
func TestPoolResolverPlan_OneOldOrkerOfAPoolRefusesAFanOut(t *testing.T) {
	fleet := eachFleet(false)
	fleet.orkers = append(fleet.orkers, currentOrker("gpu-1", "gpu", 0))
	_, _, err := eachPoolsResolver(fleet, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false)
	var refused *engine.PoolNoEachSupportError
	if !errors.As(err, &refused) || refused.OrkerID != "gpu-3" {
		t.Fatalf("Plan = %v, want PoolNoEachSupportError naming gpu-3", err)
	}
}

func TestPoolResolverPlan_IgnoresADescribeFailure(t *testing.T) {
	var logged strings.Builder
	fleet := &fakeFleet{silent: []string{"cpu-1", "gpu-3", "io-1"}}
	if _, _, err := eachPoolsResolver(fleet, slog.New(slog.NewTextHandler(&logged, nil))).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("a describe failure refused the submit: %v", err)
	}
	if !strings.Contains(logged.String(), "whether it supports fan-out is unknown") {
		t.Fatalf("the unmeasured probe was not logged: %s", logged.String())
	}
}

func TestPoolResolverPlan_AllowEmptyPoolDoesNotDowngradeIt(t *testing.T) {
	_, _, err := eachPoolsResolver(eachFleet(false), nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), true)
	var refused *engine.PoolNoEachSupportError
	if !errors.As(err, &refused) {
		t.Fatalf("Plan with allow_empty_pool = %v, want the refusal to stand", err)
	}
}

func TestPoolResolverPlan_JudgesOnlyPoolsOfFanOutsAndTheirDirectDependents(t *testing.T) {
	// cpu holds the source and io a task two hops down, so neither pool's
	// Orker is held to fan-out, however old.
	fleet := eachFleet(true)
	for _, orker := range fleet.orkers {
		if orker.GetPool() != "gpu" {
			orker.EachSupported = false
		}
	}
	if _, _, err := eachPoolsResolver(fleet, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	// An Orkflow with no fan-out on live pools reads no census at all.
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}, Fleet: refusingFleet{t: t}}
	if _, _, err := resolver.Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, twoPoolDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
}

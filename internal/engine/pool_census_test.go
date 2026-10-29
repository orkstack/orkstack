package engine_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// A pool's channel shows a poller only while one of its Orkers has a free
// slot, so a pool whose every Orker is busy reads as nobody's on that channel
// alone. These are the census's answers to the questions the channel cannot
// settle (engine.poolCensus), stated with a fleet instead of workers.

const gpuOnlyDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: gpu-only
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
`

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPoolResolver_ABusyPoolIsLiveAtSubmit(t *testing.T) {
	// Nothing polls gpu's channel: both Orkers hold every slot they have.
	fleet := fleetOf(currentOrker("gpu-1", "gpu", 0), currentOrker("gpu-2", "gpu", 0))
	resolver := &engine.PoolResolver{Probe: livePools{}, Fleet: fleet}
	_, plan, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, gpuOnlyDocument), false)
	if err != nil {
		t.Fatalf("Plan = %v, want a busy pool accepted", err)
	}
	if gpuQueue, _ := task.QueueForPool("gpu", ""); plan.Routes["gpu"] != gpuQueue {
		t.Fatalf("routes = %v", plan.Routes)
	}
}

func TestPoolResolver_TheCensusDoesNotVouchForAnotherPoolOrACordonedOrker(t *testing.T) {
	cordoned := currentOrker("gpu-1", "gpu", 0)
	cordoned.Cordoned = true
	for name, fleet := range map[string]*fakeFleet{
		"an Orker of another pool": fleetOf(currentOrker("cpu-1", "cpu", 0)),
		"a cordoned Orker":         fleetOf(cordoned),
		"no Orker at all":          fleetOf(),
	} {
		t.Run(name, func(t *testing.T) {
			resolver := &engine.PoolResolver{Probe: livePools{}, Fleet: fleet}
			_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
				compile(t, gpuOnlyDocument), false)
			var empty *engine.PoolEmptyError
			if !errors.As(err, &empty) || empty.Pool != "gpu" {
				t.Fatalf("Plan = %v, want PoolEmptyError for gpu", err)
			}
		})
	}
}

func TestPoolResolver_AnInventoryThatCannotBeReadIsUnreachableNotEmpty(t *testing.T) {
	var logged strings.Builder
	resolver := &engine.PoolResolver{
		Probe:  livePools{},
		Fleet:  &fakeFleet{observeErr: errors.New("frontend unavailable")},
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	}
	_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, gpuOnlyDocument), false)
	var unreachable *engine.PoolUnreachableError
	if !errors.As(err, &unreachable) || unreachable.Pool != "gpu" {
		t.Fatalf("Plan = %v, want PoolUnreachableError for gpu", err)
	}
	if !strings.Contains(err.Error(), "frontend unavailable") {
		t.Fatalf("the refusal hides its cause: %v", err)
	}
	// allow_empty_pool accepts the pool busy or empty, so the unknown changes
	// nothing it would decide; it is recorded, not refused.
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, gpuOnlyDocument), true); err != nil {
		t.Fatalf("Plan with allow_empty_pool = %v, want it accepted", err)
	}
	for _, want := range []string{"allow_empty_pool accepts it either way", "submit into empty pool"} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("the override was not recorded with %q: %s", want, logged.String())
		}
	}
}

// An Orker that does not answer does not say which pool it serves, so it is
// not counted, and the decision is recorded with that gap; one answer of the
// pool's own settles it, whoever else is silent.
func TestPoolResolver_AnOrkerThatDidNotAnswerIsNotCountedAndTheGapIsLogged(t *testing.T) {
	var logged strings.Builder
	resolver := &engine.PoolResolver{
		Probe:  livePools{},
		Fleet:  &fakeFleet{silent: []string{"gpu-1"}},
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	}
	_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, gpuOnlyDocument), false)
	var empty *engine.PoolEmptyError
	if !errors.As(err, &empty) {
		t.Fatalf("Plan = %v, want PoolEmptyError", err)
	}
	for _, want := range []string{"were not counted", "pool=gpu", "unanswered=1"} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("the uncounted Orker was not recorded with %q: %s", want, logged.String())
		}
	}
	resolver.Fleet = &fakeFleet{orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("gpu-2", "gpu", 0)},
		silent: []string{"gpu-1"}}
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, gpuOnlyDocument), false); err != nil {
		t.Fatalf("Plan = %v, want the busy pool accepted", err)
	}
}

func TestPoolResolver_ABusyPoolAnswersItsPinProbe(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	for name, tc := range map[string]struct {
		probe  livePools
		orkers []*orkstackv1.DescribeOrkerResult
		host   bool
	}{
		"busy, no pin host":       {probe: livePools{}, orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("gpu-1", "gpu", 0)}},
		"busy, one pin host":      {probe: livePools{}, orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("gpu-1", "gpu", 0), currentOrker("gpu-2", "gpu", 1)}, host: true},
		"idle, no pin host":       {probe: livePools{gpuQueue: 1}, orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("gpu-1", "gpu", 0)}},
		"idle, cordoned host":     {probe: livePools{gpuQueue: 2}, orkers: []*orkstackv1.DescribeOrkerResult{cordonedOrker("gpu-1", "gpu", 1), currentOrker("gpu-2", "gpu", 0)}},
		"idle, uncordoned host":   {probe: livePools{gpuQueue: 2}, orkers: []*orkstackv1.DescribeOrkerResult{cordonedOrker("gpu-1", "gpu", 0), currentOrker("gpu-2", "gpu", 2)}, host: true},
		"another pool's pin host": {probe: livePools{gpuQueue: 1}, orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("gpu-1", "gpu", 0), currentOrker("cpu-1", "cpu", 4)}},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := &engine.PoolResolver{Probe: tc.probe, Fleet: fleetOf(tc.orkers...), Logger: quietLogger()}
			_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ci"},
				compile(t, pinnedSubmitDocument), false)
			var noHost *engine.PoolNoPinHostError
			switch {
			case tc.host && err != nil:
				t.Fatalf("Plan = %v, want the pinned submit accepted", err)
			case !tc.host && !errors.As(err, &noHost):
				t.Fatalf("Plan = %v, want PoolNoPinHostError", err)
			}
		})
	}
}

func cordonedOrker(id, pool string, maxPins int32) *orkstackv1.DescribeOrkerResult {
	orker := currentOrker(id, pool, maxPins)
	orker.Cordoned, orker.CordonReason = true, "patch window"
	return orker
}

// One census per submit, however many pools and probes ask it.
func TestPoolResolver_PlanReadsTheInventoryOnce(t *testing.T) {
	const document = `
apiVersion: orkstack/v1
kind: Orkflow
name: two-busy-pools
pins:
  build: {}
tasks:
  - id: fetch
    uses: shell
    pool: cpu
    cache: {enabled: true}
    with: {op: exec}
  - id: compile
    uses: shell
    pool: gpu
    pin: build
    needs: [fetch]
    run_as_orker_user: true
    with: {op: exec}
  - id: package
    uses: shell
    pool: gpu
    needs: [compile]
    cache: {enabled: true}
    with: {op: exec}
`
	fleet := fleetOf(currentOrker("cpu-1", "cpu", 0), currentOrker("gpu-1", "gpu", 1))
	resolver := &engine.PoolResolver{Probe: livePools{}, Fleet: fleet}
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"},
		compile(t, document), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if fleet.reads != 1 || len(fleet.asked) != 2 {
		t.Fatalf("the inventory was read %d times and %d Orkers asked; want one read asking each Orker once",
			fleet.reads, len(fleet.asked))
	}
}

func TestPoolResolver_PoolsCountsABusyPoolsOrkers(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	fleet := fleetOf(
		currentOrker("gpu-1", "gpu", 0),
		currentOrker("gpu-2", "gpu", 0),
		cordonedOrker("gpu-3", "gpu", 0),
		currentOrker("cpu-1", "cpu", 0),
	)
	// One gpu Orker has a free slot and polls; the other two are busy.
	resolver := &engine.PoolResolver{Probe: livePools{gpuQueue: 1}, Fleet: fleet}
	pools, err := resolver.Pools(context.Background(),
		&orkstackv1.Namespace{Name: "ml", AllowedPools: []string{"main", "gpu"}}, nil)
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, p := range pools {
		byName[p.GetName()] = p
	}
	if got := byName["gpu"]; got.GetLiveOrkerCount() != 2 || got.GetLastSeen() == nil {
		t.Fatalf("pool gpu = %v; want the 2 uncordoned Orkers, busy or not, and when they were seen", got)
	}
	if got := byName["main"].GetLiveOrkerCount(); got != 0 {
		t.Fatalf("pool main reports %d live Orkers; another pool's Orker is not one of its", got)
	}
	if fleet.reads != 1 {
		t.Fatalf("the inventory was read %d times for one report, want once", fleet.reads)
	}
}

func TestPoolResolver_PoolsKeepsTheChannelCountWhenTheInventoryCannotBeRead(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	var logged strings.Builder
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 3},
		Fleet:  &fakeFleet{observeErr: errors.New("frontend unavailable")},
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	}
	pools, err := resolver.Pools(context.Background(),
		&orkstackv1.Namespace{Name: "ml", AllowedPools: []string{"main", "gpu"}}, nil)
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	for _, p := range pools {
		if p.GetName() == "gpu" && p.GetLiveOrkerCount() != 3 {
			t.Fatalf("pool gpu = %v; want the channel's 3", p)
		}
		if p.GetUnavailableReason() != "" {
			t.Fatalf("pool %s is marked unavailable for an inventory gap: %v", p.GetName(), p)
		}
	}
	if got := strings.Count(logged.String(), "the orker inventory could not be read"); got != 1 {
		t.Fatalf("the inventory gap was logged %d times, want once:\n%s", got, logged.String())
	}
}

func TestReadFleet_KeepsAnOrkerThatDidNotAnswer(t *testing.T) {
	fleet := &fakeFleet{orkers: []*orkstackv1.DescribeOrkerResult{currentOrker("a", "gpu", 0)}, silent: []string{"b"}}
	orkers, err := engine.ReadFleet(context.Background(), fleet)
	if err != nil {
		t.Fatalf("ReadFleet: %v", err)
	}
	if len(orkers) != 2 || orkers[0].Described.GetPool() != "gpu" || orkers[1].Err == nil {
		t.Fatalf("ReadFleet = %+v; want a's answer and b's row with its error", orkers)
	}
	if _, err := engine.ReadFleet(context.Background(), &fakeFleet{observeErr: errors.New("down")}); err == nil {
		t.Fatal("a failed presence read was not an error")
	}
}

const sizedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: sized
tasks:
  - id: small
    uses: shell
    pool: gpu
    with: {command: "echo small"}
    resources: {cpu: "2"}
  - id: train
    uses: shell
    pool: gpu
    needs: [small]
    with: {command: "echo train"}
    resources: {cpu: "12", memory: "32Gi"}
`

func cappedOrker(id, pool string, cpu float64, memory int64) *orkstackv1.DescribeOrkerResult {
	orker := currentOrker(id, pool, 0)
	orker.MaxRequest = &orkstackv1.ResourceQuantity{Cpu: cpu, MemoryBytes: memory}
	return orker
}

// A task larger than every answering Orker's per-task cap could never run on
// its pool, so it is refused at submit, by name, before a run id exists.
func TestPoolResolver_RefusesATaskNoOrkerOfItsPoolCanHold(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe: livePools{gpuQueue: 2},
		Fleet: fleetOf(cappedOrker("gpu-1", "gpu", 8, 16<<30), cappedOrker("gpu-2", "gpu", 8, 64<<30)),
	}
	for _, allowEmpty := range []bool{false, true} {
		_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"}, compile(t, sizedDocument), allowEmpty)
		var refused *engine.PoolResourcesUnsatisfiableError
		if !errors.As(err, &refused) || refused.Task != "train" || refused.Pool != "gpu" {
			t.Fatalf("allow_empty_pool=%v: Plan = %v, want train refused on gpu", allowEmpty, err)
		}
		for _, want := range []string{"RESOURCES_UNSATISFIABLE", `task "train"`, "cpu 12", "memory 32Gi", "8 cpu"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the refusal %q does not say %q", err.Error(), want)
			}
		}
	}
	// One Orker of the pool that holds it is enough.
	resolver.Fleet = fleetOf(cappedOrker("gpu-1", "gpu", 8, 16<<30), cappedOrker("gpu-3", "gpu", 16, 64<<30))
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"}, compile(t, sizedDocument), false); err != nil {
		t.Fatalf("Plan = %v, want the request held by gpu-3", err)
	}
}

func TestPoolResolver_DoesNotJudgeSizesItCannotMeasure(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	var logged strings.Builder
	// gpu-1 predates max_request: nothing to judge by, so the submit stands.
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1},
		Fleet:  fleetOf(currentOrker("gpu-1", "gpu", 0)),
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	}
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"}, compile(t, sizedDocument), false); err != nil {
		t.Fatalf("Plan = %v, want a cap nobody reported left unjudged", err)
	}
	if !strings.Contains(logged.String(), "reports its per-task cap") {
		t.Fatalf("the unjudged request was not logged: %s", logged.String())
	}
	// A definition that declares no request reads no census for sizing.
	resolver.Fleet = refusingFleet{t: t}
	if _, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"}, compile(t, gpuOnlyDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
}

// A request fits only an Orker whose cap holds every dimension of it: one
// Orker with the cpu and another with the memory hold nothing between them.
func TestPoolResolver_ARequestMustFitOneOrkerInEveryDimension(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe: livePools{gpuQueue: 2},
		Fleet: fleetOf(cappedOrker("gpu-1", "gpu", 16, 16<<30), cappedOrker("gpu-2", "gpu", 8, 64<<30)),
	}
	_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ml"}, compile(t, sizedDocument), false)
	var refused *engine.PoolResourcesUnsatisfiableError
	if !errors.As(err, &refused) || refused.Task != "train" {
		t.Fatalf("Plan = %v; 12 cpu and 32Gi fit gpu-1's cpu and gpu-2's memory, never one Orker", err)
	}
}

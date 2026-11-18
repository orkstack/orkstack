//go:build integration && gpuhost

package orker

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.temporal.io/sdk/temporal"
)

// runningUnits is the ork.units label of each running container this Orker
// labelled: the units each running task holds.
func runningUnits(ctx context.Context, t *testing.T, orkerID string) []string {
	t.Helper()
	out := gpuHostDocker(ctx, t, "ps", "--filter", "label=ork.orker="+orkerID, "--filter", "status=running",
		"--filter", "label="+labelUnits, "--format", `{{.Label "`+labelUnits+`"}}`)
	labels := strings.Fields(out)
	slices.Sort(labels)
	return labels
}

// waitRunningUnits waits until the running containers hold exactly want.
func waitRunningUnits(ctx context.Context, t *testing.T, orkerID string, want ...string) {
	t.Helper()
	slices.Sort(want)
	for {
		got := runningUnits(ctx, t, orkerID)
		if slices.Equal(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the running unit containers hold %v, never %v", got, want)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type taskOutcome struct {
	result *orkstackv1.ExecuteTaskResult
	err    error
}

// TestIntegrationGPU_UnitsExclusive: two gpu: 1 tasks dispatched at once on
// an Orker of both units run side by side, each bound to its own unit: one
// sees the amdgpu render node and /dev/kfd, the other the i915 render node,
// and neither sees the other's. The progress control is the daemon showing
// both unit containers running at the same time.
func TestIntegrationGPU_UnitsExclusive(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-exclusive"})

	ins := []*orkstackv1.ExecuteTaskInput{g.task(t, "first", 1, renderNodesTask(4)...), g.task(t, "second", 1, renderNodesTask(4)...)}
	outcomes := make([]taskOutcome, len(ins))
	var wg sync.WaitGroup
	for i, in := range ins {
		a, done := g.poll(t, in)
		wg.Go(func() {
			defer done()
			outcomes[i].result, outcomes[i].err = g.run(ctx, t, a, in)
		})
	}
	waitRunningUnits(ctx, t, g.orkerID, unitsLabel(gpuHostProfile, []int{0}), unitsLabel(gpuHostProfile, []int{1}))
	wg.Wait()

	seen := map[string]bool{}
	for i, o := range outcomes {
		v := gpuValues(t, o.result, o.err)
		nodes := strings.Fields(v["nodes"].(string))
		if len(nodes) != 1 {
			t.Fatalf("task %d saw render nodes %v; a gpu: 1 task sees its own unit's alone", i, nodes)
		}
		key := nodes[0]
		switch {
		case key == "renderD128" && v["kfd"] == true:
		case key == "renderD129" && v["kfd"] == false:
		default:
			t.Fatalf("task %d saw %v with /dev/kfd %v; want unit amd (renderD128 and /dev/kfd) or unit intel (renderD129 alone)", i, nodes, v["kfd"])
		}
		if seen[key] {
			t.Fatalf("both tasks were given %s: two tasks held one unit", key)
		}
		seen[key] = true
	}
	wantGPUReserved(t, g.c, "both tasks ended", 0)
}

// TestIntegrationGPU_TwoUnitTask: with max_gpu 2, a gpu: 2 task is given
// both units, and while it holds them the Orker grants no poll, so a
// concurrent gpu: 1 task is not polled; once its container is removed a
// poll is granted and binds unit 0. The progress control is the gpu: 2
// container observed running while each refused grant was asked.
func TestIntegrationGPU_TwoUnitTask(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-two", maxGPU: 2})

	in := g.task(t, "both", 2, renderNodesTask(5)...)
	a, done := g.poll(t, in)
	finished := make(chan taskOutcome, 1)
	go func() {
		var o taskOutcome
		o.result, o.err = g.run(ctx, t, a, in)
		done()
		finished <- o
	}()
	both := unitsLabel(gpuHostProfile, []int{0, 1})
	waitRunningUnits(ctx, t, g.orkerID, both)

	refused := 0
	var outcome taskOutcome
wait:
	for {
		select {
		case outcome = <-finished:
			break wait
		case <-ctx.Done():
			t.Fatal("the gpu: 2 task never ended")
		case <-time.After(200 * time.Millisecond):
		}
		running := slices.Contains(runningUnits(ctx, t, g.orkerID), both)
		if permit := g.c.tryReserve(); permit != nil {
			g.c.release(permit)
			if running {
				t.Fatal("a poll was granted while a gpu: 2 task held both units")
			}
			continue
		}
		if running {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("no grant was asked while the gpu: 2 container ran, so nothing shows a poll was held back")
	}
	v := gpuValues(t, outcome.result, outcome.err)
	if got := strings.Fields(v["nodes"].(string)); !slices.Equal(got, []string{"renderD128", "renderD129"}) || v["kfd"] != true {
		t.Fatalf("the gpu: 2 task saw %v with /dev/kfd %v; want both units' nodes", got, v["kfd"])
	}

	next, nextDone := g.poll(t, g.task(t, "after", 1))
	defer nextDone()
	if err := g.c.bind(next); err != nil {
		t.Fatalf("bind after the gpu: 2 task: %v", err)
	}
	if got := next.Units(); !slices.Equal(got, []int{0}) {
		t.Fatalf("the next gpu: 1 task was bound %v; want unit 0, freed with the gpu: 2 container", got)
	}
}

// createFailing is the real runtime with every Create made to fail on the
// daemon: the spec is sent with an image reference the daemon refuses. It
// records each spec's CDI devices, so the test sees the units were bound.
type createFailing struct {
	*DockerRuntime
	mu  sync.Mutex
	cdi [][]string
}

func (r *createFailing) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.cdi = append(r.cdi, slices.Clone(spec.CDIDevices))
	r.mu.Unlock()
	spec.Image = "not a valid reference!"
	return r.DockerRuntime.Create(ctx, spec)
}

// TestIntegrationGPU_CreateFailureFreesUnits: a gpu task whose create the
// real daemon refuses fails, and the unit it was bound is free again: the
// Orker's one unit binds the next task. The spec's image is made one the
// daemon refuses, a fault it does not produce on demand; the bind and the
// create are the real ones.
func TestIntegrationGPU_CreateFailureFreesUnits(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var rt *createFailing
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-createfail", units: []string{"amd"},
		runtime: func(d *DockerRuntime) ContainerRuntime { rt = &createFailing{DockerRuntime: d}; return rt }})

	in := g.task(t, "refused", 1, "/bin/true")
	a, done := g.poll(t, in)
	_, err := g.run(ctx, t, a, in)
	done()
	if err == nil {
		t.Fatal("the task succeeded; its create was to be refused")
	}
	rt.mu.Lock()
	created := slices.Clone(rt.cdi)
	rt.mu.Unlock()
	if len(created) != 1 || !slices.Equal(created[0], []string{spec.kind + "=amd"}) {
		t.Fatalf("the creates asked for CDI devices %v; want one create bound to %s=amd", created, spec.kind)
	}
	if ids := labelled(ctx, t, "ork.orker="+g.orkerID); len(ids) != 0 {
		t.Fatalf("a refused create left containers %v", ids)
	}
	wantGPUReserved(t, g.c, "after the refused create", 0)

	next, nextDone := g.poll(t, g.task(t, "next", 1))
	defer nextDone()
	if err := g.c.bind(next); err != nil {
		t.Fatalf("the Orker's one unit did not bind after the refused create: %v", err)
	}
	if got := next.Units(); !slices.Equal(got, []int{0}) {
		t.Fatalf("the next task was bound %v; want unit 0", got)
	}
}

// startCounting is the real runtime with a hook run right after the real
// create, and Start counted.
type startCounting struct {
	*DockerRuntime
	created func(ContainerID)
	starts  atomic.Int32
}

func (r *startCounting) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	id, err := r.DockerRuntime.Create(ctx, spec)
	if err == nil {
		r.created(id)
	}
	return id, err
}

func (r *startCounting) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	r.starts.Add(1)
	return r.DockerRuntime.Start(ctx, id, stdout, stderr)
}

// TestIntegrationGPU_DriftAtStart (design Q18): the runtime resolves a CDI
// device when it starts a container, so a unit node that changes between
// the create and the start is drift there. The task fails PROFILE_DRIFTED,
// never started: Start is never called, the created container is removed,
// its unit is freed, and the profile stays drifted. Changing a real render
// node needs root, so the node's minor is made to read differently through
// the dispatcher's device inspector from the moment the real create
// returns; the create, the recheck and the removal are the real ones.
func TestIntegrationGPU_DriftAtStart(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var drifted atomic.Bool
	var createdID atomic.Value
	var rt *startCounting
	inspect := func(p string) (hostDevice, error) {
		d, err := inspectHostDevice(p)
		if err == nil && drifted.Load() {
			d.Minor++
		}
		return d, err
	}
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-drift", units: []string{"amd"}, inspect: inspect,
		runtime: func(d *DockerRuntime) ContainerRuntime {
			rt = &startCounting{DockerRuntime: d, created: func(id ContainerID) {
				createdID.Store(id)
				drifted.Store(true)
			}}
			return rt
		}})

	// The engine lists PROFILE_DRIFTED as an interruption, so the attempt
	// reports it typed, with whether it started (E91 §4.4).
	g.d.listed = func(context.Context) []string { return []string{InterruptionType(ReasonProfileDrifted)} }
	in := g.task(t, "drifts", 1, "/bin/true")
	a, done := g.poll(t, in)
	_, err := g.run(ctx, t, a, in)
	done()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != InterruptionType(ReasonProfileDrifted) {
		t.Fatalf("err = %v; want the typed %s interruption", err, ReasonProfileDrifted)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if err := app.Details(&detail); err != nil || !detail.GetNeverStarted() || detail.GetStarted() {
		t.Fatalf("detail = %v, %v; a task that drifted before its start is reported never started", detail, err)
	}
	id, _ := createdID.Load().(ContainerID)
	if id == "" {
		t.Fatal("no container was created, so the drift was not found between the create and the start")
	}
	if n := rt.starts.Load(); n != 0 {
		t.Fatalf("Start was called %d times; a drift found before the start starts nothing", n)
	}
	if state := containerState(ctx, t, string(id)); state != "" {
		t.Fatalf("the created container is %s; the attempt removes what it created", state)
	}
	if !g.s.drifted(gpuHostProfile) {
		t.Fatal("the profile is not drifted after a drift at start")
	}
	wantGPUReserved(t, g.c, "after the drift", 0)
}

//go:build linux

package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// unitListRuntime is a runtime that lists unit containers and removes them,
// recording what it removed.
type unitListRuntime struct {
	unitProbeRuntime
	listed  []UnitContainer
	removed []ContainerID

	mu        sync.Mutex
	removeErr error
	asked     chan ContainerID // each removal asked for, when not nil
}

// setRemoveErr is what each later Remove answers, and makes it announced.
func (r *unitListRuntime) setRemoveErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeErr = err
	if r.asked == nil {
		r.asked = make(chan ContainerID, 64)
	}
}

// awaitRemove waits until a removal of id is asked for.
func (r *unitListRuntime) awaitRemove(ctx context.Context, t *testing.T, id ContainerID) {
	t.Helper()
	select {
	case got := <-r.asked:
		if got != id {
			t.Fatalf("removed %s, want %s", got, id)
		}
	case <-ctx.Done():
		t.Fatalf("no removal of %s was asked for", id)
	}
}

func (r *unitListRuntime) UnitContainers(context.Context) ([]UnitContainer, error) {
	return r.listed, nil
}
func (r *unitListRuntime) Remove(_ context.Context, id ContainerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.asked != nil {
		select {
		case r.asked <- id:
		default:
		}
	}
	if r.removeErr != nil {
		return r.removeErr
	}
	r.removed = append(r.removed, id)
	return nil
}

// gpuActivities is an Activities offering the gpu profile "amd" (one unit,
// audited as amdNodes) over a lock directory of its own, and that directory.
func gpuActivities(t *testing.T, rt *unitListRuntime) (*Activities, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "gpu-orker")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	old := gpuLockDirPath
	gpuLockDirPath = dir
	t.Cleanup(func() { gpuLockDirPath = old })
	declared := Profiles{ProbeImage: viewProbeImage, List: []Profile{{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: "amd.com/gpu", Units: []string{"0"}}}}}
	s := newProfileState(declared, "c0ffee")
	rt.label = "c0ffee"
	s.setAudited(map[string]profileAudit{"amd": {identity: "x", gpu: &gpuSpecAudit{}, gpuNodes: amdNodes, unitNodes: [][]auditedDevice{amdNodes}}})
	c := newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 2)
	c.useUnits("amd", 1, 1, unitHealthReporter(s, "amd", time.Now))
	a := &Activities{dispatcher: &OCIDispatcher{Runtime: rt, Policy: DefaultSecurityPolicy(), profiles: s}, profiles: s, capacity: c}
	return a, dir
}

// TestBootGPU_TakesTheLockReapsAndOffers: with the lock free, boot takes it,
// removes every container bound under it, and offers the units to dispatch.
func TestBootGPU_TakesTheLockReapsAndOffers(t *testing.T) {
	rt := &unitListRuntime{}
	a, dir := gpuActivities(t, rt)
	probe, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	identity := probe.identity()
	probe.close()
	rt.listed = []UnitContainer{{Lock: identity, Units: "amd:0", OwnContainer: OwnContainer{ID: "left"}}}
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatalf("bootGPU = %v", err)
	}
	if len(rt.removed) != 1 || rt.removed[0] != "left" {
		t.Fatalf("removed %v, want [left]", rt.removed)
	}
	if a.dispatcher.gpu.Load() == nil || a.profiles.unavailable("amd") {
		t.Fatal("the units are not offered after the lock was taken")
	}
	t.Cleanup(func() { a.dispatcher.gpu.Load().lock.(*gpuHostLock).close() })
}

// TestBootGPU_HeldLockWaitsThenOffers: a lock another holds keeps the profile
// unavailable (GPU_HOST_LOCK_HELD), naming the holder, until it is released;
// then the reap runs and the units are offered.
func TestBootGPU_HeldLockWaitsThenOffers(t *testing.T) {
	rt := &unitListRuntime{}
	a, dir := gpuActivities(t, rt)
	other, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := other.tryLock(); !ok {
		t.Fatal("the other holder did not take the lock")
	}
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatalf("bootGPU = %v", err)
	}
	held := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD
	if !a.profiles.unavailableFor("amd", held) || a.dispatcher.gpu.Load() != nil {
		t.Fatal("a held lock did not hold the profile unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.awaitGPULock(ctx, 5*time.Millisecond); close(done) }()
	other.close()
	<-done
	if a.profiles.unavailableFor("amd", held) || a.dispatcher.gpu.Load() == nil {
		t.Fatal("releasing the lock did not bring the units into service")
	}
	a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
}

// TestProbeUnitsOnce_FlipsHealthUnderTheLedger: one pass probes every free
// unit; a unit whose probe fails leaves the usable set.
func TestProbeUnitsOnce_FlipsHealthUnderTheLedger(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.capacity.limit.gpu != 1 {
		t.Fatalf("a healthy unit left the usable set: limit %d", a.capacity.limit.gpu)
	}
	rt.view = nil
	rt.runErr = os.ErrDeadlineExceeded
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.capacity.limit.gpu != 0 || !a.profiles.unavailableFor("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY) {
		t.Fatalf("a failing unit stayed usable: limit %d", a.capacity.limit.gpu)
	}
}

// silentEngine makes rt's probes the ones of an engine that answers nothing.
func silentEngine(rt *unitListRuntime) {
	rt.view, rt.runErr = nil, fmt.Errorf("%w: start the probe container", ErrProbeTimedOut)
}

// TestProbeUnitsOnce_AStuckEngineLeavesAHealthyUnitInService: a pass whose
// probe the engine never started leaves the unit usable and free, so a poll
// is granted and no unit reason is held; a hung driver, which a started probe
// reports, takes the unit out at once.
func TestProbeUnitsOnce_AStuckEngineLeavesAHealthyUnitInService(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	silentEngine(rt)
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if !a.capacity.unitUsable(0) || a.profiles.unavailable("amd") {
		t.Fatal("an engine that started no probe took a healthy unit out of service")
	}
	permit := a.capacity.tryReserve()
	if permit == nil {
		t.Fatal("no poll is granted after a probe the engine never started")
	}
	a.capacity.release(permit)

	hung := append(answering(amdNodes), heldLockEntry(a))
	hung[0] = viewEntry{Path: hung[0].Path, Error: "the driver did not answer within 5s"}
	rt.view, rt.runErr = hung, nil
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.capacity.unitUsable(0) {
		t.Fatal("a unit whose driver hangs stayed in service")
	}
}

// TestProbeUnitsOnce_AnImageThatIsGoneIsTheImages: a probe image the engine
// says it has not got makes the profile unavailable at once
// (PROBE_IMAGE_UNAVAILABLE), naming it, and leaves the unit as it was.
func TestProbeUnitsOnce_AnImageThatIsGoneIsTheImages(t *testing.T) {
	const reason = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.runErr = fmt.Errorf("%w: not found", ErrProbeImageUnavailable)
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if !a.profiles.unavailableFor("amd", reason) || !a.capacity.unitUsable(0) {
		t.Fatalf("held %v, unit usable %v; want held at once, and the unit as it was",
			a.profiles.unavailableFor("amd", reason), a.capacity.unitUsable(0))
	}
	rt.view, rt.runErr = append(answering(amdNodes), heldLockEntry(a)), nil
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.profiles.unavailable("amd") {
		t.Fatal("a pass whose image is there again did not clear it")
	}
}

// TestProbeUnits_AnEngineThatStartsNoProbe: three passes in a row whose
// probes the engine never started make the profile unavailable
// (PROBE_NOT_STARTED), naming the engine, with its units still usable; the
// recovery pass then probes them, and the first probe that starts ends it,
// whatever that probe finds of its unit.
func TestProbeUnits_AnEngineThatStartsNoProbe(t *testing.T) {
	const reason = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_NOT_STARTED
	for name, healthy := range map[string]bool{"a healthy unit": true, "an unhealthy unit": false} {
		t.Run(name, func(t *testing.T) {
			rt := &unitListRuntime{}
			a, _ := gpuActivities(t, rt)
			if err := a.bootGPU(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
			view := append(answering(amdNodes), heldLockEntry(a))
			pass := func(recovering bool) { a.probeUnits(context.Background(), t.TempDir(), recovering) }

			silentEngine(rt)
			pass(false)
			pass(false)
			rt.view, rt.runErr = view, nil // one that starts ends the run of passes
			pass(false)
			silentEngine(rt)
			pass(false)
			pass(false)
			if a.profiles.unavailable("amd") {
				t.Fatal("two passes in a row held the profile unavailable")
			}
			pass(false)
			if !a.profiles.unavailableFor("amd", reason) || !a.capacity.unitUsable(0) {
				t.Fatalf("after three passes: held %v, unit usable %v; want held and still usable",
					a.profiles.unavailableFor("amd", reason), a.capacity.unitUsable(0))
			}
			if !strings.Contains(a.profiles.causes["amd"].unavailable[reason].GetDetail(), "container engine") {
				t.Fatalf("detail %q does not name the engine", a.profiles.causes["amd"].unavailable[reason].GetDetail())
			}

			rt.view, rt.runErr = view, nil
			if !healthy {
				hung := slices.Clone(view)
				hung[0] = viewEntry{Path: hung[0].Path, Error: "the driver did not answer within 5s"}
				rt.view = hung
			}
			pass(true) // recovery: every unit is usable, and it probes all the same
			if a.profiles.unavailableFor("amd", reason) {
				t.Fatal("a probe that started did not end it")
			}
			if a.capacity.unitUsable(0) != healthy {
				t.Fatalf("unit usable %v, want %v", a.capacity.unitUsable(0), healthy)
			}
		})
	}
}

// TestProbeUnits_AnImageCheckTheEngineDoesNotAnswer: an image check that
// gets no answer counts towards the same three passes and says nothing of the
// image; an image the engine or registry says is not there holds the profile
// at once.
func TestProbeUnits_AnImageCheckTheEngineDoesNotAnswer(t *testing.T) {
	const (
		notStarted = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_NOT_STARTED
		image      = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
	)
	for name, asked := range map[string]func(*unitListRuntime, error){
		"the pull":  func(rt *unitListRuntime, err error) { rt.pullErr = err },
		"the label": func(rt *unitListRuntime, err error) { rt.labelErr = err },
	} {
		t.Run(name, func(t *testing.T) {
			rt := &unitListRuntime{}
			a, _ := gpuActivities(t, rt)
			if err := a.bootGPU(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
			rt.view = append(answering(amdNodes), heldLockEntry(a))
			pass := func() { a.probeUnits(context.Background(), t.TempDir(), false) }
			asked(rt, fmt.Errorf("%w: docker exited 1", errEngineUnreachable))
			pass()
			pass()
			if a.profiles.unavailable("amd") {
				t.Fatal("two unanswered image checks held the profile")
			}
			pass()
			if !a.profiles.unavailableFor("amd", notStarted) || a.profiles.unavailableFor("amd", image) || !a.capacity.unitUsable(0) {
				t.Fatal("three unanswered image checks are not the engine's reason alone")
			}
			asked(rt, errors.New("manifest unknown"))
			pass()
			if !a.profiles.unavailableFor("amd", image) {
				t.Fatal("an image that is not there did not hold the profile at once")
			}
			asked(rt, nil)
			pass()
			if a.profiles.unavailable("amd") {
				t.Fatal("a pass whose image and probe both answer did not clear the profile")
			}
		})
	}
}

// TestProbeUnitsOnce_AnUnhealthyUnitsLeftoverIsHeld: a unit its probe found
// unhealthy leaves service at once, and is not probed again while the probe's
// container could not be removed: a second one would share the unit with it.
// The hold is the unit's, and says nothing of the engine.
func TestProbeUnitsOnce_AnUnhealthyUnitsLeftoverIsHeld(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.profiles.declared.RecoveryInterval = time.Millisecond
	rt.said, rt.runErr = unitProbeStarted, fmt.Errorf("%w: docker exited 137", ErrPostureRefused)
	rt.leftovers = []ProbeLeftover{{Container: "stuck", Err: errors.New("device busy")}}
	rt.setRemoveErr(errors.New("device busy"))
	a.probeUnitsOnce(ctx, t.TempDir())
	if a.capacity.unitUsable(0) {
		t.Fatal("a unit whose probe started and said nothing stayed in service")
	}
	ran := len(rt.specs)
	for range notStartedPasses + 1 {
		rt.awaitRemove(ctx, t, "stuck") // the control that its removal is being asked for
		a.probeUnits(ctx, t.TempDir(), true)
	}
	if len(rt.specs) != ran {
		t.Fatalf("%d more probes ran on a unit whose probe's container is not gone", len(rt.specs)-ran)
	}
	if a.profiles.unavailableFor("amd", probeNotStarted) {
		t.Fatal("a unit's stuck container was counted against the engine")
	}
	rt.setRemoveErr(nil)
	rt.view, rt.runErr, rt.leftovers = append(answering(amdNodes), heldLockEntry(a)), nil, nil
	for len(rt.specs) == ran {
		a.probeUnits(ctx, t.TempDir(), true)
		if ctx.Err() != nil {
			t.Fatal("the unit was not probed again once the container was gone")
		}
	}
}

// TestProbeUnits_AStopDuringTheImageCheckStartsNoProbe: the Orker's stop is
// asked again after the image check, which can take minutes.
func TestProbeUnits_AStopDuringTheImageCheckStartsNoProbe(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	a.cordon = NewCordon()
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.onPull = func() { a.cordon.SetStopping(time.Now(), time.Now()) }
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if len(rt.specs) != 0 || !a.capacity.unitUsable(0) {
		t.Fatalf("a stopping Orker ran %d probes; unit usable %v", len(rt.specs), a.capacity.unitUsable(0))
	}
}

// TestProbeUnits_OnePassAtATime: one pass runs at a time. A recovery pass
// that finds one running does nothing, so one silence is never counted twice;
// the audit's waits for it and then runs, so that recovery passes one after
// the other, each slow, never keep the units in service from being probed.
func TestProbeUnits_OnePassAtATime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, rt := twoUnitActivities(t)
	unit0, unit1 := rt.units["amd.com/gpu=0"], rt.units["amd.com/gpu=1"]
	unit1.runErr = fmt.Errorf("%w: docker exited 125: CDI device injection failed", ErrPostureRefused)
	a.probeUnitsOnce(ctx, t.TempDir())
	if a.capacity.unitUsable(1) {
		t.Fatal("unit 1 is not out of service") // what the recovery passes probe
	}
	ran0, ran1 := len(unit0.specs), len(unit1.specs)

	running, let := make(chan struct{}), make(chan struct{})
	rt.onPull = func() {
		rt.onPull = nil
		close(running)
		<-let
	}
	first := make(chan struct{})
	go func() { defer close(first); a.probeUnits(ctx, t.TempDir(), true) }()
	<-running
	a.probeUnits(ctx, t.TempDir(), true) // beside the first: nothing
	if len(unit1.specs) != ran1 {
		t.Fatal("a recovery pass ran beside another")
	}
	audit := make(chan struct{})
	go func() { defer close(audit); a.probeUnitsOnce(ctx, t.TempDir()) }()
	close(let)
	for _, done := range []chan struct{}{first, audit} {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("a pass never ended")
		}
	}
	if len(unit0.specs) != ran0+1 {
		t.Fatalf("the audit's pass probed the unit in service %d times beside a running recovery pass, want once", len(unit0.specs)-ran0)
	}
}

// TestProbeUnits_ADefiniteImageAnswerEndsTheCount: the engine's answer that
// the image is not there is an answer, so the silences around it are not in a
// row.
func TestProbeUnits_ADefiniteImageAnswerEndsTheCount(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	for _, absent := range []bool{false, false, true, false, false} {
		silentEngine(rt)
		rt.pullErr = nil
		if absent {
			rt.pullErr = errors.New("manifest unknown")
		}
		a.probeUnitsOnce(context.Background(), t.TempDir())
	}
	if a.profiles.unavailableFor("amd", probeNotStarted) {
		t.Fatal("two silences, an answer and two silences were counted as three in a row")
	}
}

// twoUnitRuntime is a runtime whose probes of each unit are its own.
type twoUnitRuntime struct {
	*unitListRuntime
	units map[string]*unitProbeRuntime // by CDI device
}

func (r *twoUnitRuntime) RunBootProbe(ctx context.Context, spec ContainerSpec) (BootProbeRun, error) {
	return r.units[spec.CDIDevices[0]].RunBootProbe(ctx, spec)
}

// unitsActivities is gpuActivities over rt, offering units of amd.
func unitsActivities(t *testing.T, rt *twoUnitRuntime, units ...string) *Activities {
	t.Helper()
	a, _ := gpuActivities(t, rt.unitListRuntime)
	a.dispatcher.Runtime = rt
	a.profiles.declared.List[0].GPU.Units = units
	nodes := make([][]auditedDevice, len(units))
	for i := range nodes {
		nodes[i] = amdNodes
	}
	a.profiles.setAudited(map[string]profileAudit{"amd": {identity: "x", gpu: &gpuSpecAudit{}, gpuNodes: amdNodes, unitNodes: nodes}})
	a.capacity = newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 4)
	a.capacity.useUnits("amd", len(units), 1, unitHealthReporter(a.profiles, "amd", time.Now))
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.dispatcher.gpu.Load().lock.(*gpuHostLock).close() })
	return a
}

// twoUnitActivities offers amd's units "0" and "1", unit 1's probes silent.
func twoUnitActivities(t *testing.T) (*Activities, *twoUnitRuntime) {
	t.Helper()
	rt := &twoUnitRuntime{unitListRuntime: &unitListRuntime{}, units: map[string]*unitProbeRuntime{"amd.com/gpu=0": {}, "amd.com/gpu=1": {}}}
	a := unitsActivities(t, rt, "0", "1")
	rt.units["amd.com/gpu=0"].view = append(answering(amdNodes), heldLockEntry(a))
	rt.units["amd.com/gpu=1"].runErr = fmt.Errorf("%w: start the probe container", ErrProbeTimedOut)
	return a, rt
}

// TestProbeUnits_AUnitOutOfServiceIsNotTheEngines: the recovery passes probe
// only the units out of service, so one of those whose probe does not start
// says nothing more of the engine: the profile, and its other unit, stay up.
func TestProbeUnits_AUnitOutOfServiceIsNotTheEngines(t *testing.T) {
	a, rt := twoUnitActivities(t)
	unit1 := rt.units["amd.com/gpu=1"]
	silent := unit1.runErr
	unit1.runErr = fmt.Errorf("%w: docker exited 125: CDI device injection failed", ErrPostureRefused)
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.capacity.unitUsable(1) || !a.capacity.unitUsable(0) {
		t.Fatal("the refused unit is not the one out of service")
	}
	unit1.runErr = silent
	ran := len(unit1.specs)
	for range notStartedPasses + 1 {
		a.probeUnits(context.Background(), t.TempDir(), true)
		if a.profiles.unavailableFor("amd", probeNotStarted) {
			t.Fatal("a unit already out of service was charged to the engine, which holds the healthy one too")
		}
	}
	if len(unit1.specs) != ran+notStartedPasses+1 {
		t.Fatalf("recovery probed the unit %d times, want %d", len(unit1.specs)-ran, notStartedPasses+1) // the control
	}
}

// blockingProbeRuntime is a runtime whose unit probe blocks until let go.
type blockingProbeRuntime struct {
	*unitListRuntime
	running, let chan struct{}
}

func (r *blockingProbeRuntime) RunBootProbe(ctx context.Context, spec ContainerSpec) (BootProbeRun, error) {
	close(r.running)
	<-r.let
	return r.unitListRuntime.RunBootProbe(ctx, spec)
}

// TestProbeUnits_ASlowPassHoldsBackNoAnswer: while a pass is stuck on the
// engine, in its image check or in a probe, the Orker still answers a cordon,
// an uncordon and its description, a poll that fits is still granted, and
// the units' health is still read: a pass holds none of their locks across a
// call to the engine. A pass is no attempt, so no heartbeat waits on it.
func TestProbeUnits_ASlowPassHoldsBackNoAnswer(t *testing.T) {
	for name, inProbe := range map[string]bool{"the image check": false, "a probe": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			rt := &unitListRuntime{}
			a := newCordonActivities(t, t.TempDir())
			gpu, _ := gpuActivities(t, rt)
			a.dispatcher.Runtime, a.dispatcher.profiles = rt, gpu.profiles
			a.profiles = gpu.profiles
			a.capacity = newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 4)
			a.capacity.useUnits("amd", 2, 1, unitHealthReporter(a.profiles, "amd", time.Now))
			if err := a.bootGPU(ctx); err != nil {
				t.Fatal(err)
			}
			defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
			rt.view = append(answering(amdNodes), heldLockEntry(a))

			running, let := make(chan struct{}), make(chan struct{})
			if inProbe {
				a.dispatcher.Runtime = &blockingProbeRuntime{unitListRuntime: rt, running: running, let: let}
			} else {
				rt.onPull = func() { close(running); <-let }
			}
			pass := make(chan struct{})
			go func() { defer close(pass); a.probeUnitsOnce(ctx, t.TempDir()) }()
			<-running // the control: the pass is inside its engine call

			answered := make(chan error, 1)
			go func() {
				answered <- func() error {
					out, err := a.CordonOrker(ctx, &orkstackv1.CordonOrkerInput{Cordoned: true, Reason: "driver roll"})
					if err != nil || !out.GetCordoned() {
						return fmt.Errorf("cordon = %v, %w", out.GetCordoned(), err)
					}
					if out, err = a.CordonOrker(ctx, &orkstackv1.CordonOrkerInput{}); err != nil || out.GetCordoned() {
						return fmt.Errorf("uncordon = %v, %w", out.GetCordoned(), err)
					}
					if _, err := a.DescribeOrker(ctx); err != nil {
						return fmt.Errorf("describe: %w", err)
					}
					if !a.capacity.unitUsable(0) || a.profiles.unavailable("amd") {
						return errors.New("the unit's health reads otherwise during a pass")
					}
					permit := a.capacity.tryReserve() // two units, a cap of one: one fits beside the probe
					if permit == nil {
						return errors.New("no poll is granted beside a running pass")
					}
					a.capacity.release(permit)
					return nil
				}()
			}()
			select {
			case err := <-answered:
				if err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
				t.Error("a running pass held back the Orker's answers")
			}
			select {
			case <-pass:
				t.Error("the pass ended before it was let go, so nothing was shown")
			default:
			}
			close(let)
			<-pass
		})
	}
}

// TestProbeUnits_AHealthyUnitsLeftoverIsSeen: a unit its probe found healthy
// and whose container cannot be removed is held with no task able to have it;
// the third pass that finds it so says why on the profile, on a host of one
// unit too.
func TestProbeUnits_AHealthyUnitsLeftoverIsSeen(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.profiles.declared.RecoveryInterval = time.Hour // no removal is tried in this test
	var report strings.Builder
	if err := writeProbeView(&report, append(answering(amdNodes), heldLockEntry(a))); err != nil {
		t.Fatal(err)
	}
	rt.said, rt.runErr = unitProbeStarted+report.String(), fmt.Errorf("%w: no end", ErrProbeTimedOut)
	rt.leftovers = []ProbeLeftover{{Container: "stuck", Err: errors.New("device busy")}}
	a.probeUnitsOnce(ctx, t.TempDir())
	if !a.capacity.unitUsable(0) || a.capacity.tryReserve() != nil {
		t.Fatal("a healthy unit whose probe's container is not gone is not held in service")
	}
	for range notStartedPasses {
		a.probeUnitsOnce(ctx, t.TempDir())
	}
	if !a.profiles.unavailableFor("amd", probeNotStarted) ||
		!strings.Contains(a.profiles.causes["amd"].unavailable[probeNotStarted].GetDetail(), "stuck") {
		t.Fatal("a unit no task can have shows nowhere on its profile")
	}
}

// TestProbeUnits_AUnitThatAloneNeverStarts: while another unit's probe starts
// in the same passes the engine answers, so a unit whose own probe does not,
// as when its driver hangs while the runtime sets the device up, is the one at
// fault: the third such probe in a row takes it out, and it returns like any.
// A pass in which its probe does start, or in which no unit's does, begins the
// count again, and counts against no unit.
func TestProbeUnits_AUnitThatAloneNeverStarts(t *testing.T) {
	a, rt := twoUnitActivities(t)
	pass := func() { a.probeUnits(context.Background(), t.TempDir(), false) }
	unit0, unit1 := rt.units["amd.com/gpu=0"], rt.units["amd.com/gpu=1"]
	silent, seen := unit1.runErr, unit0.view

	unit0.view, unit0.runErr = nil, silent // neither starts: the engine's, and no unit's
	for range notStartedPasses + 1 {
		pass()
	}
	if !a.profiles.unavailableFor("amd", probeNotStarted) || !a.capacity.unitUsable(0) || !a.capacity.unitUsable(1) {
		t.Fatal("an engine that starts no probe was charged to a unit")
	}
	unit0.view, unit0.runErr = seen, nil
	pass()
	pass()
	unit1.view, unit1.runErr = seen, nil // it starts once: the count begins again
	pass()
	unit1.view, unit1.runErr = nil, silent
	pass()
	pass()
	unit0.view, unit0.runErr = nil, silent // the engine falls silent: not the unit's third
	pass()
	if !a.capacity.unitUsable(1) || !a.capacity.unitUsable(0) {
		t.Fatal("a pass in which no probe started took a unit out")
	}
	unit0.view, unit0.runErr = seen, nil // and it began the count again
	pass()
	pass()
	if !a.capacity.unitUsable(1) {
		t.Fatal("two in a row took the unit out")
	}
	pass()
	if a.capacity.unitUsable(1) || !a.capacity.unitUsable(0) || a.profiles.unavailableFor("amd", probeNotStarted) {
		t.Fatalf("usable %v and %v, engine held %v; want only the silent unit out",
			a.capacity.unitUsable(0), a.capacity.unitUsable(1), a.profiles.unavailableFor("amd", probeNotStarted))
	}
	ran := len(unit1.specs)
	for range notStartedPasses + 1 { // recovery probes only the unit that is out
		a.probeUnits(context.Background(), t.TempDir(), true)
		if a.profiles.unavailableFor("amd", probeNotStarted) {
			t.Fatal("a unit already out of service was charged to the engine, which holds the healthy one too")
		}
	}
	if len(unit1.specs) == ran {
		t.Fatal("no recovery pass probed the unit that is out") // the control
	}
	unit1.view, unit1.runErr = seen, nil
	for range recoveryPasses {
		pass()
	}
	if !a.capacity.unitUsable(1) {
		t.Fatal("the unit did not return after its good probes")
	}
}

// TestProbeUnits_RecoveryPassesLeaveAUnitsCountAlone: a pass that did not
// probe a unit says nothing of it, so the recovery passes of another unit,
// between every two of the audit's, neither begin a silent unit's count again
// nor add to it; a unit already out is not taken out again.
func TestProbeUnits_RecoveryPassesLeaveAUnitsCountAlone(t *testing.T) {
	rt := &twoUnitRuntime{unitListRuntime: &unitListRuntime{}, units: map[string]*unitProbeRuntime{
		"amd.com/gpu=0": {}, "amd.com/gpu=1": {}, "amd.com/gpu=2": {}}}
	a := unitsActivities(t, rt, "0", "1", "2")
	var log bytes.Buffer
	a.dispatcher.Logger = slog.New(slog.NewTextHandler(&log, nil))
	seen := append(answering(amdNodes), heldLockEntry(a))
	silent := fmt.Errorf("%w: start the probe container", ErrProbeTimedOut)
	rt.units["amd.com/gpu=0"].view = seen
	rt.units["amd.com/gpu=1"].runErr = fmt.Errorf("%w: docker exited 125: CDI device injection failed", ErrPostureRefused)
	rt.units["amd.com/gpu=2"].view = seen
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.capacity.unitUsable(1) || !a.capacity.unitUsable(2) {
		t.Fatal("unit 1 alone should be out of service")
	}
	rt.units["amd.com/gpu=2"].view, rt.units["amd.com/gpu=2"].runErr = nil, silent
	recovery := func(err error, passes int) {
		rt.units["amd.com/gpu=1"].runErr = err
		for range passes {
			a.probeUnits(context.Background(), t.TempDir(), true)
		}
	}
	answers := rt.units["amd.com/gpu=1"].runErr
	for i := range notStartedPasses {
		recovery(answers, 2) // unit 1's probe answers: a start, beside no probe of unit 2
		recovery(silent, 2)  // and one that does not
		if !a.capacity.unitUsable(2) {
			t.Fatalf("recovery passes that did not probe the unit took it out, after %d of its own", i)
		}
		rt.units["amd.com/gpu=1"].runErr = answers
		a.probeUnitsOnce(context.Background(), t.TempDir())
	}
	if a.capacity.unitUsable(2) {
		t.Fatal("recovery passes of another unit began the silent unit's count again")
	}
	rt.units["amd.com/gpu=1"].runErr = silent
	for range 2 * notStartedPasses {
		a.probeUnitsOnce(context.Background(), t.TempDir())
	}
	if n := strings.Count(log.String(), "a gpu unit is out of service"); n != 1 {
		t.Fatalf("logged a unit taken out of service %d times, want once:\n%s", n, log.String())
	}
}

// TestProbeUnits_AStopInAPassReleasesItsSilentUnits: a pass the Orker's stop
// cuts off judges no unit and leaves none with its probe.
func TestProbeUnits_AStopInAPassReleasesItsSilentUnits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, rt := twoUnitActivities(t)
	a.dispatcher.Runtime = &stopAfterRuntime{twoUnitRuntime: rt, device: "amd.com/gpu=1", cancel: cancel}
	a.probeUnitsOnce(ctx, t.TempDir())
	for i := range 2 {
		if !a.capacity.unitUsable(i) || a.capacity.units.owner[i] != nil {
			t.Fatalf("unit %d: usable %v, owner %v; want usable and free", i, a.capacity.unitUsable(i), a.capacity.units.owner[i])
		}
	}
	if a.capacity.reserved.gpu != 0 {
		t.Fatalf("%d units stay counted", a.capacity.reserved.gpu)
	}
}

// stopAfterRuntime cancels once device's probe has returned and the other
// unit's is asked for.
type stopAfterRuntime struct {
	*twoUnitRuntime
	device string
	cancel context.CancelFunc
	once   sync.Once
}

func (r *stopAfterRuntime) RunBootProbe(ctx context.Context, spec ContainerSpec) (BootProbeRun, error) {
	run, err := r.twoUnitRuntime.RunBootProbe(ctx, spec)
	if spec.CDIDevices[0] != r.device {
		r.once.Do(r.cancel)
	}
	return run, err
}

// TestProbeUnits_AHeldUnitIsTakenOutWhileOthersStart: a unit held for a
// container that cannot be removed, while other units' probes start, is out
// of service at the third pass and shown so, and stays held: the hold alone
// would show nowhere on a host of several units.
func TestProbeUnits_AHeldUnitIsTakenOutWhileOthersStart(t *testing.T) {
	a, rt := twoUnitActivities(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.profiles.declared.RecoveryInterval = time.Hour // no removal is tried in this test
	unit1 := rt.units["amd.com/gpu=1"]
	unit1.leftovers = []ProbeLeftover{{Container: "stuck", Err: errors.New("device busy")}}
	for i := range notStartedPasses {
		if !a.capacity.unitUsable(1) {
			t.Fatalf("the unit was out after %d passes", i)
		}
		a.probeUnitsOnce(ctx, t.TempDir())
	}
	if a.capacity.unitUsable(1) || !a.capacity.unitUsable(0) {
		t.Fatalf("usable %v and %v; want only the held unit out", a.capacity.unitUsable(0), a.capacity.unitUsable(1))
	}
	if len(unit1.specs) != 1 || a.unitEngine.holds(1) == nil {
		t.Fatalf("%d probes ran on the held unit; want it held, and one", len(unit1.specs))
	}
}

// TestProbeUnits_AHeldUnitCountsAsNotStarted: a pass that can probe nothing
// because a unit is held until an earlier probe's container is gone counts as
// one whose probe was not started; a pass with nothing to probe counts
// neither way.
func TestProbeUnits_AHeldUnitCountsAsNotStarted(t *testing.T) {
	const reason = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_NOT_STARTED
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.profiles.declared.RecoveryInterval = time.Hour // no removal is tried in this test
	silentEngine(rt)
	rt.leftovers = []ProbeLeftover{{Container: "late", Err: errors.New("no answer")}}
	a.probeUnitsOnce(ctx, t.TempDir())
	a.probeUnits(ctx, t.TempDir(), true) // every unit usable: a recovery pass has nothing to do
	a.probeUnits(ctx, t.TempDir(), true)
	if a.profiles.unavailableFor("amd", reason) {
		t.Fatal("passes with nothing to probe were counted")
	}
	a.probeUnitsOnce(ctx, t.TempDir())
	a.probeUnitsOnce(ctx, t.TempDir())
	if !a.profiles.unavailableFor("amd", reason) {
		t.Fatal("passes that found the unit held by an unstarted probe were not counted")
	}
	if detail := a.profiles.causes["amd"].unavailable[reason].GetDetail(); !strings.Contains(detail, "late") || !strings.Contains(detail, "unit 0") {
		t.Fatalf("detail %q; an operator must read which unit is held for which container", detail)
	}
}

// TestProbeUnitsOnce_AUnitIsHeldUntilItsProbeIsGone: a probe that gave no
// verdict and whose container could not be removed keeps its unit, out of
// every task's reach and still in service, until the removal is confirmed:
// the engine may yet start that container.
func TestProbeUnitsOnce_AUnitIsHeldUntilItsProbeIsGone(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.profiles.declared.RecoveryInterval = time.Millisecond
	silentEngine(rt)
	rt.leftovers = []ProbeLeftover{{Container: "late", Err: errors.New("no answer")}}
	rt.setRemoveErr(errors.New("no answer"))
	a.probeUnitsOnce(ctx, t.TempDir())
	for range 3 { // three removals refused: the control that it is being tried
		rt.awaitRemove(ctx, t, "late")
	}
	if a.capacity.tryReserve() != nil {
		t.Fatal("a poll was granted while the probe's container may still start")
	}
	if !a.capacity.unitUsable(0) {
		t.Fatal("the held unit left the usable set")
	}
	rt.setRemoveErr(nil)
	for {
		if permit := a.capacity.tryReserve(); permit != nil {
			a.capacity.release(permit)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("the unit was not freed once its probe's container was gone")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestProbeUnitsOnce_RefusesAProbeImageOfAnotherBuild: a probe image whose
// build is not this Orker's judges no unit; the gpu profile is unavailable
// (PROBE_IMAGE_MISMATCH) until an image of its own build is there.
func TestProbeUnitsOnce_RefusesAProbeImageOfAnotherBuild(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.label = "another"
	a.probeUnitsOnce(context.Background(), t.TempDir())
	mismatch := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	if len(rt.specs) != 0 || !a.profiles.unavailableFor("amd", mismatch) {
		t.Fatalf("probes run %d, mismatch held %v; want none run and PROBE_IMAGE_MISMATCH", len(rt.specs), a.profiles.unavailableFor("amd", mismatch))
	}
	rt.label, rt.view = "c0ffee", append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.profiles.unavailable("amd") || len(rt.specs) != 1 {
		t.Fatalf("an image of its own build: unavailable %v, probes %d", a.profiles.unavailable("amd"), len(rt.specs))
	}
}

// TestProbeUnits_RecoveryProbesOnlyFailedUnits: the recovery pass leaves a
// usable unit alone and brings a failed one back after recoveryPasses good
// probes.
func TestProbeUnits_RecoveryProbesOnlyFailedUnits(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnits(context.Background(), t.TempDir(), true)
	if len(rt.specs) != 0 {
		t.Fatalf("the recovery pass probed a usable unit %d times", len(rt.specs))
	}
	rt.runErr = os.ErrDeadlineExceeded
	a.probeUnitsOnce(context.Background(), t.TempDir())
	rt.runErr = nil
	for range recoveryPasses {
		a.probeUnits(context.Background(), t.TempDir(), true)
	}
	if !a.capacity.unitUsable(0) {
		t.Fatal("the failed unit did not recover after three good recovery probes")
	}
}

// heldLockEntry is the held lock directory as a probe container sees it.
func heldLockEntry(a *Activities) viewEntry {
	_, dev, ino := a.dispatcher.gpu.Load().lock.view()
	return lockEntry(dev, ino)
}

// TestBootGPU_ForeignUnitsHeldUntilGone: a container still running under
// another lock holds the profile FOREIGN_UNITS_HELD, naming it, and the next
// probe pass that finds it gone ends the hold.
func TestBootGPU_ForeignUnitsHeldUntilGone(t *testing.T) {
	rt := &unitListRuntime{listed: []UnitContainer{{Lock: "OLD", Units: "amd:0", State: "running", OwnContainer: OwnContainer{ID: "still"}}}}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	held := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
	if !a.profiles.unavailableFor("amd", held) || len(rt.removed) != 0 {
		t.Fatalf("a foreign running container: held %v, removed %v", a.profiles.unavailableFor("amd", held), rt.removed)
	}
	rt.listed = nil
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir())
	if a.profiles.unavailableFor("amd", held) {
		t.Fatal("the hold did not end once the foreign container was gone")
	}
}

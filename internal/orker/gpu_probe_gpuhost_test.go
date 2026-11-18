//go:build integration && gpuhost

package orker

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestIntegrationGPU_ProbeAndBindOnOneUnit: on an Orker of one unit, a real
// unit probe holds the unit, and while it does no poll is granted, so no
// task can be promised the unit the probe is using; once the probe ends a
// poll is granted and its task is bound the unit. The progress control is
// the probe's container observed running while a grant was refused, and the
// probe judging the unit healthy.
func TestIntegrationGPU_ProbeAndBindOnOneUnit(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-probe-bind", units: []string{"amd"}})
	image := gpuProbeImage(ctx, t)
	offer := g.d.gpu.Load()
	audit, _ := g.s.audit(gpuHostProfile)

	type probeOutcome struct {
		healthy bool
		detail  string
		err     error
	}
	holding := make(chan struct{})
	finished := make(chan probeOutcome, 1)
	var ended atomic.Bool
	go func() {
		probe, err := g.c.probeUnit(ctx, 0)
		if err != nil {
			close(holding)
			finished <- probeOutcome{err: err}
			return
		}
		close(holding)
		healthy, detail, err := g.d.probeGPUUnit(ctx, g.d.Runtime.(bootProber), g.d.WorkRoot, image, offer, 0, audit.unitNodes[0])
		if err != nil {
			g.c.abandonProbe(probe)
		} else {
			g.c.endProbe(probe, healthy)
		}
		ended.Store(true)
		finished <- probeOutcome{healthy: healthy, detail: detail, err: err}
	}()
	<-holding

	refusedWhileRunning := 0
	for !ended.Load() {
		probeRunning := slices.Contains(runningUnits(ctx, t, g.orkerID), unitsLabel(gpuHostProfile, []int{0}))
		if permit := g.c.tryReserve(); permit != nil {
			g.c.release(permit)
			if !ended.Load() {
				t.Fatal("a poll was granted while a unit probe held the Orker's one unit")
			}
			break
		}
		if probeRunning {
			refusedWhileRunning++
		}
		select {
		case <-ctx.Done():
			t.Fatal("the unit probe never ended")
		case <-time.After(20 * time.Millisecond):
		}
	}
	outcome := <-finished
	if outcome.err != nil {
		t.Fatalf("the unit probe: %v", outcome.err)
	}
	if !outcome.healthy {
		t.Fatalf("the unit probe found unit amd unhealthy: %s", outcome.detail)
	}
	if refusedWhileRunning == 0 {
		t.Fatal("no grant was asked while the probe's container ran, so nothing shows the probe held the unit")
	}

	next, done := g.poll(t, g.task(t, "after-probe", 1))
	defer done()
	if err := g.c.bind(next); err != nil {
		t.Fatalf("bind once the probe ended: %v", err)
	}
	if got := next.Units(); !slices.Equal(got, []int{0}) {
		t.Fatalf("the task was bound %v; want the one unit, 0", got)
	}
}

// TestIntegrationGPU_UnitHealthExclusion: a unit whose probe fails leaves
// service, and three good probes bring it back, and not fewer. Under max_gpu 1
// the Orker still has a usable unit for its ceiling: the next gpu: 1 task is
// bound the other unit, and once the unit is back a task is bound it again.
// Under max_gpu 2 the one usable unit is below the ceiling: the Orker names
// UNITS_BELOW_CEILING and grants no poll, because it reserves the largest
// request its channel can carry, until the unit is back. The failing probe is
// a real one whose image cannot run `orker probe unit` (the plain task image),
// as a probe image that does not answer would; the good ones run the orker
// image of this tree. The progress control is the other unit's probe, run
// between them, finding it healthy.
func TestIntegrationGPU_UnitHealthExclusion(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const below = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNITS_BELOW_CEILING
	image := gpuProbeImage(ctx, t)

	// failUnit0 fails unit 0's probe and checks the other unit is healthy.
	failUnit0 := func(t *testing.T, g *gpuOrker) {
		t.Helper()
		if healthy, detail := g.probeOnce(ctx, t, gpuHostImage(t), 0); healthy {
			t.Fatal("a probe whose image cannot run the probe found the unit healthy")
		} else if detail == "" {
			t.Fatal("an unhealthy probe said nothing of why")
		}
		if g.c.unitUsable(0) {
			t.Fatal("unit 0 is usable after its probe failed")
		}
		if healthy, detail := g.probeOnce(ctx, t, image, 1); !healthy {
			t.Fatalf("the control: unit 1's probe found it unhealthy: %s", detail)
		}
	}
	// recoverUnit0 runs the good probes that bring unit 0 back.
	recoverUnit0 := func(t *testing.T, g *gpuOrker) {
		t.Helper()
		for pass := 1; pass <= recoveryPasses; pass++ {
			if healthy, detail := g.probeOnce(ctx, t, image, 0); !healthy {
				t.Fatalf("good probe %d found unit 0 unhealthy: %s", pass, detail)
			}
			if usable := g.c.unitUsable(0); usable != (pass == recoveryPasses) {
				t.Fatalf("after %d good probes unit 0 usable = %v; it returns after %d, not fewer", pass, usable, recoveryPasses)
			}
		}
	}

	t.Run("max_gpu 1 binds the other unit", func(t *testing.T) {
		g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-health-one", maxGPU: 1})
		defer g.close()
		failUnit0(t, g)
		if g.s.unavailableFor(gpuHostProfile, below) {
			t.Fatal("UNITS_BELOW_CEILING is named while unit 1 alone meets max_gpu 1")
		}
		excluded, done := g.poll(t, g.task(t, "while-out", 1))
		if err := g.c.bind(excluded); err != nil {
			t.Fatalf("bind while unit 0 is out: %v", err)
		}
		if got := excluded.Units(); !slices.Equal(got, []int{1}) {
			t.Fatalf("a task was bound %v while unit 0 was out of service; want unit 1", got)
		}
		done()
		recoverUnit0(t, g)
		back, backDone := g.poll(t, g.task(t, "after", 1))
		defer backDone()
		if err := g.c.bind(back); err != nil {
			t.Fatalf("bind once unit 0 is back: %v", err)
		}
		if got := back.Units(); !slices.Equal(got, []int{0}) {
			t.Fatalf("a task was bound %v once unit 0 was back; want unit 0", got)
		}
	})

	t.Run("max_gpu 2 is below its ceiling", func(t *testing.T) {
		g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-health-two", maxGPU: 2})
		defer g.close()
		failUnit0(t, g)
		if !g.s.unavailableFor(gpuHostProfile, below) {
			t.Fatal("UNITS_BELOW_CEILING is not named while one of two units is out of service under max_gpu 2")
		}
		if permit := g.c.tryReserve(); permit != nil {
			g.c.release(permit)
			t.Fatal("a poll was granted while one usable unit is below max_gpu 2")
		}
		recoverUnit0(t, g)
		if g.s.unavailableFor(gpuHostProfile, below) {
			t.Fatal("UNITS_BELOW_CEILING still stands once unit 0 is back")
		}
		back, backDone := g.poll(t, g.task(t, "after", 2))
		defer backDone()
		if err := g.c.bind(back); err != nil {
			t.Fatalf("bind once unit 0 is back: %v", err)
		}
		if got := back.Units(); !slices.Equal(got, []int{0, 1}) {
			t.Fatalf("a gpu: 2 task was bound %v once unit 0 was back; want both units", got)
		}
	})
}

package orker

import (
	"context"
	"errors"
	"slices"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// gpuOffer is this Orker's gpu profile as dispatch binds it: the profile, its
// CDI kind and unit names (index i is unit i), and the held GPU host lock.
type gpuOffer struct {
	profile string
	cdiKind string
	units   []string
	lock    gpuLockView
}

// gpuLockView is what binding asks of the held GPU host lock (gpu_lock_*.go).
type gpuLockView interface {
	check() error
	identity() string
	// view is the lock directory's path and the held fd's dev and inode,
	// which a unit probe's read-only bind of it must show.
	view() (path string, dev, ino uint64)
}

// unitBinding is what step 6b adds to the container spec: the bound units'
// CDI names and the labels the lock-scoped reap finds them by; and to the
// attempt's record: the profile and the units' positions in its list.
type unitBinding struct {
	profile string
	indices []int
	cdi     []string
	labels  map[string]string
}

// bindUnits is dispatch step 6b: after the pull, before the render, a task
// asking for gpu units gets the lowest free usable ones from its admission
// (capacity_units.go), never waiting. Before it binds, the GPU host lock must
// still be the directory it holds; a mismatch is drift (HOST_LOCK_MISMATCH),
// latched until a restart, and the attempt never started. The units are
// freed with the admission, when its container is removed.
func (d *OCIDispatcher) bindUnits(_ context.Context, in *orkstackv1.ExecuteTaskInput, admission *Admission) (unitBinding, *Failure) {
	want := in.GetResources().GetGpu()
	if want == 0 {
		return unitBinding{}, nil
	}
	offer := d.gpu.Load()
	if offer == nil {
		return unitBinding{}, Fail(ReasonGPUUnitsUnavailable,
			"task %q asks for %d gpu units, and this Orker offers no gpu profile", in.GetTaskId(), want)
	}
	if !slices.ContainsFunc(in.GetDevices(), func(r *orkstackv1.ProfileRef) bool { return r.GetName() == offer.profile }) {
		return unitBinding{}, Fail(ReasonProfileNotOffered,
			"task %q asks for gpu units but names no gpu profile this Orker offers (%q) in devices:", in.GetTaskId(), offer.profile)
	}
	if admission == nil {
		return unitBinding{}, Fail(ReasonGPUUnitsUnavailable,
			"task %q asks for gpu units, and a run outside an Orker's ledger holds none", in.GetTaskId())
	}
	if err := offer.lock.check(); err != nil {
		if d.profiles != nil {
			d.profiles.setDrift(offer.profile, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_LOCK_MISMATCH, err.Error(), time.Now())
		}
		return unitBinding{}, neverStarted(FailCause(ReasonProfileDrifted, err,
			"profile %q: %v; it stays out of service until the Orker restarts", offer.profile, err))
	}
	if err := admission.c.bind(admission); err != nil {
		d.logger().Error("gpu units could not be bound: an invariant of the ledger failed",
			"run_id", in.GetRunId(), "task_id", in.GetTaskId(), "error", err)
		var failure *Failure
		if !errors.As(err, &failure) {
			failure = FailCause(ReasonGPUUnitsUnavailable, err, "bind gpu units: %v", err)
		}
		return unitBinding{}, failure
	}
	indices := admission.Units()
	// The reservation is what the poll's admission read of the task; the task is
	// what asks. Were the two ever to differ, a container with no unit, or with
	// one too many, is never the answer.
	if len(indices) != int(want) {
		failure := Fail(ReasonGPUUnitsUnavailable,
			"invariant failure: task %q asks for %d gpu units, and its admission holds %d", in.GetTaskId(), want, len(indices))
		d.logger().Error("gpu units could not be bound: an invariant of the ledger failed",
			"run_id", in.GetRunId(), "task_id", in.GetTaskId(), "error", failure)
		return unitBinding{}, failure
	}
	b := unitBinding{
		profile: offer.profile, indices: indices,
		cdi: make([]string, len(indices)),
		labels: map[string]string{
			labelUnitsLock: offer.lock.identity(),
			labelUnits:     unitsLabel(offer.profile, indices),
		},
	}
	for i, index := range indices {
		b.cdi[i] = offer.cdiKind + "=" + offer.units[index]
	}
	return b, nil
}

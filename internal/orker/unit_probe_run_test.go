package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// unitProbeRuntime is a runtime whose unit probe reports view, or fails with
// runErr. It is a fake because a unit whose driver stops answering cannot be
// produced on demand.
type unitProbeRuntime struct {
	layoutProbeRuntime
	view   []viewEntry
	runErr error
	label  string // the probe image's build label; "" for none
	specs  []ContainerSpec
	// leftovers are handed back with runErr.
	leftovers []ProbeLeftover
	// said is what the container wrote before runErr, and onPull runs in Pull.
	said   string
	onPull func()
	// pullErr and labelErr are what the image check gets.
	pullErr, labelErr error
}

func (r *unitProbeRuntime) RunBootProbe(_ context.Context, spec ContainerSpec) (BootProbeRun, error) {
	r.specs = append(r.specs, spec)
	if r.runErr != nil {
		return BootProbeRun{Stdout: r.said, Leftovers: r.leftovers}, r.runErr
	}
	var out bytes.Buffer
	if err := writeProbeView(&out, r.view); err != nil {
		return BootProbeRun{}, err
	}
	return BootProbeRun{Container: "p1", Stdout: unitProbeStarted + out.String()}, nil
}
func (r *unitProbeRuntime) StartBootProbe(context.Context, ContainerSpec) (BootProbeRun, error) {
	return BootProbeRun{}, errors.New("not used")
}
func (*unitProbeRuntime) bindProbeTimeout() time.Duration { return time.Second }
func (r *unitProbeRuntime) Pull(context.Context, string) (PullInfo, error) {
	if r.onPull != nil {
		r.onPull()
	}
	return PullInfo{}, r.pullErr
}
func (r *unitProbeRuntime) ImageLabel(_ context.Context, _, key string) (string, bool, error) {
	if r.labelErr != nil {
		return "", false, r.labelErr
	}
	if key != LabelBuild || r.label == "" {
		return "", false, nil
	}
	return r.label, true, nil
}
func (r *unitProbeRuntime) LocalDaemon() error { return nil }

type fixedLock struct {
	id       string
	dev, ino uint64
}

func (l fixedLock) check() error                   { return nil }
func (l fixedLock) identity() string               { return l.id }
func (l fixedLock) view() (string, uint64, uint64) { return "/run/orkstack/gpu-orker", l.dev, l.ino }

// lockEntry is the lock directory as a unit probe container reports it.
func lockEntry(dev, ino uint64) viewEntry {
	return viewEntry{Path: gpuLockViewPath, Kind: viewDir, Dev: dev, Ino: ino}
}

var amdNodes = []auditedDevice{
	{ProfileDevice: ProfileDevice{Path: "/dev/dri/renderD128", ContainerPath: "/dev/dri/renderD128", Permissions: "rw"},
		Host: hostDevice{Resolved: "/dev/dri/renderD128", Char: true, Major: 226, Minor: 128, GID: 987}},
	{ProfileDevice: ProfileDevice{Path: "/dev/kfd", ContainerPath: "/dev/kfd", Permissions: "rw"},
		Host: hostDevice{Resolved: "/dev/kfd", Char: true, Major: 234, Minor: 0, GID: 987}},
}

func answering(nodes []auditedDevice) []viewEntry {
	var v []viewEntry
	for _, n := range nodes {
		v = append(v, viewEntry{Path: n.ContainerPath, Kind: viewChar, Major: n.Host.Major, Minor: n.Host.Minor, GID: n.Host.GID, Driver: "amdgpu 3.64"})
	}
	return v
}

// TestProbeGPUUnit_RunsTheUnitAndJudgesIt: the probe container carries the
// unit's CDI device and the labels the lock-scoped reap finds it by, runs
// `orker probe unit` over the unit's nodes, and is healthy only when they are
// the audited ones and every driver answered.
func TestProbeGPUUnit_RunsTheUnitAndJudgesIt(t *testing.T) {
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy()}
	offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "B:1:2"}}
	rt := &unitProbeRuntime{view: append(answering(amdNodes), lockEntry(0, 0))}
	d.Runtime = rt
	healthy, detail, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
	if err != nil || !healthy {
		t.Fatalf("probeGPUUnit = %v, %q, %v", healthy, detail, err)
	}
	spec := rt.specs[0]
	if !slices.Equal(spec.CDIDevices, []string{"amd.com/gpu=0"}) || len(spec.Devices) != 0 {
		t.Fatalf("devices = %v, cdi %v", spec.Devices, spec.CDIDevices)
	}
	if spec.Labels[labelUnitsLock] != "B:1:2" || spec.Labels[labelUnits] != "amd:0" ||
		spec.Labels["ork.orker"] != "orker-a" || spec.Labels["ork.probe"] != "orker-a" {
		t.Fatalf("labels = %v", spec.Labels)
	}
	if got := strings.Join(spec.Command, " "); got != "probe unit /dev/dri/renderD128 /dev/kfd "+gpuLockViewPath {
		t.Fatalf("command = %q", got)
	}
	if spec.Network != NetworkNone {
		t.Fatalf("network = %q", spec.Network)
	}

	silent := append(answering(amdNodes), lockEntry(0, 0))
	silent[1].Driver = ""
	rt = &unitProbeRuntime{view: silent}
	d.Runtime = rt
	if healthy, _, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); healthy || err != nil {
		t.Fatalf("a silent driver = %v, %v; want unhealthy", healthy, err)
	}
	rt = &unitProbeRuntime{runErr: errors.New("start failed: CDI device injection failed")}
	d.Runtime = rt
	if healthy, detail, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); healthy || err != nil || !strings.Contains(detail, "CDI") {
		t.Fatalf("a probe that did not run = %v, %q, %v; want unhealthy, saying why", healthy, detail, err)
	}
}

// judgedProbe is probeGPUUnit of a probe that must judge the unit: healthy or
// not, and why. A probe that gives no verdict fails the test.
func judgedProbe(t *testing.T, d *OCIDispatcher, ctx context.Context, rt bootProber, root, image string, offer *gpuOffer,
	index int, nodes []auditedDevice,
) (bool, string, error) {
	t.Helper()
	out, err := d.probeGPUUnit(ctx, rt, root, image, offer, index, nodes)
	if err == nil && out.verdict == unitNoVerdict {
		t.Fatalf("the probe gave no verdict: %s", out.detail)
	}
	return out.verdict == unitIsHealthy, out.detail, err
}

// endlessProbeRuntime is an engine whose probe container never ends: after
// writing said on its stdout, RunBootProbe returns only when its context
// does, with what it could not remove. It is a fake because neither an engine
// that starts nothing nor a driver that hangs can be produced on demand.
type endlessProbeRuntime struct {
	unitProbeRuntime
	said      string
	leftovers []ProbeLeftover
}

func (r *endlessProbeRuntime) RunBootProbe(ctx context.Context, _ ContainerSpec) (BootProbeRun, error) {
	<-ctx.Done()
	return BootProbeRun{Stdout: r.said, Leftovers: r.leftovers}, fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
}

// TestProbeGPUUnit_WhatASilentProbeMeans: a probe the engine never started
// says nothing of the unit; one that started and gave no report, or whose
// report names a driver that did not answer, is an unhealthy unit, even when
// its container never ended. Each takes the run's bound and no longer, and
// none is called an interruption.
func TestProbeGPUUnit_WhatASilentProbeMeans(t *testing.T) {
	report := func(view []viewEntry) string {
		var out bytes.Buffer
		if err := writeProbeView(&out, view); err != nil {
			t.Fatal(err)
		}
		return unitProbeStarted + out.String()
	}
	hung := answering(amdNodes)
	hung[0] = viewEntry{Path: hung[0].Path, Error: "the driver did not answer within 5s"}
	for name, tc := range map[string]struct {
		said string
		want unitVerdict
		says []string
	}{
		"never started":                        {"", unitNoVerdict, []string{"container engine", unitProbeRunTimeout.String()}},
		"started, no report":                   {unitProbeStarted, unitIsUnhealthy, []string{"started", "no report", unitProbeRunTimeout.String()}},
		"started, half a report":               {unitProbeStarted + `{"view":[`, unitIsUnhealthy, []string{"started", "no report"}},
		"reported a hung driver, never ended":  {report(append(hung, lockEntry(0, 0))), unitIsUnhealthy, []string{"did not answer within 5s"}},
		"reported a healthy unit, never ended": {report(append(answering(amdNodes), lockEntry(0, 0))), unitIsHealthy, nil},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
				offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "L"}}
				rt := &endlessProbeRuntime{said: tc.said}
				d.Runtime = rt
				begin := time.Now()
				out, err := d.probeGPUUnit(t.Context(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
				if err != nil || out.verdict != tc.want {
					t.Fatalf("probeGPUUnit = %v (%s), %v; want %v", out.verdict, out.detail, err, tc.want)
				}
				if took := time.Since(begin); took != unitProbeRunTimeout {
					t.Fatalf("took %s, want %s", took, unitProbeRunTimeout)
				}
				for _, w := range tc.says {
					if !strings.Contains(out.detail, w) {
						t.Fatalf("detail %q does not say %q", out.detail, w)
					}
				}
				if strings.Contains(out.detail, "interrupted") {
					t.Fatalf("detail %q calls a spent bound an interruption", out.detail)
				}
			})
		})
	}
}

// TestProbeGPUUnit_WhatAnErrorMeans: an engine that gave no answer says
// nothing of the unit; an engine that answered and refused the container, as
// it does when the unit's device is gone, does. An image the engine has not
// got is its definite answer about the image, and the image's alone.
func TestProbeGPUUnit_WhatAnErrorMeans(t *testing.T) {
	for name, tc := range map[string]struct {
		err     error
		want    unitVerdict
		wantErr error
	}{
		"too slow":                     {fmt.Errorf("%w: start the probe container", ErrProbeTimedOut), unitNoVerdict, nil},
		"unreachable":                  {fmt.Errorf("%w: %w: docker exited 125", ErrPostureRefused, errEngineUnreachable), unitNoVerdict, nil},
		"unreachable asking for image": {fmt.Errorf("%w: %w: docker exited 1", ErrProbeImageUnavailable, errEngineUnreachable), unitNoVerdict, nil},
		"no such image":                {fmt.Errorf("%w: not found", ErrProbeImageUnavailable), unitNoVerdict, errProbeImageGone},
		"refused the node":             {fmt.Errorf("%w: docker exited 125: CDI device injection failed", ErrPostureRefused), unitIsUnhealthy, nil},
		"refused the bind":             {fmt.Errorf("%w: docker exited 125", ErrNotBindable), unitIsUnhealthy, nil},
	} {
		t.Run(name, func(t *testing.T) {
			d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
			offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "L"}}
			rt := &unitProbeRuntime{runErr: tc.err}
			d.Runtime = rt
			out, err := d.probeGPUUnit(context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
			if !errors.Is(err, tc.wantErr) || out.verdict != tc.want || !strings.Contains(out.detail, tc.err.Error()) {
				t.Fatalf("probeGPUUnit = %v (%s), %v; want %v saying %v", out.verdict, out.detail, err, tc.want, tc.err)
			}
		})
	}
}

// TestProbeGPUUnit_ContextEndIsAnError: a probe cancelled by the Orker's stop
// judges nothing.
func TestProbeGPUUnit_ContextEndIsAnError(t *testing.T) {
	d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
	offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "L"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt := &unitProbeRuntime{runErr: context.Canceled}
	d.Runtime = rt
	if _, _, err := judgedProbe(t, d, ctx, rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); err == nil {
		t.Fatal("a cancelled probe was judged")
	}
}

// TestProbeGPUUnit_ALockSeenOtherwiseIsMismatch: a probe whose read-only bind
// of the lock directory shows another dev:inode than the held fd's is a lock
// mismatch (drift), never an unhealthy unit.
func TestProbeGPUUnit_ALockSeenOtherwiseIsMismatch(t *testing.T) {
	d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
	offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "L", dev: 26, ino: 870304}}
	rt := &unitProbeRuntime{view: append(answering(amdNodes), lockEntry(26, 1))}
	d.Runtime = rt
	_, _, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
	if !errors.Is(err, errHostLockMismatch) {
		t.Fatalf("probeGPUUnit = %v, want errHostLockMismatch", err)
	}
	m := rt.specs[0].Mounts
	if last := m[len(m)-1]; last.Destination != gpuLockViewPath || !last.ReadOnly {
		t.Fatalf("the lock is not bound read-only: %+v", last)
	}
}

// TestProbeGPUUnit_TheRuntimesOwnerOfACDINodeIsHealthy: Docker gives a CDI
// device node in the container the container user's uid and gid, never the
// host node's (measured on Docker 29.9.2: renderD129, 0:987 on the host, is
// 65534:65534 to a `--user 65534:65534` container), and a task reaches the
// node's host group through the spec's additionalGids. So a unit's probe
// judges each node by kind and device number; a gid other than the host's is
// still a healthy unit, and a different device number is not.
func TestProbeGPUUnit_TheRuntimesOwnerOfACDINodeIsHealthy(t *testing.T) {
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy()}
	offer := &gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"0"}, lock: fixedLock{id: "B:1:2"}}
	chowned := answering(amdNodes)
	for i := range chowned {
		chowned[i].GID = 957 // the probe container's own group
	}
	rt := &unitProbeRuntime{view: append(chowned, lockEntry(0, 0))}
	d.Runtime = rt
	if healthy, detail, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); !healthy || err != nil {
		t.Fatalf("nodes the runtime gave the probe's own group = %v, %q, %v; want healthy", healthy, detail, err)
	}
	renumbered := answering(amdNodes)
	renumbered[0].Minor = 129
	rt = &unitProbeRuntime{view: append(renumbered, lockEntry(0, 0))}
	d.Runtime = rt
	if healthy, _, err := judgedProbe(t, d, context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); healthy || err != nil {
		t.Fatalf("a node of another device number = %v, %v; want unhealthy", healthy, err)
	}
}

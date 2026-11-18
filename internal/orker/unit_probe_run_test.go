package orker

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
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
}

func (r *unitProbeRuntime) RunBootProbe(_ context.Context, spec ContainerSpec) (BootProbeRun, error) {
	r.specs = append(r.specs, spec)
	if r.runErr != nil {
		return BootProbeRun{}, r.runErr
	}
	var out bytes.Buffer
	if err := writeProbeView(&out, r.view); err != nil {
		return BootProbeRun{}, err
	}
	return BootProbeRun{Container: "p1", Stdout: out.String()}, nil
}
func (r *unitProbeRuntime) StartBootProbe(context.Context, ContainerSpec) (BootProbeRun, error) {
	return BootProbeRun{}, errors.New("not used")
}
func (*unitProbeRuntime) bindProbeTimeout() time.Duration                  { return time.Second }
func (r *unitProbeRuntime) Pull(context.Context, string) (PullInfo, error) { return PullInfo{}, nil }
func (r *unitProbeRuntime) ImageLabel(_ context.Context, _, key string) (string, bool, error) {
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
	healthy, detail, err := d.probeGPUUnit(context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
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
	if healthy, _, err := d.probeGPUUnit(context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); healthy || err != nil {
		t.Fatalf("a silent driver = %v, %v; want unhealthy", healthy, err)
	}
	rt = &unitProbeRuntime{runErr: errors.New("start failed: CDI device injection failed")}
	d.Runtime = rt
	if healthy, detail, err := d.probeGPUUnit(context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); healthy || err != nil || !strings.Contains(detail, "CDI") {
		t.Fatalf("a probe that did not run = %v, %q, %v; want unhealthy, saying why", healthy, detail, err)
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
	if _, _, err := d.probeGPUUnit(ctx, rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes); err == nil {
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
	_, _, err := d.probeGPUUnit(context.Background(), rt, t.TempDir(), viewProbeImage, offer, 0, amdNodes)
	if !errors.Is(err, errHostLockMismatch) {
		t.Fatalf("probeGPUUnit = %v, want errHostLockMismatch", err)
	}
	m := rt.specs[0].Mounts
	if last := m[len(m)-1]; last.Destination != gpuLockViewPath || !last.ReadOnly {
		t.Fatalf("the lock is not bound read-only: %+v", last)
	}
}

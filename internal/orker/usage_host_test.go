package orker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// fakeHost lays out /proc/<pid>/cgroup and a cgroup mount under a temp dir.
// It is a fake for the fault cases a real host cannot be made to produce on
// demand — a cgroup replaced at its path, removed mid-read, a remote
// daemon's pid, rootless on cgroup v1 — and for each engine's layout on a
// host that runs only one. The real cgroup path is proven on a real engine
// by TestIntegration_UsageLocatorFindsTheContainersCgroup.
func fakeHost(t *testing.T, procCgroup string, v2 bool, dirs map[string]map[string]string) cgroupHost {
	t.Helper()
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	if err := os.MkdirAll(filepath.Join(proc, "41"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proc, "41", "cgroup"), []byte(procCgroup), 0o644); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(root, "cgroup")
	for dir, files := range dirs {
		full := filepath.Join(mount, dir)
		if err := os.MkdirAll(full, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, body := range files {
			if err := os.WriteFile(filepath.Join(full, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return cgroupHost{proc: proc, mount: mount, v2: func(string) (bool, error) { return v2, nil }, verify: (&fakeVerify{}).verify}
}

// fakeVerify stands in for the cgroup filesystem check on a test's temp
// directory, which is on no cgroup filesystem: a fault case a real host
// cannot be made to produce on demand. fail makes it refuse, as a host whose
// mount is not cgroupfs would.
type fakeVerify struct{ fail atomic.Bool }

func (f *fakeVerify) verify(*os.File) error {
	if f.fail.Load() {
		return fmt.Errorf("%w: not on a cgroup filesystem (magic 0x58465342)", errCgroupUnverified)
	}
	return nil
}

// replaceAside moves a cgroup directory out of its path, as the kernel's
// removal frees it. It is moved, not removed, because an ordinary
// filesystem hands a freed inode number to the next directory made, which
// cgroupfs (kernfs, whose inode numbers are allocated cyclically and not
// soon reused) does not: removed here, the replacement would be the same
// file to os.SameFile and the test would prove nothing.
func replaceAside(dir string) error {
	return os.Rename(dir, dir+".removed")
}

var v2Files = map[string]string{"cgroup.procs": "41\n", "cpu.stat": "usage_usec 1000000\n", "memory.current": "5\n", "memory.peak": "8\n"}

func TestCgroupHost_LocatesV2(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: v2Files})
	read, method, err := h.locate(41, dockerDialect, usageID)
	if err != nil || method != usageCgroupV2 {
		t.Fatalf("locate = %v, %v; want v2", method, err)
	}
	if r, err := read(false); err != nil || r.cpuSeconds != 1 || r.memoryPeak != 8 {
		t.Errorf("read = %+v, %v", r, err)
	}
}

func TestCgroupHost_LocatesV1(t *testing.T) {
	leaf := "docker/" + usageID
	h := fakeHost(t, "12:memory:/"+leaf+"\n4:cpu,cpuacct:/"+leaf+"\n", false, map[string]map[string]string{
		"cpuacct/" + leaf: {"cpuacct.usage": "2000000000\n"},
		"memory/" + leaf:  {"cgroup.procs": "41\n", "memory.usage_in_bytes": "3\n"},
	})
	read, method, err := h.locate(41, dockerDialect, usageID)
	if err != nil || method != usageCgroupV1 {
		t.Fatalf("locate = %v, %v; want v1", method, err)
	}
	if r, err := read(false); err != nil || r.cpuSeconds != 2 || r.memory != 3 {
		t.Errorf("read = %+v, %v", r, err)
	}
}

func TestCgroupHost_Refusals(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	for _, tc := range []struct {
		name string
		host cgroupHost
		d    dialect
		pid  int
		want error
	}{
		{"no such process on this host", fakeHost(t, "0::/"+leaf+"\n", true, nil), dockerDialect, 42, errCgroupNotFound},
		{"another process (a remote daemon's pid)", fakeHost(t, "0::/user.slice/session-3.scope\n", true, nil), dockerDialect, 41, errCgroupNotFound},
		{"rootless on v1", fakeHost(t, "12:memory:/docker/"+usageID+"\n", false, nil), dialect{cli: CLIDocker, engine: CLIDocker, rootless: true}, 41, errNoCgroup},
		{"v1 without cpuacct", fakeHost(t, "12:memory:/docker/"+usageID+"\n", false, nil), dockerDialect, 41, errCgroupNotFound},
		{"v2 host, only v1 lines", fakeHost(t, "12:memory:/docker/"+usageID+"\n", true, nil), dockerDialect, 41, errCgroupNotFound},
		{"no pid", fakeHost(t, "", true, nil), dockerDialect, 0, errCgroupGone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := tc.host.locate(tc.pid, tc.d, usageID); !errors.Is(err, tc.want) {
				t.Errorf("locate = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestDockerRuntime_UsageLocatorReadsThePidFromTheEngine: the sampler's
// locate asks the engine for the container's pid, then finds its cgroup; a
// container that is not running (pid 0) is gone, and an engine that refuses
// or answers nonsense is a read the sampler may ask again (errCgroupRead),
// never a cgroup this host cannot see.
func TestDockerRuntime_UsageLocatorReadsThePidFromTheEngine(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	host := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: v2Files})
	for _, tc := range []struct {
		name   string
		answer string
		want   error
	}{
		{"running", "  inspect) echo 41 ;;\n", nil},
		{"not running", "  inspect) echo 0 ;;\n", errCgroupGone},
		{"the engine refuses", "  inspect) echo 'no such container' >&2; exit 1 ;;\n", errCgroupRead},
		{"garbage", "  inspect) echo forty-one ;;\n", errCgroupRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "docker")
			script := "#!/bin/sh\ncase \"$1\" in\n" + tc.answer + "  *) exit 1 ;;\nesac\n"
			if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			spoken := dockerDialect
			runtime := &DockerRuntime{Bin: bin, spoken: &spoken}
			read, method, err := runtime.usageLocator(ContainerID(usageID), host)(t.Context())
			if !errors.Is(err, tc.want) {
				t.Fatalf("locate = %v, want %v", err, tc.want)
			}
			if tc.want == nil {
				if r, err := read(false); method != usageCgroupV2 || err != nil || r.cpuSeconds != 1 {
					t.Errorf("read = %+v, %v (%v)", r, err, method)
				}
			}
		})
	}
}

// TestCgroupHost_AShortTasksFinalReadIsTheKernelsTotals: a container that
// ended before any periodic read leaves an empty cgroup until its removal.
// A periodic read of it ends sampling (errCgroupEmpty); the final read takes
// its totals, the kernel's own peak included.
func TestCgroupHost_AShortTasksFinalReadIsTheKernelsTotals(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	ended := map[string]string{"cgroup.procs": "", "cpu.stat": "usage_usec 250000\n", "memory.current": "4096\n", "memory.peak": "65536\n"}
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: ended})
	read, _, err := h.locate(41, dockerDialect, usageID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(false); !errors.Is(err, errCgroupEmpty) {
		t.Fatalf("periodic read of an ended container = %v, want errCgroupEmpty", err)
	}
	r, err := read(true)
	if err != nil || r.cpuSeconds != 0.25 || r.memoryPeak != 65536 || !r.peakRead {
		t.Fatalf("final read = %+v, %v; want the container's totals with the kernel's peak", r, err)
	}
	// The final read never reaches a cgroup that is gone.
	if err := os.RemoveAll(filepath.Join(h.mount, leaf)); err != nil {
		t.Fatal(err)
	}
	if _, err := read(true); !errors.Is(err, errCgroupGone) {
		t.Fatalf("final read of a removed cgroup = %v, want errCgroupGone", err)
	}
}

// TestCgroupHost_AReusedPathIsNeverAnotherCgroupsTotals: the reader pins the
// directory it first read. A cgroup removed and made again at the same path
// — whatever made it — is the attempt's cgroup gone, on a periodic read and
// on the final one alike, never the new cgroup's numbers.
func TestCgroupHost_AReusedPathIsNeverAnotherCgroupsTotals(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: v2Files})
	read, _, err := h.locate(41, dockerDialect, usageID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(false); err != nil {
		t.Fatalf("first read: %v", err)
	}
	dir := filepath.Join(h.mount, leaf)
	if err := replaceAside(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"cgroup.procs": "77\n", "cpu.stat": "usage_usec 999000000\n", "memory.peak": "1\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, final := range []bool{false, true} {
		if r, err := read(final); !errors.Is(err, errCgroupGone) {
			t.Fatalf("read (final %v) of a reused path = %+v, %v; want errCgroupGone", final, r, err)
		}
	}
	// The v1 reader pins both of its directories.
	cpuLeaf, memLeaf := "cpuacct/docker/"+usageID, "memory/docker/"+usageID
	v1 := fakeHost(t, "4:cpu,cpuacct:/docker/"+usageID+"\n12:memory:/docker/"+usageID+"\n", false, map[string]map[string]string{
		cpuLeaf: {"cpuacct.usage": "1000000000\n"},
		memLeaf: {"cgroup.procs": "41\n", "memory.usage_in_bytes": "5\n", "memory.max_usage_in_bytes": "8\n", "memory.limit_in_bytes": "100\n"},
	})
	read, _, err = v1.locate(41, dockerDialect, usageID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(false); err != nil {
		t.Fatalf("v1 first read: %v", err)
	}
	cpuDir := filepath.Join(v1.mount, cpuLeaf)
	if err := replaceAside(cpuDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cpuDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cpuDir, "cpuacct.usage"), []byte("5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := read(true); !errors.Is(err, errCgroupGone) {
		t.Fatalf("v1 final read with a reused cpuacct path = %v, want errCgroupGone", err)
	}
}

// TestCgroupHost_APathReplacedBeforeTheFirstReadIsGone: the pin is taken at
// locate, so a cgroup replaced between locate and the first read — a short
// task's first read being its final one — is gone, not adopted.
func TestCgroupHost_APathReplacedBeforeTheFirstReadIsGone(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: v2Files})
	read, _, err := h.locate(41, dockerDialect, usageID)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(h.mount, leaf)
	if err := replaceAside(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"cgroup.procs": "", "cpu.stat": "usage_usec 999000000\n", "memory.peak": "1\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := read(true); !errors.Is(err, errCgroupGone) {
		t.Fatalf("final read of a path replaced since locate = %+v, %v; want errCgroupGone", r, err)
	}
}

// TestCgroupHost_AnUnverifiableCgroupIsNeverRead: a cgroup directory on no
// cgroup filesystem is CGROUP_UNVERIFIED at locate, and a host with no way to
// verify one at all is too; neither is read on its inode alone.
func TestCgroupHost_AnUnverifiableCgroupIsNeverRead(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{leaf: v2Files})
	v := &fakeVerify{}
	h.verify = v.verify
	v.fail.Store(true)
	if _, _, err := h.locate(41, dockerDialect, usageID); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("locate on no cgroup filesystem = %v, want errCgroupUnverified", err)
	}
	h.verify = nil
	if _, _, err := h.locate(41, dockerDialect, usageID); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("locate with no way to verify = %v, want errCgroupUnverified", err)
	}
}

// TestCgroupHost_EveryV1DirectoryIsHeldToItsIdentity: on cgroup v1 the
// memory directory, not only the cpuacct one, is held to the identity pinned
// at locate: replaced, every read after is gone.
func TestCgroupHost_EveryV1DirectoryIsHeldToItsIdentity(t *testing.T) {
	cpuLeaf, memLeaf := "cpuacct/docker/"+usageID, "memory/docker/"+usageID
	h := fakeHost(t, "4:cpu,cpuacct:/docker/"+usageID+"\n12:memory:/docker/"+usageID+"\n", false, map[string]map[string]string{
		cpuLeaf: {"cpuacct.usage": "1000000000\n"},
		memLeaf: {"cgroup.procs": "41\n", "memory.usage_in_bytes": "5\n", "memory.max_usage_in_bytes": "8\n", "memory.limit_in_bytes": "100\n"},
	})
	read, _, err := h.locate(41, dockerDialect, usageID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(false); err != nil {
		t.Fatalf("first read: %v", err)
	}
	memDir := filepath.Join(h.mount, memLeaf)
	if err := replaceAside(memDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(memDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"cgroup.procs": "77\n", "memory.usage_in_bytes": "1\n", "memory.limit_in_bytes": "100\n"} {
		if err := os.WriteFile(filepath.Join(memDir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := read(true); !errors.Is(err, errCgroupGone) {
		t.Fatalf("v1 final read with the memory directory replaced = %+v, %v; want errCgroupGone", r, err)
	}
}

// TestCgroupHost_ALinkWhereTheCgroupShouldBeIsRefused: a symbolic link at
// the container's leaf is never followed into another directory.
func TestCgroupHost_ALinkWhereTheCgroupShouldBeIsRefused(t *testing.T) {
	leaf := "system.slice/docker-" + usageID + ".scope"
	other := "system.slice/other.scope"
	h := fakeHost(t, "0::/"+leaf+"\n", true, map[string]map[string]string{other: v2Files})
	if err := os.Symlink(filepath.Join(h.mount, other), filepath.Join(h.mount, leaf)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.locate(41, dockerDialect, usageID); !errors.Is(err, errCgroupRefused) {
		t.Fatalf("locate through a link = %v, want errCgroupRefused", err)
	}
}

// TestReadCgroup_WithoutAPinIsNeverRead: a read with no identity to hold
// the directory to fails closed.
func TestReadCgroup_WithoutAPinIsNeverRead(t *testing.T) {
	dir := writeCgroup(t, v2Files)
	if _, err := readCgroupV2(dir, false, nil); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("an unpinned v2 read = %v, want errCgroupUnverified", err)
	}
	if _, err := readCgroupV1(dir, dir, false, &cgroupPin{}); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("a v1 read with an empty pin = %v, want errCgroupUnverified", err)
	}
}

// TestProbeCgroupIdentity_SaysWhetherThisHostCanVerify: a host whose cgroup
// mount is a cgroup filesystem passes; one whose mount is not, or has no way
// to verify at all, is unverified.
func TestProbeCgroupIdentity_SaysWhetherThisHostCanVerify(t *testing.T) {
	h := fakeHost(t, "", true, map[string]map[string]string{"system.slice": v2Files})
	if err := probeCgroupIdentity(h); err != nil {
		t.Fatalf("probe = %v, want none", err)
	}
	v := &fakeVerify{}
	v.fail.Store(true)
	h.verify = v.verify
	if err := probeCgroupIdentity(h); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("probe on no cgroup filesystem = %v, want errCgroupUnverified", err)
	}
	h.verify = nil
	if err := probeCgroupIdentity(h); !errors.Is(err, errCgroupUnverified) {
		t.Fatalf("probe with no identity = %v, want errCgroupUnverified", err)
	}
	// The real host: its own answer, which the boot logs.
	t.Logf("this host: %v", probeCgroupIdentity(hostCgroups()))
}

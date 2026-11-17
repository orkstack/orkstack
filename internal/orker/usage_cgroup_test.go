package orker

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const usageID = "4f0c2b1e9d8a7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9c8b7a6f5e4d3c2b"

var podmanDialect = dialect{cli: CLIPodman, engine: CLIPodman}

// TestParseProcCgroup_FindsOnlyTheContainersOwnLeaf: the container's cgroup
// is the line whose path ends in exactly the dialect's leaf for this id; a
// parent, a child, another id, a relative path or a traversal is refused.
func TestParseProcCgroup_FindsOnlyTheContainersOwnLeaf(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dialect dialect
		content string
		v2      string
		v1      map[string]string
		err     error
	}{
		{"docker systemd v2", dockerDialect, "0::/system.slice/docker-" + usageID + ".scope\n",
			"/system.slice/docker-" + usageID + ".scope", nil, nil},
		{"docker cgroupfs v2", dockerDialect, "0::/docker/" + usageID + "\n", "/docker/" + usageID, nil, nil},
		{"podman systemd v2", podmanDialect, "0::/machine.slice/libpod-" + usageID + ".scope\n",
			"/machine.slice/libpod-" + usageID + ".scope", nil, nil},
		{"podman rootless v2", podmanDialect,
			"0::/user.slice/user-1000.slice/user@1000.service/user.slice/libpod-" + usageID + ".scope\n",
			"/user.slice/user-1000.slice/user@1000.service/user.slice/libpod-" + usageID + ".scope", nil, nil},
		{"docker v1", dockerDialect,
			"12:memory:/docker/" + usageID + "\n4:cpu,cpuacct:/docker/" + usageID + "\n1:name=systemd:/docker/" + usageID + "\n",
			"", map[string]string{"memory": "/docker/" + usageID, "cpu": "/docker/" + usageID, "cpuacct": "/docker/" + usageID}, nil},
		{"hybrid keeps both", dockerDialect,
			"0::/system.slice/docker-" + usageID + ".scope\n12:memory:/system.slice/docker-" + usageID + ".scope\n",
			"/system.slice/docker-" + usageID + ".scope",
			map[string]string{"memory": "/system.slice/docker-" + usageID + ".scope"}, nil},
		{"the Orker's own cgroup", dockerDialect, "0::/system.slice/orker.service\n", "", nil, errCgroupNotFound},
		{"another container", dockerDialect, "0::/system.slice/docker-" + strings.Repeat("a", 64) + ".scope\n", "", nil, errCgroupNotFound},
		{"a child of the leaf", dockerDialect, "0::/system.slice/docker-" + usageID + ".scope/init\n", "", nil, errCgroupNotFound},
		{"an id with a suffix", dockerDialect, "0::/system.slice/docker-" + usageID + "x.scope\n", "", nil, errCgroupNotFound},
		{"podman's leaf under docker", dockerDialect, "0::/machine.slice/libpod-" + usageID + ".scope\n", "", nil, errCgroupNotFound},
		{"a traversal", dockerDialect, "0::/system.slice/../docker-" + usageID + ".scope\n", "", nil, errCgroupRefused},
		{"a relative path", dockerDialect, "0::docker/" + usageID + "\n", "", nil, errCgroupRefused},
		{"empty", dockerDialect, "", "", nil, errCgroupNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProcCgroup(tc.content, tc.dialect, usageID)
			if !errors.Is(err, tc.err) {
				t.Fatalf("parseProcCgroup = %+v, %v; want error %v", got, err, tc.err)
			}
			if err != nil {
				return
			}
			if got.v2 != tc.v2 {
				t.Errorf("v2 = %q, want %q", got.v2, tc.v2)
			}
			if len(got.v1) != len(tc.v1) {
				t.Errorf("v1 = %v, want %v", got.v1, tc.v1)
			}
			for controller, path := range tc.v1 {
				if got.v1[controller] != path {
					t.Errorf("v1[%s] = %q, want %q", controller, got.v1[controller], path)
				}
			}
		})
	}
}

// writeCgroup lays out a fake cgroup directory with the given files.
func writeCgroup(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestReadCgroupV2(t *testing.T) {
	full := map[string]string{
		"cgroup.procs":   "41\n42\n",
		"cpu.stat":       "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n",
		"memory.peak":    "104857600\n",
		"memory.current": "52428800\n",
		"memory.max":     "268435456\n",
		"memory.events":  "low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n",
	}
	got, err := readV2(writeCgroup(t, full), false)
	if err != nil {
		t.Fatal(err)
	}
	want := cgroupReading{cpuSeconds: 2.5, memory: 52428800, memoryPeak: 104857600, peakRead: true, limit: 268435456, oomKills: 1}
	if got != want {
		t.Errorf("readCgroupV2 = %+v, want %+v", got, want)
	}

	// No memory.peak (kernels before 5.19): current stands in, not exact.
	noPeak := copyFiles(full)
	delete(noPeak, "memory.peak")
	noPeak["memory.max"] = "max\n"
	got, err = readV2(writeCgroup(t, noPeak), false)
	if err != nil || got.peakRead || got.memoryPeak != 52428800 || got.limit != 0 {
		t.Errorf("without memory.peak = %+v, %v; want current as an inexact peak and no limit", got, err)
	}

	for name, broken := range map[string]map[string]string{
		"no usage_usec":      cgWith(full, "cpu.stat", "user_usec 1\n"),
		"negative usage":     cgWith(full, "cpu.stat", "usage_usec -1\n"),
		"usage out of range": cgWith(full, "cpu.stat", "usage_usec 99999999999999999999\n"),
		"garbage peak":       cgWith(full, "memory.peak", "lots\n"),
		"no memory at all":   cgWithout(cgWithout(full, "memory.peak"), "memory.current"),
		"no cpu.stat":        cgWithout(full, "cpu.stat"),
	} {
		if got, err := readV2(writeCgroup(t, broken), false); !errors.Is(err, errCgroupRead) {
			t.Errorf("%s: readCgroupV2 = %+v, %v; want errCgroupRead, never a zero", name, got, err)
		}
	}
	// An empty cgroup.procs: the container's processes are not in it (it
	// ended, or this is not its cgroup).
	if _, err := readV2(writeCgroup(t, cgWith(full, "cgroup.procs", "")), false); !errors.Is(err, errCgroupEmpty) {
		t.Errorf("empty cgroup.procs = %v, want errCgroupEmpty", err)
	}
	if _, err := readV2(filepath.Join(t.TempDir(), "gone"), false); !errors.Is(err, errCgroupGone) {
		t.Errorf("a removed cgroup = %v, want errCgroupGone", err)
	}
}

func TestReadCgroupV2_RefusesASymlinkedFile(t *testing.T) {
	dir := writeCgroup(t, map[string]string{"cgroup.procs": "41\n", "memory.current": "1\n", "memory.max": "max\n", "memory.events": "oom_kill 0\n"})
	target := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(target, []byte("usage_usec 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "cpu.stat")); err != nil {
		t.Fatal(err)
	}
	if got, err := readV2(dir, false); err == nil {
		t.Errorf("a symlinked cpu.stat was read: %+v", got)
	}
}

func TestReadCgroupV1(t *testing.T) {
	cpu := writeCgroup(t, map[string]string{"cpuacct.usage": "1500000000\n"})
	memory := writeCgroup(t, map[string]string{
		"cgroup.procs":              "41\n",
		"memory.usage_in_bytes":     "1000\n",
		"memory.max_usage_in_bytes": "4096\n",
		"memory.limit_in_bytes":     "9223372036854771712\n",
		"memory.oom_control":        "oom_kill_disable 0\nunder_oom 0\noom_kill 2\n",
	})
	got, err := readV1(cpu, memory, false)
	if err != nil {
		t.Fatal(err)
	}
	want := cgroupReading{cpuSeconds: 1.5, memory: 1000, memoryPeak: 4096, peakRead: true, limit: 0, oomKills: 2}
	if got != want {
		t.Errorf("readCgroupV1 = %+v, want %+v", got, want)
	}
}

func copyFiles(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cgWith(m map[string]string, name, body string) map[string]string {
	out := copyFiles(m)
	out[name] = body
	return out
}

func cgWithout(m map[string]string, name string) map[string]string {
	out := copyFiles(m)
	delete(out, name)
	return out
}

// FuzzParseProcCgroup: never panics, and an accepted path is absolute, has no
// traversal, and ends in the dialect's leaf for the id.
func FuzzParseProcCgroup(f *testing.F) {
	for _, seed := range []string{
		"0::/system.slice/docker-" + usageID + ".scope\n",
		"12:memory:/docker/" + usageID + "\n4:cpu,cpuacct:/docker/" + usageID + "\n",
		"0::/system.slice/../docker-" + usageID + ".scope\n",
		"0::docker/" + usageID, "", "::\n:::\n", "0::/\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, content string) {
		for _, d := range []dialect{dockerDialect, podmanDialect} {
			got, err := parseProcCgroup(content, d, usageID)
			if err != nil {
				continue
			}
			paths := []string{got.v2}
			for _, p := range got.v1 {
				paths = append(paths, p)
			}
			for _, p := range paths {
				if p == "" {
					continue
				}
				if !strings.HasPrefix(p, "/") || slices.Contains(strings.Split(p[1:], "/"), "..") ||
					slices.Contains(strings.Split(p[1:], "/"), ".") || strings.ContainsAny(p, "\n\x00") ||
					!d.cgroupLeaf(usageID, filepath.Base(filepath.Dir(p)), filepath.Base(p)) {
					t.Fatalf("accepted %q from %q", p, content)
				}
			}
		}
	})
}

// TestReadCgroupV2_ACgroupRemovedMidReadIsGone: the runtime removes a
// container's cgroup right after it exits; a read caught between files is
// "gone", so the last sample stands, never READ_FAILED.
func TestReadCgroupV2_ACgroupRemovedMidReadIsGone(t *testing.T) {
	dir := writeCgroup(t, map[string]string{"cgroup.procs": "41\n", "cpu.stat": "usage_usec 1\n", "memory.current": "1\n"})
	c, err := openCgroupDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := c.required("cpu.stat"); !errors.Is(err, errCgroupGone) {
		t.Errorf("a required file of a removed cgroup = %v, want errCgroupGone", err)
	}
	if _, _, err := c.optional("memory.peak"); !errors.Is(err, errCgroupGone) {
		t.Errorf("an optional file of a removed cgroup = %v, want errCgroupGone", err)
	}
	// A file merely absent from a cgroup still there stays absent.
	live := writeCgroup(t, map[string]string{"cgroup.procs": "41\n"})
	lc, err := openCgroupDir(live)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.close()
	if _, found, err := lc.optional("memory.peak"); err != nil || found {
		t.Errorf("an absent file of a live cgroup = found %v, %v; want absent", found, err)
	}
}

// TestReadCgroupV2_ManyProcessesAreStillOneRead: cgroup.procs is asked only
// whether it is empty, however long it is.
func TestReadCgroupV2_ManyProcessesAreStillOneRead(t *testing.T) {
	procs := strings.Repeat("1234567\n", 20000) // 160 KB, past cgroupFileMax
	dir := writeCgroup(t, map[string]string{"cgroup.procs": procs, "cpu.stat": "usage_usec 1\n", "memory.current": "1\n"})
	if _, err := readV2(dir, false); err != nil {
		t.Errorf("a cgroup with many processes = %v", err)
	}
}

func TestParseProcCgroup_PodmansContainerChild(t *testing.T) {
	path := "/machine.slice/libpod-" + usageID + ".scope/container"
	got, err := parseProcCgroup("0::"+path+"\n", podmanDialect, usageID)
	if err != nil || got.v2 != path {
		t.Errorf("parseProcCgroup = %+v, %v; want the container child %q", got, err, path)
	}
	if _, err := parseProcCgroup("0::"+path+"\n", dockerDialect, usageID); !errors.Is(err, errCgroupNotFound) {
		t.Errorf("Docker took Podman's child: %v", err)
	}
	if _, err := parseProcCgroup("0::/machine.slice/libpod-"+usageID+".scope/other\n", podmanDialect, usageID); !errors.Is(err, errCgroupNotFound) {
		t.Errorf("any other child was taken: %v", err)
	}
}

// readV2 and readV1 read a test's cgroup directories pinned as locate pins
// them, with the test's stand-in for the cgroup filesystem check.
func readV2(dir string, final bool) (cgroupReading, error) {
	pin, err := pinAt((&fakeVerify{}).verify, dir)
	if err != nil {
		return cgroupReading{}, err
	}
	return readCgroupV2(dir, final, pin)
}

func readV1(cpuDir, memoryDir string, final bool) (cgroupReading, error) {
	pin, err := pinAt((&fakeVerify{}).verify, cpuDir, memoryDir)
	if err != nil {
		return cgroupReading{}, err
	}
	return readCgroupV1(cpuDir, memoryDir, final, pin)
}

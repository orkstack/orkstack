//go:build integration

package orker_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/cdidind"
)

// nvidiaHooksEnv names the directory scripts/nvidia-hooks-fetch.sh wrote: one
// directory per table row of this arch, "<version>-<arch>-<packaging>", each
// holding the nvidia-cdi-hook the signed repository serves for it. The test
// only reads it; a missing directory is a missing prerequisite.
const nvidiaHooksEnv = "ORK_TEST_NVIDIA_HOOKS"

// emulationSpec is the dind's spec: the admitted shape nvidia-ctk 1.20.2
// wrote (testdata/cdi/nvidia-1.20.2-admitted.yaml), over nodes that are 1:3
// under NVIDIA's names, with what a workload image without a driver can have:
// no driver library mounts, and the one library symlink the real hook makes in
// the library directory every image has. Both the verbatim fixture and this
// spec are held to the audit below.
const emulationSpec = `cdiVersion: 0.5.0
kind: nvidia.com/gpu
devices:
  - name: all
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia-uvm, hostPath: /dev/nvidia-uvm, permissions: rwm}
        - {path: /dev/nvidia-uvm-tools, hostPath: /dev/nvidia-uvm-tools, permissions: rwm}
        - {path: /dev/nvidia0, hostPath: /dev/nvidia0, permissions: rwm}
        - {path: /dev/nvidiactl, hostPath: /dev/nvidiactl, permissions: rwm}
containerEdits:
  env:
    - NVIDIA_CTK_LIBCUDA_DIR=/usr/lib
    - NVIDIA_VISIBLE_DEVICES=void
  hooks:
    - hookName: createContainer
      path: /usr/bin/nvidia-cdi-hook
      args: [nvidia-cdi-hook, create-symlinks, --link, "libcuda.so.1::/usr/lib/libcuda.so"]
      env: [NVIDIA_CTK_DEBUG=false]
  mounts:
    - hostPath: /dev/null
      containerPath: /var/run/nvidia-container-devices/void
      options: [ro, nosuid, nodev, bind]
`

// TestIntegration_NVIDIAEmulation (design E91 §7.4, P4b; experimental, proven
// by emulation and not on NVIDIA hardware): for every build of nvidia-cdi-hook
// the table lists for this arch, the real binary runs as a CDI spec's hook in a
// rootful dind over nodes that are 1:3, dockerd resolves the spec, and the
// audit admits exactly what it should: the binary's build is a row of the
// table, the spec the hook ran under passes the grammar, and a stock spec, a
// build the table lacks and a hook outside the grammar do not.
func TestIntegration_NVIDIAEmulation(t *testing.T) {
	dir := os.Getenv(nvidiaHooksEnv)
	if dir == "" {
		infraAbsent(t, "%s names no directory of nvidia-cdi-hook builds; scripts/nvidia-hooks-fetch.sh makes one from NVIDIA's signed repository", nvidiaHooksEnv)
		return
	}
	arch := runtime.GOARCH
	rows := orker.HookTableRowsForTest(arch)
	if len(rows) == 0 {
		t.Fatalf("the table lists no build for %s", arch)
	}
	fixture := func(name string) string {
		raw, err := os.ReadFile(filepath.Join("testdata", "cdi", name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.NewReplacer("/mock/driver", "", "/mock/dev", "/dev").Replace(string(raw))
	}

	// Where the host-side trust check of the hook binary (root-owned file and
	// directories, hashed from one open file) is covered: TestHashHostHook reads
	// a real root-owned binary; here the fetched copies are the user's, so the
	// audit is given their digests through a seam and the table is what is held.
	// The binaries first, on the host: each is hashed, and its digest must be
	// the table's row. A directory the fetch left out is a missing prerequisite.
	binaries := map[string]string{}
	for _, row := range slices.Sorted(maps.Keys(rows)) {
		path := filepath.Join(dir, row, "nvidia-cdi-hook")
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			infraAbsent(t, "no %s: scripts/nvidia-hooks-fetch.sh has not fetched the table's row %s", path, row)
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != rows[row] {
			t.Fatalf("%s is sha256:%s, the table's row %s says %s: the table and the signed repository disagree", path, got, row, rows[row])
		}
		binaries[row] = path
	}

	// The audit on the host, per build: it admits the fixture, the dind's spec
	// and the hookless fixture, and records the build's digest.
	for _, spec := range []string{fixture("nvidia-1.20.2-admitted.yaml"), emulationSpec} {
		for row, digest := range rows {
			hooks, err := orker.AuditNVIDIASpecForTest(t, spec, arch, digest, "all")
			if err != nil || hooks["/usr/bin/nvidia-cdi-hook"] != digest {
				t.Errorf("build %s: audit = %v, %v; want the spec admitted with the build's digest", row, hooks, err)
			}
		}
	}
	if _, err := orker.AuditNVIDIASpecForTest(t, fixture("nvidia-1.20.2-hookless.yaml"), arch, strings.Repeat("0", 64), "all"); err != nil {
		t.Errorf("a hookless spec asks nothing of the hook binary: %v", err)
	}
	// What it refuses: the stock spec (update-ldcache, enable-cuda-compat), a
	// build the table lacks (a binary with one byte more), and the next
	// hooks of nvidia-ctk's other rows.
	someDigest := ""
	for _, d := range rows {
		someDigest = d
	}
	if _, err := orker.AuditNVIDIASpecForTest(t, fixture("nvidia-1.20.2-stock.yaml"), arch, someDigest, "all"); err == nil || !strings.Contains(err.Error(), orker.DetailCDIHookRefused) {
		t.Errorf("the stock spec: %v, want %s", err, orker.DetailCDIHookRefused)
	}
	first := slices.Sorted(maps.Keys(binaries))[0]
	raw, err := os.ReadFile(binaries[first])
	if err != nil {
		t.Fatal(err)
	}
	patched := sha256.Sum256(append(raw, 0))
	if _, err := orker.AuditNVIDIASpecForTest(t, emulationSpec, arch, hex.EncodeToString(patched[:]), "all"); !errors.Is(err, orker.ErrHookBuildUnknown) {
		t.Errorf("a binary the table lacks: %v, want HOOK_BUILD_UNKNOWN", err)
	}
	if _, err := orker.AuditNVIDIASpecForTest(t, strings.Replace(emulationSpec, "create-symlinks, --link, \"libcuda.so.1::/usr/lib/libcuda.so\"", "update-ldcache, --folder, /usr/lib", 1), arch, someDigest, "all"); err == nil ||
		!strings.Contains(err.Error(), orker.DetailCDIHookRefused) {
		t.Errorf("update-ldcache: %v, want %s", err, orker.DetailCDIHookRefused)
	}

	// The real hooks, one dind for all builds: dockerd resolves the spec, the
	// hook of each build runs at create and makes its link, and the nodes and
	// the placeholder env arrive as the spec says.
	d := cdidind.Start(t, func(format string, args ...any) { infraAbsent(t, format, args...) })
	if d == nil {
		return
	}
	d.MakeNodes("nvidia0", "nvidiactl", "nvidia-uvm", "nvidia-uvm-tools")
	d.WriteSpec("nvidia.yaml", emulationSpec)
	for _, row := range slices.Sorted(maps.Keys(binaries)) {
		t.Run(row, func(t *testing.T) {
			d.InstallHook(binaries[row])
			out, err := d.Run([]string{"nvidia.com/gpu=all"}, charNodes("nvidia0", "nvidiactl", "nvidia-uvm", "nvidia-uvm-tools")+
				"; echo LINK=$(readlink /usr/lib/libcuda.so); echo VISIBLE=$NVIDIA_VISIBLE_DEVICES")
			if err != nil {
				t.Fatalf("docker run --device nvidia.com/gpu=all: %v\n%s", err, out)
			}
			for _, want := range []string{"CHAR=/dev/nvidia0\n", "CHAR=/dev/nvidiactl\n", "CHAR=/dev/nvidia-uvm\n", "CHAR=/dev/nvidia-uvm-tools\n", "LINK=libcuda.so.1\n", "VISIBLE=void\n"} {
				if !strings.Contains(out, want) {
					t.Errorf("the workload's output lacks %q:\n%s", want, out)
				}
			}
		})
	}

	// The rest runs against the last build installed above; the builds are
	// proven one by one by the loop.
	hookDigest := rows[slices.Sorted(maps.Keys(binaries))[len(binaries)-1]]

	// Resolution and merge: a second file of the kind adds a device the first
	// does not name, with edits of its own; dockerd resolves both files at
	// once, and the audit admits the pair.
	t.Run("resolution and merge", func(t *testing.T) {
		d.MakeNodes("nvidia1")
		second := `cdiVersion: 0.5.0
kind: nvidia.com/gpu
devices:
  - name: second
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia1, hostPath: /dev/nvidia1, permissions: rwm}
containerEdits:
  env:
    - NVIDIA_VISIBLE_DEVICES=void
`
		d.WriteSpec("nvidia-second.yaml", second)
		out, err := d.Run([]string{"nvidia.com/gpu=second", "nvidia.com/gpu=all"}, charNodes("nvidia1", "nvidia0")+"; echo LINK=$(readlink /usr/lib/libcuda.so)")
		if err != nil || !strings.Contains(out, "CHAR=/dev/nvidia1\n") || !strings.Contains(out, "CHAR=/dev/nvidia0\n") || !strings.Contains(out, "LINK=libcuda.so.1\n") {
			t.Fatalf("a container with both files' devices: %v\n%s", err, out)
		}
		if _, err := orker.AuditNVIDIASpecForTest(t, second, arch, hookDigest, "second"); err != nil {
			t.Errorf("the second file does not pass the audit: %v", err)
		}
		d.Exec("rm -f /etc/cdi/nvidia-second.yaml")
	})

	// Drift and reload: dockerd reads a changed spec at the next create with
	// no restart (so a change under a running Orker is real, not theoretical),
	// and the Orker's periodic audit sees each kind of change as drift.
	t.Run("drift and reload", func(t *testing.T) {
		changed := strings.Replace(emulationSpec, "libcuda.so.1::/usr/lib/libcuda.so", "libcuda.so.2::/usr/lib/libcuda.so", 1)
		d.WriteSpec("nvidia.yaml", changed)
		out, err := d.Run([]string{"nvidia.com/gpu=all"}, "echo LINK=$(readlink /usr/lib/libcuda.so)")
		if err != nil || !strings.Contains(out, "LINK=libcuda.so.2") {
			t.Fatalf("dockerd did not read the changed spec at the next create: %v\n%s", err, out)
		}
		d.WriteSpec("nvidia.yaml", emulationSpec)
		// Each mutation is one the audit admits, so the verdict is the Orker's
		// drift comparison and nothing else: no error, and drift exactly when
		// the spec's audited content differs.
		extraNode := strings.Replace(emulationSpec, "        - {path: /dev/nvidiactl,", "        - {path: /dev/nvidia1, hostPath: /dev/nvidia1, permissions: rwm}\n        - {path: /dev/nvidiactl,", 1)
		for name, tc := range map[string]struct {
			later string
			drift bool
		}{
			"the same spec":  {emulationSpec, false},
			"a changed link": {changed, true},
			"a node added":   {extraNode, true},
		} {
			got, err := orker.NVIDIASpecsDriftForTest(t, emulationSpec, tc.later, arch, hookDigest, hookDigest, "all")
			if err != nil || got != tc.drift {
				t.Errorf("%s: drift = %v, %v; want %v with no error", name, got, err, tc.drift)
			}
		}
		// A spec the rules refuse is the refusal's own detail, not drift.
		if _, err := orker.NVIDIASpecsDriftForTest(t, emulationSpec, strings.Replace(emulationSpec, "NVIDIA_VISIBLE_DEVICES=void", "NVIDIA_VISIBLE_DEVICES=all", 1), arch, hookDigest, hookDigest, "all"); err == nil ||
			!strings.Contains(err.Error(), orker.DetailCDIEditRefused) {
			t.Errorf("a spec the rules refuse: %v, want %s", err, orker.DetailCDIEditRefused)
		}
		// Another listed build of the hook is a changed binary.
		if len(rows) < 2 {
			t.Fatalf("the table lists %d builds for %s; the changed-binary case needs two", len(rows), arch)
		}
		others := 0
		for _, other := range rows {
			if other == hookDigest {
				continue
			}
			others++
			if got, err := orker.NVIDIASpecsDriftForTest(t, emulationSpec, emulationSpec, arch, hookDigest, other, "all"); err != nil || !got {
				t.Errorf("another listed build: drift = %v, %v; want drift", got, err)
			}
		}
		if others == 0 {
			t.Fatal("no build other than the one under test; the table's rows share a digest")
		}
	})

	// A non-runc default runtime is refused (design §4.10 item 2): the dind's
	// own dockerd answers runc, and a second dockerd started with another
	// default runtime (the same runc binary under another name, as an NVIDIA
	// runtime wrapper is registered) answers that name.
	t.Run("a non-runc default runtime is refused", func(t *testing.T) {
		facts := func(host string) (string, string, error) {
			return orker.RuntimeFactsForTest(d.Exec("docker " + host + " info --format '" + orker.RuntimeQueryForTest() + "'"))
		}
		name, path, err := facts("")
		if err != nil || orker.CheckDefaultRuntimeForTest(name, path) != nil {
			t.Fatalf("the dind's dockerd reports %q at %q (%v), which the rule must admit", name, path, err)
		}
		d.Exec(`RUNC=$(command -v runc) && mkdir -p /tmp/alt && (dockerd -H unix:///tmp/alt/docker.sock --data-root /tmp/alt/root --exec-root /tmp/alt/exec ` +
			`--pidfile /tmp/alt/pid --iptables=false --ip6tables=false --bridge=none --add-runtime ork-alt="$RUNC" --default-runtime=ork-alt >/tmp/alt/log 2>&1 &) ` +
			`&& for i in $(seq 60); do docker -H unix:///tmp/alt/docker.sock info >/dev/null 2>&1 && exit 0; sleep 1; done; cat /tmp/alt/log; exit 1`)
		altName, altPath, err := facts("-H unix:///tmp/alt/docker.sock")
		d.Exec("kill $(cat /tmp/alt/pid)")
		if err != nil {
			t.Fatal(err)
		}
		if altName != "ork-alt" {
			t.Fatalf("the second dockerd reports the default runtime %q, want ork-alt", altName)
		}
		if err := orker.CheckDefaultRuntimeForTest(altName, altPath); err == nil || !strings.Contains(err.Error(), "runc or crun") {
			t.Fatalf("a dockerd whose default runtime is %q at %q: %v, want a refusal", altName, altPath, err)
		}
		// The same binary under the name runc, as a wrapper is registered, is
		// refused by its path.
		if err := orker.CheckDefaultRuntimeForTest("runc", "/usr/bin/nvidia-container-runtime"); err == nil {
			t.Fatal("a runc that is the nvidia wrapper was admitted")
		}
	})

	// Hostile image symlinks (security M3): an image whose /usr/lib and /lib64
	// are symlinks out of the library directories, to / and /etc. The real
	// hook of every build resolves a link path inside the container's root, so
	// nothing is written to the dind's own filesystem, which the test checks
	// from outside the container.
	t.Run("hostile image symlinks write nothing outside the container", func(t *testing.T) {
		// /usr/lib is a link to /, /lib64 one to /etc, and /usr/lib64 an
		// absolute link to a directory the dind's own filesystem also has.
		d.Exec("mkdir -p /var/ork-escape; docker rm -f ork-hostile-src >/dev/null 2>&1; docker run --pull never --network none --name ork-hostile-src " + cdidind.WorkloadTag +
			" sh -c 'mkdir -p /var/ork-escape; rm -rf /usr/lib /lib64 /usr/lib64; ln -s / /usr/lib; ln -s /etc /lib64; ln -s /var/ork-escape /usr/lib64'" +
			" && docker commit ork-hostile-src ork-hostile:1 >/dev/null && docker rm ork-hostile-src >/dev/null")
		hostile := strings.Replace(emulationSpec, `"libcuda.so.1::/usr/lib/libcuda.so"]`,
			`"libcuda.so.1::/usr/lib/ork-hostile-one", --link, "libcuda.so.1::/lib64/ork-hostile-two", --link, "libcuda.so.1::/usr/lib64/ork-hostile-three"]`, 1)
		if hostile == emulationSpec {
			t.Fatal("the hostile spec was not made")
		}
		if _, err := orker.AuditNVIDIASpecForTest(t, hostile, arch, hookDigest, "all"); err != nil {
			t.Fatalf("the audit does not admit the hostile image's links, so the test proves nothing: %v", err)
		}
		d.WriteSpec("nvidia.yaml", hostile)
		for _, row := range slices.Sorted(maps.Keys(binaries)) {
			d.InstallHook(binaries[row])
			d.Exec("rm -rf /ork-hostile-one /etc/ork-hostile-two /var/ork-escape/*")
			out, err := d.RunImage("ork-hostile:1", []string{"nvidia.com/gpu=all"},
				"echo ONE=$(readlink /ork-hostile-one); echo TWO=$(readlink /etc/ork-hostile-two); echo THREE=$(readlink /var/ork-escape/ork-hostile-three)")
			// Positive control: the hook ran. Either it made its links inside the
			// container's root, which the container reads back, or it refused and
			// the failure names it; a run that did neither proves nothing.
			if err == nil {
				for _, want := range []string{"ONE=libcuda.so.1\n", "TWO=libcuda.so.1\n", "THREE=libcuda.so.1\n"} {
					if !strings.Contains(out, want) {
						t.Errorf("build %s: the hook did not make its link inside the container (%q missing):\n%s", row, want, out)
					}
				}
			} else if !strings.Contains(out, "nvidia-cdi-hook") {
				t.Errorf("build %s: the run failed, and not in the hook, so nothing was exercised: %v\n%s", row, err, out)
			}
			// Nothing outside the container's root, seen from the dind itself.
			check := d.Exec("for f in /ork-hostile-one /etc/ork-hostile-two /usr/lib/ork-hostile-one /lib64/ork-hostile-two; do test -e $f -o -L $f && echo ESCAPED=$f; done; " +
				"for f in /var/ork-escape/* /var/ork-escape/.[!.]*; do test -e \"$f\" -o -L \"$f\" && echo ESCAPED=$f; done; echo checked")
			if strings.Contains(check, "ESCAPED=") {
				t.Errorf("build %s: the hook wrote outside the container's root:\n%s", row, check)
			}
		}
		d.WriteSpec("nvidia.yaml", emulationSpec)
	})
}

// charNodes is a shell fragment that prints CHAR=<path> for each node that is a
// character device, one token per node and no substring to be matched by luck.
func charNodes(names ...string) string {
	var b strings.Builder
	for _, n := range names {
		b.WriteString("test -c /dev/" + n + " && echo CHAR=/dev/" + n + "; ")
	}
	return strings.TrimSuffix(b.String(), "; ")
}

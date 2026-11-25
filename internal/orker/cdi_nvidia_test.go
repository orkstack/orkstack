package orker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nvidiaKind = "nvidia.com/gpu"

// nvidiaFixture is testdata/cdi/<name>, captured with nvidia-ctk (its header
// says how), mapped from the mock root it was generated over to the paths a
// host has: NVIDIA's driver root /mock/driver is the host's /, and /mock/dev
// is /dev. nvidia-ctk writes hostPath as driver root plus path, so this is
// the spec it would write on a host whose driver files lie at those paths.
func nvidiaFixture(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "cdi", name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.NewReplacer("/mock/driver", "", "/mock/dev", "/dev").Replace(string(raw))
}

// knownTestBuild is a row of the compiled table the fake host's hook binary
// has the digest of: the 1.20.2 amd64 deb.
func knownTestBuild(t testing.TB) cdiHookBuild {
	t.Helper()
	for _, b := range cdiHookTable {
		if b.Version == "1.20.2" && b.Arch == "amd64" && b.Packaging == "deb" {
			return b
		}
	}
	t.Fatal("the table has no 1.20.2 amd64 deb row")
	return cdiHookBuild{}
}

const (
	nvidiaGID = 0
	// nvidiaMode is what the captured spec's fileMode (420) is: 0644.
	nvidiaMode os.FileMode = 0o644
)

// nvidiaTestHost is testCDIHost with the nodes of the captured specs (the
// majors nvidia-ctk wrote: 195 for nvidia, 511 for nvidia-uvm), the
// mount sources the specs name, and the hook binary of a table row.
func nvidiaTestHost(t testing.TB) cdiHost {
	t.Helper()
	h := testCDIHost()
	base := h.inspect
	nodes := map[string]hostDevice{
		"/dev/nvidiactl":        {Resolved: "/dev/nvidiactl", Char: true, Major: 195, Minor: 255, GID: nvidiaGID, Mode: nvidiaMode},
		"/dev/nvidia0":          {Resolved: "/dev/nvidia0", Char: true, Major: 195, Minor: 0, GID: nvidiaGID, Mode: nvidiaMode},
		"/dev/nvidia1":          {Resolved: "/dev/nvidia1", Char: true, Major: 195, Minor: 1, GID: nvidiaGID, Mode: nvidiaMode},
		"/dev/nvidia-modeset":   {Resolved: "/dev/nvidia-modeset", Char: true, Major: 195, Minor: 254, GID: nvidiaGID, Mode: nvidiaMode},
		"/dev/nvidia-uvm":       {Resolved: "/dev/nvidia-uvm", Char: true, Major: 511, Minor: 0, GID: nvidiaGID, Mode: nvidiaMode},
		"/dev/nvidia-uvm-tools": {Resolved: "/dev/nvidia-uvm-tools", Char: true, Major: 511, Minor: 1, GID: nvidiaGID, Mode: nvidiaMode},
		// A render group's node of mode 0660, as a udev rule gives one.
		"/dev/nvidia2": {Resolved: "/dev/nvidia2", Char: true, Major: 195, Minor: 2, GID: renderGID, Mode: 0o660},
	}
	h.inspect = func(p string) (hostDevice, error) {
		if d, ok := nodes[p]; ok {
			return d, nil
		}
		return base(p)
	}
	h.drivers[195] = "nvidia-frontend"
	h.drivers[511] = "nvidia-uvm"
	h.arch = "amd64"
	h.hookBinary = func(p string) (hostHook, error) {
		return hostHook{Resolved: p, SHA256: knownTestBuild(t).BinarySHA256}, nil
	}
	h.inspectMount = func(p string) (hostMount, error) {
		switch {
		case p == "/dev/null":
			return hostMount{Resolved: "/dev/null", Kind: mountDevice}, nil
		case strings.HasPrefix(p, "/run/") || strings.HasPrefix(p, "/var/run/"):
			return hostMount{Resolved: p, Kind: mountSocket}, nil
		case p == "/tmp/nvidia-mps" || strings.HasSuffix(p, "/dir"):
			return hostMount{Resolved: p, Kind: mountDir}, nil
		case strings.HasSuffix(p, "/absent"):
			return hostMount{}, fmt.Errorf("%s: %w", p, os.ErrNotExist)
		case strings.HasPrefix(p, "/lib/") || p == "/lib64" || strings.HasPrefix(p, "/lib64/"):
			// A merged-/usr host: /lib is /usr/lib.
			return hostMount{Resolved: "/usr" + p, Kind: mountFile}, nil
		case strings.HasSuffix(p, "/untrusted"):
			return hostMount{Resolved: p, Kind: mountFile, Untrusted: p + " is owned by uid 1000, not root"}, nil
		case strings.HasSuffix(p, "/link-out"):
			return hostMount{Resolved: "/opt/elsewhere/lib.so", Kind: mountFile}, nil
		}
		return hostMount{Resolved: p, Kind: mountFile}, nil
	}
	return h
}

// nvidiaAdmitted is the captured admitted spec's text.
func nvidiaAdmitted(t *testing.T) string { return nvidiaFixture(t, "nvidia-1.20.2-admitted.yaml") }

func auditNVIDIA(t *testing.T, body string, units ...string) (gpuSpecAudit, error) {
	t.Helper()
	if len(units) == 0 {
		units = []string{"all"}
	}
	return auditGPUSpecs([]string{specDir(t, map[string]string{"nvidia.yaml": body})}, nvidiaKind, units, nvidiaTestHost(t))
}

func TestAuditGPUSpecs_NVIDIAAdmitsWhatTheGeneratorWritesOnceFlagged(t *testing.T) {
	audit, err := auditNVIDIA(t, nvidiaAdmitted(t))
	if err != nil {
		t.Fatalf("auditGPUSpecs = %v", err)
	}
	if len(audit.units) != 1 {
		t.Fatalf("units = %+v", audit.units)
	}
	var paths, classes []string
	for _, n := range audit.units[0].nodes {
		paths, classes = append(paths, n.path), append(classes, n.class)
	}
	if want := "/dev/nvidia-uvm /dev/nvidia-uvm-tools /dev/nvidia0 /dev/nvidiactl"; strings.Join(paths, " ") != want {
		t.Errorf("nodes = %v, want %s", paths, want)
	}
	if want := "nvidia-uvm nvidia-uvm nvidia nvidia"; strings.Join(classes, " ") != want {
		t.Errorf("classes = %v, want %s", classes, want)
	}
	for _, n := range audit.units[0].nodes {
		if n.shared {
			t.Errorf("%s: management mode writes its nodes on the device, so none is shared", n.path)
		}
	}
	if len(audit.hooks) != 1 || audit.hooks[cdiHookBinary] != knownTestBuild(t).BinarySHA256 {
		t.Errorf("hooks = %v", audit.hooks)
	}
	// The mounts' sources are recorded as the host resolves them.
	if got := audit.mounts["/lib/x86_64-linux-gnu/libcuda.so.999.88.77"]; got != (auditedMountSource{Resolved: "/usr/lib/x86_64-linux-gnu/libcuda.so.999.88.77", Kind: mountFile}) {
		t.Errorf("mounts = %v", audit.mounts)
	}
}

func TestAuditGPUSpecs_NVIDIAHooklessSpecIsAdmitted(t *testing.T) {
	if _, err := auditNVIDIA(t, nvidiaFixture(t, "nvidia-1.20.2-hookless.yaml")); err != nil {
		t.Fatalf("auditGPUSpecs = %v", err)
	}
}

// TestAuditGPUSpecs_NVIDIAStockSpecIsRefused: the three things a stock spec
// carries that this release refuses, each by itself, with the others removed.
func TestAuditGPUSpecs_NVIDIAStockSpecIsRefused(t *testing.T) {
	stock := nvidiaFixture(t, "nvidia-1.20.2-stock.yaml")
	_, err := auditNVIDIA(t, stock)
	if err == nil || !strings.Contains(err.Error(), DetailCDIHookRefused) {
		t.Fatalf("the stock spec: %v, want %s", err, DetailCDIHookRefused)
	}
	// Without its hooks, the modeset node is what is left.
	noHooks := stock[:strings.Index(stock, "    hooks:")] + stock[strings.Index(stock, "    mounts:"):]
	_, err = auditNVIDIA(t, noHooks)
	if err == nil || !strings.Contains(err.Error(), DetailDeviceClassRefused) || !strings.Contains(err.Error(), "nvidia-modeset") {
		t.Fatalf("the stock spec without hooks: %v, want %s naming nvidia-modeset", err, DetailDeviceClassRefused)
	}
}

// edits is an NVIDIA spec of one unit, "0", whose device-level edits are
// the lines given, over the captured spec-level mounts of the admitted spec.
func nvidiaUnitSpec(t *testing.T, deviceEdits, specEdits string) string {
	t.Helper()
	return "cdiVersion: 0.5.0\nkind: nvidia.com/gpu\ndevices:\n  - name: \"0\"\n    containerEdits:\n" + deviceEdits + "\ncontainerEdits:\n" + specEdits
}

const nvidiaNodeEdits = `      deviceNodes:
        - path: /dev/nvidia0
          hostPath: /dev/nvidia0
          permissions: rwm
`

func TestAuditGPUSpecs_NVIDIAEditsRefusedAndAdmitted(t *testing.T) {
	mount := func(src, dst, opts string) string {
		return "  mounts:\n    - hostPath: " + src + "\n      containerPath: " + dst + "\n      options: [" + opts + "]\n"
	}
	roOpts := "ro, nosuid, nodev, rbind, rprivate"
	node := func(extra string) string {
		return "      deviceNodes:\n        - path: /dev/nvidia0\n          hostPath: /dev/nvidia0\n          permissions: rwm\n" + extra
	}
	for _, tc := range []struct {
		name   string
		dev    string
		spec   string
		detail string // "" admitted
		says   string
	}{
		// Q4c: rwm, and fileMode and gid only when they are the host node's own.
		{"rwm", nvidiaNodeEdits, "  env: [NVIDIA_VISIBLE_DEVICES=void]\n", "", ""},
		{"r, w and rw stay admitted", strings.Replace(nvidiaNodeEdits, "rwm", "rw", 1), "", "", ""},
		{"a type c", node("          type: c\n"), "", "", ""},
		{"fileMode equal to the node's", node("          fileMode: 420\n"), "", "", ""},
		{"fileMode that is not the node's", node("          fileMode: 438\n"), "", DetailCDIEditRefused, "fileMode"},
		{"gid equal to the node's", node("          gid: 0\n"), "", "", ""},
		{"gid that is not the node's", node("          gid: 987\n"), "", DetailCDIEditRefused, "gid"},
		{"a uid", node("          uid: 0\n"), "", DetailCDIEditRefused, "uid"},
		{"a permission outside r, w, rw, rwm", strings.Replace(nvidiaNodeEdits, "rwm", "m", 1), "", DetailCDIEditRefused, "permissions"},
		{"a block type", node("          type: b\n"), "", DetailCDIEditRefused, "type"},
		{"another vendor's gpu node", strings.Replace(nvidiaNodeEdits, "nvidia0", "kfd", 2), "", DetailDeviceClassRefused, "nvidia"},
		{"a render node", strings.Replace(nvidiaNodeEdits, "nvidia0", "dri/renderD128", 2), "", DetailDeviceClassRefused, "nvidia"},
		{"nvidia-modeset (the class stays out)", strings.Replace(nvidiaNodeEdits, "nvidia0", "nvidia-modeset", 2), "", DetailDeviceClassRefused, "nvidia-modeset"},

		// Env.
		{"the visible-devices placeholder", nvidiaNodeEdits, "  env:\n    - NVIDIA_VISIBLE_DEVICES=void\n    - NVIDIA_CTK_LIBCUDA_DIR=/lib/x86_64-linux-gnu\n", "", ""},
		{"visible devices that are not void", nvidiaNodeEdits, "  env: [NVIDIA_VISIBLE_DEVICES=all]\n", DetailCDIEditRefused, "NVIDIA_VISIBLE_DEVICES"},
		{"visible devices empty", nvidiaNodeEdits, "  env: [NVIDIA_VISIBLE_DEVICES=]\n", DetailCDIEditRefused, "NVIDIA_VISIBLE_DEVICES"},
		{"visible devices on the device", nvidiaNodeEdits + "      env: [NVIDIA_VISIBLE_DEVICES=0]\n", "", DetailCDIEditRefused, "NVIDIA_VISIBLE_DEVICES"},
		{"LD_PRELOAD", nvidiaNodeEdits, "  env: [LD_PRELOAD=/x.so]\n", DetailCDIEditRefused, "LD_PRELOAD"},
		{"LD_LIBRARY_PATH", nvidiaNodeEdits, "  env: [LD_LIBRARY_PATH=/tmp]\n", DetailCDIEditRefused, "LD_LIBRARY_PATH"},
		{"PATH", nvidiaNodeEdits, "  env: [PATH=/tmp]\n", DetailCDIEditRefused, "PATH"},
		{"an ORK_ variable", nvidiaNodeEdits, "  env: [ORK_X=1]\n", DetailCDIEditRefused, "ORK_X"},
		{"an OTEL_ variable", nvidiaNodeEdits, "  env: [OTEL_X=1]\n", DetailCDIEditRefused, "OTEL_X"},
		{"NVIDIA_DRIVER_CAPABILITIES", nvidiaNodeEdits, "  env: [NVIDIA_DRIVER_CAPABILITIES=all]\n", DetailCDIEditRefused, "NVIDIA_DRIVER_CAPABILITIES"},
		{"the empty-device marker", nvidiaNodeEdits, "  env: [NVCT_EMPTY_DEVICE=]\n", DetailCDIEditRefused, "NVCT_EMPTY_DEVICE"},
		{"a libcuda dir outside the library roots", nvidiaNodeEdits, "  env: [NVIDIA_CTK_LIBCUDA_DIR=/tmp]\n", DetailCDIEditRefused, "NVIDIA_CTK_LIBCUDA_DIR"},
		{"a libcuda dir list", nvidiaNodeEdits, "  env: [NVIDIA_CTK_LIBCUDA_DIR=/usr/lib64:/usr/lib/x86_64-linux-gnu]\n", "", ""},
		{"a repeated env", nvidiaNodeEdits, "  env: [NVIDIA_VISIBLE_DEVICES=void, NVIDIA_VISIBLE_DEVICES=void]\n", DetailCDIEditRefused, "twice"},

		// Mounts (Q4d, Q4e, Q4f).
		{"a driver library", nvidiaNodeEdits, mount("/usr/lib/x86_64-linux-gnu/libcuda.so.580.95.05", "/usr/lib/x86_64-linux-gnu/libcuda.so.580.95.05", roOpts), "", ""},
		{"a library reached through /lib", nvidiaNodeEdits, mount("/lib/x86_64-linux-gnu/libcuda.so.580.95.05", "/lib/x86_64-linux-gnu/libcuda.so.580.95.05", roOpts), "", ""},
		{"nvidia-smi in /usr/bin", nvidiaNodeEdits, mount("/usr/bin/nvidia-smi", "/usr/bin/nvidia-smi", roOpts), "", ""},
		{"a tool in /usr/sbin (Q4f)", nvidiaNodeEdits, mount("/usr/sbin/nvidia-persistenced", "/usr/sbin/nvidia-persistenced", roOpts), "", ""},
		{"a tool in /sbin (Q4f)", nvidiaNodeEdits, mount("/sbin/nvidia-persistenced", "/sbin/nvidia-persistenced", roOpts), "", ""},
		{"a directory in /usr/sbin", nvidiaNodeEdits, mount("/usr/sbin/dir", "/usr/sbin/dir", roOpts), DetailCDIEditRefused, "directory"},
		{"firmware", nvidiaNodeEdits, mount("/lib/firmware/nvidia/580.95.05/gsp_ga10x.bin", "/lib/firmware/nvidia/580.95.05/gsp_ga10x.bin", roOpts), "", ""},
		{"a vulkan icd", nvidiaNodeEdits, mount("/etc/vulkan/icd.d/nvidia_icd.json", "/etc/vulkan/icd.d/nvidia_icd.json", roOpts), "", ""},
		{"a share file", nvidiaNodeEdits, mount("/usr/share/glvnd/egl_vendor.d/10_nvidia.json", "/usr/share/glvnd/egl_vendor.d/10_nvidia.json", roOpts), "", ""},
		{"the void mount, exactly", nvidiaNodeEdits, mount("/dev/null", "/var/run/nvidia-container-devices/void", "ro, nosuid, nodev, bind"), "", ""},
		{"the void mount, writable", nvidiaNodeEdits, mount("/dev/null", "/var/run/nvidia-container-devices/void", "nosuid, nodev, bind"), DetailCDIEditRefused, "ro"},
		{"/dev/null anywhere else", nvidiaNodeEdits, mount("/dev/null", "/usr/lib/null", roOpts), DetailCDIEditRefused, "device"},
		{"another path as the void mount", nvidiaNodeEdits, mount("/usr/lib/x86_64-linux-gnu/libcuda.so.1", "/var/run/nvidia-container-devices/void", roOpts), DetailCDIEditRefused, "nvidia-container-devices"},
		{"the persistenced socket (Q4e)", nvidiaNodeEdits, mount("/run/nvidia-persistenced/socket", "/run/nvidia-persistenced/socket", roOpts), DetailCDIEditRefused, "socket"},
		{"the fabric manager socket (Q4e)", nvidiaNodeEdits, mount("/var/run/nvidia-fabricmanager/socket", "/var/run/nvidia-fabricmanager/socket", roOpts), DetailCDIEditRefused, "socket"},
		{"the MPS directory (Q4e)", nvidiaNodeEdits, mount("/tmp/nvidia-mps", "/tmp/nvidia-mps", roOpts), DetailCDIEditRefused, "nvidia-mps"},
		{"a source outside the roots", nvidiaNodeEdits, mount("/etc/shadow", "/usr/lib/shadow", roOpts), DetailCDIEditRefused, "outside"},
		{"/etc", nvidiaNodeEdits, mount("/etc", "/usr/lib/etc", roOpts), DetailCDIEditRefused, "outside"},
		{"the root", nvidiaNodeEdits, mount("/", "/usr/lib/root", roOpts), DetailCDIEditRefused, "outside"},
		{"a source that resolves out of the roots", nvidiaNodeEdits, mount("/usr/lib/link-out", "/usr/lib/link-out", roOpts), DetailCDIEditRefused, "outside"},
		{"a source that is not root's", nvidiaNodeEdits, mount("/usr/lib/untrusted", "/usr/lib/untrusted", roOpts), DetailCDIEditRefused, "not root's"},
		{"a spec-level and a device-level mount at one path", nvidiaNodeEdits + "      mounts:\n        - {hostPath: /usr/lib/y.so, containerPath: /usr/lib/x.so, options: [ro, nosuid, nodev]}\n", mount("/usr/lib/x.so", "/usr/lib/x.so", roOpts), DetailCDIEditRefused, "twice"},
		{"a source that is gone", nvidiaNodeEdits, mount("/usr/lib/absent", "/usr/lib/absent", roOpts), DetailCDIEditRefused, "absent"},
		{"a source with ..", nvidiaNodeEdits, mount("/usr/lib/../../etc/shadow", "/usr/lib/shadow", roOpts), DetailCDIEditRefused, "normal"},
		{"a relative source", nvidiaNodeEdits, mount("usr/lib/x.so", "/usr/lib/x.so", roOpts), DetailCDIEditRefused, "normal"},
		{"a source with a colon", nvidiaNodeEdits, mount("/usr/lib/a:b", "/usr/lib/a", roOpts), DetailCDIEditRefused, "normal"},
		{"a container path over /etc", nvidiaNodeEdits, mount("/usr/lib/x.so", "/etc/passwd", roOpts), DetailCDIEditRefused, "container path"},
		{"a container path over /proc", nvidiaNodeEdits, mount("/usr/lib/x.so", "/proc/sys/x", roOpts), DetailCDIEditRefused, "container path"},
		{"a container path over /dev", nvidiaNodeEdits, mount("/usr/lib/x.so", "/dev/nvidia0", roOpts), DetailCDIEditRefused, "container path"},
		{"a relative container path", nvidiaNodeEdits, mount("/usr/lib/x.so", "usr/lib/x.so", roOpts), DetailCDIEditRefused, "container path"},
		{"a mount without ro", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "nosuid, nodev, rbind"), DetailCDIEditRefused, "ro"},
		{"a mount without nosuid", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "ro, nodev, rbind"), DetailCDIEditRefused, "nosuid"},
		{"a mount without nodev", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "ro, nosuid, rbind"), DetailCDIEditRefused, "nodev"},
		{"a mount with exec granted by dev", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "ro, nosuid, nodev, dev"), DetailCDIEditRefused, "option"},
		{"a mount with rw", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "ro, nosuid, nodev, rw"), DetailCDIEditRefused, "option"},
		{"a mount with a propagation option", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", "ro, nosuid, nodev, rshared"), DetailCDIEditRefused, "option"},
		{"a typed mount", nvidiaNodeEdits, "  mounts:\n    - hostPath: /usr/lib/x.so\n      containerPath: /usr/lib/x.so\n      type: tmpfs\n      options: [ro, nosuid, nodev]\n", DetailCDIEditRefused, "type"},
		{"a mount twice at one path", nvidiaNodeEdits, mount("/usr/lib/x.so", "/usr/lib/x.so", roOpts) + "    - hostPath: /usr/lib/y.so\n      containerPath: /usr/lib/x.so\n      options: [ro, nosuid, nodev]\n", DetailCDIEditRefused, "twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auditNVIDIA(t, nvidiaUnitSpec(t, tc.dev, tc.spec), "0")
			switch {
			case tc.detail == "" && err != nil:
				t.Fatalf("auditGPUSpecs = %v, want admitted", err)
			case tc.detail != "" && (err == nil || !strings.Contains(err.Error(), tc.detail) || !strings.Contains(err.Error(), tc.says)):
				t.Fatalf("auditGPUSpecs = %v, want %s naming %q", err, tc.detail, tc.says)
			}
		})
	}
}

// TestAuditGPUSpecs_NVIDIAGroupsAndHooksOnADevice: a per-GPU hook is audited
// as a spec-level one is, and a group the node owns is admitted.
func TestAuditGPUSpecs_NVIDIAHooksOnTheDevice(t *testing.T) {
	good := nvidiaNodeEdits + "      hooks:\n        - hookName: createContainer\n          path: /usr/bin/nvidia-cdi-hook\n          args: [nvidia-cdi-hook, create-symlinks, --link, '../card1::/dev/dri/by-path/pci-0000:01:00.0-card']\n          env: [NVIDIA_CTK_DEBUG=false]\n"
	if _, err := auditNVIDIA(t, nvidiaUnitSpec(t, good, ""), "0"); err != nil {
		t.Fatalf("a by-path hook on the device: %v", err)
	}
	bad := strings.Replace(good, "create-symlinks", "update-ldcache", 1)
	if _, err := auditNVIDIA(t, nvidiaUnitSpec(t, bad, ""), "0"); err == nil || !strings.Contains(err.Error(), DetailCDIHookRefused) {
		t.Fatalf("update-ldcache on the device: %v", err)
	}
	// The apply-cuda-memory-limits row 1.20.1 writes per device (source-built;
	// no GPU here to capture one): createRuntime, refused.
	limits := nvidiaNodeEdits + "      hooks:\n        - hookName: createRuntime\n          path: /usr/bin/nvidia-cdi-hook\n          args: [nvidia-cdi-hook, apply-cuda-memory-limits, --driver-root, /, --gpu-id, GPU-0a1b2c3d-0000-4000-8000-000000000000]\n          env: [NVIDIA_CTK_DEBUG=false]\n"
	if _, err := auditNVIDIA(t, nvidiaUnitSpec(t, limits, ""), "0"); err == nil || !strings.Contains(err.Error(), DetailCDIHookRefused) ||
		!strings.Contains(err.Error(), "apply-cuda-memory-limits") {
		t.Fatalf("apply-cuda-memory-limits: %v", err)
	}
}

// TestAuditGPUSpecs_NVIDIASharedNodesAreNotAnOverlap: the control nodes every
// unit of a spec passes are shared; a unit's own node still belongs to one.
func TestAuditGPUSpecs_NVIDIASharedNodesAreNotAnOverlap(t *testing.T) {
	spec := `cdiVersion: 0.5.0
kind: nvidia.com/gpu
devices:
  - name: "0"
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia0, hostPath: /dev/nvidia0, permissions: rwm}
  - name: "1"
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia1, hostPath: /dev/nvidia1, permissions: rwm}
containerEdits:
  deviceNodes:
    - {path: /dev/nvidiactl, hostPath: /dev/nvidiactl, permissions: rwm}
    - {path: /dev/nvidia-uvm, hostPath: /dev/nvidia-uvm, permissions: rwm}
`
	audit, err := auditNVIDIA(t, spec, "0", "1")
	if err != nil {
		t.Fatalf("auditGPUSpecs = %v", err)
	}
	if len(audit.units) != 2 || len(audit.units[0].nodes) != 3 || len(audit.units[1].nodes) != 3 {
		t.Fatalf("units = %+v", audit.units)
	}
	// Two units naming one node of their own are still an overlap.
	twice := strings.Replace(spec, "path: /dev/nvidia1, hostPath: /dev/nvidia1", "path: /dev/nvidia0, hostPath: /dev/nvidia0", 1)
	if _, err := auditNVIDIA(t, twice, "0", "1"); err == nil || !strings.Contains(err.Error(), DetailUnitsOverlap) {
		t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailUnitsOverlap)
	}
	// A group of the node's own is admitted with the nvidia rules too.
	grp := strings.Replace(strings.Replace(spec, "cdiVersion: 0.5.0", "cdiVersion: 0.7.0", 1), "{path: /dev/nvidia1, hostPath: /dev/nvidia1, permissions: rwm}", "{path: /dev/nvidia2, hostPath: /dev/nvidia2, permissions: rwm, fileMode: 432, gid: 987}\n      additionalGids: [987]", 1)
	if _, err := auditNVIDIA(t, grp, "0", "1"); err != nil {
		t.Fatalf("a gid and a fileMode that are the node's: %v", err)
	}
}

// TestAuditGPUSpecs_OthersStayHookless: the rules above are for nvidia.com
// kinds alone. Every edit an NVIDIA spec may make is still refused under
// another kind (design §4.10 item 4: AMD, Intel and every real-device-gate
// spec are hookless).
func TestAuditGPUSpecs_OthersStayHookless(t *testing.T) {
	type edit struct{ spec, unit string }
	for name, tc := range map[string]edit{
		"hooks":              {nvidiaFixture(t, "nvidia-1.20.2-admitted.yaml"), "all"},
		"spec-level edits":   {nvidiaFixture(t, "nvidia-1.20.2-hookless.yaml"), "all"},
		"a mount":            {nvidiaUnitSpec(t, nvidiaNodeEdits, "  mounts:\n    - {hostPath: /usr/lib/x.so, containerPath: /usr/lib/x.so, options: [ro, nosuid, nodev]}\n"), "0"},
		"the void env":       {nvidiaUnitSpec(t, nvidiaNodeEdits, "  env: [NVIDIA_VISIBLE_DEVICES=void]\n"), "0"},
		"rwm on a node":      {nvidiaUnitSpec(t, nvidiaNodeEdits, ""), "0"},
		"a hook on a device": {nvidiaUnitSpec(t, nvidiaNodeEdits+"      hooks:\n        - {hookName: createContainer, path: /usr/bin/nvidia-cdi-hook, args: [nvidia-cdi-hook, disable-device-node-modification], env: [NVIDIA_CTK_DEBUG=false]}\n", ""), "0"},
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(tc.spec, "nvidia.com/gpu", "amd.com/gpu", 1)
			_, err := auditGPUSpecs([]string{specDir(t, map[string]string{"a.yaml": body})}, "amd.com/gpu", []string{tc.unit}, nvidiaTestHost(t))
			if err == nil || !strings.Contains(err.Error(), DetailCDIEditRefused) {
				t.Fatalf("auditGPUSpecs under amd.com/gpu = %v, want %s", err, DetailCDIEditRefused)
			}
		})
	}
}

// TestSameGPUAudit_HooksAndMountsAreDrift: the periodic audit compares the
// hook binaries' digests and the mounts' resolved sources.
func TestSameGPUAudit_HooksAndMountsAreDrift(t *testing.T) {
	audit, err := auditNVIDIA(t, nvidiaAdmitted(t))
	if err != nil {
		t.Fatal(err)
	}
	same := func(a, b gpuSpecAudit) bool {
		return sameGPUAudit(profileAudit{gpu: &a}, profileAudit{gpu: &b})
	}
	clone := audit
	if !same(audit, clone) {
		t.Fatal("an audit differs from itself")
	}
	moved := audit
	moved.hooks = map[string]string{cdiHookBinary: strings.Repeat("0", 64)}
	if same(audit, moved) {
		t.Error("a changed hook binary is not drift")
	}
	moved = audit
	moved.mounts = map[string]auditedMountSource{}
	for k := range audit.mounts {
		moved.mounts[k] = auditedMountSource{Resolved: "/usr/lib/elsewhere", Kind: mountFile}
	}
	if same(audit, moved) {
		t.Error("a mount source that resolves elsewhere is not drift")
	}
}

// TestNVIDIASpec_NullEditsAreRefused: an empty entry of an edits list, at the
// spec's level or a device's, is refused by the audit and by the pre-create
// scan of the directories, never a panic: an NVIDIA spec's level edits are
// read too, which a hookless spec's are not.
func TestNVIDIASpec_NullEditsAreRefused(t *testing.T) {
	for _, list := range []string{"deviceNodes", "netDevices", "hooks", "mounts"} {
		for name, body := range map[string]string{
			"device": "cdiVersion: 0.7.0\nkind: nvidia.com/gpu\ndevices:\n  - name: \"0\"\n    containerEdits:\n      " + list + ":\n        -\n",
			"spec": "cdiVersion: 0.7.0\nkind: nvidia.com/gpu\ndevices:\n  - name: \"0\"\n    containerEdits:\n      env:\n        - A=b\n" +
				"containerEdits:\n  " + list + ":\n    -\n",
		} {
			t.Run(list+" "+name, func(t *testing.T) {
				dir := specDir(t, map[string]string{"nvidia.yaml": body})
				_, err := auditGPUSpecs([]string{dir}, "nvidia.com/gpu", []string{"0"}, nvidiaTestHost(t))
				if err == nil || !strings.Contains(err.Error(), DetailCDISpecInvalid) || !strings.Contains(err.Error(), list+" has an empty entry") {
					t.Fatalf("auditGPUSpecs = %v, want %s naming %s", err, DetailCDISpecInvalid, list)
				}
				if _, err := specFilesOfKind([]string{dir}, "nvidia.com/gpu"); err == nil {
					t.Fatal("specFilesOfKind read a spec with an empty edit")
				}
			})
		}
	}
}

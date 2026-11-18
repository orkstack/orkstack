package orker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const renderGID = 987

// testCDIHost is a host with AMD's renderD128 and kfd and Intel's renderD129,
// all of the render group, and trusted spec directories.
func testCDIHost() cdiHost {
	nodes := map[string]hostDevice{
		"/dev/dri/renderD128": {Resolved: "/dev/dri/renderD128", Char: true, Major: 226, Minor: 128, GID: renderGID},
		"/dev/dri/renderD129": {Resolved: "/dev/dri/renderD129", Char: true, Major: 226, Minor: 129, GID: renderGID},
		"/dev/kfd":            {Resolved: "/dev/kfd", Char: true, Major: 234, Minor: 0, GID: renderGID},
		"/dev/dri/card0":      {Resolved: "/dev/dri/card0", Char: true, Major: 226, Minor: 0, GID: 985},
		"/dev/sda":            {Resolved: "/dev/sda", Char: false, Major: 8, Minor: 0, GID: 6},
	}
	return cdiHost{
		inspect: func(p string) (hostDevice, error) {
			if d, ok := nodes[p]; ok {
				return d, nil
			}
			return hostDevice{}, fmt.Errorf("%s: %w", p, os.ErrNotExist)
		},
		drivers:   map[uint32]string{226: "drm", 234: "kfd", 8: "sd"},
		groupName: func(gid uint32) string { return map[uint32]string{renderGID: "render", 6: "disk", 0: "root"}[gid] },
		forbiddenFor: func(gid uint32) (string, bool) {
			if gid == 6 {
				return "/dev/sda, a block device", true
			}
			return "", false
		},
		trustedDir: func(string) error { return nil },
	}
}

const amdSpec = `cdiVersion: 0.7.0
kind: test.orkstack.dev/gpu-1a2b3c4d
devices:
  - name: amd
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD128
          hostPath: /dev/dri/renderD128
          type: c
          permissions: rw
        - path: /dev/kfd
          hostPath: /dev/kfd
          type: c
          permissions: rw
      additionalGids:
        - 987
  - name: intel
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD129
          hostPath: /dev/dri/renderD129
          type: c
          permissions: rw
      additionalGids:
        - 987
`

const testKind = "test.orkstack.dev/gpu-1a2b3c4d"

func specDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestAuditGPUSpecs_AcceptsTheHookless(t *testing.T) {
	dir := specDir(t, map[string]string{"gpu.yaml": amdSpec, "other.yaml": strings.ReplaceAll(amdSpec, testKind, "other.example/gpu")})
	audit, err := auditGPUSpecs([]string{dir}, testKind, []string{"amd", "intel"}, testCDIHost())
	if err != nil {
		t.Fatalf("auditGPUSpecs = %v", err)
	}
	if len(audit.units) != 2 || len(audit.units[0].nodes) != 2 || audit.units[0].nodes[1].class != "kfd" ||
		audit.units[1].nodes[0].class != "drm-render" {
		t.Fatalf("audit = %+v", audit)
	}
	if len(audit.files) != 1 {
		t.Fatalf("audit set = %v, want only the file declaring the kind", audit.files)
	}
	if got := audit.units[0].gids; len(got) != 1 || got[0] != renderGID {
		t.Fatalf("gids = %v", got)
	}
}

// TestAuditGPUSpecs_DockerdsValidatorDecides: the library dockerd runs
// rejects what dockerd rejects, such as additionalGids under 0.6.0 (spike v1).
func TestAuditGPUSpecs_DockerdsValidatorDecides(t *testing.T) {
	for name, body := range map[string]string{
		"additionalGids under 0.6.0": strings.Replace(amdSpec, "0.7.0", "0.6.0", 1),
		"an unknown field":           strings.Replace(amdSpec, "kind:", "colour: red\nkind:", 1),
		"an unknown major version":   strings.Replace(amdSpec, "0.7.0", "9.0.0", 1),
		"not YAML":                   "{{",
	} {
		t.Run(name, func(t *testing.T) {
			dir := specDir(t, map[string]string{"gpu.yaml": body})
			_, err := auditGPUSpecs([]string{dir}, testKind, []string{"amd", "intel"}, testCDIHost())
			if err == nil || !strings.Contains(err.Error(), DetailCDISpecInvalid) {
				t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailCDISpecInvalid)
			}
		})
	}
}

func TestAuditGPUSpecs_RefusesWhatTheRulesDo(t *testing.T) {
	edit := func(from, to string) string { return strings.Replace(amdSpec, from, to, 1) }
	intelOnly := `cdiVersion: 0.7.0
kind: ` + testKind + `
devices:
  - name: intel
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD129
          hostPath: /dev/dri/renderD129
          type: c
          permissions: rw
`
	for _, tc := range []struct {
		name   string
		files  map[string]string
		units  []string
		detail string
	}{
		{"a hook", map[string]string{"g.yaml": edit("      additionalGids:\n        - 987\n  - name: intel",
			"      hooks:\n        - hookName: createContainer\n          path: /usr/bin/true\n  - name: intel")}, []string{"amd"}, DetailCDIEditRefused},
		{"an env", map[string]string{"g.yaml": edit("      additionalGids:\n        - 987\n  - name: intel",
			"      env:\n        - LD_PRELOAD=/x.so\n  - name: intel")}, []string{"amd"}, DetailCDIEditRefused},
		{"a mount", map[string]string{"g.yaml": edit("      additionalGids:\n        - 987\n  - name: intel",
			"      mounts:\n        - hostPath: /etc\n          containerPath: /etc\n  - name: intel")}, []string{"amd"}, DetailCDIEditRefused},
		{"no hostPath", map[string]string{"g.yaml": edit("          hostPath: /dev/dri/renderD128\n", "")}, []string{"amd"}, DetailCDIEditRefused},
		{"no type", map[string]string{"g.yaml": edit("          type: c\n          permissions: rw\n        - path: /dev/kfd", "          permissions: rw\n        - path: /dev/kfd")}, []string{"amd"}, DetailCDIEditRefused},
		{"a uid", map[string]string{"g.yaml": edit("          permissions: rw\n        - path: /dev/kfd", "          permissions: rw\n          uid: 0\n        - path: /dev/kfd")}, []string{"amd"}, DetailCDIEditRefused},
		{"mknod permission", map[string]string{"g.yaml": edit("          permissions: rw\n        - path: /dev/kfd", "          permissions: rwm\n        - path: /dev/kfd")}, []string{"amd"}, DetailCDIEditRefused},
		{"a major that is not the node's", map[string]string{"g.yaml": edit("          type: c\n          permissions: rw\n        - path: /dev/kfd", "          type: c\n          major: 1\n          permissions: rw\n        - path: /dev/kfd")}, []string{"amd"}, DetailCDIEditRefused},
		{"a card node", map[string]string{"g.yaml": strings.ReplaceAll(intelOnly, "renderD129", "card0")}, []string{"intel"}, DetailDeviceClassRefused},
		{"a missing node", map[string]string{"g.yaml": strings.ReplaceAll(intelOnly, "renderD129", "renderD200")}, []string{"intel"}, DetailDeviceClassRefused},
		{"gid 0", map[string]string{"g.yaml": strings.ReplaceAll(amdSpec, "- 987", "- 0")}, []string{"amd"}, DetailCDIGroupRefused},
		{"a gid that owns no node", map[string]string{"g.yaml": strings.ReplaceAll(amdSpec, "- 987", "- 6")}, []string{"amd"}, DetailCDIGroupRefused},
		{"a unit no spec declares", map[string]string{"g.yaml": intelOnly}, []string{"amd"}, DetailCDIUnitUnresolved},
		{"a unit two files declare", map[string]string{"a.yaml": intelOnly, "b.yaml": intelOnly}, []string{"intel"}, DetailCDIUnitUnresolved},
		{"spec-level edits", map[string]string{"g.yaml": strings.Replace(intelOnly, "devices:", "containerEdits:\n  additionalGids:\n    - 987\ndevices:", 1)}, []string{"intel"}, DetailCDIEditRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auditGPUSpecs([]string{specDir(t, tc.files)}, testKind, tc.units, testCDIHost())
			if err == nil || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("auditGPUSpecs = %v, want %s", err, tc.detail)
			}
		})
	}
}

// TestAuditGPUSpecs_UnitsOverlap: two units sharing a node (/dev/kfd) are
// refused (O13: AMD is one unit per host).
func TestAuditGPUSpecs_UnitsOverlap(t *testing.T) {
	body := strings.Replace(amdSpec, "path: /dev/dri/renderD129\n          hostPath: /dev/dri/renderD129",
		"path: /dev/kfd\n          hostPath: /dev/kfd", 1)
	_, err := auditGPUSpecs([]string{specDir(t, map[string]string{"g.yaml": body})}, testKind, []string{"amd", "intel"}, testCDIHost())
	if err == nil || !strings.Contains(err.Error(), DetailUnitsOverlap) {
		t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailUnitsOverlap)
	}
}

// TestAuditGPUSpecs_NoSpecIsAbsent: no file declaring the kind is
// errCDISpecAbsent, which boot holds as SPEC_ABSENT, not a refusal.
func TestAuditGPUSpecs_NoSpecIsAbsent(t *testing.T) {
	_, err := auditGPUSpecs([]string{specDir(t, nil)}, testKind, []string{"amd"}, testCDIHost())
	if !errors.Is(err, errCDISpecAbsent) {
		t.Fatalf("auditGPUSpecs = %v, want errCDISpecAbsent", err)
	}
}

// TestAuditGPUSpecs_UntrustedDirectory: a spec directory a non-root user
// could write refuses the boot.
func TestAuditGPUSpecs_UntrustedDirectory(t *testing.T) {
	h := testCDIHost()
	h.trustedDir = func(dir string) error { return fmt.Errorf("%s is writable by its group", dir) }
	_, err := auditGPUSpecs([]string{specDir(t, map[string]string{"g.yaml": amdSpec})}, testKind, []string{"amd"}, h)
	if err == nil || !strings.Contains(err.Error(), DetailCDISpecDirUntrusted) {
		t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailCDISpecDirUntrusted)
	}
}

// FuzzAuditGPUSpecs: whatever a spec file holds, the audit never panics; it
// either refuses with an error or accepts a unit whose every node is of a gpu
// class, passed with r, w or rw, and whose every group owns one of them.
func FuzzAuditGPUSpecs(f *testing.F) {
	f.Add(amdSpec)
	f.Add(strings.Replace(amdSpec, "0.7.0", "0.6.0", 1))
	f.Add(strings.ReplaceAll(amdSpec, "- 987", "- 0"))
	f.Add("cdiVersion: 0.7.0\nkind: " + testKind + "\ndevices: []\n")
	f.Fuzz(func(t *testing.T, body string) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "g.yaml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		audit, err := auditGPUSpecs([]string{dir}, testKind, []string{"amd"}, testCDIHost())
		if err != nil {
			return
		}
		if len(audit.units) != 1 {
			t.Fatalf("accepted with %d units", len(audit.units))
		}
		for _, n := range audit.units[0].nodes {
			if n.class != "drm-render" && n.class != "kfd" {
				t.Fatalf("accepted a node of class %q", n.class)
			}
			if n.permissions != "r" && n.permissions != "w" && n.permissions != "rw" {
				t.Fatalf("accepted permissions %q", n.permissions)
			}
		}
		for _, gid := range audit.units[0].gids {
			if gid == 0 || !slices.ContainsFunc(audit.units[0].nodes, func(n auditedNode) bool { return n.host.GID == gid }) {
				t.Fatalf("accepted gid %d", gid)
			}
		}
	})
}

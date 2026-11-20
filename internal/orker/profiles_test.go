package orker

import (
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const testDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// validProfilesDoc declares one profile of every kind, in the shape
// docs/DEPLOY.md gives.
const validProfilesDoc = `profiles:
  probe_image: ghcr.io/orkstack/orker@` + testDigest + `
  audit_interval: 90s
  recovery_interval: 3s
  amd:
    kind: gpu
    revision: mesa-25.2
    cdi:
      kind: amd.com/gpu
      units:
        - "0"
  sensor:
    kind: devices
    devices:
      - path: /dev/serial/by-id/usb-FTDI_FT232R-if00-port0
        container_path: /dev/sensor
        permissions: rw
`

func TestParseProfiles_Valid(t *testing.T) {
	got, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	if got.ProbeImage != "ghcr.io/orkstack/orker@"+testDigest {
		t.Errorf("probe image = %q", got.ProbeImage)
	}
	if got.AuditInterval != 90*time.Second || got.RecoveryInterval != 3*time.Second {
		t.Errorf("intervals = %s, %s", got.AuditInterval, got.RecoveryInterval)
	}
	names := make([]string, 0, len(got.List))
	for _, p := range got.List {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "amd,sensor" {
		t.Fatalf("profiles are sorted by name; got %v", names)
	}
	amd, sensor := got.List[0], got.List[1]
	if amd.Kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU || amd.GPU == nil ||
		amd.GPU.CDIKind != "amd.com/gpu" || len(amd.GPU.Units) != 1 || amd.Revision != "mesa-25.2" {
		t.Errorf("gpu profile = %+v", amd)
	}
	if sensor.Kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES || len(sensor.Devices) != 1 ||
		sensor.Devices[0].ContainerPath != "/dev/sensor" || sensor.Devices[0].Permissions != "rw" {
		t.Errorf("devices profile = %+v", sensor)
	}
}

func TestParseProfiles_Defaults(t *testing.T) {
	got, err := ParseProfiles([]byte("profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.AuditInterval != DefaultProfileAuditInterval || got.RecoveryInterval != DefaultProfileRecoveryInterval {
		t.Errorf("defaults = %s, %s", got.AuditInterval, got.RecoveryInterval)
	}
}

func TestParseProfiles_EmptyIsNone(t *testing.T) {
	for _, doc := range []string{"", "  \n", "profiles: {}\n"} {
		got, err := ParseProfiles([]byte(doc))
		if err != nil {
			t.Errorf("%q: %v", doc, err)
			continue
		}
		if len(got.List) != 0 {
			t.Errorf("%q: %d profiles", doc, len(got.List))
		}
	}
}

func TestParseProfiles_Refused(t *testing.T) {
	gpu := func(body string) string {
		return "profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n" + body
	}
	tests := []struct {
		name, doc, want string
	}{
		{"probe image by tag", strings.Replace(validProfilesDoc, "orker@"+testDigest, "orker:0.4.0", 1), "probe_image"},
		{"unknown top key", "orkers: {}\n", "orkers"},
		{"second document", validProfilesDoc + "---\nprofiles: {}\n", "second YAML document"},
		{"anchor", gpu("  a: &x\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n  b: *x\n"), "alias"},
		{"name not a slug", gpu("  Amd:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n"), "Amd"},
		{"unknown kind", gpu("  x:\n    kind: fpga\n"), "kind"},
		{"missing kind", gpu("  x:\n    revision: a\n"), "kind"},
		{"unknown profile key", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n    privileged: true\n"), "privileged"},
		{"gpu all units", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - all\n"), "all"},
		{"gpu no units", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units: []\n"), "units"},
		{"gpu duplicate unit", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n        - \"0\"\n"), "duplicate"},
		{"gpu bad cdi kind", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd\n      units:\n        - \"0\"\n"), "cdi.kind"},
		{"gpu nvidia", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: nvidia.com/gpu\n      units:\n        - \"0\"\n"), "nvidia.com"},
		{"two gpu profiles", gpu("  a:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n  b:\n    kind: gpu\n    cdi:\n      kind: intel.com/gpu\n      units:\n        - \"0\"\n"), "one gpu profile"},
		{"gpu with devices key", gpu("  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n    devices: []\n"), "devices"},
		{"devices empty", gpu("  x:\n    kind: devices\n    devices: []\n"), "1 to 16"},
		{"devices bad perms", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n        permissions: rwm\n"), "permissions"},
		{"devices missing perms", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n"), "permissions"},
		{"devices path outside dev", gpu("  x:\n    kind: devices\n    devices:\n      - path: /etc/shadow\n        container_path: /dev/s\n        permissions: r\n"), "/dev"},
		{"devices path with colon", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/tty:USB0\n        container_path: /dev/s\n        permissions: r\n"), "path"},
		{"devices path traversal", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/../etc/shadow\n        container_path: /dev/s\n        permissions: r\n"), "path"},
		{"devices container path reserved", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/null\n        permissions: r\n"), "container_path"},
		{"devices container path under shm", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/shm/x\n        permissions: r\n"), "container_path"},
		{"devices container path dup", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n        permissions: r\n      - path: /dev/ttyUSB1\n        container_path: /dev/s\n        permissions: r\n"), "duplicate"},
		{"devices container path nested", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n        permissions: r\n      - path: /dev/ttyUSB1\n        container_path: /dev/s/t\n        permissions: r\n"), "nest"},
		{"devices container path under null", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/null/x\n        permissions: r\n"), "container_path"},
		{"devices with null cdi key", gpu("  x:\n    kind: devices\n    cdi: null\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n        permissions: r\n"), "cdi"},
		{"merge key", gpu("  x:\n    <<: {kind: gpu}\n"), "merge key"},
		{"explicit tag", gpu("  x:\n    kind: !!str gpu\n"), "tag"},
		{"alias only", gpu("  x: *nothing\n"), ""},
		{"written but empty", "profiles:\n", "declares nothing"},
		{"null", "profiles: null\n", "declares nothing"},
		{"comment only", "# profiles: {}\n", "declares nothing"},
		{"devices host memory", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/mem\n        container_path: /dev/s\n        permissions: r\n"), "/dev/mem"},
		{"devices block tree", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/disk/by-id/x\n        container_path: /dev/s\n        permissions: r\n"), "/dev/disk"},
		{"devices host path twice", gpu("  x:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/a\n        permissions: r\n      - path: /dev/ttyUSB0\n        container_path: /dev/b\n        permissions: rw\n"), "declared twice"},
		{"bad interval", strings.Replace(validProfilesDoc, "audit_interval: 90s", "audit_interval: soon", 1), "audit_interval"},
		{"zero interval", strings.Replace(validProfilesDoc, "recovery_interval: 3s", "recovery_interval: 0s", 1), "recovery_interval"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProfiles([]byte(tt.doc))
			if err == nil {
				t.Fatalf("accepted; want an error mentioning %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

func TestCheckProfileKindsSupported_RefusesWhatThisBuildLacks(t *testing.T) {
	got, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatal(err)
	}
	// This build implements both kinds and advertises both.
	if !slices.Equal(SupportedProfileKinds(), []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, orkstackv1.ProfileKind_PROFILE_KIND_GPU}) {
		t.Fatalf("SupportedProfileKinds = %v; this build implements devices and gpu", SupportedProfileKinds())
	}
	if err := got.CheckSupported(SupportedProfileKinds()); err != nil {
		t.Fatalf("CheckSupported of this build's own kinds = %v", err)
	}
	// A build that lacked gpu would refuse a gpu profile, naming it, rather
	// than ignore it silently.
	err = got.CheckSupported([]orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES})
	if err == nil || !strings.Contains(err.Error(), "amd") || !strings.Contains(err.Error(), "gpu") || strings.Contains(err.Error(), "sensor") {
		t.Fatalf("CheckSupported = %v; want a refusal naming profile amd and kind gpu, and not sensor", err)
	}
	none, _ := ParseProfiles(nil)
	if err := none.CheckSupported(nil); err != nil {
		t.Fatalf("no profiles: %v", err)
	}
	if err := got.CheckSupported([]orkstackv1.ProfileKind{
		orkstackv1.ProfileKind_PROFILE_KIND_GPU, orkstackv1.ProfileKind_PROFILE_KIND_DEVICES,
	}); err != nil {
		t.Fatalf("every kind supported: %v", err)
	}
}

// Structural identities are what a run pins and what every Orker of a pool
// must share (design §4.6). The goldens pin the bytes: a change to them is a
// change every running fleet sees as a profile mismatch.
func TestProfileIdentity_Golden(t *testing.T) {
	got, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Profile{}
	for _, p := range got.List {
		byName[p.Name] = p
	}
	usb := func(string) (string, error) { return "usb-serial", nil }
	want := map[string]string{
		"amd":    `{"cdi_kind":"amd.com/gpu","kind":"gpu","name":"amd"}`,
		"sensor": `{"devices":[{"class":"usb-serial","container_path":"/dev/sensor","permissions":"rw"}],"kind":"devices","name":"sensor"}`,
	}
	for name, canonical := range want {
		id, err := byName[name].StructuralIdentity(usb)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if id != sha256Hex([]byte(canonical)) {
			t.Errorf("%s identity = %s; want sha256(%s)", name, id, canonical)
		}
	}
}

func TestProfileIdentity_DevicesNeedTheirClass(t *testing.T) {
	got, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range got.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES {
			continue
		}
		if _, err := p.StructuralIdentity(nil); err == nil {
			t.Error("a devices profile's identity was computed without classifying its devices")
		}
	}
}

func TestProfileIdentity_WhatChangesIt(t *testing.T) {
	usb := func(string) (string, error) { return "usb-serial", nil }
	identity := func(doc string, name string) string {
		t.Helper()
		parsed, err := ParseProfiles([]byte(doc))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		for _, p := range parsed.List {
			if p.Name == name {
				id, err := p.StructuralIdentity(usb)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
		}
		t.Fatalf("no profile %s", name)
		return ""
	}
	tests := []struct {
		name, profile, from, to string
		changes                 bool
	}{
		{"revision is memo-only", "amd", "revision: mesa-25.2", "revision: mesa-26", false},
		{"units are not identity", "amd", "        - \"0\"\n  sensor", "        - \"0\"\n        - \"1\"\n  sensor", false},
		{"cdi kind is identity", "amd", "kind: amd.com/gpu", "kind: intel.com/gpu", true},
		{"host path is not identity", "sensor", "usb-FTDI_FT232R-if00-port0", "usb-other", false},
		{"permissions are identity", "sensor", "permissions: rw", "permissions: r", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(validProfilesDoc, tt.from) {
				t.Fatalf("fixture lacks %q", tt.from)
			}
			before := identity(validProfilesDoc, tt.profile)
			after := identity(strings.Replace(validProfilesDoc, tt.from, tt.to, 1), tt.profile)
			if (before != after) != tt.changes {
				t.Errorf("identity changed = %v; want %v", before != after, tt.changes)
			}
		})
	}
}

func TestProfileMemoComponent(t *testing.T) {
	memo := func(entries []ProfileMemoEntry, gpu int32) string {
		t.Helper()
		got, err := ProfileMemoComponent(entries, gpu)
		if err != nil {
			t.Fatal(err)
		}
		return string(got)
	}
	a := memo([]ProfileMemoEntry{{Name: "amd", Identity: "aa", Revision: "r1"}}, 1)
	if a != `{"gpu":1,"profiles":[{"identity":"aa","name":"amd","revision":"r1"}]}` {
		t.Errorf("memo component = %s", a)
	}
	if memo([]ProfileMemoEntry{{Name: "amd", Identity: "aa", Revision: "r2"}}, 1) == a {
		t.Error("revision does not change the memo component")
	}
	if memo([]ProfileMemoEntry{{Name: "amd", Identity: "aa", Revision: "r1"}}, 2) == a {
		t.Error("the gpu count does not change the memo component")
	}
	// Order of the task's list does not matter: the component sorts by name.
	x := memo([]ProfileMemoEntry{{Name: "b", Identity: "2"}, {Name: "a", Identity: "1"}}, 0)
	y := memo([]ProfileMemoEntry{{Name: "a", Identity: "1"}, {Name: "b", Identity: "2"}}, 0)
	if x != y {
		t.Errorf("order changes the component: %s vs %s", x, y)
	}
	if got := memo(nil, 0); got != "" {
		t.Errorf("a task naming no profile has no component; got %s", got)
	}
	if _, err := ProfileMemoComponent([]ProfileMemoEntry{{Name: "a", Identity: "1"}, {Name: "a", Identity: "2"}}, 0); err == nil {
		t.Error("a profile named twice was accepted")
	}
}

func TestDescribeProfiles_ControlChannel(t *testing.T) {
	got, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatal(err)
	}
	described, err := got.Describe(func(string) (string, error) { return "usb-serial", nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(described) != 2 {
		t.Fatalf("described %d", len(described))
	}
	if described[0].GetName() != "amd" || described[0].GetUnits() != 1 || described[0].GetRevision() != "mesa-25.2" ||
		len(described[0].GetIdentityDigest()) != 64 {
		t.Errorf("amd = %v", described[0])
	}
	if described[1].GetUnits() != 0 {
		t.Errorf("a devices profile has no units: %v", described[1])
	}
}

func TestProfileIdentity_DeviceOrderIsNotIdentity(t *testing.T) {
	doc := func(first, second string) string {
		return "profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n  s:\n    kind: devices\n    devices:\n" + first + second
	}
	a := "      - path: /dev/ttyUSB0\n        container_path: /dev/a\n        permissions: r\n"
	b := "      - path: /dev/ttyACM0\n        container_path: /dev/b\n        permissions: rw\n"
	classify := func(p string) (string, error) {
		if strings.Contains(p, "ACM") {
			return "usb-acm", nil
		}
		return "usb-serial", nil
	}
	id := func(text string) string {
		t.Helper()
		parsed, err := ParseProfiles([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		got, err := parsed.List[0].StructuralIdentity(classify)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if id(doc(a, b)) != id(doc(b, a)) {
		t.Error("the order devices are listed in changes the identity")
	}
}

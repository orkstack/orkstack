package docscheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mountNamespacing are the systemd directives that give a service a view of
// the filesystem its container daemon does not share. A device Orker's
// host-native check compares its own view with a probe container's, so
// under any of them the two would differ (design §4.12).
var mountNamespacing = []string{
	"ProtectSystem", "PrivateDevices", "PrivateTmp", "PrivateMounts", "ReadOnlyPaths",
	"InaccessiblePaths", "BindPaths", "BindReadOnlyPaths", "TemporaryFileSystem",
	"RootDirectory", "RootImage",
}

// TestDocs_DeviceOrkerUnitHasNoMountNamespacing holds the systemd unit
// docs/DEPLOY.md gives a device Orker ("Running a device Orker") to none of
// the mount-namespacing directives, and requires the unit to be there.
func TestDocs_DeviceOrkerUnitHasNoMountNamespacing(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "DEPLOY.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(raw), "\n### Running a device Orker\n")
	if !ok {
		t.Fatal(`docs/DEPLOY.md has no "Running a device Orker" section`)
	}
	section, _, _ = strings.Cut(section, "\n### ")
	_, unit, ok := strings.Cut(section, "```ini\n")
	if !ok {
		t.Fatal(`"Running a device Orker" has no ini block with the Orker's unit`)
	}
	unit, _, _ = strings.Cut(unit, "```")
	if !strings.Contains(unit, "[Service]") || !strings.Contains(unit, "ExecStart=") {
		t.Fatalf("the device Orker's ini block is not a service unit:\n%s", unit)
	}
	for _, line := range strings.Split(unit, "\n") {
		key, _, _ := strings.Cut(strings.TrimSpace(line), "=")
		for _, d := range mountNamespacing {
			if key == d {
				t.Errorf("the device Orker's unit sets %s, which gives the Orker a filesystem view its daemon does not share", d)
			}
		}
	}
}

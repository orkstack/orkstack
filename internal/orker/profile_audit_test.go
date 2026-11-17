package orker

import (
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const auditProcDevices = "Character devices:\n  1 mem\n166 ttyACM\n188 ttyUSB\n\nBlock devices:\n  8 sd\n"

func sensorProfiles(t *testing.T, extra string) Profiles {
	t.Helper()
	doc := "profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n" +
		"  sensor:\n    kind: devices\n    devices:\n      - path: /dev/serial/by-id/usb-x\n        container_path: /dev/sensor\n        permissions: rw\n" + extra
	p, err := ParseProfiles([]byte(doc))
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	return p
}

func usbNode(p string) (hostDevice, error) {
	switch p {
	case "/dev/serial/by-id/usb-x":
		return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 1000}, nil
	case "/dev/ttyACM0":
		return hostDevice{Resolved: "/dev/ttyACM0", Char: true, Major: 166, Minor: 0, GID: 20}, nil
	case "/dev/ttyS0":
		return hostDevice{Resolved: "/dev/ttyS0", Char: true, Major: 4, Minor: 64}, nil
	}
	return hostDevice{}, errors.New("stat " + p + ": no such file or directory")
}

// TestAuditProfiles_RecordsWhatEachDeviceIs: the boot audit classifies every
// device, records the host node behind it, and describes each profile with
// its structural identity; the identity names the class, not the host path.
func TestAuditProfiles_RecordsWhatEachDeviceIs(t *testing.T) {
	audits, described, err := auditProfiles(sensorProfiles(t, ""), auditProcDevices, usbNode)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	got := audits["sensor"]
	if got.identity == "" || len(got.devices) != 1 || got.devices[0].Host.Resolved != "/dev/ttyUSB199" {
		t.Fatalf("audit = %+v", got)
	}
	if len(described) != 1 || described[0].GetIdentityDigest() != got.identity ||
		described[0].GetKind() != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES {
		t.Fatalf("described = %v", described)
	}
	// Another host path of the same class gives the same identity: that is
	// what every Orker of a pool shares.
	other := func(p string) (hostDevice, error) {
		return hostDevice{Resolved: "/dev/ttyUSB3", Char: true, Major: 188, Minor: 3}, nil
	}
	again, _, err := auditProfiles(sensorProfiles(t, ""), auditProcDevices, other)
	if err != nil || again["sensor"].identity != got.identity {
		t.Fatalf("another host node changed the identity: %v, %v", again, err)
	}
}

// TestAuditProfiles_RefusesWhatItCannotPass: a device that is not of a class,
// one that is not there, and two profiles giving one container path refuse
// the boot, naming the profile and the device class detail.
func TestAuditProfiles_RefusesWhatItCannotPass(t *testing.T) {
	for _, tc := range []struct {
		name, extra, want string
		inspect           func(string) (hostDevice, error)
	}{
		{"not a usb serial node", "", "DEVICE_CLASS_REFUSED", func(string) (hostDevice, error) {
			return hostDevice{Resolved: "/dev/ttyS0", Char: true, Major: 4, Minor: 64}, nil
		}},
		{"not there", "", "no such file", func(string) (hostDevice, error) { return hostDevice{}, errors.New("no such file or directory") }},
		{"one container path, two profiles",
			"  modem:\n    kind: devices\n    devices:\n      - path: /dev/ttyACM0\n        container_path: /dev/sensor\n        permissions: r\n",
			"/dev/sensor", usbNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := auditProfiles(sensorProfiles(t, tc.extra), auditProcDevices, tc.inspect)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "sensor") {
				t.Fatalf("audit = %v; want a refusal naming sensor and %q", err, tc.want)
			}
		})
	}
}

// TestProfileAudit_PeriodicCheckLatchesDrift: the periodic audit re-checks
// every audited profile and latches a drift it finds, which cordons the
// Orker; a profile that already drifted is not checked again.
func TestProfileAudit_PeriodicCheckLatchesDrift(t *testing.T) {
	s, c := gateState(t)
	now := time.Unix(1_800_000_000, 0)
	if drifted, present := s.auditOnce(sameNode, now); len(drifted) != 0 || len(present) != 1 {
		t.Fatalf("an unchanged device: drifted %v, present %v", drifted, present)
	}
	if cordoned, _, _ := c.State(); cordoned {
		t.Fatal("an unchanged audit cordoned the Orker")
	}
	calls := 0
	unreadable := func(string) (hostDevice, error) { calls++; return hostDevice{}, errors.New("permission denied") }
	if drifted, present := s.auditOnce(unreadable, now); len(drifted) != 1 || drifted["sensor"] == "" || len(present) != 0 {
		t.Fatalf("drifted = %v, present %v", drifted, present)
	}
	if cordoned, _, _ := c.State(); !cordoned || !s.drifted("sensor") {
		t.Fatal("a drift found by the audit did not latch and cordon")
	}
	calls = 0
	if drifted, _ := s.auditOnce(unreadable, now); len(drifted) != 0 || calls != 0 {
		t.Fatalf("a drifted profile was audited again: %v, %d calls", drifted, calls)
	}
}

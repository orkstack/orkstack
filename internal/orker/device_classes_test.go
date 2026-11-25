package orker

import (
	"strings"
	"testing"
)

const sampleProcDevices = `Character devices:
  1 mem
  4 tty
166 ttyACM
188 ttyUSB
195 nvidia
226 drm
511 nvidia-uvm

Block devices:
  8 sd
188 notachar
`

func TestParseProcDevices_CharacterSectionOnly(t *testing.T) {
	got := parseProcDevices(sampleProcDevices)
	if got[188] != "ttyUSB" || got[166] != "ttyACM" || got[226] != "drm" || got[8] != "" {
		t.Fatalf("drivers = %v", got)
	}
}

// TestClassifyDevice holds the class table (design §4.9) to the device it
// is handed: a character node whose driver and node name are a class's, and
// nothing else.
func TestClassifyDevice(t *testing.T) {
	drivers := parseProcDevices(sampleProcDevices)
	for _, tc := range []struct {
		name    string
		dev     hostDevice
		want    string
		wantErr string
	}{
		{"usb serial adapter", hostDevice{Resolved: "/dev/ttyUSB0", Char: true, Major: 188, Minor: 0}, "usb-serial", ""},
		{"acm modem", hostDevice{Resolved: "/dev/ttyACM3", Char: true, Major: 166, Minor: 3}, "usb-serial", ""},
		{"a test node at the usb major", hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199}, "usb-serial", ""},
		{"block device", hostDevice{Resolved: "/dev/ttyUSB0", Char: false, Major: 188}, "", "not a character device"},
		{"driver not a class", hostDevice{Resolved: "/dev/ttyUSB0", Char: true, Major: 4}, "", "driver tty"},
		{"name not the driver's", hostDevice{Resolved: "/dev/ttyS0", Char: true, Major: 188}, "", "ttyS0"},
		{"unknown major", hostDevice{Resolved: "/dev/ttyUSB0", Char: true, Major: 250}, "", "no driver"},
		{"a drm node is not a devices class", hostDevice{Resolved: "/dev/dri/renderD128", Char: true, Major: 226, Minor: 128}, "", "drm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := classifyDevice(tc.dev, drivers)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("classify = %q, %v; want an error naming %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("classify = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// TestResolveDevicePath: a path resolves on the host and must stay under
// /dev; a link out of it is refused, whatever it points at.
// TestClassifyGPUNode_NVIDIAClasses: the nvidia classes (design §4.9) are
// the gpu kind's alone: a GPU node or the control node under the nvidia
// driver (or its older name, nvidia-frontend), and the two UVM nodes under
// nvidia-uvm. nvidia-modeset is display-only and stays refused, as do the
// caps nodes and any other name under those drivers.
func TestClassifyGPUNode_NVIDIAClasses(t *testing.T) {
	drivers := parseProcDevices(sampleProcDevices)
	frontend := parseProcDevices("Character devices:\n195 nvidia-frontend\n")
	for _, tc := range []struct {
		name    string
		drivers map[uint32]string
		dev     hostDevice
		want    string
		wantErr string
	}{
		{"a GPU node", drivers, hostDevice{Resolved: "/dev/nvidia0", Char: true, Major: 195, Minor: 0}, "nvidia", ""},
		{"another GPU node", drivers, hostDevice{Resolved: "/dev/nvidia7", Char: true, Major: 195, Minor: 7}, "nvidia", ""},
		{"the control node", drivers, hostDevice{Resolved: "/dev/nvidiactl", Char: true, Major: 195, Minor: 255}, "nvidia", ""},
		{"the driver's older name", frontend, hostDevice{Resolved: "/dev/nvidia0", Char: true, Major: 195, Minor: 0}, "nvidia", ""},
		{"uvm", drivers, hostDevice{Resolved: "/dev/nvidia-uvm", Char: true, Major: 511, Minor: 0}, "nvidia-uvm", ""},
		{"uvm tools", drivers, hostDevice{Resolved: "/dev/nvidia-uvm-tools", Char: true, Major: 511, Minor: 1}, "nvidia-uvm", ""},
		{"modeset is refused", drivers, hostDevice{Resolved: "/dev/nvidia-modeset", Char: true, Major: 195, Minor: 254}, "", "nvidia-modeset"},
		{"a caps node is refused", drivers, hostDevice{Resolved: "/dev/nvidia-caps/nvidia-cap1", Char: true, Major: 195, Minor: 1}, "", "nvidia-cap1"},
		{"another name under uvm", drivers, hostDevice{Resolved: "/dev/nvidia0", Char: true, Major: 511, Minor: 0}, "", "nvidia0"},
		{"a GPU node is no devices class", drivers, hostDevice{Resolved: "/dev/nvidia0", Char: true, Major: 195, Minor: 0}, "", "not of a device class"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classify := classifyGPUNode
			if strings.Contains(tc.name, "devices class") {
				classify = classifyDevice
			}
			got, err := classify(tc.dev, tc.drivers)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("classify = %q, %v; want an error naming %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("classify = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestResolveDevicePath(t *testing.T) {
	for _, tc := range []struct {
		name, path, resolved string
		wantErr              string
	}{
		{"plain", "/dev/ttyUSB0", "/dev/ttyUSB0", ""},
		{"by-id link", "/dev/serial/by-id/usb-x", "/dev/ttyUSB0", ""},
		{"link out of /dev", "/dev/serial/by-id/evil", "/etc/shadow", "outside /dev"},
		{"link to /dev itself", "/dev/serial/by-id/dir", "/dev", "outside /dev"},
		{"link to a refused path", "/dev/serial/by-id/mem", "/dev/mem", "/dev/mem"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eval := func(string) (string, error) { return tc.resolved, nil }
			got, err := resolveDevicePath(tc.path, eval)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolve = %q, %v; want an error naming %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.resolved {
				t.Fatalf("resolve = %q, %v", got, err)
			}
		})
	}
}

// TestDeviceNumbers splits a dev_t as glibc's major() and minor() do.
func TestDeviceNumbers(t *testing.T) {
	for _, tc := range []struct {
		rdev         uint64
		major, minor uint32
	}{
		{188<<8 | 199, 188, 199},
		{226<<8 | 128, 226, 128},
		{0x100000 | 15<<8 | 0x05, 15, 0x105},
	} {
		if major, minor := deviceNumbers(tc.rdev); major != tc.major || minor != tc.minor {
			t.Errorf("deviceNumbers(%#x) = %d:%d, want %d:%d", tc.rdev, major, minor, tc.major, tc.minor)
		}
	}
}

// TestInspectHostDevice_ReadsTheRealNode stats a node every Linux host has
// (/dev/null, char 1:3, driver mem), and the classifier refuses it: mem is
// not a device class.
func TestInspectHostDevice_ReadsTheRealNode(t *testing.T) {
	dev, err := inspectHostDevice("/dev/null")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !dev.Char || dev.Major != 1 || dev.Minor != 3 || dev.Resolved != "/dev/null" {
		t.Fatalf("/dev/null = %+v", dev)
	}
	classify := hostDeviceClassifier("Character devices:\n  1 mem\n")
	if class, err := classify("/dev/null"); err == nil || !strings.Contains(err.Error(), "driver mem") {
		t.Fatalf("classify(/dev/null) = %q, %v", class, err)
	}
	if _, err := inspectHostDevice("/dev/ork-no-such-node"); err == nil {
		t.Fatal("a missing node inspected")
	}
}

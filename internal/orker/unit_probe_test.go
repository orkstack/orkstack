package orker

import (
	"bytes"
	"strings"
	"testing"
)

// TestProbe_UnitRefusesWhatIsNotAUnitNode: `orker probe unit` asks the driver
// of a render node or of kfd only.
func TestProbe_UnitRefusesWhatIsNotAUnitNode(t *testing.T) {
	for _, args := range [][]string{{"unit"}, {"unit", "/dev/null"}, {"unit", "dev/dri/renderD128"}, {"unit", "/dev/dri/card0"}, {"unit", "/orkstack/other"}} {
		var out, errOut bytes.Buffer
		if code := Probe(args, &out, &errOut); code != 2 {
			t.Errorf("Probe(%q) = %d, want 2 (usage)", args, code)
		}
	}
}

// TestUnitProbe_DriverFailureIsAnError: a node whose driver does not answer
// its version ioctl is reported with an error, never as healthy.
func TestUnitProbe_DriverFailureIsAnError(t *testing.T) {
	v := unitView("/dev/null", "renderD128")
	if v.Error == "" || v.Driver != "" {
		t.Fatalf("unitView of /dev/null as a render node = %+v, want an error and no driver", v)
	}
}

// TestUnitHealthy: a unit is healthy only when the probe's view equals the
// Orker's for every node and every node's driver answered.
func TestUnitHealthy(t *testing.T) {
	own := viewEntry{Path: "/dev/dri/renderD128", Kind: viewChar, Major: 226, Minor: 128, GID: 987}
	want := []viewWant{{Check: checkDevice, Own: own}}
	good := own
	good.Driver = "amdgpu 3.64"
	if err := unitHealthy(want, []viewEntry{good}); err != nil {
		t.Fatalf("a matching, answering node = %v", err)
	}
	silent := own
	if err := unitHealthy(want, []viewEntry{silent}); err == nil || !strings.Contains(err.Error(), "driver") {
		t.Fatalf("a node whose driver did not answer = %v", err)
	}
	moved := good
	moved.Minor = 129
	if err := unitHealthy(want, []viewEntry{moved}); err == nil {
		t.Fatal("a node the probe sees as another passed")
	}
}

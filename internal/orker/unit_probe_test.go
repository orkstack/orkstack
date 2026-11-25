package orker

import (
	"bytes"
	"strings"
	"testing"
	"testing/synctest"
	"time"
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

// TestUnitViewsWithin_AHungDriverIsReported: a driver that never answers is
// reported as that when its bound ends, for its node alone, and the nodes are
// asked at the same time, so two hung ones take one bound and not two. It is a
// fake because a driver that hangs cannot be produced on demand.
func TestUnitViewsWithin_AHungDriverIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		ask := func(p string) viewEntry {
			if p != "/dev/dri/renderD129" {
				<-release
			}
			return viewEntry{Path: p, Kind: viewChar, Driver: "i915 1.6"}
		}
		begin := time.Now()
		got := unitViewsWithin([]string{"/dev/dri/renderD128", "/dev/dri/renderD129", "/dev/kfd"}, unitDriverTimeout, ask)
		if took := time.Since(begin); took != unitDriverTimeout {
			t.Fatalf("took %s, want the one bound of %s", took, unitDriverTimeout)
		}
		if len(got) != 3 || got[1].Driver != "i915 1.6" || got[1].Error != "" {
			t.Fatalf("views = %+v; want the driver that answered as it answered", got)
		}
		for i, path := range map[int]string{0: "/dev/dri/renderD128", 2: "/dev/kfd"} {
			if got[i].Path != path || got[i].Kind != "" || !strings.Contains(got[i].Error, "did not answer within "+unitDriverTimeout.String()) {
				t.Fatalf("view %d = %+v; want %s reported as not answering", i, got[i], path)
			}
		}
	})
}

// TestProbeUnitNodes_SaysItStartedFirst: the first line the probe writes says
// it started, before it opens any device, so an Orker that gets nothing else
// knows the container engine did start it; the report follows.
func TestProbeUnitNodes_SaysItStartedFirst(t *testing.T) {
	var out, errs bytes.Buffer
	if code := probeUnitNodes([]string{"/dev/kfd-absent-is-refused"}, &out, &errs); code != 2 || out.Len() != 0 {
		t.Fatalf("a refused path = %d, stdout %q; want 2 and nothing said", code, out.String())
	}
	if code := probeUnitNodes([]string{gpuLockViewPath}, &out, &errs); code != 0 {
		t.Fatalf("probeUnitNodes = %d: %s", code, errs.String())
	}
	report, started := strings.CutPrefix(out.String(), unitProbeStarted)
	if !started {
		t.Fatalf("stdout %q does not begin with %q", out.String(), unitProbeStarted)
	}
	if views, err := parseProbeView([]byte(report)); err != nil || len(views) != 1 || views[0].Path != gpuLockViewPath {
		t.Fatalf("the report after it = %+v, %v", views, err)
	}
}

package orker

import (
	"bytes"
	"strings"
	"testing"
)

func usbView() viewEntry {
	return viewEntry{Path: "/dev/ttyUSB199", Kind: viewChar, Major: 188, Minor: 199, GID: 1000, Dev: 6, Ino: 1201}
}

// TestJudgeHostView holds the host-native rule (design §4.12) to what a probe
// container reported: a device must be the same character node with the same
// group, a bind the same object, and the probe must report every path once
// and nothing else.
func TestJudgeHostView(t *testing.T) {
	own := usbView()
	bind := viewEntry{Path: "/etc/cdi/x.yaml", Kind: viewFile, Dev: 2049, Ino: 77}
	want := []viewWant{{Check: checkDevice, Own: own}, {Check: checkBind, Own: bind}}
	inContainer := own
	// --device makes a node of its own in the container: its dev:inode is
	// the container's, and a device is never judged by them.
	inContainer.Dev, inContainer.Ino = 99, 3
	with := func(f func(*viewEntry)) []viewEntry {
		d := inContainer
		f(&d)
		return []viewEntry{d, bind}
	}
	for _, tc := range []struct {
		name    string
		probed  []viewEntry
		wantErr string
	}{
		{"equal views", []viewEntry{inContainer, bind}, ""},
		{"order does not matter", []viewEntry{bind, inContainer}, ""},
		{"another device number", with(func(d *viewEntry) { d.Minor = 0 }), "188:0"},
		{"another group", with(func(d *viewEntry) { d.GID = 5 }), "gid 5"},
		{"not a char node", with(func(d *viewEntry) { d.Kind = viewFile }), "file"},
		{"a bind of another object", []viewEntry{inContainer, {Path: bind.Path, Kind: viewFile, Dev: 2049, Ino: 78}}, "2049:78"},
		{"a path missing", []viewEntry{inContainer}, "did not report /etc/cdi/x.yaml"},
		{"a path twice", []viewEntry{inContainer, bind, bind}, "twice"},
		{"a path not asked", []viewEntry{inContainer, bind, {Path: "/dev/null", Kind: viewChar, Major: 1, Minor: 3}}, "/dev/null"},
		{"a probe error for the path", with(func(d *viewEntry) { d.Kind = ""; d.Error = "no such file" }), "no such file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := judgeHostView(want, tc.probed)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("judge = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("judge = %v; want an error naming %q", err, tc.wantErr)
			}
		})
	}
}

// TestProbeView_RoundTrips: what the probe writes is what the Orker reads.
func TestProbeView_RoundTrips(t *testing.T) {
	in := []viewEntry{usbView(), {Path: "/dev/gone", Error: "stat /dev/gone: no such file or directory"}}
	var buf bytes.Buffer
	if err := writeProbeView(&buf, in); err != nil {
		t.Fatal(err)
	}
	got, err := parseProbeView(buf.Bytes())
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, buf.Bytes())
	}
	if len(got) != 2 || got[0] != in[0] || got[1] != in[1] {
		t.Fatalf("round trip = %+v", got)
	}
}

// TestParseProbeView_Strict: the probe's output is a container's, so it is
// held to its exact shape and size, never decoded leniently.
func TestParseProbeView_Strict(t *testing.T) {
	for _, tc := range []struct{ name, raw, want string }{
		{"empty", "", "probe view"},
		{"not json", "hello", "probe view"},
		{"unknown member", `{"view":[{"path":"/dev/x","kind":"char","extra":1}]}`, "extra"},
		{"unknown kind", `{"view":[{"path":"/dev/x","kind":"socket"}]}`, "socket"},
		{"relative path", `{"view":[{"path":"dev/x","kind":"char"}]}`, "absolute"},
		{"neither kind nor error", `{"view":[{"path":"/dev/x"}]}`, "kind or an error"},
		{"trailing data", `{"view":[]} {}`, "probe view"},
		{"too large", `{"view":[` + strings.Repeat(`{"path":"/dev/x","kind":"char"},`, maxProbeViewBytes/30) + `]}`, "larger than"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseProbeView([]byte(tc.raw)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parse = %v; want an error naming %q", err, tc.want)
			}
		})
	}
}

// TestStatView_ReadsTheRealNode stats a node every Linux host has.
func TestStatView_ReadsTheRealNode(t *testing.T) {
	v := statView("/dev/null")
	if v.Error != "" || v.Kind != viewChar || v.Major != 1 || v.Minor != 3 || v.Ino == 0 {
		t.Fatalf("/dev/null = %+v", v)
	}
	if d := statView(t.TempDir()); d.Kind != viewDir || d.Error != "" {
		t.Fatalf("a directory = %+v", d)
	}
	if g := statView("/dev/ork-no-such-node"); g.Error == "" || g.Kind != "" {
		t.Fatalf("a missing node = %+v", g)
	}
}

// TestProbe_ViewReportsEachPath is `orker probe view` as the probe container
// runs it: one report, every path in order, a path it cannot stat reported
// with its error rather than failing the probe.
func TestProbe_ViewReportsEachPath(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := Probe([]string{"view", "/dev/null", "/dev/ork-no-such-node"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	got, err := parseProbeView(out.Bytes())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 2 || got[0].Path != "/dev/null" || got[0].Kind != viewChar || got[1].Error == "" {
		t.Fatalf("view = %+v", got)
	}
	for _, args := range [][]string{nil, {"view"}, {"nope", "/dev/null"}, {"view", "dev/null"}, {"view", "/dev/../etc"}} {
		out.Reset()
		if code := Probe(args, &out, &errOut); code != 2 || out.Len() != 0 {
			t.Errorf("Probe(%q) = %d, wrote %q; want 2 and nothing", args, code, out.String())
		}
	}
}

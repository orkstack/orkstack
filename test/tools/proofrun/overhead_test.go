package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const overheadConfig = "testdata/overhead/playwright.config.ts"

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestOverhead_AnalysesASyntheticFullProof(t *testing.T) {
	a := analyse(readOverhead("testdata/overhead/full", overheadConfig))
	if !near(a.Total, 500) {
		t.Errorf("critical path = %v, want 500 (fast 90 + setup 60 + stack 200 + ui-e2e 150)", a.Total)
	}
	var phases []string
	for _, p := range a.Phases {
		phases = append(phases, p.Phase)
	}
	if got := strings.Join(phases, ","); got != "fast,setup,stack,ui-e2e" {
		t.Errorf("phases = %s", got)
	}
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		// Overheads 5 (heavy), 10, 2, 8: the unended b-005 is left out, and
		// TestB/sub's seconds are inside TestB's.
		{"overhead min", a.Overhead.Min, 2},
		{"overhead median", a.Overhead.Median, 5},
		{"overhead p90", a.Overhead.P90, 10},
		{"overhead max", a.Overhead.Max, 10},
		{"overhead total", a.Overhead.Total, 25},
		{"heavy overhead", a.HeavyOverhead, 5},
		{"ordinary overhead", a.OrdinaryOverhead, 20},
		{"wall overhead at 2 slots", a.WallOverhead, 15},
		{"heavy phase wall", a.Runner[0].Heavy.Wall, 40},
		{"ordinary phase wall (slot 1: 100 s)", a.Runner[0].Ordinary.Wall, 100},
		{"ordinary busy slot-seconds", a.Runner[0].Ordinary.Busy, 180},
		{"ordinary utilisation", a.Runner[0].Ordinary.utilisation(), 0.9},
		{"integration gate outside its processes", a.RunnerRest, 60},
		{"parallel invocation wall", a.UIPar.Wall, 52},
		{"solo invocation wall", a.UISolo.Wall, 60},
		{"parallel summed file walls", a.UIPar.FileWalls, 97},
		{"parallel ideal packing", a.UIPar.ideal(), 52},
		{"Playwright gate outside its tests", a.UIRest, 38},
	} {
		if !near(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if a.Unended != 1 || a.Unmeasur != 0 || len(a.Processes) != 5 {
		t.Errorf("processes = %d, unended %d, unmeasured %d; want 5, 1, 0", len(a.Processes), a.Unended, a.Unmeasur)
	}
	// The sinks decompose the critical path without overlap.
	var sum float64
	for _, s := range a.Sinks {
		sum += s.seconds
	}
	if !near(sum, a.Total) {
		t.Errorf("the sinks sum to %v, the critical path is %v: %+v", sum, a.Total, a.Sinks)
	}
}

func TestOverhead_Report(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "absent.config.ts")
	onlyFanOut := filepath.Join(t.TempDir(), "playwright.config.ts")
	if err := os.WriteFile(onlyFanOut, []byte("export const SOLO_SPECS = [/each-fan-out\\.spec\\.ts$/];\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty := t.TempDir()
	malformed := t.TempDir()
	for _, f := range []string{"integration-tests.json", "ui-e2e-tests.json", "proof.json"} {
		if err := os.WriteFile(filepath.Join(malformed, f), []byte("{not json"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name, dir, config string
		want, absent      []string
	}{
		{
			name: "full", dir: "testdata/overhead/full", config: overheadConfig,
			want: []string{
				"# Proof overhead: synthetic",
				"tier full, PROOF_JOBS 2, PROOF_UI_WORKERS 2",
				"| integration (beside each other) | pass | 200 | 40% |",
				"| **total** | | **500** (8.3 min) | 100% |",
				"| seconds (4 processes) | 2.0 | 5.0 | 10.0 | 10.0 | 25.0 |",
				"Total overhead: **25 slot-seconds**, about **15 s of wall** at PROOF_JOBS 2",
				"- 1 processes have a start line and no end line",
				"| a-002 | example.com/a | no | 100.0 | 90.0 | 10.0 |",
				"| integration.log | heavy (alone) | 1 | 1 | 40 | 40 | 100% | 5 |",
				"| integration.log | ordinary | 4 | 2 | 100 | 180 | 90% | 20 |",
				"leaving 60 s for planning",
				"| parallel project | 2 | 2 | 3 | 52 | 97 | 52 | 93% |",
				"| solo projects | 1 | 2 | 2 | 60 | 55 | 55 | 92% |",
				"leaving 38 s for listing",
				"1 WARN lines about a slow boot",
				"integration/a-002.log: time=2026-10-20T10:00:30Z level=WARN msg=\"engine boot took 23.4s\"",
				"The proof's critical path is 8.3 min. The top time sinks are integration test bodies (115 s, 23% of the critical path), Playwright tests (112 s, 22% of the critical path) and the fast gates (90 s, 18% of the critical path).",
			},
			absent: []string{"Partial proof", "level=INFO"},
		},
		{
			// Without the config the solo files are found by project name: the
			// same split.
			name: "no config", dir: "testdata/overhead/full", config: missingConfig,
			want: []string{"SOLO_SPECS not read from", "| solo projects | 1 | 2 | 2 | 60 | 55 | 55 | 92% |"},
		},
		{
			// A config naming only the fan-out spec moves the ceiling spec into
			// the parallel invocation's wall, which then spans 0..120.
			name: "config decides", dir: "testdata/overhead/full", config: onlyFanOut,
			want: []string{"| parallel project | 2 | 3 | 4 | 120 | 127 | 64 | 53% |", "| solo projects | 1 | 1 | 1 | 25 | 25 | 25 | 100% |"},
		},
		{
			name: "partial", dir: "testdata/overhead/partial", config: overheadConfig,
			want: []string{
				"**Partial proof.**",
				"proof.json (absent)",
				"integration-tests.json (absent)",
				"integration.log (absent)",
				"ui-e2e-tests.json (absent)",
				"| build | fail | 20 | 67% |",
				"No integration start/end lines",
				"No ui-e2e-tests.json",
				"No WARN line about a slow boot",
				"The top time sink is the fast gates (30 s, 100% of the critical path).",
			},
		},
		{
			name: "empty", dir: empty, config: overheadConfig,
			want: []string{"gates.tsv (absent, empty or unreadable)", "No gates.tsv: no gate table.", "Nothing in this proof directory measures where its time went."},
		},
		{
			name: "malformed", dir: malformed, config: overheadConfig,
			want: []string{"integration-tests.json (malformed:", "ui-e2e-tests.json (malformed:", "proof.json (malformed:"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := overheadReport(readOverhead(c.dir, c.config))
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("report lacks %q:\n%s", w, got)
				}
			}
			for _, w := range c.absent {
				if strings.Contains(got, w) {
					t.Errorf("report carries %q:\n%s", w, got)
				}
			}
		})
	}
}

// dirDigest is every file's path, mode and content under dir.
func dirDigest(t *testing.T, dir string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		h.Write([]byte(p + "\x00" + info.Mode().String() + "\x00"))
		if !d.IsDir() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			h.Write(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestOverhead_CommandWritesOutAndNeverTouchesTheProof(t *testing.T) {
	quiet(t)
	proof := filepath.Join(t.TempDir(), "proof")
	if err := os.CopyFS(proof, os.DirFS("testdata/overhead/full")); err != nil {
		t.Fatal(err)
	}
	before := dirDigest(t, proof)
	out := filepath.Join(t.TempDir(), "overhead.md")
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	codes := []int{
		cmdOverhead([]string{"-from", proof, "-out", out, "-config", overheadConfig}),
		cmdOverhead([]string{"-from", proof, "-out", filepath.Join(proof, "overhead.md")}),
		cmdOverhead([]string{"-from", proof, "-out", filepath.Join(proof, "integration", "x.md")}),
		cmdOverhead([]string{"-out", out}),
		cmdOverhead([]string{"-from", filepath.Join(proof, "gates.tsv")}),
	}
	os.Stdout = prev
	devnull.Close()
	if want := []int{0, 2, 2, 2, 2}; !slices.Equal(codes, want) {
		t.Errorf("exit codes = %v, want %v (-out inside the proof, no -from and a file as -from are refused)", codes, want)
	}
	b, err := os.ReadFile(out)
	if err != nil || !strings.Contains(string(b), "## Summary") {
		t.Errorf("-out %s: %v\n%s", out, err, b)
	}
	if after := dirDigest(t, proof); after != before {
		t.Error("overhead changed the proof directory")
	}
}

// A proof whose setup ran as one concurrent group records each member and
// a setup row holding the group's wall: the critical path counts that row
// once, never the members summed. The site gate is a fast gate.
func TestOverhead_CountsTheConcurrentSetupGroupOnce(t *testing.T) {
	a := analyse(readOverhead("testdata/overhead/concurrent", overheadConfig))
	if !near(a.Total, 499) {
		t.Errorf("critical path = %v, want 499 (fast 120 with the site gate + setup 29 + stack 200 + ui-e2e 150)", a.Total)
	}
	var phases []string
	for _, p := range a.Phases {
		phases = append(phases, p.Phase)
		if p.Phase == "setup" && (!near(p.Seconds, 29) || strings.Join(p.Gates, ",") != "binaries,temporal,integration-compile,images,setup") {
			t.Errorf("setup phase = %v s over %v, want 29 s over its four members and the setup row", p.Seconds, p.Gates)
		}
	}
	if got := strings.Join(phases, ","); got != "fast,setup,stack,ui-e2e" {
		t.Errorf("phases = %s", got)
	}
	got := overheadReport(readOverhead("testdata/overhead/concurrent", overheadConfig))
	for _, w := range []string{
		"| setup (Temporal, images, binaries, integration test binaries), concurrent: the setup row's wall | binaries, temporal, integration-compile, images, setup | 29 |",
		"| **total** | | **499** (8.3 min) | 100% |",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("report lacks %q:\n%s", w, got)
		}
	}
}

// A lane cannot be busy longer than it existed: when an invocation's summed
// file walls exceed its lanes' capacity (a ledger written before proofrun kept
// each invocation's lanes apart charged the gap between two invocations to
// the next one's first file), the utilisation is 100% and the report says why.
func TestOverhead_ALaneIsNeverMoreThanFullyUsed(t *testing.T) {
	got := overheadReport(readOverhead("testdata/overhead/concurrent", overheadConfig))
	for _, w := range []string{
		"| parallel project | 2 | 2 | 2 | 50 | 95 | 50 | 95% |",
		"| solo projects | 1 | 2 | 2 | 55 | 85 | 85 | 100% |",
		"The solo projects' summed file walls (85 s) exceed what 1 lane held in 55 s",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("report lacks %q:\n%s", w, got)
		}
	}
	if strings.Contains(got, "155%") {
		t.Errorf("a lane is reported more than fully used:\n%s", got)
	}
}

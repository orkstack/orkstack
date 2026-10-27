package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The floors table is strict, and the verdict is re-derived from the values
// and floors by each metric's direction, never read from the file.

const healthFixtures = "testdata/runnerhealth"

func readHealthFixture(t *testing.T, name string) *runnerHealth {
	t.Helper()
	var h runnerHealth
	if err := readJSON(filepath.Join(healthFixtures, name), &h); err != nil {
		t.Fatal(err)
	}
	return &h
}

func TestRunnerFloors_ParsesTheTableAndRefusesABadRow(t *testing.T) {
	good := "# class\tmetric\tfloor\tderived\ngithub-hosted/ubuntu24/4cpu\tdisk_write_mb_s\t40\tmeasured\n\ngithub-hosted/ubuntu24/4cpu\tfsync_p95_ms\t60\tmeasured\n"
	f, err := parseRunnerFloors(strings.NewReader(good), "floors.tsv")
	if err != nil {
		t.Fatal(err)
	}
	if got := f.forClass("github-hosted/ubuntu24/4cpu"); len(got) != 2 || got["disk_write_mb_s"] != 40 || got["fsync_p95_ms"] != 60 {
		t.Errorf("forClass = %v", got)
	}
	if got := f.forClass("local"); len(got) != 0 {
		t.Errorf("an unnamed class has floors: %v", got)
	}
	for name, row := range map[string]string{
		"three columns":        "c\tdisk_write_mb_s\t40\n",
		"an unknown metric":    "c\tdisk_write_gb_s\t40\tx\n",
		"a zero floor":         "c\tdisk_write_mb_s\t0\tx\n",
		"a non-number":         "c\tdisk_write_mb_s\tfast\tx\n",
		"no derivation":        "c\tdisk_write_mb_s\t40\t\n",
		"a duplicate":          "c\tdisk_write_mb_s\t40\tx\nc\tdisk_write_mb_s\t50\ty\n",
		"a class with a space": "c d\tdisk_write_mb_s\t40\tx\n",
	} {
		if _, err := parseRunnerFloors(strings.NewReader(row), "floors.tsv"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// The repository's own table parses, and names every metric for each
	// class it declares, so no class is judged on a subset.
	real, err := readRunnerFloors(filepath.Join("..", "..", "..", runnerFloorsFile))
	if err != nil {
		t.Fatal(err)
	}
	classes := map[string]int{}
	for _, r := range real.Rows {
		classes[r.Class]++
	}
	if len(classes) == 0 {
		t.Fatal("the repository's floors table declares no class")
	}
	for class, n := range classes {
		if n != len(healthMetricDefs) {
			t.Errorf("class %s declares %d floors, want one per metric (%d)", class, n, len(healthMetricDefs))
		}
		if !strings.HasPrefix(class, "github-hosted/ubuntu24/") || !strings.HasSuffix(class, "cpu") {
			t.Errorf("class %s is not the shape scripts/proof-part.sh names (github-hosted/<ImageOS>/<N>cpu)", class)
		}
	}
	if _, err := readRunnerFloors(filepath.Join(t.TempDir(), "absent.tsv")); err != nil {
		t.Errorf("an absent table is an error: %v", err)
	}
}

func TestRunnerHealth_BelowIsReDerivedFromValuesAndFloors(t *testing.T) {
	below := readHealthFixture(t, "below-floor.json")
	got := below.below()
	if len(got) != 2 || got[0].Metric != "disk_write_mb_s" || got[1].Metric != "fsync_p95_ms" {
		t.Fatalf("below = %+v", got)
	}
	if below.verdict() != healthBelowFloor {
		t.Errorf("verdict = %s", below.verdict())
	}
	if want := "disk_write_mb_s 12.3 MB/s below the floor 40; fsync_p95_ms 184.2 ms above the floor 60"; describeBelow(got) != want {
		t.Errorf("describeBelow = %q, want %q", describeBelow(got), want)
	}
	above := readHealthFixture(t, "above-floor.json")
	if got := above.below(); len(got) != 0 || above.verdict() != healthAboveFloor {
		t.Errorf("above-floor: below %+v verdict %s", got, above.verdict())
	}
	// The file's own verdict and below_floor flags are not believed.
	lying := readHealthFixture(t, "lying-verdict.json")
	if got := lying.below(); len(got) != 1 || got[0].Metric != "disk_write_mb_s" || lying.verdict() != healthBelowFloor {
		t.Errorf("a file whose verdict lies: below %+v verdict %s", got, lying.verdict())
	}
	// A metric that could not be measured, and one with no floor, is never
	// below: the floor only ever turns a result into "not a proof".
	h := &runnerHealth{Metrics: []healthMetric{
		{Metric: "disk_write_mb_s", Value: 0, Floor: ptrFloat(40), Error: "O_DIRECT refused"},
		{Metric: "cpu_single_mb_s", Value: 1},
		{Metric: "not_a_metric", Value: 0, Floor: ptrFloat(1)},
	}}
	if got := h.below(); len(got) != 0 {
		t.Errorf("an errored, unfloored or unknown metric was below: %+v", got)
	}
	if h.verdict() != healthAboveFloor {
		t.Errorf("verdict = %s, want %s (a floor was declared)", h.verdict(), healthAboveFloor)
	}
	if (&runnerHealth{Metrics: []healthMetric{{Metric: "cpu_single_mb_s", Value: 1}}}).verdict() != healthUnfloored {
		t.Error("a record with no floor is not unfloored")
	}
	var nilHealth *runnerHealth
	if got := nilHealth.below(); got != nil {
		t.Errorf("nil.below() = %v", got)
	}
}

func ptrFloat(v float64) *float64 { return &v }

// The subcommand measures this host, judges it against its class's floors
// and writes the file; a floor no host can meet makes the verdict
// below-floor and the exit 1, and no floors make it unfloored and 0.
func TestRunnerHealth_MeasuresAndWritesTheFile(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	floors := filepath.Join(dir, "floors.tsv")
	if err := os.WriteFile(floors, []byte("test\tcpu_single_mb_s\t1e15\tunreachable\ntest\tmem_available_mib\t1\treachable\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "part", runnerHealthFile)
	code := cmdRunnerHealth([]string{"-out", out, "-class", "test", "-floors", floors, "-seconds", "0.4"})
	if code != 1 {
		t.Fatalf("exit %d, want 1 for a runner below an unreachable floor", code)
	}
	h, err := readRunnerHealth(filepath.Dir(out))
	if err != nil || h == nil {
		t.Fatalf("readRunnerHealth: %v, %+v", err, h)
	}
	if h.Format != runnerHealthFormat || h.Class != "test" || h.Verdict != healthBelowFloor || h.Runner.CPUs < 1 || h.Seconds <= 0 {
		t.Errorf("record = %+v", h)
	}
	if h.FloorsFile != floors || h.FloorsSHA256 == "" {
		t.Errorf("the record does not name the floors it measured against: %q %q", h.FloorsFile, h.FloorsSHA256)
	}
	if len(h.Metrics) != len(healthMetricDefs) {
		t.Fatalf("%d metrics, want %d", len(h.Metrics), len(healthMetricDefs))
	}
	byName := map[string]healthMetric{}
	for i, m := range h.Metrics {
		if m.Metric != healthMetricDefs[i].Metric || m.Unit != healthMetricDefs[i].Unit {
			t.Errorf("metric %d is %s %s, want %s %s", i, m.Metric, m.Unit, healthMetricDefs[i].Metric, healthMetricDefs[i].Unit)
		}
		byName[m.Metric] = m
	}
	if m := byName["cpu_single_mb_s"]; m.Error != "" || m.Value <= 0 || m.Floor == nil || *m.Floor != 1e15 || !m.Below {
		t.Errorf("cpu_single_mb_s = %+v, want a measured value below the floor", m)
	}
	if m := byName["mem_available_mib"]; m.Error != "" || m.Value < 1 || m.Below {
		t.Errorf("mem_available_mib = %+v, want a measured value above its floor", m)
	}
	if m := byName["cpu_all_mb_s"]; m.Floor != nil || m.Below || m.Value <= 0 {
		t.Errorf("cpu_all_mb_s = %+v, want measured and unfloored", m)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(out), ".runner-health.probe")); !os.IsNotExist(err) {
		t.Error("the disk probe was left behind")
	}
	if code := cmdRunnerHealth([]string{"-out", out, "-class", "local", "-floors", filepath.Join(dir, "absent.tsv"), "-seconds", "0.2"}); code != 0 {
		t.Errorf("exit %d, want 0 for a class with no floors", code)
	}
	if h, _ := readRunnerHealth(filepath.Dir(out)); h == nil || h.Verdict != healthUnfloored || h.FloorsFile != "" {
		t.Errorf("record = %+v, want unfloored with no floors file named", h)
	}
	if code := cmdRunnerHealth([]string{"-out", out, "-class", "x", "-floors", filepath.Join(healthFixtures, "below-floor.json")}); code != 2 {
		t.Errorf("exit %d, want 2 for a floors table that cannot be read", code)
	}
	if code := cmdRunnerHealth([]string{"-out", out}); code != 2 {
		t.Errorf("exit %d, want 2 without -class", code)
	}
}

func TestRunnerHealth_AnAbsentFileIsNoRecordAndAnotherFormatIsAnError(t *testing.T) {
	dir := t.TempDir()
	if h, err := readRunnerHealth(dir); h != nil || err != nil {
		t.Errorf("absent: %+v %v", h, err)
	}
	if err := os.WriteFile(filepath.Join(dir, runnerHealthFile), []byte(`{"format": 2}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRunnerHealth(dir); err == nil || !strings.Contains(err.Error(), "format 2") {
		t.Errorf("another format: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, runnerHealthFile), []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRunnerHealth(dir); err == nil {
		t.Error("a corrupt file read as a record")
	}
}

func TestPercentile(t *testing.T) {
	for _, c := range []struct {
		samples []float64
		p       int
		want    float64
	}{
		{[]float64{5}, 95, 5},
		{[]float64{3, 1, 2}, 50, 2},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 95, 10},
		{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 95, 19},
	} {
		if got := percentile(append([]float64{}, c.samples...), c.p); got != c.want {
			t.Errorf("percentile(%v, %d) = %v, want %v", c.samples, c.p, got, c.want)
		}
	}
}

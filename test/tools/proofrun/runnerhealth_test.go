package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	// Two more disk-read bursts and an fsync reading ride along with the real
	// I/O. On a filesystem that refuses O_DIRECT every reading carries its
	// reason; either way the array has its shape, in the order the readings
	// were taken, and its first entry is the metric's own reading.
	read := byName["disk_read_mb_s"]
	if len(read.Samples) != 3 || read.Samples[0].Value != read.Value || read.Samples[0].Error != read.Error {
		t.Fatalf("disk_read_mb_s = %+v, want three samples, the first being the metric's own reading", read)
	}
	if fs := byName["fsync_p95_ms"]; len(fs.Samples) != 2 || fs.Samples[0].Value != fs.Value || fs.Samples[0].Error != fs.Error {
		t.Errorf("fsync_p95_ms = %+v, want two samples, the first being the metric's own reading", fs)
	}
	for _, m := range h.Metrics {
		var end float64
		for i, smp := range m.Samples {
			// The readings are apart: each begins after the one before it ended.
			if smp.Offset+0.002 < end {
				t.Errorf("%s sample %d began at +%vs, before the sample before it ended at +%vs", m.Metric, i, smp.Offset, end)
			}
			end = smp.Offset + smp.Seconds
		}
		// Offsets and durations are each rounded to a millisecond.
		if len(m.Samples) > 0 && end > h.Seconds+0.01 {
			t.Errorf("%s ends at +%vs, after the measurement's %vs", m.Metric, end, h.Seconds)
		}
	}
	if read.Error == "" && !(read.Samples[1].Offset > read.Samples[0].Offset+read.Samples[0].Seconds) {
		t.Errorf("the second burst (+%vs) follows the first (+%vs, %vs) with nothing between: they are not apart", read.Samples[1].Offset, read.Samples[0].Offset, read.Samples[0].Seconds)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(out), ".runner-health.probe")); !os.IsNotExist(err) {
		t.Error("the disk probe was left behind")
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(out), ".runner-health.*"))
	if len(leftovers) != 0 {
		t.Errorf("the measurement left %v behind", leftovers)
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

// The informational readings (docs/PROOFS.md §5): two more disk-read bursts
// and one more fsync reading, taken apart from the judged ones, recorded with
// their offsets. The floor still judges each metric's first reading and
// nothing else, and the judged measurements run as they always did.

// probeCall is one probe the measurement made, in the order it made it.
type probeCall struct {
	name    string
	path    string
	size    int64
	workers int
	budget  time.Duration
}

// scriptedProbes replaces the host's I/O and CPU with scripted readings and
// records every call.
type scriptedProbes struct {
	calls  []probeCall
	reads  []probeReading
	fsyncs []probeReading
}

type probeReading struct {
	value, seconds float64
	err            error
}

const scriptedWrote = 500 << 20

func (s *scriptedProbes) probes() healthProbes {
	var nread, nfsync int
	return healthProbes{
		diskWrite: func(path string, budget time.Duration) (int64, float64, float64, error) {
			s.calls = append(s.calls, probeCall{name: "write", path: path, budget: budget})
			return scriptedWrote, 203.5, 2.5, nil
		},
		diskRead: func(path string, size int64, budget time.Duration) (float64, float64, error) {
			s.calls = append(s.calls, probeCall{name: "read", path: path, size: size, budget: budget})
			r := s.reads[nread]
			nread++
			return r.value, r.seconds, r.err
		},
		fsync: func(path string, budget time.Duration) (float64, float64, error) {
			s.calls = append(s.calls, probeCall{name: "fsync", path: path, budget: budget})
			r := s.fsyncs[nfsync]
			nfsync++
			return r.value, r.seconds, r.err
		},
		cpu: func(workers int, budget time.Duration) (float64, float64) {
			s.calls = append(s.calls, probeCall{name: "cpu", workers: workers, budget: budget})
			return 1400, budget.Seconds()
		},
		memory: func() (float64, error) {
			s.calls = append(s.calls, probeCall{name: "memory"})
			return 13190, nil
		},
	}
}

func (s *scriptedProbes) names() []string {
	out := make([]string, 0, len(s.calls))
	for _, c := range s.calls {
		out = append(out, c.name)
	}
	return out
}

func healthMetricNamed(t *testing.T, h *runnerHealth, metric string) healthMetric {
	t.Helper()
	for _, m := range h.Metrics {
		if m.Metric == metric {
			return m
		}
	}
	t.Fatalf("no metric %s in %+v", metric, h.Metrics)
	return healthMetric{}
}

func TestRunnerHealth_TakesTwoMoreReadBurstsAndAnFsyncApartFromTheJudgedOnes(t *testing.T) {
	s := &scriptedProbes{
		reads:  []probeReading{{311.04, 2, nil}, {41.26, 1, nil}, {40.04, 1, nil}},
		fsyncs: []probeReading{{6.414, 1.5, nil}, {5.213, 1, nil}},
	}
	budget := 10 * time.Second
	dir := t.TempDir()
	h := measureRunnerHealthWith(s.probes(), dir, "test", healthFloors{}, budget)
	// Other measurements run between the readings, which is what spaces them
	// apart: the second burst after the single-core CPU measurement, the third
	// after the all-core one, and the fsync reading last.
	if got, want := strings.Join(s.names(), " "), "write read fsync cpu read cpu read fsync memory"; got != want {
		t.Fatalf("probes ran as %q, want %q", got, want)
	}
	wantBudget := []time.Duration{2500 * time.Millisecond, 2 * time.Second, 1500 * time.Millisecond, 2 * time.Second, time.Second, 2 * time.Second, time.Second, time.Second, 0}
	var total time.Duration
	for i, c := range s.calls {
		if c.budget != wantBudget[i] {
			t.Errorf("probe %d (%s) was given %v, want %v", i, c.name, c.budget, wantBudget[i])
		}
		total += c.budget
	}
	// The judged measurements keep today's shares of the budget: 25%, 20%,
	// 15%, 20% and 20%. Everything else is on top of them and a few seconds.
	if extra := total - budget; extra <= 0 || extra > 3*time.Second {
		t.Errorf("the extra readings take %v on top of %v, want more than nothing and at most three seconds", extra, budget)
	}
	if s.calls[3].workers != 1 || s.calls[5].workers < 1 {
		t.Errorf("cpu workers = %d then %d", s.calls[3].workers, s.calls[5].workers)
	}
	probe := s.calls[0].path
	for _, i := range []int{1, 4, 6} {
		if s.calls[i].path != probe || s.calls[i].size != scriptedWrote {
			t.Errorf("read %d of %s (%d bytes), want the file the write left (%s, %d bytes)", i, s.calls[i].path, s.calls[i].size, probe, int64(scriptedWrote))
		}
	}
	// fsync truncates the file it opens: it must never be the one the later
	// bursts read.
	for _, i := range []int{2, 7} {
		if s.calls[i].path == probe || filepath.Dir(s.calls[i].path) != dir {
			t.Errorf("fsync %d opened %s: it would truncate the probe the bursts read, or leave the directory", i, s.calls[i].path)
		}
	}

	read := healthMetricNamed(t, h, "disk_read_mb_s")
	if read.Value != 311 || read.Seconds != 2 {
		t.Errorf("disk_read_mb_s = %v over %vs, want the first burst, as it always was", read.Value, read.Seconds)
	}
	if len(read.Samples) != 3 {
		t.Fatalf("disk_read_mb_s has %d samples, want 3: %+v", len(read.Samples), read.Samples)
	}
	for i, want := range []float64{311, 41.3, 40} {
		if read.Samples[i].Value != want || read.Samples[i].Error != "" {
			t.Errorf("disk_read_mb_s sample %d = %+v, want %v", i, read.Samples[i], want)
		}
		if i > 0 && read.Samples[i].Offset < read.Samples[i-1].Offset {
			t.Errorf("sample %d began at %v, before sample %d at %v", i, read.Samples[i].Offset, i-1, read.Samples[i-1].Offset)
		}
	}
	if read.Samples[0].Seconds != read.Seconds {
		t.Errorf("the first sample took %vs, the metric %vs: they are one reading", read.Samples[0].Seconds, read.Seconds)
	}
	fsync := healthMetricNamed(t, h, "fsync_p95_ms")
	if fsync.Value != 6.41 || len(fsync.Samples) != 2 || fsync.Samples[0].Value != 6.41 || fsync.Samples[1].Value != 5.21 || fsync.Samples[1].Offset < fsync.Samples[0].Offset {
		t.Errorf("fsync_p95_ms = %v, samples %+v, want the judged reading then 5.21", fsync.Value, fsync.Samples)
	}
	for _, m := range h.Metrics {
		if m.Metric != "disk_read_mb_s" && m.Metric != "fsync_p95_ms" && len(m.Samples) != 0 {
			t.Errorf("%s carries samples %+v: only the disk read and the fsync are read twice more", m.Metric, m.Samples)
		}
	}
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"samples":[{"offset_seconds":`)) {
		t.Errorf("runner-health.json does not carry the samples with their offsets: %s", b)
	}
}

// The floor judges each metric's first reading, as it always did. A later
// reading, however slow, never degrades a runner; and a first reading below
// the floor degrades it whatever the later ones read.
func TestRunnerHealth_OnlyTheFirstReadingIsJudged(t *testing.T) {
	floors := healthFloors{Rows: []healthFloor{
		{Class: "c", Metric: "disk_read_mb_s", Floor: 75, Derived: "x"},
		{Class: "c", Metric: "fsync_p95_ms", Floor: 30, Derived: "x"},
	}}
	for name, tc := range map[string]struct {
		reads, fsyncs []probeReading
		verdict       string
		below         []string
	}{
		"later readings below the floor": {
			reads:   []probeReading{{300, 2, nil}, {12.5, 1, nil}, {9.8, 1, nil}},
			fsyncs:  []probeReading{{5, 1.5, nil}, {400, 1, nil}},
			verdict: healthAboveFloor,
		},
		"a first reading below the floor": {
			reads:   []probeReading{{41.2, 2, nil}, {305, 1, nil}, {298, 1, nil}},
			fsyncs:  []probeReading{{5, 1.5, nil}, {4, 1, nil}},
			verdict: healthBelowFloor,
			below:   []string{"disk_read_mb_s"},
		},
		"a first fsync over the floor": {
			reads:   []probeReading{{300, 2, nil}, {300, 1, nil}, {300, 1, nil}},
			fsyncs:  []probeReading{{184.2, 1.5, nil}, {4, 1, nil}},
			verdict: healthBelowFloor,
			below:   []string{"fsync_p95_ms"},
		},
		"extra readings that fail": {
			reads:   []probeReading{{300, 2, nil}, {0, 0.01, errors.New("input/output error")}, {0, 0.01, errors.New("input/output error")}},
			fsyncs:  []probeReading{{5, 1.5, nil}, {0, 0.01, errors.New("no space left on device")}},
			verdict: healthAboveFloor,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := &scriptedProbes{reads: tc.reads, fsyncs: tc.fsyncs}
			h := measureRunnerHealthWith(s.probes(), t.TempDir(), "c", floors, 10*time.Second)
			if h.Verdict != tc.verdict || h.verdict() != tc.verdict {
				t.Errorf("verdict %s (re-derived %s), want %s", h.Verdict, h.verdict(), tc.verdict)
			}
			var below []string
			for _, m := range h.below() {
				below = append(below, m.Metric)
			}
			if strings.Join(below, ",") != strings.Join(tc.below, ",") {
				t.Errorf("below = %v, want %v", below, tc.below)
			}
			for _, m := range h.Metrics {
				if m.Error != "" {
					t.Errorf("%s was marked unmeasured (%s) by a failing extra reading", m.Metric, m.Error)
				}
			}
		})
	}
	// The failed extra readings are kept, each with its reason, and carry no value.
	s := &scriptedProbes{
		reads:  []probeReading{{300, 2, nil}, {0, 0.01, errors.New("input/output error")}, {250, 1, nil}},
		fsyncs: []probeReading{{5, 1.5, nil}, {0, 0.01, errors.New("no space left on device")}},
	}
	h := measureRunnerHealthWith(s.probes(), t.TempDir(), "c", floors, 10*time.Second)
	read := healthMetricNamed(t, h, "disk_read_mb_s")
	if len(read.Samples) != 3 || read.Samples[1].Error != "input/output error" || read.Samples[1].Value != 0 || read.Samples[2].Value != 250 {
		t.Errorf("disk_read_mb_s samples = %+v", read.Samples)
	}
	if fs := healthMetricNamed(t, h, "fsync_p95_ms"); len(fs.Samples) != 2 || fs.Samples[1].Error != "no space left on device" {
		t.Errorf("fsync_p95_ms samples = %+v", fs.Samples)
	}
}

// A judged burst that cannot run (a filesystem that refuses O_DIRECT) leaves
// the metric unmeasured, as it always did, and the array keeps its shape: one
// entry per reading, each with its reason.
func TestRunnerHealth_AJudgedBurstThatCannotRunKeepsTheArrayShape(t *testing.T) {
	refused := errors.New("O_DIRECT is refused on this filesystem")
	s := &scriptedProbes{
		reads:  []probeReading{{0, 0, refused}, {0, 0, refused}, {0, 0, refused}},
		fsyncs: []probeReading{{5, 1.5, nil}, {5, 1, nil}},
	}
	floors := healthFloors{Rows: []healthFloor{{Class: "c", Metric: "disk_read_mb_s", Floor: 75, Derived: "x"}}}
	h := measureRunnerHealthWith(s.probes(), t.TempDir(), "c", floors, 10*time.Second)
	read := healthMetricNamed(t, h, "disk_read_mb_s")
	if read.Error == "" || read.Below || read.Value != 0 || len(read.Samples) != 3 {
		t.Fatalf("disk_read_mb_s = %+v", read)
	}
	for i, smp := range read.Samples {
		if smp.Error == "" || smp.Value != 0 {
			t.Errorf("sample %d = %+v, want its reason and no value", i, smp)
		}
	}
	if h.Verdict != healthAboveFloor {
		t.Errorf("verdict = %s: an unmeasurable burst must never degrade a runner", h.Verdict)
	}
}

func TestRunnerHealth_TheSummaryLogShowsEveryReading(t *testing.T) {
	var b bytes.Buffer
	writeRunnerHealthSummary(&b, readHealthFixture(t, "samples-above-floor.json"))
	out := b.String()
	for _, want := range []string{
		"runner-health: disk_read_mb_s 311 MB/s (floor 40)\n",
		"runner-health: disk_read_mb_s readings 311 MB/s at +2.5s (judged), 12.5 at +8s, 9.8 at +11s\n",
		"runner-health: fsync_p95_ms readings 6.41 ms at +4.5s (judged), 400.25 at +12s\n",
		"runner-health: verdict above-floor\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the summary lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "cpu_single_mb_s readings") {
		t.Errorf("a metric measured once has a readings line:\n%s", out)
	}
	b.Reset()
	writeRunnerHealthSummary(&b, &runnerHealth{Class: "c", Verdict: healthUnfloored, Metrics: []healthMetric{{Metric: "disk_read_mb_s", Unit: "MB/s", Value: 5, Samples: []healthSample{
		{Offset: 2.5, Value: 5, Seconds: 2}, {Offset: 8, Error: "input/output error"},
	}}}})
	if want := "runner-health: disk_read_mb_s readings 5 MB/s at +2.5s (judged), unmeasured at +8s: input/output error\n"; !strings.Contains(b.String(), want) {
		t.Errorf("the summary lacks %q:\n%s", want, b.String())
	}
}

// The verdict rule is the old one: each fixture's first readings decide, and
// the file's later readings are read and ignored.
func TestRunnerHealth_FixturesWithSamplesKeepTheirVerdict(t *testing.T) {
	above := readHealthFixture(t, "samples-above-floor.json")
	if got := above.below(); len(got) != 0 || above.verdict() != healthAboveFloor {
		t.Errorf("later readings far past the floors: below %+v verdict %s", got, above.verdict())
	}
	if read := healthMetricNamed(t, above, "disk_read_mb_s"); len(read.Samples) != 3 || read.Samples[1].Offset != 8 || read.Samples[1].Value != 12.5 || read.Samples[1].Seconds != 1 {
		t.Errorf("samples = %+v", read.Samples)
	}
	below := readHealthFixture(t, "samples-below-floor.json")
	got := below.below()
	if len(got) != 1 || got[0].Metric != "disk_read_mb_s" || got[0].Value != 31 || below.verdict() != healthBelowFloor {
		t.Errorf("a first burst below the floor: below %+v verdict %s", got, below.verdict())
	}
	if want := "disk_read_mb_s 31 MB/s below the floor 40"; describeBelow(got) != want {
		t.Errorf("describeBelow = %q, want %q", describeBelow(got), want)
	}
}

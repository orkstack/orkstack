package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
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
	write := byName["disk_write_mb_s"]
	if len(write.Samples) != 2 || write.Samples[0].Value != write.Value || write.Samples[0].Error != write.Error {
		t.Fatalf("disk_write_mb_s = %+v, want two samples, the first being the metric's own reading", write)
	}
	// Each burst that was taken covers a region of the file a write left,
	// starting on a read boundary inside it; a refused burst covers none.
	for i, smp := range read.Samples {
		if smp.Error != "" {
			if smp.Region != nil {
				t.Errorf("disk_read_mb_s sample %d failed (%s) and covers %+v", i, smp.Error, smp.Region)
			}
			continue
		}
		left := write.Samples[0].Region
		if i > 0 {
			left = write.Samples[1].Region
		}
		if smp.Region == nil || left == nil || smp.Region.Bytes <= 0 || smp.Region.Bytes > left.Bytes || smp.Region.From < 0 || smp.Region.From >= left.Bytes || smp.Region.From%directChunk != 0 {
			t.Errorf("disk_read_mb_s sample %d covers %+v of the %+v the write left", i, smp.Region, left)
		}
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

// The informational readings (docs/PROOFS.md §5): a second write of the
// probe, two more disk-read bursts over it and one more fsync reading, taken
// apart from the judged ones, recorded with their offsets. The floor still
// judges each metric's first reading and nothing else, and the judged
// measurements run as they always did, each on the file it always used.

// probeCall is one probe the measurement made, in the order it made it.
type probeCall struct {
	name    string
	path    string
	size    int64
	from    int64
	workers int
	budget  time.Duration
}

// scriptedProbes replaces the host's I/O and CPU with scripted readings and
// records every call.
type scriptedProbes struct {
	calls  []probeCall
	writes []probeWrite
	reads  []probeReading
	fsyncs []probeReading
}

type probeReading struct {
	value, seconds float64
	err            error
	// bytes is what a disk read covered; a scripted read with none covers
	// the whole file, as a burst that reached its end does.
	bytes int64
}

type probeWrite struct {
	wrote int64
	mbs   float64
	err   error
}

const scriptedWrote = 500 << 20

func (s *scriptedProbes) probes() healthProbes {
	var nwrite, nread, nfsync int
	return healthProbes{
		diskWrite: func(path string, budget time.Duration) (int64, float64, float64, error) {
			s.calls = append(s.calls, probeCall{name: "write", path: path, budget: budget})
			w := probeWrite{wrote: scriptedWrote, mbs: 203.5}
			if nwrite < len(s.writes) {
				w = s.writes[nwrite]
			}
			nwrite++
			return w.wrote, w.mbs, 2.5, w.err
		},
		diskRead: func(path string, size, from int64, budget time.Duration) (int64, float64, float64, error) {
			s.calls = append(s.calls, probeCall{name: "read", path: path, size: size, from: from, budget: budget})
			r := s.reads[nread]
			nread++
			if r.bytes == 0 && r.err == nil {
				r.bytes = size
			}
			return r.bytes, r.value, r.seconds, r.err
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

func TestRunnerHealth_TakesAnotherWriteTwoMoreReadBurstsAndAnFsyncApartFromTheJudgedOnes(t *testing.T) {
	s := &scriptedProbes{
		writes: []probeWrite{{scriptedWrote, 203.5, nil}, {300 << 20, 118.6, nil}},
		reads:  []probeReading{{311.04, 2, nil, 0}, {41.26, 1, nil, 41 << 20}, {40.04, 1, nil, 41 << 20}},
		fsyncs: []probeReading{{6.414, 1.5, nil, 0}, {5.213, 1, nil, 0}},
	}
	budget := 10 * time.Second
	dir := t.TempDir()
	h := measureRunnerHealthWith(s.probes(), dir, "test", healthFloors{}, budget)
	// The judged measurements run in the order they always did — write, read,
	// fsync, single-core CPU, all-core CPU, memory. Between them the probe is
	// written again after the single-core CPU measurement (the judged fsync
	// truncated the first), the first burst follows that write, the second
	// burst comes after the all-core measurement and the fsync reading last.
	if got, want := strings.Join(s.names(), " "), "write read fsync cpu write read cpu read fsync memory"; got != want {
		t.Fatalf("probes ran as %q, want %q", got, want)
	}
	wantBudget := []time.Duration{2500 * time.Millisecond, 2 * time.Second, 1500 * time.Millisecond, 2 * time.Second, time.Second, time.Second, 2 * time.Second, time.Second, time.Second, 0}
	var total time.Duration
	for i, c := range s.calls {
		if c.budget != wantBudget[i] {
			t.Errorf("probe %d (%s) was given %v, want %v", i, c.name, c.budget, wantBudget[i])
		}
		total += c.budget
	}
	// The judged measurements keep today's shares of the budget: 25%, 20%,
	// 15%, 20% and 20%. Everything else is on top of them and a few seconds.
	if extra := total - budget; extra <= 0 || extra > 4*time.Second {
		t.Errorf("the extra readings take %v on top of %v, want more than nothing and at most four seconds", extra, budget)
	}
	if s.calls[3].workers != 1 || s.calls[6].workers < 1 {
		t.Errorf("cpu workers = %d then %d", s.calls[3].workers, s.calls[6].workers)
	}
	// Every disk probe is on one file. The judged fsync opens it, as it always
	// did, which truncates what the judged write left; the later readings are
	// taken over what the second write left, and the last fsync truncates
	// that, as the judged one did.
	probe := s.calls[0].path
	for i, c := range s.calls {
		switch c.name {
		case "write", "read", "fsync":
			if c.path != probe || filepath.Dir(c.path) != dir {
				t.Errorf("probe %d (%s) is on %s, want the one file %s", i, c.name, c.path, probe)
			}
		}
	}
	if s.calls[1].size != scriptedWrote || s.calls[5].size != 300<<20 || s.calls[7].size != 300<<20 {
		t.Errorf("reads covered %d, %d and %d bytes, want the file each write left (%d, %d, %d)", s.calls[1].size, s.calls[5].size, s.calls[7].size, int64(scriptedWrote), int64(300<<20), int64(300<<20))
	}
	// The judged burst starts at the start, and so does the first over the new
	// file; the second burst starts where the first stopped, so it reads
	// blocks the first did not.
	if s.calls[1].from != 0 || s.calls[5].from != 0 || s.calls[7].from != 40<<20 {
		t.Errorf("bursts started at %d, %d and %d, want 0, 0 and %d", s.calls[1].from, s.calls[5].from, s.calls[7].from, int64(40<<20))
	}

	read := healthMetricNamed(t, h, "disk_read_mb_s")
	if read.Value != 311 || read.Seconds != 2 {
		t.Errorf("disk_read_mb_s = %v over %vs, want the first burst, as it always was", read.Value, read.Seconds)
	}
	if len(read.Samples) != 3 {
		t.Fatalf("disk_read_mb_s has %d samples, want 3: %+v", len(read.Samples), read.Samples)
	}
	wantRegions := []healthRegion{{From: 0, Bytes: scriptedWrote}, {From: 0, Bytes: 41 << 20}, {From: 40 << 20, Bytes: 41 << 20}}
	for i, want := range []float64{311, 41.3, 40} {
		smp := read.Samples[i]
		if smp.Value != want || smp.Error != "" {
			t.Errorf("disk_read_mb_s sample %d = %+v, want %v", i, smp, want)
		}
		if smp.Region == nil || *smp.Region != wantRegions[i] {
			t.Errorf("disk_read_mb_s sample %d covered %+v, want %+v", i, smp.Region, wantRegions[i])
		}
		if i > 0 && smp.Offset < read.Samples[i-1].Offset {
			t.Errorf("sample %d began at %v, before sample %d at %v", i, smp.Offset, i-1, read.Samples[i-1].Offset)
		}
	}
	if read.Samples[0].Seconds != read.Seconds {
		t.Errorf("the first sample took %vs, the metric %vs: they are one reading", read.Samples[0].Seconds, read.Seconds)
	}
	write := healthMetricNamed(t, h, "disk_write_mb_s")
	if write.Value != 203.5 || len(write.Samples) != 2 || write.Samples[0].Value != 203.5 || write.Samples[1].Value != 118.6 || write.Samples[1].Offset < write.Samples[0].Offset {
		t.Errorf("disk_write_mb_s = %v, samples %+v, want the judged reading then 118.6", write.Value, write.Samples)
	}
	if r := write.Samples[1].Region; r == nil || *r != (healthRegion{From: 0, Bytes: 300 << 20}) {
		t.Errorf("the second write covered %+v, want the %d bytes it left", r, int64(300<<20))
	}
	fsync := healthMetricNamed(t, h, "fsync_p95_ms")
	if fsync.Value != 6.41 || len(fsync.Samples) != 2 || fsync.Samples[0].Value != 6.41 || fsync.Samples[1].Value != 5.21 || fsync.Samples[1].Offset < fsync.Samples[0].Offset {
		t.Errorf("fsync_p95_ms = %v, samples %+v, want the judged reading then 5.21", fsync.Value, fsync.Samples)
	}
	for _, smp := range fsync.Samples {
		if smp.Region != nil {
			t.Errorf("an fsync reading covers no region of the probe: %+v", smp)
		}
	}
	for _, m := range h.Metrics {
		switch m.Metric {
		case "disk_write_mb_s", "disk_read_mb_s", "fsync_p95_ms":
		default:
			if len(m.Samples) != 0 {
				t.Errorf("%s carries samples %+v: only the disk and the fsync are read twice more", m.Metric, m.Samples)
			}
		}
	}
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"samples":[{"offset_seconds":`, `"region":{"from_bytes":41943040,"bytes":42991616}`} {
		if !bytes.Contains(b, []byte(want)) {
			t.Errorf("runner-health.json lacks %s: %s", want, b)
		}
	}
}

// The second burst starts where the first stopped, rounded down to a read,
// and wraps when the first reached the end of the file; a burst that could
// not read starts the next one at the start.
func TestRunnerHealth_EachBurstStartsWhereTheLastStoppedAndWrapsAtTheEnd(t *testing.T) {
	const file = 100 << 20
	for name, tc := range map[string]struct {
		covered  int64 // what the first extra burst covered
		wantFrom int64
	}{
		"stopped inside the file":              {covered: 41 << 20, wantFrom: 40 << 20},
		"stopped on a read boundary":           {covered: 8 << 20, wantFrom: 8 << 20},
		"stopped part way through a read":      {covered: 41_000_000, wantFrom: 36 << 20},
		"read the whole file":                  {covered: file, wantFrom: 0},
		"covered more than the file holds":     {covered: file + 9<<20, wantFrom: 8 << 20},
		"stopped within one read of the start": {covered: 1 << 20, wantFrom: 0},
	} {
		t.Run(name, func(t *testing.T) {
			s := &scriptedProbes{
				writes: []probeWrite{{file, 100, nil}, {file, 100, nil}},
				reads:  []probeReading{{300, 1, nil, 0}, {50, 1, nil, tc.covered}, {50, 1, nil, 0}},
				fsyncs: []probeReading{{5, 1, nil, 0}, {5, 1, nil, 0}},
			}
			measureRunnerHealthWith(s.probes(), t.TempDir(), "c", healthFloors{}, 10*time.Second)
			if got := s.calls[7].from; got != tc.wantFrom {
				t.Errorf("the second burst started at %d, want %d", got, tc.wantFrom)
			}
		})
	}
	// A burst that failed covered nothing: the next one starts at the start.
	s := &scriptedProbes{
		reads:  []probeReading{{300, 1, nil, 0}, {0, 0.01, errors.New("input/output error"), 0}, {50, 1, nil, 0}},
		fsyncs: []probeReading{{5, 1, nil, 0}, {5, 1, nil, 0}},
	}
	h := measureRunnerHealthWith(s.probes(), t.TempDir(), "c", healthFloors{}, 10*time.Second)
	if s.calls[7].from != 0 {
		t.Errorf("the burst after a failed one started at %d, want 0", s.calls[7].from)
	}
	if smp := healthMetricNamed(t, h, "disk_read_mb_s").Samples[1]; smp.Region != nil || smp.Error == "" {
		t.Errorf("a burst that failed covered a region: %+v", smp)
	}
}

// recordingFile is a file that remembers where each read began.
type recordingFile struct {
	data []byte
	pos  int64
	// reads is the offset of every read, in order.
	reads   []int64
	failAt  int
	failErr error
}

func (f *recordingFile) Read(p []byte) (int, error) {
	if f.failErr != nil && len(f.reads) == f.failAt {
		return 0, f.failErr
	}
	f.reads = append(f.reads, f.pos)
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *recordingFile) Seek(off int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, errors.New("only SeekStart")
	}
	f.pos = off
	return off, nil
}

// readRegion is the burst itself: it reads from a start offset, wraps to the
// start of the file at its end, and never reads more than the file holds or
// past the deadline.
func TestReadRegion_ReadsFromAnOffsetWrapsAndStopsAtTheFileOrTheDeadline(t *testing.T) {
	const chunk = 4
	file := func() *recordingFile { return &recordingFile{data: make([]byte, 5*chunk)} }
	later := time.Now().Add(time.Hour)
	for name, tc := range map[string]struct {
		from      int64
		deadline  time.Time
		wantRead  int64
		wantReads []int64
	}{
		"from the start it reads the file once":             {from: 0, deadline: later, wantRead: 5 * chunk, wantReads: []int64{0, 4, 8, 12, 16}},
		"from the middle it wraps and stops where it began": {from: 2 * chunk, deadline: later, wantRead: 5 * chunk, wantReads: []int64{8, 12, 16, 20, 0, 4}},
		"from the last chunk it wraps at once":              {from: 4 * chunk, deadline: later, wantRead: 5 * chunk, wantReads: []int64{16, 20, 0, 4, 8, 12}},
		// A read is taken before the deadline is looked at, as it always was,
		// so a burst reads at least one chunk.
		"a deadline that has passed ends it after one read": {from: 2 * chunk, deadline: time.Now().Add(-time.Second), wantRead: chunk, wantReads: []int64{8}},
	} {
		t.Run(name, func(t *testing.T) {
			f := file()
			got, err := readRegion(f, make([]byte, chunk), 5*chunk, tc.from, tc.deadline)
			if err != nil || got != tc.wantRead {
				t.Fatalf("read %d bytes, %v; want %d", got, err, tc.wantRead)
			}
			if !reflect.DeepEqual(f.reads, tc.wantReads) {
				t.Errorf("reads at %v, want %v", f.reads, tc.wantReads)
			}
		})
	}
	// A read error comes back with what was read before it.
	f := file()
	f.failAt, f.failErr = 2, errors.New("input/output error")
	got, err := readRegion(f, make([]byte, chunk), 5*chunk, 0, later)
	if err == nil || !strings.Contains(err.Error(), "input/output error") || got != 2*chunk {
		t.Errorf("readRegion = %d, %v, want 8 bytes and the error", got, err)
	}
	// A file shorter than the size it was told to read ends the burst at its end.
	short := &recordingFile{data: make([]byte, 2*chunk)}
	if got, err := readRegion(short, make([]byte, chunk), 5*chunk, 0, later); err != nil || got != 2*chunk {
		t.Errorf("a short file: read %d, %v, want 8", got, err)
	}
}

// The floor judges each metric's first reading, as it always did. A later
// reading, however slow, never degrades a runner; and a first reading below
// the floor degrades it whatever the later ones read.
func TestRunnerHealth_OnlyTheFirstReadingIsJudged(t *testing.T) {
	floors := healthFloors{Rows: []healthFloor{
		{Class: "c", Metric: "disk_write_mb_s", Floor: 40, Derived: "x"},
		{Class: "c", Metric: "disk_read_mb_s", Floor: 75, Derived: "x"},
		{Class: "c", Metric: "fsync_p95_ms", Floor: 30, Derived: "x"},
	}}
	for name, tc := range map[string]struct {
		writes        []probeWrite
		reads, fsyncs []probeReading
		verdict       string
		below         []string
	}{
		"later readings below the floor": {
			reads:   []probeReading{{300, 2, nil, 0}, {12.5, 1, nil, 0}, {9.8, 1, nil, 0}},
			fsyncs:  []probeReading{{5, 1.5, nil, 0}, {400, 1, nil, 0}},
			verdict: healthAboveFloor,
		},
		"a first reading below the floor": {
			reads:   []probeReading{{41.2, 2, nil, 0}, {305, 1, nil, 0}, {298, 1, nil, 0}},
			fsyncs:  []probeReading{{5, 1.5, nil, 0}, {4, 1, nil, 0}},
			verdict: healthBelowFloor,
			below:   []string{"disk_read_mb_s"},
		},
		"a first fsync over the floor": {
			reads:   []probeReading{{300, 2, nil, 0}, {300, 1, nil, 0}, {300, 1, nil, 0}},
			fsyncs:  []probeReading{{184.2, 1.5, nil, 0}, {4, 1, nil, 0}},
			verdict: healthBelowFloor,
			below:   []string{"fsync_p95_ms"},
		},
		"a second write under the floor": {
			writes:  []probeWrite{{scriptedWrote, 203.5, nil}, {40 << 20, 3.2, nil}},
			reads:   []probeReading{{300, 2, nil, 0}, {300, 1, nil, 0}, {300, 1, nil, 0}},
			fsyncs:  []probeReading{{5, 1.5, nil, 0}, {4, 1, nil, 0}},
			verdict: healthAboveFloor,
		},
		"a first write under the floor": {
			writes:  []probeWrite{{scriptedWrote, 31.5, nil}, {scriptedWrote, 400, nil}},
			reads:   []probeReading{{300, 2, nil, 0}, {300, 1, nil, 0}, {300, 1, nil, 0}},
			fsyncs:  []probeReading{{5, 1.5, nil, 0}, {4, 1, nil, 0}},
			verdict: healthBelowFloor,
			below:   []string{"disk_write_mb_s"},
		},
		"extra readings that fail": {
			writes:  []probeWrite{{scriptedWrote, 203.5, nil}, {0, 0, errors.New("no space left on device")}},
			reads:   []probeReading{{300, 2, nil, 0}, {0, 0.01, errors.New("input/output error"), 0}, {0, 0.01, errors.New("input/output error"), 0}},
			fsyncs:  []probeReading{{5, 1.5, nil, 0}, {0, 0.01, errors.New("no space left on device"), 0}},
			verdict: healthAboveFloor,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := &scriptedProbes{writes: tc.writes, reads: tc.reads, fsyncs: tc.fsyncs}
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
		writes: []probeWrite{{scriptedWrote, 203.5, nil}, {0, 0, errors.New("no space left on device")}},
		reads:  []probeReading{{300, 2, nil, 0}, {0, 0.01, errors.New("input/output error"), 0}, {250, 1, nil, 0}},
		fsyncs: []probeReading{{5, 1.5, nil, 0}, {0, 0.01, errors.New("no space left on device"), 0}},
	}
	h := measureRunnerHealthWith(s.probes(), t.TempDir(), "c", floors, 10*time.Second)
	if wr := healthMetricNamed(t, h, "disk_write_mb_s"); len(wr.Samples) != 2 || wr.Samples[1].Error != "no space left on device" || wr.Samples[1].Value != 0 || wr.Samples[1].Region != nil {
		t.Errorf("disk_write_mb_s samples = %+v", wr.Samples)
	}
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
		reads:  []probeReading{{0, 0, refused, 0}, {0, 0, refused, 0}, {0, 0, refused, 0}},
		fsyncs: []probeReading{{5, 1.5, nil, 0}, {5, 1, nil, 0}},
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

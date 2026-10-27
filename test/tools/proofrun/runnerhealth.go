package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// The runner-health floor (AGENTS.md §11.2). Every CI part measures its
// runner for about ten seconds before it runs anything — sequential write and
// read throughput and fsync latency on its work disk, where the object store's
// and Temporal's volumes live, single-core and all-core CPU throughput, and
// available memory — and writes runner-health.json beside its records: the
// measured values, the floors scripts/proof-runner-floors.tsv declares for
// its runner class, and a verdict. A stack part below a floor stops before
// any gate; the merger classifies it runner-degraded, exactly as a part whose
// runner GitHub lost: its tests get no outcome of the tree, the proof is not
// a proof of the tree, and that one job may run again. A part above the floor
// whose tests fail is a test failure. A local proof measures the same numbers
// for information only: the host's floors are its measured costs themselves,
// so a local verdict never changes.
//
// The floor is measured once, at the start: a runner that degrades after the
// step is not seen here, and its failures stay test failures.

const (
	runnerHealthFile   = "runner-health.json"
	runnerFloorsFile   = "scripts/proof-runner-floors.tsv"
	runnerHealthFormat = 1
	// runnerHealthSeconds is the default measurement budget, split across
	// the timed measurements by their shares below.
	runnerHealthSeconds = 10.0
)

// The verdicts runner-health.json carries. The merger re-derives them from
// the values and floors and never takes the file's word (merge.go).
const (
	healthAboveFloor = "above-floor"
	healthBelowFloor = "below-floor"
	healthUnfloored  = "unfloored"
)

// healthMetricDef is one metric the step measures: its name, unit and which
// side of the floor is healthy. A `min` metric is below its floor when the
// value is under it (a throughput, a capacity); a `max` metric when the value
// is over it (a latency).
type healthMetricDef struct {
	Metric    string
	Unit      string
	Direction string // "min" or "max"
	// Share is the metric's share of the measurement budget; a metric
	// measured without a clock (memory) has none.
	Share float64
	// Decimals is how the value is rounded in the record.
	Decimals int
}

var healthMetricDefs = []healthMetricDef{
	{"disk_write_mb_s", "MB/s", "min", 0.25, 1},
	{"disk_read_mb_s", "MB/s", "min", 0.20, 1},
	{"fsync_p95_ms", "ms", "max", 0.15, 2},
	{"cpu_single_mb_s", "MB/s", "min", 0.20, 1},
	{"cpu_all_mb_s", "MB/s", "min", 0.20, 1},
	{"mem_available_mib", "MiB", "min", 0, 0},
}

func healthMetricByName(metric string) (healthMetricDef, bool) {
	for _, d := range healthMetricDefs {
		if d.Metric == metric {
			return d, true
		}
	}
	return healthMetricDef{}, false
}

func healthMetricNames() []string {
	out := make([]string, 0, len(healthMetricDefs))
	for _, d := range healthMetricDefs {
		out = append(out, d.Metric)
	}
	return out
}

// healthMetric is one measured value against its floor, as the file records
// it.
type healthMetric struct {
	Metric string  `json:"metric"`
	Unit   string  `json:"unit"`
	Value  float64 `json:"value"`
	// Floor is the class's floor for this metric; absent when the floors
	// table declares none for the class.
	Floor *float64 `json:"floor,omitempty"`
	// Below says the value is on the wrong side of its floor: under it for
	// a throughput or a capacity, over it for a latency.
	Below bool `json:"below_floor"`
	// Error says the measurement itself failed; the value is then 0 and the
	// metric is never below its floor, so a measurement that cannot run keeps
	// a test failure a test failure.
	Error   string  `json:"error,omitempty"`
	Seconds float64 `json:"seconds,omitempty"`
}

// runnerHealth is runner-health.json.
type runnerHealth struct {
	Format       int            `json:"format"`
	Class        string         `json:"class"`
	MeasuredAt   string         `json:"measured_at"`
	Seconds      float64        `json:"seconds"`
	Dir          string         `json:"dir"`
	Runner       runnerRecord   `json:"runner"`
	FloorsFile   string         `json:"floors_file,omitempty"`
	FloorsSHA256 string         `json:"floors_sha256,omitempty"`
	Metrics      []healthMetric `json:"metrics"`
	Verdict      string         `json:"verdict"`
}

// below is the metrics on the wrong side of their floor, re-derived from the
// values and floors by each metric's direction. A metric with an error, no
// floor, or a name the step does not measure is never below.
func (h *runnerHealth) below() []healthMetric {
	if h == nil {
		return nil
	}
	var out []healthMetric
	for _, m := range h.Metrics {
		if m.Floor == nil || m.Error != "" {
			continue
		}
		d, ok := healthMetricByName(m.Metric)
		if !ok {
			continue
		}
		if (d.Direction == "min" && m.Value < *m.Floor) || (d.Direction == "max" && m.Value > *m.Floor) {
			out = append(out, m)
		}
	}
	return out
}

// verdict re-derives the file's verdict from its values and floors.
func (h *runnerHealth) verdict() string {
	if len(h.below()) > 0 {
		return healthBelowFloor
	}
	for _, m := range h.Metrics {
		if m.Floor != nil {
			return healthAboveFloor
		}
	}
	return healthUnfloored
}

// describeBelow is the one line a problem, a rerun record and the README
// carry for a degraded runner: each metric on the wrong side of its floor,
// with its unit.
func describeBelow(below []healthMetric) string {
	parts := make([]string, 0, len(below))
	for _, m := range below {
		side := "below"
		if d, ok := healthMetricByName(m.Metric); ok && d.Direction == "max" {
			side = "above"
		}
		parts = append(parts, fmt.Sprintf("%s %s %s %s the floor %s", m.Metric, trimFloat(m.Value), m.Unit, side, trimFloat(*m.Floor)))
	}
	return strings.Join(parts, "; ")
}

func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// healthFloor is one row of scripts/proof-runner-floors.tsv.
type healthFloor struct {
	Class   string
	Metric  string
	Floor   float64
	Derived string
	Line    int
}

type healthFloors struct {
	Rows []healthFloor
}

// forClass is the class's floors by metric; empty for a class the table
// does not name.
func (f healthFloors) forClass(class string) map[string]float64 {
	out := map[string]float64{}
	for _, r := range f.Rows {
		if r.Class == class {
			out[r.Metric] = r.Floor
		}
	}
	return out
}

// readRunnerFloors reads the floors table. An absent file is an empty table;
// a malformed row is an error, because a table that cannot be read would
// otherwise read as "no floor".
func readRunnerFloors(file string) (healthFloors, error) {
	if file == "" {
		return healthFloors{}, nil
	}
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return healthFloors{}, nil
	}
	if err != nil {
		return healthFloors{}, err
	}
	defer f.Close()
	return parseRunnerFloors(f, file)
}

// parseRunnerFloors is strict: four TAB-separated columns — class, metric,
// floor, derived — a metric the step measures, a positive floor, a derivation
// that says something, and one row per (class, metric).
func parseRunnerFloors(r io.Reader, name string) (healthFloors, error) {
	var out healthFloors
	seen := map[string]int{}
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		text := sc.Text()
		if strings.TrimSpace(text) == "" || strings.HasPrefix(strings.TrimSpace(text), "#") {
			continue
		}
		f := strings.Split(text, "\t")
		if len(f) != 4 {
			return out, fmt.Errorf("%s:%d: %d columns, want 4 (class, metric, floor, derived)", name, n, len(f))
		}
		row := healthFloor{Class: strings.TrimSpace(f[0]), Metric: strings.TrimSpace(f[1]), Derived: strings.TrimSpace(f[3]), Line: n}
		if row.Class == "" || strings.ContainsAny(row.Class, " ") {
			return out, fmt.Errorf("%s:%d: class %q is empty or holds a space", name, n, f[0])
		}
		if _, ok := healthMetricByName(row.Metric); !ok {
			return out, fmt.Errorf("%s:%d: metric %q is not one the step measures (%s)", name, n, row.Metric, strings.Join(healthMetricNames(), ", "))
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(f[2]), 64)
		if err != nil || v <= 0 {
			return out, fmt.Errorf("%s:%d: floor %q is not a positive number", name, n, f[2])
		}
		row.Floor = v
		if row.Derived == "" {
			return out, fmt.Errorf("%s:%d: the row says nothing about how its floor was derived", name, n)
		}
		key := row.Class + "\x00" + row.Metric
		if prev, dup := seen[key]; dup {
			return out, fmt.Errorf("%s:%d: %s %s is already declared on line %d", name, n, row.Class, row.Metric, prev)
		}
		seen[key] = n
		out.Rows = append(out.Rows, row)
	}
	return out, sc.Err()
}

// readRunnerHealth reads a part's runner-health.json. An absent file is
// (nil, nil): a part measured before the step existed keeps its meaning.
func readRunnerHealth(dir string) (*runnerHealth, error) {
	var h runnerHealth
	err := readJSON(filepath.Join(dir, runnerHealthFile), &h)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if h.Format != runnerHealthFormat {
		return nil, fmt.Errorf("%s is format %d, and this proofrun reads format %d", runnerHealthFile, h.Format, runnerHealthFormat)
	}
	return &h, nil
}

// cmdRunnerHealth is `proofrun runner-health`: measure this host against the
// floors of -class and write -out. Exit 0 above the floor or unfloored, 1
// below it, 2 for an invalid invocation or a floors table that cannot be
// read (nothing is written then).
func cmdRunnerHealth(args []string) int {
	fs := flag.NewFlagSet("runner-health", flag.ContinueOnError)
	out := fs.String("out", "", "the runner-health.json to write (required)")
	class := fs.String("class", "", "the runner class the floors table names (required)")
	floorsFile := fs.String("floors", runnerFloorsFile, "the floors table")
	dir := fs.String("dir", "", "the directory to measure the disk in (default: -out's)")
	seconds := fs.Float64("seconds", runnerHealthSeconds, "the measurement budget, split across the timed metrics")
	if err := fs.Parse(args); err != nil || *out == "" || *class == "" || fs.NArg() > 0 || *seconds <= 0 {
		fmt.Fprintln(os.Stderr, "proofrun runner-health -out FILE -class NAME [-floors FILE] [-dir DIR] [-seconds N]")
		return 2
	}
	floors, err := readRunnerFloors(*floorsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun runner-health:", err)
		return 2
	}
	if *dir == "" {
		*dir = filepath.Dir(*out)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun runner-health:", err)
		return 2
	}
	h := measureRunnerHealth(*dir, *class, floors, time.Duration(*seconds*float64(time.Second)))
	if _, statErr := os.Stat(*floorsFile); statErr == nil {
		h.FloorsFile = *floorsFile
		h.FloorsSHA256, _ = fileSHA256(*floorsFile)
	}
	if err := writeJSON(*out, h); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun runner-health:", err)
		return 2
	}
	writeRunnerHealthSummary(os.Stdout, h)
	if h.Verdict == healthBelowFloor {
		return 1
	}
	return 0
}

// writeRunnerHealthSummary is the step's log: one line per metric and the
// verdict, the lines a part's log and a local proof's setup.log carry.
func writeRunnerHealthSummary(w io.Writer, h *runnerHealth) {
	fmt.Fprintf(w, "runner-health: class %s, %d CPUs, %d MB, measured %.1fs in %s\n", h.Class, h.Runner.CPUs, h.Runner.MemMB, h.Seconds, h.Dir)
	for _, m := range h.Metrics {
		switch {
		case m.Error != "":
			fmt.Fprintf(w, "runner-health: %s unmeasured: %s\n", m.Metric, m.Error)
		case m.Floor == nil:
			fmt.Fprintf(w, "runner-health: %s %s %s (no floor for this class)\n", m.Metric, trimFloat(m.Value), m.Unit)
		case m.Below:
			fmt.Fprintf(w, "runner-health: %s %s %s BELOW the floor %s\n", m.Metric, trimFloat(m.Value), m.Unit, trimFloat(*m.Floor))
		default:
			fmt.Fprintf(w, "runner-health: %s %s %s (floor %s)\n", m.Metric, trimFloat(m.Value), m.Unit, trimFloat(*m.Floor))
		}
	}
	fmt.Fprintf(w, "runner-health: verdict %s\n", h.Verdict)
}

// measureRunnerHealth runs every measurement and judges each against the
// class's floors. Each timed measurement takes its share of budget.
func measureRunnerHealth(dir, class string, floors healthFloors, budget time.Duration) *runnerHealth {
	started := time.Now()
	h := &runnerHealth{Format: runnerHealthFormat, Class: class, MeasuredAt: started.UTC().Format(time.RFC3339), Dir: dir, Runner: currentRunner()}
	classFloors := floors.forClass(class)
	values := map[string]healthMetric{}
	share := func(metric string) time.Duration {
		d, _ := healthMetricByName(metric)
		return time.Duration(float64(budget) * d.Share)
	}
	probe := filepath.Join(dir, ".runner-health.probe")
	defer os.Remove(probe)
	wrote, writeMB, writeSec, err := measureDiskWrite(probe, share("disk_write_mb_s"))
	values["disk_write_mb_s"] = metricOf(writeMB, writeSec, err)
	readMB, readSec, err := measureDiskRead(probe, wrote, share("disk_read_mb_s"))
	values["disk_read_mb_s"] = metricOf(readMB, readSec, err)
	p95, fsyncSec, err := measureFsync(probe, share("fsync_p95_ms"))
	values["fsync_p95_ms"] = metricOf(p95, fsyncSec, err)
	single, singleSec := measureCPU(1, share("cpu_single_mb_s"))
	values["cpu_single_mb_s"] = metricOf(single, singleSec, nil)
	all, allSec := measureCPU(runtime.NumCPU(), share("cpu_all_mb_s"))
	values["cpu_all_mb_s"] = metricOf(all, allSec, nil)
	mem, err := memAvailableMiB()
	values["mem_available_mib"] = metricOf(mem, 0, err)
	for _, d := range healthMetricDefs {
		m := values[d.Metric]
		m.Metric, m.Unit = d.Metric, d.Unit
		m.Value = roundTo(m.Value, d.Decimals)
		m.Seconds = roundTo(m.Seconds, 3)
		if floor, ok := classFloors[d.Metric]; ok {
			f := floor
			m.Floor = &f
		}
		h.Metrics = append(h.Metrics, m)
	}
	for _, b := range h.below() {
		for i := range h.Metrics {
			if h.Metrics[i].Metric == b.Metric {
				h.Metrics[i].Below = true
			}
		}
	}
	h.Verdict = h.verdict()
	h.Seconds = roundTo(time.Since(started).Seconds(), 3)
	return h
}

// roundTo rounds v to decimals places.
func roundTo(v float64, decimals int) float64 {
	f, _ := strconv.ParseFloat(strconv.FormatFloat(v, 'f', decimals, 64), 64)
	return f
}

func metricOf(value, seconds float64, err error) healthMetric {
	m := healthMetric{Value: value, Seconds: seconds}
	if err != nil {
		m.Value = 0
		m.Error = err.Error()
	}
	return m
}

// The disk measurements open the probe with O_DIRECT, so a read of the file
// just written measures the disk and never the page cache. A filesystem that
// refuses O_DIRECT (tmpfs) leaves the read unmeasured, with the reason.
const (
	directAlign = 4096
	directChunk = 4 << 20
	// directCap bounds what the write measurement leaves on the disk.
	directCap = 2 << 30
)

// alignedBuffer is a chunk aligned for O_DIRECT, filled with bytes a device
// cannot compress away.
func alignedBuffer(size int) []byte {
	raw := make([]byte, size+directAlign)
	off := int(uintptr(unsafe.Pointer(&raw[0])) % directAlign)
	if off != 0 {
		off = directAlign - off
	}
	buf := raw[off : off+size]
	seed := uint32(0x9e3779b9)
	for i := range buf {
		seed = seed*1664525 + 1013904223
		buf[i] = byte(seed >> 24)
	}
	return buf
}

// openDirect opens the probe with O_DIRECT, and without it when the
// filesystem refuses, saying so.
func openDirect(path string, flag int) (*os.File, bool, error) {
	f, err := os.OpenFile(path, flag|syscall.O_DIRECT, 0o644)
	if err == nil {
		return f, true, nil
	}
	f, err = os.OpenFile(path, flag, 0o644)
	return f, false, err
}

// measureDiskWrite writes aligned chunks for budget (at most directCap) and
// fsyncs; the throughput counts the fsync. It returns the bytes written.
func measureDiskWrite(path string, budget time.Duration) (wrote int64, mbPerSec, seconds float64, err error) {
	f, _, err := openDirect(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	buf := alignedBuffer(directChunk)
	started := time.Now()
	deadline := started.Add(budget)
	for wrote < directCap {
		n, werr := f.Write(buf)
		wrote += int64(n)
		if werr != nil {
			return wrote, 0, time.Since(started).Seconds(), werr
		}
		if time.Now().After(deadline) {
			break
		}
	}
	if err := f.Sync(); err != nil {
		return wrote, 0, time.Since(started).Seconds(), err
	}
	seconds = time.Since(started).Seconds()
	if seconds == 0 || wrote == 0 {
		return wrote, 0, seconds, fmt.Errorf("nothing was written")
	}
	return wrote, float64(wrote) / 1e6 / seconds, seconds, nil
}

// measureDiskRead reads the probe back through O_DIRECT, for budget or until
// its end.
func measureDiskRead(path string, size int64, budget time.Duration) (mbPerSec, seconds float64, err error) {
	if size == 0 {
		return 0, 0, fmt.Errorf("nothing to read: the write left no bytes")
	}
	f, direct, err := openDirect(path, os.O_RDONLY)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	if !direct {
		return 0, 0, fmt.Errorf("O_DIRECT is refused on this filesystem: a read of the file just written would measure the page cache")
	}
	buf := alignedBuffer(directChunk)
	started := time.Now()
	deadline := started.Add(budget)
	var read int64
	for read < size {
		n, rerr := f.Read(buf)
		read += int64(n)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, time.Since(started).Seconds(), rerr
		}
		if time.Now().After(deadline) {
			break
		}
	}
	seconds = time.Since(started).Seconds()
	if seconds == 0 || read == 0 {
		return 0, seconds, fmt.Errorf("nothing was read")
	}
	return float64(read) / 1e6 / seconds, seconds, nil
}

// measureFsync appends 4 KiB and fsyncs, for budget or 500 samples, and
// reports the 95th percentile latency in milliseconds.
func measureFsync(path string, budget time.Duration) (p95ms, seconds float64, err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	buf := alignedBuffer(4096)
	started := time.Now()
	deadline := started.Add(budget)
	var samples []float64
	for len(samples) < 500 {
		t := time.Now()
		if _, err := f.Write(buf); err != nil {
			return 0, time.Since(started).Seconds(), err
		}
		if err := f.Sync(); err != nil {
			return 0, time.Since(started).Seconds(), err
		}
		samples = append(samples, float64(time.Since(t))/float64(time.Millisecond))
		if time.Now().After(deadline) {
			break
		}
	}
	seconds = time.Since(started).Seconds()
	if len(samples) == 0 {
		return 0, seconds, fmt.Errorf("no fsync completed")
	}
	return percentile(samples, 95), seconds, nil
}

// percentile is the nearest-rank p-th percentile of samples, which it sorts.
func percentile(samples []float64, p int) float64 {
	sort.Float64s(samples)
	idx := (len(samples)*p + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(samples) {
		idx = len(samples)
	}
	return samples[idx-1]
}

// measureCPU hashes a fixed buffer on workers goroutines for budget and
// reports the bytes hashed per second across them, in MB/s: one worker is
// the single-core number, NumCPU workers the all-core one.
func measureCPU(workers int, budget time.Duration) (mbPerSec, seconds float64) {
	if workers < 1 {
		workers = 1
	}
	buf := alignedBuffer(1 << 20)
	var mu sync.Mutex
	var total int64
	var wg sync.WaitGroup
	started := time.Now()
	deadline := started.Add(budget)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int64
			var sink [sha256.Size]byte
			for time.Now().Before(deadline) {
				sink = sha256.Sum256(buf)
				n += int64(len(buf))
			}
			_ = sink
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	seconds = time.Since(started).Seconds()
	if seconds == 0 {
		return 0, 0
	}
	return float64(total) / 1e6 / seconds, seconds
}

// memAvailableMiB reads MemAvailable from /proc/meminfo.
func memAvailableMiB() (float64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				kb, err := strconv.ParseFloat(f[1], 64)
				if err != nil {
					return 0, err
				}
				return kb / 1024, nil
			}
		}
	}
	return 0, fmt.Errorf("/proc/meminfo has no MemAvailable line")
}

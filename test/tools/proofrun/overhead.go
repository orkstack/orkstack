package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// cmdOverhead reads an existing proof directory and writes, as Markdown,
// where its wall-clock went: each gate's seconds on the serial chain, each
// integration process's wall against the seconds its tests' ledger rows
// account for (the difference is the per-process overhead: bucket create,
// binary init, Temporal namespace registration; the bucket remove was part
// of it until the runner's bucket sweep), the heavy and
// ordinary phases' walls and slot utilisation, the two Playwright
// invocations, and any slow-boot warnings the logs carry. It runs nothing
// and never writes inside the proof directory; a missing file is a line in
// the report, never a failure.
func cmdOverhead(args []string) int {
	fs := flag.NewFlagSet("overhead", flag.ContinueOnError)
	from := fs.String("from", "", "the proof directory to read, e.g. proof/<name>")
	out := fs.String("out", "", "also write the report to this Markdown file (outside the proof directory)")
	config := fs.String("config", "ui/playwright.config.ts", "the Playwright config whose SOLO_SPECS names the solo spec files")
	if err := fs.Parse(args); err != nil || *from == "" || fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "proofrun overhead -from DIR [-out FILE.md] [-config FILE]")
		return 2
	}
	if st, err := os.Stat(*from); err != nil || !st.IsDir() {
		fmt.Fprintf(os.Stderr, "proofrun overhead: %s is not a directory\n", *from)
		return 2
	}
	if *out != "" && inside(*from, *out) {
		fmt.Fprintf(os.Stderr, "proofrun overhead: -out %s is inside the proof directory, which overhead never modifies\n", *out)
		return 2
	}
	text := overheadReport(readOverhead(*from, *config))
	fmt.Print(text)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(text), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun overhead:", err)
			return 1
		}
	}
	return 0
}

// inside reports whether file would land in dir (or is dir).
func inside(dir, file string) bool {
	d, err1 := filepath.Abs(dir)
	f, err2 := filepath.Abs(file)
	if err1 != nil || err2 != nil {
		return false
	}
	if r, err := filepath.EvalSymlinks(d); err == nil {
		d = r
	}
	if r, err := filepath.EvalSymlinks(filepath.Dir(f)); err == nil {
		f = filepath.Join(r, filepath.Base(f))
	}
	rel, err := filepath.Rel(d, f)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// overheadData is everything the report is built from.
type overheadData struct {
	Dir       string
	Name      string
	Tier      string
	ProofJobs int
	UIWorkers int
	Gates     []gateRecord
	// Shards is one entry per integration log found (a local proof has one).
	Shards  []shardTimeline
	Ledger  *ledger
	UI      *uiLedger
	Solo    []*regexp.Regexp
	Boots   []string
	Missing []string
	Notes   []string
}

// jobRun is one integration process as its start and end lines tell it.
type jobRun struct {
	Label    string
	Slot     int
	Package  string
	Heavy    bool
	Wall     float64
	Ended    bool
	Exit     string
	TestSecs float64
	Tests    int
	Measured bool
	Overhead float64
}

type shardTimeline struct {
	Name string
	Jobs []*jobRun
}

var (
	startLine = regexp.MustCompile(`^start (\S+) slot (\d+): (\S+)`)
	endLine   = regexp.MustCompile(`^end\s+(\S+) after ([0-9.]+)s \((.*)\), \d+ of \d+ tests started`)
	attempt   = regexp.MustCompile(`\.\d+$`)
	bootWarn  = regexp.MustCompile(`(?i)\bboot`)
	tookWord  = regexp.MustCompile(`(?i)\btook\b|\bslow\b`)
	warnLevel = regexp.MustCompile(`WARN`)
	soloRe    = regexp.MustCompile(`(?s)SOLO_SPECS\s*=\s*\[(.*?)\]`)
	jsRegex   = regexp.MustCompile(`/((?:\\.|[^/\\])+)/[a-z]*`)
)

func readOverhead(dir, config string) overheadData {
	d := overheadData{Dir: dir, Name: filepath.Base(filepath.Clean(dir))}
	var rec proofRecord
	if err := readJSONFile(filepath.Join(dir, "proof.json"), &rec); err != nil {
		d.Missing = append(d.Missing, "proof.json ("+errText(err)+"): PROOF_JOBS and PROOF_UI_WORKERS are taken from the logs")
	} else {
		d.Tier, d.ProofJobs, d.UIWorkers = rec.Tier, rec.ProofJobs, rec.UIWorkers
		if rec.Name != "" {
			d.Name = rec.Name
		}
	}
	if gates, err := readGates(filepath.Join(dir, "gates.tsv")); err != nil || len(gates) == 0 {
		d.Missing = append(d.Missing, "gates.tsv (absent, empty or unreadable): no gate table")
	} else {
		d.Gates = gates
	}

	heavy := map[string]bool{}
	var plan integrationPlan
	if err := readJSONFile(filepath.Join(dir, "integration-plan.json"), &plan); err != nil {
		d.Missing = append(d.Missing, "integration-plan.json ("+errText(err)+"): no job is known to be heavy, so every job is read as ordinary")
	} else {
		for _, j := range plan.Jobs {
			if j.Heavy {
				heavy[j.ID] = true
			}
		}
	}

	var l ledger
	if err := readJSONFile(filepath.Join(dir, "integration-tests.json"), &l); err != nil {
		d.Missing = append(d.Missing, "integration-tests.json ("+errText(err)+"): no job's overhead can be computed")
	} else {
		d.Ledger = &l
	}
	byJob := map[string][]entry{}
	if d.Ledger != nil {
		for _, e := range d.Ledger.Tests {
			// Subtests are inside their parent's seconds.
			if e.Job == "" || strings.Contains(e.Test, "/") {
				continue
			}
			byJob[e.Job] = append(byJob[e.Job], e)
		}
	}

	logs := integrationLogs(dir)
	if len(logs) == 0 {
		d.Missing = append(d.Missing, "integration.log (absent): no integration timeline")
	}
	for _, lf := range logs {
		name, _ := filepath.Rel(dir, lf)
		s, err := parseIntegrationLog(lf, name, heavy, byJob, d.Ledger != nil)
		if err != nil {
			d.Missing = append(d.Missing, name+" ("+errText(err)+")")
			continue
		}
		d.Shards = append(d.Shards, s)
	}
	if d.ProofJobs == 0 {
		for _, s := range d.Shards {
			for _, j := range s.Jobs {
				if j.Slot > d.ProofJobs {
					d.ProofJobs = j.Slot
				}
			}
		}
		if d.ProofJobs > 0 {
			d.Notes = append(d.Notes, fmt.Sprintf("PROOF_JOBS = %d is the highest slot the log names", d.ProofJobs))
		}
	}

	var ui uiLedger
	if err := readJSONFile(filepath.Join(dir, "ui-e2e-tests.json"), &ui); err != nil {
		d.Missing = append(d.Missing, "ui-e2e-tests.json ("+errText(err)+"): no Playwright section")
	} else {
		d.UI = &ui
	}
	if solo, err := readSoloSpecs(config); err != nil {
		d.Notes = append(d.Notes, "SOLO_SPECS not read from "+config+" ("+errText(err)+"): a file is solo when its project's name starts with chromium-solo")
	} else {
		d.Solo = solo
	}
	d.Boots = slowBoots(dir, logs)
	return d
}

func errText(err error) string {
	if os.IsNotExist(err) {
		return "absent"
	}
	return err.Error()
}

func readJSONFile(file string, v any) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("malformed: %v", err)
	}
	return nil
}

// integrationLogs is the proof's integration log, or each part's when the
// proof was merged from parts.
func integrationLogs(dir string) []string {
	if _, err := os.Stat(filepath.Join(dir, "integration.log")); err == nil {
		return []string{filepath.Join(dir, "integration.log")}
	}
	var out []string
	for _, pat := range []string{"*/integration.log", "*/*/integration.log"} {
		m, _ := filepath.Glob(filepath.Join(dir, pat))
		out = append(out, m...)
	}
	sort.Strings(out)
	return out
}

func parseIntegrationLog(file, name string, heavy map[string]bool, byJob map[string][]entry, haveLedger bool) (shardTimeline, error) {
	f, err := os.Open(file)
	if err != nil {
		return shardTimeline{}, err
	}
	defer f.Close()
	s := shardTimeline{Name: name}
	runs := map[string]*jobRun{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if m := startLine.FindStringSubmatch(line); m != nil {
			slot, _ := strconv.Atoi(m[2])
			j := &jobRun{Label: m[1], Slot: slot, Package: m[3], Heavy: heavy[attempt.ReplaceAllString(m[1], "")]}
			runs[m[1]] = j
			s.Jobs = append(s.Jobs, j)
			continue
		}
		if m := endLine.FindStringSubmatch(line); m != nil {
			j := runs[m[1]]
			if j == nil {
				continue
			}
			j.Wall, _ = strconv.ParseFloat(m[2], 64)
			j.Ended, j.Exit = true, m[3]
		}
	}
	if err := sc.Err(); err != nil {
		return s, err
	}
	for _, j := range s.Jobs {
		for _, e := range byJob[j.Label] {
			j.TestSecs += e.Seconds
			j.Tests++
		}
		j.Measured = haveLedger && j.Ended && j.Tests > 0
		if j.Measured {
			j.Overhead = j.Wall - j.TestSecs
		}
	}
	return s, nil
}

// readSoloSpecs reads the regular expressions of ui/playwright.config.ts's
// SOLO_SPECS array.
func readSoloSpecs(config string) ([]*regexp.Regexp, error) {
	b, err := os.ReadFile(config)
	if err != nil {
		return nil, err
	}
	m := soloRe.FindSubmatch(b)
	if m == nil {
		return nil, fmt.Errorf("no SOLO_SPECS array")
	}
	var out []*regexp.Regexp
	for _, r := range jsRegex.FindAllSubmatch(m[1], -1) {
		re, err := regexp.Compile(string(r[1]))
		if err != nil {
			return nil, fmt.Errorf("SOLO_SPECS entry /%s/: %v", r[1], err)
		}
		out = append(out, re)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("SOLO_SPECS is empty")
	}
	return out, nil
}

// slowBoots is every WARN line about a boot that took long, from the
// integration logs, each job's own log and the Playwright gate's log.
func slowBoots(dir string, integrationLogs []string) []string {
	files := append([]string{}, integrationLogs...)
	for _, pat := range []string{"integration/*.log", "*/integration/*.log", "ui-e2e.log"} {
		m, _ := filepath.Glob(filepath.Join(dir, pat))
		files = append(files, m...)
	}
	var out []string
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		rel, _ := filepath.Rel(dir, file)
		for sc.Scan() {
			line := sc.Text()
			if warnLevel.MatchString(line) && bootWarn.MatchString(line) && tookWord.MatchString(line) {
				out = append(out, rel+": "+strings.TrimSpace(line))
			}
		}
		f.Close()
	}
	return out
}

// ---- the report -----------------------------------------------------------

// gatePhase is a gate's place on the serial chain scripts/proof.sh runs.
var gatePhase = map[string]string{
	"lint": "fast", "build": "fast", "unit": "fast", "vet-integration": "fast", "ui-check": "fast", "ui-unit": "fast", "site": "fast",
	"temporal": "setup", "images": "setup", "binaries": "setup", "integration-compile": "setup", "setup": "setup",
	"integration": "stack", "activity-fixture": "stack",
	"ui-e2e": "ui-e2e", "observability": "observability",
}

var phaseOrder = []string{"fast", "setup", "stack", "ui-e2e", "observability", "other"}

var phaseText = map[string]string{
	"fast":          "fast gates (lint, build, unit, vet, UI check and unit, site), serial",
	"setup":         "setup (Temporal, images, binaries), serial",
	"stack":         "integration beside the activity fixture: the longer of the two",
	"ui-e2e":        "Playwright",
	"observability": "observability",
	"other":         "gates this report does not place, counted as serial",
}

// phaseSink names a phase in the summary.
var phaseSink = map[string]string{
	"fast":          "the fast gates",
	"setup":         "setup (Temporal, images, binaries)",
	"stack":         "the integration and activity-fixture gates",
	"ui-e2e":        "the Playwright gate",
	"observability": "the observability gate",
	"other":         "gates this report does not place",
}

type phaseWall struct {
	Phase   string
	Gates   []string
	Seconds float64
	// Concurrent is set on the setup phase of a proof that ran its setup as
	// one concurrent group: the phase costs the group's own `setup` row.
	Concurrent bool
}

// label is the phase's row in the critical-path table.
func (p phaseWall) label() string {
	if p.Phase == "setup" && p.Concurrent {
		return "setup (Temporal, images, binaries, integration test binaries), concurrent: the setup row's wall"
	}
	return phaseText[p.Phase]
}

// criticalPath folds the gates into the serial chain's phases. Every phase
// is serial with the next; inside the stack phase integration and the
// activity fixture run beside each other, so the phase costs the longer. A
// proof that ran its setup as one concurrent group records a `setup` row
// holding the group's wall: the phase costs that row once, and its members
// (Temporal, images, binaries, the integration test binaries) nothing more.
// An older proof has no such row and ran its setup serially.
func criticalPath(gates []gateRecord) ([]phaseWall, float64) {
	_, concurrent := gateSeconds(gates, "setup")
	by := map[string]*phaseWall{}
	for _, g := range gates {
		p := gatePhase[g.Gate]
		if p == "" {
			p = "other"
		}
		w := by[p]
		if w == nil {
			w = &phaseWall{Phase: p}
			by[p] = w
		}
		w.Gates = append(w.Gates, g.Gate)
		switch {
		case p == "stack":
			w.Seconds = math.Max(w.Seconds, g.Seconds)
		case p == "setup" && concurrent:
			w.Concurrent = true
			if g.Gate == "setup" {
				w.Seconds = g.Seconds
			}
		default:
			w.Seconds += g.Seconds
		}
	}
	var out []phaseWall
	var total float64
	for _, p := range phaseOrder {
		if w := by[p]; w != nil {
			out = append(out, *w)
			total += w.Seconds
		}
	}
	return out, total
}

type dist struct {
	N                            int
	Min, Median, P90, Max, Total float64
}

func distribution(v []float64) dist {
	if len(v) == 0 {
		return dist{}
	}
	s := append([]float64{}, v...)
	sort.Float64s(s)
	d := dist{N: len(s), Min: s[0], Max: s[len(s)-1], Median: quantile(s, 0.5), P90: quantile(s, 0.9)}
	for _, x := range s {
		d.Total += x
	}
	return d
}

// quantile is the nearest-rank quantile of sorted values.
func quantile(sorted []float64, q float64) float64 {
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i]
}

// phaseStats is one runner phase reconstructed from its jobs: a slot runs
// its jobs one after another, so its busy time is their walls' sum and the
// phase's wall is its busiest slot's.
type phaseStats struct {
	Jobs     int
	Busy     float64
	Wall     float64
	Slots    int
	Overhead float64
}

func (p phaseStats) utilisation() float64 {
	if p.Wall == 0 || p.Slots == 0 {
		return 0
	}
	return p.Busy / (float64(p.Slots) * p.Wall)
}

func runnerPhases(jobs []*jobRun, slots int) (heavy, ordinary phaseStats) {
	perSlot := map[int]float64{}
	for _, j := range jobs {
		if j.Heavy {
			heavy.Jobs++
			heavy.Busy += j.Wall
			heavy.Wall += j.Wall
			heavy.Overhead += j.Overhead
			continue
		}
		ordinary.Jobs++
		ordinary.Busy += j.Wall
		ordinary.Overhead += j.Overhead
		perSlot[j.Slot] += j.Wall
	}
	heavy.Slots = 1
	for _, w := range perSlot {
		ordinary.Wall = math.Max(ordinary.Wall, w)
	}
	ordinary.Slots = slots
	if ordinary.Slots < len(perSlot) {
		ordinary.Slots = len(perSlot)
	}
	return heavy, ordinary
}

// uiInvocation is one Playwright invocation's span.
type uiInvocation struct {
	Name      string
	Files     []uiFileEntry
	Tests     int
	Wall      float64
	FileWalls float64
	Workers   int
	Longest   float64
}

// utilisation is the share of the invocation's lane time its spec files
// held: their summed walls over the workers times the wall. A lane cannot be
// busy longer than it existed, so it is at most 1; exceeds says the summed
// walls were more than the lanes held, which only a miscounted ledger gives.
func (u uiInvocation) utilisation() (use float64, exceeds bool) {
	capacity := float64(u.Workers) * u.Wall
	if capacity <= 0 {
		return 0, false
	}
	if u.FileWalls > capacity+1e-9 {
		return 1, true
	}
	return u.FileWalls / capacity, false
}

// concurrentSetup says the proof ran its setup as one concurrent group.
func concurrentSetup(gates []gateRecord) bool {
	_, ok := gateSeconds(gates, "setup")
	return ok
}

func (u uiInvocation) ideal() float64 {
	if u.Workers == 0 {
		return 0
	}
	return math.Max(u.FileWalls/float64(u.Workers), u.Longest)
}

func isSolo(file, project string, solo []*regexp.Regexp) bool {
	if len(solo) == 0 {
		return strings.HasPrefix(project, "chromium-solo")
	}
	for _, re := range solo {
		if re.MatchString(file) {
			return true
		}
	}
	return false
}

func uiInvocations(u *uiLedger, solo []*regexp.Regexp, workers int) (par, sol uiInvocation, untimed int) {
	par = uiInvocation{Name: "parallel project", Workers: workers}
	sol = uiInvocation{Name: "solo projects", Workers: 1}
	if workers <= 1 {
		// One worker runs everything in one invocation.
		par.Name, par.Workers = "one invocation", 1
	}
	type span struct{ lo, hi float64 }
	spans := map[bool]*span{}
	soloFile := map[string]bool{}
	for _, e := range u.Tests {
		s := workers > 1 && isSolo(e.File, e.Project, solo)
		soloFile[e.File] = s
		if s {
			sol.Tests++
		} else {
			par.Tests++
		}
		if e.Start == 0 && e.Seconds == 0 {
			untimed++
			continue
		}
		sp := spans[s]
		end := e.Start + e.Seconds
		if sp == nil {
			spans[s] = &span{e.Start, end}
			continue
		}
		sp.lo, sp.hi = math.Min(sp.lo, e.Start), math.Max(sp.hi, end)
	}
	for s, sp := range spans {
		if s {
			sol.Wall = sp.hi - sp.lo
		} else {
			par.Wall = sp.hi - sp.lo
		}
	}
	for _, f := range u.Files {
		inv := &par
		if soloFile[f.File] {
			inv = &sol
		}
		inv.Files = append(inv.Files, f)
		inv.FileWalls += f.WallSeconds
		inv.Longest = math.Max(inv.Longest, f.WallSeconds)
	}
	return par, sol, untimed
}

type sink struct {
	what    string
	seconds float64
}

// overheadAnalysis is every number the report prints, derived once.
type overheadAnalysis struct {
	Phases    []phaseWall
	Total     float64
	Processes []*jobRun
	Unended   int
	Unmeasur  int
	Overhead  dist
	// HeavyOverhead and OrdinaryOverhead are slot-seconds; WallOverhead is
	// their cost in wall seconds at the proof's slots.
	HeavyOverhead, OrdinaryOverhead, WallOverhead float64
	Slots                                         int
	Runner                                        []runnerShard
	// RunnerRest is the integration gate's seconds its processes do not
	// account for (one shard only; -1 when unknown).
	RunnerRest float64
	UIPar      uiInvocation
	UISolo     uiInvocation
	UIUntimed  int
	// UIRest is the ui-e2e gate's seconds outside its tests' spans (-1 when
	// unknown).
	UIRest float64
	// Sinks decompose the critical path into parts that do not overlap.
	Sinks []sink
}

type runnerShard struct {
	Name            string
	Heavy, Ordinary phaseStats
}

func gateSeconds(gates []gateRecord, name string) (float64, bool) {
	for _, g := range gates {
		if g.Gate == name {
			return g.Seconds, true
		}
	}
	return 0, false
}

func analyse(d overheadData) overheadAnalysis {
	a := overheadAnalysis{RunnerRest: -1, UIRest: -1, Slots: d.ProofJobs}
	if a.Slots < 1 {
		a.Slots = 1
	}
	a.Phases, a.Total = criticalPath(d.Gates)
	for _, s := range d.Shards {
		a.Processes = append(a.Processes, s.Jobs...)
	}
	var ov []float64
	for _, j := range a.Processes {
		switch {
		case !j.Ended:
			a.Unended++
		case !j.Measured:
			a.Unmeasur++
		default:
			ov = append(ov, j.Overhead)
			if j.Heavy {
				a.HeavyOverhead += j.Overhead
			} else {
				a.OrdinaryOverhead += j.Overhead
			}
		}
	}
	a.Overhead = distribution(ov)
	a.WallOverhead = a.HeavyOverhead + a.OrdinaryOverhead/float64(a.Slots)
	var runnerWall float64
	for _, s := range d.Shards {
		h, o := runnerPhases(s.Jobs, d.ProofJobs)
		a.Runner = append(a.Runner, runnerShard{s.Name, h, o})
		runnerWall = math.Max(runnerWall, h.Wall+o.Wall)
	}
	integ, haveInteg := gateSeconds(d.Gates, "integration")
	if haveInteg && len(d.Shards) == 1 {
		a.RunnerRest = integ - runnerWall
	}
	if d.UI != nil {
		workers := d.UIWorkers
		if workers < 1 {
			workers = 1
		}
		a.UIPar, a.UISolo, a.UIUntimed = uiInvocations(d.UI, d.Solo, workers)
		if g, ok := gateSeconds(d.Gates, "ui-e2e"); ok {
			a.UIRest = g - a.UIPar.Wall - a.UISolo.Wall
		}
	}

	// The sinks. Each critical-path phase is one sink, except the two this
	// report can open: the stack phase and Playwright.
	for _, p := range a.Phases {
		switch {
		case p.Phase == "stack" && a.RunnerRest >= 0:
			r := a.Runner[0]
			slots := float64(r.Ordinary.Slots)
			if slots < 1 {
				slots = 1
			}
			tests := (r.Heavy.Busy - r.Heavy.Overhead) + (r.Ordinary.Busy-r.Ordinary.Overhead)/slots
			idle := r.Ordinary.Wall - r.Ordinary.Busy/slots
			a.Sinks = append(a.Sinks,
				sink{"integration test bodies", tests},
				sink{"integration per-process overhead", r.Heavy.Overhead + r.Ordinary.Overhead/slots},
				sink{fmt.Sprintf("idle integration slots (ordinary phase %s utilised)", pct(r.Ordinary.utilisation(), 1)), idle},
				sink{"integration planning, listing and ledger", a.RunnerRest})
			if p.Seconds > integ {
				a.Sinks = append(a.Sinks, sink{"the activity fixture past the integration gate", p.Seconds - integ})
			}
		case p.Phase == "ui-e2e" && a.UIRest >= 0:
			a.Sinks = append(a.Sinks,
				sink{"Playwright tests", a.UIPar.Wall + a.UISolo.Wall},
				sink{"Playwright setup outside the tests (list, plan, global setup, ledger)", a.UIRest})
		default:
			a.Sinks = append(a.Sinks, sink{phaseSink[p.Phase], p.Seconds})
		}
	}
	return a
}

func overheadReport(d overheadData) string {
	a := analyse(d)
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	w("# Proof overhead: %s\n\n", d.Name)
	tier := d.Tier
	if tier == "" {
		tier = "unknown"
	}
	w("Read from `%s` (tier %s, PROOF_JOBS %s, PROOF_UI_WORKERS %s). Nothing was run and nothing in the directory was changed.\n\n",
		d.Dir, tier, orUnknown(d.ProofJobs), orUnknown(d.UIWorkers))
	if len(d.Missing) > 0 {
		w("**Partial proof.** Not available:\n\n")
		for _, m := range d.Missing {
			w("- %s\n", m)
		}
		w("\n")
	}
	for _, n := range d.Notes {
		w("- Note: %s\n", n)
	}
	if len(d.Notes) > 0 {
		w("\n")
	}

	// 1. Gates.
	w("## Gates\n\n")
	if len(d.Gates) == 0 {
		w("No gates.tsv: no gate table.\n\n")
	} else {
		w("| Gate | Result | Seconds | Share of the critical path |\n|---|---|---:|---:|\n")
		for _, g := range d.Gates {
			note := ""
			if gatePhase[g.Gate] == "stack" {
				note = " (beside each other)"
			}
			if gatePhase[g.Gate] == "setup" && g.Gate != "setup" && concurrentSetup(d.Gates) {
				note = " (in the setup group)"
			}
			w("| %s%s | %s | %.0f | %s |\n", g.Gate, note, g.Result, g.Seconds, pct(g.Seconds, a.Total))
		}
		w("\nCritical path (each phase serial with the next):\n\n| Phase | Gates | Seconds | Share |\n|---|---|---:|---:|\n")
		for _, p := range a.Phases {
			w("| %s | %s | %.0f | %s |\n", p.label(), strings.Join(p.Gates, ", "), p.Seconds, pct(p.Seconds, a.Total))
		}
		w("| **total** | | **%.0f** (%s) | 100%% |\n\n", a.Total, minutes(a.Total))
	}

	// 2. Integration processes.
	w("## Integration processes\n\n")
	if len(a.Processes) == 0 {
		w("No integration start/end lines: no per-process table.\n\n")
	} else {
		w("%d processes. A process's overhead is its wall (its `start`/`end` lines) minus the seconds its tests' top-level ledger rows report: bucket create, binary init and Temporal namespace registration, together (the bucket remove ran here too until the runner's bucket sweep took it off the slot).\n\n", len(a.Processes))
		if a.Unended > 0 {
			w("- %d processes have a start line and no end line (stopped, or the log is cut short); they are left out.\n", a.Unended)
		}
		if a.Unmeasur > 0 {
			w("- %d ended processes have no ledger rows; their overhead is unknown and left out.\n", a.Unmeasur)
		}
		if a.Unended+a.Unmeasur > 0 {
			w("\n")
		}
		od := a.Overhead
		if od.N == 0 {
			w("No process's overhead could be computed.\n\n")
		} else {
			w("| Overhead per process | min | median | p90 | max | total |\n|---|---:|---:|---:|---:|---:|\n")
			w("| seconds (%d processes) | %.1f | %.1f | %.1f | %.1f | %.1f |\n\n", od.N, od.Min, od.Median, od.P90, od.Max, od.Total)
			w("Total overhead: **%.0f slot-seconds**, about **%s of wall** at PROOF_JOBS %d (heavy processes run alone: %.0f s; ordinary ones share %d slots: %.0f slot-s / %d).\n\n",
				od.Total, minutes(a.WallOverhead), a.Slots, a.HeavyOverhead, a.Slots, a.OrdinaryOverhead, a.Slots)
			top := append([]*jobRun{}, a.Processes...)
			sort.SliceStable(top, func(i, k int) bool {
				if top[i].Measured != top[k].Measured {
					return top[i].Measured
				}
				return top[i].Overhead > top[k].Overhead
			})
			w("The largest overheads:\n\n| Process | Package | Heavy | Wall s | Tests s | Overhead s |\n|---|---|---|---:|---:|---:|\n")
			for i, j := range top {
				if i == 5 || !j.Measured {
					break
				}
				w("| %s | %s | %s | %.1f | %.1f | %.1f |\n", j.Label, j.Package, yesNo(j.Heavy), j.Wall, j.TestSecs, j.Overhead)
			}
			w("\n")
		}
		w("### Runner phases\n\n")
		w("A slot runs its processes one after another, so a phase's wall is its busiest slot's summed walls (the gaps between processes are not in the log and are not counted).\n\n")
		w("| Log | Phase | Processes | Slots | Wall s | Busy slot-s | Utilisation | Overhead slot-s |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
		for _, r := range a.Runner {
			w("| %s | heavy (alone) | %d | 1 | %.0f | %.0f | %s | %.0f |\n", r.Name, r.Heavy.Jobs, r.Heavy.Wall, r.Heavy.Busy, pct(r.Heavy.Busy, r.Heavy.Wall), r.Heavy.Overhead)
			w("| %s | ordinary | %d | %d | %.0f | %.0f | %s | %.0f |\n", r.Name, r.Ordinary.Jobs, r.Ordinary.Slots, r.Ordinary.Wall, r.Ordinary.Busy, pct(r.Ordinary.utilisation(), 1), r.Ordinary.Overhead)
		}
		w("\n")
		if a.RunnerRest >= 0 {
			integ, _ := gateSeconds(d.Gates, "integration")
			w("The integration gate took %.0f s; its processes account for %.0f s, leaving %.0f s for planning, listing the binaries and writing the ledger.\n\n",
				integ, integ-a.RunnerRest, a.RunnerRest)
		}
	}

	// 3. Playwright.
	w("## Playwright\n\n")
	if d.UI == nil {
		w("No ui-e2e-tests.json: no Playwright section.\n\n")
	} else {
		w("| Invocation | Workers | Files | Tests | Wall s | Summed file walls s | Ideal packing s | Lane utilisation |\n|---|---:|---:|---:|---:|---:|---:|---:|\n")
		var over []uiInvocation
		for _, inv := range []uiInvocation{a.UIPar, a.UISolo} {
			if inv.Tests == 0 {
				continue
			}
			use, exceeds := inv.utilisation()
			if exceeds {
				over = append(over, inv)
			}
			w("| %s | %d | %d | %d | %.0f | %.0f | %.0f | %s |\n", inv.Name, inv.Workers, len(inv.Files), inv.Tests, inv.Wall, inv.FileWalls, inv.ideal(), pct(use, 1))
		}
		w("\nA wall is the span from its first test's start to its last test's end. Ideal packing is the larger of the summed file walls over the workers and the longest file: the wall a perfect lane packing would reach.\n")
		for _, inv := range over {
			lanes := "lanes"
			if inv.Workers == 1 {
				lanes = "lane"
			}
			w("The %s' summed file walls (%.0f s) exceed what %d %s held in %.0f s: the ledger counted time outside the invocation into its files (a ledger written before each invocation's worker lanes were kept apart charged the gap between two invocations to the next one's first file), so its utilisation reads 100%%.\n",
				inv.Name, inv.FileWalls, inv.Workers, lanes, inv.Wall)
		}
		if a.UIUntimed > 0 {
			w("%d tests carry no start time and are left out of the walls.\n", a.UIUntimed)
		}
		w("\n")
		if a.UIRest >= 0 {
			g, _ := gateSeconds(d.Gates, "ui-e2e")
			w("The ui-e2e gate took %.0f s; the invocations' tests span %.0f s, leaving %.0f s for listing, planning, the global setup (binaries, UI build, browser server) and the ledger.\n\n",
				g, a.UIPar.Wall+a.UISolo.Wall, a.UIRest)
		}
		files := append(append([]uiFileEntry{}, a.UIPar.Files...), a.UISolo.Files...)
		sort.SliceStable(files, func(i, k int) bool { return files[i].WallSeconds > files[k].WallSeconds })
		if len(files) > 0 {
			w("Longest spec files:\n\n| File | Tests | Tests s | Wall s |\n|---|---:|---:|---:|\n")
			for i, f := range files {
				if i == 5 {
					break
				}
				w("| %s | %d | %.0f | %.0f |\n", f.File, f.Tests, f.Seconds, f.WallSeconds)
			}
			w("\n")
		}
	}

	// 4. Slow boots.
	w("## Slow boots\n\n")
	if len(d.Boots) == 0 {
		w("No WARN line about a slow boot in the logs.\n\n")
	} else {
		w("%d WARN lines about a slow boot:\n\n", len(d.Boots))
		for i, l := range d.Boots {
			if i == 20 {
				w("- … %d more\n", len(d.Boots)-20)
				break
			}
			w("- `%s`\n", strings.ReplaceAll(l, "`", "'"))
		}
		w("\n")
	}

	w("## Summary\n\n%s\n", summaryParagraph(a.Sinks, a.Total))
	return b.String()
}

// summaryParagraph names the three largest time sinks.
func summaryParagraph(sinks []sink, total float64) string {
	var kept []sink
	for _, s := range sinks {
		if s.seconds > 0 {
			kept = append(kept, s)
		}
	}
	if len(kept) == 0 {
		return "Nothing in this proof directory measures where its time went."
	}
	sort.SliceStable(kept, func(i, k int) bool { return kept[i].seconds > kept[k].seconds })
	if len(kept) > 3 {
		kept = kept[:3]
	}
	var parts []string
	for _, s := range kept {
		p := fmt.Sprintf("%s (%.0f s", s.what, s.seconds)
		if total > 0 {
			p += ", " + pct(s.seconds, total) + " of the critical path"
		}
		parts = append(parts, p+")")
	}
	head := "The top time sinks are "
	if len(parts) == 1 {
		head = "The top time sink is "
	}
	text := head + joinAnd(parts) + "."
	if total > 0 {
		text = fmt.Sprintf("The proof's critical path is %s. ", minutes(total)) + text
	}
	return text
}

func joinAnd(p []string) string {
	switch len(p) {
	case 1:
		return p[0]
	case 2:
		return p[0] + " and " + p[1]
	}
	return strings.Join(p[:len(p)-1], ", ") + " and " + p[len(p)-1]
}

func pct(part, whole float64) string {
	if whole <= 0 {
		return "–"
	}
	return fmt.Sprintf("%.0f%%", 100*part/whole)
}

func minutes(s float64) string {
	if s < 90 {
		return fmt.Sprintf("%.0f s", s)
	}
	return fmt.Sprintf("%.1f min", s/60)
}

func orUnknown(n int) string {
	if n == 0 {
		return "unknown"
	}
	return strconv.Itoa(n)
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var uiT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// uiTestSpec is one passing test of a fixture report, starting at start
// seconds after uiT0 and lasting dur seconds.
type uiTestSpec struct {
	file, title string
	start, dur  float64
}

// fixtureReport lays tests out as the JSON reporter does: one suite per file,
// each test carrying its own start time and duration.
func fixtureReport(tests ...uiTestSpec) pwReport {
	var r pwReport
	idx := map[string]int{}
	for _, tc := range tests {
		i, ok := idx[tc.file]
		if !ok {
			i = len(r.Suites)
			idx[tc.file] = i
			r.Suites = append(r.Suites, pwSuite{Title: tc.file, File: tc.file})
		}
		start := uiT0.Add(time.Duration(tc.start * float64(time.Second)))
		r.Suites[i].Specs = append(r.Suites[i].Specs, pwSpec{Title: tc.title, File: tc.file, Tests: []pwTest{{
			ProjectName: "chromium", Status: "expected",
			Results: []pwResult{{Status: "passed", Duration: tc.dur * 1000, StartTime: start.Format(time.RFC3339Nano)}},
		}}})
	}
	return r
}

// interleaved runs a.spec.ts and b.spec.ts' tests alternately, with a 2 s gap
// before b's first test and a 3 s gap before a's second.
func interleaved() pwReport {
	return fixtureReport(
		uiTestSpec{"a.spec.ts", "one", 0, 10},
		uiTestSpec{"b.spec.ts", "one", 12, 5},
		uiTestSpec{"a.spec.ts", "two", 20, 4},
		uiTestSpec{"b.spec.ts", "two", 24, 6},
	)
}

func listingOf(files map[string]int) pwReport {
	var tests []uiTestSpec
	for f, n := range files {
		for i := 0; i < n; i++ {
			tests = append(tests, uiTestSpec{f, fmt.Sprintf("t%d", i), 0, 1})
		}
	}
	return fixtureReport(tests...)
}

func TestUIPlan_SplitsSpecFilesByCostNeverByCount(t *testing.T) {
	costs := map[string]float64{"big.spec.ts": 500, "a.spec.ts": 100, "b.spec.ts": 100, "c.spec.ts": 100, "d.spec.ts": 100, "e.spec.ts": 100}
	got := splitSpecs(costs, 2)
	want := []uiShard{
		{Index: 1, Files: []string{"big.spec.ts"}, EstimatedSeconds: 500},
		{Index: 2, Files: []string{"a.spec.ts", "b.spec.ts", "c.spec.ts", "d.spec.ts", "e.spec.ts"}, EstimatedSeconds: 500},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shards = %+v, want %+v (a split by count would give three files each)", got, want)
	}
}

func TestUIPlan_EveryListedFileInExactlyOneShard(t *testing.T) {
	listing := listingOf(map[string]int{"a.spec.ts": 3, "b.spec.ts": 1, "c.spec.ts": 2, "d.spec.ts": 4, "e.spec.ts": 1})
	for m := 1; m <= 8; m++ {
		p := buildUIPlan(listing, []byte("x"), map[string]float64{"a.spec.ts": 40, "c.spec.ts": 10}, "costs.tsv", "sha", m)
		seen := map[string]int{}
		for _, s := range p.Shards {
			for _, f := range s.Files {
				seen[f]++
			}
		}
		if len(seen) != 5 {
			t.Errorf("M=%d: %d files in shards, want 5: %v", m, len(seen), seen)
		}
		for f, n := range seen {
			if n != 1 {
				t.Errorf("M=%d: %s is in %d shards", m, f, n)
			}
		}
		if want := min(m, 5); len(p.Shards) != want {
			t.Errorf("M=%d: %d shards, want %d (clamped to the number of files)", m, len(p.Shards), want)
		}
		for _, f := range p.Files {
			if f.Shard < 1 || f.Shard > len(p.Shards) {
				t.Errorf("M=%d: %s has shard %d", m, f.File, f.Shard)
			}
		}
	}
}

func TestUIPlan_AnUnmeasuredSpecCostsTheDefault(t *testing.T) {
	listing := listingOf(map[string]int{"known.spec.ts": 1, "new.spec.ts": 2})
	p := buildUIPlan(listing, []byte("x"), map[string]float64{"known.spec.ts": 30}, "c", "s", 1)
	byFile := map[string]uiPlanFile{}
	for _, f := range p.Files {
		byFile[f.File] = f
	}
	if f := byFile["new.spec.ts"]; f.EstimatedSeconds != 160 || f.Measured || f.Tests != 2 {
		t.Errorf("new.spec.ts = %+v, want 160 s, unmeasured, 2 tests", f)
	}
	if f := byFile["known.spec.ts"]; f.EstimatedSeconds != 30 || !f.Measured {
		t.Errorf("known.spec.ts = %+v", f)
	}
	if p.Shards[0].EstimatedSeconds != 190 {
		t.Errorf("shard estimate = %v, want 190", p.Shards[0].EstimatedSeconds)
	}
}

func TestUIPlan_EachShardKeepsTheLocalFileOrder(t *testing.T) {
	costs := map[string]float64{"z.spec.ts": 90, "m.spec.ts": 80, "a.spec.ts": 70, "q.spec.ts": 10, "b.spec.ts": 5}
	for _, s := range splitSpecs(costs, 2) {
		if !sortedStrings(s.Files) {
			t.Errorf("shard %d runs %v, want path order", s.Index, s.Files)
		}
	}
}

func sortedStrings(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i-1] > s[i] {
			return false
		}
	}
	return true
}

func TestUIPlan_IsDeterministic(t *testing.T) {
	costs := map[string]float64{}
	for i := 0; i < 30; i++ {
		costs[fmt.Sprintf("f%02d.spec.ts", i)] = float64(10 + i%4*5) // many ties
	}
	first := splitSpecs(costs, 5)
	for i := 0; i < 20; i++ {
		// Go randomises map iteration, so every call sees another order.
		if got := splitSpecs(costs, 5); !reflect.DeepEqual(got, first) {
			t.Fatalf("call %d differs:\n%+v\n%+v", i, got, first)
		}
	}
}

func TestUIPlan_CommandWritesThePlanAndTheMatrix(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	listing := listingOf(map[string]int{"a.spec.ts": 2, "b.spec.ts": 1, "c.spec.ts": 1})
	listFile := filepath.Join(dir, "list.json")
	if err := writeJSON(listFile, listing); err != nil {
		t.Fatal(err)
	}
	costsFile := filepath.Join(dir, "costs.tsv")
	if err := os.WriteFile(costsFile, []byte("# source: x\na.spec.ts\t300.00\nb.spec.ts\t100.00\nc.spec.ts\t100.00\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, matrix := filepath.Join(dir, "ui-plan.json"), filepath.Join(dir, "matrix.json")
	if code := cmdUIPlan([]string{"-list", listFile, "-costs", costsFile, "-shards", "5", "-out", out, "-matrix-out", matrix}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	b, err := os.ReadFile(matrix)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(b)) != "[1,2,3]" {
		t.Errorf("matrix = %s, want [1,2,3] (clamped to three files)", b)
	}
	var p uiPlan
	if err := readJSON(out, &p); err != nil {
		t.Fatal(err)
	}
	if p.Format != 1 || len(p.Tests) != 4 || p.RequestedShards != 5 || p.Costs.SHA256 == "" || p.ListingSHA256 == "" {
		t.Errorf("plan = %+v", p)
	}
	if code := cmdUIPlan([]string{"-list", listFile, "-out", out, "-shards", "0"}); code != 2 {
		t.Errorf("-shards 0: exit %d, want 2", code)
	}
}

func TestCosts_AFilesWallIncludesTheGapBeforeEachOfItsTests(t *testing.T) {
	_, tests := flattenPW(interleaved())
	var entries []uiEntry
	for _, k := range []string{"a.spec.ts › one [chromium]", "b.spec.ts › one [chromium]", "a.spec.ts › two [chromium]", "b.spec.ts › two [chromium]"} {
		entries = append(entries, tests[k])
	}
	got := fileWalls(entries)
	want := map[string]float64{"a.spec.ts": 17, "b.spec.ts": 13}
	if len(got) != 2 {
		t.Fatalf("files = %+v", got)
	}
	for _, f := range got {
		if math.Abs(f.WallSeconds-want[f.File]) > 1e-6 {
			t.Errorf("%s wall = %v, want %v (tests %v + gaps)", f.File, f.WallSeconds, want[f.File], f.Seconds)
		}
	}
	if got[0].Seconds != 14 || got[1].Seconds != 11 || got[0].Tests != 2 {
		t.Errorf("sums = %+v", got)
	}
}

func TestUILedger_RecordsEachSpecFilesWall(t *testing.T) {
	report := interleaved()
	l := buildUILedger(report, report, 0, 3)
	if len(l.Problems) != 0 {
		t.Fatalf("problems %v", l.Problems)
	}
	want := map[string]float64{"a.spec.ts": 17, "b.spec.ts": 13}
	if len(l.Files) != 2 {
		t.Fatalf("files = %+v", l.Files)
	}
	for _, f := range l.Files {
		if math.Abs(f.WallSeconds-want[f.File]) > 1e-6 {
			t.Errorf("%s wall = %v, want %v", f.File, f.WallSeconds, want[f.File])
		}
	}
	starts := map[string]float64{}
	for _, e := range l.Tests {
		starts[e.File+e.Title] = e.Start
	}
	if starts["a.spec.tsone"] != 0 || starts["b.spec.tsone"] != 12 || starts["a.spec.tstwo"] != 20 {
		t.Errorf("starts = %v, want seconds from the first test", starts)
	}
}

func TestUILedger_CommandRecordsTheShard(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	report := interleaved()
	for name, v := range map[string]any{"list.json": report, "report.json": report} {
		if err := writeJSON(filepath.Join(dir, name), v); err != nil {
			t.Fatal(err)
		}
	}
	cfg := filepath.Join(dir, "playwright.config.ts")
	if err := os.WriteFile(cfg, []byte("const NETWORK_CHANGE_RETRY_CEILING = 3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code := cmdUILedger([]string{"-list", filepath.Join(dir, "list.json"), "-report", filepath.Join(dir, "report.json"),
		"-config", cfg, "-out", dir, "-shard", "4"})
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var l uiLedger
	if err := readJSON(filepath.Join(dir, "ui-e2e-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	if len(l.Files) != 2 || l.Files[0].Shard != 4 || l.Files[1].Shard != 4 {
		t.Errorf("files = %+v, want both on shard 4", l.Files)
	}
}

// costsDir writes the pieces of a source proof `costs` reads.
func costsDir(t *testing.T, ledgerFiles bool, withReport bool) string {
	t.Helper()
	dir := t.TempDir()
	report := interleaved()
	l := buildUILedger(report, report, 0, 3)
	if !ledgerFiles {
		l.Files = nil
	}
	proof := proofRecord{Tier: "full", Name: "src", Commit: "0123456789abcdef0123"}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(writeJSON(filepath.Join(dir, "proof.json"), proof))
	must(writeJSON(filepath.Join(dir, "ui-e2e-tests.json"), l))
	must(writeJSON(filepath.Join(dir, "integration-tests.json"), ledger{Suite: "integration", Tests: []entry{{Package: "p", Test: "TestX", Outcome: outcomePass, Seconds: 1}}}))
	if withReport {
		must(writeJSON(filepath.Join(dir, "ui-e2e-report.json"), report))
	}
	return dir
}

func runCosts(t *testing.T, from, uiOut string) string {
	t.Helper()
	quiet(t)
	d := t.TempDir()
	code := cmdCosts([]string{"-from", from, "-costs-out", filepath.Join(d, "costs.tsv"), "-slow-out", filepath.Join(d, "slow.txt"), "-ui-costs-out", uiOut})
	if code != 0 {
		t.Fatalf("costs exit %d", code)
	}
	b, _ := os.ReadFile(uiOut)
	return string(b)
}

func TestCosts_RefreshesTheUICostsFromAMergedArtifactAlone(t *testing.T) {
	merged := costsDir(t, true, false) // proof.json and a ledger with files: nothing else
	older := costsDir(t, false, true)  // a ledger before files, and the report beside it
	fromMerged := runCosts(t, merged, filepath.Join(t.TempDir(), "ui.tsv"))
	fromReport := runCosts(t, older, filepath.Join(t.TempDir(), "ui.tsv"))
	if fromMerged != fromReport {
		t.Errorf("the merged artifact gives\n%s\nthe report gives\n%s", fromMerged, fromReport)
	}
	for _, want := range []string{"# source: proof/src (commit 0123456789ab)", "a.spec.ts\t17.00", "b.spec.ts\t13.00"} {
		if !strings.Contains(fromMerged, want) {
			t.Errorf("costs lack %q:\n%s", want, fromMerged)
		}
	}
}

func TestCosts_ASourceWithoutFileWallsLeavesTheUICostsAlone(t *testing.T) {
	src := costsDir(t, false, false)
	ui := filepath.Join(t.TempDir(), "ui.tsv")
	const before = "# source: earlier\nkeep.spec.ts\t42.00\n"
	if err := os.WriteFile(ui, []byte(before), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := runCosts(t, src, ui); got != before {
		t.Errorf("the UI costs changed:\n%s", got)
	}
}

func TestCosts_ARoundTripThroughTheUICostsFile(t *testing.T) {
	var b strings.Builder
	writeUICosts(&b, []uiFileEntry{{File: "b.spec.ts", WallSeconds: 2.5}, {File: "a.spec.ts", WallSeconds: 100}}, "proof/x")
	m, source, err := parseUICosts(strings.NewReader(b.String()), "t")
	if err != nil || source != "proof/x" || m["a.spec.ts"] != 100 || m["b.spec.ts"] != 2.5 {
		t.Errorf("round trip = %v %q %v\n%s", m, source, err, b.String())
	}
	if _, _, err := parseUICosts(strings.NewReader("a.spec.ts\n"), "t"); err == nil {
		t.Error("a row with no seconds was accepted")
	}
}

func TestUILedger_ADeclaredRunnerSizeMissExitsCleanly(t *testing.T) {
	code, l := runUILedgerMiss(t, "ui-e2e:3", "failed", pwTimingMessage, hsRow, hsRow)
	if code != 0 {
		t.Fatalf("exit %d, want 0 for a declared runner-size miss on a CI UI shard", code)
	}
	if len(l.Problems) != 0 || len(l.KnownGaps) != 1 || len(l.HostSpeedFailures) != 1 {
		t.Fatalf("ledger = %+v", l)
	}
	if !strings.Contains(l.KnownGaps[0], "8123") || l.HostSpeedFailures[0].Observed != 8.123 {
		t.Errorf("the gap lacks the measured number: %q, %+v", l.KnownGaps[0], l.HostSpeedFailures[0])
	}
}

// runUILedgerMiss runs ui-ledger over one listed test of e2e/a.spec.ts that

// ended with status and message, under a host-speed table whose reference

// CPU count is above any host's, and returns its exit and ledger.

func runUILedgerMiss(t *testing.T, part, status, message, tree, base string) (int, uiLedger) {
	t.Helper()
	quiet(t)
	dir := t.TempDir()
	listed := listingOf(map[string]int{"e2e/a.spec.ts": 1})
	report := fixtureReport(uiTestSpec{"e2e/a.spec.ts", "t0", 0, 8})
	tc := &report.Suites[0].Specs[0].Tests[0]
	switch status {
	case "skipped":
		tc.Status = "skipped"
		tc.Results[0].Status = "skipped"
	case "none":
		report.Suites = nil
	default:
		tc.Status = "unexpected"
		tc.Results[0].Status = "failed"
		tc.Results[0].Error = &pwError{Message: message}
	}
	for name, v := range map[string]any{"list.json": listed, "report.json": report} {
		if err := writeJSON(filepath.Join(dir, name), v); err != nil {
			t.Fatal(err)
		}
	}
	cfg := filepath.Join(dir, "playwright.config.ts")
	host := filepath.Join(dir, "host-speed.tsv")
	hostBase := filepath.Join(dir, "host-speed-base.txt")
	for file, text := range map[string]string{cfg: "const NETWORK_CHANGE_RETRY_CEILING = 3\n", host: tree, hostBase: base} {
		if err := os.WriteFile(file, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	code := cmdUILedger([]string{"-list", filepath.Join(dir, "list.json"), "-report", filepath.Join(dir, "report.json"),
		"-config", cfg, "-out", dir, "-host-speed", host, "-host-speed-base", hostBase, "-part", part})
	var l uiLedger
	if err := readJSON(filepath.Join(dir, "ui-e2e-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	return code, l
}

// pwTimingMessage is Playwright's message for a failed toBeLessThanOrEqual.
const pwTimingMessage = "Error: live run page\n\nexpect(received).toBeLessThanOrEqual(expected)\n\nExpected: <= 5000\nReceived:    8123"

// hsRow declares e2e/a.spec.ts › t0 with a reference no host reaches.
const hsRow = "ui\te2e/a.spec.ts\tt0\tlessThan 5000\t5\t9999\tdeclared budget\n"

// Everything but the bound's own timing miss, on a CI UI shard, under a row

// the base carries, stays a shard failure (R2-M1).

func TestUILedger_OnlyTheBoundsOwnTimingMissExitsCleanly(t *testing.T) {
	cases := []struct {
		name, part, status, message, tree, base string
	}{
		{"a local proof", "local", "failed", pwTimingMessage, hsRow, hsRow},
		{"another assertion", "ui-e2e:3", "failed", "Error: expect(locator).toBeVisible() failed\n\nLocator: getByRole('row')", hsRow, hsRow},
		{"a test timeout", "ui-e2e:3", "failed", "Test timeout of 30000ms exceeded.", hsRow, hsRow},
		{"a skip", "ui-e2e:3", "skipped", "", hsRow, hsRow},
		{"no outcome", "ui-e2e:3", "none", "", hsRow, hsRow},
		{"a row the base lacks", "ui-e2e:3", "failed", pwTimingMessage, hsRow, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, l := runUILedgerMiss(t, c.part, c.status, c.message, c.tree, c.base)
			if code != 1 || len(l.Problems) == 0 || len(l.KnownGaps) != 0 || len(l.HostSpeedFailures) != 0 {
				t.Errorf("exit %d, ledger problems %v gaps %v", code, l.Problems, l.KnownGaps)
			}
		})
	}
}

// A row declaring the header's visibility timeout lets exactly that failure
// exit cleanly on a CI UI shard; another locator's timeout does not.
func TestUILedger_ADeclaredVisibilityTimeoutExitsCleanlyAndNoOther(t *testing.T) {
	row := "ui\te2e/a.spec.ts\tt0\tvisibleWithin 60000 getByTestId('run-status')\t5\t9999\tthe header never drew\n"
	msg := func(locator string) string {
		return "Error: expect(locator).toBeVisible() failed\n\nLocator: " + locator + "\nExpected: visible\nTimeout: 60000ms\nError: element(s) not found\n"
	}
	code, l := runUILedgerMiss(t, "ui-e2e:1", "failed", msg("getByTestId('run-status')"), row, row)
	if code != 0 || len(l.KnownGaps) != 1 || l.HostSpeedFailures[0].Assertion != "visibleWithin 60000 getByTestId('run-status')" {
		t.Fatalf("the declared timeout: exit %d, %+v", code, l)
	}
	code, l = runUILedgerMiss(t, "ui-e2e:1", "failed", msg("getByTestId('task-t3999-inspect')"), row, row)
	if code != 1 || len(l.KnownGaps) != 0 {
		t.Errorf("another locator's timeout: exit %d, gaps %v", code, l.KnownGaps)
	}
	code, l = runUILedgerMiss(t, "local", "failed", msg("getByTestId('run-status')"), row, row)
	if code != 1 || len(l.KnownGaps) != 0 {
		t.Errorf("a local proof: exit %d, gaps %v", code, l.KnownGaps)
	}
}

// With several workers, the gap before a test is measured within its own
// lane: the other lane's test that ended last says nothing about what this
// file paid in beforeAll. Here a.spec.ts runs in lane 0 and b.spec.ts in
// lane 1, interleaved in time exactly as interleaved() is; measured on one
// global timeline their walls would be 17 and 13, losing most of each file's
// start-up to the other file.
func TestCosts_AFilesWallIsMeasuredWithinItsOwnWorkerLane(t *testing.T) {
	entry := func(file, title string, lane int, start, dur float64) uiEntry {
		return uiEntry{File: file, Title: title, Project: "chromium", Outcome: outcomePass, Seconds: dur,
			abs: 1_000 + start, endAbs: 1_000 + start + dur, lane: lane}
	}
	got := fileWalls([]uiEntry{
		entry("a.spec.ts", "one", 0, 0, 10),
		entry("b.spec.ts", "one", 1, 12, 5),
		entry("a.spec.ts", "two", 0, 20, 4),
		entry("b.spec.ts", "two", 1, 24, 6),
	})
	want := map[string]float64{"a.spec.ts": 14 + 10, "b.spec.ts": 11 + 7}
	if len(got) != 2 {
		t.Fatalf("files = %+v", got)
	}
	for _, f := range got {
		if math.Abs(f.WallSeconds-want[f.File]) > 1e-6 {
			t.Errorf("%s wall = %v, want %v (its tests %v plus the gap before its second test in its own lane)", f.File, f.WallSeconds, want[f.File], f.Seconds)
		}
	}
}

// The lane comes from the report: a result's parallelIndex.
func TestUILedger_ReadsEachResultsWorkerLane(t *testing.T) {
	report := interleaved()
	report.Suites[1].Specs[0].Tests[0].Results[0].ParallelIndex = 2
	_, tests := flattenPW(report)
	if got := tests["b.spec.ts › one [chromium]"].lane; got != 2 {
		t.Errorf("lane = %d, want 2", got)
	}
	if got := tests["a.spec.ts › one [chromium]"].lane; got != 0 {
		t.Errorf("lane = %d, want 0", got)
	}
}

// A proof with several workers runs the suite as two invocations — the
// parallel project, then the solo projects on one worker — and the ledger
// reads both reports as one run: every listed test finds its outcome.
func TestUILedger_ReadsSeveralReportsAsOneRun(t *testing.T) {
	a := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10})
	b := fixtureReport(uiTestSpec{"solo.spec.ts", "budget", 20, 4})
	list := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10}, uiTestSpec{"solo.spec.ts", "budget", 20, 4})
	merged := mergeReports(a, b)
	l := buildUILedger(list, merged, 0, 0)
	if len(l.Problems) != 0 || l.Pass != 2 || l.Listed != 2 {
		t.Fatalf("ledger pass %d of %d, problems %v", l.Pass, l.Listed, l.Problems)
	}
	half := buildUILedger(list, a, 0, 0)
	if half.Pass != 1 || len(half.Problems) == 0 {
		t.Errorf("with one report only, the solo test must have no outcome: pass %d, problems %v", half.Pass, half.Problems)
	}
}

// A proof with several workers runs Playwright as two invocations whose
// worker 0 lanes are different lanes: the gap between the parallel
// invocation's last test and the solo invocation's first (the second
// invocation's start, its global setup) belongs to no file.
func TestUILedger_ALaneNeverSpansTwoInvocations(t *testing.T) {
	par := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10})
	solo := fixtureReport(uiTestSpec{"solo.spec.ts", "budget", 60, 4}, uiTestSpec{"solo.spec.ts", "again", 66, 4})
	_, tests := flattenPW(mergeReports(par, solo))
	entries := make([]uiEntry, 0, len(tests))
	for _, e := range tests {
		entries = append(entries, e)
	}
	for _, f := range fileWalls(entries) {
		want := map[string]float64{"a.spec.ts": 10, "solo.spec.ts": 10}[f.File]
		if math.Abs(f.WallSeconds-want) > 1e-6 {
			t.Errorf("%s wall = %v, want %v (its tests plus the 2 s gap inside its own invocation)", f.File, f.WallSeconds, want)
		}
	}
}

// The nightly spec list decides the plan's files by tier: the full tier
// (defer) leaves out the specs both lists carry and records them as
// deferred, its tests still planned so the ledger can name them; the nightly
// tier (only) plans the listed specs alone; the pre-release gate (include)
// plans everything.
func TestUIPlan_TheNightlySpecListDecidesTheFilesByTier(t *testing.T) {
	listing := fixtureReport(
		uiTestSpec{"a.spec.ts", "one", 0, 10},
		uiTestSpec{"run-page-ceiling.spec.ts", "at 4,000", 0, 400},
		uiTestSpec{"run-page-reduced-ceiling.spec.ts", "at 1,000", 0, 100},
	)
	row, err := parseNightlySpecs(strings.NewReader(specRow+"\n"), "list")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name         string
		mode         string
		base         []nightlySpecEntry
		files, tests []string
		deferred     []string
	}{
		{name: "include", mode: nightlyInclude, base: row,
			files: []string{"a.spec.ts", "run-page-ceiling.spec.ts", "run-page-reduced-ceiling.spec.ts"},
			tests: []string{"a.spec.ts", "run-page-ceiling.spec.ts", "run-page-reduced-ceiling.spec.ts"}},
		{name: "defer", mode: nightlyDefer, base: row,
			files:    []string{"a.spec.ts", "run-page-reduced-ceiling.spec.ts"},
			tests:    []string{"a.spec.ts", "run-page-ceiling.spec.ts", "run-page-reduced-ceiling.spec.ts"},
			deferred: []string{"run-page-ceiling.spec.ts"}},
		{name: "defer, a row the base lacks is run", mode: nightlyDefer, base: nil,
			files: []string{"a.spec.ts", "run-page-ceiling.spec.ts", "run-page-reduced-ceiling.spec.ts"},
			tests: []string{"a.spec.ts", "run-page-ceiling.spec.ts", "run-page-reduced-ceiling.spec.ts"}},
		{name: "only", mode: nightlyOnly, base: row,
			files: []string{"run-page-ceiling.spec.ts"},
			tests: []string{"run-page-ceiling.spec.ts"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, problems := buildUIPlanNightly(listing, []byte("x"), nil, "costs", "sha", 2,
				uiNightlyInput{Mode: c.mode, File: "tree.txt", Tree: row, BaseFile: "base.txt", Base: c.base})
			if len(problems) != 0 {
				t.Fatalf("problems = %v", problems)
			}
			var files, tests, sharded, deferred []string
			for _, f := range p.Files {
				files = append(files, f.File)
			}
			for _, s := range p.Shards {
				sharded = append(sharded, s.Files...)
			}
			seen := map[string]bool{}
			for _, x := range p.Tests {
				if !seen[x.File] {
					seen[x.File] = true
					tests = append(tests, x.File)
				}
			}
			for _, d := range p.Deferred {
				deferred = append(deferred, d.File)
				if d.Entry != specRow {
					t.Errorf("deferred %s carries entry %q", d.File, d.Entry)
				}
			}
			sort.Strings(sharded)
			sort.Strings(tests)
			if !reflect.DeepEqual(files, c.files) || !reflect.DeepEqual(sharded, c.files) {
				t.Errorf("files = %v, sharded %v, want %v", files, sharded, c.files)
			}
			if !reflect.DeepEqual(tests, c.tests) {
				t.Errorf("planned tests' files = %v, want %v", tests, c.tests)
			}
			if !reflect.DeepEqual(orEmpty(deferred), orEmpty(c.deferred)) {
				t.Errorf("deferred = %v, want %v", deferred, c.deferred)
			}
			if p.Nightly == nil || p.Nightly.Mode != c.mode || p.Nightly.File != "tree.txt" {
				t.Errorf("nightly = %+v", p.Nightly)
			}
		})
	}
	// A row naming a spec the listing does not hold fails the plan.
	stale, _ := parseNightlySpecs(strings.NewReader("ui/e2e/gone.spec.ts\tui/e2e/a.spec.ts\t2026-10-26 Daniel Santos\tr\n"), "list")
	if _, problems := buildUIPlanNightly(listing, []byte("x"), nil, "costs", "sha", 1,
		uiNightlyInput{Mode: nightlyDefer, Tree: stale, Base: stale}); len(problems) != 1 || !strings.Contains(problems[0], "gone.spec.ts") {
		t.Errorf("a stale row's problems = %v", problems)
	}
}

// A spec file the UI plan defers ran nowhere: its listed tests are recorded
// as deferred with the list row, and one that has an outcome all the same is
// a problem.
func TestUILedger_RecordsThePlansDeferredSpecsAsDeferred(t *testing.T) {
	listing := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10}, uiTestSpec{"run-page-ceiling.spec.ts", "at 4,000", 0, 400})
	deferred := map[string]string{"run-page-ceiling.spec.ts": specRow}
	l := buildUILedgerDeferring(listing, fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10}), 0, 0, deferred)
	if len(l.Problems) != 0 || l.Pass != 1 || l.Deferred != 1 || l.Listed != 2 {
		t.Fatalf("pass %d deferred %d of %d, problems %v", l.Pass, l.Deferred, l.Listed, l.Problems)
	}
	for _, e := range l.Tests {
		if e.File == "run-page-ceiling.spec.ts" && (e.Outcome != "deferred" || e.Detail != specRow) {
			t.Errorf("deferred entry = %+v", e)
		}
	}
	ran := buildUILedgerDeferring(listing, listing, 0, 0, deferred)
	if got := strings.Join(ran.Problems, "\n"); !strings.Contains(got, "run-page-ceiling.spec.ts › at 4,000 [chromium] ran, and the UI plan defers its spec file") {
		t.Errorf("a deferred spec that ran is not a problem: %v", ran.Problems)
	}
}

// The command reads the two lists, writes the deferral into the plan and
// names the files a run passes Playwright; defer needs the base's copy.
func TestUIPlan_CommandDefersAndNamesTheFilesToRun(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	listFile := filepath.Join(dir, "list.json")
	if err := writeJSON(listFile, listingOf(map[string]int{"a.spec.ts": 1, "run-page-ceiling.spec.ts": 1, "run-page-reduced-ceiling.spec.ts": 1})); err != nil {
		t.Fatal(err)
	}
	tree, base := filepath.Join(dir, "specs.txt"), filepath.Join(dir, "specs-base.txt")
	for _, f := range []string{tree, base} {
		if err := os.WriteFile(f, []byte(specRow+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out, run := filepath.Join(dir, "ui-plan.json"), filepath.Join(dir, "run.txt")
	plan := func(mode string, extra ...string) (int, string, uiPlan) {
		args := append([]string{"-list", listFile, "-out", out, "-shards", "1", "-nightly-mode", mode, "-nightly-specs", tree, "-run-files-out", run}, extra...)
		code := cmdUIPlan(args)
		b, _ := os.ReadFile(run)
		var p uiPlan
		_ = readJSON(out, &p)
		return code, string(b), p
	}
	code, files, p := plan(nightlyDefer, "-nightly-specs-base", base)
	if code != 0 || files != "e2e/a.spec.ts\ne2e/run-page-reduced-ceiling.spec.ts\n" || len(p.Deferred) != 1 || p.Nightly.BaseSHA256 == "" {
		t.Errorf("defer: exit %d, run files %q, plan %+v", code, files, p)
	}
	if code, files, _ := plan(nightlyOnly); code != 0 || files != "e2e/run-page-ceiling.spec.ts\n" {
		t.Errorf("only: exit %d, run files %q", code, files)
	}
	if code, files, p := plan(nightlyInclude); code != 0 || files != "" || len(p.Deferred) != 0 {
		t.Errorf("include: exit %d, run files %q, deferred %v", code, files, p.Deferred)
	}
	if code, _, _ := plan(nightlyDefer); code != 2 {
		t.Errorf("defer without the base's copy: exit %d, want 2", code)
	}
}

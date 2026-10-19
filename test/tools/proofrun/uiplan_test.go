package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
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

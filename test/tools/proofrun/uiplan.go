package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// The UI plan splits Playwright spec files across runners by their measured
// wall time, never by count and never with Playwright's own --shard: a spec
// file's wall is its tests' seconds plus the gap before each of them, which is
// where page start, per-file fixtures and beforeAll land (plan §1.3 F15).

// unmeasuredSpecSeconds is what a spec file with no measurement costs: above
// every measured file's wall but run-page-ceiling.spec.ts, so a new file is
// never packed as if it were free.
const unmeasuredSpecSeconds = 160.0

type uiShard struct {
	Index            int      `json:"index"`
	Files            []string `json:"files"`
	EstimatedSeconds float64  `json:"estimated_seconds"`
}

type uiPlanTest struct {
	File    string `json:"file"`
	Title   string `json:"title"`
	Project string `json:"project"`
}

type uiPlanFile struct {
	File             string  `json:"file"`
	Tests            int     `json:"tests"`
	EstimatedSeconds float64 `json:"estimated_seconds"`
	Measured         bool    `json:"measured"`
	Shard            int     `json:"shard"`
}

type uiPlan struct {
	Format int `json:"format"`
	Costs  struct {
		File   string `json:"file"`
		SHA256 string `json:"sha256"`
	} `json:"costs"`
	ListingSHA256   string       `json:"listing_sha256"`
	Tests           []uiPlanTest `json:"tests"`
	Files           []uiPlanFile `json:"files"`
	Shards          []uiShard    `json:"shards"`
	RequestedShards int          `json:"requested_shards"`
	// Nightly is how the plan read the nightly spec list; Deferred are the
	// spec files a full proof leaves to the nightly tier: their tests are in
	// Tests, so a ledger names each as deferred, and they are in no shard.
	Nightly  *uiPlanNightly   `json:"nightly,omitempty"`
	Deferred []uiDeferredSpec `json:"deferred,omitempty"`
}

// uiPlanNightly records the nightly spec lists the plan read, hashed and
// embedded, so the merger can re-derive the deferral from the files.
type uiPlanNightly struct {
	Mode        string   `json:"mode"`
	File        string   `json:"file"`
	SHA256      string   `json:"sha256"`
	BaseFile    string   `json:"base_file,omitempty"`
	BaseSHA256  string   `json:"base_sha256,omitempty"`
	Entries     []string `json:"entries"`
	BaseEntries []string `json:"base_entries,omitempty"`
}

// uiDeferredSpec is one spec file the plan defers, with its list row.
type uiDeferredSpec struct {
	File  string `json:"file"`
	Entry string `json:"entry"`
}

// uiNightlyInput is what a plan reads of the nightly spec lists.
type uiNightlyInput struct {
	Mode           string
	File, BaseFile string
	SHA256         string
	BaseSHA256     string
	Tree, Base     []nightlySpecEntry
}

// buildUIPlanNightly plans by tier: the full tier (defer) leaves out of its
// files and shards the spec files both nightly spec lists carry and records
// them as deferred, their tests still planned; the nightly tier (only)
// plans the listed spec files alone; the pre-release gate (include) plans
// every file. A row whose spec the listing does not hold is a problem.
func buildUIPlanNightly(list pwReport, listBytes []byte, measured map[string]float64, costsFile, costsSHA string, m int, in uiNightlyInput) (uiPlan, []string) {
	listed := map[string]bool{}
	for _, s := range list.Suites {
		listed[s.File] = true
	}
	problems := nightlySpecProblems(in.Tree, listed, in.Mode != nightlyOnly)
	keep := func(string) bool { return true }
	deferred := map[string]nightlySpecEntry{}
	switch in.Mode {
	case nightlyDefer:
		deferred = deferredSpecSet(in.Tree, in.Base)
		keep = func(f string) bool { _, d := deferred[f]; return !d }
	case nightlyOnly:
		on := map[string]bool{}
		for _, e := range in.Tree {
			on[e.file()] = true
		}
		keep = func(f string) bool { return on[f] }
	}
	runs := list
	runs.Suites = nil
	for _, s := range list.Suites {
		if keep(s.File) {
			runs.Suites = append(runs.Suites, s)
		}
	}
	p := buildUIPlan(runs, listBytes, measured, costsFile, costsSHA, m)
	if in.Mode == nightlyDefer && len(deferred) > 0 {
		// Every listed test stays planned: a deferred one is recorded as
		// deferred, never as missing.
		all := buildUIPlan(list, listBytes, measured, costsFile, costsSHA, m)
		p.Tests = all.Tests
		files := make([]string, 0, len(deferred))
		for f := range deferred {
			files = append(files, f)
		}
		sort.Strings(files)
		for _, f := range files {
			p.Deferred = append(p.Deferred, uiDeferredSpec{File: f, Entry: deferred[f].Text})
		}
	}
	p.Nightly = &uiPlanNightly{
		Mode: in.Mode, File: in.File, SHA256: in.SHA256, BaseFile: in.BaseFile, BaseSHA256: in.BaseSHA256,
		Entries: specEntryLines(in.Tree), BaseEntries: specEntryLines(in.Base),
	}
	if in.Mode != nightlyDefer {
		p.Nightly.BaseFile, p.Nightly.BaseSHA256, p.Nightly.BaseEntries = "", "", nil
	}
	return p, problems
}

// splitSpecs puts each file, longest first, into the least-loaded of m shards.
// A tie in cost goes to the file name, a tie in load to the lowest shard, so
// the result is the same for any order of files. m is clamped to the number
// of files, and each shard's files are sorted by path, the order they run in
// locally.
func splitSpecs(files map[string]float64, m int) []uiShard {
	names := make([]string, 0, len(files))
	for f := range files {
		names = append(names, f)
	}
	sort.Slice(names, func(i, j int) bool {
		if files[names[i]] != files[names[j]] {
			return files[names[i]] > files[names[j]]
		}
		return names[i] < names[j]
	})
	if m > len(names) {
		m = len(names)
	}
	if m < 1 {
		m = 1
	}
	shards := make([]uiShard, m)
	for i := range shards {
		shards[i].Index = i + 1
	}
	for _, f := range names {
		k := 0
		for i := range shards {
			if shards[i].EstimatedSeconds < shards[k].EstimatedSeconds {
				k = i
			}
		}
		shards[k].Files = append(shards[k].Files, f)
		shards[k].EstimatedSeconds += files[f]
	}
	for i := range shards {
		sort.Strings(shards[i].Files)
		if shards[i].Files == nil {
			shards[i].Files = []string{}
		}
	}
	return shards
}

// readUICosts parses scripts/proof-ui-costs.tsv: "file<TAB>wall_seconds",
// '#' comments ignored, the source proof named in its header. A missing file
// is no measurements at all.
func readUICosts(file string) (map[string]float64, string, error) {
	out := map[string]float64{}
	if file == "" {
		return out, "", nil
	}
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return out, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	return parseUICosts(f, file)
}

func parseUICosts(r io.Reader, name string) (map[string]float64, string, error) {
	out := map[string]float64{}
	var source string
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if strings.HasPrefix(line, "# source: ") {
			source = strings.TrimPrefix(line, "# source: ")
		}
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			return nil, "", fmt.Errorf("%s:%d: want \"file<TAB>wall_seconds\"", name, n)
		}
		s, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, "", fmt.Errorf("%s:%d: seconds: %v", name, n, err)
		}
		out[fields[0]] = s
	}
	return out, source, sc.Err()
}

// writeUICosts writes the spec files' measured walls, sorted by file.
func writeUICosts(w io.Writer, files []uiFileEntry, source string) {
	sorted := append([]uiFileEntry{}, files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].File < sorted[j].File })
	fmt.Fprintf(w, "# Measured wall of every Playwright spec file: its tests' seconds plus the gap before each of them.\n")
	fmt.Fprintf(w, "# source: %s\n", source)
	fmt.Fprintf(w, "# Regenerated by `make proof-slow-list FROM=proof/<name>`; read by test/tools/proofrun ui-plan.\n")
	fmt.Fprintf(w, "# file\twall_seconds\n")
	for _, f := range sorted {
		fmt.Fprintf(w, "%s\t%.2f\n", f.File, f.WallSeconds)
	}
}

// buildUIPlan splits the listing's spec files into m shards by measured wall.
func buildUIPlan(list pwReport, listBytes []byte, measured map[string]float64, costsFile, costsSHA string, m int) uiPlan {
	keys, tests := flattenPW(list)
	p := uiPlan{Format: 1, RequestedShards: m}
	p.Costs.File, p.Costs.SHA256 = costsFile, costsSHA
	sum := sha256.Sum256(listBytes)
	p.ListingSHA256 = hex.EncodeToString(sum[:])
	count := map[string]int{}
	cost := map[string]float64{}
	var order []string
	for _, k := range keys {
		e := tests[k]
		p.Tests = append(p.Tests, uiPlanTest{File: e.File, Title: e.Title, Project: e.Project})
		if count[e.File] == 0 {
			order = append(order, e.File)
			if s, ok := measured[e.File]; ok {
				cost[e.File] = s
			} else {
				cost[e.File] = unmeasuredSpecSeconds
			}
		}
		count[e.File]++
	}
	if p.Tests == nil {
		p.Tests = []uiPlanTest{}
	}
	p.Shards = splitSpecs(cost, m)
	shardOf := map[string]int{}
	for _, s := range p.Shards {
		for _, f := range s.Files {
			shardOf[f] = s.Index
		}
	}
	sort.Strings(order)
	for _, f := range order {
		_, ok := measured[f]
		p.Files = append(p.Files, uiPlanFile{File: f, Tests: count[f], EstimatedSeconds: cost[f], Measured: ok, Shard: shardOf[f]})
	}
	if p.Files == nil {
		p.Files = []uiPlanFile{}
	}
	return p
}

func cmdUIPlan(args []string) int {
	fs := flag.NewFlagSet("ui-plan", flag.ContinueOnError)
	listFile := fs.String("list", "", "`playwright test --list --reporter=json` output")
	costsFile := fs.String("costs", "scripts/proof-ui-costs.tsv", "measured wall of each spec file")
	shards := fs.Int("shards", 1, "how many shards to split the spec files into")
	out := fs.String("out", "", "where to write ui-plan.json")
	matrixOut := fs.String("matrix-out", "", "where to write the shard indices as a JSON array")
	mode := fs.String("nightly-mode", nightlyInclude, "include (default), defer (leave out the spec files both nightly spec lists carry) or only")
	specs := fs.String("nightly-specs", nightlySpecFile, "the tree's nightly spec list")
	specsBase := fs.String("nightly-specs-base", "", "the base's copy of the nightly spec list (required with -nightly-mode defer)")
	runFilesOut := fs.String("run-files-out", "", "where to write the spec files to run, one per line relative to ui/ (empty when the plan leaves no listed file out)")
	if err := fs.Parse(args); err != nil || *listFile == "" || *out == "" || *shards < 1 || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan -list FILE -shards M -out FILE [-costs FILE] [-matrix-out FILE] [-nightly-mode include|defer|only -nightly-specs FILE -nightly-specs-base FILE]")
		return 2
	}
	switch *mode {
	case nightlyInclude, nightlyDefer, nightlyOnly:
	default:
		fmt.Fprintln(os.Stderr, "proofrun ui-plan: -nightly-mode must be include, defer or only")
		return 2
	}
	if *mode == nightlyDefer && *specsBase == "" {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan: -nightly-mode defer requires -nightly-specs-base: a proof defers only the rows its base also lists")
		return 2
	}
	in := uiNightlyInput{Mode: *mode, File: *specs}
	var err error
	if in.Tree, err = readNightlySpecs(*specs); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
		return 2
	}
	if in.SHA256, err = fileSHA256(*specs); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
		return 2
	}
	if *mode == nightlyDefer {
		in.BaseFile = *specsBase
		if in.Base, err = readNightlySpecs(*specsBase); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun ui-plan: the base's nightly spec list:", err)
			return 2
		}
		if in.BaseSHA256, err = fileSHA256(*specsBase); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
			return 2
		}
	}
	listBytes, err := os.ReadFile(*listFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
		return 2
	}
	var list pwReport
	if err := json.Unmarshal(listBytes, &list); err != nil {
		fmt.Fprintf(os.Stderr, "proofrun ui-plan: %s: %v\n", *listFile, err)
		return 2
	}
	measured, _, err := readUICosts(*costsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
		return 2
	}
	var costsSHA string
	if b, err := os.ReadFile(*costsFile); err == nil {
		sum := sha256.Sum256(b)
		costsSHA = hex.EncodeToString(sum[:])
	}
	p, problems := buildUIPlanNightly(list, listBytes, measured, *costsFile, costsSHA, *shards, in)
	if len(p.Files) == 0 && *mode != nightlyOnly {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan: the listing holds no spec file")
		return 2
	}
	if len(p.Files) == 0 {
		// A nightly tier with no listed spec runs no UI part at all.
		p.Shards = []uiShard{}
	}
	if err := writeJSON(*out, p); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
		return 1
	}
	if *runFilesOut != "" {
		// The files to name on Playwright's command line: only when the plan
		// leaves a listed file out (a deferred spec, or the nightly tier's
		// listed specs alone); otherwise none, and the run is the listing's.
		var lines []string
		if len(p.Deferred) > 0 || *mode == nightlyOnly {
			for _, f := range p.Files {
				lines = append(lines, "e2e/"+f.File+"\n")
			}
		}
		if err := os.WriteFile(*runFilesOut, []byte(strings.Join(lines, "")), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
			return 1
		}
	}
	if *matrixOut != "" {
		idx := make([]int, len(p.Shards))
		for i := range idx {
			idx[i] = i + 1
		}
		b, _ := json.Marshal(idx)
		if err := os.WriteFile(*matrixOut, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun ui-plan:", err)
			return 1
		}
	}
	var floor float64
	var floorFile string
	unmeasured := 0
	for _, f := range p.Files {
		if f.EstimatedSeconds > floor {
			floor, floorFile = f.EstimatedSeconds, f.File
		}
		if !f.Measured {
			unmeasured++
		}
	}
	fmt.Fprintf(stdout, "ui plan: %d tests in %d spec files, %d shards (%d requested); %d files unmeasured (%.0fs each)\n",
		len(p.Tests), len(p.Files), len(p.Shards), p.RequestedShards, unmeasured, unmeasuredSpecSeconds)
	for _, s := range p.Shards {
		fmt.Fprintf(stdout, "  shard %d: %d files, estimated %.0fs\n", s.Index, len(s.Files), s.EstimatedSeconds)
	}
	fmt.Fprintf(stdout, "  floor: %.0fs (%s)\n", floor, floorFile)
	for _, d := range p.Deferred {
		fmt.Fprintf(stdout, "  deferred to the nightly tier: %s\n", d.File)
	}
	for _, pr := range problems {
		fmt.Fprintln(stdout, "ui plan problem:", pr)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// uiFilesFrom finds each spec file's measured wall in a proof directory: its
// UI ledger's files array, which a merged CI artifact carries on its own, or
// else the Playwright report beside an older ledger. why says what was found
// when there is nothing to read.
func uiFilesFrom(dir string) (files []uiFileEntry, why string) {
	var l uiLedger
	ledgerErr := readJSON(dir+"/ui-e2e-tests.json", &l)
	if ledgerErr == nil && len(l.Files) > 0 {
		return l.Files, ""
	}
	var report pwReport
	if err := readJSON(dir+"/ui-e2e-report.json", &report); err == nil {
		keys, tests := flattenPW(report)
		entries := make([]uiEntry, 0, len(keys))
		for _, k := range keys {
			entries = append(entries, tests[k])
		}
		setStarts(entries)
		if files = fileWalls(entries); len(files) > 0 {
			return files, ""
		}
	}
	if ledgerErr != nil {
		return nil, "it has no Playwright ledger"
	}
	return nil, "its Playwright ledger carries no spec file walls and there is no ui-e2e-report.json beside it"
}

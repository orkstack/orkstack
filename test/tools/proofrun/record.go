package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// proof.json is the machine-readable record of a proof (AGENTS.md §11.2):
// what tree it measured, which gates ran and how they ended, and whether it
// may gate a merge — with every reason it may not. Workflow scripts read it;
// nothing should grep the README.

// A gate's result is pass or fail; a proof that stopped at the first failing
// gate (scripts/proof.sh, PROOF_FAIL_FAST) also records the gates it stopped
// while they ran and the ones it never started. Neither is a pass: a proof
// with one is a failing proof, and its Command column says why.
const (
	gateStopped    = "stopped"
	gateNotStarted = "not-started"
)

type gateRecord struct {
	Gate    string  `json:"gate"`
	Result  string  `json:"result"`
	Seconds float64 `json:"seconds"`
	Log     string  `json:"log"`
	Command string  `json:"command"`
}

type proofRecord struct {
	Tier        string       `json:"tier"`
	Name        string       `json:"name"`
	Commit      string       `json:"commit"`
	Branch      string       `json:"branch"`
	Tree        string       `json:"tree"`
	TreeEnd     string       `json:"tree_end"`
	CleanStart  bool         `json:"clean_start"`
	CleanEnd    bool         `json:"clean_end"`
	BaseRef     string       `json:"base_ref,omitempty"`
	BaseCommit  string       `json:"base_commit,omitempty"`
	MergeBase   string       `json:"merge_base,omitempty"`
	Gates       []gateRecord `json:"gates"`
	Ledgers     any          `json:"ledgers"`
	ImageTag    string       `json:"image_tag,omitempty"`
	ImageTagOwn bool         `json:"image_tag_own"`
	Images      any          `json:"image_digests,omitempty"`
	Temporal    struct {
		Mode    string `json:"mode"`
		Image   string `json:"image,omitempty"`
		Address string `json:"address,omitempty"`
	} `json:"temporal"`
	ProofJobs     int      `json:"proof_jobs"`
	UIWorkers     int      `json:"proof_ui_workers"`
	GuardRetries  *int     `json:"network_change_retries,omitempty"`
	GuardCeiling  *int     `json:"network_change_retry_ceiling,omitempty"`
	GatesOverride string   `json:"proof_gates_override,omitempty"`
	Result        string   `json:"result"`
	MergeEligible bool     `json:"merge_eligible"`
	Ineligible    []string `json:"ineligible_reasons"`
	// EligibleButForTier says that the only ineligible reason is the tier's
	// own ("only a full proof is merge evidence"): every identity and
	// completeness rule held. A release tag needs it (verify -release).
	EligibleButForTier bool     `json:"eligible_but_for_tier"`
	EvidenceFailed     []string `json:"evidence_without_a_pass,omitempty"`

	// Merged proofs: the parts they were built from, the reruns GitHub caused,
	// and the plan, lists and gaps the merger recorded (plan §2.6).
	Parts             []string           `json:"parts,omitempty"`
	Reruns            []rerunRecord      `json:"reruns,omitempty"`
	Plan              *planSummary       `json:"plan,omitempty"`
	UIPlan            *uiPlanSummary     `json:"ui_plan,omitempty"`
	Quarantined       []string           `json:"quarantined,omitempty"`
	Quarantine        *quarantineRecord  `json:"quarantine,omitempty"`
	Nightly           *nightlyRecord     `json:"nightly,omitempty"`
	HostSpeedFailures []hostSpeedFailure `json:"host_speed_failures,omitempty"`
	KnownGaps         []string           `json:"known_gaps,omitempty"`
	BinaryDigestNotes []string           `json:"binary_digest_notes,omitempty"`
	// SetupRetries are the fetches a part's runner needed a second attempt
	// for, from the parts' setup logs (.github/actions/proof-fetch): every
	// attempt line of each such fetch, prefixed with the part. A retry
	// within one attempt of a part is not a rerun and costs no eligibility;
	// it is recorded so a slow blob store or registry is seen, not guessed.
	SetupRetries []string     `json:"setup_retries,omitempty"`
	ListsChanged []listChange `json:"lists_changed,omitempty"`
	// RunnerHealth is every part attempt's runner-health record (its verdict
	// re-derived by the merger), and DegradedRunners every attempt that
	// measured below the floor, with the metric, its value and the floor: a
	// part listed there without a later attempt makes the proof not a proof
	// of the tree (AGENTS.md §11.2; merge.go).
	RunnerHealth    []partHealth     `json:"runner_health,omitempty"`
	DegradedRunners []degradedRunner `json:"degraded_runners,omitempty"`
	// QuarantineWarnings are the entries within seven days of their date.
	QuarantineWarnings []string  `json:"quarantine_warnings,omitempty"`
	PR                 *prRecord `json:"pr,omitempty"`
	CI                 *ciRecord `json:"ci,omitempty"`
}

// mainBranch is the branch a merge candidate fast-forwards, and the only base
// a full proof diffs against (AGENTS.md §11.3).
const mainBranch = "main"

// fullGates is every gate the merge gate runs; observability joins it when
// the tree changed a telemetry path. The site gate runs right after the UI's
// fast gates (scripts/proof.sh), and ui-e2e stays last. The nightly tier is
// held to the integration gate alone (merge.go).
var fullGates = []string{"lint", "build", "unit", suiteRace, "vet-integration", "images", "integration", "activity-fixture", "ui-check", "ui-unit", "site", "ui-e2e"}

// nightlyGates are the gates the nightly tier is held to besides the
// integration gate (and ui-e2e, when the tree lists a spec file): the race
// repeat and the fuzz lane. Each leaves a ledger of its own.
var nightlyGates = []string{suiteRaceRepeat, suiteFuzz}

// laneLedgers are the ledgers a proof reads besides the Playwright one: one
// per suite, <suite>-tests.json, named for the gate that wrote it.
var laneLedgers = []string{suiteUnit, suiteRace, suiteRaceRepeat, suiteFuzz, "integration"}

func readGates(file string) ([]gateRecord, error) {
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []gateRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.SplitN(sc.Text(), "\t", 5)
		if len(fields) != 5 {
			continue
		}
		s, _ := strconv.ParseFloat(fields[2], 64)
		out = append(out, gateRecord{Gate: fields[0], Result: fields[1], Seconds: s, Log: fields[3], Command: fields[4]})
	}
	return out, sc.Err()
}

// verdict decides a record's result and merge eligibility.
func verdict(r *proofRecord, status int, observabilityRequired bool, ledgersOK map[string]bool) {
	// A merger seeds Ineligible with its own rules before this runs; they are
	// never discarded, so each condition is written once (plan §2.6 rule 12).
	r.Ineligible = append([]string{}, r.Ineligible...)
	ran := map[string]string{}
	pass := status == 0
	for _, g := range r.Gates {
		ran[g.Gate] = g.Result
		if g.Result != "pass" {
			pass = false
		}
	}
	if len(r.EvidenceFailed) > 0 {
		pass = false
	}
	for name, ok := range ledgersOK {
		if !ok {
			pass = false
			r.Ineligible = append(r.Ineligible, "the "+name+" ledger is incomplete or has problems")
		}
	}
	r.Result = "fail"
	if pass {
		r.Result = "pass"
	}
	tierOnly := 0
	if r.Tier != "full" {
		r.Ineligible = append(r.Ineligible, tierIneligible(r.Tier))
		tierOnly = 1
	}
	if !pass {
		r.Ineligible = append(r.Ineligible, "the proof did not pass")
	}
	if r.Tier == "full" {
		required := append([]string{}, fullGates...)
		if observabilityRequired {
			required = append(required, "observability")
		}
		for _, g := range required {
			if _, ok := ran[g]; !ok {
				r.Ineligible = append(r.Ineligible, "gate "+g+" did not run")
			}
		}
	}
	if r.GatesOverride != "" {
		r.Ineligible = append(r.Ineligible, "PROOF_GATES="+r.GatesOverride+" chose the gates")
	}
	// The full tier decides whether the observability gate is due from its
	// diff against main; any other base could have left that gate out.
	if r.Tier == "full" && r.BaseRef != mainBranch {
		r.Ineligible = append(r.Ineligible, "the full proof diffed against "+orDash(r.BaseRef)+", not "+mainBranch+", so it may have left out the observability gate")
	}
	if !r.CleanStart {
		r.Ineligible = append(r.Ineligible, "the working tree was not clean when the proof started")
	}
	if !r.CleanEnd {
		r.Ineligible = append(r.Ineligible, "the working tree was not clean when the proof ended")
	}
	if r.Tree == "" || r.Tree != r.TreeEnd {
		r.Ineligible = append(r.Ineligible, "the tree changed while the proof ran ("+short(r.Tree)+" → "+short(r.TreeEnd)+")")
	}
	if r.Temporal.Mode != "own" && (ran["integration"] != "" || ran["ui-e2e"] != "") {
		r.Ineligible = append(r.Ineligible, "PROOF_TEMPORAL="+r.Temporal.Mode+": the proof did not run its own Temporal")
	}
	if r.ImageTag != "" && !r.ImageTagOwn {
		r.Ineligible = append(r.Ineligible, "ORK_TEST_IMAGE_TAG was supplied from outside the proof")
	}
	if r.GuardRetries != nil && r.GuardCeiling != nil && *r.GuardRetries > *r.GuardCeiling {
		r.Ineligible = append(r.Ineligible, "network-change retries above the ceiling")
	}
	r.MergeEligible = len(r.Ineligible) == 0
	r.EligibleButForTier = len(r.Ineligible) == tierOnly
}

// tierIneligible is the one reason a passing release or nightly proof is not
// merge evidence; verdict counts it apart, so nothing matches its text.
func tierIneligible(tier string) string {
	return "tier " + tier + ": only a full proof is merge evidence"
}

// evidenceFailures lists the feature evidence that has no pass. A skip, even
// an allowed one, is not evidence.
func evidenceFailures(sel *selection, o outcomes, specs map[string]string) []string {
	var out []string
	for _, u := range sel.UnitEvidence {
		if e := o[testKey{u.Package, u.Test}]; e.Outcome != outcomePass {
			out = append(out, fmt.Sprintf("%s.%s: %s", relDir(u.Package), u.Test, orDash(e.Outcome)))
		}
	}
	for _, p := range sortedKeys(sel.Integration) {
		ps := sel.Integration[p]
		for _, t := range sortedKeys(ps.Evidence) {
			if e := o[testKey{p, t}]; e.Outcome != outcomePass {
				out = append(out, fmt.Sprintf("%s.%s: %s", relDir(p), t, orDash(e.Outcome)))
			}
		}
	}
	for _, s := range sel.Specs {
		if specs[s.File] != outcomePass {
			out = append(out, fmt.Sprintf("%s: %s", s.File, orDash(specs[s.File])))
		}
	}
	return out
}

func cmdRecord(args []string) int {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	var r proofRecord
	dir := fs.String("dir", "", "the proof directory")
	part := fs.String("part", "", "write this part's part.json (full, release and nightly always do)")
	attempt := fs.Int("attempt", 1, "this part's attempt (1-based)")
	planFile := fs.String("plan", "", "the integration plan the part read")
	uiPlanFile := fs.String("ui-plan", "", "the UI plan the part read")
	outDir := fs.String("out-dir", "", "where part.json goes (default -dir)")
	fs.StringVar(&r.Tier, "tier", "full", "full, release, nightly or feature")
	fs.StringVar(&r.Name, "name", "", "proof name")
	fs.StringVar(&r.Commit, "commit", "", "HEAD")
	fs.StringVar(&r.Branch, "branch", "", "branch")
	fs.StringVar(&r.Tree, "tree-start", "", "tree hash at start")
	fs.StringVar(&r.TreeEnd, "tree-end", "", "tree hash at end")
	fs.BoolVar(&r.CleanStart, "clean-start", false, "clean at start")
	fs.BoolVar(&r.CleanEnd, "clean-end", false, "clean at end")
	fs.StringVar(&r.BaseRef, "base-ref", "", "feature base ref")
	fs.StringVar(&r.BaseCommit, "base-commit", "", "feature base commit")
	fs.StringVar(&r.MergeBase, "merge-base", "", "feature merge base")
	fs.StringVar(&r.ImageTag, "image-tag", "", "activity image tag")
	fs.BoolVar(&r.ImageTagOwn, "image-tag-own", false, "the tag is this proof's own")
	imagesFile := fs.String("images", "", "name<TAB>reference<TAB>index digest per activity image")
	fs.StringVar(&r.Temporal.Mode, "temporal", "none", "own, shared or none")
	fs.StringVar(&r.Temporal.Image, "temporal-image", "", "the proof Temporal's image")
	fs.StringVar(&r.Temporal.Address, "temporal-address", "", "the Temporal the proof used")
	fs.IntVar(&r.ProofJobs, "jobs", 0, "PROOF_JOBS")
	fs.IntVar(&r.UIWorkers, "ui-workers", 0, "PROOF_UI_WORKERS")
	fs.StringVar(&r.GatesOverride, "gates-override", "", "PROOF_GATES, when set")
	obsRequired := fs.Bool("observability-required", false, "the tree changed a telemetry path")
	setupLog := fs.String("setup-log", "", "the runner's setup log: every fetch attempt made before this part")
	healthFile := fs.String("runner-health", "", "the part's runner-health.json: what its runner measured before any gate")
	selFile := fs.String("select", "", "the feature selection")
	status := fs.Int("status", 0, "the proof script's own status")
	if err := fs.Parse(args); err != nil || *dir == "" || *attempt < 1 {
		fmt.Fprintln(os.Stderr, "proofrun record -dir DIR -tier TIER ...")
		return 2
	}
	// The full, release and nightly tiers have one verdict path: the merger.
	// Their record writes a part, always, locally too (plan §2.7).
	if r.Tier != tierFeature && *part == "" {
		*part = "local"
	}
	if *outDir == "" {
		*outDir = *dir
	}
	gates, err := readGates(filepath.Join(*dir, "gates.tsv"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	r.Gates = gates
	if r.Gates == nil {
		r.Gates = []gateRecord{}
	}
	// A health file the part names and cannot read is an error, never a
	// part that reads as if its runner went unmeasured.
	var health *runnerHealth
	if *healthFile != "" {
		var h runnerHealth
		if err := readJSON(*healthFile, &h); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun record: -runner-health:", err)
			return 2
		}
		if h.Format != runnerHealthFormat {
			fmt.Fprintf(os.Stderr, "proofrun record: -runner-health: %s is format %d, and this proofrun reads format %d\n", *healthFile, h.Format, runnerHealthFormat)
			return 2
		}
		health = &h
	}
	ledgers := map[string]any{}
	ledgerFiles := map[string]string{}
	ledgersOK := map[string]bool{}
	for _, name := range laneLedgers {
		var l ledger
		if readJSON(filepath.Join(*dir, name+"-tests.json"), &l) == nil {
			ledgers[name] = map[string]any{"summary": l.Summary, "problems": len(l.Problems)}
			ledgerFiles[name] = name + "-tests.json"
			ledgersOK[name] = l.ok()
		}
	}
	var ul uiLedger
	if readJSON(filepath.Join(*dir, "ui-e2e-tests.json"), &ul) == nil {
		ledgers["ui-e2e"] = map[string]any{"listed": ul.Listed, "pass": ul.Pass, "deferred": ul.Deferred, "problems": len(ul.Problems)}
		ledgerFiles["ui-e2e"] = "ui-e2e-tests.json"
		ledgersOK["ui-e2e"] = len(ul.Problems) == 0
		r.GuardRetries, r.GuardCeiling = &ul.GuardRetries, &ul.GuardRetryCeiling
	}
	// A gate that ran without leaving its ledger proved nothing about which
	// tests ran.
	for _, g := range r.Gates {
		if g.Result == gateNotStarted {
			continue
		}
		switch g.Gate {
		case suiteUnit, suiteRace, suiteRaceRepeat, suiteFuzz, "integration", "ui-e2e":
			if _, ok := ledgersOK[g.Gate]; !ok {
				ledgersOK[g.Gate] = false
			}
		}
	}
	r.Ledgers = ledgers
	if *imagesFile != "" {
		if b, err := os.ReadFile(*imagesFile); err == nil {
			digests := map[string]string{}
			for _, l := range strings.Split(string(b), "\n") {
				f := strings.Split(l, "\t")
				if len(f) == 3 {
					digests[f[1]] = f[2]
				}
			}
			r.Images = digests
		}
	}
	if *part != "" {
		rec := partRecord{
			Part: *part, Attempt: *attempt, Tier: r.Tier, Name: r.Name, Commit: r.Commit,
			Branch: r.Branch, Tree: r.Tree, TreeEnd: r.TreeEnd, CleanStart: r.CleanStart,
			CleanEnd: r.CleanEnd, BaseRef: r.BaseRef, BaseCommit: r.BaseCommit,
			MergeBase: r.MergeBase, Gates: r.Gates, Temporal: temporalRecord(r.Temporal),
			Images: imageDigests(r.Images), ImageTag: r.ImageTag, ImageTagOwn: r.ImageTagOwn,
			ProofJobs: r.ProofJobs, UIWorkers: r.UIWorkers, GatesOverride: r.GatesOverride,
			GuardRetries: r.GuardRetries, GuardCeiling: r.GuardCeiling,
			Runner: currentRunner(), Ledgers: ledgerFiles, Health: health,
		}
		// A plan the part names and cannot hash is an error, never an empty
		// hash the merger would read as "no plan was read".
		var hashErr error
		if rec.PlanSHA256, hashErr = fieldSHA256(*planFile); hashErr != nil && *planFile != "" {
			fmt.Fprintln(os.Stderr, "proofrun record: -plan:", hashErr)
			return 2
		}
		if rec.UIPlanSHA256, hashErr = fieldSHA256(*uiPlanFile); hashErr != nil && *uiPlanFile != "" {
			fmt.Fprintln(os.Stderr, "proofrun record: -ui-plan:", hashErr)
			return 2
		}
		if *setupLog != "" {
			// A setup log the part names and cannot read is an error, never
			// a part that reads as if its runner's setup went unrecorded.
			b, err := os.ReadFile(*setupLog)
			if err != nil {
				fmt.Fprintln(os.Stderr, "proofrun record: -setup-log:", err)
				return 2
			}
			for _, l := range strings.Split(string(b), "\n") {
				if l = strings.TrimSpace(l); l != "" {
					rec.Setup = append(rec.Setup, l)
				}
			}
		}
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		for _, f := range ledgerFiles {
			if filepath.Clean(*dir) == filepath.Clean(*outDir) {
				continue
			}
			if b, err := os.ReadFile(filepath.Join(*dir, f)); err == nil {
				if err := os.WriteFile(filepath.Join(*outDir, f), b, 0o644); err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 1
				}
			}
		}
		if err := writeJSON(filepath.Join(*outDir, "part.json"), rec); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		readme := fmt.Sprintf("# PART — NOT EVIDENCE ON ITS OWN\n\n- part: %s\n- attempt: %d\n- tier: %s\n- tree: %s\n\nThe merger decides this proof's result and merge eligibility.\n", rec.Part, rec.Attempt, rec.Tier, rec.Tree)
		_ = os.WriteFile(filepath.Join(*outDir, "README.md"), []byte(readme), 0o644)
		fmt.Printf("part %s attempt %d -> %s\n", rec.Part, rec.Attempt, filepath.Join(*outDir, "part.json"))
		if *status != 0 {
			return 1
		}
		return 0
	}
	if *selFile != "" {
		var sel selection
		if err := readJSON(*selFile, &sel); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		r.EvidenceFailed = evidenceFailures(&sel, loadOutcomes(*dir), loadSpecOutcomes(*dir))
	}
	if health != nil {
		// A feature proof's own host, for information: the floor never binds
		// a local part.
		r.RunnerHealth = []partHealth{{Part: "local", Attempt: 1, Class: health.Class, Verdict: health.verdict(), Seconds: health.Seconds, Metrics: health.Metrics}}
	}
	verdict(&r, *status, *obsRequired, ledgersOK)
	if err := writeJSON(filepath.Join(*dir, "proof.json"), r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	writeVerdict(os.Stdout, &r)
	if r.Result != "pass" {
		return 1
	}
	return 0
}

func imageDigests(v any) map[string]string {
	m, _ := v.(map[string]string)
	return m
}

// currentRunner records the host a part ran on.
func currentRunner() runnerRecord {
	r := runnerRecord{OS: runtime.GOOS}
	r.CPUs = runtime.NumCPU()
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				f := strings.Fields(line)
				if len(f) >= 2 {
					kb, _ := strconv.Atoi(f[1])
					r.MemMB = kb / 1024
				}
			}
		}
	}
	return r
}

// writeVerdict is the README's closing section. The full tier's result
// lines are byte-identical to every earlier proof's.
func writeVerdict(w io.Writer, r *proofRecord) {
	fmt.Fprintf(w, "## Verdict\n\n")
	if len(r.EvidenceFailed) > 0 {
		fmt.Fprintf(w, "Evidence without a pass (a skip is not evidence):\n\n")
		for _, e := range r.EvidenceFailed {
			fmt.Fprintf(w, "- %s\n", e)
		}
		fmt.Fprintln(w)
	}
	if r.MergeEligible {
		fmt.Fprintf(w, "- merge-eligible: **yes** — tree %s\n", r.Tree)
	} else {
		fmt.Fprintf(w, "- merge-eligible: **no**\n")
		for _, reason := range r.Ineligible {
			fmt.Fprintf(w, "  - %s\n", reason)
		}
	}
	fmt.Fprintln(w)
	switch {
	case r.Tier != "feature" && r.Result == "pass":
		fmt.Fprintln(w, "Result: **PASS**")
	case r.Tier != "feature":
		fmt.Fprintln(w, "Result: **FAIL**")
	case r.Result == "pass":
		fmt.Fprintln(w, "Result: **FEATURE PASS — not merge evidence**")
	default:
		fmt.Fprintln(w, "Result: **FEATURE FAIL — not merge evidence**")
	}
}

// cmdVerify is `proofrun verify`, behind `make merge-check PROOF=`: it
// refuses unless the proof is a full, passing, merge-eligible proof of
// exactly HEAD's tree, the worktree is clean, HEAD's first parent is the main
// branch's current tip, and the proof's own base was that same tip.
//
// -release is the pre-release gate: a passing release proof of the exact
// commit the tag names (or HEAD, before the tag exists). -nightly requires a
// passing nightly proof of main's tip while main's nightly list is non-empty.
func cmdVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dir := fs.String("proof", "", "the proof directory")
	release := fs.Bool("release", false, "verify a pre-release proof for a tag")
	tag := fs.String("tag", "", "with -release: the tag the proof must cover")
	nightlyDir := fs.String("nightly", "", "a passing nightly proof of main's tip")
	if err := fs.Parse(args); err != nil || *dir == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun verify -proof proof/<name> [-release -tag vX.Y.Z] [-nightly DIR]")
		return 2
	}
	if *release {
		return verifyRelease(*dir, *tag)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(*dir, "proof.json"), &r); err != nil {
		fmt.Printf("merge-check: %v\n", err)
		return 1
	}
	for _, w := range r.QuarantineWarnings {
		fmt.Printf("merge-check: %s\n", w)
	}
	if len(r.ListsChanged) > 0 {
		fmt.Printf("merge-check: this proof's change edits the gate's own files:\n")
		for _, c := range r.ListsChanged {
			fmt.Printf("  - %s (%d added, %d removed)\n", c.File, len(c.Added), len(c.Removed))
		}
		fmt.Println()
	}
	head := strings.TrimSpace(mustGit("rev-parse", "HEAD^{tree}"))
	parent := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", "HEAD^1"))
	// The branch is not a choice: a candidate fast-forwards main, and naming
	// an older commit instead would let a proof outlive the tip it was taken on.
	mainTip := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", mainBranch+"^{commit}"))
	status := mustGit("status", "--porcelain", "--untracked-files=all")
	problems := verifyProblems(r, head, parent, mainTip, status, mainBranch)
	problems = append(problems, verifyNightlyProblems(*nightlyDir, mainTip)...)
	if len(problems) > 0 {
		fmt.Printf("merge-check: %s is not merge evidence for HEAD:\n", *dir)
		for _, p := range problems {
			fmt.Println("  -", p)
		}
		return 1
	}
	fmt.Printf("merge-check: %s is a full, passing, merge-eligible proof of tree %s; HEAD^1 is %s's tip. Fast-forward with git merge --ff-only.\n", *dir, head, mainBranch)
	return 0
}

// verifyRelease is the tag precondition: a passing release proof, of the exact
// commit the tag names (or of HEAD, before the tag is created), clean, whose
// every identity and eligibility rule held.
func verifyRelease(dir, tag string) int {
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		fmt.Printf("release-check: %v\n", err)
		return 1
	}
	target := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", "HEAD^{commit}"))
	if tag != "" {
		if t := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", tag+"^{commit}")); t != "" {
			target = t
		}
	}
	tree := ""
	if target != "" {
		tree = strings.TrimSpace(mustGit("rev-parse", target+"^{tree}"))
	}
	problems := releaseProofProblems(r, target, tree)
	if len(problems) > 0 {
		fmt.Printf("release-check: %s cannot tag %s:\n", dir, orDash(tag))
		for _, p := range problems {
			fmt.Println("  -", p)
		}
		return 1
	}
	fmt.Printf("release-check: %s is a passing release proof of %s (tree %s).\n", dir, short(r.Commit), r.Tree)
	return 0
}

// releaseProofProblems is verify -release's rule, pure: tier release, a pass,
// the exact target commit and its tree, clean, and no ineligible reason but
// the release tier's own. A shared Temporal, an outside image tag, a
// PROOF_GATES override, parts that ran other image digests: each is an
// ineligible reason verdict() or the merger recorded, so each refuses a tag.
func releaseProofProblems(r proofRecord, target, tree string) []string {
	var problems []string
	if r.Tier != tierRelease {
		problems = append(problems, "it is a "+orDash(r.Tier)+" proof; a tag needs the release tier")
	}
	if r.Result != "pass" {
		problems = append(problems, "its result is "+orDash(r.Result))
	}
	if r.Commit == "" || r.Commit != target {
		problems = append(problems, fmt.Sprintf("it proved commit %s, and the tag is %s", orDash(short(r.Commit)), orDash(short(target))))
	}
	if r.Tree == "" || r.Tree != tree {
		problems = append(problems, fmt.Sprintf("it proved tree %s, and the commit's tree is %s", orDash(short(r.Tree)), orDash(short(tree))))
	}
	if !r.CleanStart || !r.CleanEnd || r.Tree != r.TreeEnd {
		problems = append(problems, "the tree was not clean at the proof's start and end")
	}
	if !r.EligibleButForTier {
		var reasons []string
		for _, reason := range r.Ineligible {
			if reason != tierIneligible(r.Tier) {
				reasons = append(reasons, reason)
			}
		}
		if len(reasons) == 0 {
			reasons = []string{"its proof.json does not record that every identity rule held (eligible_but_for_tier)"}
		}
		problems = append(problems, "it is not eligible evidence: "+strings.Join(reasons, "; "))
	}
	return problems
}

// verifyNightlyProblems is interim rule A's mechanical check: while main's
// nightly list is non-empty, a merge needs a passing nightly proof of main's
// tip tree.
func verifyNightlyProblems(nightlyDir, mainTip string) []string {
	if mainTip == "" {
		return nil
	}
	const listPath = "scripts/proof-nightly-tests.txt"
	// Absent and unreadable are different: a main with no nightly list needs
	// no nightly proof, a git error is a refusal.
	ls, err := gitOut("ls-tree", "--name-only", mainTip, "--", listPath)
	if err != nil {
		return []string{fmt.Sprintf("whether %s carries a nightly list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
	}
	list := ""
	if strings.TrimSpace(ls) != "" {
		if list, err = gitOut("show", mainTip+":"+listPath); err != nil {
			return []string{fmt.Sprintf("%s's nightly list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
		}
	}
	specs := ""
	if ls, err := gitOut("ls-tree", "--name-only", mainTip, "--", nightlySpecFile); err != nil {
		return []string{fmt.Sprintf("whether %s carries a nightly spec list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
	} else if strings.TrimSpace(ls) != "" {
		if specs, err = gitOut("show", mainTip+":"+nightlySpecFile); err != nil {
			return []string{fmt.Sprintf("%s's nightly spec list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
		}
	}
	tree, err := gitOut("rev-parse", mainTip+"^{tree}")
	if err != nil {
		return []string{fmt.Sprintf("%s's tip tree cannot be read: %v", mainBranch, err)}
	}
	return nightlyProofProblemsWith(list, specs, nightlyDir, strings.TrimSpace(tree))
}

// nightlyProofProblems is verifyNightlyProblems, pure over main's list text,
// the nightly proof's directory and main's tip tree.
func nightlyProofProblems(list, nightlyDir, mainTree string) []string {
	return nightlyProofProblemsWith(list, "", nightlyDir, mainTree)
}

// nightlyProofProblemsWith also reads main's nightly spec list: a merge needs
// a nightly proof while either list is non-empty.
func nightlyProofProblemsWith(list, specList, nightlyDir, mainTree string) []string {
	entries, err := parseNightlyList(strings.NewReader(list), mainBranch+":scripts/proof-nightly-tests.txt")
	if err != nil {
		// An unreadable list is not an empty one: it fails closed.
		return []string{fmt.Sprintf("%s's nightly list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
	}
	specs, err := parseNightlySpecs(strings.NewReader(specList), mainBranch+":"+nightlySpecFile)
	if err != nil {
		return []string{fmt.Sprintf("%s's nightly spec list cannot be read, so whether a nightly proof is due is unknown: %v", mainBranch, err)}
	}
	if len(entries) == 0 && len(specs) == 0 {
		return nil
	}
	if nightlyDir == "" {
		return []string{fmt.Sprintf("%s's nightly lists hold %d test(s) and %d spec file(s): name a passing nightly proof of its tip with NIGHTLY=proof/<name>", mainBranch, len(entries), len(specs))}
	}
	var r proofRecord
	if err := readJSON(filepath.Join(nightlyDir, "proof.json"), &r); err != nil {
		return []string{fmt.Sprintf("the nightly proof %s is unreadable: %v", nightlyDir, err)}
	}
	var p []string
	if r.Tier != tierNightly {
		p = append(p, "the nightly proof is tier "+orDash(r.Tier))
	}
	if r.Result != "pass" {
		p = append(p, "the nightly proof did not pass")
	}
	for _, d := range degradedProblems(r) {
		p = append(p, "the nightly proof: "+d)
	}
	if !r.EligibleButForTier {
		var reasons []string
		for _, reason := range r.Ineligible {
			if reason != tierIneligible(r.Tier) {
				reasons = append(reasons, reason)
			}
		}
		p = append(p, "the nightly proof is not eligible evidence: "+orDash(strings.Join(reasons, "; ")))
	}
	if r.Tree == "" || r.Tree != mainTree {
		p = append(p, fmt.Sprintf("the nightly proof measured tree %s, and %s's tip tree is %s", orDash(short(r.Tree)), mainBranch, orDash(short(mainTree))))
	}
	if !r.CleanStart || !r.CleanEnd || r.Tree != r.TreeEnd {
		p = append(p, "the nightly proof's tree was not clean")
	}
	return p
}

// degradedProblems names every part whose last attempt ran on a degraded
// runner: the proof is not a proof of the tree, and the remedy is that one
// job again, never a new rule. A degraded attempt that a later attempt
// replaced is a recorded rerun and no problem here.
func degradedProblems(r proofRecord) []string {
	rerun := map[string]bool{}
	for _, rr := range r.Reruns {
		for _, a := range rr.EarlierAttempts {
			rerun[fmt.Sprintf("%s#%d", rr.Part, a)] = true
		}
	}
	var p []string
	seen := map[string]bool{}
	for _, d := range r.DegradedRunners {
		key := fmt.Sprintf("%s#%d", d.Part, d.Attempt)
		if rerun[key] || seen[key] {
			continue
		}
		seen[key] = true
		var metrics []string
		for _, x := range r.DegradedRunners {
			if x.Part == d.Part && x.Attempt == d.Attempt {
				metrics = append(metrics, fmt.Sprintf("%s %s %s, floor %s", x.Metric, trimFloat(x.Value), x.Unit, trimFloat(x.Floor)))
			}
		}
		p = append(p, fmt.Sprintf("part %s ran on a degraded runner (attempt %d, %s: %s): it is not a proof of the tree; re-run that job alone", d.Part, d.Attempt, d.Class, strings.Join(metrics, "; ")))
	}
	return p
}

// maxQuarantineAgeDays is how old a proof's quarantine check date may be: the
// merged proof artifact's private retention (docs/PROOFS.md §7), so a proof
// older than the artifact that carries it is not judged by a stale review-by
// date.
const maxQuarantineAgeDays = 30

// quarantineAgeProblem refuses a proof whose quarantine list was checked more
// than maxQuarantineAgeDays before today. time.Now is allowed here, in verify,
// and never in the merger.
func quarantineAgeProblem(r proofRecord, today time.Time) string {
	if r.Quarantine == nil || r.Quarantine.CheckedOn == "" {
		// A merge gate's proof always records when its quarantine list was
		// checked; an absent date cannot be judged, so it is not fresh.
		if r.Tier == "full" || r.Tier == "release" {
			return "it records no quarantine check date: prove the candidate again"
		}
		return ""
	}
	on, err := time.Parse("2006-01-02", r.Quarantine.CheckedOn)
	if err != nil {
		return fmt.Sprintf("its quarantine check date %q is not a date", r.Quarantine.CheckedOn)
	}
	if age := int(today.Sub(on).Hours() / 24); age > maxQuarantineAgeDays {
		return fmt.Sprintf("its quarantine list was checked on %s, %d days ago (more than %d): prove the candidate again", r.Quarantine.CheckedOn, age, maxQuarantineAgeDays)
	}
	return ""
}

func verifyProblems(r proofRecord, headTree, parent, mainTip, status, mainRef string) []string {
	var p []string
	if msg := quarantineAgeProblem(r, todayUTC()); msg != "" {
		p = append(p, msg)
	}
	p = append(p, degradedProblems(r)...)
	if r.Tier != "full" {
		p = append(p, "it is a "+orDash(r.Tier)+" proof; only a full proof gates a merge")
	}
	if r.Result != "pass" {
		p = append(p, "its result is "+orDash(r.Result))
	}
	if !r.MergeEligible {
		p = append(p, "it is not merge-eligible: "+strings.Join(r.Ineligible, "; "))
	}
	if r.Tree != headTree {
		p = append(p, fmt.Sprintf("it proved tree %s, and HEAD's tree is %s", orDash(r.Tree), headTree))
	}
	if strings.TrimSpace(status) != "" {
		p = append(p, "the worktree is not clean")
	}
	if r.BaseCommit == "" || r.BaseCommit != mainTip {
		p = append(p, fmt.Sprintf("it diffed against %s, not %s's current tip (%s), so whether the observability gate was due was decided against another base: prove the candidate again", orDash(short(r.BaseCommit)), mainRef, orDash(short(mainTip))))
	}
	if parent == "" || parent != mainTip {
		p = append(p, fmt.Sprintf("HEAD^1 (%s) is not %s's current tip (%s): rebuild the candidate on it and prove it again", orDash(short(parent)), mainRef, orDash(short(mainTip))))
	}
	return p
}

func mustGit(args ...string) string {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

// composeTemporal reads the temporal service's image and command from
// `docker compose config --format json`.
func composeTemporal(b []byte) (image string, args []string, err error) {
	var doc struct {
		Services map[string]struct {
			Image   string          `json:"image"`
			Command json.RawMessage `json:"command"`
		} `json:"services"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return "", nil, err
	}
	svc, ok := doc.Services["temporal"]
	if !ok || svc.Image == "" {
		return "", nil, fmt.Errorf("the compose file has no temporal service with an image")
	}
	if len(svc.Command) > 0 && string(svc.Command) != "null" {
		if err := json.Unmarshal(svc.Command, &args); err != nil {
			var s string
			if json.Unmarshal(svc.Command, &s) != nil {
				return "", nil, fmt.Errorf("temporal command: %v", err)
			}
			args = strings.Fields(s)
		}
	}
	return svc.Image, args, nil
}

func cmdTemporalArgs(args []string) int {
	fs := flag.NewFlagSet("temporal-args", flag.ContinueOnError)
	compose := fs.String("compose", "deploy/docker-compose.yml", "the compose file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	out, err := exec.Command("docker", "compose", "-f", *compose, "config", "--format", "json").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun temporal-args: docker compose config:", err)
		return 1
	}
	image, cargs, err := composeTemporal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun temporal-args:", err)
		return 1
	}
	fmt.Println(image)
	for _, a := range cargs {
		fmt.Println(a)
	}
	return 0
}

func cmdCosts(args []string) int {
	fs := flag.NewFlagSet("costs", flag.ContinueOnError)
	var from multiFlag
	fs.Var(&from, "from", "a full, release or nightly proof's directory, or a merged CI proof's (repeatable; a later source is newer)")
	costsOut := fs.String("costs-out", "scripts/proof-test-costs.tsv", "where to write the cost ledger (its committed rows are kept where no source measured)")
	slowOut := fs.String("slow-out", "scripts/proof-slow-tests.txt", "where to write the slow list")
	uiCostsOut := fs.String("ui-costs-out", "scripts/proof-ui-costs.tsv", "where to write the spec files' measured walls")
	threshold := fs.Float64("threshold", slowThreshold, "seconds at which a test is slow")
	if err := fs.Parse(args); err != nil || len(from) == 0 {
		fmt.Fprintln(os.Stderr, "proofrun costs -from proof/<name> [-from proof/<newer>]...")
		return 2
	}
	var sources []costSource
	for _, dir := range from {
		src, err := readCostSource(dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "proofrun costs:", err)
			return 2
		}
		sources = append(sources, src)
	}
	committed, err := readCosts(*costsOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun costs:", err)
		return 2
	}
	lines, notes := mergeCosts(committed, sources)
	if len(lines) == 0 {
		fmt.Fprintln(os.Stderr, "proofrun costs: no measured test")
		return 2
	}
	var names []string
	for _, src := range sources {
		names = append(names, src.name)
	}
	source := strings.Join(names, "; ")
	for _, n := range notes {
		fmt.Println("note:", n)
	}
	cf, err := os.Create(*costsOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	writeCosts(cf, append([]costLine{}, lines...), source)
	for _, n := range notes {
		fmt.Fprintf(cf, "# note: %s\n", n)
	}
	_ = cf.Close()
	sf, err := os.Create(*slowOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	writeSlowList(sf, lines, *threshold, source)
	_ = sf.Close()
	fmt.Printf("wrote %s and %s from %s (%d tests)\n", *costsOut, *slowOut, source, len(lines))
	// The UI costs come from the newest source with a Playwright ledger.
	for i := len(sources) - 1; i >= 0; i-- {
		files, why := uiFilesFrom(sources[i].dir)
		if len(files) == 0 {
			fmt.Printf("%s: %s\n", sources[i].name, why)
			continue
		}
		uf, err := os.Create(*uiCostsOut)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		writeUICosts(uf, files, sources[i].name)
		_ = uf.Close()
		fmt.Printf("wrote %s from %s (%d spec files)\n", *uiCostsOut, sources[i].name, len(files))
		return 0
	}
	fmt.Printf("left %s as it is: no source has a Playwright ledger with file walls\n", *uiCostsOut)
	return 0
}

// costSource is one proof a costs refresh reads: what it measured, and what
// its plan listed.
type costSource struct {
	dir, name string
	measured  []costLine
	// listed is every test the source's plan lists (packages[].listed); for
	// a format-1 plan, or none, the source's ledger entries.
	listed   map[testKey]bool
	fallback bool
}

// readCostSource reads one source directory: a local proof, or a merged CI
// proof whose plan is under parts/.
func readCostSource(dir string) (costSource, error) {
	src := costSource{dir: dir, name: filepath.Base(dir), listed: map[testKey]bool{}}
	var r proofRecord
	if readJSON(filepath.Join(dir, "proof.json"), &r) == nil {
		if r.Tier == tierFeature {
			return src, fmt.Errorf("%s: only a full, release or nightly proof measures tests in full; this is a %s proof", dir, r.Tier)
		}
		src.name = fmt.Sprintf("proof/%s (commit %s)", r.Name, short(r.Commit))
	}
	var l ledger
	if err := readJSON(filepath.Join(dir, "integration-tests.json"), &l); err == nil {
		src.measured = costsFromLedger(l)
		for _, e := range l.Tests {
			if e.Outcome != "deferred" {
				src.listed[testKey{e.Package, e.Test}] = true
			}
		}
	} else {
		f, err := os.Open(filepath.Join(dir, "integration.log"))
		if err != nil {
			return src, fmt.Errorf("%s: neither integration-tests.json nor integration.log: %v", dir, err)
		}
		defer f.Close()
		if src.measured, err = costsFromGoTestLog(f); err != nil {
			return src, err
		}
		for _, c := range src.measured {
			src.listed[testKey{c.Package, c.Test}] = true
		}
		if src.name == filepath.Base(dir) {
			src.name = "proof/" + src.name + readmeCommit(filepath.Join(dir, "README.md"))
		}
	}
	if listed, ok := planListing(dir); ok {
		src.listed = listed
	} else {
		src.fallback = true
	}
	return src, nil
}

// planListing reads a source's format-2 plan (beside it, or in a merged CI
// proof's parts/) and returns every test its packages list. A format-1 plan
// (a bare array of jobs) or no plan lists nothing.
func planListing(dir string) (map[testKey]bool, bool) {
	candidates := []string{filepath.Join(dir, "integration-plan.json")}
	if more, err := filepath.Glob(filepath.Join(dir, "parts", "*", "integration-plan.json")); err == nil {
		candidates = append(candidates, more...)
	}
	for _, file := range candidates {
		b, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		if t := strings.TrimSpace(string(b)); strings.HasPrefix(t, "[") {
			return nil, false
		}
		var p integrationPlan
		if json.Unmarshal(b, &p) != nil || p.Format != planFormat {
			continue
		}
		listed := map[testKey]bool{}
		for _, pk := range p.Packages {
			for _, t := range pk.Listed {
				listed[testKey{pk.ImportPath, t}] = true
			}
		}
		return listed, true
	}
	return nil, false
}

// mergeCosts is the costs refresh (plan §2.1, costs): per test, the newest
// source that measured it wins; a test some source's plan lists and no source
// measured keeps its committed row; a committed test no source lists is
// dropped, because it no longer exists. So a nightly proof alone rewrites only
// the rows it measured.
func mergeCosts(committed costs, sources []costSource) ([]costLine, []string) {
	var notes []string
	measured := map[testKey]float64{}
	listed := map[testKey]bool{}
	for _, src := range sources {
		for _, c := range src.measured {
			measured[testKey{c.Package, c.Test}] = c.Seconds
		}
		for k := range src.listed {
			listed[k] = true
		}
		if src.fallback {
			notes = append(notes, src.name+" has no format-2 plan: the tests it lists are its ledger's entries")
		}
	}
	var out []costLine
	for k := range listed {
		if s, ok := measured[k]; ok {
			out = append(out, costLine{k.Package, k.Test, s})
		} else if s, ok := committed[k]; ok {
			out = append(out, costLine{k.Package, k.Test, s})
		}
	}
	for k, s := range measured {
		if !listed[k] {
			out = append(out, costLine{k.Package, k.Test, s})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Package != out[j].Package {
			return out[i].Package < out[j].Package
		}
		return out[i].Test < out[j].Test
	})
	return out, notes
}

// readmeCommit finds "- commit: <sha>" in a proof README written before
// proof.json existed, as " (commit <short sha>)".
func readmeCommit(file string) string {
	b, err := os.ReadFile(file)
	if err != nil {
		return ""
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "- commit: ") {
			f := strings.Fields(strings.TrimPrefix(l, "- commit: "))
			if len(f) > 0 {
				return " (commit " + short(f[0]) + ")"
			}
		}
	}
	return ""
}

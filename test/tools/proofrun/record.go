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
	"strconv"
	"strings"
)

// proof.json is the machine-readable record of a proof (AGENTS.md §11.2):
// what tree it measured, which gates ran and how they ended, and whether it
// may gate a merge — with every reason it may not. Workflow scripts read it;
// nothing should grep the README.

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
	ProofJobs      int      `json:"proof_jobs"`
	UIWorkers      int      `json:"proof_ui_workers"`
	GuardRetries   *int     `json:"network_change_retries,omitempty"`
	GuardCeiling   *int     `json:"network_change_retry_ceiling,omitempty"`
	GatesOverride  string   `json:"proof_gates_override,omitempty"`
	Result         string   `json:"result"`
	MergeEligible  bool     `json:"merge_eligible"`
	Ineligible     []string `json:"ineligible_reasons"`
	EvidenceFailed []string `json:"evidence_without_a_pass,omitempty"`
}

// fullGates is every gate the merge gate runs; observability joins it when
// the tree changed a telemetry path.
var fullGates = []string{"lint", "build", "unit", "vet-integration", "images", "integration", "activity-fixture", "ui-check", "ui-unit", "ui-e2e"}

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
	r.Ineligible = []string{}
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
	if r.Tier != "full" {
		r.Ineligible = append(r.Ineligible, "tier "+r.Tier+": a feature proof is never merge evidence")
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
	fs.StringVar(&r.Tier, "tier", "full", "full or feature")
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
	selFile := fs.String("select", "", "the feature selection")
	status := fs.Int("status", 0, "the proof script's own status")
	if err := fs.Parse(args); err != nil || *dir == "" {
		fmt.Fprintln(os.Stderr, "proofrun record -dir DIR ...")
		return 2
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
	ledgers := map[string]any{}
	ledgersOK := map[string]bool{}
	for _, name := range []string{"unit", "integration"} {
		var l ledger
		if readJSON(filepath.Join(*dir, name+"-tests.json"), &l) == nil {
			ledgers[name] = map[string]any{"summary": l.Summary, "problems": len(l.Problems)}
			ledgersOK[name] = l.ok()
		}
	}
	var ul uiLedger
	if readJSON(filepath.Join(*dir, "ui-e2e-tests.json"), &ul) == nil {
		ledgers["ui-e2e"] = map[string]any{"listed": ul.Listed, "pass": ul.Pass, "problems": len(ul.Problems)}
		ledgersOK["ui-e2e"] = len(ul.Problems) == 0
		r.GuardRetries, r.GuardCeiling = &ul.GuardRetries, &ul.GuardRetryCeiling
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
	if *selFile != "" {
		var sel selection
		if err := readJSON(*selFile, &sel); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		r.EvidenceFailed = evidenceFailures(&sel, loadOutcomes(*dir), loadSpecOutcomes(*dir))
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
	case r.Tier == "full" && r.Result == "pass":
		fmt.Fprintln(w, "Result: **PASS**")
	case r.Tier == "full":
		fmt.Fprintln(w, "Result: **FAIL**")
	case r.Result == "pass":
		fmt.Fprintln(w, "Result: **FEATURE PASS — not merge evidence**")
	default:
		fmt.Fprintln(w, "Result: **FEATURE FAIL — not merge evidence**")
	}
}

// cmdVerify is `proofrun verify`, behind `make merge-check PROOF=`: it
// refuses unless the proof is a full, passing, merge-eligible proof of
// exactly HEAD's tree, the worktree is clean, and HEAD's first parent is the
// main branch's current tip.
func cmdVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dir := fs.String("proof", "", "the proof directory")
	mainRef := fs.String("main", "main", "the branch the candidate fast-forwards")
	if err := fs.Parse(args); err != nil || *dir == "" {
		fmt.Fprintln(os.Stderr, "proofrun verify -proof proof/<name> [-main main]")
		return 2
	}
	var r proofRecord
	if err := readJSON(filepath.Join(*dir, "proof.json"), &r); err != nil {
		fmt.Printf("merge-check: %v\n", err)
		return 1
	}
	head := strings.TrimSpace(mustGit("rev-parse", "HEAD^{tree}"))
	parent := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", "HEAD^1"))
	mainTip := strings.TrimSpace(mustGit("rev-parse", "--verify", "--quiet", *mainRef+"^{commit}"))
	status := mustGit("status", "--porcelain", "--untracked-files=all")
	problems := verifyProblems(r, head, parent, mainTip, status, *mainRef)
	if len(problems) > 0 {
		fmt.Printf("merge-check: %s is not merge evidence for HEAD:\n", *dir)
		for _, p := range problems {
			fmt.Println("  -", p)
		}
		return 1
	}
	fmt.Printf("merge-check: %s is a full, passing, merge-eligible proof of tree %s; HEAD^1 is %s's tip. Fast-forward with git merge --ff-only.\n", *dir, head, *mainRef)
	return 0
}

func verifyProblems(r proofRecord, headTree, parent, mainTip, status, mainRef string) []string {
	var p []string
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
	from := fs.String("from", "", "a full proof's directory")
	costsOut := fs.String("costs-out", "scripts/proof-test-costs.tsv", "where to write the cost ledger")
	slowOut := fs.String("slow-out", "scripts/proof-slow-tests.txt", "where to write the slow list")
	threshold := fs.Float64("threshold", slowThreshold, "seconds at which a test is slow")
	if err := fs.Parse(args); err != nil || *from == "" {
		fmt.Fprintln(os.Stderr, "proofrun costs -from proof/<name>")
		return 2
	}
	var lines []costLine
	var r proofRecord
	source := filepath.Base(*from)
	if readJSON(filepath.Join(*from, "proof.json"), &r) == nil {
		if r.Tier != "full" {
			fmt.Fprintln(os.Stderr, "proofrun costs: only a full proof measures every test; this is a "+r.Tier+" proof")
			return 2
		}
		source = fmt.Sprintf("proof/%s (commit %s)", r.Name, short(r.Commit))
	}
	var l ledger
	if err := readJSON(filepath.Join(*from, "integration-tests.json"), &l); err == nil {
		lines = costsFromLedger(l)
	} else {
		f, err := os.Open(filepath.Join(*from, "integration.log"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "proofrun costs: neither integration-tests.json nor integration.log:", err)
			return 2
		}
		defer f.Close()
		if lines, err = costsFromGoTestLog(f); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun costs:", err)
			return 2
		}
		if source == filepath.Base(*from) {
			source = "proof/" + source + readmeCommit(filepath.Join(*from, "README.md"))
		}
	}
	if len(lines) == 0 {
		fmt.Fprintln(os.Stderr, "proofrun costs: no measured test")
		return 2
	}
	cf, err := os.Create(*costsOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	writeCosts(cf, append([]costLine{}, lines...), source)
	_ = cf.Close()
	sf, err := os.Create(*slowOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	writeSlowList(sf, lines, *threshold, source)
	_ = sf.Close()
	fmt.Printf("wrote %s and %s from %s (%d tests)\n", *costsOut, *slowOut, source, len(lines))
	return 0
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

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
)

// cmdFreePort prints a port nothing listens on at the moment of asking.
// Another process may take it before the caller binds it; the caller's bind
// then fails loudly, which is never a false pass.
func cmdFreePort(args []string) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun free-port:", err)
		return 1
	}
	defer l.Close()
	fmt.Println(l.Addr().(*net.TCPAddr).Port)
	return 0
}

// cmdTouches reports whether the change since the merge base with -base
// touches a path whose scope rule carries -action (exit 0, the paths
// printed) or not (exit 1). The full proof asks it whether the observability
// gate is due. With -gate-files it instead writes the changed gate files, and
// the rows the excusing lists add or remove, as the JSON proof.json carries as
// lists_changed (plan §6.4).
func cmdTouches(args []string) int {
	fs := flag.NewFlagSet("touches", flag.ContinueOnError)
	base := fs.String("base", "main", "the ref the change is measured from")
	action := fs.String("action", "", "a scope action, e.g. observability")
	scopeFile := fs.String("scope", "scripts/proof-scope.tsv", "the scope table")
	gateFiles := fs.Bool("gate-files", false, "list the changed gate files and the rows the excusing lists add or remove")
	if err := fs.Parse(args); err != nil || (!*gateFiles && !knownActions[*action]) {
		fmt.Fprintln(os.Stderr, "proofrun touches -base REF (-action ACTION | -gate-files)")
		return 2
	}
	if *gateFiles {
		return touchesGateFiles(os.Stdout, *base)
	}
	scope, err := readScope(*scopeFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	bc, err := gitOut("rev-parse", "--verify", "--quiet", *base+"^{commit}")
	if err != nil {
		fmt.Fprintf(os.Stderr, "proofrun touches: base %q is not a commit\n", *base)
		return 2
	}
	mb, err := gitOut("merge-base", strings.TrimSpace(bc), "HEAD")
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	paths, err := diffPaths(strings.TrimSpace(mb))
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	hit := touching(scope, paths, *action)
	for _, p := range hit {
		fmt.Println(p)
	}
	if len(hit) == 0 {
		return 1
	}
	return 0
}

// touchesGateFiles writes the -gate-files JSON. A failure to compute it is
// written as one entry carrying the error, and exits 1, so a caller that
// keeps the output never reads "no gate file changed" into a failure (plan
// §6.4).
func touchesGateFiles(w io.Writer, base string) int {
	changes, err := gateFilesSince(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		changes = []listChange{{Error: err.Error()}}
	}
	if changes == nil {
		changes = []listChange{}
	}
	b, mErr := json.MarshalIndent(changes, "", "  ")
	if mErr != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", mErr)
		return 2
	}
	fmt.Fprintln(w, string(b))
	if err != nil {
		return 1
	}
	return 0
}

func gateFilesSince(base string) ([]listChange, error) {
	bc, err := gitOut("rev-parse", "--verify", "--quiet", base+"^{commit}")
	if err != nil {
		return nil, fmt.Errorf("base %q is not a commit", base)
	}
	mb, err := gitOut("merge-base", strings.TrimSpace(bc), "HEAD")
	if err != nil {
		return nil, fmt.Errorf("no merge base of %s and HEAD: %v", base, err)
	}
	paths, err := diffPaths(strings.TrimSpace(mb))
	if err != nil {
		return nil, err
	}
	return gateFileChanges(paths, func(path string) (before, after string) {
		before = string(gitShow(strings.TrimSpace(mb), path))
		if data, err := os.ReadFile(path); err == nil {
			after = string(data)
		}
		return before, after
	}), nil
}

// gateFilePatterns are the files that decide what a proof runs or excuses
// (plan §2.1's -gate-files): the lists, the harness, proofrun, the workflows
// and the composite action. .github/CODEOWNERS must cover exactly these.
var gateFilePatterns = []string{
	"scripts/proof.sh",
	"scripts/proof-part.sh",
	"scripts/proof-nightly-tests.txt",
	"scripts/proof-quarantine.txt",
	"scripts/proof-allowed-skips.txt",
	"scripts/proof-test-costs.tsv",
	"scripts/proof-slow-tests.txt",
	"scripts/proof-ui-costs.tsv",
	"scripts/proof-section11-coverage.tsv",
	"scripts/proof-host-speed.tsv",
	"scripts/proof-scope.tsv",
	"scripts/integration.sh",
	"scripts/github-merge-gate.sh",
	"scripts/github/",
	".github/",
	"test/tools/proofrun/",
	"Makefile",
	"ui/playwright.config.ts",
}

// excusingLists are the lists that excuse a test or a failure; a change to
// one is reported with the rows it adds and removes, so a reader sees exactly
// which allowance moved.
var excusingLists = []string{
	"scripts/proof-nightly-tests.txt",
	"scripts/proof-quarantine.txt",
	"scripts/proof-allowed-skips.txt",
	"scripts/proof-host-speed.tsv",
}

// isGateFile reports whether path is one of gateFilePatterns. A pattern
// ending in "/" covers the directory; everything else is an exact file.
func isGateFile(path string) bool {
	for _, pattern := range gateFilePatterns {
		if strings.HasSuffix(pattern, "/") {
			if strings.HasPrefix(path, pattern) {
				return true
			}
			continue
		}
		if path == pattern {
			return true
		}
	}
	return false
}

// gateFileChanges is the -gate-files computation, pure over the change set and
// a reader of each path's base and tree text, so a test can drive it.
func gateFileChanges(paths []changedPath, read func(path string) (before, after string)) []listChange {
	var out []listChange
	seen := map[string]bool{}
	for _, p := range paths {
		if !isGateFile(p.Path) || seen[p.Path] {
			continue
		}
		seen[p.Path] = true
		change := listChange{File: p.Path}
		before, after := read(p.Path)
		for _, list := range excusingLists {
			if p.Path == list {
				change.Added, change.Removed = listRowChanges(before, after)
			}
		}
		out = append(out, change)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}

// listRowChanges diffs two copies of an excusing list by non-comment row.
func listRowChanges(before, after string) (added, removed []string) {
	rows := func(text string) []string {
		var out []string
		for _, line := range strings.Split(text, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			out = append(out, trimmed)
		}
		return out
	}
	was, is := map[string]bool{}, map[string]bool{}
	for _, row := range rows(before) {
		was[row] = true
	}
	for _, row := range rows(after) {
		is[row] = true
	}
	for _, row := range rows(after) {
		if !was[row] {
			added = append(added, row)
		}
	}
	for _, row := range rows(before) {
		if !is[row] {
			removed = append(removed, row)
		}
	}
	return added, removed
}

func touching(scope scopeTable, paths []changedPath, action string) []string {
	var hit []string
	for _, p := range paths {
		if r := scope.match(p.Path); r != nil {
			for _, a := range r.Actions {
				if a == action {
					hit = append(hit, p.Path)
					break
				}
			}
		}
	}
	return hit
}

// cmdGates prints, as KEY=VALUE lines a shell reads without eval, the gates
// a feature selection runs and the specs it names.
func cmdGates(args []string) int {
	fs := flag.NewFlagSet("gates", flag.ContinueOnError)
	selFile := fs.String("select", "", "a selection written by proofrun select")
	if err := fs.Parse(args); err != nil || *selFile == "" {
		fmt.Fprintln(os.Stderr, "proofrun gates -select FILE")
		return 2
	}
	var sel selection
	if err := readJSON(*selFile, &sel); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gates:", err)
		return 2
	}
	writeGates(os.Stdout, &sel)
	return 0
}

func writeGates(w io.Writer, sel *selection) {
	yes := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	integration := false
	for _, ps := range sel.Integration {
		if ps.Whole != "" || len(ps.run()) > 0 {
			integration = true
		}
	}
	specs := ""
	switch {
	case sel.AllSpecs != "":
		specs = "ALL"
	default:
		var files []string
		for _, s := range sel.Specs {
			files = append(files, strings.TrimPrefix(s.File, "ui/"))
		}
		specs = strings.Join(files, " ")
	}
	fmt.Fprintf(w, "integration=%s\n", yes(integration))
	fmt.Fprintf(w, "ui_check=%s\n", yes(sel.Gates.UICheck))
	fmt.Fprintf(w, "ui_unit=%s\n", yes(sel.Gates.UIUnit))
	fmt.Fprintf(w, "site=%s\n", yes(sel.Gates.Site))
	fmt.Fprintf(w, "ui_e2e=%s\n", yes(specs != ""))
	fmt.Fprintf(w, "activity_fixture=%s\n", yes(sel.Gates.Fixture))
	fmt.Fprintf(w, "observability=%s\n", yes(sel.Gates.Observability))
	fmt.Fprintf(w, "specs=%s\n", specs)
}

// cmdEvidenceArgs turns a pull request body's `Proof-tests:` and
// `Proof-specs:` lines into the arguments a feature proof takes, one per
// line, so a shell reads them without eval. A malformed line is exit 2.
func cmdEvidenceArgs(args []string) int {
	fs := flag.NewFlagSet("evidence-args", flag.ContinueOnError)
	body := fs.String("body", "", "a file holding the pull request body or the commit message")
	if err := fs.Parse(args); err != nil || *body == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun evidence-args -body FILE")
		return 2
	}
	f, err := os.Open(*body)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun evidence-args:", err)
		return 2
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 4<<20)
	var out []string
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "Proof-tests:"):
			v := strings.TrimSpace(strings.TrimPrefix(line, "Proof-tests:"))
			pkg, re, ok := strings.Cut(v, ":")
			if !ok || strings.TrimSpace(pkg) == "" || strings.TrimSpace(re) == "" {
				fmt.Fprintf(os.Stderr, "proofrun evidence-args: line %d: want Proof-tests: <package>:<regexp>\n", n)
				return 2
			}
			if _, err := regexp.Compile(re); err != nil {
				fmt.Fprintf(os.Stderr, "proofrun evidence-args: line %d: %v\n", n, err)
				return 2
			}
			out = append(out, "--tests", v)
		case strings.HasPrefix(line, "Proof-specs:"):
			v := strings.TrimSpace(strings.TrimPrefix(line, "Proof-specs:"))
			if v == "" {
				fmt.Fprintf(os.Stderr, "proofrun evidence-args: line %d: Proof-specs: names no file\n", n)
				return 2
			}
			if st, err := os.Stat(v); err != nil || st.IsDir() {
				fmt.Fprintf(os.Stderr, "proofrun evidence-args: line %d: %s is not a file\n", n, v)
				return 2
			}
			out = append(out, "--specs", v)
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun evidence-args:", err)
		return 2
	}
	for _, s := range out {
		fmt.Println(s)
	}
	return 0
}

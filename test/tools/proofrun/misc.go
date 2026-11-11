package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/internal/stablejson"
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

// cmdTouches writes the gate files the change since the merge base with
// -base edits, and the rows the excusing lists add or remove, as the JSON
// proof.json carries as lists_changed (plan §6.4).
func cmdTouches(args []string) int {
	fs := flag.NewFlagSet("touches", flag.ContinueOnError)
	base := fs.String("base", "main", "the ref the change is measured from")
	gateFiles := fs.Bool("gate-files", false, "list the changed gate files and the rows the excusing lists add or remove")
	if err := fs.Parse(args); err != nil || !*gateFiles || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun touches -base REF -gate-files")
		return 2
	}
	return touchesGateFiles(os.Stdout, *base)
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
	b, mErr := stablejson.MarshalIndent(changes, "  ")
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
// and the composite action. .github/CODEOWNERS should cover these; no test
// holds the two to each other.
var gateFilePatterns = []string{
	"scripts/proof.sh",
	"scripts/proof-part.sh",
	"scripts/proof-nightly-tests.txt",
	"scripts/proof-nightly-specs.txt",
	"scripts/proof-quarantine.txt",
	"scripts/proof-allowed-skips.txt",
	"scripts/proof-test-costs.tsv",
	"scripts/proof-ui-costs.tsv",
	"scripts/integration.sh",
	"scripts/github-merge-gate.sh",
	"scripts/github/",
	".github/",
	"test/tools/proofrun/",
	// The helper a Go test checks a wall-clock budget through: it decides
	// the bound a host is held to.
	"internal/testsupport/proof/",
	"Makefile",
	"ui/playwright.config.ts",
}

// excusingLists are the lists that excuse a test or a failure; a change to
// one is reported with the rows it adds and removes, so a reader sees exactly
// which allowance moved.
var excusingLists = []string{
	"scripts/proof-nightly-tests.txt",
	"scripts/proof-nightly-specs.txt",
	"scripts/proof-quarantine.txt",
	"scripts/proof-allowed-skips.txt",
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

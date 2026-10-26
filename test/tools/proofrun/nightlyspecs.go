package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// The nightly spec list (scripts/proof-nightly-specs.txt) is the Playwright
// side of scripts/proof-nightly-tests.txt: spec files a full proof defers to
// the nightly tier and the pre-release gate. A row names a spec file, the
// merge-gate spec files that cover its behaviour at a smaller size, the day
// the user approved the move and who, and why. It is parsed as strictly as
// the test list, and anchored the same way: a full proof defers only the
// rows its base's list also carries.

// nightlySpecFile is the tree's list, read from the repository root.
const nightlySpecFile = "scripts/proof-nightly-specs.txt"

// nightlySpecEntry is one row of the list.
type nightlySpecEntry struct {
	Spec     string   // repository path, ui/e2e/<name>.spec.ts
	Covering []string // repository paths of the merge-gate specs that cover it
	Approved string
	Reason   string
	Line     int
	Text     string
}

// file is the spec as the listing and the ledgers name it: relative to the
// Playwright test directory, ui/e2e.
func (e nightlySpecEntry) file() string { return strings.TrimPrefix(e.Spec, "ui/e2e/") }

func (e nightlySpecEntry) coveringFiles() []string {
	out := make([]string, 0, len(e.Covering))
	for _, c := range e.Covering {
		out = append(out, strings.TrimPrefix(c, "ui/e2e/"))
	}
	return out
}

var specPath = regexp.MustCompile(`^ui/e2e/[A-Za-z0-9][A-Za-z0-9._-]*\.spec\.ts$`)

// readNightlySpecs parses the list; an absent file is an empty list, because
// the base of a change may predate it.
func readNightlySpecs(file string) ([]nightlySpecEntry, error) {
	if file == "" {
		return nil, nil
	}
	b, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parseNightlySpecs(bytes.NewReader(b), file)
}

func parseNightlySpecs(r io.Reader, name string) ([]nightlySpecEntry, error) {
	var out []nightlySpecEntry
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
			return nil, fmt.Errorf("%s:%d: want \"spec<TAB>covering specs<TAB>approved<TAB>reason\", got %d columns", name, n, len(f))
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
			if f[i] == "" {
				return nil, fmt.Errorf("%s:%d: column %d is empty", name, n, i+1)
			}
		}
		if !specPath.MatchString(f[0]) {
			return nil, fmt.Errorf("%s:%d: %q is not a spec file under ui/e2e/", name, n, f[0])
		}
		if !approvedColumn.MatchString(f[2]) {
			return nil, fmt.Errorf("%s:%d: approved column %q is not \"YYYY-MM-DD <name>\"", name, n, f[2])
		}
		var covering []string
		for _, c := range strings.Split(f[1], ",") {
			c = strings.TrimSpace(c)
			if !specPath.MatchString(c) {
				return nil, fmt.Errorf("%s:%d: covering spec %q is not a spec file under ui/e2e/", name, n, c)
			}
			if c == f[0] {
				return nil, fmt.Errorf("%s:%d: %s covers itself", name, n, c)
			}
			covering = append(covering, c)
		}
		e := nightlySpecEntry{Spec: f[0], Covering: covering, Approved: f[2], Reason: f[3], Line: n, Text: text}
		if prev, dup := seen[e.Spec]; dup {
			return nil, fmt.Errorf("%s:%d: %s is already listed on line %d", name, n, e.Spec, prev)
		}
		seen[e.Spec] = n
		out = append(out, e)
	}
	return out, sc.Err()
}

// nightlySpecProblems checks the list against a listing's spec files: a row
// whose spec the listing does not hold is stale. With complete set (a full
// or release listing, which holds every spec file), a covering spec must be
// a listed file and not itself on the list; the nightly tier lists only the
// specs it runs, so there the covering specs are not checked.
func nightlySpecProblems(entries []nightlySpecEntry, listed map[string]bool, complete bool) []string {
	var problems []string
	on := map[string]bool{}
	for _, e := range entries {
		on[e.Spec] = true
	}
	for _, e := range entries {
		if !listed[e.file()] {
			problems = append(problems, fmt.Sprintf("nightly spec list line %d names %s, which the listing does not hold: remove the stale entry", e.Line, e.Spec))
		}
		if !complete {
			continue
		}
		for _, c := range e.Covering {
			switch {
			case on[c]:
				problems = append(problems, fmt.Sprintf("nightly spec list line %d: covering spec %s is itself on the nightly spec list", e.Line, c))
			case !listed[strings.TrimPrefix(c, "ui/e2e/")]:
				problems = append(problems, fmt.Sprintf("nightly spec list line %d: covering spec %s is not a listed spec file", e.Line, c))
			}
		}
	}
	return problems
}

// deferredSpecSet is what the full tier defers: the rows both the tree's
// list and its base's list carry, keyed by the spec as the listing names it.
func deferredSpecSet(tree, base []nightlySpecEntry) map[string]nightlySpecEntry {
	inBase := map[string]bool{}
	for _, e := range base {
		inBase[e.Spec] = true
	}
	out := map[string]nightlySpecEntry{}
	for _, e := range tree {
		if inBase[e.Spec] {
			out[e.file()] = e
		}
	}
	return out
}

func specEntryLines(entries []nightlySpecEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Spec)
	}
	sort.Strings(out)
	return out
}

// nightlyUIShards is how many UI parts a nightly CI proof runs: its UI plan
// holds the listed spec files alone, so at most one shard per row, at most
// the requested shards, and none when the list is empty.
func nightlyUIShards(requested int, rows []nightlySpecEntry) int {
	return max(0, min(requested, len(rows)))
}

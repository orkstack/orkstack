package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// The nightly list (scripts/proof-nightly-tests.txt) names the integration
// tests the full tier defers to the nightly tier and the pre-release gate
// (plan §4). It changes only with the user's approval, and it is parsed as
// strictly as the allowed skips: a row the parser cannot read, an approval
// that names no date and person, a test its package does not list, or a
// covering test that is not a listed test of the same package all fail.

// nightlyEntry is one row of the nightly list.
type nightlyEntry struct {
	Package  string
	Test     string
	Covering []string
	Approved string
	Reason   string
	Line     int
	Text     string
}

func (e nightlyEntry) key() testKey { return testKey{e.Package, e.Test} }

var approvedColumn = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \S.*$`)

// readNightlyList parses the list; an absent file is an empty list, because
// the base of a change may predate the list.
func readNightlyList(file string) ([]nightlyEntry, error) {
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
	return parseNightlyList(bytes.NewReader(b), file)
}

func parseNightlyList(r io.Reader, name string) ([]nightlyEntry, error) {
	var out []nightlyEntry
	seen := map[testKey]int{}
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		text := sc.Text()
		if strings.TrimSpace(text) == "" || strings.HasPrefix(strings.TrimSpace(text), "#") {
			continue
		}
		f := strings.Split(text, "\t")
		if len(f) != 5 {
			return nil, fmt.Errorf("%s:%d: want \"package<TAB>test<TAB>covering tests<TAB>approved<TAB>reason\", got %d columns", name, n, len(f))
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
			if f[i] == "" {
				return nil, fmt.Errorf("%s:%d: column %d is empty", name, n, i+1)
			}
		}
		if !listingName.MatchString(f[1]) {
			return nil, fmt.Errorf("%s:%d: %q is not a test name", name, n, f[1])
		}
		if !approvedColumn.MatchString(f[3]) {
			return nil, fmt.Errorf("%s:%d: approved column %q is not \"YYYY-MM-DD <name>\"", name, n, f[3])
		}
		var covering []string
		for _, c := range strings.Split(f[2], ",") {
			c = strings.TrimSpace(c)
			if !listingName.MatchString(c) {
				return nil, fmt.Errorf("%s:%d: covering test %q is not a test name", name, n, c)
			}
			covering = append(covering, c)
		}
		e := nightlyEntry{Package: f[0], Test: f[1], Covering: covering, Approved: f[3], Reason: f[4], Line: n, Text: text}
		if prev, dup := seen[e.key()]; dup {
			return nil, fmt.Errorf("%s:%d: %s is already listed on line %d", name, n, e.key(), prev)
		}
		seen[e.key()] = n
		out = append(out, e)
	}
	return out, sc.Err()
}

// nightlyProblems checks the tree's list against the packages' listings: a
// listed test its compiled package does not list is stale, and a covering
// test must be a listed test of the same package and not itself on the list.
// An entry whose package no plan package compiles is checked by nothing here.
func nightlyProblems(entries []nightlyEntry, listed map[string]map[string]bool) []string {
	var problems []string
	on := map[testKey]bool{}
	for _, e := range entries {
		on[e.key()] = true
	}
	for _, e := range entries {
		names, compiled := listed[e.Package]
		if !compiled {
			problems = append(problems, fmt.Sprintf("nightly list line %d names package %s, which is not an integration package of this proof", e.Line, e.Package))
			continue
		}
		if !names[e.Test] {
			problems = append(problems, fmt.Sprintf("nightly list line %d names %s, which its package does not list: remove the stale entry", e.Line, e.key()))
		}
		for _, c := range e.Covering {
			switch {
			case !names[c]:
				problems = append(problems, fmt.Sprintf("nightly list line %d: covering test %s.%s is not a listed test of that package", e.Line, e.Package, c))
			case on[testKey{e.Package, c}]:
				problems = append(problems, fmt.Sprintf("nightly list line %d: covering test %s.%s is itself on the nightly list", e.Line, e.Package, c))
			}
		}
	}
	return problems
}

// deferredSet is what the full tier defers: the entries both the tree's list
// and its base's list carry (R3-M2). An entry the change itself adds is run.
func deferredSet(tree, base []nightlyEntry) map[testKey]nightlyEntry {
	inBase := map[testKey]bool{}
	for _, e := range base {
		inBase[e.key()] = true
	}
	out := map[testKey]nightlyEntry{}
	for _, e := range tree {
		if inBase[e.key()] {
			out[e.key()] = e
		}
	}
	return out
}

func entryLines(entries []nightlyEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Package+"\t"+e.Test)
	}
	sort.Strings(out)
	return out
}

// fileSHA256 is the hex sha256 of a file's bytes; an absent file hashes as
// empty, the same as an empty list.
func fileSHA256(file string) (string, error) {
	if file == "" {
		return bytesSHA256(nil), nil
	}
	b, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return bytesSHA256(nil), nil
	}
	if err != nil {
		return "", err
	}
	return bytesSHA256(b), nil
}

func bytesSHA256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

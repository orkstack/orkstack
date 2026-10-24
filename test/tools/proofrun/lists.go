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
	"time"
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
// An entry whose package no plan package compiles is checked by nothing here
// in a scoped run (complete false: a selection, -run or explicit package
// patterns), which compiles only some integration packages. A complete run
// plans every integration package, so there such an entry is a stale or
// mistyped package and a problem.
func nightlyProblems(entries []nightlyEntry, listed map[string]map[string]bool, complete bool) []string {
	var problems []string
	on := map[testKey]bool{}
	for _, e := range entries {
		on[e.key()] = true
	}
	for _, e := range entries {
		names, compiled := listed[e.Package]
		if !compiled {
			if !complete {
				continue
			}
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

// The quarantine list (scripts/proof-quarantine.txt) names the tests whose
// failures are known, owned and dated (plan §6.3). It is parsed as strictly
// as the allowed skips, and stricter: every row carries an owner with an
// address, a since date, a review-by date no more than 30 days later, and a
// reason; at most five rows exist; an entry names a listed test; and a test
// may not be both quarantined and an allowed skip. Quarantine never makes a
// pass: a quarantined failure is recorded as `quarantined-fail`, with its
// entry, and is never evidence the merge gate counts as success.

// quarantineEntry is one row of the quarantine list.
type quarantineEntry struct {
	Suite    string
	Package  string
	Test     string
	Owner    string
	Since    string
	ReviewBy string
	Reason   string
	Line     int
	Text     string
}

func (e quarantineEntry) key() testKey { return testKey{e.Package, e.Test} }

// maxQuarantine is the budget: at most this many dated debts at a time.
const maxQuarantine = 5

var isoDate = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// readQuarantine parses the list; an absent file is an empty list, because a
// base predating the list excuses nothing.
func readQuarantine(file string) ([]quarantineEntry, error) {
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
	return parseQuarantine(bytes.NewReader(b), file)
}

func parseQuarantine(r io.Reader, name string) ([]quarantineEntry, error) {
	var out []quarantineEntry
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
		if len(f) != 7 {
			return nil, fmt.Errorf("%s:%d: want \"suite<TAB>package<TAB>test<TAB>owner<TAB>since<TAB>review-by<TAB>reason\", got %d columns", name, n, len(f))
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
			if f[i] == "" {
				return nil, fmt.Errorf("%s:%d: column %d is empty", name, n, i+1)
			}
		}
		switch f[0] {
		case "unit", "integration", "ui-e2e", "any":
		default:
			return nil, fmt.Errorf("%s:%d: suite %q is not unit, integration, ui-e2e or any", name, n, f[0])
		}
		if !listingName.MatchString(strings.SplitN(f[2], "/", 2)[0]) {
			return nil, fmt.Errorf("%s:%d: %q is not a test name", name, n, f[2])
		}
		if !strings.Contains(f[3], "<") || !strings.Contains(f[3], "@") || !strings.HasSuffix(f[3], ">") {
			return nil, fmt.Errorf("%s:%d: owner %q is not \"Name <address>\"", name, n, f[3])
		}
		since, err := time.Parse("2006-01-02", f[4])
		if err != nil || !isoDate.MatchString(f[4]) {
			return nil, fmt.Errorf("%s:%d: since %q is not a YYYY-MM-DD date", name, n, f[4])
		}
		review, err := time.Parse("2006-01-02", f[5])
		if err != nil || !isoDate.MatchString(f[5]) {
			return nil, fmt.Errorf("%s:%d: review-by %q is not a YYYY-MM-DD date", name, n, f[5])
		}
		if review.Before(since) {
			return nil, fmt.Errorf("%s:%d: review-by %s is before since %s", name, n, f[5], f[4])
		}
		if review.Sub(since) > 30*24*time.Hour {
			return nil, fmt.Errorf("%s:%d: review-by %s is more than 30 days after since %s", name, n, f[5], f[4])
		}
		e := quarantineEntry{Suite: f[0], Package: f[1], Test: f[2], Owner: f[3], Since: f[4], ReviewBy: f[5], Reason: f[6], Line: n, Text: text}
		if prev, dup := seen[e.key()]; dup {
			return nil, fmt.Errorf("%s:%d: %s is already listed on line %d", name, n, e.key(), prev)
		}
		seen[e.key()] = n
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) > maxQuarantine {
		return nil, fmt.Errorf("%s: %d entries; the budget is %d quarantined tests at a time", name, len(out), maxQuarantine)
	}
	return out, nil
}

// quarantineProblems checks the list against what the proof lists and what
// the allowlist excuses: a stale name fails the gate, and a test cannot be
// both quarantined and an allowed skip.
func quarantineProblems(entries []quarantineEntry, listed map[string]map[string]bool, allow []allowedSkip) []string {
	var problems []string
	allowed := map[testKey]bool{}
	for _, a := range allow {
		allowed[testKey{a.Package, a.Test}] = true
	}
	for _, e := range entries {
		top := testKey{e.Package, strings.SplitN(e.Test, "/", 2)[0]}
		if names, compiled := listed[e.Package]; compiled && !names[top.Test] {
			problems = append(problems, fmt.Sprintf(
				"quarantine line %d names %s, which its package does not list: remove the stale entry", e.Line, e.key()))
		}
		if allowed[e.key()] {
			problems = append(problems, fmt.Sprintf(
				"quarantine line %d names %s, which is also an allowed skip: a test is excused one way", e.Line, e.key()))
		}
	}
	return problems
}

// quarantineSet is what actually excuses a failure in this proof: entries
// the base also carries, whose review-by date has not passed. An entry a
// change adds excuses nothing until main carries it, and an expired entry
// stops excusing.
func quarantineSet(tree, base []quarantineEntry, checkedOn time.Time) map[testKey]quarantineEntry {
	inBase := map[testKey]bool{}
	for _, e := range base {
		inBase[e.key()] = true
	}
	out := map[testKey]quarantineEntry{}
	for _, e := range tree {
		if !inBase[e.key()] {
			continue
		}
		review, err := time.Parse("2006-01-02", e.ReviewBy)
		if err != nil || checkedOn.After(review) {
			continue
		}
		out[e.key()] = e
	}
	return out
}

// quarantineExpiring names the entries whose review date is within seven
// days of checkedOn, so every proof warns before an entry stops excusing.
func quarantineExpiring(entries []quarantineEntry, checkedOn time.Time) []quarantineEntry {
	var out []quarantineEntry
	for _, e := range entries {
		review, err := time.Parse("2006-01-02", e.ReviewBy)
		if err != nil {
			continue
		}
		if !review.Before(checkedOn) && review.Sub(checkedOn) <= 7*24*time.Hour {
			out = append(out, e)
		}
	}
	return out
}

// quarantineProof reads a proof's quarantine lists and decides what excuses
// here. Problem strings are gate failures; expiring entries are warnings,
// printed by every caller and never a refusal.
func quarantineProof(treeFile, baseFile string, listed map[string]map[string]bool, allow []allowedSkip,
	checkedOn time.Time,
) (map[testKey]quarantineEntry, []string, []quarantineEntry, error) {
	tree, err := readQuarantine(treeFile)
	if err != nil {
		return nil, nil, nil, err
	}
	base, err := readQuarantine(baseFile)
	if err != nil {
		return nil, nil, nil, err
	}
	return quarantineSet(tree, base, checkedOn), quarantineProblems(tree, listed, allow),
		quarantineExpiring(tree, checkedOn), nil
}

// quarantineWarning is the WARNING every proof's README, the proof job's
// summary and merge-check print for an entry within seven days of its date.
func quarantineWarning(e quarantineEntry) string {
	return fmt.Sprintf("WARNING: quarantine entry for %s expires on %s and stops excusing its failure: fix it, or renew it with the user's approval",
		e.key(), e.ReviewBy)
}

// printQuarantineWarnings says which entries stop excusing soon.
func printQuarantineWarnings(w io.Writer, entries []quarantineEntry) {
	for _, e := range entries {
		fmt.Fprintln(w, quarantineWarning(e))
	}
}

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
)

// The scope table (scripts/proof-scope.tsv) says what a feature proof must run
// for each changed path. It fails closed: a path no row matches refuses the
// feature tier, so a new kind of file can never be silently unproven.

// Actions a scope row can name.
const (
	actGo            = "go"            // the Go rule for the file's package
	actUI            = "ui"            // UI source: type-check, unit, needs an evidence spec
	actSpec          = "spec"          // a Playwright spec: run it
	actUIUnit        = "ui-unit"       // a UI unit test file
	actAllSpecs      = "all-specs"     // the Playwright harness: run every spec
	actProto         = "proto"         // proto source: UI type-check and unit
	actImages        = "images"        // the activity contract fixture + the Go rule for internal/orker
	actExamples      = "examples"      // the example tests and the examples spec
	actTool          = "tool"          // the tests and specs that run this test/tools tool
	actObservability = "observability" // the observability gate
	actNone          = "none"          // no evidence needed
	actFull          = "full"          // refuse the feature tier
)

var knownActions = map[string]bool{
	actGo: true, actUI: true, actSpec: true, actUIUnit: true, actAllSpecs: true, actProto: true,
	actImages: true, actExamples: true, actTool: true, actObservability: true, actNone: true, actFull: true,
}

// scopeRule is one row: a glob, the actions it triggers, and its flags.
type scopeRule struct {
	Pattern string
	Actions []string
	// Temporal marks a Temporal- or storage-facing package: its evidence must
	// include an integration test.
	Temporal bool
	Line     int
	re       *regexp.Regexp
}

// scopeTable is the ordered rule list; the first matching row wins.
type scopeTable []scopeRule

func readScope(file string) (scopeTable, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseScope(f, file)
}

func parseScope(r io.Reader, name string) (scopeTable, error) {
	var t scopeTable
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		var cols []string
		for _, f := range fields {
			if f = strings.TrimSpace(f); f != "" {
				cols = append(cols, f)
			}
		}
		if len(cols) < 2 {
			return nil, fmt.Errorf("%s:%d: want \"pattern<TAB>actions[<TAB>flags]\"", name, n)
		}
		rule := scopeRule{Pattern: cols[0], Line: n}
		for _, a := range strings.Split(cols[1], ",") {
			a = strings.TrimSpace(a)
			if !knownActions[a] {
				return nil, fmt.Errorf("%s:%d: unknown action %q", name, n, a)
			}
			rule.Actions = append(rule.Actions, a)
		}
		if len(cols) > 2 {
			for _, flag := range strings.Split(cols[2], ",") {
				switch strings.TrimSpace(flag) {
				case "temporal":
					rule.Temporal = true
				default:
					return nil, fmt.Errorf("%s:%d: unknown flag %q", name, n, flag)
				}
			}
		}
		re, err := globRegexp(rule.Pattern)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %v", name, n, err)
		}
		rule.re = re
		t = append(t, rule)
	}
	return t, sc.Err()
}

// globRegexp compiles a path glob: `**` matches any number of path segments
// (including none), `*` anything but a slash, `?` one character but a slash.
// A pattern without a slash matches the base name at any depth.
func globRegexp(glob string) (*regexp.Regexp, error) {
	if glob == "" {
		return nil, fmt.Errorf("empty pattern")
	}
	var b strings.Builder
	b.WriteString("^")
	if !strings.Contains(glob, "/") {
		b.WriteString("(?:.*/)?")
	}
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch {
		case c == '*' && i+1 < len(glob) && glob[i+1] == '*':
			i++
			if i+1 < len(glob) && glob[i+1] == '/' {
				i++
				b.WriteString("(?:.*/)?")
			} else {
				b.WriteString(".*")
			}
		case c == '*':
			b.WriteString("[^/]*")
		case c == '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

// match returns the first row matching p, or nil.
func (t scopeTable) match(p string) *scopeRule {
	p = path.Clean(p)
	for i := range t {
		if t[i].re.MatchString(p) {
			return &t[i]
		}
	}
	return nil
}

// temporalFacing reports whether the scope table marks the package in dir
// (a repository-relative directory) as Temporal- or storage-facing: whether
// the row matching a Go file in it carries the flag.
func (t scopeTable) temporalFacing(dir string) bool {
	if r := t.match(path.Join(dir, "x.go")); r != nil {
		return r.Temporal
	}
	return false
}

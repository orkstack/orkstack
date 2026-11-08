package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// conventionalTypes are the Conventional Commit types the repository takes
// (AGENTS.md §12), the closed set a pull request's title may start with.
var conventionalTypes = []string{"feat", "fix", "docs", "chore", "test", "ci", "refactor", "perf", "build", "style", "revert"}

// conventionalShape is type(scope)!: description, with the scope and the ! both
// optional and exactly one space after the colon. A scope may name several
// parts, as the history does: fix(orker,engine).
var conventionalShape = regexp.MustCompile(`^([a-z]+)(?:\(([a-z0-9][a-z0-9._/,-]*)\))?(!)?: (.+)$`)

// conventionalTitle is a title the check accepted, in its parts.
type conventionalTitle struct {
	Type        string
	Scope       string
	Breaking    bool
	Description string
}

// parseConventionalTitle reads a commit's subject or a pull request's title,
// which becomes the subject of the squash commit that lands it, and refuses one
// that is not a Conventional Commit. A refusal is one line, naming what is
// wrong; the title in it is quoted, so it cannot begin a line of a CI log.
func parseConventionalTitle(title string) (conventionalTitle, error) {
	if !utf8.ValidString(title) {
		return conventionalTitle{}, fmt.Errorf("%q is not valid UTF-8", title)
	}
	if strings.ContainsFunc(title, invisibleInTitle) {
		return conventionalTitle{}, fmt.Errorf("%q holds a control character", title)
	}
	m := conventionalShape.FindStringSubmatch(title)
	if m == nil {
		return conventionalTitle{}, fmt.Errorf("%q must read type(scope)!: description, the scope and the ! optional, one space after the colon, its type one of %s", title, strings.Join(conventionalTypes, ", "))
	}
	if !slices.Contains(conventionalTypes, m[1]) {
		return conventionalTitle{}, fmt.Errorf("in %q its type %q is not one of %s", title, m[1], strings.Join(conventionalTypes, ", "))
	}
	if m[4] != strings.TrimSpace(m[4]) {
		return conventionalTitle{}, fmt.Errorf("in %q the description starts or ends with whitespace", title)
	}
	return conventionalTitle{Type: m[1], Scope: m[2], Breaking: m[3] == "!", Description: m[4]}, nil
}

// invisibleInTitle is a rune that changes how a subject reads without showing:
// a control or format character, such as a bidirectional override, or a line or
// paragraph separator.
func invisibleInTitle(r rune) bool {
	return unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp)
}

// cmdPRTitle is `proofrun pr-title`, behind the required pr-title check
// (.github/workflows/pr-title.yml): exit 0 for a Conventional Commit, 1 for
// anything else, 2 for an invocation that names no title. An empty -title is a
// bad title, never a check that did not run.
func cmdPRTitle(args []string) int {
	fs := flag.NewFlagSet("pr-title", flag.ContinueOnError)
	title := fs.String("title", "", "the pull request's title")
	err := fs.Parse(args)
	given := false
	fs.Visit(func(f *flag.Flag) { given = given || f.Name == "title" })
	if err != nil || fs.NArg() > 0 || !given {
		fmt.Fprintln(os.Stderr, "proofrun pr-title -title TITLE")
		return 2
	}
	if _, err := parseConventionalTitle(*title); err != nil {
		fmt.Printf("pr-title: %v\n", err)
		fmt.Println("pr-title: the title becomes the subject of the squash commit that lands this pull request: edit the title, and this check runs again")
		return 1
	}
	fmt.Printf("pr-title: %q is a Conventional Commit\n", *title)
	return 0
}

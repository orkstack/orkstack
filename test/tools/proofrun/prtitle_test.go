package main

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The table of titles the check accepts and refuses. The accepted rows carry
// every type the repository uses and the shapes its history holds.
var prTitleCases = []struct {
	title string
	want  string // the refusal's words; empty means accepted
}{
	{"feat: add a thing", ""},
	{"fix: repair a thing", ""},
	{"docs: describe a thing", ""},
	{"chore: tidy a thing", ""},
	{"test: prove a thing", ""},
	{"ci: run a thing", ""},
	{"refactor: move a thing", ""},
	{"perf: speed a thing", ""},
	{"build: compile a thing", ""},
	{"style: format a thing", ""},
	{"revert: feat(engine): add a thing", ""},
	{"feat(engine): add a thing", ""},
	{"fix(orker,engine): repair both", ""},
	{"chore(deps): bump the go group with 3 updates", ""},
	{"chore(merge): the fifth train", ""},
	{"feat!: remove a thing", ""},
	{"feat(dsl)!: remove a key", ""},
	{"fix(ui): selecting a map cell lands on its task (#1234)", ""},
	{"docs: explain naïve café handling", ""},

	{"", "must read type(scope)!: description"},
	{"add a thing", "must read type(scope)!: description"},
	{"Merge branch 'main' into feature", "must read type(scope)!: description"},
	{`Revert "feat: add a thing"`, "must read type(scope)!: description"},
	{"Bump golang from 1.27.3 to 1.27.4", "must read type(scope)!: description"},
	{"feat:add a thing", "must read type(scope)!: description"},
	{"feat:  add a thing", "starts or ends with whitespace"},
	{"feat: ", "must read type(scope)!: description"},
	{"feat:", "must read type(scope)!: description"},
	{"feat : add a thing", "must read type(scope)!: description"},
	{"feat (engine): add a thing", "must read type(scope)!: description"},
	{"feat(): add a thing", "must read type(scope)!: description"},
	{"feat(Engine): add a thing", "must read type(scope)!: description"},
	{"feat(engine: add a thing", "must read type(scope)!: description"},
	{"feat(engine)! add a thing", "must read type(scope)!: description"},
	{"feat!(engine): add a thing", "must read type(scope)!: description"},
	{"Feat: add a thing", "must read type(scope)!: description"},
	{"FIX: repair a thing", "must read type(scope)!: description"},
	{" feat: add a thing", "must read type(scope)!: description"},
	{"feat: add a thing ", "starts or ends with whitespace"},
	{"wip: half a thing", `its type "wip" is not one of`},
	{"feature: add a thing", `its type "feature" is not one of`},
	{"fixup: repair a thing", `its type "fixup" is not one of`},
	{"feat: add\na thing", "control character"},
	{"feat: add\ta thing", "control character"},
	{"feat: add\x1b[31m a thing", "control character"},
	{"feat: add\u202e a thing", "control character"},
	{"feat: add\u200b a thing", "control character"},
	{"feat: add\u2028 a thing", "control character"},
	{"feat: add \xff a thing", "not valid UTF-8"},
}

func TestPRTitle_AcceptsAndRefuses(t *testing.T) {
	for _, c := range prTitleCases {
		t.Run(c.title, func(t *testing.T) {
			_, err := parseConventionalTitle(c.title)
			switch {
			case c.want == "" && err != nil:
				t.Errorf("refused: %v", err)
			case c.want != "" && err == nil:
				t.Errorf("accepted, want a refusal naming %q", c.want)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Errorf("refused with %q, want %q", err, c.want)
			}
		})
	}
}

func TestPRTitle_ReadsItsParts(t *testing.T) {
	for _, c := range []struct {
		title string
		want  conventionalTitle
	}{
		{"feat: add", conventionalTitle{Type: "feat", Description: "add"}},
		{"fix(orker): repair", conventionalTitle{Type: "fix", Scope: "orker", Description: "repair"}},
		{"docs!: drop", conventionalTitle{Type: "docs", Breaking: true, Description: "drop"}},
		{"ci(proof)!: drop", conventionalTitle{Type: "ci", Scope: "proof", Breaking: true, Description: "drop"}},
		{"revert: feat(x): y", conventionalTitle{Type: "revert", Description: "feat(x): y"}},
	} {
		got, err := parseConventionalTitle(c.title)
		if err != nil {
			t.Errorf("%q: %v", c.title, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q = %+v, want %+v", c.title, got, c.want)
		}
	}
}

// Every type a refusal names is one the check accepts, and the list is the one
// the repository's history uses.
func TestPRTitle_TheTypesAreTheRepositorysOwn(t *testing.T) {
	want := []string{"feat", "fix", "docs", "chore", "test", "ci", "refactor", "perf", "build", "style", "revert"}
	if !slices.Equal(conventionalTypes, want) {
		t.Errorf("conventionalTypes = %v, want %v", conventionalTypes, want)
	}
	_, err := parseConventionalTitle("wip: a thing")
	if err == nil {
		t.Fatal("a type outside the list was accepted")
	}
	for _, typ := range want {
		if !strings.Contains(err.Error(), typ) {
			t.Errorf("the refusal %q does not name %s", err, typ)
		}
		if _, err := parseConventionalTitle(typ + ": a thing"); err != nil {
			t.Errorf("%s is listed and refused: %v", typ, err)
		}
	}
}

// capturePRTitle runs the subcommand and returns its exit status and what it
// wrote to its standard output and error.
func capturePRTitle(t *testing.T, args ...string) (int, string) {
	t.Helper()
	r, w, err := os.Pipe()
	must.OK(t, err)
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	code := runCommand(append([]string{"pr-title"}, args...))
	os.Stdout, os.Stderr = stdout, stderr
	must.OK(t, w.Close())
	return code, <-done
}

func TestPRTitle_TheSubcommandsExitStatus(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		code int
		want string
	}{
		{"a good title", []string{"-title", "feat(engine): add a thing"}, 0, "is a Conventional Commit"},
		{"a bad title", []string{"-title", "add a thing"}, 1, "must read type(scope)!: description"},
		// The workflow passes "$PR_TITLE": an empty title is a bad title, never
		// a check that did not run.
		{"an empty title", []string{"-title", ""}, 1, "must read type(scope)!: description"},
		{"a title that looks like a flag", []string{"-title", "-x"}, 1, "must read type(scope)!: description"},
		{"no title flag", nil, 2, "pr-title -title"},
		{"a stray argument", []string{"-title", "feat: x", "y"}, 2, "pr-title -title"},
		{"an unknown flag", []string{"-nope"}, 2, "pr-title -title"},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, out := capturePRTitle(t, c.args...)
			if code != c.code || !strings.Contains(out, c.want) {
				t.Errorf("exit = %d, want %d, with %q in:\n%s", code, c.code, c.want, out)
			}
		})
	}
}

// A runner reads a line that starts with :: as a workflow command, so a
// refusal never lets a title begin a line of the log.
func TestPRTitle_ARefusalKeepsTheTitleOnItsOwnLine(t *testing.T) {
	code, out := capturePRTitle(t, "-title", "bad\n::add-mask::x\n::error::forged")
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "::") {
			t.Errorf("the title began a line of the log: %q", line)
		}
	}
}

// FuzzPRTitle: a pull request's title is input nobody here controls. Parsing
// never panics, every accepted title reads back to itself from its parts, holds
// a listed type and no control character, and every refusal says why on one
// line.
func FuzzPRTitle(f *testing.F) {
	for _, c := range prTitleCases {
		f.Add(c.title)
	}
	f.Fuzz(func(t *testing.T, title string) {
		got, err := parseConventionalTitle(title)
		if err != nil {
			if err.Error() == "" {
				t.Fatalf("%q was refused with no reason", title)
			}
			if strings.ContainsAny(err.Error(), "\n\r") {
				t.Fatalf("the refusal of %q spans lines: %q", title, err)
			}
			return
		}
		if !slices.Contains(conventionalTypes, got.Type) {
			t.Fatalf("%q was accepted with the type %q", title, got.Type)
		}
		if got.Description == "" {
			t.Fatalf("%q was accepted with no description", title)
		}
		if !utf8.ValidString(title) {
			t.Fatalf("%q was accepted and is not UTF-8", title)
		}
		if strings.ContainsFunc(title, invisibleInTitle) {
			t.Fatalf("%q was accepted with a control character", title)
		}
		rebuilt := got.Type
		if got.Scope != "" {
			rebuilt += fmt.Sprintf("(%s)", got.Scope)
		}
		if got.Breaking {
			rebuilt += "!"
		}
		if rebuilt += ": " + got.Description; rebuilt != title {
			t.Fatalf("%q reads back as %q", title, rebuilt)
		}
	})
}

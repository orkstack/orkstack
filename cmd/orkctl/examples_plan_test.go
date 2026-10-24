package main

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// planLineArgs tokenizes one documented plan line the way a POSIX shell does
// for the only quoting the examples use: words split on spaces outside single
// quotes, and single quotes stripped. Anything else — an unbalanced quote, a
// double quote outside single quotes — is an error, so a header line that a
// shell would read differently from this test can never pass as a CI check.
func planLineArgs(line string) ([]string, error) {
	var args []string
	var word strings.Builder
	inWord, quoted := false, false
	for _, r := range line {
		switch {
		case quoted && r == '\'':
			quoted = false
		case quoted:
			word.WriteRune(r)
		case r == '\'':
			quoted, inWord = true, true
		case r == '"':
			return nil, errors.New(`a double quote outside single quotes`)
		case r == ' ':
			if inWord {
				args = append(args, word.String())
				word.Reset()
				inWord = false
			}
		default:
			word.WriteRune(r)
			inWord = true
		}
	}
	if quoted {
		return nil, errors.New("an unbalanced single quote")
	}
	if inWord {
		args = append(args, word.String())
	}
	return args, nil
}

func TestPlanLineArgs(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		want    []string
		wantErr string
	}{
		{"inputs and asserts",
			"orkctl run plan --file examples/x.ork.yaml --input release=1.4.2 --assert-runs regional",
			[]string{"orkctl", "run", "plan", "--file", "examples/x.ork.yaml", "--input", "release=1.4.2", "--assert-runs", "regional"}, ""},
		{"a quoted JSON output",
			`orkctl run plan --file examples/x.ork.yaml --output decide='{"result":{"route":"canary","notify":true}}' --assert-skipped hold`,
			[]string{"orkctl", "run", "plan", "--file", "examples/x.ork.yaml", "--output", `decide={"result":{"route":"canary","notify":true}}`, "--assert-skipped", "hold"}, ""},
		{"an assumed failure",
			"orkctl run plan --file examples/x.ork.yaml --assume-failed shipped --assert-runs release",
			[]string{"orkctl", "run", "plan", "--file", "examples/x.ork.yaml", "--assume-failed", "shipped", "--assert-runs", "release"}, ""},
		{"a quoted value with spaces",
			"--output approval='{\"approved\":false, \"approver\":\"dana\"}'",
			[]string{"--output", `approval={"approved":false, "approver":"dana"}`}, ""},
		{"an unbalanced quote", "--output a='{", nil, "unbalanced"},
		{"a double quote", `--input note="x y"`, nil, "double quote"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := planLineArgs(tc.line)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("planLineArgs(%q) = %v, %v; want an error containing %q", tc.line, got, err, tc.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("planLineArgs(%q) = %q, %v; want %q", tc.line, got, err, tc.want)
			}
		})
	}
}

// examplePlanLines returns every plan line of one example's header: the
// leading comment block's lines of the form `#   orkctl run plan --file …`,
// with the comment marker stripped.
func examplePlanLines(t *testing.T, path string) []string {
	t.Helper()
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(source))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "#") {
			break // the header is the leading comment block
		}
		text := strings.TrimSpace(strings.TrimPrefix(line, "#"))
		if strings.HasPrefix(text, "orkctl run plan --file ") {
			lines = append(lines, text)
		}
	}
	return lines
}

// TestExamples_DocumentedPlansHold: every `orkctl run plan --file` line
// in an example's header is a CI check. Each runs through the real CLI
// against the real Connect service, exactly as a reader would type it, and
// must exit 0 — its --assert-runs and --assert-skipped hold.
func TestExamples_DocumentedPlansHold(t *testing.T) {
	f := newFixture(t)
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.ork.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v (%d found)", err, len(paths))
	}
	sort.Strings(paths)

	// Every example another example composes is applied first, as a reader
	// must (examples/README.md, "Composing examples").
	byName := map[string]string{}
	composed := map[string]bool{}
	for _, path := range paths {
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatalf("compile %s: %v", path, err)
		}
		byName[wf.GetName()] = path
		for _, task := range wf.GetTasks() {
			if ref := task.GetOrkflow(); ref != nil {
				composed[ref.GetName()] = true
			}
		}
	}
	var children []string
	for name := range composed {
		children = append(children, name)
	}
	sort.Strings(children)
	for _, name := range children {
		path, ok := byName[name]
		if !ok {
			t.Fatalf("an example composes %q, which no shipped example defines", name)
		}
		f.run("apply", path).requireExit(t, exitOK)
	}

	perExample := map[string]int{}
	for _, path := range paths {
		file := filepath.Base(path)
		for _, line := range examplePlanLines(t, path) {
			args, err := planLineArgs(line)
			if err != nil {
				t.Errorf("%s: plan line %q: %v", file, line, err)
				continue
			}
			args = args[1:] // drop "orkctl"
			for i, a := range args {
				if strings.HasPrefix(a, "examples/") {
					args[i] = filepath.Join("..", "..", a)
				}
			}
			out := f.run(args...)
			if out.code != exitOK {
				t.Errorf("%s: %s\nexit %d\nstdout:\n%s\nstderr:\n%s", file, line, out.code, out.stdout, out.stderr)
				continue
			}
			perExample[strings.TrimSuffix(file, ".ork.yaml")]++
		}
	}
	for _, name := range []string{"decide-and-route", "inputs-and-escaping", "order-saga", "approval-gate"} {
		if perExample[name] == 0 {
			t.Errorf("%s has no plan line that held; want at least one", name)
		}
	}
	t.Logf("plan lines held per example: %v", perExample)
}

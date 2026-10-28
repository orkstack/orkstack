package main

import (
	"reflect"
	"strings"
	"testing"
)

// A feature proof runs the race gate for the packages the change touches and
// no others (AGENTS.md §11.2): the unit gate of a feature proof runs every
// package, and the race gate is the longer run, so it follows the change.

func TestSelect_TheRaceGateRunsForTheChangedPackagesOnly(t *testing.T) {
	for _, c := range []struct {
		name    string
		changes []change
		want    []string
	}{
		{"one package", []change{{"internal/dsl/parse.go", "package dsl\n\nfunc Parse() int { return 2 }\n"}}, []string{"./internal/dsl"}},
		{"a test file only", []change{{"internal/dsl/parse_test.go", "package dsl\n\nimport \"testing\"\n\nfunc TestParse(t *testing.T) { _ = Parse(); _ = 2 }\n"}}, []string{"./internal/dsl"}},
		{"two packages, sorted", []change{
			{"internal/engine/run.go", "package engine\n\nfunc Run() int { return 2 }\n"},
			{"internal/dsl/parse.go", "package dsl\n\nfunc Parse() int { return 2 }\n"},
		}, []string{"./internal/dsl", "./internal/engine"}},
		{"a data file the package's tests read", []change{{"internal/dsl/testdata/a.yaml", "a: 2\n"}}, []string{"./internal/dsl"}},
		{"documentation", []change{{"docs/DSL.md", "# DSL, revised\n"}}, nil},
		{"the UI", []change{{"ui/src/lib/flakes.ts", "export const x = 2;\n"}}, nil},
		{"the activity images change no package", []change{{"images/activity-shell/entry.sh", "#!/bin/sh\necho hi\n"}}, nil},
		{"a Go file under testdata is no package of the run", []change{{"internal/dsl/testdata/gen/gen.go", "package gen\n"}}, nil},
		{"a deleted package has nothing to run", []change{{"cmd/orkctl/main.go", ""}, {"cmd/orkctl/cli_integration_test.go", ""}}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			sel := fakeSelect(t, c.changes, nil, nil)
			if !reflect.DeepEqual(sel.RacePackages, c.want) {
				t.Errorf("race packages = %v, want %v", sel.RacePackages, c.want)
			}
		})
	}
}

func TestWriteGates_NamesTheRacePackages(t *testing.T) {
	sel := &selection{Integration: map[string]*pkgSelection{}, RacePackages: []string{"./internal/dsl", "./internal/engine"}}
	var b strings.Builder
	writeGates(&b, sel)
	if !strings.Contains(b.String(), "race_packages=./internal/dsl ./internal/engine\n") {
		t.Errorf("got\n%s", b.String())
	}
	b.Reset()
	writeGates(&b, &selection{Integration: map[string]*pkgSelection{}})
	if !strings.Contains(b.String(), "race_packages=\n") {
		t.Errorf("an empty selection must still say race_packages=:\n%s", b.String())
	}
}

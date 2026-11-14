package dsl_test

import (
	"os"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// TestCompile_RefusesTopLevelKeysThatAreNotPlainStrings: a merge key, an
// alias used as a key, an anchored or a tagged top-level key is refused,
// naming it, so what an Orkflow says at its top level is what every reader
// of it reads (identity rev18; docs/DSL.md).
func TestCompile_RefusesTopLevelKeysThatAreNotPlainStrings(t *testing.T) {
	raw, err := os.ReadFile("testdata/orkflows/hello.ork.yaml")
	if err != nil {
		t.Fatal(err)
	}
	good := string(raw)
	if _, err := dsl.Compile(raw); err != nil {
		t.Fatalf("the fixture itself: %v", err)
	}
	for name, extra := range map[string]string{
		"merge key":    "<<: {namespace: payments}\n",
		"aliased key":  "labels: {k: &k namespace}\n*k : payments\n",
		"anchored key": "&a namespace: payments\n",
		"tagged key":   "!!str namespace: payments\n",
	} {
		_, err := dsl.Compile([]byte(good + extra))
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), "top-level key") {
			t.Errorf("%s: refused as %v; want the top-level key named", name, err)
		}
	}
}

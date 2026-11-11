package main

import (
	"encoding/json/v2"
	"os"
	"strings"
	"testing"
)

func TestTouches_GateFilesListAddedAndRemovedRows(t *testing.T) {
	const nightly = "scripts/proof-nightly-tests.txt"
	base := "# a comment\n" +
		"example.com/m\tTestSoak\tTestCover\t2026-10-18 Daniel Santos\tkept\n" +
		"example.com/m\tTestOld\tTestCover\t2026-10-18 Daniel Santos\tremoved\n"
	tree := "# a changed comment is not a row\n" +
		"example.com/m\tTestSoak\tTestCover\t2026-10-18 Daniel Santos\tkept\n" +
		"example.com/m\tTestNew\tTestCover\t2026-10-18 Daniel Santos\tadded\n"
	paths := []changedPath{
		{Path: nightly},
		{Path: "scripts/proof-quarantine.txt"},
		{Path: "internal/engine/a.go"},
		{Path: "docs/PROOFS.md"},
	}
	changes := gateFileChanges(paths, func(path string) (string, string) {
		switch path {
		case nightly:
			return base, tree
		case "scripts/proof-quarantine.txt":
			return "", "unit\texample.com/m\tTestX\tDaniel Santos <danlsgiga@gmail.com>\t2026-10-19\t2026-11-18\twhy\n"
		}
		return "", ""
	})
	if len(changes) != 2 {
		t.Fatalf("changes = %+v, want only the two gate files of the diff", changes)
	}
	if changes[0].File != nightly || changes[1].File != "scripts/proof-quarantine.txt" {
		t.Fatalf("changes = %+v", changes)
	}
	if got := strings.Join(changes[0].Added, "|"); !strings.Contains(got, "TestNew") || strings.Contains(got, "TestSoak") {
		t.Errorf("added = %v", changes[0].Added)
	}
	if got := strings.Join(changes[0].Removed, "|"); !strings.Contains(got, "TestOld") || strings.Contains(got, "TestSoak") {
		t.Errorf("removed = %v", changes[0].Removed)
	}
	if len(changes[1].Added) != 1 || !strings.Contains(changes[1].Added[0], "TestX") {
		t.Errorf("a new quarantine row was not reported: %+v", changes[1])
	}
}

// A failure to list the gate files is written as an error entry, never as
// an empty list a caller would read as "no gate file changed".
func TestTouches_AFailureIsAnErrorEntryNotAnEmptyList(t *testing.T) {
	quiet(t)
	var b strings.Builder
	prev := os.Stderr
	devnull, _ := os.Open(os.DevNull)
	os.Stderr = devnull
	code := touchesGateFiles(&b, "no-such-ref-for-this-test")
	os.Stderr = prev
	var got []listChange
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b.String())
	}
	if code != 1 || len(got) != 1 || !strings.Contains(got[0].Error, "is not a commit") {
		t.Errorf("exit %d, %+v", code, got)
	}
}

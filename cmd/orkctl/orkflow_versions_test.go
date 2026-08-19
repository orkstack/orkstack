package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// orkflow versions / promote / diff: golden human and --json output
// ---------------------------------------------------------------------------

// TestOrkflowVersionCommands_GoldenHumanAndJSON pins both renderings of the
// version surface. Human output and --json are two contracts: a person reads
// the first and a script parses the second, and either changing silently
// breaks somebody.
func TestOrkflowVersionCommands_GoldenHumanAndJSON(t *testing.T) {
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)

	// Nothing promoted yet: the listing says so, and says what runs instead.
	unpromoted := f.run("orkflow", "versions", "deploy").requireExit(t, exitOK)
	requireGolden(t, "orkflow-versions-unpromoted.golden", unpromoted.stdout)

	promoted := f.run("orkflow", "promote", "deploy", "1.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-promote.golden", promoted.stdout)

	listed := f.run("orkflow", "versions", "deploy").requireExit(t, exitOK)
	requireGolden(t, "orkflow-versions.golden", listed.stdout)

	rolled := f.run("orkflow", "promote", "deploy", "2.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-promote-rollback.golden", rolled.stdout)

	diffed := f.run("orkflow", "diff", "deploy", "1.0.0", "2.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-diff.golden", diffed.stdout)

	same := f.run("orkflow", "diff", "deploy", "1.0.0", "1.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-diff-identical.golden", same.stdout)

	jsonFixture := newFixture(t)
	jsonFixture.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	jsonFixture.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)

	promotedJSON := jsonFixture.run("--json", "orkflow", "promote", "deploy", "1.0.0").
		requireExit(t, exitOK)
	requireGolden(t, "orkflow-promote.json.golden", promotedJSON.stdout)

	listedJSON := jsonFixture.run("--json", "orkflow", "versions", "deploy").requireExit(t, exitOK)
	requireGolden(t, "orkflow-versions.json.golden", listedJSON.stdout)

	diffedJSON := jsonFixture.run("--json", "orkflow", "diff", "deploy", "1.0.0", "2.0.0").
		requireExit(t, exitOK)
	requireGolden(t, "orkflow-diff.json.golden", diffedJSON.stdout)

	// --json is machine output: it parses, and it carries the fields a script
	// needs rather than the sentence a person reads.
	var decoded struct {
		Versions []struct {
			Version   string `json:"version"`
			IsCurrent bool   `json:"isCurrent"`
		} `json:"versions"`
		CurrentVersion string `json:"currentVersion"`
		HeadVersion    string `json:"headVersion"`
	}
	if err := json.Unmarshal([]byte(listedJSON.stdout), &decoded); err != nil {
		t.Fatalf("--json output does not parse: %v\n%s", err, listedJSON.stdout)
	}
	if decoded.CurrentVersion != "1.0.0" || decoded.HeadVersion != "2.0.0" {
		t.Errorf("--json reports current %q head %q, want 1.0.0 and 2.0.0",
			decoded.CurrentVersion, decoded.HeadVersion)
	}
	if len(decoded.Versions) != 2 || decoded.Versions[0].Version != "2.0.0" {
		t.Fatalf("--json versions = %+v, want 2.0.0 first", decoded.Versions)
	}
	if decoded.Versions[0].IsCurrent || !decoded.Versions[1].IsCurrent {
		t.Errorf("--json marks the wrong version current: %+v", decoded.Versions)
	}
}

// ---------------------------------------------------------------------------
// behavior
// ---------------------------------------------------------------------------

// TestOrkflowPromote_MovesWhatRunsAndBack is the promote-then-rollback
// acceptance through the CLI: the listing, the catalog and a submitted run all
// follow the pointer, in both directions.
func TestOrkflowPromote_MovesWhatRunsAndBack(t *testing.T) {
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)

	// Before any promotion the newest applied version runs.
	f.run("run", "submit", "deploy").requireExit(t, exitOK).requireContains(t, "2.0.0")

	f.run("orkflow", "promote", "deploy", "1.0.0").requireExit(t, exitOK)
	f.run("run", "submit", "deploy").requireExit(t, exitOK).requireContains(t, "1.0.0")
	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "1.0.0")

	// Rolling back is the same command, and it says how to undo itself.
	rollback := f.run("orkflow", "promote", "deploy", "2.0.0").requireExit(t, exitOK)
	rollback.requireContains(t, "Previous: 1.0.0", "orkctl orkflow promote deploy 1.0.0")
	f.run("run", "submit", "deploy").requireExit(t, exitOK).requireContains(t, "2.0.0")

	// A version nobody applied is refused, and the pointer does not move.
	refused := f.run("orkflow", "promote", "deploy", "9.9.9")
	if refused.code == exitOK {
		t.Fatalf("promoting an unknown version succeeded:\n%s", refused.stdout)
	}
	if !strings.Contains(refused.stderr, "not_found") {
		t.Errorf("the refusal does not name the reason:\n%s", refused.stderr)
	}
	f.run("run", "submit", "deploy").requireExit(t, exitOK).requireContains(t, "2.0.0")
}

// TestOrkflowVersions_AreScopedByNamespace: -n reaches the version surface, and
// promoting in one namespace decides nothing in another.
func TestOrkflowVersions_AreScopedByNamespace(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "alpha").requireExit(t, exitOK)
	f.run("namespace", "create", "bravo").requireExit(t, exitOK)
	for _, namespace := range []string{"alpha", "bravo"} {
		for _, version := range []string{"1.0.0", "2.0.0"} {
			f.run("apply", writeVersionedOrkflow(t, "deploy", version, namespace+"-"+version),
				"-n", namespace).requireExit(t, exitOK)
		}
	}

	f.run("orkflow", "promote", "deploy", "1.0.0", "-n", "alpha").requireExit(t, exitOK)

	f.run("orkflow", "versions", "deploy", "-n", "alpha").
		requireExit(t, exitOK).
		requireContains(t, "Current: 1.0.0")
	f.run("orkflow", "versions", "deploy", "-n", "bravo").
		requireExit(t, exitOK).
		requireContains(t, "No version has been promoted")

	// The diff answers from the namespace it was asked in.
	f.run("orkflow", "diff", "deploy", "1.0.0", "2.0.0", "-n", "bravo").
		requireExit(t, exitOK).
		requireContains(t, "bravo-2.0.0")

	// The default namespace holds neither.
	if outcome := f.run("orkflow", "versions", "deploy"); outcome.code == exitOK {
		t.Fatalf("the default namespace answered for another namespace's orkflow:\n%s",
			outcome.stdout)
	}
}

// TestOrkflowEdit_RoundTripsThroughTheEditorAndAppliesANewVersion is the
// $EDITOR acceptance. The "editor" is a real program this test writes, because
// the round trip IS the feature: fetch the source, hand a file to a process,
// apply what that process saved.
func TestOrkflowEdit_RoundTripsThroughTheEditorAndAppliesANewVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)

	// An editor that rewrites the command and bumps the version, the way a
	// person would.
	editor := stubEditor(t, `sed -i.bak -e 's/echo one/echo edited/' -e 's/1\.0\.0/1\.1\.0/' "$1"`)

	edited := f.run("orkflow", "edit", "deploy", "--editor", editor).requireExit(t, exitOK)
	edited.requireContains(t, "Applied orkflow deploy", "1.1.0")

	// The edit became a NEW version; the old one is untouched.
	versions := f.run("orkflow", "versions", "deploy").requireExit(t, exitOK)
	versions.requireContains(t, "1.0.0", "1.1.0")
	f.run("orkflow", "get", "deploy", "--version", "1.0.0").
		requireExit(t, exitOK).
		requireContains(t, "1.0.0")
	f.run("orkflow", "diff", "deploy", "1.0.0", "1.1.0").
		requireExit(t, exitOK).
		requireContains(t, "-      command: echo one", "+      command: echo edited")

	// An editor that changes nothing applies nothing.
	unchanged := f.run("orkflow", "edit", "deploy", "--editor", stubEditor(t, `true`)).
		requireExit(t, exitOK)
	unchanged.requireContains(t, "No change to deploy@1.1.0")
	after := f.run("--json", "orkflow", "versions", "deploy").requireExit(t, exitOK)
	if count := strings.Count(after.stdout, `"version"`); count != 2 {
		t.Fatalf("an editor that changed nothing created a version:\n%s", after.stdout)
	}
}

// TestOrkflowEdit_ReportsAnEditMadeFromAStaleVersion is acceptance 5 through
// the CLI, and it is a real interleaving rather than a description of one: the
// stub editor blocks until this test has applied a competing version, so the
// edit is genuinely written against a version that stopped being the head
// while the editor was open.
func TestOrkflowEdit_ReportsAnEditMadeFromAStaleVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)

	gate := filepath.Join(t.TempDir(), "gate")
	opened := filepath.Join(t.TempDir(), "opened")
	editor := stubEditor(t, fmt.Sprintf(
		`touch %q; while [ ! -f %q ]; do sleep 0.02; done; sed -i.bak 's/echo one/echo mine/' "$1"`,
		opened, gate))

	done := make(chan result, 1)
	go func() { done <- f.run("orkflow", "edit", "deploy", "--editor", editor) }()
	awaitFile(t, opened, "the stub editor never opened the document")

	// Somebody else applies while the editor is open. This is the whole race.
	f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatalf("release the editor: %v", err)
	}

	outcome := <-done
	if outcome.code == exitOK {
		t.Fatalf("a stale edit was applied:\n%s", outcome.stdout)
	}
	for _, want := range []string{"failed_precondition", "1.0.0", "2.0.0", "orkctl orkflow diff"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, outcome.stderr)
		}
	}
	// The user's work survives the refusal.
	kept := keptEditPath(t, outcome.stderr)
	body, err := os.ReadFile(kept)
	if err != nil {
		t.Fatalf("the refused edit was not kept: %v", err)
	}
	t.Cleanup(func() { os.Remove(kept) })
	if !strings.Contains(string(body), "echo mine") {
		t.Errorf("the kept file is not the user's edit:\n%s", body)
	}

	// Nothing was stored for the refused edit: the versions are the two that
	// were really applied.
	versions := f.run("--json", "orkflow", "versions", "deploy").requireExit(t, exitOK)
	if strings.Contains(versions.stdout, "echo mine") {
		t.Errorf("a refused edit reached the store:\n%s", versions.stdout)
	}
	if count := strings.Count(versions.stdout, `"version"`); count != 2 {
		t.Errorf("want exactly the two applied versions:\n%s", versions.stdout)
	}
}

// awaitFile blocks until path exists, so a test can meet a subprocess halfway
// without sleeping for a guessed interval.
func awaitFile(t *testing.T, path, message string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s (%s never appeared)", message, path)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// writeVersionedOrkflow writes a one-task orkflow whose body carries a marker,
// so two versions differ by what they would run and not only by their number.
func writeVersionedOrkflow(t *testing.T, name, version, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+"-"+version+".ork.yaml")
	document := fmt.Sprintf(`name: %s
version: "%s"
description: the %s version
tasks:
  - id: build
    plugin: shell
    with:
      op: exec
      command: echo %s
`, name, version, version, marker)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write orkflow %s@%s: %v", name, version, err)
	}
	return path
}

// stubEditor writes a shell script that edits the file it is handed, and
// returns its path. It stands where $EDITOR stands: orkctl execs it with one
// argument and reads back what it saved.
func stubEditor(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	body := "#!/bin/sh\nset -e\n" + script + "\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub editor: %v", err)
	}
	return path
}

// keptEditPath reads the path orkctl reported for a refused edit.
func keptEditPath(t *testing.T, stderr string) string {
	t.Helper()
	const marker = "your edit was kept at "
	index := strings.Index(stderr, marker)
	if index < 0 {
		t.Fatalf("orkctl did not say where it kept the refused edit:\n%s", stderr)
	}
	return strings.TrimSpace(strings.SplitN(stderr[index+len(marker):], "\n", 2)[0])
}

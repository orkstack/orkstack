package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// `orkctl orkflow edit --version <v>`: editing a version that is not the head.
//
// The two halves of this file are one feature: the CLI can start an edit from
// any stored version, and the apply that follows still loses to a competing
// apply — it is the HEAD the editor saw that decides the race, not the version
// it was reading.

// TestOrkflowEdit_VersionScopedEditForksFromTheChosenVersion is the owner's
// scenario through the CLI: two versions, the OLDER one promoted, then an edit
// of that promoted (current, non-head) version applied as a new version.
//
// Human and --json are two contracts — a person reads the first, a script
// parses the second — so both are golden.
func TestOrkflowEdit_VersionScopedEditForksFromTheChosenVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)
	// Promote the OLDER version: head stays 2.0.0, current moves to 1.0.0.
	f.run("orkflow", "promote", "deploy", "1.0.0").requireExit(t, exitOK)

	// The editor is handed 1.0.0's source, not the head's. The assertion is
	// the stub's own: it fails unless the file it was given says `echo one`.
	editor := stubEditor(t, `grep -q 'echo one' "$1" || { echo "not 1.0.0's source" >&2; exit 1; }
sed -i.bak -e 's/echo one/echo forked/' -e 's/1\.0\.0/1\.1\.0/' "$1"`)

	edited := f.run("orkflow", "edit", "deploy", "--version", "1.0.0", "--editor", editor).
		requireExit(t, exitOK)
	requireGolden(t, "orkflow-edit-version.golden", edited.stdout)

	// The store, not the client's claim: three versions, the fork is the
	// newest applied one, and the pointer never moved.
	listed := f.run("orkflow", "versions", "deploy").requireExit(t, exitOK)
	listed.requireContains(t, "1.0.0", "2.0.0", "1.1.0", "Current: 1.0.0   Newest applied: 1.1.0")
	f.run("--json", "orkflow", "get", "deploy", "--version", "1.1.0").
		requireExit(t, exitOK).
		requireContains(t, "echo forked")
	// The fork carries the BASE's body: it is 1.0.0 that was edited, so the
	// head's `echo two` is nowhere in it.
	f.run("orkflow", "diff", "deploy", "1.0.0", "1.1.0").
		requireExit(t, exitOK).
		requireContains(t, "-      command: echo one", "+      command: echo forked")
	// And what an unpinned run executes is still the promoted version.
	f.run("run", "submit", "deploy").requireExit(t, exitOK).requireContains(t, "1.0.0")

	// Saving without changing anything applies nothing, and names the version
	// that was open rather than the head.
	f.run("orkflow", "edit", "deploy", "--version", "2.0.0", "--editor", stubEditor(t, `true`)).
		requireExit(t, exitOK).
		requireContains(t, "No change to deploy@2.0.0")

	// ---- the same round trip as machine output --------------------------
	machine := newFixture(t)
	machine.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	machine.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)
	machine.run("orkflow", "promote", "deploy", "1.0.0").requireExit(t, exitOK)

	editedJSON := machine.run("--json", "orkflow", "edit", "deploy",
		"--version", "1.0.0", "--editor", editor).requireExit(t, exitOK)
	requireGolden(t, "orkflow-edit-version.json.golden", editedJSON.stdout)

	var decoded struct {
		Orkflow struct {
			Version    string `json:"version"`
			SourceYaml string `json:"sourceYaml"`
		} `json:"orkflow"`
		Created bool `json:"created"`
	}
	if err := json.Unmarshal([]byte(editedJSON.stdout), &decoded); err != nil {
		t.Fatalf("--json output does not parse: %v\n%s", err, editedJSON.stdout)
	}
	if decoded.Orkflow.Version != "1.1.0" || !decoded.Created {
		t.Errorf("--json reports version %q created=%v, want 1.1.0 and true",
			decoded.Orkflow.Version, decoded.Created)
	}
	if !strings.Contains(decoded.Orkflow.SourceYaml, "echo forked") {
		t.Errorf("--json does not carry the edited source:\n%s", decoded.Orkflow.SourceYaml)
	}
	versionsJSON := machine.run("--json", "orkflow", "versions", "deploy").requireExit(t, exitOK)
	var listing struct {
		CurrentVersion string `json:"currentVersion"`
		HeadVersion    string `json:"headVersion"`
	}
	if err := json.Unmarshal([]byte(versionsJSON.stdout), &listing); err != nil {
		t.Fatalf("--json versions do not parse: %v\n%s", err, versionsJSON.stdout)
	}
	if listing.CurrentVersion != "1.0.0" || listing.HeadVersion != "1.1.0" {
		t.Errorf("after the fork: current %q head %q, want 1.0.0 and 1.1.0",
			listing.CurrentVersion, listing.HeadVersion)
	}
}

// TestOrkflowEdit_RefusesAVersionThatWasNeverApplied: `--version` is checked
// before the editor opens, so a typo costs a message rather than an editing
// session. The stub records that it ran; the test fails if it did.
func TestOrkflowEdit_RefusesAVersionThatWasNeverApplied(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)

	ran := filepath.Join(t.TempDir(), "editor-ran")
	editor := stubEditor(t, fmt.Sprintf(`touch %q`, ran))

	outcome := f.run("orkflow", "edit", "deploy", "--version", "9.9.9", "--editor", editor)
	if outcome.code == exitOK {
		t.Fatalf("editing a version nobody applied succeeded:\n%s", outcome.stdout)
	}
	requireGolden(t, "orkflow-edit-unknown-version.golden", outcome.stderr)
	for _, want := range []string{"get orkflow", "not_found", "9.9.9"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, outcome.stderr)
		}
	}
	if _, err := os.Stat(ran); err == nil {
		t.Error("the editor was opened on a version that does not exist")
	}
}

// TestOrkflowEdit_RefusesWhenTheHeadMovedUnderAVersionScopedEdit is the race
// through the CLI, and it is a real interleaving rather than a description of
// one: the stub editor blocks until this test has applied a competing version,
// so the edit is genuinely written while the head moves.
//
// The base is deliberately NOT the head — the user asked for 1.0.0. Choosing
// an old base is a fork, not an exemption: the head the editor saw (2.0.0) is
// gone, so the apply
// is refused naming it and the head that replaced it.
func TestOrkflowEdit_RefusesWhenTheHeadMovedUnderAVersionScopedEdit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	for _, mode := range []struct {
		name   string
		args   []string
		golden string
	}{
		{"human", nil, "orkflow-edit-conflict.golden"},
		{"json", []string{"--json"}, "orkflow-edit-conflict.json.golden"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f := newFixture(t)
			f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
			f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)

			gate := filepath.Join(t.TempDir(), "gate")
			opened := filepath.Join(t.TempDir(), "opened")
			editor := stubEditor(t, fmt.Sprintf(
				`touch %q; while [ ! -f %q ]; do sleep 0.02; done; `+
					`sed -i.bak -e 's/echo one/echo mine/' -e 's/1\.0\.0/1\.5\.0/' "$1"`,
				opened, gate))

			args := append(append([]string(nil), mode.args...),
				"orkflow", "edit", "deploy", "--version", "1.0.0", "--editor", editor)
			done := make(chan result, 1)
			go func() { done <- f.run(args...) }()
			awaitFile(t, opened, "the stub editor never opened the document")

			// Somebody else applies while the editor is open. This is the
			// whole race, and it is the HEAD that moves — the version being
			// edited is untouched.
			f.run("apply", writeVersionedOrkflow(t, "deploy", "3.0.0", "three")).
				requireExit(t, exitOK)
			if err := os.WriteFile(gate, nil, 0o644); err != nil {
				t.Fatalf("release the editor: %v", err)
			}

			outcome := <-done
			if outcome.code == exitOK {
				t.Fatalf("an edit applied over a head that moved:\n%s", outcome.stdout)
			}
			// The refusal is stderr in both renderings: --json is the shape of
			// an ANSWER, and a refusal has no answer to shape. Both goldens
			// exist so that changing one of them is a decision, not a
			// side effect.
			requireGolden(t, mode.golden, redactEditPaths(t, outcome.stderr))

			// The user's work survives the refusal, exactly as it does on the
			// head path: the file the refusal names holds the edit.
			kept := keptEditPath(t, outcome.stderr)
			body, err := os.ReadFile(kept)
			if err != nil {
				t.Fatalf("the refused edit was not kept: %v", err)
			}
			t.Cleanup(func() { os.Remove(kept) })
			if !strings.Contains(string(body), "echo mine") {
				t.Errorf("the kept file is not the user's edit:\n%s", body)
			}

			// Nothing was stored: the versions are the three that were really
			// applied, and the fork the user attempted is not among them.
			versions := f.run("--json", "orkflow", "versions", "deploy").requireExit(t, exitOK)
			if strings.Contains(versions.stdout, "1.5.0") {
				t.Errorf("a refused edit reached the store:\n%s", versions.stdout)
			}
			if count := strings.Count(versions.stdout, `"version"`); count != 3 {
				t.Errorf("want exactly the three applied versions:\n%s", versions.stdout)
			}
		})
	}
}

// TestOrkflowEdit_HeadPathKeepsSendingTheHeadAsItsOwnBase pins what did NOT
// change: with no --version the command still edits the newest applied version,
// and its apply is still refused when a competing one lands. `--version` is an
// addition, not a change of default.
func TestOrkflowEdit_HeadPathKeepsSendingTheHeadAsItsOwnBase(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stub editor is a shell script")
	}
	f := newFixture(t)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	f.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)
	f.run("orkflow", "promote", "deploy", "1.0.0").requireExit(t, exitOK)

	// No --version: the head (2.0.0) is what opens, even though 1.0.0 is what
	// runs. Editing follows lineage; promotion decides execution.
	editor := stubEditor(t, `grep -q 'echo two' "$1" || { echo "not the head's source" >&2; exit 1; }
sed -i.bak -e 's/echo two/echo head-edit/' -e 's/2\.0\.0/2\.1\.0/' "$1"`)
	edited := f.run("orkflow", "edit", "deploy", "--editor", editor).requireExit(t, exitOK)
	edited.requireContains(t, "Applied orkflow deploy", "2.1.0")
	// The base WAS the head, so there is no fork to report.
	if strings.Contains(edited.stdout, "Forked from") {
		t.Errorf("an edit of the head reported a fork:\n%s", edited.stdout)
	}
	f.run("orkflow", "versions", "deploy").
		requireExit(t, exitOK).
		requireContains(t, "Current: 1.0.0   Newest applied: 2.1.0")
}

// redactEditPaths replaces the two temporary paths a refused edit prints with
// fixed tokens: the editor orkctl exec'd, and the file it kept the edit in.
//
// Both are fresh temporary paths on every run — being unique is the point of
// the second one — so they are the parts of the session that cannot be golden.
// What they carry is asserted separately: the kept file is opened and the
// user's edit read out of it, and the editor line's VERSION (not its path) is
// what the golden pins.
func redactEditPaths(t *testing.T, stderr string) string {
	t.Helper()
	lines := strings.Split(stderr, "\n")
	kept := false
	for i, line := range lines {
		if prefix, _, found := strings.Cut(line, " in /"); found &&
			strings.HasPrefix(line, "editing ") {
			lines[i] = prefix + " in <editor>"
		}
		const marker = "your edit was kept at "
		if strings.HasPrefix(line, marker) {
			lines[i] = marker + "<kept-edit-path>"
			kept = true
		}
	}
	if !kept {
		t.Fatalf("orkctl did not say where it kept the refused edit:\n%s", stderr)
	}
	return strings.Join(lines, "\n")
}

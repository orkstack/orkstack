package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// namespace: golden human and --json output
// ---------------------------------------------------------------------------

// TestNamespaceCommands_GoldenHumanAndJSON pins both renderings of every
// namespace command.
//
// Human output and --json are two contracts, not one plus a debug dump: a
// person reads the first and a script parses the second, and either changing
// silently breaks somebody. The commands run in one sequence so the goldens
// describe a real session rather than five unrelated fixtures.
func TestNamespaceCommands_GoldenHumanAndJSON(t *testing.T) {
	f := newFixture(t)

	created := f.run("namespace", "create", "payments",
		"--description", "the payments tenant",
		"--default-pool", "batch",
		"--allow-pool", "batch",
		"--allow-pool", "main",
		"--label", "team=money",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-create.golden", created.stdout)

	// The same create again: idempotent, and it says so.
	repeat := f.run("namespace", "create", "payments",
		"--description", "the payments tenant",
		"--default-pool", "batch",
		"--allow-pool", "batch",
		"--allow-pool", "main",
		"--label", "team=money",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-create-repeat.golden", repeat.stdout)

	listed := f.run("namespace", "list").requireExit(t, exitOK)
	requireGolden(t, "namespace-list.golden", listed.stdout)

	got := f.run("namespace", "get", "payments").requireExit(t, exitOK)
	requireGolden(t, "namespace-get.golden", got.stdout)

	updated := f.run("namespace", "update", "payments",
		"--description", "the payments tenant, renamed",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-update.golden", updated.stdout)

	f.run("namespace", "create", "scratch").requireExit(t, exitOK)
	deleted := f.run("namespace", "delete", "scratch").requireExit(t, exitOK)
	requireGolden(t, "namespace-delete.golden", deleted.stdout)

	// --json for each of the five.
	jsonFixture := newFixture(t)
	createdJSON := jsonFixture.run("--json", "namespace", "create", "payments",
		"--description", "the payments tenant",
		"--default-pool", "batch",
		"--allow-pool", "batch",
		"--allow-pool", "main",
		"--label", "team=money",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-create.json.golden", createdJSON.stdout)

	listedJSON := jsonFixture.run("--json", "namespace", "list").requireExit(t, exitOK)
	requireGolden(t, "namespace-list.json.golden", listedJSON.stdout)

	gotJSON := jsonFixture.run("--json", "namespace", "get", "payments").requireExit(t, exitOK)
	requireGolden(t, "namespace-get.json.golden", gotJSON.stdout)

	updatedJSON := jsonFixture.run("--json", "namespace", "update", "payments",
		"--description", "the payments tenant, renamed",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-update.json.golden", updatedJSON.stdout)

	jsonFixture.run("--json", "namespace", "create", "scratch").requireExit(t, exitOK)
	deletedJSON := jsonFixture.run("--json", "namespace", "delete", "scratch").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-delete.json.golden", deletedJSON.stdout)
}

// ---------------------------------------------------------------------------
// namespace: behavior
// ---------------------------------------------------------------------------

// TestNamespaceFlag_ScopesApplyListGetAndSubmit is the CLI half of the
// namespace contract: -n reaches every command that names an orkflow or a
// run, and two namespaces holding the same name never see each other.
func TestNamespaceFlag_ScopesApplyListGetAndSubmit(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "alpha").requireExit(t, exitOK)
	f.run("namespace", "create", "bravo").requireExit(t, exitOK)

	alpha := writeOrkflow(t, "deploy", "build")
	bravo := writeOrkflow(t, "deploy", "ship")
	f.run("apply", alpha, "-n", "alpha").requireExit(t, exitOK)
	f.run("apply", bravo, "-n", "bravo").requireExit(t, exitOK)

	f.run("orkflow", "get", "deploy", "-n", "alpha").
		requireExit(t, exitOK).
		requireContains(t, "build")
	f.run("orkflow", "get", "deploy", "-n", "bravo").
		requireExit(t, exitOK).
		requireContains(t, "ship")

	// The default namespace holds neither.
	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "No orkflows.")

	f.run("run", "submit", "deploy", "-n", "alpha").
		requireExit(t, exitOK).
		requireContains(t, "run-001")
	f.run("run", "submit", "deploy", "-n", "bravo").
		requireExit(t, exitOK).
		requireContains(t, "run-002")

	alphaRuns := f.run("run", "list", "-n", "alpha").requireExit(t, exitOK)
	if !strings.Contains(alphaRuns.stdout, "run-001") ||
		strings.Contains(alphaRuns.stdout, "run-002") {
		t.Fatalf("run list -n alpha leaked bravo's run:\n%s", alphaRuns.stdout)
	}
	bravoRuns := f.run("run", "list", "-n", "bravo").requireExit(t, exitOK)
	if !strings.Contains(bravoRuns.stdout, "run-002") ||
		strings.Contains(bravoRuns.stdout, "run-001") {
		t.Fatalf("run list -n bravo leaked alpha's run:\n%s", bravoRuns.stdout)
	}
	defaultRuns := f.run("run", "list").requireExit(t, exitOK)
	if strings.Contains(defaultRuns.stdout, "run-00") {
		t.Fatalf("run list in the default namespace showed another namespace's runs:\n%s",
			defaultRuns.stdout)
	}
}

// TestNamespaceUse_PersistsTheDefaultAndTheFlagStillWins is the "not retyping
// it" half, and its limit.
func TestNamespaceUse_PersistsTheDefaultAndTheFlagStillWins(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("namespace", "create", "staging").requireExit(t, exitOK)

	f.run("namespace", "use", "payments").
		requireExit(t, exitOK).
		requireContains(t, "payments")

	// The file is real, and holds what was selected.
	raw, err := os.ReadFile(f.configPath)
	if err != nil {
		t.Fatalf("read the saved config: %v", err)
	}
	var saved struct {
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatalf("parse the saved config %q: %v", raw, err)
	}
	if saved.Namespace != "payments" {
		t.Fatalf("saved namespace = %q, want payments", saved.Namespace)
	}

	// A later command with no -n acts in it.
	f.run("apply", writeOrkflow(t, "deploy", "build")).requireExit(t, exitOK)
	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "deploy")
	f.run("orkflow", "get", "deploy", "-n", "payments").requireExit(t, exitOK)

	// And -n still overrides it.
	f.run("orkflow", "list", "-n", "staging").
		requireExit(t, exitOK).
		requireContains(t, "No orkflows.")

	// So does the environment, over the file but under the flag.
	t.Setenv(namespaceEnvVar, "staging")
	f.run("orkflow", "list").requireExit(t, exitOK).requireContains(t, "No orkflows.")
	f.run("orkflow", "list", "-n", "payments").requireExit(t, exitOK).requireContains(t, "deploy")
}

// TestNamespaceUse_RefusesANamespaceTheEngineDoesNotHave keeps a typo out of
// the config file, where it would misroute every later command from a place
// the user has stopped looking at.
func TestNamespaceUse_RefusesANamespaceTheEngineDoesNotHave(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("namespace", "use", "paymnets")
	if outcome.code == exitOK {
		t.Fatalf("saving an unknown namespace succeeded:\n%s", outcome.stdout)
	}
	if _, err := os.Stat(f.configPath); !os.IsNotExist(err) {
		t.Fatalf("a refused `namespace use` wrote %s", f.configPath)
	}
}

// TestSubmitIntoAMissingNamespace_NamesItAndListsTheOnesThatExist measures the
// refusal at the surface a person actually reads.
func TestSubmitIntoAMissingNamespace_NamesItAndListsTheOnesThatExist(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("namespace", "create", "staging").requireExit(t, exitOK)

	outcome := f.run("run", "submit", "deploy", "-n", "paymnets")
	if outcome.code == exitOK {
		t.Fatalf("submitting into a missing namespace succeeded:\n%s", outcome.stdout)
	}
	for _, want := range []string{"paymnets", "default", "payments", "staging"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("stderr does not name %q; the mistake and the alternatives are "+
				"what make it fixable:\n%s", want, outcome.stderr)
		}
	}
}

// TestNamespaceDelete_RefusesWhileItHoldsOrkflowsAndNamesThem.
func TestNamespaceDelete_RefusesWhileItHoldsOrkflowsAndNamesThem(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("apply", writeOrkflow(t, "nightly-close", "settle"), "-n", "payments").
		requireExit(t, exitOK)

	outcome := f.run("namespace", "delete", "payments")
	if outcome.code == exitOK {
		t.Fatalf("deleting a namespace holding an orkflow succeeded:\n%s", outcome.stdout)
	}
	for _, want := range []string{"payments", "nightly-close"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("stderr does not name %q:\n%s", want, outcome.stderr)
		}
	}
	f.run("namespace", "get", "payments").requireExit(t, exitOK)
}

// TestNamespaceDelete_RefusesTheDefault.
func TestNamespaceDelete_RefusesTheDefault(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("namespace", "delete", "default")
	if outcome.code == exitOK {
		t.Fatalf("deleting the default namespace succeeded:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "default") {
		t.Errorf("stderr does not name the namespace:\n%s", outcome.stderr)
	}
}

// TestApply_RefusesASelectorThatContradictsTheDocument: the file and the flag
// have to agree, and the CLI passes both through so the engine can say so.
func TestApply_RefusesASelectorThatContradictsTheDocument(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("namespace", "create", "staging").requireExit(t, exitOK)

	path := filepath.Join(t.TempDir(), "deploy.ork.yaml")
	document := "namespace: payments\nname: deploy\nversion: 1.0.0\ntasks:\n" +
		"  - id: build\n    uses: shell\n    with: {command: \"echo build\"}\n"
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write orkflow: %v", err)
	}

	outcome := f.run("apply", path, "-n", "staging")
	if outcome.code == exitOK {
		t.Fatalf("applying a payments document into staging succeeded:\n%s", outcome.stdout)
	}
	for _, want := range []string{"payments", "staging"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("stderr does not name %q:\n%s", want, outcome.stderr)
		}
	}
}

// writeOrkflow writes a one-task orkflow document and returns its path. The
// task id varies so two namespaces' documents are distinguishable by their
// contents, not only by where they were stored.
func writeOrkflow(t *testing.T, name, taskID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".ork.yaml")
	document := "name: " + name + "\nversion: 1.0.0\ntasks:\n" +
		"  - id: " + taskID + "\n    uses: shell\n    with: {command: \"echo hi\"}\n"
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write orkflow %s: %v", name, err)
	}
	return path
}

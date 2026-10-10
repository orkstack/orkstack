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

	// Admission: a hold with its reason, the document read back, the release,
	// and a cap. The since is the fixture's held clock.
	held := f.run("namespace", "update", "payments",
		"--hold", "partner CI loop, INC-4412",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-hold.golden", held.stdout)
	gotHeld := f.run("namespace", "get", "payments").requireExit(t, exitOK)
	requireGolden(t, "namespace-get-held.golden", gotHeld.stdout)
	listedHeld := f.run("namespace", "list").requireExit(t, exitOK)
	requireGolden(t, "namespace-list-held.golden", listedHeld.stdout)
	released := f.run("namespace", "update", "payments", "--release-hold").requireExit(t, exitOK)
	requireGolden(t, "namespace-release-hold.golden", released.stdout)
	capped := f.run("namespace", "update", "payments", "--max-running", "20").requireExit(t, exitOK)
	requireGolden(t, "namespace-max-running.golden", capped.stdout)

	// Secret policy: set through update, read back, cleared; and set through
	// create, the other command sharing namespaceSpecFlags.
	allowed := f.run("namespace", "update", "payments",
		"--allow-secret", "env:PAYMENTS_*",
		"--allow-secret", "file:/etc/orkstack/secrets/payments/*",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-allow-secret.golden", allowed.stdout)
	gotAllowed := f.run("namespace", "get", "payments").requireExit(t, exitOK)
	requireGolden(t, "namespace-get-allowed-secrets.golden", gotAllowed.stdout)
	cleared := f.run("namespace", "update", "payments", "--clear-allowed-secrets").requireExit(t, exitOK)
	requireGolden(t, "namespace-clear-allowed-secrets.golden", cleared.stdout)
	sandbox := f.run("namespace", "create", "sandbox", "--allow-secret", "env:SANDBOX_*").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-create-allow-secret.golden", sandbox.stdout)
	listedSecrets := f.run("namespace", "list").requireExit(t, exitOK)
	requireGolden(t, "namespace-list-allowed-secrets.golden", listedSecrets.stdout)

	// Budget: set through update, enforced; then cleared.
	budgeted := f.run("namespace", "update", "payments",
		"--budget-cpu-seconds", "5000000", "--budget-period", "720h", "--budget-enforce",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-budget.golden", budgeted.stdout)
	clearedBudget := f.run("namespace", "update", "payments", "--clear-budget").requireExit(t, exitOK)
	requireGolden(t, "namespace-clear-budget.golden", clearedBudget.stdout)

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

	heldJSON := jsonFixture.run("--json", "namespace", "update", "payments",
		"--hold", "partner CI loop, INC-4412",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-hold.json.golden", heldJSON.stdout)
	releasedJSON := jsonFixture.run("--json", "namespace", "update", "payments", "--release-hold").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-release-hold.json.golden", releasedJSON.stdout)
	cappedJSON := jsonFixture.run("--json", "namespace", "update", "payments", "--max-running", "20").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-max-running.json.golden", cappedJSON.stdout)

	allowedJSON := jsonFixture.run("--json", "namespace", "update", "payments",
		"--allow-secret", "env:PAYMENTS_*",
		"--allow-secret", "file:/etc/orkstack/secrets/payments/*",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-allow-secret.json.golden", allowedJSON.stdout)
	gotAllowedJSON := jsonFixture.run("--json", "namespace", "get", "payments").requireExit(t, exitOK)
	requireGolden(t, "namespace-get-allowed-secrets.json.golden", gotAllowedJSON.stdout)
	clearedJSON := jsonFixture.run("--json", "namespace", "update", "payments", "--clear-allowed-secrets").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-clear-allowed-secrets.json.golden", clearedJSON.stdout)
	sandboxJSON := jsonFixture.run("--json", "namespace", "create", "sandbox", "--allow-secret", "env:SANDBOX_*").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-create-allow-secret.json.golden", sandboxJSON.stdout)

	budgetedJSON := jsonFixture.run("--json", "namespace", "update", "payments",
		"--budget-cpu-seconds", "5000000", "--budget-period", "720h", "--budget-enforce",
	).requireExit(t, exitOK)
	requireGolden(t, "namespace-budget.json.golden", budgetedJSON.stdout)
	clearedBudgetJSON := jsonFixture.run("--json", "namespace", "update", "payments", "--clear-budget").
		requireExit(t, exitOK)
	requireGolden(t, "namespace-clear-budget.json.golden", clearedBudgetJSON.stdout)

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

// ---------------------------------------------------------------------------
// namespace: secret policy
// ---------------------------------------------------------------------------

// exfilDocument names a secret source a sandbox list of env:SANDBOX_* does
// not allow.
func exfilDocument(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "exfil.ork.yaml")
	document := "name: exfil\nversion: 1.0.0\ntasks:\n" +
		"  - id: grab\n    uses: shell\n" +
		"    secrets:\n      - name: db\n        source: env:PROD_DB_PASSWORD\n" +
		"    with: {command: \"cat /orkstack/input/secrets/db\"}\n"
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write exfil: %v", err)
	}
	return path
}

const exfilSentence = `namespace "sandbox" may not resolve "env:PROD_DB_PASSWORD" (task "grab", secret "db"); allowed: env:SANDBOX_*`

func TestNamespaceUpdate_RefusesAllowSecretAndClearTogether(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "sandbox").requireExit(t, exitOK)
	before := len(f.requests())
	result := f.run("namespace", "update", "sandbox",
		"--allow-secret", "env:SANDBOX_*", "--clear-allowed-secrets")
	if result.code == exitOK {
		t.Fatal("--allow-secret with --clear-allowed-secrets succeeded")
	}
	result.requireStderrContains(t, "--allow-secret", "--clear-allowed-secrets")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a contradictory update reached the server: %v", after[before:])
	}
}

func TestNamespaceUpdate_RefusesAMalformedPatternLocally(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "sandbox").requireExit(t, exitOK)
	for _, args := range [][]string{
		{"namespace", "update", "sandbox", "--allow-secret", "file:/a/../b"},
		{"namespace", "create", "other", "--allow-secret", "file:/a/../b"},
	} {
		before := len(f.requests())
		result := f.run(args...)
		if result.code == exitOK {
			t.Fatalf("%v succeeded", args)
		}
		result.requireStderrContains(t, `invalid --allow-secret "file:/a/../b"`, `".." segment`)
		if after := f.requests(); len(after) != before {
			t.Fatalf("%v reached the server: %v", args, after[before:])
		}
	}
}

func TestOrkflowValidate_PrintsTheSecretIssueOnItsFieldPath(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "sandbox", "--allow-secret", "env:SANDBOX_*").requireExit(t, exitOK)
	path := exfilDocument(t)

	human := f.run("orkflow", "validate", path, "-n", "sandbox")
	if human.code == exitOK {
		t.Fatal("validate of a forbidden secret exited 0")
	}
	human.requireContains(t, "tasks[0].secrets[0].source: "+exfilSentence)

	machine := f.run("--json", "orkflow", "validate", path, "-n", "sandbox")
	var decoded struct {
		Issues []struct {
			Field    string `json:"field"`
			Message  string `json:"message"`
			Severity string `json:"severity"`
		} `json:"issues"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &decoded); err != nil {
		t.Fatalf("decode --json: %v\n%s", err, machine.stdout)
	}
	if len(decoded.Issues) != 1 || decoded.Issues[0].Field != "tasks[0].secrets[0].source" ||
		decoded.Issues[0].Severity != "ERROR" || decoded.Issues[0].Message != exfilSentence {
		t.Fatalf("--json issues = %+v", decoded.Issues)
	}

	// apply is refused with the same sentence and stores nothing.
	apply := f.run("apply", path, "-n", "sandbox")
	if apply.code == exitOK {
		t.Fatal("apply of a forbidden secret succeeded")
	}
	apply.requireStderrContains(t, "invalid_argument", exfilSentence)
	f.run("orkflow", "list", "-n", "sandbox").requireExit(t, exitOK).requireContains(t, "No orkflows.")
}

func TestRunSubmit_RefusedByASecretPolicyNamesTheSourceAndTheList(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "sandbox").requireExit(t, exitOK)
	f.run("apply", exfilDocument(t), "-n", "sandbox").requireExit(t, exitOK)
	f.run("namespace", "update", "sandbox", "--allow-secret", "env:SANDBOX_*").requireExit(t, exitOK)

	submit := f.run("run", "submit", "exfil", "-n", "sandbox")
	if submit.code == exitOK {
		t.Fatal("a submit naming a forbidden secret succeeded")
	}
	submit.requireStderrContains(t, "invalid_argument", `"env:PROD_DB_PASSWORD"`, "env:SANDBOX_*", exfilSentence)
	f.run("run", "list", "-n", "sandbox").requireExit(t, exitOK).requireContains(t, "No runs")
}

// ---------------------------------------------------------------------------
// namespace: budgets
// ---------------------------------------------------------------------------

func TestNamespaceUpdate_BudgetNeedsAPeriodWhenNoneIsStored(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "analytics").requireExit(t, exitOK)
	result := f.run("namespace", "update", "analytics", "--budget-cpu-seconds", "5000000", "--budget-enforce")
	if result.code == exitOK {
		t.Fatal("a budget with no period was accepted on a namespace that has none")
	}
	result.requireStderrContains(t,
		"--budget-period is required to set a budget on a namespace that has none (e.g. --budget-period 720h)")
	create := f.run("namespace", "create", "payments", "--budget-cpu-seconds", "10")
	if create.code == exitOK {
		t.Fatal("create with a budget and no period succeeded")
	}
	create.requireStderrContains(t, "--budget-period is required")
}

func TestNamespaceUpdate_ClearBudgetRefusesOtherBudgetFlags(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "analytics").requireExit(t, exitOK)
	before := len(f.requests())
	result := f.run("namespace", "update", "analytics", "--clear-budget", "--budget-cpu-seconds", "10")
	if result.code == exitOK {
		t.Fatal("--clear-budget with --budget-cpu-seconds succeeded")
	}
	result.requireStderrContains(t, "--clear-budget", "--budget-cpu-seconds")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a contradictory update reached the server: %v", after[before:])
	}
}

func TestNamespaceUpdate_BudgetEnforceFalseKeepsTheBounds(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "analytics",
		"--budget-cpu-seconds", "5000000", "--budget-gib-seconds", "20000000",
		"--budget-period", "720h", "--budget-enforce").requireExit(t, exitOK)
	f.run("namespace", "update", "analytics", "--budget-enforce=false").requireExit(t, exitOK)
	got := f.run("--json", "namespace", "get", "analytics").requireExit(t, exitOK)
	var decoded struct {
		Namespace struct {
			Budget struct {
				CPUSeconds string `json:"cpuSeconds"`
				GiBSeconds string `json:"gibSeconds"`
				Period     string `json:"period"`
				Enforce    bool   `json:"enforce"`
			} `json:"budget"`
		} `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &decoded); err != nil {
		t.Fatalf("decode: %v\n%s", err, got.stdout)
	}
	b := decoded.Namespace.Budget
	if b.CPUSeconds != "5000000" || b.GiBSeconds != "20000000" || b.Period != "2592000s" || b.Enforce {
		t.Fatalf("budget after --budget-enforce=false = %+v, want the stored bounds and period, soft", b)
	}
	f.run("namespace", "get", "analytics").requireExit(t, exitOK).
		requireContains(t, "5000000 cpu-seconds and 20000000 GiB-seconds per 720h0m0s, soft (reported, never refuses)")
	f.run("namespace", "list").requireExit(t, exitOK).
		requireContains(t, "5000000 cpu-s + 20000000 GiB-s/720h0m0s soft")
}

// TestNamespaceList_Filters: --filter passes the filters to ListNamespaces
// and lists every match, `default` included by the same rule as any other
// name; without the flag the listing is every namespace, exactly as before
// (the goldens above pin that rendering).
func TestNamespaceList_Filters(t *testing.T) {
	f := newFixture(t)
	for _, args := range [][]string{
		{"payments", "--label", "team=money"},
		{"pay-later", "--label", "team=money"},
		{"billing", "--label", "team=finance"},
	} {
		f.run(append([]string{"namespace", "create"}, args...)...).requireExit(t, exitOK)
	}

	cases := []struct {
		args    []string
		listed  []string
		omitted []string
	}{
		{[]string{"namespace", "list"}, []string{"default", "payments", "pay-later", "billing"}, nil},
		{[]string{"namespace", "list", "--filter", "name~pay"}, []string{"payments", "pay-later"}, []string{"default", "billing"}},
		{[]string{"namespace", "list", "--filter", "name~fault"}, []string{"default"}, []string{"payments", "pay-later", "billing"}},
		{[]string{"namespace", "list", "--filter", "labels.team=money"}, []string{"payments", "pay-later"}, []string{"default", "billing"}},
		{[]string{"namespace", "list", "--filter", "labels.team=money", "--filter", "name^payments"}, []string{"payments"},
			[]string{"default", "pay-later", "billing"}},
		{[]string{"namespace", "list", "--filter", "labels.team=money,finance"}, []string{"payments", "pay-later", "billing"},
			[]string{"default"}},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			out := f.run(tc.args...).requireExit(t, exitOK).stdout
			rows := map[string]bool{}
			for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
				rows[strings.Fields(line)[0]] = true
			}
			for _, name := range tc.listed {
				if !rows[name] {
					t.Errorf("%v: %s not listed:\n%s", tc.args, name, out)
				}
			}
			for _, name := range tc.omitted {
				if rows[name] {
					t.Errorf("%v: %s listed, want it filtered out:\n%s", tc.args, name, out)
				}
			}
		})
	}

	machine := f.run("--json", "namespace", "list", "--filter", "name~pay").requireExit(t, exitOK)
	var decoded struct {
		Namespaces []struct {
			Name string `json:"name"`
		} `json:"namespaces"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &decoded); err != nil {
		t.Fatalf("decode: %v\n%s", err, machine.stdout)
	}
	if len(decoded.Namespaces) != 2 || decoded.Namespaces[0].Name != "pay-later" ||
		decoded.Namespaces[1].Name != "payments" || decoded.NextPageToken != "" {
		t.Fatalf("--json --filter name~pay = %+v, want [pay-later payments] and no page token", decoded)
	}

	none := f.run("namespace", "list", "--filter", "name~zz").requireExit(t, exitOK)
	requireGolden(t, "namespace-list-no-match.golden", none.stdout)

	// A semantic refusal is the server's, printed verbatim, exit non-zero.
	refused := f.run("namespace", "list", "--filter", "nope=x").requireExit(t, exitError)
	if !strings.Contains(refused.stderr, `filters[0]: "nope" is not a filterable field of Namespace; filterable: name, description, labels.KEY`) {
		t.Fatalf("nope=x stderr = %q", refused.stderr)
	}
	named := f.run("namespace", "list", "--filter", "name=Payments").requireExit(t, exitError)
	if !strings.Contains(named.stderr, `filters[0]: name "Payments" is not a slug`) {
		t.Fatalf("name=Payments stderr = %q", named.stderr)
	}
}

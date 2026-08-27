//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_PromoteRollbackAndEditThroughTheCLI is M10.4's user-facing
// acceptance, driven by the real orkctl binary against a real `ork`, a real
// Orker and a real Temporal cluster.
//
// The session it records is the milestone's transcript: apply two versions,
// see them, promote the older one, watch a run execute THAT one, roll the
// pointer forward, watch a run execute the newer one, diff them, and edit
// through $EDITOR into a third version.
//
// Isolation (AGENTS.md §6): its own Temporal namespace and its own pool, so a
// parallel agent can neither steal its tasks nor decide its verdict.
func TestIntegration_PromoteRollbackAndEditThroughTheCLI(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	dialIsolatedNamespace(ctx, t, address, namespace)

	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}
	root := repoRoot(t)
	binDir := t.TempDir()
	orkctlPath := buildBinary(t, root, binDir, "orkctl")
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+taskQueue,
		"ORK_ORKER_WORKDIR="+filepath.Join(t.TempDir(), "work"),
	)
	engineAddr := freeAddress(t)
	startProcess(ctx, t, "ork", enginePath,
		"ORK_HTTP_ADDR="+engineAddr,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+taskQueue,
	)

	session := &sessionRecorder{}
	// A per-run config file, so this test never reads or writes the
	// developer's own `orkctl namespace use` choice.
	configPath := filepath.Join(t.TempDir(), "orkctl.json")
	cli := func(args ...string) (string, string, int) {
		full := append(append([]string(nil), args...), "--server", "http://"+engineAddr)
		stdout, stderr, code := runBinaryWithEnv(ctx, t, orkctlPath, root,
			[]string{configEnvVar + "=" + configPath}, full...)
		session.record(full, stdout, stderr, code)
		return stdout, stderr, code
	}
	waitForEngine(t, func(args ...string) (string, string, int) {
		return runBinaryWithEnv(ctx, t, orkctlPath, root,
			[]string{configEnvVar + "=" + configPath},
			append(args, "--server", "http://"+engineAddr)...)
	})
	t.Cleanup(func() { writeVersionSession(t, session) })

	name := fmt.Sprintf("versions-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	first := writeVersionedCLIOrkflow(t, name, "1.0.0", pool, "one")
	second := writeVersionedCLIOrkflow(t, name, "2.0.0", pool, "two")

	requireCLI(t, cli, "apply the first version", "apply", first)

	// The second version is applied TWICE, both times as `--json`, because
	// that is the only rendering that can tell the two applies apart:
	// `ApplyOrkflowResponse.created` is a bool, so protojson prints it on the
	// apply that created a version and omits it on the repeat, while the human
	// rendering prints the same "Applied orkflow …" line for both.
	//
	// The first of the pair is the control. Without it, an absent `created`
	// would prove nothing — a field can be missing because the apply was a
	// no-op, or because the CLI never sends it at all.
	firstApply := requireCLI(t, cli, "apply the second version", "apply", second, "--json")
	if !strings.Contains(firstApply, `"created": true`) {
		t.Fatalf("applying a new version did not report that it created one:\n%s", firstApply)
	}
	repeat := requireCLI(t, cli, "re-apply the unchanged file", "apply", second, "--json")
	if strings.Contains(repeat, `"created"`) {
		t.Fatalf("re-applying an unchanged file reported that it created a version:\n%s", repeat)
	}
	if !strings.Contains(repeat, `"version": "2.0.0"`) {
		t.Fatalf("the no-op did not answer with the stored version:\n%s", repeat)
	}

	listed := requireCLI(t, cli, "list the versions", "orkflow", "versions", name)
	if !strings.Contains(listed, "No version has been promoted") {
		t.Fatalf("a fresh orkflow claims a promotion:\n%s", listed)
	}

	// ---- promote the OLDER version, and run it ----------------------------
	promoted := requireCLI(t, cli, "promote 1.0.0", "orkflow", "promote", name, "1.0.0")
	if !strings.Contains(promoted, "Promoted "+name+" to version 1.0.0") {
		t.Fatalf("promote stdout:\n%s", promoted)
	}
	current := requireCLI(t, cli, "the listing marks it", "orkflow", "versions", name)
	if !strings.Contains(current, "Current: 1.0.0") {
		t.Fatalf("the listing does not mark 1.0.0 current:\n%s", current)
	}

	ran := requireCLI(t, cli, "run the current version", "run", "submit", name, "--watch")
	if !strings.Contains(ran, "SUCCEEDED") {
		t.Fatalf("the run did not succeed:\n%s", ran)
	}
	runID := parseRunID(t, ran)
	describeOne := requireCLI(t, cli, "the run reports its version", "run", "get", runID)
	if !strings.Contains(describeOne, "1.0.0") {
		t.Fatalf("run %s does not report version 1.0.0:\n%s", runID, describeOne)
	}

	// ---- roll the pointer forward, and run again --------------------------
	rolled := requireCLI(t, cli, "roll forward to 2.0.0", "orkflow", "promote", name, "2.0.0")
	if !strings.Contains(rolled, "Previous: 1.0.0") {
		t.Fatalf("the rollback does not report what it replaced:\n%s", rolled)
	}
	ranAgain := requireCLI(t, cli, "run the new current version", "run", "submit", name, "--watch")
	secondRun := parseRunID(t, ranAgain)
	describeTwo := requireCLI(t, cli, "the second run reports its version", "run", "get", secondRun)
	if !strings.Contains(describeTwo, "2.0.0") {
		t.Fatalf("run %s does not report version 2.0.0:\n%s", secondRun, describeTwo)
	}

	// A run pinned by hand still runs the version it names, whatever the
	// pointer says.
	pinned := requireCLI(t, cli, "pin a run to the old version",
		"run", "submit", name, "--version", "1.0.0", "--watch")
	pinnedRun := parseRunID(t, pinned)
	describePinned := requireCLI(t, cli, "the pinned run reports its version",
		"run", "get", pinnedRun)
	if !strings.Contains(describePinned, "1.0.0") {
		t.Fatalf("the pinned run does not report 1.0.0:\n%s", describePinned)
	}

	// ---- diff, and the promote-to-unknown refusal -------------------------
	diffed := requireCLI(t, cli, "diff the two versions",
		"orkflow", "diff", name, "1.0.0", "2.0.0")
	if !strings.Contains(diffed, "-      command: echo one") ||
		!strings.Contains(diffed, "+      command: echo two") {
		t.Fatalf("the diff does not describe the change:\n%s", diffed)
	}
	if _, stderr, code := cli("orkflow", "promote", name, "9.9.9"); code == 0 {
		t.Fatal("promoting to an unknown version succeeded")
	} else if !strings.Contains(stderr, "not_found") {
		t.Fatalf("the refusal does not say why:\n%s", stderr)
	}

	// ---- $EDITOR round trip ------------------------------------------------
	editor := writeCLIEditor(t, `sed -i.bak -e 's/echo two/echo edited/' -e 's/2\.0\.0/3\.0\.0/' "$1"`)
	edited := requireCLI(t, cli, "edit in $EDITOR and apply the result",
		"orkflow", "edit", name, "--editor", editor)
	if !strings.Contains(edited, "3.0.0") {
		t.Fatalf("the edit did not produce version 3.0.0:\n%s", edited)
	}
	afterEdit := requireCLI(t, cli, "the edit added a version and moved nothing",
		"orkflow", "versions", name)
	if !strings.Contains(afterEdit, "Current: 2.0.0") {
		t.Fatalf("applying an edit moved the current pointer:\n%s", afterEdit)
	}
	for _, version := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		if !strings.Contains(afterEdit, version) {
			t.Errorf("version %s is missing from the listing:\n%s", version, afterEdit)
		}
	}
	// The version the editor started from is untouched: its source is what it
	// always was.
	original := requireCLI(t, cli, "the edited-from version is unchanged",
		"orkflow", "diff", name, "2.0.0", "3.0.0")
	if !strings.Contains(original, "-      command: echo two") {
		t.Fatalf("2.0.0 no longer holds what it held:\n%s", original)
	}

	// ---- an edit made while somebody else applied --------------------------
	//
	// The editor is a real program, so it can do what a second operator does
	// between a fetch and a save: apply a competing version. The refusal that
	// follows is the stale-edit gate, seen end to end and recorded in the
	// session beside this test.
	competing := writeVersionedCLIOrkflow(t, name, "4.0.0", pool, "competing")
	racing := writeCLIEditor(t, fmt.Sprintf(
		`%q --server %q apply %q >/dev/null; sed -i.bak 's/echo edited/echo mine/' "$1"`,
		orkctlPath, "http://"+engineAddr, competing))
	stdout, stderr, code := cli("orkflow", "edit", name, "--editor", racing)
	if code == 0 {
		t.Fatalf("an edit derived from a version that stopped being the head was applied:\n%s",
			stdout)
	}
	for _, want := range []string{"failed_precondition", "3.0.0", "4.0.0", "orkctl orkflow diff"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, stderr)
		}
	}
	if !strings.Contains(stderr, "your edit was kept at ") {
		t.Errorf("a refused edit was not kept anywhere:\n%s", stderr)
	}
	// Nothing was stored for the refused edit: the versions are the four that
	// were really applied.
	final := requireCLI(t, cli, "the refused edit stored nothing",
		"orkflow", "versions", name)
	if strings.Contains(final, "echo mine") {
		t.Errorf("a refused edit reached the store:\n%s", final)
	}
}

// requireCLI runs one command, fails the test when it exits non-zero, and
// returns its stdout.
func requireCLI(
	t *testing.T,
	cli func(args ...string) (string, string, int),
	what string,
	args ...string,
) string {
	t.Helper()
	stdout, stderr, code := cli(args...)
	if code != 0 {
		t.Fatalf("%s: orkctl %s exited %d\nstdout:\n%s\nstderr:\n%s",
			what, strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout
}

// writeVersionedCLIOrkflow writes one version of a pool-pinned orkflow whose
// task prints a marker, so a run's output identifies the version that ran.
func writeVersionedCLIOrkflow(t *testing.T, name, version, pool, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+"-"+version+".ork.yaml")
	document := fmt.Sprintf(`name: %s
version: "%s"
description: M10.4 %s
tasks:
  - id: report
    uses: shell
    pool: %s
    with:
      op: exec
      command: echo %s
`, name, version, version, pool, marker)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write %s@%s: %v", name, version, err)
	}
	return path
}

// writeCLIEditor writes the stand-in for $EDITOR: a real program orkctl execs
// with the document as its only argument.
func writeCLIEditor(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -e\n"+script+"\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	return path
}

// writeVersionSession records the CLI transcript when the milestone's
// reproduce.sh asks for it.
func writeVersionSession(t *testing.T, session *sessionRecorder) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	header := fmt.Sprintf(
		"# orkctl promote / rollback / diff / edit against a real ork + orker + Temporal (%s)\n"+
			"# Runs execute for real on a private pool this test's Orker alone polls.\n"+
			"# generated by: go test -tags=integration -p 1 ./cmd/orkctl/ \\\n"+
			"#                 -run TestIntegration_PromoteRollbackAndEditThroughTheCLI\n\n",
		time.Now().UTC().Format(time.RFC3339))
	path := filepath.Join(dir, "promote-rollback-session.txt")
	if err := os.WriteFile(path, []byte(header+session.transcript.String()), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s", path)
}

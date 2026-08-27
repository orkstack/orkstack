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

// TestIntegration_EditingTheCurrentVersionForksItForwardThroughTheCLI is
// M11.2's acceptance end to end, and it is the owner's scenario verbatim:
//
//  1. apply two versions;
//  2. promote the OLDER one — head stays where it was, `current` moves;
//  3. edit the CURRENT version, which is NOT the head, and apply the result;
//  4. head moves to the fork, `current` is exactly where the promotion left it,
//     and an unpinned run still executes the promoted version.
//
// Step 3 was impossible before this slice: the CLI could only open the head,
// and the server refused any base that was not the head. Both halves are here,
// against a real `ork`, a real Orker and a real Temporal cluster, so the
// acceptance is measured on the tree a user runs rather than on a fake.
//
// It also runs the race the fork does NOT exempt anybody from: a competing
// apply lands while the editor is open, and the apply is refused naming the
// head that was fetched and the head that replaced it — with the edit kept on
// disk, as it always was.
//
// Isolation (AGENTS.md §6): its own Temporal namespace and its own pool, so a
// parallel agent can neither steal its tasks nor decide its verdict.
func TestIntegration_EditingTheCurrentVersionForksItForwardThroughTheCLI(t *testing.T) {
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
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
	)
	engineAddr := freeAddress(t)
	startProcess(ctx, t, "ork", enginePath,
		"ORK_HTTP_ADDR="+engineAddr,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+taskQueue,
	)

	session := &sessionRecorder{}
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
	t.Cleanup(func() { writeVersionScopedEditSession(t, session) })

	name := fmt.Sprintf("forked-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)

	// ---- 1. two versions ---------------------------------------------------
	requireCLI(t, cli, "apply 1.0.0", "apply",
		writeM112Orkflow(t, name, "1.0.0", pool, "one"))
	requireCLI(t, cli, "apply 2.0.0", "apply",
		writeM112Orkflow(t, name, "2.0.0", pool, "two"))

	// ---- 2. promote the OLDER one -----------------------------------------
	requireCLI(t, cli, "promote the older version", "orkflow", "promote", name, "1.0.0")
	before := requireCLI(t, cli, "head stayed, current moved", "orkflow", "versions", name)
	if !strings.Contains(before, "Current: 1.0.0   Newest applied: 2.0.0") {
		t.Fatalf("promotion did not leave current 1.0.0 with head 2.0.0:\n%s", before)
	}

	// ---- 3. edit the CURRENT version, which is not the head ---------------
	//
	// The stub editor asserts what it was handed: 1.0.0's source. Handed the
	// head's text it fails here, which is the bug this slice fixes stated as a
	// test rather than as prose.
	editor := writeCLIEditor(t, `grep -q 'echo one' "$1" || { echo "not 1.0.0's source" >&2; exit 1; }
sed -i.bak -e 's/echo one/echo forked/' -e 's/1\.0\.0/1\.1\.0/' "$1"`)
	forked := requireCLI(t, cli, "edit the CURRENT version and apply it as a new one",
		"orkflow", "edit", name, "--version", "1.0.0", "--editor", editor)
	for _, want := range []string{"1.1.0", "Forked from " + name + "@1.0.0", "(2.0.0 was)"} {
		if !strings.Contains(forked, want) {
			t.Fatalf("the fork does not report %q:\n%s", want, forked)
		}
	}

	// ---- 4. head moved, current did not -----------------------------------
	after := requireCLI(t, cli, "head moved to the fork; current is untouched",
		"orkflow", "versions", name)
	if !strings.Contains(after, "Current: 1.0.0   Newest applied: 1.1.0") {
		t.Fatalf("after the fork: want current 1.0.0 and head 1.1.0:\n%s", after)
	}
	for _, version := range []string{"1.0.0", "2.0.0", "1.1.0"} {
		if !strings.Contains(after, version) {
			t.Errorf("version %s is missing from the listing:\n%s", version, after)
		}
	}
	// The fork is 1.0.0's body edited, not the head's: `echo two` never
	// entered it.
	diffed := requireCLI(t, cli, "the fork came from 1.0.0, not from the head",
		"orkflow", "diff", name, "1.0.0", "1.1.0")
	if !strings.Contains(diffed, "-      command: [\"sh\", \"-c\", \"echo one\"]") ||
		!strings.Contains(diffed, "+      command: [\"sh\", \"-c\", \"echo forked\"]") {
		t.Fatalf("the fork is not 1.0.0 edited:\n%s", diffed)
	}
	// The version it was derived from is untouched — a fork adds, never
	// rewrites — and so is the version that was head when the editor opened.
	for version, marker := range map[string]string{"1.0.0": "echo one", "2.0.0": "echo two"} {
		stored := requireCLI(t, cli, "the stored version is unchanged",
			"--json", "orkflow", "get", name, "--version", version)
		if !strings.Contains(stored, marker) {
			t.Errorf("version %s no longer holds %q:\n%s", version, marker, stored)
		}
	}

	// Applying is not promoting: an unpinned run still executes what the
	// operator promoted, and it runs for real on this test's own pool.
	ran := requireCLI(t, cli, "an unpinned run still executes the promoted version",
		"run", "submit", name, "--watch")
	if !strings.Contains(ran, "SUCCEEDED") {
		t.Fatalf("the run did not succeed:\n%s", ran)
	}
	describe := requireCLI(t, cli, "the run reports the version it executed",
		"run", "get", parseRunID(t, ran))
	if !strings.Contains(describe, "1.0.0") {
		t.Fatalf("an unpinned run did not execute the promoted 1.0.0:\n%s", describe)
	}

	// ---- the race a fork does not exempt anybody from ---------------------
	//
	// The editor is a real program, so it can do what a second operator does
	// between a fetch and a save: apply a competing version. The base is
	// 1.0.0 — deliberately not the head — and the refusal is about the HEAD
	// that moved (1.1.0 -> 3.0.0), which is the only thing that changed
	// underneath the edit.
	competing := writeM112Orkflow(t, name, "3.0.0", pool, "competing")
	racing := writeCLIEditor(t, fmt.Sprintf(
		`%q --server %q apply %q >/dev/null; `+
			`sed -i.bak -e 's/echo one/echo mine/' -e 's/1\.0\.0/1\.9\.0/' "$1"`,
		orkctlPath, "http://"+engineAddr, competing))
	stdout, stderr, code := cli("orkflow", "edit", name, "--version", "1.0.0", "--editor", racing)
	if code == 0 {
		t.Fatalf("an edit applied over a head that moved while it was open:\n%s", stdout)
	}
	for _, want := range []string{"failed_precondition", "1.0.0", "1.1.0", "3.0.0",
		"orkctl orkflow diff", "your edit was kept at "} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, stderr)
		}
	}
	final := requireCLI(t, cli, "the refused edit stored nothing", "orkflow", "versions", name)
	if strings.Contains(final, "1.9.0") {
		t.Errorf("a refused edit reached the store:\n%s", final)
	}
	if !strings.Contains(final, "Current: 1.0.0   Newest applied: 3.0.0") {
		t.Errorf("after the refusal: want current 1.0.0 and head 3.0.0:\n%s", final)
	}

	// Re-reading the head is all it takes: the same old base, retargeted at
	// the head that is really there, lands.
	retarget := writeCLIEditor(t,
		`sed -i.bak -e 's/echo one/echo retargeted/' -e 's/1\.0\.0/1\.9\.0/' "$1"`)
	landed := requireCLI(t, cli, "the retargeted fork lands from the same old base",
		"orkflow", "edit", name, "--version", "1.0.0", "--editor", retarget)
	if !strings.Contains(landed, "1.9.0") {
		t.Fatalf("the retargeted fork did not apply:\n%s", landed)
	}
	closing := requireCLI(t, cli, "the lineage, at the end", "orkflow", "versions", name)
	if !strings.Contains(closing, "Current: 1.0.0   Newest applied: 1.9.0") {
		t.Fatalf("at the end: want current 1.0.0 and head 1.9.0:\n%s", closing)
	}
}

// writeM112Orkflow writes one version of a pool-pinned orkflow whose task
// prints a marker, so a run's output identifies the version that ran.
func writeM112Orkflow(t *testing.T, name, version, pool, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+"-"+version+".ork.yaml")
	document := fmt.Sprintf(`name: %s
version: "%s"
description: M11.2 %s
tasks:
  - id: report
    uses: shell
    pool: %s
    with:
      op: exec
      command: ["sh", "-c", "echo %s"]
`, name, version, version, pool, marker)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write %s@%s: %v", name, version, err)
	}
	return path
}

// writeVersionScopedEditSession records the CLI transcript when the
// milestone's reproduce.sh asks for it (ORK_PROOF_DIR). A plain
// `go test -tags=integration ./...` writes nothing into the tree.
func writeVersionScopedEditSession(t *testing.T, session *sessionRecorder) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	header := fmt.Sprintf(
		"# M11.2: editing a version that is not the head, against a real ork +\n"+
			"# orker + Temporal (%s)\n"+
			"# The owner's scenario, in order: apply two versions; promote the OLDER\n"+
			"# one (head stays, current moves); edit the CURRENT version — not the\n"+
			"# head — and apply it as a new version (head moves, current does not);\n"+
			"# then a competing apply mid-edit, refused naming the head that was\n"+
			"# fetched and the head that replaced it.\n"+
			"# Runs execute for real on a private pool this test's Orker alone polls.\n"+
			"# generated by: go test -tags=integration -p 1 ./cmd/orkctl/ \\\n"+
			"#                 -run TestIntegration_EditingTheCurrentVersionForksItForwardThroughTheCLI\n\n",
		time.Now().UTC().Format(time.RFC3339))
	path := filepath.Join(dir, "version-scoped-edit-session.txt")
	if err := os.WriteFile(path, []byte(header+session.transcript.String()), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s", path)
}

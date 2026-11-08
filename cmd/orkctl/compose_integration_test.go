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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The composed-orkflows CLI suite (docs/DSL.md "Composition"): real `ork`,
// `orker` and `orkctl` against a real cluster and bucket, applying the
// shipped composition examples into a stamped OrkStack namespace.

// composedNamespace creates a fresh OrkStack namespace through the CLI and
// applies the two examples into it, child first.
func composedNamespace(t *testing.T, server string) string {
	t.Helper()
	ns := fmt.Sprintf("compose-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	planCLI(t, server, "namespace", "create", ns).requireExit(t, exitOK)
	root := repoRoot(t)
	for _, file := range []string{"release-block.ork.yaml", "composed-release.ork.yaml"} {
		planCLI(t, server, "-n", ns, "apply", filepath.Join(root, orkflowFixtures, file)).requireExit(t, exitOK)
	}
	return ns
}

// submitComposed submits composed-release and waits for it.
func submitComposed(t *testing.T, server, ns string) string {
	t.Helper()
	submitted := &orkstackv1.SubmitRunResponse{}
	raw := planCLI(t, server, "-n", ns, "run", "submit", "composed-release", "--json").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(raw), submitted); err != nil {
		t.Fatalf("decode submit JSON: %v\n%s", err, raw)
	}
	runID := submitted.GetRun().GetId()
	planCLI(t, server, "-n", ns, "run", "watch", runID).requireExit(t, exitOK)
	return runID
}

func childRunOf(t *testing.T, server, ns, runID string) string {
	t.Helper()
	run := decodeRun(t, planCLI(t, server, "-n", ns, "run", "get", runID, "--json").requireExit(t, exitOK).stdout)
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == "release" {
			if id := tr.GetChild().GetRunId(); id != "" {
				return id
			}
		}
	}
	t.Fatalf("run %s names no child run:\n%v", runID, run)
	return ""
}

func TestIntegration_CLIComposedTreeAndChildren(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, true)
	isolateCLIEnv(t)
	server := "http://" + cluster.a.addr
	ns := composedNamespace(t, server)
	runID := submitComposed(t, server, ns)
	childID := childRunOf(t, server, ns, runID)

	tree := planCLI(t, server, "-n", ns, "run", "get", runID, "--tree").requireExit(t, exitOK)
	tree.requireContains(t, "release → "+childID+"  release-block@0.1.0  SUCCEEDED", "  sign ", "  publish ")
	children := planCLI(t, server, "-n", ns, "run", "list", "--filter", "origin.kind=PARENT_RUN", "--filter", "origin.id="+runID).requireExit(t, exitOK)
	rows := 0
	for _, line := range strings.Split(strings.TrimSpace(children.stdout), "\n") {
		if strings.HasPrefix(line, "ork-") {
			rows++
		}
	}
	if !strings.Contains(children.stdout, childID) || rows != 1 {
		t.Fatalf("the children list is not exactly the child:\n%s", children.stdout)
	}
	refused := planCLI(t, server, "-n", ns, "run", "logs", runID, "--task", "release")
	if refused.code == exitOK || !strings.Contains(refused.stderr, "child run "+childID) {
		t.Fatalf("run logs --task release = %d\n%s", refused.code, refused.stderr)
	}
	logs := planCLI(t, server, "-n", ns, "run", "logs", childID).requireExit(t, exitOK)
	whole := planCLI(t, server, "-n", ns, "run", "logs", runID).requireExit(t, exitOK)
	if !strings.Contains(whole.stdout, "release: runs as child run "+childID) {
		t.Fatalf("run logs of the parent does not point at the child:\n%s", whole.stdout)
	}
	proof.Write(t, "orkctl", "compose-C1.txt", []byte(fmt.Sprintf(
		"# run get --tree\n%s\n# run list --filter origin.kind=PARENT_RUN --filter origin.id=%s\n%s\n# run logs --task release\n%s\n# run logs %s\n%s\n",
		tree.stdout, runID, children.stdout, refused.stderr, childID, logs.stdout)))
}

// chainTopDocument composes the shipped composed-release, itself composing
// release-block, so a submitted run is the top of a three-level chain.
const chainTopDocument = `apiVersion: orkstack/v1
kind: Orkflow
name: chain-top
version: "1.0.0"
tasks:
  - id: release
    uses: orkflow:composed-release@0.1.0
`

// TestIntegration_CLIChainNamesItsRoot is the chain through the real API and
// CLI, on a replica that did not start it: the grandchild names the top run as
// well as its parent in `run get` and --json, a live or archived listing row
// carries it, one filter lists everything below the top, a top-level run shows
// no line, and a re-run of the grandchild is a new top-level run with no root.
func TestIntegration_CLIChainNamesItsRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, true)
	isolateCLIEnv(t)
	serverA := "http://" + cluster.a.addr
	serverB := "http://" + cluster.b.addr
	ns := composedNamespace(t, serverA)
	document := filepath.Join(t.TempDir(), "chain-top.ork.yaml")
	if err := os.WriteFile(document, []byte(chainTopDocument), 0o644); err != nil {
		t.Fatal(err)
	}
	planCLI(t, serverA, "-n", ns, "apply", document).requireExit(t, exitOK)

	submitted := &orkstackv1.SubmitRunResponse{}
	raw := planCLI(t, serverA, "-n", ns, "run", "submit", "chain-top", "--json").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(raw), submitted); err != nil {
		t.Fatalf("decode submit JSON: %v\n%s", err, raw)
	}
	topID := submitted.GetRun().GetId()
	planCLI(t, serverA, "-n", ns, "run", "watch", topID).requireExit(t, exitOK)
	midID := childRunOf(t, serverA, ns, topID)
	leafID := childRunOf(t, serverA, ns, midID)

	leaf := planCLI(t, serverB, "-n", ns, "run", "get", leafID).requireExit(t, exitOK)
	leaf.requireContains(t, "parent-run "+midID, "Root run:", topID)
	mid := planCLI(t, serverB, "-n", ns, "run", "get", midID).requireExit(t, exitOK)
	mid.requireContains(t, "parent-run "+topID, "Root run:")
	top := planCLI(t, serverB, "-n", ns, "run", "get", topID).requireExit(t, exitOK)
	if strings.Contains(top.stdout, "Root run:") {
		t.Fatalf("a top-level run shows a root:\n%s", top.stdout)
	}

	leafJSON := decodeRun(t, planCLI(t, serverB, "-n", ns, "run", "get", leafID, "--json").requireExit(t, exitOK).stdout)
	if got := leafJSON.GetOrigin(); got.GetId() != midID || got.GetRootRunId() != topID {
		t.Fatalf("--json origin = %v, want parent %s and root %s", got, midID, topID)
	}
	listed := &orkstackv1.ListRunsResponse{}
	rawList := planCLI(t, serverB, "-n", ns, "run", "list", "--json",
		"--filter", "origin.kind=PARENT_RUN", "--filter", "origin.id="+midID).requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(rawList), listed); err != nil {
		t.Fatalf("decode list JSON: %v\n%s", err, rawList)
	}
	if len(listed.GetRuns()) != 1 || listed.GetRuns()[0].GetOrigin().GetRootRunId() != topID {
		t.Fatalf("the children of %s list as %v, want the leaf with root %s", midID, listed.GetRuns(), topID)
	}

	// One query for everything below the top, on the replica that did not start
	// it. The engine registered the attribute at boot and stamped each child, so
	// this is the cluster's index answering, to a deadline for its visibility.
	chain := &orkstackv1.ListRunsResponse{}
	var rawChain string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		rawChain = planCLI(t, serverB, "-n", ns, "run", "list", "--json", "--filter", "origin.root_run_id="+topID).requireExit(t, exitOK).stdout
		chain.Reset()
		if err := api.UnmarshalJSON([]byte(rawChain), chain); err != nil {
			t.Fatalf("decode chain JSON: %v\n%s", err, rawChain)
		}
		if len(chain.GetRuns()) == 2 || time.Now().After(deadline) {
			break
		}
	}
	got := map[string]bool{}
	for _, run := range chain.GetRuns() {
		got[run.GetId()] = true
	}
	if len(chain.GetRuns()) != 2 || !got[midID] || !got[leafID] {
		t.Fatalf("the chain below %s lists as %s, want %s and %s", topID, rawChain, midID, leafID)
	}

	again := &orkstackv1.SubmitRunResponse{}
	rawAgain := planCLI(t, serverA, "-n", ns, "run", "again", leafID, "--json").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(rawAgain), again); err != nil {
		t.Fatalf("decode run again JSON: %v\n%s", err, rawAgain)
	}
	if got := again.GetRun().GetOrigin(); got.GetId() != leafID || got.GetRootRunId() != "" {
		t.Fatalf("a re-run of the leaf has origin %v, want its source and no root", got)
	}
	// A re-run of the leaf is a new top-level run, so it never joins the chain.
	stillTwo := &orkstackv1.ListRunsResponse{}
	if err := api.UnmarshalJSON([]byte(planCLI(t, serverB, "-n", ns, "run", "list", "--json",
		"--filter", "origin.root_run_id="+topID).requireExit(t, exitOK).stdout), stillTwo); err != nil {
		t.Fatal(err)
	}
	if len(stillTwo.GetRuns()) != 2 {
		t.Fatalf("after the re-run the chain lists %d runs, want 2", len(stillTwo.GetRuns()))
	}
	proof.Write(t, "orkctl", "root-run-chain.txt", []byte(fmt.Sprintf(
		"# replica B: run get %s\n%s\n# replica B: run list --filter origin.kind=PARENT_RUN --filter origin.id=%s\n%s\n# replica B: run list --filter origin.root_run_id=%s\n%s\n",
		leafID, leaf.stdout, midID, rawList, topID, rawChain)))
}

func TestIntegration_ComposedRunReadsOnASecondReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, true)
	isolateCLIEnv(t)
	serverA := "http://" + cluster.a.addr
	serverB := "http://" + cluster.b.addr
	ns := composedNamespace(t, serverA)
	runID := submitComposed(t, serverA, ns)
	childID := childRunOf(t, serverA, ns, runID)

	tree := planCLI(t, serverB, "-n", ns, "run", "get", runID, "--tree").requireExit(t, exitOK)
	tree.requireContains(t, "release → "+childID+"  release-block@0.1.0  SUCCEEDED")
	children := planCLI(t, serverB, "-n", ns, "run", "list", "--filter", "origin.kind=PARENT_RUN", "--filter", "origin.id="+runID).requireExit(t, exitOK)
	if !strings.Contains(children.stdout, childID) {
		t.Fatalf("replica B's children list lacks %s:\n%s", childID, children.stdout)
	}
	proof.Write(t, "orkctl", "compose-C2.txt", []byte(fmt.Sprintf(
		"# replica B: run get --tree %s\n%s\n# replica B: run list --filter origin.kind=PARENT_RUN --filter origin.id=%s\n%s\n",
		runID, tree.stdout, runID, children.stdout)))
}

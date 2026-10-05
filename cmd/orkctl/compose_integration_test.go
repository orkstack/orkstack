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

// The composed-orkflows CLI suite (spec composed-orkflows C1–C2): real `ork`,
// `orker` and `orkctl` against a real cluster and bucket, applying the §2.6
// examples into a stamped OrkStack namespace.

// composedNamespace creates a fresh OrkStack namespace through the CLI and
// applies the two examples into it, child first.
func composedNamespace(t *testing.T, server string) string {
	t.Helper()
	ns := fmt.Sprintf("compose-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	planCLI(t, server, "namespace", "create", ns).requireExit(t, exitOK)
	root := repoRoot(t)
	for _, file := range []string{"release-block.ork.yaml", "composed-release.ork.yaml"} {
		planCLI(t, server, "-n", ns, "apply", filepath.Join(root, "examples", file)).requireExit(t, exitOK)
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

// C1.
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
	children := planCLI(t, server, "-n", ns, "run", "list", "--origin", "parent-run:"+runID).requireExit(t, exitOK)
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
		"# run get --tree\n%s\n# run list --origin parent-run:%s\n%s\n# run logs --task release\n%s\n# run logs %s\n%s\n",
		tree.stdout, runID, children.stdout, refused.stderr, childID, logs.stdout)))
}

// C2.
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
	children := planCLI(t, serverB, "-n", ns, "run", "list", "--origin", "parent-run:"+runID).requireExit(t, exitOK)
	if !strings.Contains(children.stdout, childID) {
		t.Fatalf("replica B's children list lacks %s:\n%s", childID, children.stdout)
	}
	proof.Write(t, "orkctl", "compose-C2.txt", []byte(fmt.Sprintf(
		"# replica B: run get --tree %s\n%s\n# replica B: run list --origin parent-run:%s\n%s\n",
		runID, tree.stdout, runID, children.stdout)))
}

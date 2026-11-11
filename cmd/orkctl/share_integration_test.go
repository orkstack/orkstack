//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// Sharing across namespaces end to end (docs/DSL.md "Sharing across
// namespaces"): real `ork`, `orker` and `orkctl` against a real cluster and
// bucket. The owner shares deploy with the caller's namespace; the caller's
// release composes it; the child runs, is listed and is read in the caller's
// namespace and names its owner; and every door that is not composition
// refuses another namespace's Orkflow.
func TestIntegration_CLISharedOrkflowRunsInTheCallersNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, true)
	isolateCLIEnv(t)
	server := "http://" + cluster.a.addr
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	owner, caller := "platform-"+stamp, "search-"+stamp
	for _, ns := range []string{owner, caller} {
		planCLI(t, server, "namespace", "create", ns).requireExit(t, exitOK)
	}
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	deploy := write("deploy.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1.0.0"
shared_with:
  - namespace: `+caller+`
tasks:
  - id: ship
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo shipped"]
`)
	private := write("private.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: private
version: "1"
tasks:
  - id: t
    uses: shell
    with: { op: eval, command: ["true"] }
`)
	release := write("release.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: release
version: "1"
tasks:
  - id: deploy
    uses: orkflow:`+owner+`/deploy@1.0.0
`)
	usesPrivate := write("uses-private.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: uses-private
version: "1"
tasks:
  - id: p
    uses: orkflow:`+owner+`/private@1
`)
	planCLI(t, server, "-n", owner, "orkflow", "create", deploy).requireExit(t, exitOK)
	planCLI(t, server, "-n", owner, "orkflow", "create", private).requireExit(t, exitOK)
	planCLI(t, server, "-n", owner, "orkflow", "get", "deploy").requireExit(t, exitOK).
		requireContains(t, "namespace:"+caller)
	planCLI(t, server, "-n", caller, "orkflow", "create", release).requireExit(t, exitOK)

	// Not shared: refused at apply, writing nothing.
	planCLI(t, server, "-n", caller, "orkflow", "create", usesPrivate).requireExit(t, exitError).
		requireStderrContains(t, "ORKFLOW_NOT_SHARED")

	submitted := &orkstackv1.SubmitRunResponse{}
	raw := planCLI(t, server, "-n", caller, "run", "submit", "release", "--json").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(raw), submitted); err != nil {
		t.Fatalf("decode submit JSON: %v\n%s", err, raw)
	}
	runID := submitted.GetRun().GetId()
	planCLI(t, server, "-n", caller, "run", "watch", runID).requireExit(t, exitOK)
	parent := decodeRun(t, planCLI(t, server, "-n", caller, "run", "get", runID, "--json").requireExit(t, exitOK).stdout)
	var childID string
	for _, tr := range parent.GetTaskRuns() {
		if tr.GetTaskId() == "deploy" {
			childID = tr.GetChild().GetRunId()
			if tr.GetChild().GetOrkflowNamespace() != owner {
				t.Fatalf("the composed task's row names owner %q, want %q", tr.GetChild().GetOrkflowNamespace(), owner)
			}
		}
	}
	if childID == "" {
		t.Fatalf("run %s names no child run: %v", runID, parent)
	}
	child := decodeRun(t, planCLI(t, server, "-n", caller, "run", "get", childID, "--json").requireExit(t, exitOK).stdout)
	if child.GetNamespace() != caller || child.GetOrkflowNamespace() != owner || child.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("child run: namespace %q, orkflow namespace %q, status %v", child.GetNamespace(), child.GetOrkflowNamespace(), child.GetStatus())
	}
	planCLI(t, server, "-n", caller, "run", "get", childID).requireExit(t, exitOK).
		requireContains(t, owner+"/deploy (shared from namespace "+owner+")")
	planCLI(t, server, "-n", caller, "run", "get", runID, "--tree").requireExit(t, exitOK).
		requireContains(t, owner+"/deploy@1.0.0")
	planCLI(t, server, "-n", caller, "run", "list").requireExit(t, exitOK).requireContains(t, owner+"/deploy")
	// The owner's namespace holds none of the caller's runs.
	planCLI(t, server, "-n", owner, "run", "get", childID).requireExit(t, exitError)

	// The owner's deletion sees the child run in the caller's namespace, and
	// the caller's own Orkflow of that name is not held by it.
	planCLI(t, server, "-n", owner, "orkflow", "delete", "deploy", "--yes").requireExit(t, exitError).
		requireStderrContains(t, `cannot delete orkflow "deploy" in namespace "`+owner+`"`, `1 in namespace "`+caller+`"`)
	callerDeploy := write("caller-deploy.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1"
tasks:
  - id: t
    uses: shell
    with: { op: eval, command: ["true"] }
`)
	planCLI(t, server, "-n", caller, "orkflow", "create", callerDeploy).requireExit(t, exitOK)
	planCLI(t, server, "-n", caller, "orkflow", "delete", "deploy", "--yes").requireExit(t, exitOK)

	// Every other door refuses another namespace's Orkflow.
	planCLI(t, server, "-n", caller, "run", "again", childID).requireExit(t, exitError).
		requireStderrContains(t, "SHARED_ORKFLOW_NOT_ROOT", runID)
	planCLI(t, server, "-n", caller, "schedule", "create", "nightly", "--orkflow", owner+"/deploy", "--cron", "0 2 * * *").
		requireExit(t, exitError).requireStderrContains(t, "a schedule runs only its own namespace's Orkflows")
	planCLI(t, server, "-n", caller, "trigger", "create", "--orkflow", owner+"/deploy").
		requireExit(t, exitError).requireStderrContains(t, "a trigger runs only its own namespace's Orkflows")
}

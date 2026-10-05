package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	composedParentID = "ork-1758790021000000000-1000000000000000001"
	composedChildID  = "ork-1758790021000000000-4081623940183294"
	composedGrandID  = "ork-1758790021000000000-4081623940183295"
)

func composedTS(offset time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(fixedNow.Add(-time.Hour).Add(offset))
}

func composedTaskRow(id string, offset time.Duration, orker string) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{
		TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: orker, Attempt: 1,
		ScheduledAt: composedTS(offset), StartedAt: composedTS(offset + time.Second),
		FinishedAt: composedTS(offset + 3*time.Second),
	}
}

// composedRuns is a parent whose `release` started the child, whose own
// `notarize` started a grandchild; `later` was skipped and `never` failed
// before its start.
func composedRuns(namespace string) (parent, child, grand *orkstackv1.Run) {
	child = &orkstackv1.Run{
		Id: composedChildID, Namespace: namespace, OrkflowName: "release-block", OrkflowVersion: "2.1.0",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: composedTS(4 * time.Second),
		Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: composedParentID},
		TaskRuns: []*orkstackv1.TaskRun{
			composedTaskRow("sign", 5*time.Second, "orker-3"),
			composedTaskRow("publish", 8*time.Second, "orker-1"),
			{TaskId: "notarize", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				ScheduledAt: composedTS(11 * time.Second), StartedAt: composedTS(11 * time.Second),
				FinishedAt: composedTS(20 * time.Second),
				Child: &orkstackv1.TaskChild{OrkflowName: "notarize", OrkflowVersion: "1.0",
					OrkflowDefinitionDigest: "sha256:" + strings.Repeat("ab", 32), RunId: composedGrandID}},
		},
	}
	grand = &orkstackv1.Run{
		Id: composedGrandID, Namespace: namespace, OrkflowName: "notarize", OrkflowVersion: "1.0",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: composedTS(11 * time.Second),
		Origin:   &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: composedChildID},
		TaskRuns: []*orkstackv1.TaskRun{composedTaskRow("stamp", 12*time.Second, "orker-2")},
	}
	parent = &orkstackv1.Run{
		Id: composedParentID, Namespace: namespace, OrkflowName: "deploy", OrkflowVersion: "1.0.0",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: composedTS(0), StartedAt: composedTS(0),
		FinishedAt: composedTS(30 * time.Second),
		TaskRuns: []*orkstackv1.TaskRun{
			composedTaskRow("build", time.Second, "orker-1"),
			{TaskId: "release", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				ScheduledAt: composedTS(4 * time.Second), StartedAt: composedTS(4 * time.Second),
				FinishedAt: composedTS(21 * time.Second),
				Child: &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0",
					OrkflowDefinitionDigest: "sha256:" + strings.Repeat("cd", 32), RunId: composedChildID}},
			{TaskId: "later", Status: orkstackv1.Status_STATUS_SKIPPED, FinishedAt: composedTS(21 * time.Second),
				Child: &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0"}},
			{TaskId: "never", Status: orkstackv1.Status_STATUS_FAILED, FinishedAt: composedTS(22 * time.Second),
				ScheduledAt: composedTS(21 * time.Second),
				Error: &orkstackv1.Error{Code: "CHILD_INPUT_RENDER_FAILED", Message: "the child run's input could not be rendered",
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
				Child: &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0"}},
		},
	}
	return parent, child, grand
}

func newComposedCLIFixture(t *testing.T, namespace string, runs ...*orkstackv1.Run) *fixture {
	t.Helper()
	f := newFixture(t)
	if namespace != "" && namespace != "default" {
		f.run("namespace", "create", namespace).requireExit(t, exitOK)
	}
	for _, run := range runs {
		f.runs.Apply(run)
	}
	return f
}

func TestRunGetComposedGoldens(t *testing.T) {
	parent, child, grand := composedRuns("team-a")
	f := newComposedCLIFixture(t, "team-a", parent, child, grand)
	got := f.run("-n", "team-a", "run", "get", composedParentID).requireExit(t, exitOK)
	requireGolden(t, "run-get-composed.golden", got.stdout)
	got.requireContains(t,
		"release: child run "+composedChildID+" (release-block@2.1.0). Open it with: orkctl -n team-a run get "+composedChildID,
		"later: skipped; no child run was started",
		"never: no child run was started (CHILD_INPUT_RENDER_FAILED)")

	tree := f.run("-n", "team-a", "run", "get", composedParentID, "--tree").requireExit(t, exitOK)
	requireGolden(t, "run-get-composed-tree.golden", tree.stdout)
	tree.requireContains(t, "release → "+composedChildID+"  release-block@2.1.0  SUCCEEDED",
		"  notarize → "+composedGrandID+"  notarize@1.0  SUCCEEDED", "    stamp")
}

func TestRunGetComposedTreeUnavailableGolden(t *testing.T) {
	parent, _, _ := composedRuns("team-a")
	f := newComposedCLIFixture(t, "team-a", parent)
	got := f.run("-n", "team-a", "run", "get", composedParentID, "--tree").requireExit(t, exitOK)
	requireGolden(t, "run-get-composed-tree-unavailable.golden", got.stdout)
	got.requireContains(t, "release → "+composedChildID+": unavailable (not_found: ")
}

func TestRunGet_TreeWithJSONIsRefused(t *testing.T) {
	parent, _, _ := composedRuns("default")
	f := newComposedCLIFixture(t, "", parent)
	before := len(f.requests())
	got := f.run("--json", "run", "get", composedParentID, "--tree").requireExit(t, exitError)
	got.requireStderrContains(t, "--tree reads one run per child and cannot be one JSON document; read each child with orkctl run get <id> --json (their ids are taskRuns[].child.runId)")
	if len(f.requests()) != before {
		t.Fatal("a refused --tree sent a request")
	}
}

func TestRunGet_DepthRequiresTree(t *testing.T) {
	parent, _, _ := composedRuns("default")
	f := newComposedCLIFixture(t, "", parent)
	f.run("run", "get", composedParentID, "--depth", "1").requireExit(t, exitError).
		requireStderrContains(t, "--depth limits --tree")
	f.run("run", "get", composedParentID, "--tree", "--depth", "0").requireExit(t, exitError).
		requireStderrContains(t, "--depth must be at least 1")
}

func TestRunGet_TreeStopsAtDepth(t *testing.T) {
	parent, child, grand := composedRuns("team-a")
	f := newComposedCLIFixture(t, "team-a", parent, child, grand)
	got := f.run("-n", "team-a", "run", "get", composedParentID, "--tree", "--depth", "1").requireExit(t, exitOK)
	got.requireContains(t, "release → "+composedChildID)
	if strings.Contains(got.stdout, "notarize → ") {
		t.Fatalf("--depth 1 descended to the grandchild:\n%s", got.stdout)
	}
}

func TestRunGet_TreeReadsChildrenInTheParentsNamespace(t *testing.T) {
	parent, child, _ := composedRuns("team-a")
	f := newComposedCLIFixture(t, "team-a", parent, child)
	f.run("-n", "team-a", "run", "get", composedParentID, "--tree").requireExit(t, exitOK).
		requireContains(t, "release → "+composedChildID+"  release-block@2.1.0  SUCCEEDED")

	// A parent of another namespace reads its child there, never here.
	other, _, _ := composedRuns("default")
	other.Id = "ork-1758790021000000000-1000000000000000009"
	f.runs.Apply(other)
	f.run("run", "get", other.GetId(), "--tree").requireExit(t, exitOK).
		requireContains(t, "release → "+composedChildID+": unavailable (not_found")
}

func TestOrkflowGetComposedGolden(t *testing.T) {
	f := newFixture(t)
	child := writePlanFile(t, "release-block.ork.yaml", `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`)
	f.run("apply", child).requireExit(t, exitOK)
	parent := writePlanFile(t, "deploy.ork.yaml", composedPlanYAML)
	f.run("apply", parent).requireExit(t, exitOK)
	got := f.run("orkflow", "get", "deploy").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-composed.golden", got.stdout)
	got.requireContains(t, "COMPOSED  ORKFLOW", "release   release-block@2.1.0  sha256:", "OUTPUT  TEMPLATE",
		"digest  {{ .tasks.release.output.digest }}")
	plan := f.run("run", "plan", "deploy").requireExit(t, exitOK)
	requireGolden(t, "run-plan-composed.golden", plan.stdout)
	plan.requireContains(t, "orkflow release-block@2.1.0 as a child run (not planned: orkctl run plan release-block --version 2.1.0)")
}

const composedPlanYAML = `
name: deploy
version: "1.0.0"
outputs:
  digest: '{{ .tasks.release.output.digest }}'
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
    with:
      image: '{{ .tasks.build.output.value }}'
`

func TestRunLogsComposedHintGolden(t *testing.T) {
	parent, child, grand := composedRuns("team-a")
	parent.TaskRuns = parent.TaskRuns[1:2] // release only: the hint, nothing else
	f := newComposedCLIFixture(t, "team-a", parent, child, grand)
	got := f.run("-n", "team-a", "run", "logs", composedParentID).requireExit(t, exitOK)
	requireGolden(t, "run-logs-composed-hint.golden", got.stdout)
	got.requireContains(t, "release: runs as child run "+composedChildID+"; read its logs with orkctl -n team-a run logs "+composedChildID)
}

func TestTaskRun_RefusesAComposedTask(t *testing.T) {
	fx := newTaskRunFixture(t)
	fx.run("apply", fx.writeDoc(t, "release-block.ork.yaml", `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`)).requireExit(t, exitOK)
	doc := fx.writeDoc(t, "composed.ork.yaml", composedPlanYAML)
	before := len(fx.requests())
	rt := &fakeTaskRuntime{}
	fx.runTaskRun(rt, "--file", doc, "--task", "release").requireExit(t, exitError).
		requireStderrContains(t, `task "release" runs orkflow release-block@2.1.0 as a child run, not a container: run one of its tasks with orkctl task run --from-run <child-run-id> --task <task>`)
	// The document was validated, and nothing else was asked: no fetch, no
	// upload, no container.
	for _, path := range requestsSince(fx.fixture, before) {
		if !strings.HasSuffix(path, "/ValidateOrkflow") {
			t.Fatalf("a refused composed task sent %s", path)
		}
	}
	requireNoSession(t, fx.workdir)
}

func TestRunTiming_ComposedRowIsLabelledChild(t *testing.T) {
	parent, child, grand := composedRuns("team-a")
	f := newComposedCLIFixture(t, "team-a", parent, child, grand)
	got := f.run("-n", "team-a", "run", "timing", composedParentID).requireExit(t, exitOK)
	var line string
	for _, l := range strings.Split(got.stdout, "\n") {
		if strings.HasPrefix(l, "release ") {
			line = l
		}
	}
	if !strings.Contains(line, " child ") || !strings.Contains(line, "17s") {
		t.Fatalf("release timing row = %q\n%s", line, got.stdout)
	}
	if strings.Contains(got.stdout, "  release  queue wait") {
		t.Fatalf("a composed task listed attempts:\n%s", got.stdout)
	}
}

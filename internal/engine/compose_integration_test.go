//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The composed-orkflows integration suite (docs/DSL.md "Composition"): real
// Temporal, real MinIO and the real Orker running real containers through
// Docker, on the shape of the shipped composition examples:
// build → release (a child run of release-block: sign → publish, declaring
// digest and url) → announce, the parent declaring its own digest. The
// definitions run the pinned alpine image the commit suite uses, so the
// containers write their values.json themselves. Every scenario runs in a
// private Temporal namespace on private channels (AGENTS.md §11.1).

// composedChild is release-block over alpine: sign hashes its rendered
// config, publish turns the digest into a record URL, and the outputs project
// both. signScript, when set, replaces sign's command (a failing or a
// sleeping child).
func (e *eachE2E) composedChild(name, signScript string) *orkstackv1.Orkflow {
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	sign := signScript
	if sign == "" {
		sign = `d=$(sha256sum /orkstack/input/config.json | cut -d' ' -f1) && ` +
			`printf '{"value":"sha256:%s"}' "$d" > /orkstack/output/values.json`
	}
	publish := `d=$(sed -n 's/.*"digest": *"\([^"]*\)".*/\1/p' /orkstack/input/config.json) && ` +
		`printf '{"value":"https://registry.example/records/%s"}' "$d" > /orkstack/output/values.json`
	signWith, _ := structpbStruct(map[string]any{"image": "{{ .inputs.image }}"})
	publishWith, _ := structpbStruct(map[string]any{"digest": "{{ .tasks.sign.output.value }}"})
	inputs, _ := structpbStruct(map[string]any{"image": map[string]any{"type": "string", "required": true}})
	child := &orkstackv1.Orkflow{
		Name: name, Version: "0.1.0", Inputs: inputs,
		Outputs: map[string]string{
			"digest": "{{ .tasks.sign.output.value }}",
			"url":    "{{ .tasks.publish.output.value }}",
		},
		Tasks: []*orkstackv1.Task{
			{Id: "sign", Uses: e.alpine, Image: image, Command: []string{"/bin/sh", "-c", sign}, With: signWith,
				TimeoutSeconds: 900, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "publish", Uses: e.alpine, Image: image, Needs: []string{"sign"},
				Command: []string{"/bin/sh", "-c", publish}, With: publishWith,
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
	stampDigest(e.t, child)
	return child
}

// composedParent is composed-release over alpine, composing child.
func (e *eachE2E) composedParent(name string, child *orkstackv1.Orkflow) *orkstackv1.Orkflow {
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	build := `printf '{"value":"ghcr.io/acme/app:1.4"}' > /orkstack/output/values.json`
	announce := `v=$(cat /orkstack/input/upstream/release/values.json) && echo "released $v" && ` +
		`printf '{"seen":%s}' "$v" > /orkstack/output/values.json`
	releaseWith, _ := structpbStruct(map[string]any{"image": "{{ .tasks.build.output.value }}"})
	parent := &orkstackv1.Orkflow{
		Name: name, Version: "0.1.0",
		Outputs: map[string]string{"digest": "{{ .tasks.release.output.digest }}"},
		Tasks: []*orkstackv1.Task{
			{Id: "build", Uses: e.alpine, Image: image, Command: []string{"/bin/sh", "-c", build},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "release", Uses: "orkflow:" + child.GetName() + "@" + child.GetVersion(), Needs: []string{"build"},
				With: releaseWith, Orkflow: &orkstackv1.OrkflowRef{
					Name: child.GetName(), Version: child.GetVersion(), DefinitionDigest: child.GetDefinitionDigest()}},
			{Id: "announce", Uses: e.alpine, Image: image, Needs: []string{"release"},
				Command:        []string{"/bin/sh", "-c", announce},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
	stampDigest(e.t, parent)
	return parent
}

func stampDigest(t *testing.T, wf *orkstackv1.Orkflow) {
	t.Helper()
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatal(err)
	}
	wf.DefinitionDigest = digest
}

// startComposed starts parent with child planned under its composed task;
// it returns the child's run id.
func (e *eachE2E) startComposed(runID string, parent, child *orkstackv1.Orkflow) string {
	e.t.Helper()
	cp, err := engine.NewChildRunPlan(child, e.plan(), false, false)
	if err != nil {
		e.t.Fatal(err)
	}
	cp.RunID = runID + "-child"
	plan := e.plan()
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	if err := starter.StartRun(e.ctx, runID, parent, nil, plan); err != nil {
		e.t.Fatalf("StartRun: %v", err)
	}
	return cp.RunID
}

// awaitTaskStatus blocks, by deadline, until a run's task reads want.
func (e *eachE2E) awaitTaskStatus(runID, taskID string, want orkstackv1.Status, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if run, ok := e.runs.Get(runID); ok && taskRunNamed(run, taskID).GetStatus() == want {
			return
		}
		select {
		case <-e.ctx.Done():
			e.t.Fatalf("context ended waiting for %s/%s", runID, taskID)
		case <-time.After(100 * time.Millisecond):
		}
	}
	e.t.Fatalf("task %s of run %s never reached %v within %s", taskID, runID, want, within)
}

// awaitTaskLog blocks, by deadline, until a run's task has logged want. A
// task reads RUNNING once its activity starts, which can be before its
// container has run a line of its script; a test that signals the container
// and then reads what the script's trap recorded must first see the script
// running, or the signal can land before the trap exists.
func (e *eachE2E) awaitTaskLog(runID, taskID, want string, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if strings.Contains(e.taskLog(runID, taskID), want) {
			return
		}
		select {
		case <-e.ctx.Done():
			e.t.Fatalf("context ended waiting for %s/%s to log %q", runID, taskID, want)
		case <-time.After(100 * time.Millisecond):
		}
	}
	e.t.Fatalf("task %s of run %s never logged %q within %s", taskID, runID, want, within)
}

// childStarts counts the parent's StartChildWorkflowExecutionInitiated events.
func childStarts(h *history.History) int {
	n := 0
	for _, event := range h.GetEvents() {
		if event.GetStartChildWorkflowExecutionInitiatedEventAttributes() != nil {
			n++
		}
	}
	return n
}

// childLog is everything a child run's task wrote to stdout and stderr.
func (e *eachE2E) taskLog(runID, taskID string) string {
	e.t.Helper()
	var b strings.Builder
	for attempt := int32(1); attempt <= 3; attempt++ {
		chunks, err := e.logs.ReadLogChunks(e.ctx, artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: attempt}, 0, 0, 0)
		if err != nil {
			continue
		}
		for _, chunk := range chunks {
			b.Write(chunk.Data)
		}
	}
	return b.String()
}

func TestIntegration_ComposedRunEndToEnd(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i1")
	child := e.composedChild("release-block-i1", "")
	parent := e.composedParent("composed-release-i1", child)
	runID := fmt.Sprintf("ork-compose-i1-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent = %v %v", run.GetStatus(), run.GetError())
	}
	childRun := e.wait(childID)
	if childRun.GetId() != childID || childRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("child = %v", childRun)
	}
	if o := childRun.GetOrigin(); o.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN || o.GetId() != runID {
		t.Fatalf("child origin = %v", o)
	}
	if childStarts(e.history(childID)) != 0 || len(e.history(childID).GetEvents()) == 0 {
		t.Fatal("the child has no history of its own")
	}
	release := taskRunNamed(run, "release")
	if release.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || release.GetChild().GetRunId() != childID {
		t.Fatalf("release row = %v", release)
	}
	values := readValues(t, e, release.GetValuesRef())
	if !strings.Contains(values, `"digest":"sha256:`) || !strings.Contains(values, `"url":"https://registry.example/records/sha256:`) {
		t.Fatalf("release values = %s", values)
	}
	announce := readValues(t, e, taskRunNamed(run, "announce").GetValuesRef())
	digest := run.GetOutput().GetData().AsMap()["digest"]
	if digest == nil || !strings.Contains(announce, fmt.Sprint(digest)) {
		t.Fatalf("announce read %s; the run output digest is %v", announce, digest)
	}
	if out := run.GetOutput().GetData().AsMap(); len(out) != 1 {
		t.Fatalf("parent output = %v, want exactly {digest}", out)
	}
	proof.Write(t, "engine", "compose-I1.txt", []byte(fmt.Sprintf(
		"parent=%s child=%s child.origin=PARENT_RUN:%s\nrelease.values=%s\nannounce.values=%s\noutput.digest=%v\n",
		runID, childID, childRun.GetOrigin().GetId(), values, announce, digest)))
}

func TestIntegration_ComposedChildFailureFailsTheParent(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i2")
	child := e.composedChild("release-block-i2", `echo sign failed >&2; exit 1`)
	parent := e.composedParent("composed-release-i2", child)
	runID := fmt.Sprintf("ork-compose-i2-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	run := e.wait(runID)
	release := taskRunNamed(run, "release")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || release.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		release.GetError().GetCode() != engine.ChildRunFailedCode ||
		release.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Fatalf("parent %v, release %v", run.GetStatus(), release.GetError())
	}
	if childRun := e.wait(childID); childRun.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("child = %v", childRun.GetStatus())
	}
	proof.Write(t, "engine", "compose-I2.txt", []byte(fmt.Sprintf("parent=%s release=%s/%s: %s\n",
		runID, release.GetError().GetCode(), release.GetError().GetCategory(), release.GetError().GetMessage())))
}

// sleepUntilTerm is a child task that records SIGTERM on stdout and exits.
const sleepUntilTerm = `trap 'echo got SIGTERM; exit 143' TERM; echo sleeping; sleep 600 & wait`

func TestIntegration_CancelParentCancelsTheChildsContainer(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i3")
	child := e.composedChild("release-block-i3", sleepUntilTerm)
	parent := e.composedParent("composed-release-i3", child)
	runID := fmt.Sprintf("ork-compose-i3-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	e.awaitTaskStatus(childID, "sign", orkstackv1.Status_STATUS_RUNNING, 3*time.Minute)
	e.awaitTaskLog(childID, "sign", "sleeping", time.Minute)
	if err := e.temporal.CancelWorkflow(e.ctx, runID, ""); err != nil {
		t.Fatalf("cancel parent: %v", err)
	}
	run := e.wait(runID)
	childRun := e.wait(childID)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || childRun.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("parent %v, child %v", run.GetStatus(), childRun.GetStatus())
	}
	if run.GetFinishedAt().AsTime().Before(childRun.GetFinishedAt().AsTime()) {
		t.Fatalf("the parent finished (%v) before its child (%v)", run.GetFinishedAt().AsTime(), childRun.GetFinishedAt().AsTime())
	}
	logged := e.taskLog(childID, "sign")
	if !strings.Contains(logged, "got SIGTERM") {
		t.Fatalf("the child's container did not record SIGTERM: %q", logged)
	}
	proof.Write(t, "engine", "compose-I3.txt", []byte(fmt.Sprintf("parent=%s CANCELED at %v; child=%s CANCELED at %v; sign log: %q\n",
		runID, run.GetFinishedAt().AsTime(), childID, childRun.GetFinishedAt().AsTime(), logged)))
}

func TestIntegration_CancelChildCancelsItsParent(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i4")
	child := e.composedChild("release-block-i4", sleepUntilTerm)
	parent := e.composedParent("composed-release-i4", child)
	runID := fmt.Sprintf("ork-compose-i4-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	e.awaitTaskStatus(childID, "sign", orkstackv1.Status_STATUS_RUNNING, 3*time.Minute)
	if err := e.temporal.CancelWorkflow(e.ctx, childID, ""); err != nil {
		t.Fatalf("cancel child: %v", err)
	}
	run := e.wait(runID)
	release := taskRunNamed(run, "release")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || release.GetError().GetCode() != engine.ChildRunCanceledCode ||
		!strings.Contains(run.GetError().GetMessage(), childID) {
		t.Fatalf("parent %v %v, release %v", run.GetStatus(), run.GetError(), release.GetError())
	}
	proof.Write(t, "engine", "compose-I4.txt", []byte(fmt.Sprintf("child=%s canceled; parent=%s %s: %s\n",
		childID, runID, run.GetError().GetCode(), run.GetError().GetMessage())))
}

func TestIntegration_TerminatedParentCancelsItsChild(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i5")
	child := e.composedChild("release-block-i5", sleepUntilTerm)
	parent := e.composedParent("composed-release-i5", child)
	runID := fmt.Sprintf("ork-compose-i5-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	e.awaitTaskStatus(childID, "sign", orkstackv1.Status_STATUS_RUNNING, 3*time.Minute)
	e.awaitTaskLog(childID, "sign", "sleeping", time.Minute)
	if err := e.temporal.TerminateWorkflow(e.ctx, runID, "", "composed-orkflows I5"); err != nil {
		t.Fatalf("terminate parent: %v", err)
	}
	childRun := e.wait(childID)
	if childRun.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("child = %v %v", childRun.GetStatus(), childRun.GetError())
	}
	logged := e.taskLog(childID, "sign")
	if !strings.Contains(logged, "got SIGTERM") {
		t.Fatalf("the child's container did not record SIGTERM: %q", logged)
	}
	described, err := e.temporal.DescribeWorkflowExecution(e.ctx, childID, "")
	if err != nil {
		t.Fatal(err)
	}
	if status := described.GetWorkflowExecutionInfo().GetStatus(); status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatal("the child is still running after its parent was terminated")
	}
	proof.Write(t, "engine", "compose-I5.txt", []byte(fmt.Sprintf(
		"parent=%s terminated; child=%s %v (cluster status %v); sign log: %q\n",
		runID, childID, childRun.GetStatus(), described.GetWorkflowExecutionInfo().GetStatus(), logged)))
}

func TestIntegration_EngineRestartMidChildStartsOneChild(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i6")
	child := e.composedChild("release-block-i6", `sleep 5; d=$(sha256sum /orkstack/input/config.json | cut -d' ' -f1) && `+
		`printf '{"value":"sha256:%s"}' "$d" > /orkstack/output/values.json`)
	parent := e.composedParent("composed-release-i6", child)
	runID := fmt.Sprintf("ork-compose-i6-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	e.awaitTaskStatus(childID, "sign", orkstackv1.Status_STATUS_RUNNING, 3*time.Minute)
	e.mu.Lock()
	old := e.runWorker
	e.mu.Unlock()
	old.Stop()
	e.startRunWorker()
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent = %v %v", run.GetStatus(), run.GetError())
	}
	if n := childStarts(e.history(runID)); n != 1 {
		t.Fatalf("the parent's history holds %d child starts, want exactly 1", n)
	}
	proof.Write(t, "engine", "compose-I6.txt", []byte(fmt.Sprintf("parent=%s child starts in history=1\n", runID)))
}

func TestIntegration_ChildArchivedWithItsOwnIndexAndOrigin(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-i7")
	child := e.composedChild("release-block-i7", "")
	parent := e.composedParent("composed-release-i7", child)
	runID := fmt.Sprintf("ork-compose-i7-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	run := e.wait(runID)
	childRun := e.wait(childID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || childRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent %v child %v", run.GetStatus(), childRun.GetStatus())
	}
	store, err := artifacts.NewRunArchive(e.cfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	archiver := engine.NewRunArchiver(store)
	// Each run is archived by its own terminal event's snapshot, as the
	// projection does.
	for _, id := range []string{childID, runID} {
		snapshot, ok := e.runs.Get(id)
		if !ok {
			t.Fatalf("no snapshot of %s", id)
		}
		if err := archiver.Archive(e.ctx, snapshot, e.bus.Events(id, 0)); err != nil {
			t.Fatalf("archive %s: %v", id, err)
		}
	}
	children, err := archiver.ListRuns(e.ctx, engine.ArchiveQuery{
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: runID,
	})
	if err != nil || len(children) != 1 || children[0].GetId() != childID {
		t.Fatalf("archived children of %s = %v (%v)", runID, children, err)
	}
	ref := taskRunNamed(run, "release").GetValuesRef()
	objectID, ok := artifacts.ObjectIDFromURI(ref.GetUri())
	if !ok {
		t.Fatalf("the composed task's values ref %s has no object id", ref.GetUri())
	}
	holders, err := archiver.ObjectReferences(e.ctx, objectID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, holder := range holders {
		found = found || holder == runID
	}
	if !found {
		t.Fatalf("the composed task's values.json has no marker under the parent: %v", holders)
	}
	proof.Write(t, "engine", "compose-I7.txt", []byte(fmt.Sprintf(
		"parent=%s child=%s origin-index=parent-run/%s; values object %s held by %v\n",
		runID, childID, runID, objectID, holders)))
}

// The child-on-its-parent's-version test lives in
// versioning_compose_integration_test.go.

// A submission the deployment can no longer plan is refused, and nothing
// is started.
func TestIntegration_ComposedSubmitRefusalsMintNothing(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)
	starter := engine.NewTemporalStarter(temporalClient, fmt.Sprintf("ork-compose-i9-%d", time.Now().UnixNano()))
	reader := engine.NewTemporalRunReader(temporalClient)
	store := engine.NewMemoryOrkflowStore()
	var ids int
	service := func(maxDepth int32, pools engine.PoolProbe) *api.Service {
		return api.NewService(api.Config{
			Orkflows: store, Starter: starter, Reader: reader, ComposeMaxDepth: maxDepth, Pools: pools,
			NewRunID: func() string { ids++; return fmt.Sprintf("ork-compose-i9-%d-%d", time.Now().UnixNano(), ids) },
		})
	}
	full := service(0, nil)
	apply := func(yaml string) {
		t.Helper()
		if _, err := full.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-leaf\nversion: \"1\"\ntasks:\n  - id: x\n    uses: shell\n    with: {op: exec, command: [\"true\"]}\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-gpu\nversion: \"1\"\ntasks:\n  - id: x\n    uses: shell\n    pool: gpu\n    with: {op: exec, command: [\"true\"]}\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-mid\nversion: \"1\"\ntasks:\n  - id: leaf\n    uses: orkflow:i9-leaf@1\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-deep\nversion: \"1\"\ntasks:\n  - id: mid\n    uses: orkflow:i9-mid@1\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-pool\nversion: \"1\"\ntasks:\n  - id: g\n    uses: orkflow:i9-gpu@1\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: i9-gone\nversion: \"1\"\ntasks:\n  - id: leaf\n    uses: orkflow:i9-leaf@1\n")

	before, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	var lines []string
	submit := func(svc *api.Service, name string, code connect.Code, want string) {
		t.Helper()
		_, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
		if connect.CodeOf(err) != code || !strings.Contains(err.Error(), want) {
			t.Fatalf("submit %s = %v; want %v containing %q", name, err, code, want)
		}
		lines = append(lines, fmt.Sprintf("%s: %v", name, err))
	}
	// A lowered cap.
	submit(service(1, nil), "i9-deep", connect.CodeFailedPrecondition, "ORK_COMPOSE_MAX_DEPTH was lowered")
	// A child pool with no Orkers.
	submit(service(0, emptyGPU{}), "i9-pool", connect.CodeFailedPrecondition, "child orkflow i9-gpu@1")
	// A deleted child.
	if err := store.Delete(ctx, "", "i9-leaf"); err != nil {
		t.Fatal(err)
	}
	submit(full, "i9-gone", connect.CodeFailedPrecondition, "no longer exists")
	after, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("refused submissions left %d runs behind", len(after)-len(before))
	}
	proof.Write(t, "engine", "compose-I9.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

// emptyGPU answers the gpu pool with no Orkers and every other pool live.
type emptyGPU struct{}

func (emptyGPU) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	if strings.Contains(queue, "gpu") {
		return engine.PoolLiveness{}, nil
	}
	return engine.PoolLiveness{Orkers: 1, LastSeen: time.Now()}, nil
}

// Opt-in recorder of the composed fixtures.
func TestIntegration_RecordComposedRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + composedParentHistory + " and " + composedChildHistory +
			" — the committed fixtures are meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-compose-rec-run-" + suffix
	orkerQueue := "ork-compose-rec-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-compose-rec-" + suffix, BuildID: "v1"}

	values := &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}}
	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	compose := engine.NewComposeActivities(values)
	runWorker.RegisterActivityWithOptions(compose.RenderComposedValues,
		activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	runWorker.RegisterActivityWithOptions(compose.StoreComposedValues,
		activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	var mu sync.Mutex
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	}))
	orkerWorker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		defer mu.Unlock()
		value := map[string]any{"value": "fixture-" + in.GetTaskId()}
		if in.GetTaskId() == "sign" {
			value["value"] = "sha256:" + strings.Repeat("ab", 32)
		}
		raw := valuesJSON(value)
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
			ValuesJson: raw, ValuesInlined: true,
			ValuesRef: values.put("mem://values/"+in.GetRunId()+"/"+in.GetTaskId(), raw)}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)

	// The shipped composition examples, as apply would store them: the child's digest
	// stamped into the parent's composed task.
	child := exampleOrkflow(t, "release-block", "0.1.0")
	parent := exampleOrkflow(t, "composed-release", "0.1.0")
	runID := fmt.Sprintf("ork-compose-fixture-%d", time.Now().UnixNano())
	childPlan := rawPlan(orkerQueue)
	childPlan.CommitRoot = "mem://commits"
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = runID + "-child"
	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	if err := starter.StartRun(ctx, runID, parent, nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("fixture run %s did not succeed: %v", runID, err)
	}
	for path, id := range map[string]string{composedParentHistory: runID, composedChildHistory: cp.RunID} {
		recorded := &history.History{}
		iter := temporalClient.GetWorkflowHistory(ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				t.Fatalf("read history of %s: %v", id, err)
			}
			recorded.Events = append(recorded.Events, event)
		}
		encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
		if err != nil {
			t.Fatalf("encode history: %v", err)
		}
		if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
			len(recorded.Events), id, path, deployment)
	}
}

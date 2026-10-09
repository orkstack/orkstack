//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_ComposedChildRunsOnItsParentsVersion is the observation the
// composed-child versioning rule (docs/DEPLOY.md §2) rests on, not the
// assumption. A parent starts
// while v1 is current and holds at its gate while v2 is promoted; only then
// does it reach its composed task and start the child. The child must start
// on v1 — the version of the Pinned parent that started it, whose release
// built the child's plan — and every task of the child must execute on v1's
// Orker, though v2 is current by then.
func TestIntegration_ComposedChildRunsOnItsParentsVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-compose-rollout-" + suffix
	runQueue := "ork-compose-rollout-run-" + suffix
	orkerQueue := "ork-compose-rollout-tasks-" + suffix
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	snapshots := newTestSnapshotStore()
	values := &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}}
	registerCompose := func(w worker.Worker) {
		compose := engine.NewComposeActivities(values)
		w.RegisterActivityWithOptions(compose.RenderComposedValues,
			activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
		w.RegisterActivityWithOptions(compose.StoreComposedValues,
			activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	}
	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)

	first := newReplicaWith(t, c, v1, runQueue, orkerQueue, release, snapshots, registerCompose)
	defer first.stop()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	transcript.step("v1 workers up; current version = v1")

	child := &orkstackv1.Orkflow{
		Name: "compose-rollout-child", Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "child-first", Uses: "shell", TimeoutSeconds: 120, With: mustStruct(map[string]any{"run": "echo one"})},
			{Id: "child-second", Uses: "shell", Needs: []string{"child-first"}, TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo two"})},
		},
	}
	stampDigest(t, child)
	parent := &orkstackv1.Orkflow{
		Name: "compose-rollout-parent", Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: gateTaskID, Uses: "shell", TimeoutSeconds: 200, With: mustStruct(map[string]any{"run": "echo gate"})},
			{Id: "release", Uses: "orkflow:compose-rollout-child@1.0.0", Needs: []string{gateTaskID},
				Orkflow: &orkstackv1.OrkflowRef{Name: child.GetName(), Version: child.GetVersion(),
					DefinitionDigest: child.GetDefinitionDigest()}},
		},
	}
	stampDigest(t, parent)
	runID := "ork-compose-rollout-" + suffix
	cp, err := engine.NewChildRunPlan(child, rawPlan(orkerQueue), false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = runID + "-child"
	plan := rawPlan(orkerQueue)
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	if err := starter.StartRun(ctx, runID, parent, nil, plan); err != nil {
		t.Fatalf("start the parent: %v", err)
	}
	transcript.step("submitted parent " + runID + " while v1 was current")
	select {
	case <-first.started:
	case <-ctx.Done():
		t.Fatalf("the parent never reached its gate: %v", ctx.Err())
	}
	transcript.step("parent " + runID + " is at its gate on v1; its child has not started")

	second := newReplicaWith(t, c, v2, runQueue, orkerQueue, release, snapshots, registerCompose)
	defer second.stop()
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue)
	transcript.step("v2 workers up; current version = v2")

	releaseGate()
	transcript.step("released the gate: the parent now starts its child, after the switch")
	if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the parent %s did not complete: %v", runID, err)
	}
	if err := c.GetWorkflow(ctx, cp.RunID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the child %s did not complete: %v", cp.RunID, err)
	}
	assertRunVersion(t, ctx, c, transcript, runID, v1)
	assertRunVersion(t, ctx, c, transcript, cp.RunID, v1)
	builds := assertTaskBuilds(t, ctx, c, transcript, cp.RunID, "v1")
	if len(builds) != 2 {
		t.Fatalf("the child ran %d tasks, want 2: %v", len(builds), builds)
	}
	var lines []string
	for taskID, build := range builds {
		lines = append(lines, fmt.Sprintf("child %s task %s build_id=%s", cp.RunID, taskID, build))
	}
	for key, build := range second.ranTasks() {
		if strings.HasPrefix(key, cp.RunID+"/") {
			t.Fatalf("v2's Orker ran %s (%s): the child moved to the new version", key, build)
		}
	}
	transcript.result(strings.Join(lines, "\n         "))
	transcript.logOnly(t)
	proof.Write(t, "engine", "compose-I8.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

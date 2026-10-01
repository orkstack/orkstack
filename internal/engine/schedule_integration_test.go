//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_ScheduleFireDuringVersionRollout: a fire open on v1 while
// v2 becomes the Current Version finishes its run on v1 — the fire is Pinned
// and its child run is pinned with it — and the next fire, started after the
// switch, runs on v2. The schedule, its fires and the runs are the real
// engine code against a real cluster; only PrepareScheduledRun is a stub that
// hands back a fixed preparation (the API is not under test here).
func TestIntegration_ScheduleFireDuringVersionRollout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-sched-rollout-" + suffix
	runQueue := "ork-sched-run-" + suffix
	orkerQueue := "ork-sched-tasks-" + suffix
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	snapshots := newTestSnapshotStore()
	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)

	// The stub preparation: the first fire's run is gated, every later one
	// plain. Run ids are recorded in order.
	var mu sync.Mutex
	var runIDs []string
	prepare := func(_ context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		mu.Lock()
		defer mu.Unlock()
		wf := plainOrkflow()
		if len(runIDs) == 0 {
			wf = gatedOrkflow()
		}
		wf.Namespace = "default"
		runID := fmt.Sprintf("ork-sched-rollout-%d-%s", len(runIDs)+1, suffix)
		runIDs = append(runIDs, runID)
		plan := rawPlan(orkerQueue)
		plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: rec.GetScheduleId(),
			Reference: at.UTC().Format(time.RFC3339), Description: rec.GetName()}
		encoded, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		return &orkstackv1.ScheduledRunPreparation{
			Run: &orkstackv1.Run{Id: runID, Namespace: "default", OrkflowName: wf.GetName(), OrkflowVersion: wf.GetVersion(),
				Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.Now()},
			Orkflow: wf,
			Plan:    encoded,
		}, nil
	}
	register := func(w worker.Worker) {
		w.RegisterActivityWithOptions(prepare, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	}
	runID := func(i int) string {
		mu.Lock()
		defer mu.Unlock()
		if i < len(runIDs) {
			return runIDs[i]
		}
		return ""
	}

	first := newReplicaWith(t, c, v1, runQueue, orkerQueue, release, snapshots, register)
	defer first.stop()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	transcript.step("v1 workers up; current version = v1")

	scheduler := engine.NewTemporalScheduler(c, runQueue)
	name := "rollout-" + suffix
	rec := &orkstackv1.ScheduleRecord{
		ScheduleId: "sch-00000000" + fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff),
		Name:       name, Namespace: "default", OrkflowName: "rollout-gated",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL},
	}
	if _, err := scheduler.Create(ctx, rec, true); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	defer func() { _ = scheduler.Delete(context.Background(), "default", name) }()
	if err := scheduler.Fire(ctx, "default", name, orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED); err != nil {
		t.Fatalf("fire: %v", err)
	}
	transcript.step("fired the schedule while v1 was current")
	select {
	case <-first.started:
	case <-ctx.Done():
		t.Fatalf("the first fire's run never reached its gate: %v", ctx.Err())
	}
	pinnedRun := runID(0)
	transcript.step("run " + pinnedRun + " is in flight: its gate task is executing on v1")

	second := newReplicaWith(t, c, v2, runQueue, orkerQueue, release, snapshots, register)
	defer second.stop()
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue)
	transcript.step("v2 workers up; current version = v2, while " + pinnedRun + " is still executing")

	if err := scheduler.Fire(ctx, "default", name, orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED); err != nil {
		t.Fatalf("second fire: %v", err)
	}
	transcript.step("fired the schedule again after the switch")
	deadline := time.Now().Add(60 * time.Second)
	for runID(1) == "" && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	newRun := runID(1)
	if newRun == "" {
		t.Fatal("the second fire never prepared a run")
	}
	if err := c.GetWorkflow(ctx, newRun, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run of the fire after the switch (%s) did not complete: %v", newRun, err)
	}
	releaseGate()
	if err := c.GetWorkflow(ctx, pinnedRun, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run of the fire opened on v1 (%s) did not complete: %v", pinnedRun, err)
	}
	transcript.step("both runs completed")

	assertRunVersion(t, ctx, c, transcript, pinnedRun, v1)
	assertRunVersion(t, ctx, c, transcript, newRun, v2)
	assertTaskBuilds(t, ctx, c, transcript, pinnedRun, "v1")
	assertTaskBuilds(t, ctx, c, transcript, newRun, "v2")

	fires := scheduleFireIDs(t, ctx, c, rec.GetScheduleId(), 2)
	for i, fire := range fires {
		want := []task.Deployment{v1, v2}[i]
		assertRunVersion(t, ctx, c, transcript, fire, want)
	}
	proof.Write(t, "engine", "schedule-fire-rollout-transcript.txt", []byte(strings.Join(transcript.lines, "\n")+"\n"))
}

// scheduleFireIDs lists a schedule's fire workflows, oldest first, waiting for
// visibility to show n of them.
func scheduleFireIDs(t *testing.T, ctx context.Context, c interface {
	ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
}, scheduleID string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query: fmt.Sprintf("WorkflowType = '%s'", engine.ScheduleFireWorkflowType),
		})
		if err != nil {
			t.Fatalf("list fire workflows: %v", err)
		}
		type fire struct {
			id    string
			start time.Time
		}
		var fires []fire
		for _, info := range resp.GetExecutions() {
			if strings.HasPrefix(info.GetExecution().GetWorkflowId(), engine.ScheduleFireWorkflowIDPrefix+scheduleID) &&
				info.GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
				fires = append(fires, fire{info.GetExecution().GetWorkflowId(), info.GetStartTime().AsTime()})
			}
		}
		if len(fires) >= n {
			sort.Slice(fires, func(i, j int) bool { return fires[i].start.Before(fires[j].start) })
			ids := make([]string, 0, len(fires))
			for _, f := range fires {
				ids = append(ids, f.id)
			}
			return ids
		}
		if time.Now().After(deadline) {
			t.Fatalf("visibility listed %d fires of %s, want %d", len(fires), scheduleID, n)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

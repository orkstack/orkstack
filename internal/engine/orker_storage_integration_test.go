//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_OrkerMaxConcurrentBoundsAFanOut is F-orker-storage-perf-1's
// end-to-end half: an Orker booted with orker.max_concurrent=2, its task
// worker built with the Orker's own Tuner exactly as cmd/orker builds it,
// serving a 4-item fan-out with no max_parallel, never has more than two
// ExecuteTask attempts in flight — every item is dispatched at once, and only
// the ceiling decides how many this Orker takes.
func TestIntegration_OrkerMaxConcurrentBoundsAFanOut(t *testing.T) {
	e := startEachE2E(t, true)
	refs, _ := e.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(e.store, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs,
		Pool: task.DefaultPool, OrkerID: proof.OrkerID("ceiling-1"), WorkRoot: e.workRoot,
		MaxConcurrent: 2,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	t.Cleanup(activities.Close)
	tuner, err := activities.WorkerTuner()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	inFlight, peak, executed := 0, 0, 0
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true, Tuner: tuner,
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		inFlight++
		executed++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}()
		return activities.ExecuteTask(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	described, err := activities.DescribeOrker(e.ctx)
	if err != nil || described.GetMaxConcurrent() != 2 {
		t.Fatalf("DescribeOrker max_concurrent = %d (%v); want 2", described.GetMaxConcurrent(), err)
	}

	runID := fmt.Sprintf("ork-ceiling-%d", time.Now().UnixNano())
	// Each item sleeps long enough that an unbounded Orker would run all four
	// at once; max_parallel 0 is "absent".
	e.start(runID, e.eachDefinition("ceiling", 4, 0, "sleep 2 && "))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	mu.Lock()
	defer mu.Unlock()
	if peak > 2 {
		t.Fatalf("peak in-flight ExecuteTask = %d; the ceiling is 2", peak)
	}
	if peak < 2 {
		t.Fatalf("peak in-flight ExecuteTask = %d; four sleeping items never overlapped, so the bound was not exercised", peak)
	}
	proof.Write(t, "engine", "orker-max-concurrent.txt",
		[]byte(fmt.Sprintf("max_concurrent=2 items=4 max_parallel=absent executed=%d peak_in_flight=%d\n", executed, peak)))
}

// countingGetStore counts Gets per URI in front of the real store.
type countingGetStore struct {
	artifacts.Store
	mu   sync.Mutex
	gets map[string]int
}

func (s *countingGetStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets[uri]++
	s.mu.Unlock()
	return s.Store.Get(ctx, uri)
}

// TestIntegration_FanOutDownloadsASharedInputOncePerOrker is
// F-orker-storage-perf-2 end to end: 20 items of a fan-out, all running at
// once on one Orker and all staging the same upstream artifact, fetch it from
// MinIO once — the staging cache links one verified download into every
// item's input tree.
func TestIntegration_FanOutDownloadsASharedInputOncePerOrker(t *testing.T) {
	e := startEachE2E(t, true)
	counting := &countingGetStore{Store: e.store, gets: map[string]int{}}
	refs, _ := e.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(counting, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs,
		Pool: task.DefaultPool, OrkerID: proof.OrkerID("stage-share-1"), WorkRoot: e.workRoot,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	t.Cleanup(activities.Close)
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(activities.ExecuteTask, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	const items = 20
	parts := make([]string, items)
	for i := range parts {
		parts[i] = fmt.Sprintf(`{"name":"p%d"}`, i)
	}
	seed := fmt.Sprintf(`head -c 1048576 /dev/zero > /orkstack/output/artifacts/shared.bin && `+
		`printf '%%s' '{"parts":[%s]}' > /orkstack/output/values.json`, strings.Join(parts, ","))
	// Every item holds its attempt open long enough that all twenty overlap.
	convert := `test "$(wc -c < /orkstack/input/artifacts/seed/shared.bin)" -eq 1048576 && sleep 3 && ` +
		`printf '{}' > /orkstack/output/values.json`
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	wf := &orkstackv1.Orkflow{
		Name: "stage-share", Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "seed", Uses: e.alpine, Image: image, Command: []string{"/bin/sh", "-c", seed},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "convert", Uses: e.alpine, Image: image, Needs: []string{"seed"},
				Command:        []string{"/bin/sh", "-c", convert},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
				Each: &orkstackv1.EachSpec{Over: "{{ .tasks.seed.output.parts }}", As: "part", MaxItems: 50,
					Source: &orkstackv1.EachSource{TaskId: "seed", Path: []string{"parts"}}}},
		},
	}
	runID := fmt.Sprintf("ork-stage-share-%d", time.Now().UnixNano())
	e.start(runID, wf)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	shared := taskRunNamed(run, "seed").GetArtifacts()
	if len(shared) != 1 || shared[0].GetName() != "shared.bin" {
		t.Fatalf("seed artifacts = %v", shared)
	}
	counting.mu.Lock()
	gets := counting.gets[shared[0].GetUri()]
	counting.mu.Unlock()
	if gets != 1 {
		t.Fatalf("%d items fetched the shared artifact %d times; want once per Orker", items, gets)
	}
	proof.Write(t, "engine", "fanout-shared-input-gets.txt",
		[]byte(fmt.Sprintf("items=%d orkers=1 shared_artifact_gets=%d uri=%s\n", items, gets, shared[0].GetUri())))
}

//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// capsFanOut declares 5,000 items over its run input, so its submit needs
// the each-items-10k capability.
const capsFanOut = `apiVersion: orkstack/v1
kind: Orkflow
name: caps-wide
version: "1.0.0"
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: busybox:latest
    each: {over: "{{ .inputs.parts }}", max_items: 5000}
    with: {op: exec}
`

// describePollers is the cluster's own poller list for one channel of ns.
func describePollers(ctx context.Context, c client.Client, ns, queue string, kind enumspb.TaskQueueType) ([]*taskqueuepb.PollerInfo, error) {
	resp, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     ns,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: queue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: kind,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetPollers(), nil
}

// waitForPoller polls ns's queue until a poller satisfying match appears,
// within a deadline (never a sleep), and returns it.
func waitForPoller(t *testing.T, ctx context.Context, c client.Client, ns, queue string, kind enumspb.TaskQueueType,
	within time.Duration, match func(*taskqueuepb.PollerInfo) bool,
) *taskqueuepb.PollerInfo {
	t.Helper()
	deadline := time.Now().Add(within)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	var last []*taskqueuepb.PollerInfo
	for time.Now().Before(deadline) {
		pollers, err := describePollers(ctx, c, ns, queue, kind)
		if err == nil {
			last = pollers
			for _, p := range pollers {
				if match(p) {
					return p
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	t.Fatalf("no poller of %s (%v) matched within %s; saw %v", queue, kind, within, last)
	return nil
}

// TestIntegration_EngineIdentityAdvertisesCapabilities: a real ork process's
// pollers carry its capability tag in the cluster's own poller list, on the
// engine channel and on the projection channel; a second worker the test owns,
// polling the engine channel with an identity lacking the tag, makes a
// 5,000-item submit refused, naming a count; once the test stops that worker
// and its record has aged out of a freshness window the test configures
// short, the same submit is accepted.
func TestIntegration_EngineIdentityAdvertisesCapabilities(t *testing.T) {
	temporal := requireTemporal(t)
	cfg := requireMinIO(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	ns := fmt.Sprintf("%s-caps-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	transcript := &strings.Builder{}
	note := func(format string, args ...any) {
		fmt.Fprintf(transcript, format+"\n", args...)
	}
	note("# engine capabilities and the fleet check, on namespace %s", ns)

	binary := buildOrk(t)
	_, _, _ = bootEngine(ctx, t, binary, temporal, "engine-caps", "ORK_TEMPORAL_NAMESPACE="+ns)
	pid := engines["engine-caps"].cmd.Process.Pid

	control, err := client.DialContext(ctx, client.Options{HostPort: temporal, Namespace: ns, Identity: "caps-control"})
	if err != nil {
		t.Fatalf("dial %s: %v", temporal, err)
	}
	defer control.Close()

	ours := func(p *taskqueuepb.PollerInfo) bool {
		return strings.HasPrefix(p.GetIdentity(), strconv.Itoa(pid)+"@")
	}
	runPoller := waitForPoller(t, ctx, control, ns, task.DefaultQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, time.Minute, ours)
	eventPoller := waitForPoller(t, ctx, control, ns, engine.EventQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, time.Minute, ours)
	for _, p := range []*taskqueuepb.PollerInfo{runPoller, eventPoller} {
		if !strings.HasSuffix(p.GetIdentity(), " ork-caps="+strings.Join(engine.EngineCapabilities, ",")) {
			t.Fatalf("the ork process polls as %q, want its identity to end in ork-caps=%s",
				p.GetIdentity(), strings.Join(engine.EngineCapabilities, ","))
		}
		caps := engine.IdentityCapabilities(p.GetIdentity())
		if len(caps) == 0 || caps[0] != engine.CapabilityEachItems10k {
			t.Fatalf("parsed %v from %q", caps, p.GetIdentity())
		}
	}
	note("$ temporal task-queue describe --task-queue %s --task-queue-type workflow", task.DefaultQueue)
	note("  identity: %s", runPoller.GetIdentity())
	note("$ temporal task-queue describe --task-queue %s --task-queue-type activity", engine.EventQueue)
	note("  identity: %s", eventPoller.GetIdentity())

	// The fleet check, over the same cluster, with a freshness window short
	// enough that a stopped worker ages out within the test.
	const freshness = 15 * time.Second
	cluster, err := engine.NewTemporalReleaseCluster(control, nil, ns, task.DefaultDeploymentName, task.DefaultQueue, "")
	if err != nil {
		t.Fatal(err)
	}
	cluster.Freshness = freshness
	var logs bytes.Buffer
	svc := api.NewService(api.Config{
		Orkflows:            engine.NewMemoryOrkflowStore(),
		Starter:             engine.NewTemporalStarter(control, task.DefaultQueue),
		Artifacts:           cfg,
		EachMaxItems:        5000,
		ContinuationEnabled: true,
		Capabilities:        cluster,
		Logger:              slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: capsFanOut})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	submit := func() (*orkstackv1.SubmitRunResponse, error) {
		data, err := structpb.NewStruct(map[string]any{"parts": []any{"a", "b"}})
		if err != nil {
			t.Fatal(err)
		}
		input := &orkstackv1.Payload{Data: data}
		resp, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "caps-wide", Input: input}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	// An older process: a workflow poller on the engine channel whose
	// identity advertises nothing. It registers one workflow no run uses.
	const lackingIdentity = "4242@older-engine@"
	older := worker.New(control, task.DefaultQueue, worker.Options{Identity: lackingIdentity})
	older.RegisterWorkflowWithOptions(func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: "caps-older-engine"})
	if err := older.Start(); err != nil {
		t.Fatalf("start the older engine's worker: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			older.Stop()
		}
	}()
	// Fresh: its first poll's record, within the window.
	waitForPoller(t, ctx, control, ns, task.DefaultQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, time.Minute,
		func(p *taskqueuepb.PollerInfo) bool {
			return p.GetIdentity() == lackingIdentity && time.Since(p.GetLastAccessTime().AsTime()) < freshness/3
		})
	_, err = submit()
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "1 engine process runs a release") {
		t.Fatalf("submit while an older process polls = %v; want failed_precondition counting 1 engine process", err)
	}
	if strings.Contains(err.Error(), "older-engine") || strings.Contains(err.Error(), "4242") {
		t.Fatalf("the refusal %q names an identity", err.Error())
	}
	if !strings.Contains(logs.String(), lackingIdentity) {
		t.Fatalf("the engine's warning does not name the identity:\n%s", logs.String())
	}
	note("$ submit caps-wide (5000 items) while %s polls", lackingIdentity)
	note("  %v", err)

	older.Stop()
	stopped = true
	stoppedAt := time.Now()
	deadline := time.Now().Add(3 * time.Minute)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		lacking, err := cluster.MissingCapability(ctx, engine.CapabilityEachItems10k, engine.ScopeWorkflow)
		if err == nil && len(lacking) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stopped worker never aged out of the %s window: %v %v", freshness, lacking, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	note("  the older worker stopped; aged out of the %s window after %s", freshness, time.Since(stoppedAt).Round(time.Second))
	accepted, err := submit()
	if err != nil {
		t.Fatalf("submit once every process has the capability: %v", err)
	}
	defer func() {
		_ = control.TerminateWorkflow(context.Background(), accepted.GetRun().GetId(), "", "test cleanup")
	}()
	note("$ submit caps-wide (5000 items) again")
	note("  accepted: run %s", accepted.GetRun().GetId())
	proof.Write(t, "ork", "engine-capabilities.txt", []byte(transcript.String()))
}

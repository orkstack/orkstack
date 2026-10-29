//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_DescribeOrkerTravelsThroughTemporalToTheRealOrkerActivity
// is the end-to-end seam submit's pool checks read (engine.ReadFleet): the
// engine finds the Orker on the presence channel, schedules the named
// activity on that Orker's own control channel, and receives the identity
// produced by the real Orker Activities implementation. A direct method call
// would not prove registration, routing, or serialization.
func TestIntegration_DescribeOrkerTravelsThroughTemporalToTheRealOrkerActivity(t *testing.T) {
	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		infraAbsent(t, "docker is not reachable: %v: %s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := dialVersioningCluster(t, ctx, poolRoutingNamespace())

	pool := fmt.Sprintf("describe-%d", os.Getpid())
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool: %v", err)
	}
	policy := orker.DefaultSecurityPolicy()
	activities := orker.NewActivitiesWithOptions(nil, orker.Options{
		Runtime:       orker.NewDockerRuntime(),
		Policy:        &policy,
		OrkerID:       "orker-describe-proof",
		BuildID:       "build-describe-proof",
		Pool:          pool,
		MaxConcurrent: 7,
	})

	control, presence, err := orker.NewChannelWorkers(c, activities)
	if err != nil {
		t.Fatalf("channel workers: %v", err)
	}
	for _, w := range []worker.Worker{control, presence} {
		if err := w.Start(); err != nil {
			t.Fatalf("start Orker channel worker: %v", err)
		}
		defer w.Stop()
	}

	// The presence channel lists a poller once its first poll arrives: ask
	// again, by deadline.
	starter := engine.NewTemporalStarter(c, task.DefaultQueue)
	var result *orkstackv1.DescribeOrkerResult
	for deadline := time.Now().Add(time.Minute); result == nil; {
		orkers, err := engine.ReadFleet(ctx, starter)
		if err != nil {
			t.Fatalf("read the orker inventory: %v", err)
		}
		for _, o := range orkers {
			if o.Presence.Identity != "orker-describe-proof" {
				continue
			}
			if o.Err != nil {
				t.Fatalf("DescribeOrker on the Orker's own channel: %v", o.Err)
			}
			result = o.Described
		}
		if result != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("orker-describe-proof never appeared on the presence channel (pool %s, channel %s)", pool, queue)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	if result.GetOrkerId() != "orker-describe-proof" ||
		result.GetBuildId() != "build-describe-proof" ||
		result.GetPool() != pool || result.GetMaxConcurrent() != 7 {
		t.Fatalf("DescribeOrker result = %v; identity did not survive the Temporal round trip", result)
	}
	if result.GetRuntimeName() != "docker" || result.GetRuntimeVersion() == "" {
		t.Fatalf("runtime identity = %q %q, want live Docker details", result.GetRuntimeName(), result.GetRuntimeVersion())
	}
	if !strings.Contains(result.GetSecurityProfile(), "no-new-privileges") {
		t.Fatalf("security profile = %q, want the enforced posture", result.GetSecurityProfile())
	}
	t.Logf("DescribeOrker answered through Temporal: orker_id=%s build_id=%s pool=%s runtime=%s %s security=%s max_concurrent=%d",
		result.GetOrkerId(), result.GetBuildId(), result.GetPool(), result.GetRuntimeName(),
		result.GetRuntimeVersion(), result.GetSecurityProfile(), result.GetMaxConcurrent())
}

// startStandInOrker is an Orker of an older build as submit's probes meet it:
// it polls its pool's channel, the presence channel and its own control
// channel, all under its orker_id, and answers DescribeOrker with described,
// whose orker_id and pool it must carry. It fails any task it is handed.
func startStandInOrker(t *testing.T, c client.Client, queue string, described *orkstackv1.DescribeOrkerResult) {
	t.Helper()
	control, err := task.QueueForOrker(described.GetOrkerId())
	if err != nil {
		t.Fatal(err)
	}
	describe := func(context.Context) (*orkstackv1.DescribeOrkerResult, error) { return described, nil }
	for _, channel := range []string{queue, task.OrkersQueue, control} {
		w := worker.New(c, channel, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
			Identity:                    described.GetOrkerId(),
		})
		w.RegisterActivityWithOptions(describe, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
		if channel == queue {
			w.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return nil, temporal.NewNonRetryableApplicationError(
					"an older Orker would fail to decode this input", "ActivityFailed", nil)
			}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
		}
		if err := w.Start(); err != nil {
			t.Fatalf("start stand-in Orker on %s: %v", channel, err)
		}
		t.Cleanup(w.Stop)
	}
}

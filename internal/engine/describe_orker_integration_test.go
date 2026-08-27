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
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_DescribeOrkerTravelsThroughTemporalToTheRealOrkerActivity
// is the end-to-end pool-liveness seam from §6.5: the engine schedules the
// named activity on one pool's Temporal channel and receives the identity
// produced by the real Orker Activities implementation. A direct method call
// would not prove registration, routing, serialization, or the replacement of
// ListPlugins on the channel.
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

	w := worker.New(c, queue, worker.Options{
		DisableWorkflowWorker:       true,
		DisableRegistrationAliasing: true,
		Identity:                    "orker-describe-proof-poller",
	})
	w.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		t.Fatalf("start Orker worker: %v", err)
	}
	defer w.Stop()

	result, err := engine.NewTemporalStarter(c, task.DefaultQueue).OrkerOnPool(ctx, queue)
	if err != nil {
		t.Fatalf("DescribeOrker through Temporal channel %q: %v", queue, err)
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

package orker

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"

	"github.com/orkstack/orkstack/internal/task"
)

// TestIndexEachLines_AdmittedByLedger: the line indexer is counted as a
// running task while it runs — what a drain and a stopping Orker wait on —
// and is no longer once it ends.
func TestIndexEachLines_AdmittedByLedger(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	during := int32(-1)
	env.RegisterActivityWithOptions(func(ctx context.Context) error {
		described, err := a.DescribeOrker(ctx)
		if err != nil {
			return err
		}
		during = described.GetRunningTasks()
		return nil
	}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	if _, err := env.ExecuteActivity(task.ActivityIndexEachLines); err != nil {
		t.Fatal(err)
	}
	if during != 1 {
		t.Fatalf("running_tasks while IndexEachLines ran = %d; want 1", during)
	}
	if got := a.RunningTasks(); got != 0 {
		t.Fatalf("running_tasks after = %d; want 0", got)
	}
}

// TestIndexEachLines_ReservesFixedRequest: one core and 64 MiB, each capped
// by the pool's per-task cap.
func TestIndexEachLines_ReservesFixedRequest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policy   SecurityPolicy
		milli    int64
		bytesMax int64
	}{
		{"no cap", SecurityPolicy{}, 1000, 64 << 20},
		{"a larger cap", SecurityPolicy{MaxCPU: "4", MaxMemory: "8Gi"}, 1000, 64 << 20},
		{"a smaller cap", SecurityPolicy{MaxCPU: "500m", MaxMemory: "32Mi"}, 500, 32 << 20},
	} {
		got := indexEachLinesRequest(tc.policy)
		if got.milli != tc.milli || got.bytes != tc.bytesMax {
			t.Errorf("%s: request = %+v; want %d milli, %d bytes", tc.name, got, tc.milli, tc.bytesMax)
		}
	}
}

package orker

import (
	"go.temporal.io/sdk/activity"

	"github.com/orkstack/orkstack/internal/task"
)

// TaskRegistrar is where the task worker's activities are registered: a
// worker, or a test environment that stands for one.
type TaskRegistrar interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// RegisterTaskActivities registers everything the task worker runs, as
// cmd/orker registers it. A test that drives the worker's interceptor does
// it through this, so it is handed the arguments production's is.
func (a *Activities) RegisterTaskActivities(w TaskRegistrar) {
	// Raw: a sealed run's task input is opened with its run tree's key, and
	// its result sealed under it (payload.go); a task that is not sealed runs
	// exactly as through ExecuteTask.
	w.RegisterActivityWithOptions(a.ExecuteTaskRaw, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(a.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	// The line indexer of a fan-out over an artifact's lines, scheduled on
	// the fan-out's pool channel (docs/DSL.md, "Fan-out over an artifact's
	// lines"). It starts no container. Raw, as a task is: its request names an
	// artifact of the run.
	w.RegisterActivityWithOptions(a.IndexEachLinesRaw, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	// Registered unconditionally, on the ordinary worker. The engine schedules
	// it on the pin's own SESSION context, which is what routes it to the host
	// that owns the directory (§6.4); an Orker with no pins registers a name
	// nobody will ever schedule, which costs nothing and keeps the registration
	// block one shape.
	w.RegisterActivityWithOptions(a.ReleasePin, activity.RegisterOptions{Name: task.ActivityReleasePin})
}

// RegisterSourceActivities registers what the source worker runs: a pool's event
// sources' instances, on its source channel.
func (a *Activities) RegisterSourceActivities(w TaskRegistrar) {
	w.RegisterActivityWithOptions(a.HostEventSource, activity.RegisterOptions{Name: task.ActivityHostEventSource})
}

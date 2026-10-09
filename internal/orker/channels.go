package orker

import (
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/task"
)

// The two Orker channels beside the pool channel (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
// They live here rather than in cmd/orker so the in-process test Orkers of the
// engine's integration suites are built exactly as the shipped binary is.

// ChannelWorkerOptions configures the control and presence workers. Both are
// UNVERSIONED, exactly as the engine's projection worker is (AGENTS.md §7.2):
// the control channel is polled by exactly one process and the presence
// channel is never dispatched to, so a versioned routing would only add a way
// for a cordon to land nowhere. One poller each: two idle long polls per Orker
// is the whole standing cost.
func ChannelWorkerOptions(orkerID string) worker.Options {
	return worker.Options{
		DisableRegistrationAliasing:      true,
		DisableWorkflowWorker:            true,
		Identity:                         orkerID,
		MaxConcurrentActivityTaskPollers: 1,
	}
}

// NewChannelWorkers builds this Orker's control worker (its own channel,
// carrying DescribeOrker and CordonOrker to this process only) and its
// presence worker (the channel every Orker polls and nothing dispatches to;
// its poller list is the fleet). CordonOrker is registered on the control
// worker ONLY — the one property that makes "this cordon lands on this Orker"
// true by construction.
func NewChannelWorkers(c client.Client, activities *Activities) (worker.Worker, worker.Worker, error) {
	orkerID := activities.Dispatcher().OrkerID
	controlQueue, err := task.QueueForOrker(orkerID)
	if err != nil {
		return nil, nil, fmt.Errorf("ORKER_ID_INVALID: %w", err)
	}
	control := worker.New(c, controlQueue, ChannelWorkerOptions(orkerID))
	control.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	control.RegisterActivityWithOptions(activities.CordonOrker,
		activity.RegisterOptions{Name: task.ActivityCordonOrker})

	presence := worker.New(c, task.OrkersQueue, ChannelWorkerOptions(orkerID))
	// Registered so a stray dispatch here fails nothing; nothing in OrkStack
	// dispatches to this channel (TestOrkersQueue_CarriesNoDispatch).
	presence.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	return control, presence, nil
}

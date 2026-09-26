package engine

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// OrkerFleet is the engine's view of the deployment's Orkers: who is polling
// the presence channel, and one Orker's own answer or cordon, delivered on its
// control channel (orker-cordon-drain §6). Production wires *TemporalStarter;
// a test states the facts directly.
//
// Nothing here holds an Orker list. Every answer is derived on read from the
// cluster's poller records and from the Orker itself, so any replica answers
// identically (AGENTS.md §7).
type OrkerFleet interface {
	// ObserveOrkers lists the identities polling OrkersQueue within the
	// freshness window, with each one's last-seen time.
	ObserveOrkers(ctx context.Context) ([]OrkerPresence, error)
	// ObserveOrker reports whether one Orker's control channel has a live
	// poller — the cheap "does this id exist" question CordonOrker and a
	// single-Orker ListOrkers ask before dispatching anything.
	ObserveOrker(ctx context.Context, orkerID string) (OrkerPresence, bool, error)
	// DescribeOrkerByID executes DescribeOrker on ONE Orker's control channel.
	DescribeOrkerByID(ctx context.Context, orkerID string) (*orkstackv1.DescribeOrkerResult, error)
	// CordonOrker executes CordonOrker on ONE Orker's control channel and
	// returns the Orker's state afterwards.
	CordonOrker(ctx context.Context, orkerID string, in *orkstackv1.CordonOrkerInput) (*orkstackv1.DescribeOrkerResult, error)
}

// orkerDescribeTimeout bounds one Orker's answer inside the inventory fan-out.
// Shorter than poolDescribeTimeout because ListOrkers asks the whole fleet and
// an Orker that is down should cost the reader one wave, not the request.
const orkerDescribeTimeout = 5 * time.Second

// orkerControlTimeout bounds one cordon delivery.
const orkerControlTimeout = 10 * time.Second

// OrkerDescribeTimeout and OrkerControlTimeout are the two bounds, exported
// for the API: the fan-out bounds each call's context by the first, and the
// cordon's Unavailable message names the second.
const (
	OrkerDescribeTimeout = orkerDescribeTimeout
	OrkerControlTimeout  = orkerControlTimeout
)

var _ OrkerFleet = (*TemporalStarter)(nil)

// ObserveOrkers reads the presence channel's poller list: the fleet.
func (s *TemporalStarter) ObserveOrkers(ctx context.Context) ([]OrkerPresence, error) {
	description, err := s.describeActivityPollers(ctx, task.OrkersQueue)
	if err != nil {
		return nil, fmt.Errorf("describe the orker presence channel: %w", err)
	}
	return livePollerRecords(description, time.Now(), DefaultPollerFreshness), nil
}

// ObserveOrker reads one Orker's control channel. A channel the cluster has
// never seen answers an empty poller list, which is (_, false, nil) — the same
// reading ObservePool gives an unknown channel.
func (s *TemporalStarter) ObserveOrker(ctx context.Context, orkerID string) (OrkerPresence, bool, error) {
	queue, err := task.QueueForOrker(orkerID)
	if err != nil {
		return OrkerPresence{}, false, err
	}
	description, err := s.describeActivityPollers(ctx, queue)
	if err != nil {
		return OrkerPresence{}, false, fmt.Errorf("describe orker %q's channel: %w", orkerID, err)
	}
	records := livePollerRecords(description, time.Now(), DefaultPollerFreshness)
	if len(records) == 0 {
		return OrkerPresence{}, false, nil
	}
	// One process polls a control channel. Should two share an id, the newest
	// record answers; ListOrkers reports the collision from the describe.
	newest := records[0]
	for _, record := range records[1:] {
		if record.LastSeen.After(newest.LastSeen) {
			newest = record
		}
	}
	return newest, true, nil
}

// describeActivityPollers is ObservePool's question, with the same options:
// activity pollers, every active version plus unversioned, bounded in time.
func (s *TemporalStarter) describeActivityPollers(ctx context.Context, queue string) (client.TaskQueueDescription, error) {
	probeCtx, cancel := context.WithTimeout(ctx, poolProbeTimeout)
	defer cancel()
	return s.client.DescribeTaskQueueEnhanced(probeCtx, client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      queue,
		TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
		ReportPollers:  true,
		Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
	})
}

// DescribeOrkerByID asks one Orker, on its own control channel, who it is and
// what it is doing.
func (s *TemporalStarter) DescribeOrkerByID(ctx context.Context, orkerID string) (*orkstackv1.DescribeOrkerResult, error) {
	queue, err := task.QueueForOrker(orkerID)
	if err != nil {
		return nil, err
	}
	return s.describeOrkerOn(ctx, queue, orkerDescribeTimeout)
}

// CordonOrker delivers a cordon (or its clearing) to one Orker on its own
// control channel. The activity is idempotent, so it may be retried once.
func (s *TemporalStarter) CordonOrker(
	ctx context.Context,
	orkerID string,
	in *orkstackv1.CordonOrkerInput,
) (*orkstackv1.DescribeOrkerResult, error) {
	queue, err := task.QueueForOrker(orkerID)
	if err != nil {
		return nil, err
	}
	result := &orkstackv1.DescribeOrkerResult{}
	handle, err := s.client.ExecuteActivity(ctx, client.StartActivityOptions{
		ID:                     newActivityID("cordon-orker"),
		TaskQueue:              queue,
		ScheduleToCloseTimeout: orkerControlTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 2},
	}, task.ActivityCordonOrker, in)
	if err != nil {
		return nil, fmt.Errorf("cordon orker: %w", err)
	}
	if err := handle.Get(ctx, result); err != nil {
		return nil, fmt.Errorf("cordon orker: %w", err)
	}
	return result, nil
}

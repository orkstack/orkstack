package api

import (
	"context"
	"sort"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// ActivityLister is the seam that replaces PluginLister.
//
// The change is not a rename. PluginLister asked a live ORKER what it could
// run, because a plugin was a Go type compiled into an Orker binary. Under the
// OCI Activity Contract an activity is an IMAGE, and which images exist is
// ENGINE CONFIGURATION (§4.2) — a catalog the engine holds, not a capability
// an Orker reports. So the question stops travelling to a worker entirely.
//
// Phase 3 supplies the implementation that reads the configured catalog and
// resolves each entry's index digest. Phase 1 freezes the seam and serves an
// explicitly configured list, so the RPC exists, answers, and is testable
// through the cut.
type ActivityLister interface {
	// ListActivities returns the deployment's catalog, ordered by name.
	ListActivities(ctx context.Context) ([]*orkstackv1.ActivityInfo, error)
}

// OrkerDescriber is the pool-liveness seam. It replaces the ListPlugins probe
// one-for-one (§6.5): the call site, its timeouts and its retry policy are
// unchanged, and only the payload differs.
type OrkerDescriber interface {
	DescribeOrker(ctx context.Context) (*orkstackv1.DescribeOrkerResult, error)
}

// ListActivities reports what this deployment can run.
//
// It answers from ENGINE CONFIGURATION and never asks an Orker, which is the
// substantive difference from the ListPlugins it replaces. That also makes it
// unable to fail the way ListPlugins could: there is no worker to be
// unreachable, so there is no Unavailable on this path.
//
// The order is by name, and that is part of the contract rather than an
// implementation detail: the Admin page renders this list, and a list that
// reordered itself between reads would look like the catalog had changed.
func (s *Service) ListActivities(
	ctx context.Context,
	_ *connect.Request[orkstackv1.ListActivitiesRequest],
) (*connect.Response[orkstackv1.ListActivitiesResponse], error) {
	var activities []*orkstackv1.ActivityInfo
	if s.activities != nil {
		listed, err := s.activities.ListActivities(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		activities = listed
	}
	sort.SliceStable(activities, func(i, j int) bool {
		return activities[i].GetName() < activities[j].GetName()
	})
	return connect.NewResponse(&orkstackv1.ListActivitiesResponse{Activities: activities}), nil
}

// DescribeOrker asks the deployment's default pool who is serving it.
//
// This is the liveness probe, and its failure mapping is deliberately the one
// ListPlugins had: a default pool that cannot answer is Unavailable, because
// that is a real fact an operator can act on — nothing is polling the channel
// this deployment dispatches to.
func (s *Service) DescribeOrker(
	ctx context.Context,
	_ *connect.Request[orkstackv1.DescribeOrkerRequest],
) (*connect.Response[orkstackv1.DescribeOrkerResponse], error) {
	if s.orkers == nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			errNoOrkerDescriber)
	}
	result, err := s.orkers.DescribeOrker(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&orkstackv1.DescribeOrkerResponse{Orker: result}), nil
}

// errNoOrkerDescriber is the answer when this replica has no way to reach a
// worker at all. It is Unavailable rather than Unimplemented: the RPC exists,
// and what is missing is the wiring, which is an operator's problem and not a
// client's.
var errNoOrkerDescriber = errNoOrker{}

type errNoOrker struct{}

func (errNoOrker) Error() string {
	return "this engine is not wired to a temporal client, so it cannot ask pool " +
		task.DefaultQueue + " which orker is serving it"
}

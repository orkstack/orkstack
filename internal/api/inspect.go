package api

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// errNoLiveInspection is the sentence an operator reads when this engine's
// read model is its own memory: an in-process run store cannot describe a
// Temporal execution, and an empty list would be a lie.
const errNoLiveInspection = "live attempt inspection is not available: this engine does not read runs from the cluster"

// InspectRun answers "what is this task doing right now" for every in-flight
// task of one run, from the cluster's describe of the execution.
//
// It is a read and nothing else: the run comes from the namespace-checked
// read model (LookupRunIn), the attempts from Temporal, and nothing is
// written, cached across calls or retried in line. A terminal run answers
// from its snapshot without touching the cluster, so an archived run costs
// nothing and is addressable forever. A cluster failure is Unavailable,
// never an empty list.
func (s *Service) InspectRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.InspectRunRequest],
) (*connect.Response[orkstackv1.InspectRunResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	// Nothing is described before the namespace check: a describe by run id
	// alone would confirm to a caller of the wrong namespace that it exists.
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	taskID := req.Msg.GetTaskId()
	if err := checkItemRequiresTask(taskID, req.Msg.Item); err != nil {
		return nil, err
	}
	if taskID != "" && !hasTask(run, taskID) {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("task %q not found in run %q", taskID, run.GetId()))
	}
	if _, err := itemRunOf(run, taskID, req.Msg.Item); err != nil {
		return nil, err
	}
	now := s.now().UTC()
	nothingLive := func() (*connect.Response[orkstackv1.InspectRunResponse], error) {
		return connect.NewResponse(&orkstackv1.InspectRunResponse{
			RunStatus:  run.GetStatus(),
			Attempts:   []*orkstackv1.LiveAttempt{},
			ComputedAt: timestamppb.New(now),
		}), nil
	}
	if isTerminalStatus(run.GetStatus()) {
		return nothingLive()
	}

	binding, err := s.locate(ctx, run.GetId())
	if err != nil {
		return nil, err
	}
	reader, ok := binding.Reader.(engine.LiveAttemptReader)
	if binding.Reader == nil || !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(errNoLiveInspection))
	}
	live, err := reader.LiveAttempts(ctx, run.GetId(), now)
	switch {
	case errors.Is(err, engine.ErrRunNotFound):
		// The execution left the cluster between the lookup and the
		// describe: it closed and was retired in that instant. The next
		// GetRun settles the status; nothing is live either way.
		return nothingLive()
	case errors.Is(err, engine.ErrNoLiveReader):
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(errNoLiveInspection))
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("describe run %q: %w", run.GetId(), err))
	}
	if live.Closed {
		return nothingLive()
	}

	order := make(map[string]int, len(run.GetTaskRuns()))
	for i, task := range run.GetTaskRuns() {
		order[task.GetTaskId()] = i
	}
	attempts := make([]*orkstackv1.LiveAttempt, 0, len(live.Attempts))
	for _, attempt := range live.Attempts {
		if taskID != "" && attempt.GetTaskId() != taskID {
			continue
		}
		if req.Msg.Item != nil && (attempt.Item == nil || attempt.GetItem() != req.Msg.GetItem()) {
			continue
		}
		attempts = append(attempts, attempt)
	}
	// Snapshot order, as ListTaskAttempts orders. A pending attempt whose
	// task the snapshot does not name yet (a projection lag of one event)
	// is live and KEPT, last, in task id order.
	sort.SliceStable(attempts, func(i, j int) bool {
		a, aKnown := order[attempts[i].GetTaskId()]
		b, bKnown := order[attempts[j].GetTaskId()]
		switch {
		case aKnown && bKnown && a != b:
			return a < b
		case aKnown && bKnown:
			// One fan-out: its items ascending, a task's own attempt first.
			return itemOrder(attempts[i].Item) < itemOrder(attempts[j].Item)
		case aKnown != bKnown:
			return aKnown
		default:
			return attempts[i].GetTaskId() < attempts[j].GetTaskId()
		}
	})
	return connect.NewResponse(&orkstackv1.InspectRunResponse{
		RunStatus:  run.GetStatus(),
		Attempts:   attempts,
		ComputedAt: timestamppb.New(now),
	}), nil
}

// itemOrder sorts a task's own attempt (no item) before its items.
func itemOrder(item *int32) int64 {
	if item == nil {
		return -1
	}
	return int64(*item)
}

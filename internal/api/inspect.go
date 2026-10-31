package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
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
	s.markCapacityWaits(ctx, run, attempts, now)
	return connect.NewResponse(&orkstackv1.InspectRunResponse{
		RunStatus:  run.GetStatus(),
		Attempts:   attempts,
		ComputedAt: timestamppb.New(now),
	}), nil
}

// capacityWaitAfter is how long an attempt must have waited before it is
// asked about: an Orker's outstanding poll takes a task within milliseconds,
// and while it is out it stands reserved at the per-task cap, so an attempt
// read in that instant would be called waiting for capacity by the poll that
// is about to take it. An attempt still not started after this had no poll to
// take it.
const capacityWaitAfter = 2 * time.Second

// markCapacityWaits sets capacity_wait on every attempt that is waiting for
// capacity rather than for an Orker (engine.CapacityWaitsOf).
//
// Its cost is bounded, because the run page and `run inspect --watch` call
// InspectRun on a timer: nothing is read unless an attempt is waiting for an
// Orker (waitingByPool), and then the answer reads the replica's shared
// inventory (sharedInventory): one presence read and one describe per live
// Orker — the cost of one ListOrkers — at most once per capacityInventoryTTL
// for every page and watch on this replica while its readings are whole, and
// one read at a time while they are not, each ending at capacityReadTimeout.
//
// "Waiting for an Orker" stays true whatever the inventory says, so a read
// that fails, or one in which a live Orker did not answer, leaves the attempts
// as they are and is logged; it never fails the live view.
func (s *Service) markCapacityWaits(
	ctx context.Context,
	run *orkstackv1.Run,
	attempts []*orkstackv1.LiveAttempt,
	now time.Time,
) {
	if s.inventory == nil {
		return
	}
	waiting, pools := waitingByPool(run, attempts, now)
	if len(pools) == 0 {
		return
	}
	orkers, err := s.inventory.read(ctx)
	switch {
	case err != nil && ctx.Err() != nil:
		// The caller left; there is no one to answer and nothing to report.
		return
	case err != nil:
		if s.inventory.warnDue() {
			s.logger.Warn("inspect run: the Orker inventory could not be read, so no waiting task is "+
				"reported as waiting for capacity (said at most once a minute)",
				"run_id", run.GetId(), "error", err)
		}
		return
	}
	waits, unanswered := engine.CapacityWaitsOf(orkers, pools)
	if unanswered > 0 {
		// Every poll of a run page would repeat it, and ListOrkers already
		// warns of each Orker that does not answer.
		s.logger.Debug("inspect run: some live Orkers did not answer when asked, so no waiting task is "+
			"reported as waiting for capacity", "run_id", run.GetId(), "unanswered", unanswered)
		return
	}
	for pool, list := range waiting {
		wait := waits[pool]
		if wait == nil {
			continue
		}
		for _, attempt := range list {
			attempt.CapacityWait = proto.Clone(wait).(*orkstackv1.CapacityWait)
		}
	}
}

// waitingByPool collects the attempts that may be waiting for capacity, by
// the pool each waits on, and the pools in the order first met: SCHEDULED
// with no retry backoff for at least capacityWaitAfter, and waitingPool's.
func waitingByPool(
	run *orkstackv1.Run,
	attempts []*orkstackv1.LiveAttempt,
	now time.Time,
) (map[string][]*orkstackv1.LiveAttempt, []string) {
	waiting := map[string][]*orkstackv1.LiveAttempt{}
	var pools []string
	var tasks map[string]*orkstackv1.TaskRun
	for _, attempt := range attempts {
		if attempt.GetState() != orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED ||
			attempt.GetNextAttemptAt() != nil ||
			attempt.GetAttemptScheduledAt() == nil ||
			now.Sub(attempt.GetAttemptScheduledAt().AsTime()) < capacityWaitAfter {
			continue
		}
		if tasks == nil {
			tasks = make(map[string]*orkstackv1.TaskRun, len(run.GetTaskRuns()))
			for _, taskRun := range run.GetTaskRuns() {
				tasks[taskRun.GetTaskId()] = taskRun
			}
		}
		pool, ok := waitingPool(tasks[attempt.GetTaskId()], attempt)
		if !ok {
			continue
		}
		if _, seen := waiting[pool]; !seen {
			pools = append(pools, pool)
		}
		waiting[pool] = append(waiting[pool], attempt)
	}
	return waiting, pools
}

// waitingPool is the pool a SCHEDULED attempt of taskRun waits on, as the
// snapshot records it — a fan-out item's own row first, then its task's — or
// false when the attempt cannot be waiting for capacity: its task is pinned,
// or the snapshot names no pool for it yet (a projection lag of one event).
func waitingPool(taskRun *orkstackv1.TaskRun, attempt *orkstackv1.LiveAttempt) (string, bool) {
	if taskRun == nil || taskRun.GetPin() != "" {
		return "", false
	}
	pool := taskRun.GetPool()
	if attempt.Item != nil {
		if index := int(attempt.GetItem()); index >= 0 && index < len(taskRun.GetItems()) {
			if itemPool := taskRun.GetItems()[index].GetPool(); itemPool != "" {
				pool = itemPool
			}
		}
	}
	return pool, pool != ""
}

// itemOrder sorts a task's own attempt (no item) before its items.
func itemOrder(item *int32) int64 {
	if item == nil {
		return -1
	}
	return int64(*item)
}

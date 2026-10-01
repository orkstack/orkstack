package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// IndexAnswerer reports the engine's boot answers about the run search
// attributes: engine.TemporalStarter.IndexAnswers.
type IndexAnswerer interface {
	IndexAnswers() (namespaceIndexed, originIndexed bool)
}

// ScheduleActivities is the one engine activity a fire runs. It lives in
// internal/api because it IS the submit preparation, and internal/engine
// cannot import internal/api.
type ScheduleActivities struct {
	svc     *Service
	indexes IndexAnswerer
}

// NewScheduleActivities returns the fire's activities over svc, stamping the
// starter's boot answers onto every preparation.
func NewScheduleActivities(svc *Service, indexes IndexAnswerer) *ScheduleActivities {
	return &ScheduleActivities{svc: svc, indexes: indexes}
}

// PrepareScheduledRun runs the ordinary submit preparation for one fire and
// returns everything the fire needs to start the run as its child. It starts
// nothing and writes nothing: a refusal leaves nothing behind, on prepare's
// own promise.
func (a *ScheduleActivities) PrepareScheduledRun(
	ctx context.Context,
	rec *orkstackv1.ScheduleRecord,
	scheduledAt time.Time,
) (*orkstackv1.ScheduledRunPreparation, error) {
	// The SCHEDULE row of run-origin §3, with its open cell decided: the
	// reference is the nominal fire time in RFC 3339 UTC, so a backfilled run
	// says which night it stood in for.
	origin := &orkstackv1.RunOrigin{
		Kind:        orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE,
		Id:          rec.GetScheduleId(),
		Reference:   scheduledAt.UTC().Format(time.RFC3339),
		Description: boundOriginAssertion(rec.GetName()),
	}
	sub, err := a.svc.prepare(ctx, &orkstackv1.SubmitRunRequest{
		OrkflowName:    rec.GetOrkflowName(),
		OrkflowVersion: rec.GetOrkflowVersion(),
		Namespace:      rec.GetNamespace(),
		Input:          rec.GetInput(),
		AllowEmptyPool: false,
	}, origin)
	if err != nil {
		return nil, fireError(err)
	}

	// The pending run is NOT saved: a scheduled run's pending window is
	// served from the child's own start event, and a process-memory row
	// would exist only on the replica that ran this activity.
	run := a.svc.newPendingRun(a.svc.newRunID(), sub.wf, sub.input, origin)
	plan, err := json.Marshal(sub.plan)
	if err != nil {
		return nil, refusedFire(fmt.Sprintf("encode the run plan: %v", err))
	}
	var nsIdx, orIdx bool
	if a.indexes != nil {
		nsIdx, orIdx = a.indexes.IndexAnswers()
	}
	prep := &orkstackv1.ScheduledRunPreparation{
		Run:              run,
		Orkflow:          sub.wf,
		Input:            sub.input,
		Plan:             plan,
		NamespaceIndexed: nsIdx,
		OriginIndexed:    orIdx,
	}
	// The same bytes travel as RunWorkflow's start arguments; a preparation
	// that does not fit would not have fit as a submission either.
	if proto.Size(prep) > task.MaxActivityInputBytes {
		return nil, refusedFire("prepared run exceeds 256 KiB")
	}
	return prep, nil
}

// refusedFire is a final refusal: the fire started no run, and retrying the
// same preparation would refuse again.
func refusedFire(msg string) error {
	return temporal.NewNonRetryableApplicationError(msg, engine.ScheduleFireRefusedType, nil)
}

// fireError sorts a preparation failure. A caller-shaped Connect code is a
// refusal and is final; anything else — an unreachable store or cluster — is
// returned as-is and retried by the local activity's policy within its
// budget.
func fireError(err error) error {
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeFailedPrecondition,
			connect.CodeResourceExhausted, connect.CodePermissionDenied:
			return refusedFire(ce.Message())
		}
	}
	return err
}

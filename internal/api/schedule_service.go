package api

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/schedules"
)

// ScheduleService manages schedules: objects bound to one orkflow in one
// namespace that start a run of it whenever a Temporal Schedule fires. It is
// the third starter beside SubmitRun and TriggerService, and it stores
// nothing: every field is the cluster's (engine.TemporalScheduler).

// schedulesConfigured refuses every schedule RPC on a service with no
// controller, the posture a nil retention controller takes for its RPCs.
func (s *Service) schedulesConfigured() error {
	if s.schedules == nil {
		return connect.NewError(connect.CodeUnimplemented, fmt.Errorf("no schedule controller configured"))
	}
	return nil
}

// scheduleName validates the name every request addresses a schedule by.
func scheduleName(name string) error {
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	if err := dsl.ValidSlug("schedule name", name); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// scheduleAddress resolves the namespace and validates the name of one
// addressed schedule.
func (s *Service) scheduleAddress(ctx context.Context, namespace, name string) (string, error) {
	if err := s.schedulesConfigured(); err != nil {
		return "", err
	}
	if err := scheduleName(name); err != nil {
		return "", err
	}
	ns, err := s.namespace(ctx, namespace)
	if err != nil {
		return "", err
	}
	return ns.GetName(), nil
}

// CreateSchedule creates one schedule. The namespace is resolved and the
// orkflow (and pinned version) confirmed before the cluster is asked, on the
// checks CreateTrigger makes; a refused create creates nothing.
func (s *Service) CreateSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.CreateScheduleRequest],
) (*connect.Response[orkstackv1.CreateScheduleResponse], error) {
	msg := req.Msg
	ns, err := s.scheduleAddress(ctx, msg.GetNamespace(), msg.GetName())
	if err != nil {
		return nil, err
	}
	orkflow := msg.GetOrkflowName()
	if orkflow == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("orkflow_name is required"))
	}
	version := msg.GetOrkflowVersion()
	if _, err := s.orkflows.Get(ctx, ns, orkflow, version); err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q", orkflow, version, ns)
	}
	if err := ValidateScheduleSpec(msg.GetSpec(), s.now()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := validateScheduleText("description", msg.GetDescription()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	id, err := schedules.NewID()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	rec := &orkstackv1.ScheduleRecord{
		ScheduleId:     id,
		Name:           msg.GetName(),
		Namespace:      ns,
		OrkflowName:    orkflow,
		OrkflowVersion: version,
		Input:          cloneScheduleInput(msg.GetInput()),
		Description:    msg.GetDescription(),
		Spec:           normalizeScheduleSpec(msg.GetSpec()),
	}
	view, err := s.schedules.Create(ctx, rec, msg.GetPaused())
	if err != nil {
		return nil, scheduleConnectError(err, ns, msg.GetName())
	}
	return connect.NewResponse(&orkstackv1.CreateScheduleResponse{Schedule: scheduleProto(view)}), nil
}

// cloneScheduleInput copies a request's input so the stored record never
// aliases the request.
func cloneScheduleInput(in *orkstackv1.Payload) *orkstackv1.Payload {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*orkstackv1.Payload)
}

// ListSchedules returns one namespace's schedules, ordered by name, narrowed
// to one orkflow when orkflow_name is set.
func (s *Service) ListSchedules(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListSchedulesRequest],
) (*connect.Response[orkstackv1.ListSchedulesResponse], error) {
	if err := s.schedulesConfigured(); err != nil {
		return nil, err
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	views, err := s.schedules.List(ctx, ns.GetName())
	if err != nil {
		return nil, scheduleConnectError(err, ns.GetName(), "")
	}
	out := make([]*orkstackv1.Schedule, 0, len(views))
	for _, view := range views {
		if name := req.Msg.GetOrkflowName(); name != "" && view.OrkflowName != name {
			continue
		}
		out = append(out, scheduleProto(view))
	}
	return connect.NewResponse(&orkstackv1.ListSchedulesResponse{Schedules: out}), nil
}

// GetSchedule reads one schedule by name. A schedule of another namespace is
// NotFound.
func (s *Service) GetSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetScheduleRequest],
) (*connect.Response[orkstackv1.GetScheduleResponse], error) {
	ns, err := s.scheduleAddress(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	view, err := s.schedules.Describe(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, scheduleConnectError(err, ns, req.Msg.GetName())
	}
	return connect.NewResponse(&orkstackv1.GetScheduleResponse{Schedule: scheduleProto(view)}), nil
}

// UpdateSchedule replaces a schedule's mutable fields. Last writer wins.
func (s *Service) UpdateSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateScheduleRequest],
) (*connect.Response[orkstackv1.UpdateScheduleResponse], error) {
	msg := req.Msg
	ns, err := s.scheduleAddress(ctx, msg.GetNamespace(), msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := ValidateScheduleSpec(msg.GetSpec(), s.now()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := validateScheduleText("description", msg.GetDescription()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	spec := normalizeScheduleSpec(msg.GetSpec())
	input := cloneScheduleInput(msg.GetInput())
	version := msg.GetOrkflowVersion()
	var pinErr error
	view, err := s.schedules.Update(ctx, ns, msg.GetName(), func(rec *orkstackv1.ScheduleRecord) error {
		// A pinned version must exist at update as at create. It is checked
		// here, where the record names the orkflow the schedule is bound to.
		if version != "" {
			if _, err := s.orkflows.Get(ctx, ns, rec.GetOrkflowName(), version); err != nil {
				pinErr = orkflowStoreError(err, "orkflow %q version %q in namespace %q",
					rec.GetOrkflowName(), version, ns)
				return pinErr
			}
		}
		rec.OrkflowVersion = version
		rec.Input = input
		rec.Description = msg.GetDescription()
		rec.Spec = spec
		return nil
	})
	if pinErr != nil {
		return nil, pinErr
	}
	if err != nil {
		return nil, scheduleConnectError(err, ns, msg.GetName())
	}
	return connect.NewResponse(&orkstackv1.UpdateScheduleResponse{Schedule: scheduleProto(view)}), nil
}

// describeAfter answers a write with the schedule as the cluster now
// describes it.
func (s *Service) describeAfter(ctx context.Context, ns, name string) (*orkstackv1.Schedule, error) {
	view, err := s.schedules.Describe(ctx, ns, name)
	if err != nil {
		return nil, scheduleConnectError(err, ns, name)
	}
	return scheduleProto(view), nil
}

// PauseSchedule stops fires without deleting anything.
func (s *Service) PauseSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.PauseScheduleRequest],
) (*connect.Response[orkstackv1.PauseScheduleResponse], error) {
	ns, err := s.scheduleAddress(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := validateScheduleText("reason", req.Msg.GetReason()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.schedules.Pause(ctx, ns, req.Msg.GetName(), req.Msg.GetReason()); err != nil {
		return nil, scheduleConnectError(err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.PauseScheduleResponse{Schedule: schedule}), nil
}

// ResumeSchedule lets a paused schedule fire again.
func (s *Service) ResumeSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.ResumeScheduleRequest],
) (*connect.Response[orkstackv1.ResumeScheduleResponse], error) {
	ns, err := s.scheduleAddress(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := s.schedules.Resume(ctx, ns, req.Msg.GetName()); err != nil {
		return nil, scheduleConnectError(err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.ResumeScheduleResponse{Schedule: schedule}), nil
}

// FireSchedule fires once, now. The overlap override is handed on AS
// RECEIVED: an absent one stays UNSPECIFIED, which the engine sends as "no
// override", so the schedule's own policy applies.
func (s *Service) FireSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.FireScheduleRequest],
) (*connect.Response[orkstackv1.FireScheduleResponse], error) {
	ns, err := s.scheduleAddress(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if !validOverlapPolicy(req.Msg.GetOverlap()) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("overlap: unknown overlap policy %d", int32(req.Msg.GetOverlap())))
	}
	if err := s.schedules.Fire(ctx, ns, req.Msg.GetName(), req.Msg.GetOverlap()); err != nil {
		return nil, scheduleConnectError(err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.FireScheduleResponse{Schedule: schedule}), nil
}

// BackfillSchedule fires once for every nominal time in [start, end].
func (s *Service) BackfillSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.BackfillScheduleRequest],
) (*connect.Response[orkstackv1.BackfillScheduleResponse], error) {
	msg := req.Msg
	ns, err := s.scheduleAddress(ctx, msg.GetNamespace(), msg.GetName())
	if err != nil {
		return nil, err
	}
	if !validOverlapPolicy(msg.GetOverlap()) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("overlap: unknown overlap policy %d", int32(msg.GetOverlap())))
	}
	if msg.GetStart() == nil || msg.GetEnd() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("start and end are both required"))
	}
	start, end := msg.GetStart().AsTime(), msg.GetEnd().AsTime()
	if end.Before(start) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("end must not precede start"))
	}
	if end.Sub(start) > scheduleMaxBackfillWindow {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("backfill window is %s; the limit is 366 days", end.Sub(start)))
	}
	if err := s.schedules.Backfill(ctx, ns, msg.GetName(), start, end, msg.GetOverlap()); err != nil {
		return nil, scheduleConnectError(err, ns, msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.BackfillScheduleResponse{Schedule: schedule}), nil
}

// DeleteSchedule removes the schedule. Runs it already started continue. A
// repeat is NotFound.
func (s *Service) DeleteSchedule(
	ctx context.Context,
	req *connect.Request[orkstackv1.DeleteScheduleRequest],
) (*connect.Response[orkstackv1.DeleteScheduleResponse], error) {
	ns, err := s.scheduleAddress(ctx, req.Msg.GetNamespace(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := s.schedules.Delete(ctx, ns, req.Msg.GetName()); err != nil {
		return nil, scheduleConnectError(err, ns, req.Msg.GetName())
	}
	return connect.NewResponse(&orkstackv1.DeleteScheduleResponse{}), nil
}

package api

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
	if err := ownNamespaceTarget("schedule", orkflow); err != nil {
		return nil, err
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
	submits, err := scheduleSubmission(rec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// A create binds its caller. The entry is recorded before the cluster is
	// asked (schedule_binding.go).
	entry := s.boundBy(submits, authn.Recorded(ctx, req.Header()))
	if err := s.addScheduleBinding(ctx, ns, id, entry); err != nil {
		return nil, err
	}
	view, err := s.schedules.Create(ctx, rec, msg.GetPaused())
	if err != nil {
		// The cluster holds no such schedule: the object is removed, best
		// effort. One left behind names an id no schedule has.
		if s.scheduleBindings != nil && scheduleRefused(err) {
			_ = s.scheduleBindings.Delete(context.WithoutCancel(ctx), ns, id)
		}
		return nil, scheduleConnectError(ctx, err, ns, msg.GetName())
	}
	s.trimScheduleBindings(ctx, ns, view, entry)
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionCreate)
	return connect.NewResponse(&orkstackv1.CreateScheduleResponse{Schedule: s.scheduleProtoWith(ctx, view)}), nil
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
// by filters. The filters on id, name and orkflow_name are decided on the
// cluster's list entry before a schedule is described, so a narrowed listing
// describes only what it may return; the whole set then applies to each
// described row, and a row the cluster could not describe is kept under
// every filter on a field it did not measure.
func (s *Service) ListSchedules(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListSchedulesRequest],
) (*connect.Response[orkstackv1.ListSchedulesResponse], error) {
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	if err := s.schedulesConfigured(); err != nil {
		return nil, err
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	identity := set.Restrict("id", "name", "orkflow_name")
	keep := func(id, name, orkflow string) bool {
		return identity.Match(&orkstackv1.Schedule{Id: id, Name: name, Namespace: ns.GetName(), OrkflowName: orkflow})
	}
	views, err := s.schedules.List(ctx, ns.GetName(), keep)
	if err != nil {
		return nil, scheduleConnectError(ctx, err, ns.GetName(), "")
	}
	out := make([]*orkstackv1.Schedule, 0, len(views))
	for _, view := range views {
		if row := scheduleProto(view); set.Match(row) {
			s.scheduleConsent(ctx, view, row)
			out = append(out, row)
		}
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
		return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
	}
	return connect.NewResponse(&orkstackv1.GetScheduleResponse{Schedule: s.scheduleProtoWith(ctx, view)}), nil
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
	who := authn.Recorded(ctx, req.Header())
	if msg.GetRebind() && who.ID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"--rebind records you as the schedule's binder, and this request names nobody: send Ork-Principal, or a credential the engine verifies"))
	}
	var pinErr, bindErr error
	var entry artifacts.ScheduleBinding
	var added bool
	var savedID string
	var wasSub artifacts.ScheduleBinding
	view, err := s.schedules.Update(ctx, ns, msg.GetName(), func(rec *orkstackv1.ScheduleRecord) error {
		// The SDK may run this more than once: what an earlier run decided
		// is undone and forgotten, so only this run's outcome stands.
		if added && !entry.Submits(wasSub) {
			s.dropScheduleBinding(ctx, ns, savedID, entry)
		}
		pinErr, bindErr, added = nil, nil, false
		// A pinned version must exist at update as at create. It is checked
		// here, where the record names the orkflow the schedule is bound to.
		if version != "" {
			if _, err := s.orkflows.Get(ctx, ns, rec.GetOrkflowName(), version); err != nil {
				pinErr = orkflowStoreError(err, "orkflow %q version %q in namespace %q",
					rec.GetOrkflowName(), version, ns)
				return pinErr
			}
		}
		was, err := scheduleSubmission(rec)
		if err != nil {
			bindErr = connect.NewError(connect.CodeInternal, err)
			return bindErr
		}
		rec.OrkflowVersion = version
		rec.Input = input
		rec.Description = msg.GetDescription()
		rec.Spec = spec
		// The save's entry, recorded before the cluster is written: the
		// caller binds when the save changes what is submitted or asks to
		// rebind; otherwise the binder and consent stay.
		next, err := scheduleSubmission(rec)
		if err != nil {
			bindErr = connect.NewError(connect.CodeInvalidArgument, err)
			return bindErr
		}
		// A changed submission is recorded with its new binder before the
		// cluster holds it. A --rebind of what the cluster already holds is
		// not: the binder stays until the cluster accepts the save, and only
		// then is the caller recorded (step 3), so a refused or lost save
		// never names a binder who did not bind.
		changed := !next.Submits(was)
		entry, err = s.addScheduleBindingFrom(ctx, ns, rec.GetScheduleId(), func(doc artifacts.ScheduleBindings) artifacts.ScheduleBinding {
			if changed {
				return s.boundBy(next, who)
			}
			return s.keptBinder(next, was, doc)
		})
		if err != nil {
			bindErr = err
			return bindErr
		}
		added, savedID, wasSub = true, rec.GetScheduleId(), was
		return nil
	})
	if pinErr != nil {
		return nil, pinErr
	}
	if bindErr != nil {
		return nil, bindErr
	}
	if err != nil {
		// The cluster definitely refused the save: its entry goes. A lost
		// answer may have landed, so its entry stays for the next trim.
		// A save that submits what the cluster already holds replaced that
		// submission's entry with one of the same binder; dropping it would
		// leave the held submission unrecorded, so it stays.
		if added && scheduleRefused(err) && !entry.Submits(wasSub) {
			s.dropScheduleBinding(ctx, ns, savedID, entry)
		}
		return nil, scheduleConnectError(ctx, err, ns, msg.GetName())
	}
	if msg.GetRebind() && entry.Submits(wasSub) {
		// The cluster has the save: the caller now binds what it holds.
		if err := s.rebindScheduleRecord(ctx, ns, view, s.boundBy(entry, who)); err != nil {
			return nil, err
		}
	} else {
		s.trimScheduleBindings(ctx, ns, view, entry)
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionUpdate)
	return connect.NewResponse(&orkstackv1.UpdateScheduleResponse{Schedule: s.scheduleProtoWith(ctx, view)}), nil
}

// describeAfter answers a write with the schedule as the cluster now
// describes it.
func (s *Service) describeAfter(ctx context.Context, ns, name string) (*orkstackv1.Schedule, error) {
	view, err := s.schedules.Describe(ctx, ns, name)
	if err != nil {
		return nil, scheduleConnectError(ctx, err, ns, name)
	}
	return s.scheduleProtoWith(ctx, view), nil
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
		return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionPause)
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
		return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionResume)
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
		return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionFire)
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
		return nil, scheduleConnectError(ctx, err, ns, msg.GetName())
	}
	schedule, err := s.describeAfter(ctx, ns, msg.GetName())
	if err != nil {
		return nil, err
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionBackfill)
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
	// The object goes in three steps: emptied, so it admits nothing; then the
	// cluster's schedule; then the object itself. A schedule that cannot be
	// read refuses the delete; a cluster delete that fails puts the entries
	// back, so a schedule that still fires keeps its record.
	var id string
	var kept []artifacts.ScheduleBinding
	if s.scheduleBindings != nil {
		view, err := s.schedules.Describe(ctx, ns, req.Msg.GetName())
		switch {
		case errors.Is(err, engine.ErrScheduleNotFound):
		case err != nil:
			return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
		case view.ID != "":
			id = view.ID
			if _, err := s.scheduleBindings.Update(ctx, ns, id, func(doc *artifacts.ScheduleBindings) error {
				kept = slices.Clone(doc.Entries)
				doc.Keep()
				return nil
			}); err != nil {
				return nil, scheduleBindingError(ctx, err)
			}
		}
	}
	if err := s.schedules.Delete(ctx, ns, req.Msg.GetName()); err != nil {
		if id != "" && len(kept) > 0 && !errors.Is(err, engine.ErrScheduleNotFound) {
			restore, cancel := context.WithTimeout(context.WithoutCancel(ctx), scheduleRecordDetachedTimeout)
			defer cancel()
			if _, rerr := s.scheduleBindings.Update(restore, ns, id, func(doc *artifacts.ScheduleBindings) error {
				for _, e := range kept {
					doc.Add(e)
				}
				return nil
			}); rerr != nil {
				s.logger.Error("a schedule whose delete failed could not have its record restored; it fires with no binder recorded until it is rebound",
					"namespace", ns, "schedule", req.Msg.GetName(), "error", rerr)
			}
		}
		return nil, scheduleConnectError(ctx, err, ns, req.Msg.GetName())
	}
	if id != "" {
		remove, cancel := context.WithTimeout(context.WithoutCancel(ctx), scheduleRecordDetachedTimeout)
		defer cancel()
		if err := s.scheduleBindings.Delete(remove, ns, id); err != nil {
			s.logger.Warn("the schedule was deleted, but its emptied binding record was not; it admits nothing",
				"namespace", ns, "schedule", req.Msg.GetName(), "error", err)
		}
	}
	s.scheduleMetrics.Request(ctx, ns, telemetry.ScheduleActionDelete)
	return connect.NewResponse(&orkstackv1.DeleteScheduleResponse{}), nil
}

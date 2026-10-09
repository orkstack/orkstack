package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// ScheduleController is the engine's schedule seam: what the API needs from
// the cluster and nothing more. engine.TemporalScheduler is the production
// implementation; a fake serves the unit tests, as IdempotencyClaimer's does.
type ScheduleController interface {
	Create(ctx context.Context, rec *orkstackv1.ScheduleRecord, paused bool) (engine.ScheduleView, error)
	Describe(ctx context.Context, namespace, name string) (engine.ScheduleView, error)
	// List describes one namespace's schedules; keep (nil: every one) is
	// decided on the list entry's id, name and Orkflow before any describe.
	List(ctx context.Context, namespace string, keep func(id, name, orkflow string) bool) ([]engine.ScheduleView, error)
	Update(ctx context.Context, namespace, name string, apply func(*orkstackv1.ScheduleRecord) error) (engine.ScheduleView, error)
	Pause(ctx context.Context, namespace, name, reason string) error
	Resume(ctx context.Context, namespace, name string) error
	Fire(ctx context.Context, namespace, name string, overlap orkstackv1.OverlapPolicy) error
	Backfill(ctx context.Context, namespace, name string, start, end time.Time, overlap orkstackv1.OverlapPolicy) error
	Delete(ctx context.Context, namespace, name string) error
}

const (
	// scheduleCronMaxBytes bounds one cron expression.
	scheduleCronMaxBytes = 200
	// scheduleTextMaxBytes bounds a description and a pause reason, on the
	// rule origin references use.
	scheduleTextMaxBytes = 256
	// scheduleMinCatchupWindow is the cluster's own minimum catch-up window.
	scheduleMinCatchupWindow = 10 * time.Second
	// scheduleMaxBackfillWindow bounds a backfill: a typo must not enqueue
	// years of fires.
	scheduleMaxBackfillWindow = 366 * 24 * time.Hour
)

// ValidateScheduleSpec is the one validation of an authored spec, used by
// create and update. Syntax of a cron expression is the cluster's to judge;
// everything checkable without it is checked here, each failure naming the
// field.
func ValidateScheduleSpec(spec *orkstackv1.ScheduleSpec, now time.Time) error {
	if spec == nil || (len(spec.GetCron()) == 0 && spec.GetInterval() == nil) {
		return fmt.Errorf("spec: at least one cron expression or an interval is required")
	}
	for i, expr := range spec.GetCron() {
		field := fmt.Sprintf("spec.cron[%d]", i)
		if strings.TrimSpace(expr) == "" {
			return fmt.Errorf("%s: an expression is required", field)
		}
		if len(expr) > scheduleCronMaxBytes {
			return fmt.Errorf("%s: %d bytes; the limit is %d", field, len(expr), scheduleCronMaxBytes)
		}
		for j := 0; j < len(expr); j++ {
			if b := expr[j]; b < 0x20 || b == 0x7f {
				return fmt.Errorf("%s: contains control character 0x%02x at byte %d", field, b, j)
			}
		}
		upper := strings.ToUpper(strings.TrimSpace(expr))
		if strings.HasPrefix(upper, "CRON_TZ=") || strings.HasPrefix(upper, "TZ=") {
			return fmt.Errorf("%s: set spec.timezone instead of a CRON_TZ= prefix", field)
		}
	}
	if iv := spec.GetInterval(); iv != nil && iv.AsDuration() <= 0 {
		return fmt.Errorf("spec.interval: must be positive, got %s", iv.AsDuration())
	}
	if j := spec.GetJitter(); j != nil && j.AsDuration() < 0 {
		return fmt.Errorf("spec.jitter: must not be negative, got %s", j.AsDuration())
	}
	if tz := spec.GetTimezone(); tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return fmt.Errorf("spec.timezone: %q is not an IANA time zone: %v", tz, err)
		}
	}
	if cw := spec.GetCatchupWindow(); cw != nil && cw.AsDuration() < scheduleMinCatchupWindow {
		return fmt.Errorf("spec.catchup_window: must be at least %s (the cluster's minimum), got %s",
			scheduleMinCatchupWindow, cw.AsDuration())
	}
	if spec.GetStartAt() != nil && spec.GetEndAt() != nil && spec.GetStartAt().AsTime().After(spec.GetEndAt().AsTime()) {
		return fmt.Errorf("spec.start_at: %s is after spec.end_at %s",
			spec.GetStartAt().AsTime().UTC().Format(time.RFC3339), spec.GetEndAt().AsTime().UTC().Format(time.RFC3339))
	}
	if end := spec.GetEndAt(); end != nil && !end.AsTime().After(now) {
		return fmt.Errorf("spec.end_at: %s is not in the future; a schedule with no fire left is retired by the cluster",
			end.AsTime().UTC().Format(time.RFC3339))
	}
	if !validOverlapPolicy(spec.GetOverlap()) {
		return fmt.Errorf("spec.overlap: unknown overlap policy %d", int32(spec.GetOverlap()))
	}
	return nil
}

// validOverlapPolicy reports whether p is a DECLARED value. The reserved 5
// (Temporal's TERMINATE_OTHER) and every other undeclared number are not.
func validOverlapPolicy(p orkstackv1.OverlapPolicy) bool {
	_, ok := orkstackv1.OverlapPolicy_name[int32(p)]
	return ok
}

// validateScheduleText is the rule for a description and a pause reason: at
// most 256 bytes, valid UTF-8, no control character. Validated, never
// sanitized.
func validateScheduleText(field, value string) error {
	if len(value) > scheduleTextMaxBytes {
		return fmt.Errorf("%s: %d bytes; the limit is %d", field, len(value), scheduleTextMaxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s: not valid UTF-8", field)
	}
	for i := 0; i < len(value); i++ {
		if b := value[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("%s: contains control character 0x%02x at byte %d", field, b, i)
		}
	}
	return nil
}

// normalizeScheduleSpec is the one place ScheduleSpec.overlap's
// "UNSPECIFIED is SKIP" rule is applied: a clone whose overlap is SKIP when
// the request's was UNSPECIFIED, and otherwise the request's spec unchanged.
// The request itself is never mutated.
func normalizeScheduleSpec(spec *orkstackv1.ScheduleSpec) *orkstackv1.ScheduleSpec {
	out := proto.Clone(spec).(*orkstackv1.ScheduleSpec)
	if out.GetOverlap() == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		out.Overlap = orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP
	}
	return out
}

// scheduleNote is what a read shows as the note: the placeholder notes Pause
// and Resume send when given no reason read as no note.
func scheduleNote(note string) string {
	if note == engine.SchedulePauseNote || note == engine.ScheduleResumeNote {
		return ""
	}
	return note
}

// scheduleProto maps one view. An unavailable row carries identity (1–4) and
// unavailable_reason only: never a zero that reads as a real value.
func scheduleProto(view engine.ScheduleView) *orkstackv1.Schedule {
	out := &orkstackv1.Schedule{
		Id:          view.ID,
		Name:        view.Name,
		Namespace:   view.Namespace,
		OrkflowName: view.OrkflowName,
	}
	if view.Unavailable != "" || view.Record == nil {
		out.UnavailableReason = view.Unavailable
		if out.UnavailableReason == "" {
			out.UnavailableReason = "the cluster returned no schedule record"
		}
		return out
	}
	rec := view.Record
	out.OrkflowVersion = rec.GetOrkflowVersion()
	out.Input = rec.GetInput()
	out.Description = rec.GetDescription()
	out.Spec = rec.GetSpec()
	out.Paused = view.Paused
	out.Note = scheduleNote(view.Note)
	for _, t := range view.NextFireTimes {
		out.NextFireTimes = append(out.NextFireTimes, timestamppb.New(t))
	}
	for _, f := range view.RecentFires {
		fire := &orkstackv1.ScheduleFire{
			RunId:            f.RunID,
			Refusal:          f.Refusal,
			DetailsAvailable: f.DetailsAvailable,
		}
		if !f.ScheduledAt.IsZero() {
			fire.ScheduledAt = timestamppb.New(f.ScheduledAt)
		}
		if !f.ActualAt.IsZero() {
			fire.ActualAt = timestamppb.New(f.ActualAt)
		}
		out.RecentFires = append(out.RecentFires, fire)
	}
	out.FiresTaken = view.FiresTaken
	out.FiresSkippedOverlap = view.FiresSkippedOverlap
	out.FiresMissedCatchup = view.FiresMissedCatchup
	if !view.CreatedAt.IsZero() {
		out.CreatedAt = timestamppb.New(view.CreatedAt)
	}
	if !view.UpdatedAt.IsZero() {
		out.UpdatedAt = timestamppb.New(view.UpdatedAt)
	}
	return out
}

// scheduleConnectError maps a controller failure onto a Connect code. The
// three sentinels are the caller's to act on; an unreachable cluster is
// Unavailable; anything else — ErrOverlapPolicyUnknown included, since the
// API refuses every undeclared value first — is Internal. A cluster failure
// never reads as an empty list or a successful delete.
func scheduleConnectError(err error, namespace, name string) error {
	switch {
	case errors.Is(err, engine.ErrScheduleNotFound):
		return connect.NewError(connect.CodeNotFound,
			fmt.Errorf("schedule %q not found in namespace %q", name, namespace))
	case errors.Is(err, engine.ErrScheduleExists):
		return connect.NewError(connect.CodeAlreadyExists,
			fmt.Errorf("schedule %q already exists in namespace %q", name, namespace))
	case errors.Is(err, engine.ErrScheduleSpecRefused):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	}
	var unavailable *serviceerror.Unavailable
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unavailable) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

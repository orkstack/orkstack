package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

const (
	// TemporalScheduleIDPrefix + "{namespace}/{name}" is the cluster-side id
	// of a schedule. "/" is the separator because a slug can never contain
	// one, so (a, b-c) and (a-b, c) cannot collide. Deriving the cluster id
	// from the name is what makes a name unique within a namespace ATOMICALLY:
	// ScheduleClient.Create refuses an id that exists, and no second store has
	// to agree.
	TemporalScheduleIDPrefix = "ork-schedule/"
	// ScheduleFireWorkflowIDPrefix + "{schedule id}" is the action's workflow
	// id; the cluster appends "-{nominal fire time}" to make each fire's id
	// unique. Internal, never user-facing.
	ScheduleFireWorkflowIDPrefix = "ork-schedule-fire/"

	// Memo keys on the SCHEDULE. A memo is immutable on a schedule (the SDK's
	// ScheduleUpdate carries none), which is exactly right for identity.
	// MemoNamespace and MemoOrkflowName (reader.go) are reused as-is.
	MemoScheduleID   = "orkflow_schedule_id"
	MemoScheduleName = "orkflow_schedule_name"

	// SchedulePauseNote and ScheduleResumeNote are what Pause and Resume send
	// when no reason is given. The SDK substitutes "Paused via Go SDK" and
	// "Unpaused via Go SDK" for an empty note, so an empty one cannot be sent;
	// these two literals are sent instead and read back as no note.
	SchedulePauseNote  = "paused"
	ScheduleResumeNote = "resumed"

	// maxScheduleFireTimes bounds next_fire_times and recent_fires (§2).
	maxScheduleFireTimes = 10
	// scheduleListPageSize is the list page the cluster is asked for.
	scheduleListPageSize = 1000
)

var (
	// ErrScheduleNotFound reports that no schedule of that name exists in
	// that namespace.
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleExists reports that the name is already a schedule of the
	// namespace.
	ErrScheduleExists = errors.New("schedule already exists")
	// ErrScheduleSpecRefused wraps the cluster's own refusal of a spec (a cron
	// expression it cannot parse), whose message is kept verbatim.
	ErrScheduleSpecRefused = errors.New("schedule spec refused by the cluster")
	// ErrOverlapPolicyUnknown is returned for any OverlapPolicy the switch does
	// not name — including 5, Temporal's TERMINATE_OTHER, which the OrkStack
	// enum reserves. The API refuses undeclared values first, so this is
	// reached only by a caller that skipped validation; it is still never a
	// cast.
	ErrOverlapPolicyUnknown = errors.New("unknown overlap policy")
	// errScheduleIdentityChanged refuses an update closure that touched a
	// schedule's identity (id, name, namespace, orkflow).
	errScheduleIdentityChanged = errors.New("a schedule's id, name, namespace and orkflow are immutable")
)

// TemporalScheduleID is the cluster-side id of the schedule (namespace, name).
func TemporalScheduleID(namespace, name string) string {
	return TemporalScheduleIDPrefix + namespace + "/" + name
}

// ScheduleView is one schedule as the cluster described it for one call.
type ScheduleView struct {
	Record                                              *orkstackv1.ScheduleRecord // nil when Unavailable is set
	ID, Name, Namespace, OrkflowName                    string                     // from the memo, always set
	Paused                                              bool
	Note                                                string
	NextFireTimes                                       []time.Time
	RecentFires                                         []ScheduleFireView
	FiresTaken, FiresSkippedOverlap, FiresMissedCatchup int64
	CreatedAt, UpdatedAt                                time.Time
	Unavailable                                         string
}

// ScheduleFireView is one fire as the cluster and the fire's own memo
// describe it.
type ScheduleFireView struct {
	ScheduledAt, ActualAt time.Time
	RunID, Refusal        string
	DetailsAvailable      bool
}

// TemporalScheduler creates, reads and changes schedules on the cluster, and
// is the ONLY place a Temporal ScheduleClient is used. It stores nothing:
// the schedule is the record.
type TemporalScheduler struct {
	client    client.Client
	queue     string                  // task.DefaultQueue: the engine's run channel
	converter converter.DataConverter // the client's; decodes action args and memos
	logger    *slog.Logger
}

// NewTemporalScheduler returns the scheduler over c, whose fires start on
// runQueue (an empty one is task.DefaultQueue).
func NewTemporalScheduler(c client.Client, runQueue string) *TemporalScheduler {
	if runQueue == "" {
		runQueue = task.DefaultQueue
	}
	return &TemporalScheduler{
		client:    c,
		queue:     runQueue,
		converter: converter.GetDefaultDataConverter(),
		logger:    slog.Default(),
	}
}

// WithLogger sets where the drift warning is written.
func (s *TemporalScheduler) WithLogger(logger *slog.Logger) *TemporalScheduler {
	if logger != nil {
		s.logger = logger
	}
	return s
}

// Create creates the schedule in one cluster call and describes it, so the
// first read already carries real next fire times.
func (s *TemporalScheduler) Create(ctx context.Context, rec *orkstackv1.ScheduleRecord, paused bool) (ScheduleView, error) {
	policies, err := policiesToTemporal(rec.GetSpec())
	if err != nil {
		return ScheduleView{}, err
	}
	options := client.ScheduleOptions{
		ID:             TemporalScheduleID(rec.GetNamespace(), rec.GetName()),
		Spec:           specToTemporal(rec.GetSpec()),
		Action:         s.action(rec),
		Overlap:        policies.Overlap,
		CatchupWindow:  policies.CatchupWindow,
		PauseOnFailure: policies.PauseOnFailure,
		Paused:         paused,
		Note:           "",
		Memo: map[string]any{
			MemoScheduleID:   rec.GetScheduleId(),
			MemoScheduleName: rec.GetName(),
			MemoNamespace:    rec.GetNamespace(),
			MemoOrkflowName:  rec.GetOrkflowName(),
		},
	}
	if _, err := s.client.ScheduleClient().Create(ctx, options); err != nil {
		return ScheduleView{}, createError(err)
	}
	view, err := s.Describe(ctx, rec.GetNamespace(), rec.GetName())
	if err != nil {
		// The schedule exists: answering with an error would tell the caller
		// to retry into AlreadyExists. The row is returned from the record,
		// carrying why its live state is missing, on List's terms.
		return ScheduleView{
			ID:          rec.GetScheduleId(),
			Name:        rec.GetName(),
			Namespace:   rec.GetNamespace(),
			OrkflowName: rec.GetOrkflowName(),
			Unavailable: err.Error(),
		}, nil
	}
	return view, nil
}

// createError maps the cluster's answer to Create onto the sentinels.
func createError(err error) error {
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	var exists *serviceerror.AlreadyExists
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) || errors.As(err, &started) || errors.As(err, &exists) {
		return fmt.Errorf("%w: %w", ErrScheduleExists, err)
	}
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		return fmt.Errorf("%w: %s", ErrScheduleSpecRefused, invalid.Error())
	}
	return fmt.Errorf("create schedule: %w", err)
}

// handleError maps a handle call's NotFound onto ErrScheduleNotFound.
func handleError(op string, err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %w", ErrScheduleNotFound, err)
	}
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		return fmt.Errorf("%w: %s", ErrScheduleSpecRefused, invalid.Error())
	}
	return fmt.Errorf("%s schedule: %w", op, err)
}

// action is the schedule's action: ScheduleFireWorkflow with the record as
// its one argument. No execution timeout and no retry policy: a fire is never
// retried by the cluster, and the run has its own policy.
func (s *TemporalScheduler) action(rec *orkstackv1.ScheduleRecord) *client.ScheduleWorkflowAction {
	return &client.ScheduleWorkflowAction{
		ID:        ScheduleFireWorkflowIDPrefix + rec.GetScheduleId(),
		Workflow:  ScheduleFireWorkflowType,
		Args:      []any{rec},
		TaskQueue: s.queue,
	}
}

// specToTemporal is the client.ScheduleSpec of an authored spec. Overlap is a
// policy, not part of the spec, and is converted separately.
func specToTemporal(spec *orkstackv1.ScheduleSpec) client.ScheduleSpec {
	out := client.ScheduleSpec{
		CronExpressions: append([]string(nil), spec.GetCron()...),
		TimeZoneName:    spec.GetTimezone(),
	}
	if spec.GetInterval() != nil {
		out.Intervals = []client.ScheduleIntervalSpec{{Every: spec.GetInterval().AsDuration()}}
	}
	if spec.GetJitter() != nil {
		out.Jitter = spec.GetJitter().AsDuration()
	}
	if spec.GetStartAt() != nil {
		out.StartAt = spec.GetStartAt().AsTime()
	}
	if spec.GetEndAt() != nil {
		out.EndAt = spec.GetEndAt().AsTime()
	}
	return out
}

// policiesToTemporal is the client.SchedulePolicies of an authored spec.
func policiesToTemporal(spec *orkstackv1.ScheduleSpec) (client.SchedulePolicies, error) {
	overlap, err := specOverlapToTemporal(spec.GetOverlap())
	if err != nil {
		return client.SchedulePolicies{}, err
	}
	policies := client.SchedulePolicies{
		Overlap:        overlap,
		PauseOnFailure: spec.GetPauseOnFailure(),
	}
	if spec.GetCatchupWindow() != nil {
		policies.CatchupWindow = spec.GetCatchupWindow().AsDuration()
	}
	return policies, nil
}

// specOverlapToTemporal converts a schedule's AUTHORED overlap
// (ScheduleSpec.overlap) for Create, Update and Describe's drift check.
func specOverlapToTemporal(p orkstackv1.OverlapPolicy) (enumspb.ScheduleOverlapPolicy, error) {
	if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, nil
	}
	return declaredOverlapToTemporal(p)
}

// overrideOverlapToTemporal converts a PER-CALL override
// (FireScheduleRequest.overlap, BackfillScheduleRequest.overlap) for Fire
// and Backfill. UNSPECIFIED passes through as UNSPECIFIED: no override.
func overrideOverlapToTemporal(p orkstackv1.OverlapPolicy) (enumspb.ScheduleOverlapPolicy, error) {
	if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil
	}
	return declaredOverlapToTemporal(p)
}

// declaredOverlapToTemporal maps the five declared non-zero values, one case
// each. Never `enumspb.ScheduleOverlapPolicy(p)`: the numbers are aligned
// today (proto comment), but a cast would forward an undeclared 5 as
// TERMINATE_OTHER, and nothing may ever produce TERMINATE_OTHER.
func declaredOverlapToTemporal(p orkstackv1.OverlapPolicy) (enumspb.ScheduleOverlapPolicy, error) {
	switch p {
	case orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP:
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, nil
	case orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, nil
	case orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ALL:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, nil
	case orkstackv1.OverlapPolicy_OVERLAP_POLICY_CANCEL_OTHER:
		return enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, nil
	case orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL:
		return enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, nil
	default:
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			fmt.Errorf("%w: %d", ErrOverlapPolicyUnknown, int32(p))
	}
}

// Describe reads one schedule, its recent fires included.
func (s *TemporalScheduler) Describe(ctx context.Context, namespace, name string) (ScheduleView, error) {
	desc, err := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).Describe(ctx)
	if err != nil {
		return ScheduleView{}, handleError("describe", err)
	}
	return s.view(ctx, namespace, name, desc)
}

// view builds a ScheduleView out of one description.
func (s *TemporalScheduler) view(ctx context.Context, namespace, name string, desc *client.ScheduleDescription) (ScheduleView, error) {
	if desc == nil {
		return ScheduleView{}, fmt.Errorf("describe schedule %s/%s: empty description", namespace, name)
	}
	rec, err := s.decodeRecord(desc.Schedule.Action)
	if err != nil {
		return ScheduleView{}, fmt.Errorf("describe schedule %s/%s: %w", namespace, name, err)
	}
	// Impossible by construction of the id; treated as absent if it happens.
	if rec.GetNamespace() != namespace || rec.GetName() != name {
		return ScheduleView{}, fmt.Errorf("%w: schedule record names %s/%s", ErrScheduleNotFound,
			rec.GetNamespace(), rec.GetName())
	}
	s.checkDrift(rec, desc.Schedule.Policy)

	view := ScheduleView{
		Record:              rec,
		ID:                  rec.GetScheduleId(),
		Name:                rec.GetName(),
		Namespace:           rec.GetNamespace(),
		OrkflowName:         rec.GetOrkflowName(),
		FiresTaken:          int64(desc.Info.NumActions),
		FiresSkippedOverlap: int64(desc.Info.NumActionsSkippedOverlap),
		FiresMissedCatchup:  int64(desc.Info.NumActionsMissedCatchupWindow),
		CreatedAt:           desc.Info.CreatedAt,
		UpdatedAt:           desc.Info.LastUpdateAt,
	}
	if state := desc.Schedule.State; state != nil {
		view.Paused = state.Paused
		view.Note = state.Note
	}
	// The cluster's own answer, paused or not: a schedule created paused
	// shows the times it would fire at, which is why it was created paused.
	for _, t := range desc.Info.NextActionTimes {
		if len(view.NextFireTimes) == maxScheduleFireTimes {
			break
		}
		view.NextFireTimes = append(view.NextFireTimes, t.UTC())
	}
	view.RecentFires = s.recentFires(ctx, desc.Info.RecentActions)
	return view, nil
}

// decodeRecord reads the ScheduleRecord out of the action's first argument,
// which a description carries still encoded.
func (s *TemporalScheduler) decodeRecord(action client.ScheduleAction) (*orkstackv1.ScheduleRecord, error) {
	wa, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || wa == nil {
		return nil, fmt.Errorf("schedule action is %T, not a workflow action", action)
	}
	if len(wa.Args) == 0 {
		return nil, errors.New("schedule action carries no record")
	}
	switch arg := wa.Args[0].(type) {
	case *orkstackv1.ScheduleRecord:
		return proto.Clone(arg).(*orkstackv1.ScheduleRecord), nil
	case *commonpb.Payload:
		rec := &orkstackv1.ScheduleRecord{}
		if err := s.converter.FromPayload(arg, &rec); err != nil {
			return nil, fmt.Errorf("decode schedule record: %w", err)
		}
		return rec, nil
	default:
		return nil, fmt.Errorf("schedule action argument is %T", arg)
	}
}

// checkDrift logs, at warn, a cluster policy that disagrees with the
// authored spec. It can only happen if someone edited the schedule with the
// temporal CLI; the authored spec is still what every read returns.
func (s *TemporalScheduler) checkDrift(rec *orkstackv1.ScheduleRecord, policy *client.SchedulePolicies) {
	if policy == nil {
		return
	}
	var drift []any
	want, err := specOverlapToTemporal(rec.GetSpec().GetOverlap())
	got := policy.Overlap
	if got == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		got = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP // the cluster's default
	}
	if err != nil || got != want {
		drift = append(drift, "overlap_authored", rec.GetSpec().GetOverlap().String(), "overlap_cluster", policy.Overlap.String())
	}
	if cw := rec.GetSpec().GetCatchupWindow(); cw != nil && cw.AsDuration() != policy.CatchupWindow {
		drift = append(drift, "catchup_authored", cw.AsDuration().String(), "catchup_cluster", policy.CatchupWindow.String())
	}
	if rec.GetSpec().GetPauseOnFailure() != policy.PauseOnFailure {
		drift = append(drift, "pause_on_failure_authored", rec.GetSpec().GetPauseOnFailure(),
			"pause_on_failure_cluster", policy.PauseOnFailure)
	}
	if len(drift) == 0 {
		return
	}
	args := append([]any{"schedule", rec.GetName(), "namespace", rec.GetNamespace()}, drift...)
	s.logger.Warn("schedule policy on the cluster differs from the authored spec; "+
		"it was edited outside OrkStack, and reads return the authored spec", args...)
}

// recentFires describes up to the last ten fires, oldest first, reading each
// fire workflow's memo for the run it started or why it started none.
func (s *TemporalScheduler) recentFires(ctx context.Context, actions []client.ScheduleActionResult) []ScheduleFireView {
	sorted := append([]client.ScheduleActionResult(nil), actions...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ActualTime.Before(sorted[j].ActualTime) })
	if len(sorted) > maxScheduleFireTimes {
		sorted = sorted[len(sorted)-maxScheduleFireTimes:]
	}
	fires := make([]ScheduleFireView, 0, len(sorted))
	for _, action := range sorted {
		fire := ScheduleFireView{ScheduledAt: action.ScheduleTime.UTC(), ActualAt: action.ActualTime.UTC()}
		if started := action.StartWorkflowResult; started != nil && started.WorkflowID != "" {
			fire.RunID, fire.Refusal, fire.DetailsAvailable = s.fireDetails(ctx, started.WorkflowID, started.FirstExecutionRunID)
		}
		fires = append(fires, fire)
	}
	return fires
}

// fireDetails reads one fire workflow's memo. Any failure — the fire retired
// past the cluster's execution retention, or the cluster did not answer —
// reports the details unavailable rather than guessing.
func (s *TemporalScheduler) fireDetails(ctx context.Context, workflowID, runID string) (string, string, bool) {
	resp, err := s.client.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil || resp == nil {
		return "", "", false
	}
	fields := resp.GetWorkflowExecutionInfo().GetMemo().GetFields()
	runIDValue, ok1 := s.memoString(fields, MemoScheduledRunID)
	refusal, ok2 := s.memoString(fields, MemoScheduleFireRefusal)
	if !ok1 || !ok2 {
		return "", "", false
	}
	return runIDValue, refusal, true
}

// memoString decodes one memo field as a string; an absent field is "".
func (s *TemporalScheduler) memoString(fields map[string]*commonpb.Payload, key string) (string, bool) {
	payload, ok := fields[key]
	if !ok || payload == nil {
		return "", true
	}
	var value string
	if err := s.converter.FromPayload(payload, &value); err != nil {
		return "", false
	}
	return value, true
}

// List returns one namespace's schedules, ordered by name, each described on
// the cluster for this call. A schedule the cluster could not describe is
// still listed, carrying Unavailable.
func (s *TemporalScheduler) List(ctx context.Context, namespace string) ([]ScheduleView, error) {
	iter, err := s.client.ScheduleClient().List(ctx, client.ScheduleListOptions{PageSize: scheduleListPageSize})
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	var views []ScheduleView
	for iter.HasNext() {
		entry, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		fields := entry.Memo.GetFields()
		id, _ := s.memoString(fields, MemoScheduleID)
		if id == "" {
			continue // not one of ours
		}
		ns, _ := s.memoString(fields, MemoNamespace)
		if ns != namespace {
			continue
		}
		name, _ := s.memoString(fields, MemoScheduleName)
		orkflow, _ := s.memoString(fields, MemoOrkflowName)
		view, err := s.Describe(ctx, namespace, name)
		if errors.Is(err, ErrScheduleNotFound) {
			// Listed by visibility, which lags, but already deleted: the
			// cluster's own answer for the schedule itself is that it is gone.
			continue
		}
		if err != nil {
			view = ScheduleView{ID: id, Name: name, Namespace: namespace, OrkflowName: orkflow, Unavailable: err.Error()}
		}
		views = append(views, view)
	}
	sort.SliceStable(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views, nil
}

// Update replaces the schedule's record through apply, rebuilding the
// compiled spec, the policy and the action's argument in the one update call
// the SDK issues. State passes through: an update never pauses or resumes.
func (s *TemporalScheduler) Update(
	ctx context.Context,
	namespace, name string,
	apply func(*orkstackv1.ScheduleRecord) error,
) (ScheduleView, error) {
	handle := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name))
	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			current, err := s.decodeRecord(in.Description.Schedule.Action)
			if err != nil {
				return nil, err
			}
			if current.GetNamespace() != namespace || current.GetName() != name {
				return nil, ErrScheduleNotFound
			}
			next := proto.Clone(current).(*orkstackv1.ScheduleRecord)
			if err := apply(next); err != nil {
				return nil, err
			}
			if next.GetScheduleId() != current.GetScheduleId() || next.GetName() != current.GetName() ||
				next.GetNamespace() != current.GetNamespace() || next.GetOrkflowName() != current.GetOrkflowName() {
				return nil, errScheduleIdentityChanged
			}
			return s.scheduleUpdate(next, in.Description.Schedule.State)
		},
	})
	if err != nil {
		if errors.Is(err, ErrOverlapPolicyUnknown) || errors.Is(err, errScheduleIdentityChanged) ||
			errors.Is(err, ErrScheduleNotFound) {
			return ScheduleView{}, err
		}
		return ScheduleView{}, handleError("update", err)
	}
	return s.Describe(ctx, namespace, name)
}

// scheduleUpdate is the full replacement schedule for one record.
func (s *TemporalScheduler) scheduleUpdate(rec *orkstackv1.ScheduleRecord, state *client.ScheduleState) (*client.ScheduleUpdate, error) {
	policies, err := policiesToTemporal(rec.GetSpec())
	if err != nil {
		return nil, err
	}
	spec := specToTemporal(rec.GetSpec())
	return &client.ScheduleUpdate{Schedule: &client.Schedule{
		Action: s.action(rec),
		Spec:   &spec,
		Policy: &policies,
		State:  state,
	}}, nil
}

// Pause stops fires; the reason is kept by the cluster as the note.
func (s *TemporalScheduler) Pause(ctx context.Context, namespace, name, reason string) error {
	note := reason
	if note == "" {
		note = SchedulePauseNote
	}
	err := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).
		Pause(ctx, client.SchedulePauseOptions{Note: note})
	if err != nil {
		return handleError("pause", err)
	}
	return nil
}

// Resume lets a paused schedule fire again.
func (s *TemporalScheduler) Resume(ctx context.Context, namespace, name string) error {
	err := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).
		Unpause(ctx, client.ScheduleUnpauseOptions{Note: ScheduleResumeNote})
	if err != nil {
		return handleError("resume", err)
	}
	return nil
}

// Fire fires once, now. UNSPECIFIED is no override: the schedule's own
// policy applies.
func (s *TemporalScheduler) Fire(ctx context.Context, namespace, name string, overlap orkstackv1.OverlapPolicy) error {
	policy, err := overrideOverlapToTemporal(overlap)
	if err != nil {
		return err
	}
	err = s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).
		Trigger(ctx, client.ScheduleTriggerOptions{Overlap: policy})
	if err != nil {
		return handleError("fire", err)
	}
	return nil
}

// Backfill fires once for every nominal time in [start, end].
func (s *TemporalScheduler) Backfill(
	ctx context.Context,
	namespace, name string,
	start, end time.Time,
	overlap orkstackv1.OverlapPolicy,
) error {
	policy, err := overrideOverlapToTemporal(overlap)
	if err != nil {
		return err
	}
	err = s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).
		Backfill(ctx, client.ScheduleBackfillOptions{Backfill: []client.ScheduleBackfill{{
			Start: start, End: end, Overlap: policy,
		}}})
	if err != nil {
		return handleError("backfill", err)
	}
	return nil
}

// Delete removes the schedule. Runs it already started continue.
func (s *TemporalScheduler) Delete(ctx context.Context, namespace, name string) error {
	if err := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(namespace, name)).Delete(ctx); err != nil {
		return handleError("delete", err)
	}
	return nil
}

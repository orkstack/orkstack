package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The live-attempt read is tested against a synthesized describe and a
// synthesized history, because what has to be right is the derivation: which
// heartbeat belongs to which attempt, and which pending activity is which
// task. The integration tests (cmd/orkctl/inspect_integration_test.go,
// internal/engine/live_integration_test.go) prove the same against a real
// cluster.

const liveRunID = "ork-live-1"

// scheduledExecuteTask is the ACTIVITY_TASK_SCHEDULED event RunWorkflow
// records for one ExecuteTask: its SDK-assigned activity id and the whole
// ExecuteTaskInput as the first (and only) argument.
func scheduledExecuteTask(t *testing.T, eventID int64, activityID string, in *orkstackv1.ExecuteTaskInput) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   activityID,
				ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
				Input:        payloadsOf(t, in),
			},
		},
	}
}

// scheduledPublishEvent is a projection activity's scheduling event: pending
// activities of the same execution that are not attempts.
func scheduledOther(eventID int64, activityID, name string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   activityID,
				ActivityType: &commonpb.ActivityType{Name: name},
			},
		},
	}
}

func fitInput() *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{TaskId: "fit", TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30}
}

func detailsOf(t *testing.T, values ...any) *commonpb.Payloads {
	t.Helper()
	encoded, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	if err != nil {
		t.Fatalf("encode heartbeat details: %v", err)
	}
	return encoded
}

func ts(at time.Time) *timestamppb.Timestamp { return timestamppb.New(at) }

// liveFixture is a fake cluster holding one open execution with the given
// pending activities and history.
func liveFixture(pending []*workflowpb.PendingActivityInfo, events ...*historypb.HistoryEvent) *fakeReadClient {
	return &fakeReadClient{
		history: map[string]*fakeHistory{liveRunID: {events: events}},
		describes: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
			liveRunID: {
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{},
				PendingActivities:     pending,
			},
		},
	}
}

// liveRow is one row of the derivation scenario table (spec, "Test plan").
type liveRow struct {
	name    string
	pending *workflowpb.PendingActivityInfo
	now     time.Time
	want    *orkstackv1.LiveAttempt
}

func liveRows(t *testing.T) []liveRow {
	t0 := time.Date(2026, 9, 23, 9, 20, 11, 0, time.UTC)
	scheduled := t0.Add(-2 * time.Second)
	expires := t0.Add(2 * time.Hour)
	executeTask := &commonpb.ActivityType{Name: task.ActivityExecuteTask}
	running := task.HeartbeatProgress{Phase: "run", ContainerID: "3f2a9c1d0b4e7f00", BytesLogged: 432013312, OrkerID: "orker-7"}
	const (
		unspecified = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNSPECIFIED
		notStarted  = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NOT_STARTED
		noBeatYet   = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NO_HEARTBEAT_YET
		undecodable = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNDECODABLE
		stScheduled = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED
		stStarted   = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED
		stCancel    = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED
		stUnknown   = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED
	)
	base := func(state enumspb.PendingActivityState, attempt int32) *workflowpb.PendingActivityInfo {
		return &workflowpb.PendingActivityInfo{
			ActivityId:     "5",
			ActivityType:   executeTask,
			State:          state,
			Attempt:        attempt,
			ScheduledTime:  ts(scheduled),
			ExpirationTime: ts(expires),
		}
	}
	want := func(state orkstackv1.LiveAttemptState, attempt int32, reason orkstackv1.HeartbeatUnavailableReason) *orkstackv1.LiveAttempt {
		return &orkstackv1.LiveAttempt{
			TaskId: "fit", Attempt: attempt, State: state,
			AttemptScheduledAt: ts(scheduled), ExpiresAt: ts(expires),
			TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30,
			HeartbeatUnavailableReason: reason,
		}
	}
	available := func(w *orkstackv1.LiveAttempt, hb time.Time, p task.HeartbeatProgress, stale bool) *orkstackv1.LiveAttempt {
		w.AttemptStartedAt = ts(t0)
		w.HeartbeatAvailable = true
		w.LastHeartbeatAt = ts(hb)
		w.HeartbeatStale = stale
		w.HeartbeatDeadlineAt = ts(hb.Add(30 * time.Second))
		w.Phase, w.ContainerId, w.BytesLogged, w.OrkerId = p.Phase, p.ContainerID, p.BytesLogged, p.OrkerID
		return w
	}

	var rows []liveRow

	// L1: a healthy running attempt.
	{
		hb := t0.Add(10 * time.Second)
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime, pa.HeartbeatDetails = ts(t0), ts(hb), detailsOf(t, running)
		rows = append(rows, liveRow{"L1 started with a heartbeat", pa, hb.Add(3 * time.Second),
			available(want(stStarted, 1, unspecified), hb, running, false)})
	}
	// L2: started, nothing beat yet.
	{
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime = ts(t0)
		w := want(stStarted, 1, noBeatYet)
		w.AttemptStartedAt = ts(t0)
		rows = append(rows, liveRow{"L2 started without a heartbeat", pa, t0.Add(time.Second), w})
	}
	// L3: attempt 2 started; the cluster still carries attempt 1's detail,
	// recorded BEFORE attempt 2 started. It must not be rendered.
	{
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 2)
		pa.LastStartedTime = ts(t0)
		pa.LastHeartbeatTime = ts(time.Date(2026, 9, 23, 9, 19, 58, 0, time.UTC))
		pa.HeartbeatDetails = detailsOf(t, task.HeartbeatProgress{Phase: "exit", BytesLogged: 9000})
		w := want(stStarted, 2, noBeatYet)
		w.AttemptStartedAt = ts(t0)
		rows = append(rows, liveRow{"L3 a previous attempt's carried-over detail", pa, t0.Add(time.Second), w})
	}
	// L4: scheduled, no Orker has it.
	{
		pa := base(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, 1)
		rows = append(rows, liveRow{"L4 scheduled, waiting for an orker", pa, t0,
			want(stScheduled, 1, notStarted)})
	}
	// L5: attempt 3 waiting out its backoff, attempt 2's start and detail
	// still on the record.
	{
		now := t0.Add(time.Minute)
		pa := base(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, 3)
		pa.NextAttemptScheduleTime = ts(now.Add(42 * time.Second))
		pa.LastStartedTime = ts(t0)
		pa.LastHeartbeatTime = ts(t0.Add(5 * time.Second))
		pa.HeartbeatDetails = detailsOf(t, task.HeartbeatProgress{Phase: "exit", BytesLogged: 9000, OrkerID: "orker-7"})
		w := want(stScheduled, 3, notStarted)
		w.NextAttemptAt = ts(now.Add(42 * time.Second))
		rows = append(rows, liveRow{"L5 a retry waiting out its backoff", pa, now, w})
	}
	// L6: stale — 24 s since the last beat, 6 s before Temporal acts.
	{
		hb := t0.Add(5 * time.Second)
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime, pa.HeartbeatDetails = ts(t0), ts(hb), detailsOf(t, running)
		rows = append(rows, liveRow{"L6 a stale heartbeat", pa, hb.Add(24 * time.Second),
			available(want(stStarted, 1, unspecified), hb, running, true)})
	}
	// L7: a beat from this attempt that is not the Orker's shape.
	{
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime = ts(t0), ts(t0.Add(5*time.Second))
		pa.HeartbeatDetails = detailsOf(t, map[string]int{"foo": 1})
		w := want(stStarted, 1, undecodable)
		w.AttemptStartedAt = ts(t0)
		rows = append(rows, liveRow{"L7 an undecodable detail", pa, t0.Add(6 * time.Second), w})
	}
	// L8: cancel requested; the Orker keeps beating through the grace.
	{
		hb := t0.Add(40 * time.Second)
		cancelling := task.HeartbeatProgress{Phase: "cancel", ContainerID: "3f2a9c1d0b4e7f00", BytesLogged: 100, OrkerID: "orker-7"}
		pa := base(enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime, pa.HeartbeatDetails = ts(t0), ts(hb), detailsOf(t, cancelling)
		rows = append(rows, liveRow{"L8 cancel requested", pa, hb.Add(time.Second),
			available(want(stCancel, 1, unspecified), hb, cancelling, false)})
	}
	// L9: an old Orker's beat — no orker_id, no container.
	{
		hb := t0.Add(5 * time.Second)
		old := task.HeartbeatProgress{Phase: "image"}
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime = ts(t0), ts(hb)
		pa.HeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
			Data:     []byte(`{"phase":"image"}`),
		}}}
		rows = append(rows, liveRow{"L9 an old orker's beat", pa, hb.Add(time.Second),
			available(want(stStarted, 1, unspecified), hb, old, false)})
	}
	// L10: PAUSED has no OrkStack member.
	{
		pa := base(enumspb.PENDING_ACTIVITY_STATE_PAUSED, 1)
		pa.LastStartedTime = ts(t0)
		rows = append(rows, liveRow{"L10 paused", pa, t0, want(stUnknown, 1, notStarted)})
	}
	// L11: exactly at the threshold is not stale.
	{
		hb := t0.Add(5 * time.Second)
		pa := base(enumspb.PENDING_ACTIVITY_STATE_STARTED, 1)
		pa.LastStartedTime, pa.LastHeartbeatTime, pa.HeartbeatDetails = ts(t0), ts(hb), detailsOf(t, running)
		rows = append(rows, liveRow{"L11 exactly at the stale threshold", pa, hb.Add(task.HeartbeatStaleAfter),
			available(want(stStarted, 1, unspecified), hb, running, false)})
	}
	return rows
}

// TestLiveAttempts_DerivesEveryScenarioOfTheTable: L1–L11 through the
// real reader over a fake cluster, every field of the derivation asserted.
func TestLiveAttempts_DerivesEveryScenarioOfTheTable(t *testing.T) {
	for _, row := range liveRows(t) {
		t.Run(row.name, func(t *testing.T) {
			fake := liveFixture([]*workflowpb.PendingActivityInfo{row.pending},
				scheduledExecuteTask(t, 5, "5", fitInput()))
			live, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, row.now)
			if err != nil {
				t.Fatalf("LiveAttempts: %v", err)
			}
			if live.Closed || len(live.Attempts) != 1 {
				t.Fatalf("live = %+v, want one open attempt", live)
			}
			got := live.Attempts[0]
			assertLiveAttempt(t, got, row.want)
			t.Logf("%s: state=%s attempt=%d available=%v reason=%s phase=%q bytes=%d orker=%q stale=%v deadline=%s",
				row.name, got.GetState(), got.GetAttempt(), got.GetHeartbeatAvailable(),
				got.GetHeartbeatUnavailableReason(), got.GetPhase(), got.GetBytesLogged(), got.GetOrkerId(),
				got.GetHeartbeatStale(), fmtTS(got.GetHeartbeatDeadlineAt()))
		})
	}
}

func fmtTS(at *timestamppb.Timestamp) string {
	if at == nil {
		return "unset"
	}
	return at.AsTime().Format(time.RFC3339)
}

func assertLiveAttempt(t *testing.T, got, want *orkstackv1.LiveAttempt) {
	t.Helper()
	check := func(field string, g, w any) {
		if fmt.Sprint(g) != fmt.Sprint(w) {
			t.Errorf("%s = %v, want %v", field, g, w)
		}
	}
	check("task_id", got.GetTaskId(), want.GetTaskId())
	check("attempt", got.GetAttempt(), want.GetAttempt())
	check("state", got.GetState(), want.GetState())
	check("attempt_scheduled_at", fmtTS(got.GetAttemptScheduledAt()), fmtTS(want.GetAttemptScheduledAt()))
	check("attempt_started_at", fmtTS(got.GetAttemptStartedAt()), fmtTS(want.GetAttemptStartedAt()))
	check("next_attempt_at", fmtTS(got.GetNextAttemptAt()), fmtTS(want.GetNextAttemptAt()))
	check("timeout_seconds", got.GetTimeoutSeconds(), want.GetTimeoutSeconds())
	check("cancellation_grace_period_seconds", got.GetCancellationGracePeriodSeconds(), want.GetCancellationGracePeriodSeconds())
	check("expires_at", fmtTS(got.GetExpiresAt()), fmtTS(want.GetExpiresAt()))
	check("heartbeat_available", got.GetHeartbeatAvailable(), want.GetHeartbeatAvailable())
	check("heartbeat_unavailable_reason", got.GetHeartbeatUnavailableReason(), want.GetHeartbeatUnavailableReason())
	check("last_heartbeat_at", fmtTS(got.GetLastHeartbeatAt()), fmtTS(want.GetLastHeartbeatAt()))
	check("heartbeat_stale", got.GetHeartbeatStale(), want.GetHeartbeatStale())
	check("heartbeat_deadline_at", fmtTS(got.GetHeartbeatDeadlineAt()), fmtTS(want.GetHeartbeatDeadlineAt()))
	check("phase", got.GetPhase(), want.GetPhase())
	check("container_id", got.GetContainerId(), want.GetContainerId())
	check("bytes_logged", got.GetBytesLogged(), want.GetBytesLogged())
	check("orker_id", got.GetOrkerId(), want.GetOrkerId())
}

// TestLiveAttempts_MapsPendingActivitiesToTasksThroughTheSchedulingEvent is
// SDK-assigned activity ids map to task ids through their scheduling
// events, and the engine's own pending activities are not attempts.
func TestLiveAttempts_MapsPendingActivitiesToTasksThroughTheSchedulingEvent(t *testing.T) {
	executeTask := &commonpb.ActivityType{Name: task.ActivityExecuteTask}
	pending := []*workflowpb.PendingActivityInfo{
		{ActivityId: "3", ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent}, State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED},
		{ActivityId: "5", ActivityType: executeTask, State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
		{ActivityId: "7", ActivityType: &commonpb.ActivityType{Name: task.ActivityDescribeOrker}, State: enumspb.PENDING_ACTIVITY_STATE_STARTED},
		{ActivityId: "9", ActivityType: executeTask, State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
		{ActivityId: "11", ActivityType: &commonpb.ActivityType{Name: task.ActivityReleasePin}, State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED},
	}
	fake := liveFixture(pending,
		scheduledOther(3, "3", ActivityPublishEvent),
		scheduledExecuteTask(t, 5, "5", &orkstackv1.ExecuteTaskInput{TaskId: "fetch", TimeoutSeconds: 60}),
		scheduledOther(7, "7", task.ActivityDescribeOrker),
		scheduledExecuteTask(t, 9, "9", &orkstackv1.ExecuteTaskInput{TaskId: "transform", TimeoutSeconds: 120}),
		scheduledOther(11, "11", task.ActivityReleasePin),
	)
	live, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err != nil {
		t.Fatalf("LiveAttempts: %v", err)
	}
	if len(live.Attempts) != 2 {
		t.Fatalf("attempts = %d, want the two ExecuteTask activities only", len(live.Attempts))
	}
	if live.Attempts[0].GetTaskId() != "fetch" || live.Attempts[0].GetTimeoutSeconds() != 60 ||
		live.Attempts[1].GetTaskId() != "transform" || live.Attempts[1].GetTimeoutSeconds() != 120 {
		t.Fatalf("attempts = %v, want fetch(60s) then transform(120s)", live.Attempts)
	}
}

// countingConverter counts FromPayload calls so a test can prove which inputs
// the reader decoded.
type countingConverter struct {
	converter.DataConverter
	fromPayload int
}

func (c *countingConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	c.fromPayload++
	return c.DataConverter.FromPayload(payload, valuePtr)
}

// TestLiveAttempts_DecodesOnlyThePendingActivitiesInputs: three tasks
// were scheduled, one is pending, one input is decoded.
func TestLiveAttempts_DecodesOnlyThePendingActivitiesInputs(t *testing.T) {
	pending := []*workflowpb.PendingActivityInfo{{
		ActivityId: "9", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
		State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1,
	}}
	fake := liveFixture(pending,
		scheduledExecuteTask(t, 5, "5", &orkstackv1.ExecuteTaskInput{TaskId: "a"}),
		scheduledExecuteTask(t, 7, "7", &orkstackv1.ExecuteTaskInput{TaskId: "b"}),
		scheduledExecuteTask(t, 9, "9", &orkstackv1.ExecuteTaskInput{TaskId: "c"}),
	)
	counting := &countingConverter{DataConverter: converter.GetDefaultDataConverter()}
	live, err := newRunReader(fake, counting).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err != nil {
		t.Fatalf("LiveAttempts: %v", err)
	}
	if len(live.Attempts) != 1 || live.Attempts[0].GetTaskId() != "c" {
		t.Fatalf("attempts = %v, want task c", live.Attempts)
	}
	if counting.fromPayload != 1 {
		t.Fatalf("FromPayload called %d times, want exactly 1 (only the pending activity's input)", counting.fromPayload)
	}
	t.Logf("3 ExecuteTask scheduling events, 1 pending: FromPayload called %d time(s)", counting.fromPayload)
}

// TestLiveAttempts_AClosedExecutionReadsNoHistory.
func TestLiveAttempts_AClosedExecutionReadsNoHistory(t *testing.T) {
	fake := liveFixture(nil)
	fake.describes[liveRunID].WorkflowExecutionInfo.CloseTime = ts(readerEpoch)
	live, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err != nil {
		t.Fatalf("LiveAttempts: %v", err)
	}
	if !live.Closed || len(live.Attempts) != 0 {
		t.Fatalf("live = %+v, want closed and empty", live)
	}
	if got := fake.count("history"); got != 0 {
		t.Fatalf("history reads = %d for a closed execution, want 0", got)
	}
	t.Logf("closed execution: describes=%d history reads=%d", fake.count("describe"), fake.count("history"))
}

// TestLiveAttempts_NothingPendingReadsNoHistory.
func TestLiveAttempts_NothingPendingReadsNoHistory(t *testing.T) {
	fake := liveFixture([]*workflowpb.PendingActivityInfo{{
		ActivityId: "3", ActivityType: &commonpb.ActivityType{Name: ActivityPublishEvent},
	}})
	live, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err != nil {
		t.Fatalf("LiveAttempts: %v", err)
	}
	if live.Closed || len(live.Attempts) != 0 {
		t.Fatalf("live = %+v, want open and empty", live)
	}
	if got := fake.count("history"); got != 0 {
		t.Fatalf("history reads = %d with nothing pending, want 0", got)
	}
}

// TestLiveAttempts_NotFoundIsErrRunNotFound.
func TestLiveAttempts_NotFoundIsErrRunNotFound(t *testing.T) {
	fake := liveFixture(nil)
	_, err := newRunReader(fake, nil).LiveAttempts(context.Background(), "ork-missing", time.Now())
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("err = %v, want ErrRunNotFound", err)
	}
	transport := fmt.Errorf("connection refused")
	fake.describeErr = transport
	_, err = newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err == nil || errors.Is(err, ErrRunNotFound) || !errors.Is(err, transport) {
		t.Fatalf("err = %v, want the transport error itself, never absence", err)
	}
	fake.describeErr = serviceerror.NewUnavailable("frontend down")
	_, err = newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err == nil || errors.Is(err, ErrRunNotFound) {
		t.Fatalf("err = %v, want an unavailable cluster to be an error, not absence", err)
	}
}

// TestLiveAttempts_APendingActivityWithoutASchedulingEventFails.
func TestLiveAttempts_APendingActivityWithoutASchedulingEventFails(t *testing.T) {
	fake := liveFixture([]*workflowpb.PendingActivityInfo{{
		ActivityId: "13", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
		State: enumspb.PENDING_ACTIVITY_STATE_STARTED,
	}}, scheduledExecuteTask(t, 5, "5", fitInput()))
	live, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if err == nil {
		t.Fatalf("live = %+v, want an error for a pending activity with no scheduling event", live)
	}
	t.Logf("invariant broken → %v", err)
}

// TestLiveAttempts_DescribesBeforeItReadsHistory.
func TestLiveAttempts_DescribesBeforeItReadsHistory(t *testing.T) {
	fake := liveFixture([]*workflowpb.PendingActivityInfo{{
		ActivityId: "5", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
		State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
	}}, scheduledExecuteTask(t, 5, "5", fitInput()))
	if _, err := newRunReader(fake, nil).LiveAttempts(context.Background(), liveRunID, time.Now()); err != nil {
		t.Fatalf("LiveAttempts: %v", err)
	}
	if fmt.Sprint(fake.calls) != "[describe history]" {
		t.Fatalf("calls = %v, want [describe history]", fake.calls)
	}
}

// TestLiveAttemptState_MapsEveryTemporalValueExplicitly: a Temporal
// upgrade that adds a pending-activity state fails here until the switch
// names it.
func TestLiveAttemptState_MapsEveryTemporalValueExplicitly(t *testing.T) {
	want := map[enumspb.PendingActivityState]orkstackv1.LiveAttemptState{
		enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED:      orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED,
		enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:        orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED,
		enumspb.PENDING_ACTIVITY_STATE_STARTED:          orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED,
		enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED,
		enumspb.PENDING_ACTIVITY_STATE_PAUSED:           orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED,
		enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED:  orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED,
	}
	for number, name := range enumspb.PendingActivityState_name {
		state := enumspb.PendingActivityState(number)
		if !liveAttemptStateNamed[state] {
			t.Errorf("Temporal's %s (%d) is not named by liveAttemptState's switch", name, number)
			continue
		}
		if got := liveAttemptState(state); got != want[state] {
			t.Errorf("liveAttemptState(%s) = %s, want %s", name, got, want[state])
		}
	}
}

// TestLiveAttempts_ANilClientIsErrNoLiveReader.
func TestLiveAttempts_ANilClientIsErrNoLiveReader(t *testing.T) {
	_, err := newRunReader(nil, nil).LiveAttempts(context.Background(), liveRunID, time.Now())
	if !errors.Is(err, ErrNoLiveReader) {
		t.Fatalf("err = %v, want ErrNoLiveReader", err)
	}
}

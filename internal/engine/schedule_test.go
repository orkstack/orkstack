package engine_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// ---- the conversions ----

func TestTemporalScheduler_IDsAreDerivedAndSeparated(t *testing.T) {
	a := engine.TemporalScheduleID("a", "b-c")
	b := engine.TemporalScheduleID("a-b", "c")
	if a == b {
		t.Fatalf("(a, b-c) and (a-b, c) collide on %q", a)
	}
	if a != "ork-schedule/a/b-c" || b != "ork-schedule/a-b/c" {
		t.Fatalf("ids = %q, %q; want ork-schedule/a/b-c, ork-schedule/a-b/c", a, b)
	}
	if engine.ScheduleFireWorkflowIDPrefix != "ork-schedule-fire/" {
		t.Fatalf("fire prefix = %q", engine.ScheduleFireWorkflowIDPrefix)
	}
}

func TestTemporalScheduler_SpecToTemporal(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	cases := []struct {
		name string
		spec *orkstackv1.ScheduleSpec
		want client.ScheduleSpec
	}{
		{
			name: "cron only",
			spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *", "@hourly"}},
			want: client.ScheduleSpec{CronExpressions: []string{"0 2 * * *", "@hourly"}},
		},
		{
			name: "interval only, epoch aligned",
			spec: &orkstackv1.ScheduleSpec{Interval: durationpb.New(15 * time.Minute)},
			want: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 15 * time.Minute}}},
		},
		{
			name: "everything",
			spec: &orkstackv1.ScheduleSpec{
				Cron: []string{"0 2 * * *"}, Interval: durationpb.New(time.Hour), Timezone: "Europe/Lisbon",
				Jitter: durationpb.New(30 * time.Second), StartAt: timestamppb.New(start), EndAt: timestamppb.New(end),
			},
			want: client.ScheduleSpec{
				CronExpressions: []string{"0 2 * * *"}, Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour}},
				TimeZoneName: "Europe/Lisbon", Jitter: 30 * time.Second, StartAt: start, EndAt: end,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := engine.SpecToTemporal(tc.spec)
			if strings.Join(got.CronExpressions, "|") != strings.Join(tc.want.CronExpressions, "|") ||
				len(got.Intervals) != len(tc.want.Intervals) || got.TimeZoneName != tc.want.TimeZoneName ||
				got.Jitter != tc.want.Jitter || !got.StartAt.Equal(tc.want.StartAt) || !got.EndAt.Equal(tc.want.EndAt) {
				t.Fatalf("SpecToTemporal = %+v, want %+v", got, tc.want)
			}
			for i := range got.Intervals {
				if got.Intervals[i] != tc.want.Intervals[i] {
					t.Fatalf("interval %d = %+v, want %+v", i, got.Intervals[i], tc.want.Intervals[i])
				}
			}
		})
	}

	// The policy fields travel on ScheduleOptions: catch-up and
	// pause-on-failure as set; unset catch-up is zero, the SDK's "server
	// default".
	for _, tc := range []struct {
		name    string
		spec    *orkstackv1.ScheduleSpec
		catchup time.Duration
		pof     bool
	}{
		{"unset catch-up", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}}, 0, false},
		{"set catch-up and pause", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"},
			CatchupWindow: durationpb.New(time.Hour), PauseOnFailure: true}, time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newFakeScheduleCluster()
			s := engine.NewTemporalScheduler(cluster, "")
			if _, err := s.Create(context.Background(), scheduleRecord("n", "s", tc.spec), false); err != nil {
				t.Fatalf("Create: %v", err)
			}
			got := cluster.sc.created[0]
			if got.CatchupWindow != tc.catchup || got.PauseOnFailure != tc.pof {
				t.Fatalf("options catch-up %v pause %v; want %v %v", got.CatchupWindow, got.PauseOnFailure, tc.catchup, tc.pof)
			}
		})
	}
}

// expectedOverlap is the hand-written overlap table, against the real
// enumspb constants.
var expectedSpecOverlap = map[orkstackv1.OverlapPolicy]enumspb.ScheduleOverlapPolicy{
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED:  enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP:         enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE:   enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ALL:   enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_CANCEL_OTHER: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
	orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL:    enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
}

func TestSpecOverlapToTemporal_EveryDeclaredValue(t *testing.T) {
	for number, name := range orkstackv1.OverlapPolicy_name {
		p := orkstackv1.OverlapPolicy(number)
		want, ok := expectedSpecOverlap[p]
		if !ok {
			t.Errorf("declared value %s has no row in the expected table", name)
			continue
		}
		got, err := engine.SpecOverlapToTemporal(p)
		if err != nil || got != want {
			t.Errorf("SpecOverlapToTemporal(%s) = %v, %v; want %v", name, got, err, want)
		}
	}
	got, _ := engine.SpecOverlapToTemporal(orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL)
	if int32(got) != 6 {
		t.Fatalf("ALLOW_ALL converts to %d, want 6", int32(got))
	}
	for _, bad := range []int32{5, 99} {
		if _, err := engine.SpecOverlapToTemporal(orkstackv1.OverlapPolicy(bad)); !errors.Is(err, engine.ErrOverlapPolicyUnknown) {
			t.Errorf("SpecOverlapToTemporal(%d) err = %v, want ErrOverlapPolicyUnknown", bad, err)
		}
	}
}

func TestOverrideOverlapToTemporal_UnspecifiedIsNoOverride(t *testing.T) {
	for number, name := range orkstackv1.OverlapPolicy_name {
		p := orkstackv1.OverlapPolicy(number)
		want, ok := expectedSpecOverlap[p]
		if !ok {
			t.Errorf("declared value %s has no row in the expected table", name)
			continue
		}
		if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
			want = enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
		}
		got, err := engine.OverrideOverlapToTemporal(p)
		if err != nil || got != want {
			t.Errorf("OverrideOverlapToTemporal(%s) = %v, %v; want %v", name, got, err, want)
		}
	}
	got, _ := engine.OverrideOverlapToTemporal(orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL)
	if int32(got) != 6 {
		t.Fatalf("ALLOW_ALL converts to %d, want 6", int32(got))
	}
	for _, bad := range []int32{5, 99} {
		if _, err := engine.OverrideOverlapToTemporal(orkstackv1.OverlapPolicy(bad)); !errors.Is(err, engine.ErrOverlapPolicyUnknown) {
			t.Errorf("OverrideOverlapToTemporal(%d) err = %v, want ErrOverlapPolicyUnknown", bad, err)
		}
	}
}

func TestOverlapConversion_NeverProducesTerminateOther(t *testing.T) {
	for n := int32(-1); n <= 16; n++ {
		p := orkstackv1.OverlapPolicy(n)
		_, declared := orkstackv1.OverlapPolicy_name[n]
		for name, convert := range map[string]func(orkstackv1.OverlapPolicy) (enumspb.ScheduleOverlapPolicy, error){
			"spec": engine.SpecOverlapToTemporal, "override": engine.OverrideOverlapToTemporal,
		} {
			got, err := convert(p)
			if got == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
				t.Errorf("%s(%d) produced TERMINATE_OTHER", name, n)
			}
			if !declared && err == nil {
				t.Errorf("%s(%d): undeclared value converted to %v instead of an error", name, n, got)
			}
			if declared && err != nil {
				t.Errorf("%s(%d): declared value refused: %v", name, n, err)
			}
		}
	}
}

func TestOverlapPolicy_EnumHasNoTerminate(t *testing.T) {
	desc := orkstackv1.OverlapPolicy(0).Descriptor()
	values := desc.Values()
	for i := 0; i < values.Len(); i++ {
		if strings.Contains(string(values.Get(i).Name()), "TERMINATE") {
			t.Errorf("OverlapPolicy declares %s", values.Get(i).Name())
		}
	}
	if !desc.ReservedRanges().Has(5) {
		t.Errorf("OverlapPolicy does not reserve 5")
	}
	if !desc.ReservedNames().Has("OVERLAP_POLICY_TERMINATE_OTHER") {
		t.Errorf("OverlapPolicy does not reserve the name OVERLAP_POLICY_TERMINATE_OTHER")
	}
}

func TestOverlapPolicy_NumbersMatchTemporal(t *testing.T) {
	for number, name := range orkstackv1.OverlapPolicy_name {
		if number == 0 {
			continue
		}
		temporalNumber, ok := enumspb.ScheduleOverlapPolicy_value["SCHEDULE_"+name]
		if !ok {
			t.Errorf("Temporal has no SCHEDULE_%s", name)
			continue
		}
		if temporalNumber != number {
			t.Errorf("%s is %d here and %d in Temporal", name, number, temporalNumber)
		}
	}
}

// ---- the scheduler over a fake cluster ----

var dc = converter.GetDefaultDataConverter()

func mustPayload(t testing.TB, v any) *commonpb.Payload {
	t.Helper()
	p, err := dc.ToPayload(v)
	if err != nil {
		t.Fatalf("encode %v: %v", v, err)
	}
	return p
}

func scheduleRecord(namespace, name string, spec *orkstackv1.ScheduleSpec) *orkstackv1.ScheduleRecord {
	return &orkstackv1.ScheduleRecord{
		ScheduleId: "sch-00000000000000" + name[:1] + "1", Name: name, Namespace: namespace,
		OrkflowName: "reconcile", Spec: spec,
	}
}

// fakeScheduleCluster stands in for the SDK client: a ScheduleClient and the
// fire workflows' descriptions.
type fakeScheduleCluster struct {
	client.Client
	sc    *fakeScheduleClient
	fires map[string]*workflowservice.DescribeWorkflowExecutionResponse
}

func newFakeScheduleCluster() *fakeScheduleCluster {
	return &fakeScheduleCluster{
		sc:    &fakeScheduleClient{handles: map[string]*fakeScheduleHandle{}},
		fires: map[string]*workflowservice.DescribeWorkflowExecutionResponse{},
	}
}

func (c *fakeScheduleCluster) ScheduleClient() client.ScheduleClient { return c.sc }

func (c *fakeScheduleCluster) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	resp, ok := c.fires[workflowID]
	if !ok {
		return nil, serviceerror.NewNotFound("workflow not found")
	}
	return resp, nil
}

func (c *fakeScheduleCluster) fire(t testing.TB, workflowID string, memo map[string]string) {
	fields := map[string]*commonpb.Payload{}
	for k, v := range memo {
		fields[k] = mustPayload(t, v)
	}
	c.fires[workflowID] = &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Memo: &commonpb.Memo{Fields: fields}},
	}
}

type fakeScheduleClient struct {
	created   []client.ScheduleOptions
	createErr error
	handles   map[string]*fakeScheduleHandle
	entries   []*client.ScheduleListEntry
}

func (c *fakeScheduleClient) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	c.created = append(c.created, options)
	if c.createErr != nil {
		return nil, c.createErr
	}
	h := &fakeScheduleHandle{id: options.ID, desc: describedFrom(options)}
	c.handles[options.ID] = h
	return h, nil
}

func (c *fakeScheduleClient) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	if h, ok := c.handles[id]; ok {
		return h
	}
	return &fakeScheduleHandle{id: id, err: serviceerror.NewNotFound("schedule not found")}
}

func (c *fakeScheduleClient) List(context.Context, client.ScheduleListOptions) (client.ScheduleListIterator, error) {
	return &fakeListIterator{entries: c.entries}, nil
}

type fakeListIterator struct{ entries []*client.ScheduleListEntry }

func (i *fakeListIterator) HasNext() bool { return len(i.entries) > 0 }
func (i *fakeListIterator) Next() (*client.ScheduleListEntry, error) {
	e := i.entries[0]
	i.entries = i.entries[1:]
	return e, nil
}

// describedFrom is what the cluster would describe for created options: the
// action's arguments come back still encoded, as on a real describe.
func describedFrom(options client.ScheduleOptions) *client.ScheduleDescription {
	action := *options.Action.(*client.ScheduleWorkflowAction)
	args := make([]any, len(action.Args))
	for i, a := range action.Args {
		p, _ := dc.ToPayload(a)
		args[i] = p
	}
	action.Args = args
	spec := options.Spec
	return &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &action,
			Spec:   &spec,
			Policy: &client.SchedulePolicies{Overlap: options.Overlap, CatchupWindow: options.CatchupWindow, PauseOnFailure: options.PauseOnFailure},
			State:  &client.ScheduleState{Note: options.Note, Paused: options.Paused},
		},
	}
}

type fakeScheduleHandle struct {
	id    string
	desc  *client.ScheduleDescription
	err   error
	calls []string

	pause    client.SchedulePauseOptions
	unpause  client.ScheduleUnpauseOptions
	trigger  client.ScheduleTriggerOptions
	backfill client.ScheduleBackfillOptions
	updated  *client.ScheduleUpdate
}

func (h *fakeScheduleHandle) GetID() string { return h.id }
func (h *fakeScheduleHandle) Delete(context.Context) error {
	h.calls = append(h.calls, "delete")
	return h.err
}
func (h *fakeScheduleHandle) Backfill(_ context.Context, o client.ScheduleBackfillOptions) error {
	h.calls = append(h.calls, "backfill")
	h.backfill = o
	return h.err
}
func (h *fakeScheduleHandle) Update(_ context.Context, o client.ScheduleUpdateOptions) error {
	h.calls = append(h.calls, "update")
	if h.err != nil {
		return h.err
	}
	u, err := o.DoUpdate(client.ScheduleUpdateInput{Description: *h.desc})
	if err != nil {
		return err
	}
	h.updated = u
	h.desc = describedFrom(client.ScheduleOptions{
		Action: u.Schedule.Action, Spec: *u.Schedule.Spec, Overlap: u.Schedule.Policy.Overlap,
		CatchupWindow: u.Schedule.Policy.CatchupWindow, PauseOnFailure: u.Schedule.Policy.PauseOnFailure,
		Paused: u.Schedule.State.Paused, Note: u.Schedule.State.Note,
	})
	return nil
}
func (h *fakeScheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	if h.err != nil {
		return nil, h.err
	}
	return h.desc, nil
}
func (h *fakeScheduleHandle) Trigger(_ context.Context, o client.ScheduleTriggerOptions) error {
	h.calls = append(h.calls, "trigger")
	h.trigger = o
	return h.err
}
func (h *fakeScheduleHandle) Pause(_ context.Context, o client.SchedulePauseOptions) error {
	h.calls = append(h.calls, "pause")
	h.pause = o
	return h.err
}
func (h *fakeScheduleHandle) Unpause(_ context.Context, o client.ScheduleUnpauseOptions) error {
	h.calls = append(h.calls, "unpause")
	h.unpause = o
	return h.err
}

func TestTemporalScheduler_CreateIsOneCallAndMapsErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		policy orkstackv1.OverlapPolicy
		want   enumspb.ScheduleOverlapPolicy
	}{
		{orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
		{orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
	} {
		cluster := newFakeScheduleCluster()
		s := engine.NewTemporalScheduler(cluster, "")
		rec := scheduleRecord("payments", "nightly", &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: tc.policy})
		view, err := s.Create(ctx, rec, true)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(cluster.sc.created) != 1 {
			t.Fatalf("Create made %d cluster calls, want 1", len(cluster.sc.created))
		}
		opts := cluster.sc.created[0]
		if opts.Overlap != tc.want {
			t.Errorf("overlap %v sent as %v, want %v", tc.policy, opts.Overlap, tc.want)
		}
		if opts.ID != "ork-schedule/payments/nightly" || !opts.Paused {
			t.Errorf("options id %q paused %v", opts.ID, opts.Paused)
		}
		action := opts.Action.(*client.ScheduleWorkflowAction)
		if action.ID != engine.ScheduleFireWorkflowIDPrefix+rec.GetScheduleId() ||
			action.Workflow != engine.ScheduleFireWorkflowType || action.TaskQueue != task.DefaultQueue ||
			action.RetryPolicy != nil || action.WorkflowExecutionTimeout != 0 {
			t.Errorf("action = %+v", action)
		}
		if !proto.Equal(action.Args[0].(*orkstackv1.ScheduleRecord), rec) {
			t.Errorf("action argument is not the record")
		}
		for k, want := range map[string]string{
			engine.MemoScheduleID: rec.GetScheduleId(), engine.MemoScheduleName: "nightly",
			engine.MemoNamespace: "payments", engine.MemoOrkflowName: "reconcile",
		} {
			if opts.Memo[k] != want {
				t.Errorf("memo[%s] = %v, want %q", k, opts.Memo[k], want)
			}
		}
		if !proto.Equal(view.Record, rec) || !view.Paused {
			t.Errorf("view = %+v", view)
		}
	}

	// A reserved overlap never reaches the cluster.
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	_, err := s.Create(ctx, scheduleRecord("n", "s", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Overlap: 5}), false)
	if !errors.Is(err, engine.ErrOverlapPolicyUnknown) || len(cluster.sc.created) != 0 {
		t.Fatalf("Create(overlap 5) = %v after %d calls; want ErrOverlapPolicyUnknown and none", err, len(cluster.sc.created))
	}

	for _, tc := range []struct {
		clusterErr error
		want       error
	}{
		{temporal.ErrScheduleAlreadyRunning, engine.ErrScheduleExists},
		{serviceerror.NewWorkflowExecutionAlreadyStarted("exists", "", ""), engine.ErrScheduleExists},
		{serviceerror.NewInvalidArgument("invalid cron string: \"0 2 * *\""), engine.ErrScheduleSpecRefused},
	} {
		cluster := newFakeScheduleCluster()
		cluster.sc.createErr = tc.clusterErr
		_, err := engine.NewTemporalScheduler(cluster, "").Create(ctx,
			scheduleRecord("n", "s", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}}), false)
		if !errors.Is(err, tc.want) {
			t.Errorf("cluster %v mapped to %v, want %v", tc.clusterErr, err, tc.want)
		}
		if tc.want == engine.ErrScheduleSpecRefused && !strings.Contains(err.Error(), `invalid cron string: "0 2 * *"`) {
			t.Errorf("refusal %q does not quote the cluster verbatim", err)
		}
	}
}

func TestTemporalScheduler_DescribeDecodesRecordAndState(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "").WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	rec := scheduleRecord("payments", "nightly", &orkstackv1.ScheduleSpec{
		Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL})
	if _, err := s.Create(ctx, rec, false); err != nil {
		t.Fatalf("Create: %v", err)
	}
	h := cluster.sc.handles["ork-schedule/payments/nightly"]
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	next := []time.Time{time.Date(2026, 9, 25, 1, 0, 0, 0, time.UTC), time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC)}
	h.desc.Schedule.State = &client.ScheduleState{Paused: true, Note: "Q3 close"}
	h.desc.Info = client.ScheduleInfo{
		NumActions: 142, NumActionsSkippedOverlap: 3, NumActionsMissedCatchupWindow: 1,
		NextActionTimes: next, CreatedAt: created, LastUpdateAt: created.Add(time.Hour),
	}
	logs.Reset()
	view, err := s.Describe(ctx, "payments", "nightly")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if !proto.Equal(view.Record, rec) || view.ID != rec.GetScheduleId() || view.OrkflowName != "reconcile" {
		t.Errorf("record/identity = %+v", view)
	}
	if !view.Paused || view.Note != "Q3 close" {
		t.Errorf("state paused=%v note=%q", view.Paused, view.Note)
	}
	if len(view.NextFireTimes) != 2 || !view.NextFireTimes[0].Equal(next[0]) {
		t.Errorf("a paused schedule reports next fires %v, want the cluster's %v", view.NextFireTimes, next)
	}
	if view.FiresTaken != 142 || view.FiresSkippedOverlap != 3 || view.FiresMissedCatchup != 1 ||
		!view.CreatedAt.Equal(created) || !view.UpdatedAt.Equal(created.Add(time.Hour)) {
		t.Errorf("counters/times = %+v", view)
	}
	if logs.Len() != 0 {
		t.Errorf("ALLOW_ALL described as ALLOW_ALL (6) logged drift: %s", logs.String())
	}

	h.desc.Schedule.State = &client.ScheduleState{}
	h.desc.Info.LastUpdateAt = time.Unix(0, 0) // never updated, as the SDK reports it
	view, _ = s.Describe(ctx, "payments", "nightly")
	if !view.UpdatedAt.IsZero() {
		t.Errorf("a schedule never updated reports updated_at %v, want unset", view.UpdatedAt)
	}
	if len(view.NextFireTimes) != 2 || !view.NextFireTimes[0].Equal(next[0]) {
		t.Errorf("active next fires = %v", view.NextFireTimes)
	}

	// Drift: TERMINATE_OTHER on the cluster against an authored ALLOW_ALL.
	h.desc.Schedule.Policy.Overlap = enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	logs.Reset()
	view, err = s.Describe(ctx, "payments", "nightly")
	if err != nil || view.Record.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatalf("drifted describe = %v, %v; want the authored spec", view.Record, err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "overlap_cluster") {
		t.Errorf("drift not logged at warn: %q", logs.String())
	}

	// SKIP authored, UNSPECIFIED read back: the cluster default, not drift.
	skip := scheduleRecord("payments", "skip", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"},
		Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP})
	if _, err := s.Create(ctx, skip, false); err != nil {
		t.Fatal(err)
	}
	cluster.sc.handles["ork-schedule/payments/skip"].desc.Schedule.Policy.Overlap = enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
	logs.Reset()
	if _, err := s.Describe(ctx, "payments", "skip"); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Errorf("SKIP read back as UNSPECIFIED logged drift: %s", logs.String())
	}

	if _, err := s.Describe(ctx, "other", "nightly"); !errors.Is(err, engine.ErrScheduleNotFound) {
		t.Errorf("another namespace's describe = %v, want ErrScheduleNotFound", err)
	}
}

func memoOf(t testing.TB, kv map[string]string) *commonpb.Memo {
	fields := map[string]*commonpb.Payload{}
	for k, v := range kv {
		fields[k] = mustPayload(t, v)
	}
	return &commonpb.Memo{Fields: fields}
}

func TestTemporalScheduler_ListFiltersByMemoNamespaceAndDescribesEach(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	for _, r := range []*orkstackv1.ScheduleRecord{
		scheduleRecord("payments", "zeta", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}}),
		scheduleRecord("payments", "alpha", &orkstackv1.ScheduleSpec{Cron: []string{"@hourly"}}),
		scheduleRecord("staging", "beta", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}}),
	} {
		if _, err := s.Create(ctx, r, false); err != nil {
			t.Fatal(err)
		}
		cluster.sc.entries = append(cluster.sc.entries, &client.ScheduleListEntry{
			ID: engine.TemporalScheduleID(r.GetNamespace(), r.GetName()),
			Memo: memoOf(t, map[string]string{
				engine.MemoScheduleID: r.GetScheduleId(), engine.MemoScheduleName: r.GetName(),
				engine.MemoNamespace: r.GetNamespace(), engine.MemoOrkflowName: r.GetOrkflowName(),
			}),
		})
	}
	// Not one of ours: no schedule id in its memo.
	cluster.sc.entries = append(cluster.sc.entries, &client.ScheduleListEntry{ID: "someone-else",
		Memo: memoOf(t, map[string]string{engine.MemoNamespace: "payments"})})

	// Visibility lags a delete: an entry whose schedule is already gone is
	// not listed.
	cluster.sc.entries = append(cluster.sc.entries, &client.ScheduleListEntry{ID: "ork-schedule/payments/deleted",
		Memo: memoOf(t, map[string]string{engine.MemoScheduleID: "sch-dead", engine.MemoScheduleName: "deleted",
			engine.MemoNamespace: "payments", engine.MemoOrkflowName: "reconcile"})})
	views, err := s.List(ctx, "payments")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 2 || views[0].Name != "alpha" || views[1].Name != "zeta" {
		t.Fatalf("List = %+v, want alpha, zeta", views)
	}
	for _, v := range views {
		if v.Record == nil || v.Unavailable != "" {
			t.Errorf("row %s was not described: %+v", v.Name, v)
		}
	}

	// A describe that fails still lists the row, identity intact.
	cluster.sc.handles["ork-schedule/payments/zeta"].err = serviceerror.NewUnavailable("cluster busy")
	cluster.sc.entries = append([]*client.ScheduleListEntry(nil), cluster.sc.entries...)
	views, err = s.List(ctx, "payments")
	if err != nil {
		t.Fatal(err)
	}
	zeta := views[1]
	if zeta.Name != "zeta" || zeta.ID == "" || zeta.OrkflowName != "reconcile" || zeta.Namespace != "payments" ||
		zeta.Record != nil || !strings.Contains(zeta.Unavailable, "cluster busy") {
		t.Fatalf("unavailable row = %+v", zeta)
	}
}

func TestTemporalScheduler_RecentFiresReadMemoAndMarkUnavailable(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	rec := scheduleRecord("payments", "nightly", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}})
	if _, err := s.Create(ctx, rec, false); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 21, 1, 0, 0, 0, time.UTC)
	var actions []client.ScheduleActionResult
	for i, id := range []string{"ran", "refused", "preparing", "retired"} {
		at := base.Add(time.Duration(i) * 24 * time.Hour)
		actions = append(actions, client.ScheduleActionResult{
			ScheduleTime: at, ActualTime: at.Add(time.Second),
			StartWorkflowResult: &client.ScheduleWorkflowExecution{WorkflowID: id, FirstExecutionRunID: "r"},
		})
	}
	cluster.fire(t, "ran", map[string]string{engine.MemoScheduledRunID: "ork-20260921-1"})
	cluster.fire(t, "refused", map[string]string{engine.MemoScheduleFireRefusal: `pool "gpu" has no live Orker`})
	cluster.fire(t, "preparing", nil)
	cluster.sc.handles["ork-schedule/payments/nightly"].desc.Info.RecentActions = actions

	view, err := s.Describe(ctx, "payments", "nightly")
	if err != nil {
		t.Fatal(err)
	}
	want := []engine.ScheduleFireView{
		{RunID: "ork-20260921-1", DetailsAvailable: true},
		{Refusal: `pool "gpu" has no live Orker`, DetailsAvailable: true},
		{DetailsAvailable: true},
		{DetailsAvailable: false},
	}
	if len(view.RecentFires) != len(want) {
		t.Fatalf("recent fires = %+v", view.RecentFires)
	}
	for i, w := range want {
		got := view.RecentFires[i]
		if got.RunID != w.RunID || got.Refusal != w.Refusal || got.DetailsAvailable != w.DetailsAvailable {
			t.Errorf("fire %d = %+v, want %+v", i, got, w)
		}
		if !got.ScheduledAt.Equal(actions[i].ScheduleTime) || !got.ActualAt.Equal(actions[i].ActualTime) {
			t.Errorf("fire %d times = %v/%v", i, got.ScheduledAt, got.ActualAt)
		}
	}
}

func TestTemporalScheduler_UpdateRebuildsSpecPolicyAndArgsTogether(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	rec := scheduleRecord("payments", "nightly", &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"},
		Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP})
	if _, err := s.Create(ctx, rec, true); err != nil {
		t.Fatal(err)
	}
	h := cluster.sc.handles["ork-schedule/payments/nightly"]
	h.desc.Schedule.State = &client.ScheduleState{Paused: true, Note: "Q3 close"}
	view, err := s.Update(ctx, "payments", "nightly", func(r *orkstackv1.ScheduleRecord) error {
		r.OrkflowVersion = "3"
		r.Description = "Nightly"
		r.Spec = &orkstackv1.ScheduleSpec{Cron: []string{"30 3 * * *"}, Timezone: "Europe/Lisbon",
			Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL, PauseOnFailure: true}
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	u := h.updated.Schedule
	if u.Spec.CronExpressions[0] != "30 3 * * *" || u.Spec.TimeZoneName != "Europe/Lisbon" {
		t.Errorf("compiled spec not rebuilt: %+v", u.Spec)
	}
	if u.Policy.Overlap != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || !u.Policy.PauseOnFailure {
		t.Errorf("policy not rebuilt: %+v", u.Policy)
	}
	arg := u.Action.(*client.ScheduleWorkflowAction).Args[0].(*orkstackv1.ScheduleRecord)
	if arg.GetOrkflowVersion() != "3" || arg.GetSpec().GetCron()[0] != "30 3 * * *" || arg.GetScheduleId() != rec.GetScheduleId() {
		t.Errorf("action argument not rebuilt: %v", arg)
	}
	if !u.State.Paused || u.State.Note != "Q3 close" {
		t.Errorf("state changed by an update: %+v", u.State)
	}
	if view.Record.GetDescription() != "Nightly" {
		t.Errorf("view after update = %v", view.Record)
	}

	// Identity is immutable: refused before any update is sent.
	h.updated = nil
	_, err = s.Update(ctx, "payments", "nightly", func(r *orkstackv1.ScheduleRecord) error {
		r.Name = "renamed"
		return nil
	})
	if err == nil || h.updated != nil {
		t.Fatalf("an identity change was accepted: %v", err)
	}

	if _, err := s.Update(ctx, "payments", "missing", func(*orkstackv1.ScheduleRecord) error { return nil }); !errors.Is(err, engine.ErrScheduleNotFound) {
		t.Errorf("update of a missing schedule = %v", err)
	}
}

func TestTemporalScheduler_PauseResumeFireBackfillDelete(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	if _, err := s.Create(ctx, scheduleRecord("payments", "nightly", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}}), false); err != nil {
		t.Fatal(err)
	}
	h := cluster.sc.handles["ork-schedule/payments/nightly"]

	if err := s.Pause(ctx, "payments", "nightly", "Q3 close"); err != nil || h.pause.Note != "Q3 close" {
		t.Errorf("Pause = %v, note %q", err, h.pause.Note)
	}
	if err := s.Pause(ctx, "payments", "nightly", ""); err != nil || h.pause.Note != engine.SchedulePauseNote {
		t.Errorf("Pause() note %q, want %q", h.pause.Note, engine.SchedulePauseNote)
	}
	if err := s.Resume(ctx, "payments", "nightly"); err != nil || h.unpause.Note != engine.ScheduleResumeNote {
		t.Errorf("Resume = %v, note %q", err, h.unpause.Note)
	}

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	for _, tc := range []struct {
		in   orkstackv1.OverlapPolicy
		want enumspb.ScheduleOverlapPolicy
	}{
		{orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED},
		{orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
	} {
		if err := s.Fire(ctx, "payments", "nightly", tc.in); err != nil || h.trigger.Overlap != tc.want {
			t.Errorf("Fire(%v) = %v, sent %v; want %v", tc.in, err, h.trigger.Overlap, tc.want)
		}
		if err := s.Backfill(ctx, "payments", "nightly", start, end, tc.in); err != nil {
			t.Fatalf("Backfill: %v", err)
		}
		b := h.backfill.Backfill
		if len(b) != 1 || b[0].Overlap != tc.want || !b[0].Start.Equal(start) || !b[0].End.Equal(end) {
			t.Errorf("Backfill(%v) sent %+v", tc.in, b)
		}
	}
	calls := len(h.calls)
	if err := s.Fire(ctx, "payments", "nightly", 5); !errors.Is(err, engine.ErrOverlapPolicyUnknown) {
		t.Errorf("Fire(5) = %v", err)
	}
	if err := s.Backfill(ctx, "payments", "nightly", start, end, 5); !errors.Is(err, engine.ErrOverlapPolicyUnknown) {
		t.Errorf("Backfill(5) = %v", err)
	}
	if len(h.calls) != calls {
		t.Errorf("a reserved overlap reached the handle: %v", h.calls[calls:])
	}

	if err := s.Delete(ctx, "payments", "nightly"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "payments", "missing"); !errors.Is(err, engine.ErrScheduleNotFound) {
		t.Errorf("Delete(missing) = %v, want ErrScheduleNotFound", err)
	}
	if err := s.Pause(ctx, "payments", "missing", ""); !errors.Is(err, engine.ErrScheduleNotFound) {
		t.Errorf("Pause(missing) = %v, want ErrScheduleNotFound", err)
	}
}

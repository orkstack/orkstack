package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/schedules"
)

// fakeSchedules is the ScheduleController seam without a cluster: it keeps
// the records it was handed and answers the way TemporalScheduler would.
type fakeSchedules struct {
	mu       sync.Mutex
	records  map[string]*orkstackv1.ScheduleRecord
	paused   map[string]bool
	notes    map[string]string
	extra    []engine.ScheduleView // rows List returns beyond the records
	created  []*orkstackv1.ScheduleRecord
	fires    []orkstackv1.OverlapPolicy
	backfill []orkstackv1.OverlapPolicy
	calls    int
	err      error // every call fails with it when set
	listErr  error
}

func newFakeSchedules() *fakeSchedules {
	return &fakeSchedules{
		records: map[string]*orkstackv1.ScheduleRecord{},
		paused:  map[string]bool{},
		notes:   map[string]string{},
	}
}

func scheduleKey(ns, name string) string { return ns + "/" + name }

func (f *fakeSchedules) view(ns, name string) (engine.ScheduleView, error) {
	rec, ok := f.records[scheduleKey(ns, name)]
	if !ok {
		return engine.ScheduleView{}, engine.ErrScheduleNotFound
	}
	return engine.ScheduleView{
		Record: proto.Clone(rec).(*orkstackv1.ScheduleRecord), ID: rec.GetScheduleId(), Name: rec.GetName(),
		Namespace: rec.GetNamespace(), OrkflowName: rec.GetOrkflowName(),
		Paused: f.paused[scheduleKey(ns, name)], Note: f.notes[scheduleKey(ns, name)],
		NextFireTimes: []time.Time{time.Date(2026, 9, 25, 1, 0, 0, 0, time.UTC)},
	}, nil
}

func (f *fakeSchedules) Create(_ context.Context, rec *orkstackv1.ScheduleRecord, paused bool) (engine.ScheduleView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return engine.ScheduleView{}, f.err
	}
	key := scheduleKey(rec.GetNamespace(), rec.GetName())
	if _, ok := f.records[key]; ok {
		return engine.ScheduleView{}, engine.ErrScheduleExists
	}
	f.created = append(f.created, proto.Clone(rec).(*orkstackv1.ScheduleRecord))
	f.records[key] = proto.Clone(rec).(*orkstackv1.ScheduleRecord)
	f.paused[key] = paused
	return f.view(rec.GetNamespace(), rec.GetName())
}

func (f *fakeSchedules) Describe(_ context.Context, ns, name string) (engine.ScheduleView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return engine.ScheduleView{}, f.err
	}
	return f.view(ns, name)
}

func (f *fakeSchedules) List(_ context.Context, ns string) ([]engine.ScheduleView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []engine.ScheduleView
	for _, rec := range f.records {
		if rec.GetNamespace() == ns {
			v, _ := f.view(ns, rec.GetName())
			out = append(out, v)
		}
	}
	for _, v := range f.extra {
		if v.Namespace == ns {
			out = append(out, v)
		}
	}
	return out, nil
}

func (f *fakeSchedules) Update(_ context.Context, ns, name string, apply func(*orkstackv1.ScheduleRecord) error) (engine.ScheduleView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	rec, ok := f.records[scheduleKey(ns, name)]
	if !ok {
		return engine.ScheduleView{}, engine.ErrScheduleNotFound
	}
	next := proto.Clone(rec).(*orkstackv1.ScheduleRecord)
	if err := apply(next); err != nil {
		return engine.ScheduleView{}, err
	}
	if next.GetScheduleId() != rec.GetScheduleId() || next.GetName() != rec.GetName() ||
		next.GetNamespace() != rec.GetNamespace() || next.GetOrkflowName() != rec.GetOrkflowName() {
		return engine.ScheduleView{}, errors.New("identity changed")
	}
	f.records[scheduleKey(ns, name)] = next
	return f.view(ns, name)
}

func (f *fakeSchedules) Pause(_ context.Context, ns, name, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[scheduleKey(ns, name)]; !ok {
		return engine.ErrScheduleNotFound
	}
	f.paused[scheduleKey(ns, name)] = true
	if reason == "" {
		reason = engine.SchedulePauseNote
	}
	f.notes[scheduleKey(ns, name)] = reason
	return nil
}

func (f *fakeSchedules) Resume(_ context.Context, ns, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[scheduleKey(ns, name)]; !ok {
		return engine.ErrScheduleNotFound
	}
	f.paused[scheduleKey(ns, name)] = false
	f.notes[scheduleKey(ns, name)] = engine.ScheduleResumeNote
	return nil
}

func (f *fakeSchedules) Fire(_ context.Context, ns, name string, overlap orkstackv1.OverlapPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[scheduleKey(ns, name)]; !ok {
		return engine.ErrScheduleNotFound
	}
	f.fires = append(f.fires, overlap)
	return nil
}

func (f *fakeSchedules) Backfill(_ context.Context, ns, name string, _, _ time.Time, overlap orkstackv1.OverlapPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[scheduleKey(ns, name)]; !ok {
		return engine.ErrScheduleNotFound
	}
	f.backfill = append(f.backfill, overlap)
	return nil
}

func (f *fakeSchedules) Delete(_ context.Context, ns, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[scheduleKey(ns, name)]; !ok {
		return engine.ErrScheduleNotFound
	}
	delete(f.records, scheduleKey(ns, name))
	return nil
}

type scheduleHarness struct {
	svc       *api.Service
	schedules *fakeSchedules
	starter   *fakeStarter
	runs      *engine.RunStore
	now       time.Time
}

func newScheduleHarness(t *testing.T) *scheduleHarness {
	t.Helper()
	h := &scheduleHarness{
		schedules: newFakeSchedules(),
		starter:   &fakeStarter{},
		runs:      engine.NewRunStore(),
		now:       time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	}
	var counter int
	h.svc = api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: newFakeRegistry(),
		Runs:       h.runs,
		Bus:        engine.NewEventBus(),
		Starter:    h.starter,
		Schedules:  h.schedules,
		Now:        func() time.Time { return h.now },
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("ork-sched-%d", counter)
		},
	})
	h.apply(t, helloYAML(t))
	return h
}

func (h *scheduleHarness) apply(t *testing.T, yaml string) {
	t.Helper()
	if _, err := h.svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
}

func dailySpec() *orkstackv1.ScheduleSpec {
	return &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon"}
}

func (h *scheduleHarness) create(req *orkstackv1.CreateScheduleRequest) (*orkstackv1.Schedule, error) {
	resp, err := h.svc.CreateSchedule(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetSchedule(), nil
}

func (h *scheduleHarness) mustCreate(t *testing.T, name string) *orkstackv1.Schedule {
	t.Helper()
	s, err := h.create(&orkstackv1.CreateScheduleRequest{Name: name, OrkflowName: "hello-ork", Spec: dailySpec()})
	if err != nil {
		t.Fatalf("CreateSchedule(%s): %v", name, err)
	}
	return s
}

func TestValidateScheduleSpec(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	cron := func(e ...string) *orkstackv1.ScheduleSpec { return &orkstackv1.ScheduleSpec{Cron: e} }
	withOverlap := func(p orkstackv1.OverlapPolicy) *orkstackv1.ScheduleSpec {
		return &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Overlap: p}
	}
	cases := []struct {
		name    string
		spec    *orkstackv1.ScheduleSpec
		wantErr string // "" = valid
	}{
		{"empty", nil, "spec: at least one cron expression or an interval is required"},
		{"no cron no interval", &orkstackv1.ScheduleSpec{Timezone: "UTC"}, "spec: at least one"},
		{"cron only", cron("0 2 * * *"), ""},
		{"interval only", &orkstackv1.ScheduleSpec{Interval: durationpb.New(time.Hour)}, ""},
		{"both", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Interval: durationpb.New(time.Hour)}, ""},
		{"CRON_TZ prefix", cron("CRON_TZ=Europe/Lisbon 0 2 * * *"), "spec.cron[0]: set spec.timezone instead"},
		{"TZ prefix", cron("@daily", "TZ=UTC 0 2 * * *"), "spec.cron[1]: set spec.timezone"},
		{"control character", cron("0 2 * * *\n"), "spec.cron[0]: contains control character"},
		{"201-byte expression", cron(strings.Repeat("1", 201)), "spec.cron[0]: 201 bytes"},
		{"empty expression", cron(""), "spec.cron[0]"},
		{"zero interval", &orkstackv1.ScheduleSpec{Interval: durationpb.New(0)}, "spec.interval"},
		{"negative jitter", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Jitter: durationpb.New(-time.Second)}, "spec.jitter"},
		{"bad zone", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Timezone: "Mars/Olympus"}, "spec.timezone"},
		{"good zone", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Timezone: "Europe/Lisbon"}, ""},
		{"5s catch-up", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, CatchupWindow: durationpb.New(5 * time.Second)}, "spec.catchup_window"},
		{"10s catch-up", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, CatchupWindow: durationpb.New(10 * time.Second)}, ""},
		{"start after end", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"},
			StartAt: timestamppb.New(now.Add(48 * time.Hour)), EndAt: timestamppb.New(now.Add(24 * time.Hour))}, "spec.start_at"},
		{"end in the past", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, EndAt: timestamppb.New(now.Add(-time.Hour))}, "spec.end_at"},
		{"reserved overlap 5", withOverlap(5), "spec.overlap: unknown overlap policy 5"},
		{"undeclared overlap 7", withOverlap(7), "spec.overlap: unknown overlap policy 7"},
		{"unspecified overlap", withOverlap(orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED), ""},
		{"allow-all overlap", withOverlap(orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := api.ValidateScheduleSpec(tc.spec, now)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateScheduleSpec = %v, want valid", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateScheduleSpec = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestCreateSchedule_ResolvesNamespaceAndOrkflowBeforeTheCluster(t *testing.T) {
	h := newScheduleHarness(t)
	for name, tc := range map[string]struct {
		req  *orkstackv1.CreateScheduleRequest
		code connect.Code
		want string
	}{
		"unknown namespace": {&orkstackv1.CreateScheduleRequest{Name: "s", Namespace: "nowhere", OrkflowName: "hello-ork", Spec: dailySpec()},
			connect.CodeNotFound, "nowhere"},
		"unknown orkflow": {&orkstackv1.CreateScheduleRequest{Name: "s", OrkflowName: "missing", Spec: dailySpec()},
			connect.CodeNotFound, `orkflow "missing"`},
		"unknown pinned version": {&orkstackv1.CreateScheduleRequest{Name: "s", OrkflowName: "hello-ork", OrkflowVersion: "9.9.9", Spec: dailySpec()},
			connect.CodeNotFound, `version "9.9.9"`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.create(tc.req)
			if connect.CodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateSchedule = %v, want %s naming %q", err, tc.code, tc.want)
			}
			if h.schedules.calls != 0 {
				t.Fatalf("a refused create reached the cluster")
			}
		})
	}

	input, _ := structpb.NewStruct(map[string]any{"url": "https://example.test"})
	s, err := h.create(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0", Description: "Nightly",
		Input: &orkstackv1.Payload{Data: input}, Spec: dailySpec(), Paused: true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	if len(h.schedules.created) != 1 {
		t.Fatalf("Create calls = %d, want 1", len(h.schedules.created))
	}
	rec := h.schedules.created[0]
	if !schedules.ValidID(rec.GetScheduleId()) || s.GetId() != rec.GetScheduleId() {
		t.Fatalf("id %q (response %q) is not a fresh sch- id", rec.GetScheduleId(), s.GetId())
	}
	if rec.GetNamespace() != "default" || rec.GetName() != "nightly" || rec.GetOrkflowName() != "hello-ork" ||
		rec.GetOrkflowVersion() != "0.2.0" || rec.GetDescription() != "Nightly" ||
		!proto.Equal(rec.GetInput(), &orkstackv1.Payload{Data: input}) {
		t.Fatalf("record = %v", rec)
	}
	if !s.GetPaused() || len(s.GetNextFireTimes()) != 1 {
		t.Fatalf("response = %v, want paused with real next fire times", s)
	}
}

func TestCreateAndUpdateSchedule_RecordUnspecifiedOverlapAsSkip(t *testing.T) {
	h := newScheduleHarness(t)
	req := &orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}
	s, err := h.create(req)
	if err != nil {
		t.Fatal(err)
	}
	if h.schedules.created[0].GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP ||
		s.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP {
		t.Fatalf("create recorded overlap %v, answered %v; want SKIP", h.schedules.created[0].GetSpec().GetOverlap(), s.GetSpec().GetOverlap())
	}
	if req.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		t.Fatalf("the request message was mutated")
	}

	all, err := h.create(&orkstackv1.CreateScheduleRequest{Name: "all", OrkflowName: "hello-ork",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"@hourly"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL}})
	if err != nil || all.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatalf("ALLOW_ALL create = %v, %v", all, err)
	}

	update := &orkstackv1.UpdateScheduleRequest{Name: "all", Spec: dailySpec()}
	resp, err := h.svc.UpdateSchedule(context.Background(), connect.NewRequest(update))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetSchedule().GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP ||
		h.schedules.records["default/all"].GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP {
		t.Fatalf("update with no overlap recorded %v", h.schedules.records["default/all"].GetSpec().GetOverlap())
	}
	if update.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		t.Fatalf("the update request was mutated")
	}
}

func TestCreateSchedule_DuplicateNameIsAlreadyExists(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	_, err := h.create(&orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()})
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), `"nightly"`) ||
		!strings.Contains(err.Error(), `"default"`) {
		t.Fatalf("duplicate create = %v, want AlreadyExists naming name and namespace", err)
	}
}

func TestCreateSchedule_ClusterRefusalIsInvalidArgumentVerbatim(t *testing.T) {
	h := newScheduleHarness(t)
	const msg = `invalid cron string: "0 2 * *"`
	h.schedules.err = fmt.Errorf("%w: %s", engine.ErrScheduleSpecRefused, msg)
	_, err := h.create(&orkstackv1.CreateScheduleRequest{Name: "s", OrkflowName: "hello-ork",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * *"}}})
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), msg) {
		t.Fatalf("cluster refusal = %v, want InvalidArgument quoting %q", err, msg)
	}
}

func TestCreateSchedule_DescriptionAndNameAreValidatedNotSanitized(t *testing.T) {
	h := newScheduleHarness(t)
	for name, req := range map[string]*orkstackv1.CreateScheduleRequest{
		"no name":         {OrkflowName: "hello-ork", Spec: dailySpec()},
		"uppercase name":  {Name: "Nightly", OrkflowName: "hello-ork", Spec: dailySpec()},
		"name with slash": {Name: "a/b", OrkflowName: "hello-ork", Spec: dailySpec()},
		"long name":       {Name: "a" + strings.Repeat("b", 200), OrkflowName: "hello-ork", Spec: dailySpec()},
		"control char":    {Name: "ok", OrkflowName: "hello-ork", Spec: dailySpec(), Description: "line\nbreak"},
		"long desc":       {Name: "ok", OrkflowName: "hello-ork", Spec: dailySpec(), Description: strings.Repeat("x", 257)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.create(req)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("CreateSchedule = %v, want InvalidArgument", err)
			}
			if len(h.schedules.created) != 0 {
				t.Fatalf("an invalid create was sanitized and stored: %v", h.schedules.created)
			}
		})
	}
}

func TestGetSchedule_OtherNamespaceIsNotFound(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	h.schedules.records["payments/nightly"] = &orkstackv1.ScheduleRecord{Name: "nightly", Namespace: "payments"}
	got, err := h.svc.GetSchedule(context.Background(), connect.NewRequest(&orkstackv1.GetScheduleRequest{Name: "nightly"}))
	if err != nil || got.Msg.GetSchedule().GetNamespace() != "default" {
		t.Fatalf("GetSchedule(default) = %v, %v", got, err)
	}
	_, err = h.svc.GetSchedule(context.Background(), connect.NewRequest(&orkstackv1.GetScheduleRequest{Name: "other"}))
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), `schedule "other" not found in namespace "default"`) {
		t.Fatalf("GetSchedule(missing) = %v", err)
	}
}

func TestListSchedules_NarrowsByOrkflowAndCarriesUnavailableRows(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	h.schedules.extra = []engine.ScheduleView{
		{ID: "sch-0000000000000001", Name: "broken", Namespace: "default", OrkflowName: "hello-ork", Unavailable: "cluster busy"},
		{ID: "sch-0000000000000002", Name: "etl", Namespace: "default", OrkflowName: "etl", Unavailable: "x"},
	}
	resp, err := h.svc.ListSchedules(context.Background(), connect.NewRequest(&orkstackv1.ListSchedulesRequest{OrkflowName: "hello-ork"}))
	if err != nil {
		t.Fatal(err)
	}
	rows := resp.Msg.GetSchedules()
	if len(rows) != 2 {
		t.Fatalf("rows = %v, want nightly and broken", rows)
	}
	var broken *orkstackv1.Schedule
	for _, r := range rows {
		if r.GetName() == "broken" {
			broken = r
		}
	}
	if broken == nil || broken.GetUnavailableReason() != "cluster busy" || broken.GetId() == "" ||
		broken.GetOrkflowName() != "hello-ork" || broken.GetSpec() != nil || broken.GetCreatedAt() != nil || broken.GetPaused() {
		t.Fatalf("unavailable row = %v, want identity and reason only", broken)
	}
	h.schedules.listErr = serviceerror.NewUnavailable("cluster down")
	_, err = h.svc.ListSchedules(context.Background(), connect.NewRequest(&orkstackv1.ListSchedulesRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("a list the cluster could not answer = %v, want Unavailable, never an empty list", err)
	}
}

func TestUpdateSchedule_ReplacesMutableFieldsOnly(t *testing.T) {
	h := newScheduleHarness(t)
	input, _ := structpb.NewStruct(map[string]any{"url": "x"})
	if _, err := h.create(&orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
		Description: "old", Input: &orkstackv1.Payload{Data: input}, Spec: dailySpec()}); err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(h.schedules.records["default/nightly"]).(*orkstackv1.ScheduleRecord)
	newSpec := &orkstackv1.ScheduleSpec{Interval: durationpb.New(time.Hour), Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE}
	if _, err := h.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", Description: "new", Spec: newSpec,
	})); err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	after := h.schedules.records["default/nightly"]
	if after.GetOrkflowVersion() != "" || after.GetInput() != nil || after.GetDescription() != "new" ||
		!proto.Equal(after.GetSpec(), newSpec) {
		t.Fatalf("after update = %v", after)
	}
	if after.GetScheduleId() != before.GetScheduleId() || after.GetName() != before.GetName() ||
		after.GetNamespace() != before.GetNamespace() || after.GetOrkflowName() != before.GetOrkflowName() {
		t.Fatalf("update changed identity: %v -> %v", before, after)
	}
	_, err := h.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{Name: "missing", Spec: dailySpec()}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("update of a missing schedule = %v", err)
	}
}

func TestUpdateSchedule_PinnedVersionMustExist(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	_, err := h.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", OrkflowVersion: "9.9.9", Spec: dailySpec()}))
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), `version "9.9.9"`) {
		t.Fatalf("update pinning a missing version = %v", err)
	}
	if h.schedules.records["default/nightly"].GetOrkflowVersion() != "" {
		t.Fatalf("the refused pin was written")
	}
	if _, err := h.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", OrkflowVersion: "0.2.0", Spec: dailySpec()})); err != nil {
		t.Fatalf("update pinning an existing version: %v", err)
	}
}

func TestPauseSchedule_ReasonBoundedAndRendered(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	ctx := context.Background()
	resp, err := h.svc.PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "nightly", Reason: "Q3 close"}))
	if err != nil || !resp.Msg.GetSchedule().GetPaused() || resp.Msg.GetSchedule().GetNote() != "Q3 close" {
		t.Fatalf("pause = %v, %v", resp, err)
	}
	resp, err = h.svc.PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "nightly"}))
	if err != nil || resp.Msg.GetSchedule().GetNote() != "" {
		t.Fatalf("pause without a reason renders note %q, want none", resp.Msg.GetSchedule().GetNote())
	}
	for _, reason := range []string{strings.Repeat("r", 257), "tab\there"} {
		if _, err := h.svc.PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "nightly", Reason: reason})); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("pause with reason %q = %v, want InvalidArgument", reason, err)
		}
	}
}

func TestResumeSchedule(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	ctx := context.Background()
	if _, err := h.svc.PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "nightly", Reason: "freeze"})); err != nil {
		t.Fatal(err)
	}
	resp, err := h.svc.ResumeSchedule(ctx, connect.NewRequest(&orkstackv1.ResumeScheduleRequest{Name: "nightly"}))
	if err != nil || resp.Msg.GetSchedule().GetPaused() || resp.Msg.GetSchedule().GetNote() != "" {
		t.Fatalf("resume = %v, %v", resp, err)
	}
	if _, err := h.svc.ResumeSchedule(ctx, connect.NewRequest(&orkstackv1.ResumeScheduleRequest{Name: "missing"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("resume missing = %v", err)
	}
}

func TestFireSchedule_PassesOverlapOverride(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	ctx := context.Background()
	if _, err := h.svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "nightly"})); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "nightly",
		Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL})); err != nil {
		t.Fatal(err)
	}
	_, err := h.svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "nightly", Overlap: 5}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("fire with overlap 5 = %v", err)
	}
	want := []orkstackv1.OverlapPolicy{orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL}
	if len(h.schedules.fires) != 2 || h.schedules.fires[0] != want[0] || h.schedules.fires[1] != want[1] {
		t.Fatalf("fires reached the controller as %v, want %v (never SKIP for none)", h.schedules.fires, want)
	}
}

func TestBackfillSchedule_ValidatesWindow(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	ctx := context.Background()
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for name, req := range map[string]*orkstackv1.BackfillScheduleRequest{
		"missing end":    {Name: "nightly", Start: timestamppb.New(start)},
		"missing start":  {Name: "nightly", End: timestamppb.New(start)},
		"end < start":    {Name: "nightly", Start: timestamppb.New(start), End: timestamppb.New(start.Add(-time.Minute))},
		"367-day window": {Name: "nightly", Start: timestamppb.New(start), End: timestamppb.New(start.Add(367 * 24 * time.Hour))},
		"overlap 5":      {Name: "nightly", Start: timestamppb.New(start), End: timestamppb.New(start.Add(time.Hour)), Overlap: 5},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := h.svc.BackfillSchedule(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("backfill = %v, want InvalidArgument", err)
			}
		})
	}
	if len(h.schedules.backfill) != 0 {
		t.Fatalf("a refused backfill reached the controller")
	}
	for _, p := range []orkstackv1.OverlapPolicy{orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL} {
		if _, err := h.svc.BackfillSchedule(ctx, connect.NewRequest(&orkstackv1.BackfillScheduleRequest{
			Name: "nightly", Start: timestamppb.New(start), End: timestamppb.New(start.Add(3 * time.Minute)), Overlap: p,
		})); err != nil {
			t.Fatal(err)
		}
	}
	if h.schedules.backfill[0] != orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED ||
		h.schedules.backfill[1] != orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatalf("backfill overlaps = %v", h.schedules.backfill)
	}
}

func TestDeleteSchedule_RepeatIsNotFound(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	ctx := context.Background()
	if _, err := h.svc.DeleteSchedule(ctx, connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "nightly"})); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.DeleteSchedule(ctx, connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "nightly"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("repeat delete = %v, want NotFound", err)
	}
}

func TestScheduleService_NilControllerIsUnimplemented(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	ctx := context.Background()
	calls := map[string]func() error{
		"create": func() error {
			_, err := svc.CreateSchedule(ctx, connect.NewRequest(&orkstackv1.CreateScheduleRequest{Name: "s"}))
			return err
		},
		"list": func() error {
			_, err := svc.ListSchedules(ctx, connect.NewRequest(&orkstackv1.ListSchedulesRequest{}))
			return err
		},
		"get": func() error {
			_, err := svc.GetSchedule(ctx, connect.NewRequest(&orkstackv1.GetScheduleRequest{Name: "s"}))
			return err
		},
		"update": func() error {
			_, err := svc.UpdateSchedule(ctx, connect.NewRequest(&orkstackv1.UpdateScheduleRequest{Name: "s"}))
			return err
		},
		"pause": func() error {
			_, err := svc.PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "s"}))
			return err
		},
		"resume": func() error {
			_, err := svc.ResumeSchedule(ctx, connect.NewRequest(&orkstackv1.ResumeScheduleRequest{Name: "s"}))
			return err
		},
		"fire": func() error {
			_, err := svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "s"}))
			return err
		},
		"backfill": func() error {
			_, err := svc.BackfillSchedule(ctx, connect.NewRequest(&orkstackv1.BackfillScheduleRequest{Name: "s"}))
			return err
		},
		"delete": func() error {
			_, err := svc.DeleteSchedule(ctx, connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "s"}))
			return err
		},
	}
	for name, call := range calls {
		if err := call(); connect.CodeOf(err) != connect.CodeUnimplemented || !strings.Contains(err.Error(), "no schedule controller configured") {
			t.Errorf("%s = %v, want Unimplemented", name, err)
		}
	}
}

// ---- PrepareScheduledRun ----

type fixedIndexes struct{ ns, origin bool }

func (f fixedIndexes) IndexAnswers() (bool, bool) { return f.ns, f.origin }

func scheduleRec(version string) *orkstackv1.ScheduleRecord {
	return &orkstackv1.ScheduleRecord{ScheduleId: "sch-4a1c0e2b7d3f8156", Name: "nightly", Namespace: "default",
		OrkflowName: "hello-ork", OrkflowVersion: version, Spec: dailySpec()}
}

func TestPrepareScheduledRun_UsesPrepareAndRecordsScheduleOrigin(t *testing.T) {
	h := newScheduleHarness(t)
	// The current pointer moves after the schedule was created: an unpinned
	// fire resolves it at fire time.
	h.apply(t, strings.Replace(helloYAML(t), `version: "0.2.0"`, `version: "0.3.0"`, 1))
	acts := api.NewScheduleActivities(h.svc, fixedIndexes{ns: true, origin: false})
	at := time.Date(2026, 9, 24, 1, 0, 0, 0, time.FixedZone("Lisbon", 3600))
	prep, err := acts.PrepareScheduledRun(context.Background(), scheduleRec(""), at)
	if err != nil {
		t.Fatalf("PrepareScheduledRun: %v", err)
	}
	if prep.GetOrkflow().GetVersion() != "0.3.0" || prep.GetRun().GetOrkflowVersion() != "0.3.0" {
		t.Fatalf("unpinned fire prepared %q, want the current 0.3.0", prep.GetOrkflow().GetVersion())
	}
	want := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, Id: "sch-4a1c0e2b7d3f8156",
		Reference: "2026-09-24T00:00:00Z", Description: "nightly"}
	if !proto.Equal(prep.GetRun().GetOrigin(), want) {
		t.Fatalf("origin = %v, want %v", prep.GetRun().GetOrigin(), want)
	}
	var plan engine.RunPlan
	if err := json.Unmarshal(prep.GetPlan(), &plan); err != nil {
		t.Fatalf("plan does not decode: %v", err)
	}
	if !proto.Equal(plan.Origin.Proto(), want) {
		t.Fatalf("plan origin = %v", plan.Origin.Proto())
	}
	if len(plan.Routes) == 0 || plan.CommitRoot == "" {
		t.Fatalf("plan is not the ordinary submit plan: %+v", plan)
	}
	if !prep.GetNamespaceIndexed() || prep.GetOriginIndexed() {
		t.Fatalf("index answers = %v/%v, want the starter's true/false", prep.GetNamespaceIndexed(), prep.GetOriginIndexed())
	}
	if !strings.HasPrefix(prep.GetRun().GetId(), "ork-sched-") || prep.GetRun().GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Fatalf("run = %v", prep.GetRun())
	}
	if len(h.starter.startedRuns()) != 0 {
		t.Fatalf("prepare started a run: %v", h.starter.startedRuns())
	}
	if n := len(h.runs.List(engine.ListFilter{})); n != 0 {
		t.Fatalf("prepare saved %d runs in process memory", n)
	}

	pinned, err := acts.PrepareScheduledRun(context.Background(), scheduleRec("0.2.0"), at)
	if err != nil || pinned.GetOrkflow().GetVersion() != "0.2.0" {
		t.Fatalf("pinned fire = %v, %v; want 0.2.0", pinned.GetOrkflow().GetVersion(), err)
	}
}

func TestPrepareScheduledRun_SubmitsInputVerbatim(t *testing.T) {
	h := newScheduleHarness(t)
	acts := api.NewScheduleActivities(h.svc, fixedIndexes{})
	rec := scheduleRec("")
	prep, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := prep.GetInput().GetData().AsMap(); len(got) != 1 || got["url"] != "https://httpbin.org/json" {
		t.Fatalf("no input: prepared %v, want the declared default only", got)
	}
	input, _ := structpb.NewStruct(map[string]any{"url": "https://caller.test", "extra": "{{ .now }}"})
	rec.Input = &orkstackv1.Payload{Data: input}
	prep, err = acts.PrepareScheduledRun(context.Background(), rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(prep.GetInput(), rec.GetInput()) {
		t.Fatalf("input = %v, want %v verbatim", prep.GetInput(), rec.GetInput())
	}
}

func TestPrepareScheduledRun_RefusalIsNonRetryableAndOutageIsRetryable(t *testing.T) {
	h := newScheduleHarness(t)
	acts := api.NewScheduleActivities(h.svc, fixedIndexes{})
	for name, rec := range map[string]*orkstackv1.ScheduleRecord{
		"unknown version (NotFound)": scheduleRec("9.9.9"),
		"unknown namespace":          {ScheduleId: "sch-1", Name: "n", Namespace: "nowhere", OrkflowName: "hello-ork"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now())
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != engine.ScheduleFireRefusedType || !app.NonRetryable() {
				t.Fatalf("refusal = %v, want non-retryable %s", err, engine.ScheduleFireRefusedType)
			}
		})
	}
	// FailedPrecondition: no run starter configured.
	noStarter := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	if _, err := noStarter.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatal(err)
	}
	_, err := api.NewScheduleActivities(noStarter, nil).PrepareScheduledRun(context.Background(), scheduleRec(""), time.Now())
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ScheduleFireRefusedType {
		t.Fatalf("FailedPrecondition = %v, want a refusal", err)
	}

	// An outage of the store is retryable: not the refusal type.
	outage := api.NewService(api.Config{Orkflows: failingOrkflows{err: errors.New("bucket unreachable")}, Starter: &fakeStarter{}})
	_, err = api.NewScheduleActivities(outage, nil).PrepareScheduledRun(context.Background(), scheduleRec(""), time.Now())
	if err == nil {
		t.Fatal("an outage prepared a run")
	}
	if errors.As(err, &app) && app.Type() == engine.ScheduleFireRefusedType {
		t.Fatalf("an outage was a final refusal: %v", err)
	}
}

// failingOrkflows is an orkflow store whose every read fails as an
// unreachable bucket does.
type failingOrkflows struct {
	engine.OrkflowStore
	err error
}

func (f failingOrkflows) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, f.err
}

func TestPrepareScheduledRun_OversizedPreparationIsRefused(t *testing.T) {
	h := newScheduleHarness(t)
	acts := api.NewScheduleActivities(h.svc, fixedIndexes{})
	rec := scheduleRec("")
	input, _ := structpb.NewStruct(map[string]any{"url": strings.Repeat("x", 300*1024)})
	rec.Input = &orkstackv1.Payload{Data: input}
	_, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now())
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ScheduleFireRefusedType || !strings.Contains(app.Message(), "256 KiB") {
		t.Fatalf("oversized preparation = %v, want a refusal naming 256 KiB", err)
	}
}

// ---- DeleteOrkflow ----

func TestDeleteOrkflow_RefusedWhileAScheduleNamesIt(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	_, err := h.svc.DeleteOrkflow(context.Background(), connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: "hello-ork"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "1 schedule(s) name it (nightly)") {
		t.Fatalf("DeleteOrkflow = %v, want FailedPrecondition naming the schedule", err)
	}
	if _, err := h.svc.DeleteSchedule(context.Background(), connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "nightly"})); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.DeleteOrkflow(context.Background(), connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: "hello-ork"})); err != nil {
		t.Fatalf("DeleteOrkflow after the schedule is gone: %v", err)
	}
}

func TestDeleteOrkflow_ScheduleListUnavailableRefuses(t *testing.T) {
	h := newScheduleHarness(t)
	h.schedules.listErr = serviceerror.NewUnavailable("cluster down")
	_, err := h.svc.DeleteOrkflow(context.Background(), connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: "hello-ork"}))
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "has schedules") {
		t.Fatalf("DeleteOrkflow = %v, want Unavailable", err)
	}
	if _, getErr := h.svc.GetOrkflow(context.Background(), connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "hello-ork"})); getErr != nil {
		t.Fatalf("the orkflow was deleted: %v", getErr)
	}
}

// ---- audit and JSON ----

func TestAuditInterceptor_RecordsScheduleCreateWithoutInput(t *testing.T) {
	store := artifacts.NewMemoryAuditStore()
	now := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Starter: &fakeStarter{}, Schedules: newFakeSchedules(), AuditStore: store,
		Now: func() time.Time { return now },
	})
	if _, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatal(err)
	}
	server := newHTTPTestServer(t, svc)
	client := orkstackv1connect.NewScheduleServiceClient(server.Client(), server.URL)
	input, _ := structpb.NewStruct(map[string]any{"token": "SECRET-INPUT-VALUE"})
	created, err := client.CreateSchedule(context.Background(), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Input: &orkstackv1.Payload{Data: input},
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon"}, Paused: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.ListAuditRecords(context.Background(), artifacts.AuditQuery{Since: now.Add(-time.Hour), Until: now.Add(time.Hour), Limit: 10})
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("records = %v, %v", page.Records, err)
	}
	rec := page.Records[0]
	if rec.GetAction() != "schedule.create" || rec.GetTarget() != created.Msg.GetSchedule().GetId() || rec.GetNamespace() != "default" {
		t.Fatalf("record = %v", rec)
	}
	for _, want := range []string{"name=nightly", "hello-ork@current", "cron=[0 2 * * *]", "timezone=Europe/Lisbon", "overlap=skip", "paused=true"} {
		if !strings.Contains(rec.GetDetail(), want) {
			t.Errorf("detail %q lacks %q", rec.GetDetail(), want)
		}
	}
	if strings.Contains(rec.GetDetail(), "SECRET-INPUT-VALUE") || strings.Contains(rec.GetDetail(), "token") {
		t.Fatalf("detail carries the input: %q", rec.GetDetail())
	}
	if _, err := client.FireSchedule(context.Background(), connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "nightly"})); err != nil {
		t.Fatal(err)
	}
	page, _ = store.ListAuditRecords(context.Background(), artifacts.AuditQuery{Since: now.Add(-time.Hour), Until: now.Add(time.Hour), Limit: 10})
	var fire *orkstackv1.AuditRecord
	for _, r := range page.Records {
		if r.GetAction() == "schedule.fire" {
			fire = r
		}
	}
	if fire == nil || fire.GetDetail() != "overlap=schedule" || fire.GetTarget() != "nightly" {
		t.Fatalf("fire record = %v", fire)
	}
}

func TestScheduleJSON_OverlapAndFireRenderShort(t *testing.T) {
	schedule := &orkstackv1.Schedule{
		Id: "sch-4a1c0e2b7d3f8156", Name: "nightly",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE},
		RecentFires: []*orkstackv1.ScheduleFire{{ScheduledAt: timestamppb.New(time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)),
			RunId: "ork-1", DetailsAvailable: true}},
	}
	encoded, err := api.MarshalJSON(&orkstackv1.GetScheduleResponse{Schedule: schedule})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"overlap":"BUFFER_ONE"`, `"runId":"ork-1"`, `"detailsAvailable":true`, `"scheduledAt":"2026-09-24T01:00:00Z"`} {
		if !strings.Contains(string(encoded), want) {
			t.Errorf("JSON lacks %s: %s", want, encoded)
		}
	}
	var back orkstackv1.GetScheduleResponse
	if err := api.UnmarshalJSON(encoded, &back); err != nil || !proto.Equal(back.GetSchedule(), schedule) {
		t.Fatalf("round trip = %v, %v", back.GetSchedule(), err)
	}
}

func newHTTPTestServer(t *testing.T, svc *api.Service) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	return server
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// cliSchedules is the schedule seam behind the CLI fixture: the records it was
// handed, answered back as a cluster would.
type cliSchedules struct {
	mu       sync.Mutex
	records  map[string]*orkstackv1.ScheduleRecord
	fires    []orkstackv1.OverlapPolicy
	backfill []orkstackv1.OverlapPolicy
	// refuseCron, when set, is a cron expression the "cluster" cannot parse:
	// a create or update carrying it is refused as the cluster would.
	refuseCron string
}

func (c *cliSchedules) view(ns, name string) (engine.ScheduleView, error) {
	rec, ok := c.records[ns+"/"+name]
	if !ok {
		return engine.ScheduleView{}, engine.ErrScheduleNotFound
	}
	return engine.ScheduleView{Record: proto.Clone(rec).(*orkstackv1.ScheduleRecord), ID: rec.GetScheduleId(),
		Name: rec.GetName(), Namespace: rec.GetNamespace(), OrkflowName: rec.GetOrkflowName()}, nil
}

func (c *cliSchedules) Create(_ context.Context, rec *orkstackv1.ScheduleRecord, _ bool) (engine.ScheduleView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, expr := range rec.GetSpec().GetCron() {
		if c.refuseCron != "" && expr == c.refuseCron {
			return engine.ScheduleView{}, fmt.Errorf("%w: invalid cron expression %q", engine.ErrScheduleSpecRefused, expr)
		}
	}
	if _, exists := c.records[rec.GetNamespace()+"/"+rec.GetName()]; exists {
		return engine.ScheduleView{}, engine.ErrScheduleExists
	}
	c.records[rec.GetNamespace()+"/"+rec.GetName()] = proto.Clone(rec).(*orkstackv1.ScheduleRecord)
	return c.view(rec.GetNamespace(), rec.GetName())
}
func (c *cliSchedules) Describe(_ context.Context, ns, name string) (engine.ScheduleView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.view(ns, name)
}
func (c *cliSchedules) List(_ context.Context, ns string, keep func(id, name, orkflow string) bool) ([]engine.ScheduleView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var names []string
	for _, rec := range c.records {
		if rec.GetNamespace() == ns && (keep == nil || keep(rec.GetScheduleId(), rec.GetName(), rec.GetOrkflowName())) {
			names = append(names, rec.GetName())
		}
	}
	sort.Strings(names)
	out := make([]engine.ScheduleView, 0, len(names))
	for _, name := range names {
		view, _ := c.view(ns, name)
		out = append(out, view)
	}
	return out, nil
}
func (c *cliSchedules) Update(_ context.Context, ns, name string, apply func(*orkstackv1.ScheduleRecord) error) (engine.ScheduleView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[ns+"/"+name]
	if !ok {
		return engine.ScheduleView{}, engine.ErrScheduleNotFound
	}
	if err := apply(rec); err != nil {
		return engine.ScheduleView{}, err
	}
	return c.view(ns, name)
}
func (c *cliSchedules) Pause(context.Context, string, string, string) error { return nil }
func (c *cliSchedules) Resume(context.Context, string, string) error        { return nil }
func (c *cliSchedules) Fire(_ context.Context, _, _ string, p orkstackv1.OverlapPolicy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fires = append(c.fires, p)
	return nil
}
func (c *cliSchedules) Backfill(_ context.Context, _, _ string, _, _ time.Time, p orkstackv1.OverlapPolicy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backfill = append(c.backfill, p)
	return nil
}
func (c *cliSchedules) Delete(_ context.Context, ns, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.records[ns+"/"+name]; !ok {
		return engine.ErrScheduleNotFound
	}
	delete(c.records, ns+"/"+name)
	return nil
}

func newScheduleFixture(t *testing.T) (*fixture, *cliSchedules) {
	t.Helper()
	schedules := &cliSchedules{records: map[string]*orkstackv1.ScheduleRecord{}}
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Schedules = schedules })
	f.applyExample().requireExit(t, exitOK)
	return f, schedules
}

func TestOverlapPolicySlug_MatchesTheEnum(t *testing.T) {
	values := orkstackv1.File_orkstack_v1_schedule_proto.Enums().ByName("OverlapPolicy").Values()
	var slugs []string
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		p := orkstackv1.OverlapPolicy(value.Number())
		slug, ok := overlapPolicySlug(p)
		if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
			if ok {
				t.Fatalf("UNSPECIFIED has slug %q", slug)
			}
			continue
		}
		want := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(string(value.Name()), "OVERLAP_POLICY_")), "_", "-")
		if !ok || slug != want {
			t.Fatalf("%s slug = %q (%v), want %q", value.Name(), slug, ok, want)
		}
		back, err := parseOverlapPolicy(slug)
		if err != nil || back != p {
			t.Fatalf("parseOverlapPolicy(%q) = %v, %v", slug, back, err)
		}
		slugs = append(slugs, slug)
	}
	if got := strings.Join(overlapPolicySlugs(), ","); got != "skip,buffer-one,buffer-all,cancel-other,allow-all" || got != strings.Join(slugs, ",") {
		t.Fatalf("slugs = %s", got)
	}
	if _, ok := overlapPolicySlug(5); ok {
		t.Fatal("the reserved number 5 has a slug")
	}
}

func TestParseOverlapPolicy(t *testing.T) {
	for _, bad := range []string{"bogus", "unspecified", "terminate-other", ""} {
		_, err := parseOverlapPolicy(bad)
		if err == nil {
			t.Fatalf("parseOverlapPolicy(%q) succeeded", bad)
		}
		for _, slug := range []string{"skip", "buffer-one", "buffer-all", "cancel-other", "allow-all"} {
			if !strings.Contains(err.Error(), slug) {
				t.Errorf("error for %q does not name %s: %v", bad, slug, err)
			}
		}
	}
	if p, err := parseOverlapPolicy("allow-all"); err != nil || p != orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatalf("allow-all = %v, %v", p, err)
	}
}

func TestScheduleCreate_BuildsRequest(t *testing.T) {
	f, schedules := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--version", "0.2.0",
		"--cron", "0 2 * * *", "--cron", "30 14 * * 1", "--interval", "1h", "--timezone", "Europe/Lisbon",
		"--jitter", "30s", "--start-at", "2026-10-01T00:00:00Z", "--end-at", "2030-12-31T23:59:59Z",
		"--overlap", "buffer-one", "--catchup-window", "1h", "--pause-on-failure",
		"--input", "url=https://example.test", "--input-json", `{"mode":"full"}`,
		"--description", "Nightly reconcile", "--paused",
	).requireExit(t, exitOK).requireContains(t, "Created schedule nightly")

	var req orkstackv1.CreateScheduleRequest
	if !f.lastRequest(orkstackv1connect.ScheduleServiceCreateScheduleProcedure, &req) {
		t.Fatal("no CreateSchedule request was sent")
	}
	spec := req.GetSpec()
	if req.GetName() != "nightly" || req.GetOrkflowName() != "hello-ork" || req.GetOrkflowVersion() != "0.2.0" ||
		req.GetDescription() != "Nightly reconcile" || !req.GetPaused() {
		t.Fatalf("request = %v", &req)
	}
	if strings.Join(spec.GetCron(), "|") != "0 2 * * *|30 14 * * 1" || spec.GetInterval().AsDuration() != time.Hour ||
		spec.GetTimezone() != "Europe/Lisbon" || spec.GetJitter().AsDuration() != 30*time.Second ||
		!spec.GetStartAt().AsTime().Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) ||
		!spec.GetEndAt().AsTime().Equal(time.Date(2030, 12, 31, 23, 59, 59, 0, time.UTC)) ||
		spec.GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE ||
		spec.GetCatchupWindow().AsDuration() != time.Hour || !spec.GetPauseOnFailure() {
		t.Fatalf("spec = %v", spec)
	}
	if got := req.GetInput().GetData().AsMap(); got["url"] != "https://example.test" || got["mode"] != "full" {
		t.Fatalf("input = %v", got)
	}
	if len(schedules.records) != 1 {
		t.Fatalf("records = %v", schedules.records)
	}

	// No --overlap sends UNSPECIFIED; the server records it as skip.
	f.run("schedule", "create", "plain", "--orkflow", "hello-ork", "--cron", "@daily").requireExit(t, exitOK)
	f.lastRequest(orkstackv1connect.ScheduleServiceCreateScheduleProcedure, &req)
	if req.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		t.Fatalf("no --overlap sent %v", req.GetSpec().GetOverlap())
	}
	if schedules.records["default/plain"].GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP {
		t.Fatalf("server recorded %v", schedules.records["default/plain"].GetSpec().GetOverlap())
	}

	// Local refusals, before any request.
	before := len(f.requests())
	missingOrkflow := f.run("schedule", "create", "x", "--cron", "@daily")
	if missingOrkflow.code == exitOK || !strings.Contains(missingOrkflow.stderr, "--orkflow is required") {
		t.Fatalf("missing --orkflow = %+v", missingOrkflow)
	}
	missingWhen := f.run("schedule", "create", "x", "--orkflow", "hello-ork")
	if missingWhen.code == exitOK || !strings.Contains(missingWhen.stderr, "at least one cron expression or an interval is required") {
		t.Fatalf("missing --cron/--interval = %+v", missingWhen)
	}
	badOverlap := f.run("schedule", "create", "x", "--orkflow", "hello-ork", "--cron", "@daily", "--overlap", "unspecified")
	if badOverlap.code == exitOK || !strings.Contains(badOverlap.stderr, "allow-all") {
		t.Fatalf("--overlap unspecified = %+v", badOverlap)
	}
	if len(f.requests()) != before {
		t.Fatalf("a local refusal sent a request: %v", f.requests()[before:])
	}
}

func TestScheduleFire_NoOverlapFlagSendsNoOverride(t *testing.T) {
	f, schedules := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--cron", "@daily").requireExit(t, exitOK)
	f.run("schedule", "fire", "nightly").requireExit(t, exitOK)
	f.run("schedule", "fire", "nightly", "--overlap", "allow-all").requireExit(t, exitOK)
	f.run("schedule", "backfill", "nightly", "--start", "2026-09-01T00:00:00Z", "--end", "2026-09-01T00:03:00Z").requireExit(t, exitOK)
	f.run("schedule", "backfill", "nightly", "--start", "2026-09-01T00:00:00Z", "--end", "2026-09-01T00:03:00Z",
		"--overlap", "allow-all").requireExit(t, exitOK)
	unspecified, allowAll := orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL
	if len(schedules.fires) != 2 || schedules.fires[0] != unspecified || schedules.fires[1] != allowAll {
		t.Fatalf("fires = %v, want [UNSPECIFIED ALLOW_ALL]", schedules.fires)
	}
	if len(schedules.backfill) != 2 || schedules.backfill[0] != unspecified || schedules.backfill[1] != allowAll {
		t.Fatalf("backfills = %v, want [UNSPECIFIED ALLOW_ALL]", schedules.backfill)
	}
}

func TestScheduleUpdate_ReadModifyWrite(t *testing.T) {
	f, schedules := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--version", "0.2.0", "--cron", "0 2 * * *",
		"--timezone", "Europe/Lisbon", "--overlap", "allow-all", "--input", "url=x", "--description", "old").requireExit(t, exitOK)

	f.run("schedule", "update", "nightly", "--description", "new").requireExit(t, exitOK).
		requireContains(t, "Updated schedule nightly")
	var req orkstackv1.UpdateScheduleRequest
	if !f.lastRequest(orkstackv1connect.ScheduleServiceUpdateScheduleProcedure, &req) {
		t.Fatal("no UpdateSchedule request was sent")
	}
	want := &orkstackv1.UpdateScheduleRequest{
		Name: "nightly", OrkflowVersion: "0.2.0", Description: "new",
		Input: schedules.records["default/nightly"].GetInput(),
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon",
			Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL},
	}
	if !proto.Equal(&req, want) {
		t.Fatalf("update request = %v, want the full record with only the description changed: %v", &req, want)
	}

	f.run("schedule", "update", "nightly", "--follow-current", "--clear-input", "--cron", "@hourly",
		"--pause-on-failure=true").requireExit(t, exitOK)
	f.lastRequest(orkstackv1connect.ScheduleServiceUpdateScheduleProcedure, &req)
	if req.GetOrkflowVersion() != "" || req.GetInput() != nil || strings.Join(req.GetSpec().GetCron(), "|") != "@hourly" ||
		!req.GetSpec().GetPauseOnFailure() || req.GetSpec().GetTimezone() != "Europe/Lisbon" ||
		req.GetSpec().GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL || req.GetDescription() != "new" {
		t.Fatalf("second update = %v", &req)
	}
	if out := f.run("schedule", "update", "nightly", "--version", "1", "--follow-current"); out.code == exitOK {
		t.Fatal("--version with --follow-current was accepted")
	}
}

// ---- rendering goldens ----

func goldenSchedule() *orkstackv1.Schedule {
	input, _ := structpb.NewStruct(map[string]any{"mode": "full"})
	at := func(s string) *timestamppb.Timestamp {
		ts, _ := time.Parse(time.RFC3339, s)
		return timestamppb.New(ts)
	}
	return &orkstackv1.Schedule{
		Id: "sch-4a1c0e2b7d3f8156", Name: "nightly-reconcile", Namespace: "payments", OrkflowName: "reconcile",
		Input: &orkstackv1.Payload{Data: input}, Description: "Nightly reconcile",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon",
			Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP, CatchupWindow: durationpb.New(time.Hour),
			PauseOnFailure: true, Jitter: durationpb.New(30 * time.Second),
			StartAt: at("2026-10-01T00:00:00Z"), EndAt: at("2026-12-31T23:59:59Z")},
		NextFireTimes: []*timestamppb.Timestamp{at("2026-09-25T01:00:00Z"), at("2026-09-26T01:00:00Z")},
		RecentFires: []*orkstackv1.ScheduleFire{
			{ScheduledAt: at("2026-08-01T01:00:00Z"), ActualAt: at("2026-08-01T01:00:01Z")},
			{ScheduledAt: at("2026-09-23T01:00:00Z"), ActualAt: at("2026-09-23T01:00:01Z"),
				Refusal: `namespace "payments" is held: Q3 close`, DetailsAvailable: true},
			{ScheduledAt: at("2026-09-24T01:00:00Z"), ActualAt: at("2026-09-24T01:00:02Z"),
				RunId: "ork-20260924-7", DetailsAvailable: true},
			{ScheduledAt: at("2026-09-24T09:15:00Z"), ActualAt: at("2026-09-24T09:15:00Z"), DetailsAvailable: true},
		},
		FiresTaken: 142, FiresSkippedOverlap: 3, FiresMissedCatchup: 0,
		CreatedAt: at("2026-09-01T10:00:00Z"), UpdatedAt: at("2026-09-20T09:12:00Z"),
	}
}

func renderScheduleString(t *testing.T, s *orkstackv1.Schedule) string {
	t.Helper()
	var out bytes.Buffer
	if err := renderSchedule(&out, s); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestScheduleList_Golden(t *testing.T) {
	active := goldenSchedule()
	hourly := &orkstackv1.Schedule{Id: "sch-00000000000000a1", Name: "hourly-sync", OrkflowName: "sync", OrkflowVersion: "3",
		Spec: &orkstackv1.ScheduleSpec{Interval: durationpb.New(time.Hour)}, Paused: true, Note: "Q3 close"}
	refused := &orkstackv1.Schedule{Id: "sch-00000000000000a2", Name: "refusing", OrkflowName: "etl",
		Spec:        &orkstackv1.ScheduleSpec{Cron: []string{"*/15 * * * *"}},
		RecentFires: []*orkstackv1.ScheduleFire{{Refusal: "no live Orker", DetailsAvailable: true}}}
	broken := &orkstackv1.Schedule{Id: "sch-00000000000000a3", Name: "broken", OrkflowName: "etl", UnavailableReason: "cluster busy"}
	var out bytes.Buffer
	if err := renderScheduleList(&out, []*orkstackv1.Schedule{active, hourly, refused, broken}); err != nil {
		t.Fatal(err)
	}
	requireGolden(t, "schedule-list.golden", out.String())
}

func TestScheduleGet_Golden(t *testing.T) {
	requireGolden(t, "schedule-get.golden", renderScheduleString(t, goldenSchedule()))
}

func TestScheduleGet_Paused_Golden(t *testing.T) {
	s := goldenSchedule()
	s.Paused, s.Note, s.NextFireTimes = true, "Q3 close", nil
	requireGolden(t, "schedule-get-paused.golden", renderScheduleString(t, s))
}

func TestScheduleGet_Refused_Golden(t *testing.T) {
	s := goldenSchedule()
	s.RecentFires = s.RecentFires[1:2]
	s.RecentFires[0].Refusal = `pool "gpu" has no live Orker`
	requireGolden(t, "schedule-get-refused.golden", renderScheduleString(t, s))
}

func TestScheduleGet_Plain_Golden(t *testing.T) {
	s := &orkstackv1.Schedule{Id: "sch-00000000000000b1", Name: "plain", Namespace: "default", OrkflowName: "hello-ork",
		Spec:      &orkstackv1.ScheduleSpec{Interval: durationpb.New(15 * time.Minute), Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP},
		CreatedAt: timestamppb.New(fixedNow), UpdatedAt: timestamppb.New(fixedNow)}
	requireGolden(t, "schedule-get-plain.golden", renderScheduleString(t, s))
}

func TestScheduleGet_JSONCarriesFires(t *testing.T) {
	var out bytes.Buffer
	if err := writeJSON(&out, &orkstackv1.GetScheduleResponse{Schedule: goldenSchedule()}); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Schedule struct {
			Spec struct {
				Overlap string `json:"overlap"`
			} `json:"spec"`
			RecentFires []struct {
				ScheduledAt      string `json:"scheduledAt"`
				RunID            string `json:"runId"`
				DetailsAvailable bool   `json:"detailsAvailable"`
			} `json:"recentFires"`
		} `json:"schedule"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v\n%s", err, out.String())
	}
	if decoded.Schedule.Spec.Overlap != "SKIP" {
		t.Fatalf("overlap = %q, want SKIP", decoded.Schedule.Spec.Overlap)
	}
	fires := decoded.Schedule.RecentFires
	if len(fires) != 4 || fires[2].RunID != "ork-20260924-7" || !fires[2].DetailsAvailable ||
		fires[2].ScheduledAt != "2026-09-24T01:00:00Z" || fires[0].DetailsAvailable {
		t.Fatalf("recentFires = %+v", fires)
	}
}

func TestScheduleFire_PrintsWhereToLook(t *testing.T) {
	f, _ := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--cron", "@daily").requireExit(t, exitOK)
	f.run("schedule", "fire", "nightly").requireExit(t, exitOK).requireContains(t,
		"Fire requested for schedule nightly.", "'orkctl schedule get nightly'", "'orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=sch-")
}

func TestScheduleBackfill_ParsesWindow(t *testing.T) {
	f, _ := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--cron", "* * * * *").requireExit(t, exitOK)
	f.run("schedule", "backfill", "nightly", "--start", "2026-09-01T00:00:00Z", "--end", "2026-09-01T00:03:00Z").
		requireExit(t, exitOK).requireContains(t, "from 2026-09-01T00:00:00Z to 2026-09-01T00:03:00Z")
	var req orkstackv1.BackfillScheduleRequest
	f.lastRequest(orkstackv1connect.ScheduleServiceBackfillScheduleProcedure, &req)
	if !req.GetStart().AsTime().Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) ||
		!req.GetEnd().AsTime().Equal(time.Date(2026, 9, 1, 0, 3, 0, 0, time.UTC)) {
		t.Fatalf("window = %v – %v", req.GetStart(), req.GetEnd())
	}
	before := len(f.requests())
	if out := f.run("schedule", "backfill", "nightly", "--start", "yesterday", "--end", "2026-09-01T00:03:00Z"); out.code == exitOK ||
		!strings.Contains(out.stderr, "--start must be RFC3339") {
		t.Fatalf("bad --start = %+v", out)
	}
	if out := f.run("schedule", "backfill", "nightly", "--start", "2026-09-01T00:00:00Z"); out.code == exitOK {
		t.Fatalf("missing --end accepted")
	}
	if len(f.requests()) != before {
		t.Fatalf("a malformed window was sent")
	}
}

func TestScheduleDelete_Prints(t *testing.T) {
	f, _ := newScheduleFixture(t)
	f.run("schedule", "create", "nightly", "--orkflow", "hello-ork", "--cron", "@daily").requireExit(t, exitOK)
	f.run("schedule", "delete", "nightly").requireExit(t, exitOK).requireContains(t, "Deleted schedule nightly")
	out := f.run("schedule", "delete", "nightly")
	if out.code == exitOK || !strings.Contains(out.stderr, "not_found") {
		t.Fatalf("repeat delete = %+v", out)
	}
}

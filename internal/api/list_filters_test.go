package api_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// orkflowYAML is a minimal orkflow named name, labelled team=team.
func orkflowYAML(name, team string) string {
	return fmt.Sprintf(`name: %s
version: "1.0.0"
description: the %s orkflow
labels: {team: %s}
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`, name, name, team)
}

func listOrkflowsPage(t *testing.T, svc *api.Service, size int32, token string, texts ...string) *orkstackv1.ListOrkflowsResponse {
	t.Helper()
	resp, err := svc.ListOrkflows(context.Background(), connect.NewRequest(&orkstackv1.ListOrkflowsRequest{
		PageSize: size, PageToken: token, Filters: filters(t, texts...),
	}))
	if err != nil {
		t.Fatalf("ListOrkflows(size=%d filters=%q): %v", size, texts, err)
	}
	return resp.Msg
}

func orkflowNames(list []*orkstackv1.Orkflow) string {
	names := make([]string, len(list))
	for i, wf := range list {
		names[i] = wf.GetName()
	}
	return strings.Join(names, ",")
}

func TestListOrkflows_FiltersBeforePagingAndBindsTheToken(t *testing.T) {
	svc, _ := pagedService(t)
	for i, team := range []string{"payments", "search", "payments", "payments", "search", "payments"} {
		mustApply(t, svc, orkflowYAML(fmt.Sprintf("flow-%d", i), team), "")
	}
	first := listOrkflowsPage(t, svc, 2, "", "labels.team=payments")
	if got := orkflowNames(first.GetOrkflows()); got != "flow-0,flow-2" || first.GetNextPageToken() == "" {
		t.Fatalf("first page = %s (token %q), want flow-0,flow-2 and a token", got, first.GetNextPageToken())
	}
	second := listOrkflowsPage(t, svc, 2, first.GetNextPageToken(), "labels.team=payments")
	if got := orkflowNames(second.GetOrkflows()); got != "flow-3,flow-5" || second.GetNextPageToken() != "" {
		t.Fatalf("second page = %s (token %q), want flow-3,flow-5 and no token", got, second.GetNextPageToken())
	}
	if got := orkflowNames(listOrkflowsPage(t, svc, 0, "", "name^flow-", "description~flow-4").GetOrkflows()); got != "flow-4" {
		t.Fatalf("name^flow- description~flow-4 = %s", got)
	}
	_, err := svc.ListOrkflows(context.Background(), connect.NewRequest(&orkstackv1.ListOrkflowsRequest{
		PageSize: 2, PageToken: first.GetNextPageToken(), Filters: filters(t, "labels.team=search"),
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "page_token was issued for other filters") {
		t.Fatalf("token under another set = %v; want the other-filters refusal", err)
	}
}

func TestListOrkflows_FilteredPastTheDefaultPageHasAToken(t *testing.T) {
	svc, _ := pagedService(t)
	for i := range engine.DefaultRunPageSize + 1 {
		mustApply(t, svc, orkflowYAML(fmt.Sprintf("flow-%03d", i), "payments"), "")
	}
	mustApply(t, svc, orkflowYAML("other", "search"), "")
	page := listOrkflowsPage(t, svc, 0, "", "labels.team=payments")
	if len(page.GetOrkflows()) != engine.DefaultRunPageSize || page.GetNextPageToken() == "" {
		t.Fatalf("201 matches at page_size 0 = %d rows (token %q); want %d and a token",
			len(page.GetOrkflows()), page.GetNextPageToken(), engine.DefaultRunPageSize)
	}
	rest := listOrkflowsPage(t, svc, 0, page.GetNextPageToken(), "labels.team=payments")
	if got := orkflowNames(rest.GetOrkflows()); got != "flow-200" || rest.GetNextPageToken() != "" {
		t.Fatalf("second page = %s (token %q), want flow-200 alone", got, rest.GetNextPageToken())
	}
}

// TestListOrkflows_TimeFilters: created_at reaches the rows ListOrkflows
// matches, so an at-or-after or before bound selects by when each orkflow's
// listed version was applied.
func TestListOrkflows_TimeFilters(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	for name, day := range map[string]int{"flow-a": 1, "flow-b": 10, "flow-c": 15} {
		wf := &orkstackv1.Orkflow{
			Name: name, Version: "1.0.0",
			CreatedAt: timestamppb.New(time.Date(2026, time.March, day, 12, 0, 0, 0, time.UTC)),
		}
		if _, _, err := store.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s): %v", name, err)
		}
	}
	svc := api.NewService(api.Config{
		Orkflows: store, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Starter: &fakeStarter{}, NewRunID: func() string { return "run-001" },
	})
	for _, tc := range []struct {
		texts []string
		want  string
	}{
		{[]string{"created_at>=2026-03-10T12:00:00Z"}, "flow-b,flow-c"},
		{[]string{"created_at<2026-03-10T12:00:00Z"}, "flow-a"},
		{[]string{"created_at>=2026-03-02T00:00:00Z", "created_at<2026-03-15T00:00:00Z"}, "flow-b"},
	} {
		if got := orkflowNames(listOrkflowsPage(t, svc, 0, "", tc.texts...).GetOrkflows()); got != tc.want {
			t.Errorf("%q = %s, want %s", tc.texts, got, tc.want)
		}
	}
}

func TestListWebhooks_FiltersAndEmptyNamespaceSelectsDeploymentWide(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, req := range []*orkstackv1.RegisterWebhookRequest{
		{Url: "https://everything.example.test/hook"},
		{Url: "https://pay.example.test/hook", Namespace: "payments", Name: "pay-hook"},
		{Url: "https://search.example.test/hook", Namespace: "search", OrkflowName: "index"},
	} {
		if _, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(req)); err != nil {
			t.Fatalf("RegisterWebhook(%v): %v", req, err)
		}
	}
	urls := func(texts ...string) string {
		t.Helper()
		resp, err := f.webhookClient().ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{Filters: filters(t, texts...)}))
		if err != nil {
			t.Fatalf("ListWebhooks(%q): %v", texts, err)
		}
		var out []string
		for _, hook := range resp.Msg.GetWebhooks() {
			out = append(out, strings.Split(strings.TrimPrefix(hook.GetUrl(), "https://"), ".")[0])
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		texts []string
		want  string
	}{
		{nil, "everything,pay,search"},
		{[]string{"namespace="}, "everything"},
		{[]string{"namespace!="}, "pay,search"},
		{[]string{"namespace=payments"}, "pay"},
		{[]string{"url~search"}, "search"},
		{[]string{"orkflow_name=index"}, "search"},
		{[]string{"name=pay-hook"}, "pay"},
		{[]string{"name="}, "everything,search"},
		{[]string{"namespace=payments", "url~search"}, ""},
	} {
		if got := urls(tc.texts...); got != tc.want {
			t.Errorf("%q = %q, want %q", tc.texts, got, tc.want)
		}
	}
}

func TestListTriggers_FiltersWithinTheNamespace(t *testing.T) {
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Namespaces = newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
	})
	ctx := context.Background()
	mustApply(t, f.svc, helloYAML(t), "")
	mustApply(t, f.svc, helloYAML(t), "payments")
	mustApply(t, f.svc, orkflowYAML("other-ork", "x"), "")
	f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork", Name: "hello-a", Description: "first"})
	f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork", Name: "hello-b", OrkflowVersion: "0.2.0"})
	f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "other-ork", Name: "other-a"})
	f.createTrigger(t, &orkstackv1.CreateTriggerRequest{Namespace: "payments", OrkflowName: "hello-ork", Name: "hello-a"})
	names := func(namespace string, texts ...string) string {
		t.Helper()
		resp, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{
			Namespace: namespace, Filters: filters(t, texts...)}))
		if err != nil {
			t.Fatalf("ListTriggers(%q): %v", texts, err)
		}
		var out []string
		for _, trigger := range resp.Msg.GetTriggers() {
			out = append(out, trigger.GetNamespace()+"/"+trigger.GetName())
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		namespace string
		texts     []string
		want      string
	}{
		{"", []string{"orkflow_name=hello-ork"}, "default/hello-a,default/hello-b"},
		{"", []string{"orkflow_name!=hello-ork"}, "default/other-a"},
		{"", []string{"orkflow_name=hello-ork", "orkflow_version="}, "default/hello-a"},
		{"", []string{"name^hello", "description~first"}, "default/hello-a"},
		{"", []string{"orkflow_name="}, ""},
		{"payments", []string{"orkflow_name=hello-ork"}, "payments/hello-a"},
		{"payments", []string{"name=hello-b"}, ""},
	} {
		if got := names(tc.namespace, tc.texts...); got != tc.want {
			t.Errorf("namespace %q %q = %q, want %q", tc.namespace, tc.texts, got, tc.want)
		}
	}
}

func listSchedules(t *testing.T, h *scheduleHarness, texts ...string) []*orkstackv1.Schedule {
	t.Helper()
	resp, err := h.svc.ListSchedules(context.Background(), connect.NewRequest(&orkstackv1.ListSchedulesRequest{Filters: filters(t, texts...)}))
	if err != nil {
		t.Fatalf("ListSchedules(%q): %v", texts, err)
	}
	return resp.Msg.GetSchedules()
}

func scheduleNames(rows []*orkstackv1.Schedule) string {
	names := make([]string, len(rows))
	for i, row := range rows {
		names[i] = row.GetName()
	}
	return strings.Join(names, ",")
}

func TestListSchedules_IdentityFiltersSkipDescribe(t *testing.T) {
	h := newScheduleHarness(t)
	for _, name := range []string{"alpha", "bravo", "charlie", "delta"} {
		h.mustCreate(t, name)
	}
	h.schedules.described = 0
	if got := scheduleNames(listSchedules(t, h, "name=bravo")); got != "bravo" {
		t.Fatalf("name=bravo = %s", got)
	}
	if h.schedules.described != 1 {
		t.Fatalf("name=bravo described %d schedules; want only the one it returns", h.schedules.described)
	}
	h.schedules.described = 0
	if got := scheduleNames(listSchedules(t, h, "name^c", "orkflow_name=hello-ork")); got != "charlie" || h.schedules.described != 1 {
		t.Fatalf("name^c = %s with %d describes; want charlie and 1", got, h.schedules.described)
	}
	// A filter on a described field reads every identity-matching schedule.
	h.schedules.described = 0
	if got := scheduleNames(listSchedules(t, h, "paused=false")); got != "alpha,bravo,charlie,delta" || h.schedules.described != 4 {
		t.Fatalf("paused=false = %s with %d describes", got, h.schedules.described)
	}
}

func TestListSchedules_UnavailableRowKeptUnderPaused(t *testing.T) {
	h := newScheduleHarness(t)
	h.mustCreate(t, "nightly")
	h.schedules.extra = []engine.ScheduleView{
		{ID: "sch-0000000000000001", Name: "broken", Namespace: "default", OrkflowName: "hello-ork", Unavailable: "cluster busy"},
	}
	// nightly is measured and not paused; broken was not measured, so it is
	// kept under every filter on a field it could not report.
	if got := scheduleNames(listSchedules(t, h, "paused=true")); got != "broken" {
		t.Fatalf("paused=true = %s, want broken alone", got)
	}
	if got := scheduleNames(listSchedules(t, h, "description~nothing")); got != "broken" {
		t.Fatalf("description~nothing = %s, want broken alone", got)
	}
	// Identity is always measured: the unreachable row filters normally on it.
	if got := scheduleNames(listSchedules(t, h, "name=nightly", "paused=true")); got != "" {
		t.Fatalf("name=nightly paused=true = %s, want nothing", got)
	}
}

func TestListPools_Filters(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {Name: "default", AllowedPools: []string{"main", "gpu", "cold"}}},
		Pools:      livePools{task.DefaultQueue: 2, gpu: 1},
		PoolOrkers: poolOrkers{task.DefaultQueue: {}, gpu: {}},
	})
	pools := func(texts ...string) string {
		t.Helper()
		resp, err := svc.ListPools(context.Background(), connect.NewRequest(&orkstackv1.ListPoolsRequest{Filters: filters(t, texts...)}))
		if err != nil {
			t.Fatalf("ListPools(%q): %v", texts, err)
		}
		if resp.Msg.GetDefaultPool() != task.DefaultPool {
			t.Errorf("default_pool = %q; a filter never narrows it", resp.Msg.GetDefaultPool())
		}
		var out []string
		for _, pool := range resp.Msg.GetPools() {
			out = append(out, pool.GetName())
		}
		return strings.Join(out, ",")
	}
	if got := pools("is_default=true"); got != "main" {
		t.Errorf("is_default=true = %q, want main", got)
	}
	if got := pools("is_default=false"); got != "cold,gpu" {
		t.Errorf("is_default=false = %q, want cold,gpu", got)
	}
	if got := pools("name=gpu,cold"); got != "cold,gpu" {
		t.Errorf("name in = %q", got)
	}
}

// stubActivities is a catalog read with no registry behind it.
type stubActivities []*orkstackv1.ActivityInfo

func (s stubActivities) ListActivities(context.Context) ([]*orkstackv1.ActivityInfo, error) {
	out := make([]*orkstackv1.ActivityInfo, len(s))
	for i, a := range s {
		out[i] = proto.Clone(a).(*orkstackv1.ActivityInfo)
	}
	return out, nil
}

func TestListActivities_Filters(t *testing.T) {
	svc := api.NewService(api.Config{Activities: stubActivities{
		{Name: "shell", Reference: "registry.test/orkstack/activity-shell:dev", IndexDigest: "sha256:aaa"},
		{Name: "file", Reference: "registry.test/orkstack/activity-file:dev", IndexDigest: "sha256:bbb"},
		{Name: "http", Reference: "other.test/http:1", IndexDigest: "sha256:ccc"},
	}})
	names := func(texts ...string) string {
		t.Helper()
		resp, err := svc.ListActivities(context.Background(), connect.NewRequest(&orkstackv1.ListActivitiesRequest{Filters: filters(t, texts...)}))
		if err != nil {
			t.Fatalf("ListActivities(%q): %v", texts, err)
		}
		var out []string
		for _, a := range resp.Msg.GetActivities() {
			out = append(out, a.GetName())
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		texts []string
		want  string
	}{
		{nil, "file,http,shell"},
		{[]string{"reference^registry.test/"}, "file,shell"},
		{[]string{"index_digest=sha256:ccc"}, "http"},
		{[]string{"name!=shell", "reference~orkstack"}, "file"},
	} {
		if got := names(tc.texts...); got != tc.want {
			t.Errorf("%q = %q, want %q", tc.texts, got, tc.want)
		}
	}
}

func TestListReleases_BuildIDEqualityCountsOnlyThatRelease(t *testing.T) {
	stub := rolloutReleases()
	resp := listReleases(t, releaseService(stub, releaseFleet()), "v2")
	if got := builds(resp); got != "v2" {
		t.Fatalf("releases = %s, want v2", got)
	}
	if fmt.Sprint(stub.counted) != "[v2]" {
		t.Fatalf("counted runs of %v; an equality filter counts only that release", stub.counted)
	}
	// A fragment is no pushdown: every release is counted, then filtered.
	stub.counted = nil
	if got := builds(listReleases(t, releaseService(stub, releaseFleet()), "", "build_id^v2")); got != "v2" {
		t.Fatalf("build_id^v2 = %s", got)
	}
	if len(stub.counted) != 3 {
		t.Fatalf("build_id^v2 counted %v; want every release", stub.counted)
	}
}

func TestListReleases_RoleFilterLeavesDeploymentFieldsWhole(t *testing.T) {
	whole := listReleases(t, releaseService(rolloutReleases(), releaseFleet()), "")
	current := listReleases(t, releaseService(rolloutReleases(), releaseFleet()), "", "role=current")
	if got := builds(current); got != "v1" {
		t.Fatalf("role=current = %s, want v1", got)
	}
	if !proto.Equal(current.GetReleases()[0], releaseByBuild(whole, "v1")) {
		t.Fatalf("the filtered row differs from the unfiltered one:\n%v\n%v", current.GetReleases()[0], releaseByBuild(whole, "v1"))
	}
	trimmed := proto.Clone(whole).(*orkstackv1.ListReleasesResponse)
	trimmed.Releases = nil
	rest := proto.Clone(current).(*orkstackv1.ListReleasesResponse)
	rest.Releases = nil
	if !proto.Equal(trimmed, rest) {
		t.Fatalf("a role filter changed a deployment field:\n%v\n%v", trimmed, rest)
	}
	var drained []string
	for _, r := range whole.GetReleases() {
		if r.GetDrained() {
			drained = append(drained, r.GetBuildId())
		}
	}
	if got := builds(listReleases(t, releaseService(rolloutReleases(), releaseFleet()), "", "drained=true")); got != strings.Join(drained, ",") || got == "" {
		t.Fatalf("drained=true = %s, want the unfiltered answer's drained rows %v", got, drained)
	}
}

// TestListRequests_RefuseAnInvalidFilterUniformly: every filtered list
// answers an unknown field and an operator its field does not take with the
// same INVALID_ARGUMENT text, before it reads anything.
func TestListRequests_RefuseAnInvalidFilterUniformly(t *testing.T) {
	svc := api.NewService(api.Config{})
	ctx := context.Background()
	type call func([]*orkstackv1.FieldFilter) error
	for _, tc := range []struct {
		rpc, resource, wrongOp string
		call                   call
	}{
		{"ListNamespaces", "Namespace", "created_at~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListNamespaces(ctx, connect.NewRequest(&orkstackv1.ListNamespacesRequest{Filters: f}))
			return err
		}},
		{"ListOrkflows", "Orkflow", "created_at~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{Filters: f}))
			return err
		}},
		{"ListWebhooks", "Webhook", "id~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{Filters: f}))
			return err
		}},
		{"ListTriggers", "Trigger", "id~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{Filters: f}))
			return err
		}},
		{"ListSchedules", "Schedule", "paused~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListSchedules(ctx, connect.NewRequest(&orkstackv1.ListSchedulesRequest{Filters: f}))
			return err
		}},
		{"ListPools", "Pool", "is_default~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListPools(ctx, connect.NewRequest(&orkstackv1.ListPoolsRequest{Filters: f}))
			return err
		}},
		{"ListActivities", "ActivityInfo", "index_digest~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListActivities(ctx, connect.NewRequest(&orkstackv1.ListActivitiesRequest{Filters: f}))
			return err
		}},
		{"ListOrkers", "Orker", "cordoned~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListOrkers(ctx, connect.NewRequest(&orkstackv1.ListOrkersRequest{Filters: f}))
			return err
		}},
		{"ListReleases", "Release", "role~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{Filters: f}))
			return err
		}},
		{"ListDeliveries", "Delivery", "run_id~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListDeliveries(ctx, connect.NewRequest(&orkstackv1.ListDeliveriesRequest{Filters: f}))
			return err
		}},
		{"ListAuditRecords", "AuditRecord", "succeeded~x", func(f []*orkstackv1.FieldFilter) error {
			_, err := svc.ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{Filters: f}))
			return err
		}},
	} {
		err := tc.call(filters(t, "nope=x"))
		want := fmt.Sprintf(`filters[0]: "nope" is not a filterable field of %s; filterable: `, tc.resource)
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), want) {
			t.Errorf("%s nope=x = %v; want InvalidArgument %q", tc.rpc, err, want)
		}
		err = tc.call(filters(t, tc.wrongOp))
		field := strings.SplitN(tc.wrongOp, "~", 2)[0]
		want = fmt.Sprintf("filters[0]: %s does not take contains (~); it takes ", field)
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), want) {
			t.Errorf("%s %s = %v; want InvalidArgument %q", tc.rpc, tc.wrongOp, err, want)
		}
	}
}

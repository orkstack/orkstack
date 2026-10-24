package engine

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// runFilters compiles CLI-grammar filters against ListRunsRequest's
// allowlist, exactly as the ListRuns handler does.
func runFilters(t *testing.T, texts ...string) listfilter.Set {
	t.Helper()
	wire := make([]*orkstackv1.FieldFilter, 0, len(texts))
	for _, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		wire = append(wire, f)
	}
	set, err := listfilter.MustSpec(&orkstackv1.ListRunsRequest{}).Compile(wire)
	if err != nil {
		t.Fatalf("Compile(%q): %v", texts, err)
	}
	return set
}

func TestRunListPlan_PushesOnlySingleEqualities(t *testing.T) {
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	for _, tc := range []struct {
		name    string
		filters []string
		want    RunQuery // the pushdown fields only
	}{
		{name: "no filter"},
		{name: "one orkflow equals", filters: []string{"orkflow_name=deploy"}, want: RunQuery{OrkflowName: "deploy"}},
		{name: "an in of one is equals", filters: []string{"status=failed"}, want: RunQuery{Status: orkstackv1.Status_STATUS_FAILED}},
		{name: "two different status equals push nothing", filters: []string{"status=FAILED", "status=RUNNING"}},
		{name: "an empty orkflow equals pushes nothing", filters: []string{"orkflow_name="}},
		{name: "equals beside prefix on one path pushes nothing", filters: []string{"orkflow_name=deploy", "orkflow_name^dep"}},
		{name: "not equals pushes nothing", filters: []string{"status!=FAILED"}},
		{name: "in of two pushes nothing", filters: []string{"status=FAILED,CANCELED"}},
		{name: "contains pushes nothing", filters: []string{"orkflow_name~ep"}},
		{name: "origin kind alone", filters: []string{"origin.kind=TRIGGER"}, want: RunQuery{OriginKind: trigger}},
		{name: "origin kind and id", filters: []string{"origin.kind=trigger", "origin.id=trg-1"}, want: RunQuery{OriginKind: trigger, OriginID: "trg-1"}},
		{name: "an empty origin id is not pushed", filters: []string{"origin.kind=SUBMIT", "origin.id="}, want: RunQuery{OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}},
		{
			name:    "created_at bounds are the collapsed interval",
			filters: []string{"created_at>=2026-09-01T00:00:00Z", "created_at>=2026-10-01T00:00:00Z", "created_at<2026-10-02T00:00:00Z"},
			want:    RunQuery{Since: at, Until: at.Add(24 * time.Hour)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := runFilters(t, tc.filters...)
			got := RunListPlan(set)
			if got.OrkflowName != tc.want.OrkflowName || got.Status != tc.want.Status ||
				got.OriginKind != tc.want.OriginKind || got.OriginID != tc.want.OriginID ||
				!got.Since.Equal(tc.want.Since) || !got.Until.Equal(tc.want.Until) {
				t.Fatalf("plan = {orkflow %q status %s kind %s id %q since %v until %v}, want {orkflow %q status %s kind %s id %q since %v until %v}",
					got.OrkflowName, got.Status, got.OriginKind, got.OriginID, got.Since, got.Until,
					tc.want.OrkflowName, tc.want.Status, tc.want.OriginKind, tc.want.OriginID, tc.want.Since, tc.want.Until)
			}
			if (got.Keep == nil) != set.Empty() {
				t.Fatalf("Keep set = %v for a set of %d filters; want a residual exactly when filters exist", got.Keep != nil, set.Len())
			}
			if got.Namespace != "" || got.Limit != 0 || !got.BeforeCreated.IsZero() || got.Complete || got.Exhaustive {
				t.Fatalf("the plan set the caller's fields: %+v", got)
			}
		})
	}
}

func TestMatchesRunQuery_AppliesTheResidualFilters(t *testing.T) {
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	run := &orkstackv1.Run{
		Id: "ork-1", Namespace: "default", OrkflowName: "deploy-api", Status: orkstackv1.Status_STATUS_RUNNING,
		CreatedAt: timestamppb.New(at),
		Origin:    &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1"},
	}
	for _, tc := range []struct {
		filters []string
		want    bool
	}{
		{[]string{"status!=FAILED"}, true},
		{[]string{"status!=RUNNING"}, false},
		{[]string{"status=FAILED,RUNNING"}, true},
		{[]string{"status=FAILED,CANCELED"}, false},
		{[]string{"orkflow_name^deploy-"}, true},
		{[]string{"orkflow_name~api"}, true},
		{[]string{"orkflow_name~web"}, false},
		{[]string{"orkflow_name!=deploy-api"}, false},
		{[]string{"orkflow_name="}, false},
		{[]string{"status=FAILED", "status=RUNNING"}, false},
		{[]string{"created_at>=2026-10-01T09:00:00Z"}, true},
		{[]string{"created_at<2026-10-01T09:00:00Z"}, false},
		{[]string{"origin.kind=TRIGGER", "origin.id=trg-1"}, true},
		{[]string{"origin.kind=TRIGGER", "origin.id=trg-2"}, false},
		{[]string{"origin.kind=SCHEDULE"}, false},
	} {
		q := RunListPlan(runFilters(t, tc.filters...))
		q.Namespace = "default"
		if got := matchesRunQuery(run, q); got != tc.want {
			t.Errorf("matchesRunQuery(%q) = %v, want %v", tc.filters, got, tc.want)
		}
	}
}

func TestRunFilterView_CreatedAtIsTheIndexTime(t *testing.T) {
	started := time.Date(2026, 10, 1, 9, 0, 0, 123456789, time.UTC)
	onlyStarted := &orkstackv1.Run{Id: "ork-s", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_RUNNING,
		StartedAt: timestamppb.New(started)}
	view := RunFilterView(onlyStarted)
	if !view.GetCreatedAt().AsTime().Equal(RunIndexTime(onlyStarted)) {
		t.Fatalf("view created_at = %v, want RunIndexTime %v", view.GetCreatedAt().AsTime(), RunIndexTime(onlyStarted))
	}
	if view.GetStartedAt() != nil || view.GetTaskRuns() != nil {
		t.Fatalf("the view carries more than the filterable fields: %v", view)
	}
	// A run with only started_at is selected by created_at on its started_at,
	// at the microsecond the list orders it by.
	for _, tc := range []struct {
		filter string
		want   bool
	}{
		{"created_at>=2026-10-01T09:00:00.123456Z", true},
		{"created_at>=2026-10-01T09:00:00.123457Z", false},
		{"created_at<2026-10-01T09:00:00.123457Z", true},
		{"created_at<2026-10-01T09:00:00.123456Z", false},
	} {
		if got := runFilters(t, tc.filter).Match(RunFilterView(onlyStarted)); got != tc.want {
			t.Errorf("%s on a run that only started at %v = %v, want %v", tc.filter, started, got, tc.want)
		}
	}
	// A run with no time at all has no created_at and matches neither bound.
	timeless := &orkstackv1.Run{Id: "ork-t", OrkflowName: "deploy"}
	if RunFilterView(timeless).GetCreatedAt() != nil {
		t.Fatal("a run with no time got a created_at in its filter view")
	}
	for _, filter := range []string{"created_at>=1970-01-01T00:00:00Z", "created_at<2999-01-01T00:00:00Z"} {
		if runFilters(t, filter).Match(RunFilterView(timeless)) {
			t.Errorf("%s matched a run with no time", filter)
		}
	}
}

func TestRunStoreList_KeepSeesTheFilterView(t *testing.T) {
	store := NewRunStore()
	started := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	// In process, a run may carry started_at and no created_at.
	store.Save(&orkstackv1.Run{Id: "ork-started", Namespace: "default", OrkflowName: "deploy",
		Status: orkstackv1.Status_STATUS_RUNNING, StartedAt: timestamppb.New(started)})
	store.Save(&orkstackv1.Run{Id: "ork-old", Namespace: "default", OrkflowName: "deploy",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(started.Add(-time.Hour))})

	var seen []*orkstackv1.Run
	plan := RunListPlan(runFilters(t, "created_at>=2026-10-01T09:00:00Z"))
	keep := plan.Keep
	runs := store.List(ListFilter{Namespace: "default", Keep: func(view *orkstackv1.Run) bool {
		seen = append(seen, view)
		return keep(view)
	}})
	if len(runs) != 1 || runs[0].GetId() != "ork-started" {
		t.Fatalf("listed %v, want only ork-started (selected on its started_at)", runIDs(runs))
	}
	if runs[0].GetStartedAt() == nil {
		t.Fatal("the listed run is the view, not the stored run")
	}
	for _, view := range seen {
		if view.GetCreatedAt() == nil || view.GetStartedAt() != nil {
			t.Fatalf("Keep saw %v, want the filter view", view)
		}
	}
}

// countingArchive counts body reads, so a test can prove a key Keep rejects
// is never hydrated.
type countingArchive struct {
	artifacts.RunArchive
	mu    sync.Mutex
	reads []string
}

func (c *countingArchive) GetRun(ctx context.Context, runID string) ([]byte, error) {
	c.mu.Lock()
	c.reads = append(c.reads, runID)
	c.mu.Unlock()
	return c.RunArchive.GetRun(ctx, runID)
}

func TestRunArchiver_KeepSeesTheIndexProjection(t *testing.T) {
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	counting := &countingArchive{RunArchive: local}
	archiver := NewRunArchiver(counting)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	trigger := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1"}
	for i, status := range []orkstackv1.Status{
		orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_FAILED,
	} {
		run := &orkstackv1.Run{
			Id: "ork-" + string(rune('a'+i)), Namespace: "default", OrkflowName: "deploy", OrkflowVersion: "1.0.0",
			Status: status, CreatedAt: timestamppb.New(at.Add(time.Duration(i) * time.Minute)), Origin: trigger,
		}
		if err := archiver.Archive(context.Background(), run, nil); err != nil {
			t.Fatalf("Archive(%s): %v", run.GetId(), err)
		}
	}

	var seen []*orkstackv1.Run
	plan := RunListPlan(runFilters(t, "status!=FAILED"))
	runs, err := archiver.ListRuns(context.Background(), ArchiveQuery{
		Namespace: "default",
		Keep: func(view *orkstackv1.Run) bool {
			seen = append(seen, view)
			return plan.Keep(view)
		},
	})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if ids := runIDs(runs); !slices.Equal(ids, []string{"ork-c", "ork-a"}) {
		t.Fatalf("status!=FAILED listed %v, want [ork-c ork-a]", ids)
	}
	slices.Sort(counting.reads)
	if !slices.Equal(counting.reads, []string{"ork-a", "ork-c"}) {
		t.Fatalf("bodies read = %v, want only the two kept runs'", counting.reads)
	}
	if len(seen) != 4 {
		t.Fatalf("Keep saw %d entries, want all 4 keys", len(seen))
	}
	for _, view := range seen {
		if view.GetNamespace() != "default" || view.GetOrkflowName() != "deploy" ||
			view.GetStatus() == orkstackv1.Status_STATUS_UNSPECIFIED || view.GetCreatedAt() == nil {
			t.Fatalf("Keep saw %v, want the key's namespace, orkflow, status and created_at", view)
		}
		if view.GetOrigin() != nil {
			t.Fatalf("a run-index entry projected an origin: %v", view)
		}
		if view.GetOrkflowVersion() != "" {
			t.Fatalf("the projection read a body: %v", view)
		}
	}

	// From the origin family, the projection carries the origin.
	counting.reads, seen = nil, nil
	plan = RunListPlan(runFilters(t, "origin.kind=TRIGGER", "origin.id=trg-1", "status=CANCELED"))
	plan.Status = orkstackv1.Status_STATUS_UNSPECIFIED // let Keep, not the prefix, decide the status
	runs, err = archiver.ListRuns(context.Background(), ArchiveQuery{
		Namespace: "default", OriginKind: plan.OriginKind, OriginID: plan.OriginID,
		Keep: func(view *orkstackv1.Run) bool {
			seen = append(seen, view)
			return plan.Keep(view)
		},
	})
	if err != nil {
		t.Fatalf("ListRuns(origin): %v", err)
	}
	if ids := runIDs(runs); !slices.Equal(ids, []string{"ork-c"}) {
		t.Fatalf("origin filters listed %v, want [ork-c]", ids)
	}
	if !slices.Equal(counting.reads, []string{"ork-c"}) {
		t.Fatalf("bodies read = %v, want only ork-c's", counting.reads)
	}
	for _, view := range seen {
		if view.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER || view.GetOrigin().GetId() != "trg-1" {
			t.Fatalf("an origin-index entry projected %v, want its origin", view.GetOrigin())
		}
	}
}

func runIDs(runs []*orkstackv1.Run) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		out = append(out, run.GetId())
	}
	return out
}

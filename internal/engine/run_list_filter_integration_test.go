//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// runFilterCorpus is one namespace's runs split across BOTH halves ListRuns
// merges, on real infrastructure: live runs in a private Temporal namespace
// (started with the namespace and origin attributes stamped, and no worker,
// so they stay RUNNING), one run terminated there AND archived — the case the
// merge must deduplicate — and finished runs that only the archive, in a
// private MinIO bucket, still holds. The archived bodies are written through
// engine.RunArchiver, so their index keys are exactly the ones a finished run
// gets.
//
// Newest first, the corpus is:
//
//	live2      build-web   RUNNING    trigger trg-a   (live)
//	live1      deploy-api  RUNNING    submit          (live)
//	ended      deploy-api  CANCELED   trigger trg-a   (live, closed, and archived)
//	arch4      build-web   FAILED     submit          (archive only)
//	arch3      build-web   SUCCEEDED  trigger trg-a   (archive only)
//	arch2      deploy-api  FAILED     schedule sch-1  (archive only)
//	arch1      deploy-api  SUCCEEDED  submit          (archive only)
type runFilterCorpus struct {
	svc   *api.Service
	ids   map[string]string // label -> run id
	times map[string]time.Time
}

func newRunFilterCorpus(t *testing.T, ctx context.Context) *runFilterCorpus {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	cfg, _ := privateCleanupBucket(t, ctx)

	temporalNamespace := fmt.Sprintf("%s-runfilter-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)
	if indexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, temporalNamespace); err != nil || !indexed {
		t.Fatalf("provision the namespace index: %v (indexed=%v)", err, indexed)
	}
	if indexed, err := task.EnsureRunOriginSearchAttributes(ctx, temporalClient, temporalNamespace); err != nil || !indexed {
		t.Fatalf("provision the origin index: %v (indexed=%v)", err, indexed)
	}

	store, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	archiver := engine.NewRunArchiver(store)

	stamp := time.Now().UnixNano()
	c := &runFilterCorpus{ids: map[string]string{}, times: map[string]time.Time{}}
	for _, label := range []string{"live2", "live1", "ended", "arch4", "arch3", "arch2", "arch1"} {
		c.ids[label] = fmt.Sprintf("ork-runfilter-%s-%d", label, stamp)
	}

	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(true).WithOriginIndex(true)
	start := func(label, orkflow string, origin *engine.RunOrigin) {
		t.Helper()
		wf := &orkstackv1.Orkflow{
			Name: orkflow, Version: "0.1.0", Namespace: artifacts.DefaultNamespace,
			Tasks: []*orkstackv1.Task{{Id: "settle", Uses: "shell", Pool: task.DefaultPool}},
		}
		plan := engine.RunPlan{
			Namespace: artifacts.DefaultNamespace,
			Routes:    map[string]string{task.DefaultPool: task.DefaultQueue},
			Origin:    origin,
		}
		if err := starter.StartRun(ctx, c.ids[label], wf, nil, plan); err != nil {
			t.Fatalf("StartRun(%s): %v", label, err)
		}
		t.Cleanup(func() {
			_ = temporalClient.TerminateWorkflow(context.Background(), c.ids[label], "", "run filter integration test cleanup")
		})
	}
	trgA := &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-a"}
	// Started oldest first, so the live order is live2, live1, ended.
	start("ended", "deploy-api", trgA)
	start("live1", "deploy-api", &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"})
	start("live2", "build-web", trgA)
	if err := temporalClient.TerminateWorkflow(ctx, c.ids["ended"], "", "run filter integration test: end one"); err != nil {
		t.Fatalf("TerminateWorkflow(ended): %v", err)
	}

	reader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true).WithOriginIndex(true)
	c.svc = api.NewService(api.Config{Reader: reader, Runs: engine.NewRunStore(), Archive: archiver})

	// Wait for visibility: three runs, the terminated one closed.
	deadline := time.Now().Add(90 * time.Second)
	for {
		runs, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: artifacts.DefaultNamespace, Limit: 10})
		if err != nil {
			t.Fatalf("reader.ListRuns: %v", err)
		}
		closed := false
		for _, run := range runs {
			if run.GetId() == c.ids["ended"] && run.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
				closed = true
				c.times["ended"] = engine.RunIndexTime(run)
			}
			for _, label := range []string{"live1", "live2"} {
				if run.GetId() == c.ids[label] {
					c.times[label] = engine.RunIndexTime(run)
				}
			}
		}
		if len(runs) == 3 && closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("visibility listed %d runs (ended closed=%v), want 3 with ended CANCELED", len(runs), closed)
		}
		time.Sleep(time.Second)
	}

	// The archive: the terminated run's settled record at the instant
	// visibility orders it by, and four runs only the archive still holds,
	// an hour older than every live one.
	archive := func(label, orkflow string, status orkstackv1.Status, created time.Time, origin *orkstackv1.RunOrigin) {
		t.Helper()
		run := &orkstackv1.Run{
			Id: c.ids[label], Namespace: artifacts.DefaultNamespace, OrkflowName: orkflow, OrkflowVersion: "0.1.0",
			Status: status, CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(created.Add(time.Second)),
			Origin: origin,
		}
		if err := archiver.Archive(ctx, run, nil); err != nil {
			t.Fatalf("Archive(%s): %v", label, err)
		}
		c.times[label] = engine.RunIndexTime(run)
	}
	trigger := func(id string) *orkstackv1.RunOrigin {
		return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: id}
	}
	submit := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}
	archive("ended", "deploy-api", orkstackv1.Status_STATUS_CANCELED, c.times["ended"], trigger("trg-a"))
	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	archive("arch1", "deploy-api", orkstackv1.Status_STATUS_SUCCEEDED, base, submit)
	archive("arch2", "deploy-api", orkstackv1.Status_STATUS_FAILED, base.Add(time.Minute),
		&orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, Id: "sch-1"})
	archive("arch3", "build-web", orkstackv1.Status_STATUS_SUCCEEDED, base.Add(2*time.Minute), trigger("trg-a"))
	archive("arch4", "build-web", orkstackv1.Status_STATUS_FAILED, base.Add(3*time.Minute), submit)
	return c
}

// filters parses CLI-grammar texts into wire filters.
func (c *runFilterCorpus) filters(t *testing.T, texts ...string) []*orkstackv1.FieldFilter {
	t.Helper()
	out := make([]*orkstackv1.FieldFilter, 0, len(texts))
	for _, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		out = append(out, f)
	}
	return out
}

func (c *runFilterCorpus) labels(t *testing.T, runs []*orkstackv1.Run) []string {
	t.Helper()
	byID := map[string]string{}
	for label, id := range c.ids {
		byID[id] = label
	}
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		label, ok := byID[run.GetId()]
		if !ok {
			t.Fatalf("listed %s, which this test never started", run.GetId())
		}
		out = append(out, label)
	}
	return out
}

func (c *runFilterCorpus) instant(label string) string {
	return c.times[label].UTC().Format(time.RFC3339Nano)
}

// TestIntegration_RunFiltersAnswerAlikeLiveAndArchived proves the one filter
// set is applied to both halves: for each filter, the live runs the cluster
// lists and the finished runs only the archive holds are selected by the same
// rule, the run in both halves is listed once (from the archive), and nothing
// that does not match leaks from either side.
func TestIntegration_RunFiltersAnswerAlikeLiveAndArchived(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := newRunFilterCorpus(t, ctx)

	for _, tc := range []struct {
		filters []string
		want    []string
	}{
		{nil, []string{"live2", "live1", "ended", "arch4", "arch3", "arch2", "arch1"}},
		{[]string{"orkflow_name=deploy-api"}, []string{"live1", "ended", "arch2", "arch1"}},
		{[]string{"orkflow_name!=deploy-api"}, []string{"live2", "arch4", "arch3"}},
		{[]string{"orkflow_name=build-web,deploy-api"}, []string{"live2", "live1", "ended", "arch4", "arch3", "arch2", "arch1"}},
		{[]string{"orkflow_name~web"}, []string{"live2", "arch4", "arch3"}},
		{[]string{"orkflow_name^deploy"}, []string{"live1", "ended", "arch2", "arch1"}},
		{[]string{"status=RUNNING"}, []string{"live2", "live1"}},
		{[]string{"status!=RUNNING"}, []string{"ended", "arch4", "arch3", "arch2", "arch1"}},
		{[]string{"status=FAILED,CANCELED"}, []string{"ended", "arch4", "arch2"}},
		{[]string{"status=SUCCEEDED", "orkflow_name=build-web"}, []string{"arch3"}},
		{[]string{"created_at>=" + c.instant("arch2"), "created_at<" + c.instant("arch4")}, []string{"arch3", "arch2"}},
		{[]string{"created_at>=" + c.instant("ended")}, []string{"live2", "live1", "ended"}},
		{[]string{"origin.kind=TRIGGER"}, []string{"live2", "ended", "arch3"}},
		{[]string{"origin.kind=TRIGGER", "origin.id=trg-a"}, []string{"live2", "ended", "arch3"}},
		{[]string{"origin.kind=SCHEDULE", "origin.id=sch-1"}, []string{"arch2"}},
		{[]string{"origin.kind=submit", "orkflow_name=deploy-api"}, []string{"live1", "arch1"}},
	} {
		resp, err := c.svc.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{Filters: c.filters(t, tc.filters...)}))
		if err != nil {
			t.Fatalf("ListRuns(%q): %v", tc.filters, err)
		}
		got := c.labels(t, resp.Msg.GetRuns())
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ListRuns(%q) = %v, want %v", tc.filters, got, tc.want)
			continue
		}
		for _, run := range resp.Msg.GetRuns() {
			if run.GetId() == c.ids["ended"] && run.GetOrkflowVersion() != "0.1.0" {
				t.Errorf("ListRuns(%q) listed the run in both halves from the index, want the archive's record", tc.filters)
			}
		}
		t.Logf("EVIDENCE run filters %s -> %s", strings.Join(tc.filters, " "), strings.Join(got, " "))
	}
}

// TestIntegration_RunFilterPagesVisitEveryMatchOnce pages a filtered
// live+archive union one run at a time: every match is visited exactly once,
// in order, across the boundary between the two halves.
func TestIntegration_RunFilterPagesVisitEveryMatchOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := newRunFilterCorpus(t, ctx)

	for _, tc := range []struct {
		filters []string
		want    []string
	}{
		{[]string{"orkflow_name^deploy"}, []string{"live1", "ended", "arch2", "arch1"}},
		{[]string{"status!=FAILED"}, []string{"live2", "live1", "ended", "arch3", "arch1"}},
		{[]string{"origin.kind=TRIGGER", "origin.id=trg-a"}, []string{"live2", "ended", "arch3"}},
	} {
		var visited []string
		req := &orkstackv1.ListRunsRequest{PageSize: 1, Filters: c.filters(t, tc.filters...)}
		for page := 0; ; page++ {
			if page > len(tc.want)+1 {
				t.Fatalf("%q did not end after %d pages: %v", tc.filters, page, visited)
			}
			resp, err := c.svc.ListRuns(ctx, connect.NewRequest(req))
			if err != nil {
				t.Fatalf("ListRuns(%q) page %d: %v", tc.filters, page+1, err)
			}
			visited = append(visited, c.labels(t, resp.Msg.GetRuns())...)
			if resp.Msg.GetNextPageToken() == "" {
				break
			}
			req.PageToken = resp.Msg.GetNextPageToken()
		}
		if !reflect.DeepEqual(visited, tc.want) {
			t.Errorf("pages of one under %q visited %v, want %v (each once, in order)", tc.filters, visited, tc.want)
		}
		t.Logf("EVIDENCE run filter pages %s -> %s", strings.Join(tc.filters, " "), strings.Join(visited, " "))
	}

	// A token is bound to its filter set.
	first, err := c.svc.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		PageSize: 1, Filters: c.filters(t, "orkflow_name^deploy"),
	}))
	if err != nil || first.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, %v", first, err)
	}
	_, err = c.svc.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		PageSize: 1, PageToken: first.Msg.GetNextPageToken(), Filters: c.filters(t, "orkflow_name^build"),
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "page_token was issued for other filters") {
		t.Fatalf("a token replayed under other filters = %v, want the filter-set refusal", err)
	}
}

// runListQuery is the RunQuery the ListRuns handler builds for filters
// written in the CLI grammar: compiled against ListRunsRequest's allowlist,
// then planned by engine.RunListPlan, scoped to namespace and capped at limit.
func runListQuery(t *testing.T, namespace string, limit int, texts ...string) engine.RunQuery {
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
	q := engine.RunListPlan(set)
	q.Namespace, q.Limit = namespace, limit
	return q
}

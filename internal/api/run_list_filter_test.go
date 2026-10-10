package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// runListFixture is a namespace's runs split across both halves ListRuns
// merges: finished runs in the archive and live ones in the run index, with
// one finished run the index still lists (and, for one of them, still reads
// as RUNNING — visibility lags the archive).
type runListFixture struct {
	svc    *api.Service
	reader *fakeReader
}

func newRunListFixture(t *testing.T) *runListFixture {
	t.Helper()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	run := func(id, orkflow string, minute int, status orkstackv1.Status) *orkstackv1.Run {
		return &orkstackv1.Run{
			Id: id, Namespace: "default", OrkflowName: orkflow, OrkflowVersion: "1.0.0", Status: status,
			CreatedAt: timestamppb.New(at.Add(time.Duration(minute) * time.Minute)),
			Origin:    &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT},
		}
	}
	archiver := newArchiver(t)
	for _, archived := range []*orkstackv1.Run{
		run("ork-a1", "deploy", 1, orkstackv1.Status_STATUS_SUCCEEDED),
		run("ork-a2", "deploy", 2, orkstackv1.Status_STATUS_FAILED),
		run("ork-a3", "build", 3, orkstackv1.Status_STATUS_SUCCEEDED),
		run("ork-a4", "build", 4, orkstackv1.Status_STATUS_CANCELED),
		// ork-lag finished SUCCEEDED; the index has not caught up yet.
		run("ork-lag", "deploy", 5, orkstackv1.Status_STATUS_SUCCEEDED),
	} {
		if err := archiver.Archive(context.Background(), archived, nil); err != nil {
			t.Fatalf("Archive(%s): %v", archived.GetId(), err)
		}
	}
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{}}
	// ork-a2 is in both halves with the same status: it must appear once.
	reader.setRun(run("ork-a2", "deploy", 2, orkstackv1.Status_STATUS_FAILED))
	reader.setRun(run("ork-lag", "deploy", 5, orkstackv1.Status_STATUS_RUNNING))
	reader.setRun(run("ork-l1", "deploy", 6, orkstackv1.Status_STATUS_RUNNING))
	reader.setRun(run("ork-l2", "build", 7, orkstackv1.Status_STATUS_RUNNING))
	return &runListFixture{
		svc:    api.NewService(api.Config{Reader: reader, Runs: engine.NewRunStore(), Archive: archiver}),
		reader: reader,
	}
}

func (f *runListFixture) list(t *testing.T, req *orkstackv1.ListRunsRequest) ([]string, map[string]orkstackv1.Status, string) {
	t.Helper()
	resp, err := f.svc.ListRuns(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("ListRuns(%v): %v", req, err)
	}
	ids := []string{}
	statuses := map[string]orkstackv1.Status{}
	for _, r := range resp.Msg.GetRuns() {
		ids = append(ids, r.GetId())
		statuses[r.GetId()] = r.GetStatus()
	}
	return ids, statuses, resp.Msg.GetNextPageToken()
}

func TestListRuns_MergesLiveAndArchivedRunsUnderFilters(t *testing.T) {
	f := newRunListFixture(t)
	for _, tc := range []struct {
		name    string
		filters []string
		want    []string
	}{
		{"no filter", nil, []string{"ork-l2", "ork-l1", "ork-lag", "ork-a4", "ork-a3", "ork-a2", "ork-a1"}},
		{"orkflow equals", []string{"orkflow_name=deploy"}, []string{"ork-l1", "ork-lag", "ork-a2", "ork-a1"}},
		{"orkflow not equals", []string{"orkflow_name!=deploy"}, []string{"ork-l2", "ork-a4", "ork-a3"}},
		{"orkflow in", []string{"orkflow_name=build,deploy"}, []string{"ork-l2", "ork-l1", "ork-lag", "ork-a4", "ork-a3", "ork-a2", "ork-a1"}},
		{"orkflow contains", []string{"orkflow_name~uil"}, []string{"ork-l2", "ork-a4", "ork-a3"}},
		{"orkflow prefix", []string{"orkflow_name^dep"}, []string{"ork-l1", "ork-lag", "ork-a2", "ork-a1"}},
		{"status equals", []string{"status=FAILED"}, []string{"ork-a2"}},
		{"status in", []string{"status=FAILED,CANCELED"}, []string{"ork-a4", "ork-a2"}},
		{"status not equals twice", []string{"status!=SUCCEEDED", "status!=RUNNING"}, []string{"ork-a4", "ork-a2"}},
		{"created_at window", []string{"created_at>=2026-10-01T09:02:00Z", "created_at<2026-10-01T09:06:00Z"}, []string{"ork-lag", "ork-a4", "ork-a3", "ork-a2"}},
		{"origin kind", []string{"origin.kind=SUBMIT", "orkflow_name=build"}, []string{"ork-l2", "ork-a4", "ork-a3"}},
		{"origin kind no match", []string{"origin.kind=TRIGGER"}, []string{}},
		{"two different equals", []string{"status=FAILED", "status=SUCCEEDED"}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, _ := f.list(t, &orkstackv1.ListRunsRequest{Filters: filters(t, tc.filters...)})
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("listed %v, want %v newest first with no duplicate", got, tc.want)
			}
		})
	}

	// The archive's settled record wins where both halves hold a run.
	_, statuses, _ := f.list(t, &orkstackv1.ListRunsRequest{Filters: filters(t, "status=SUCCEEDED")})
	if statuses["ork-lag"] != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status=SUCCEEDED listed ork-lag as %s, want the archive's SUCCEEDED", statuses["ork-lag"])
	}
	// The accepted lag: under a status filter that excludes the settled
	// status, the archive rejects its copy and the stale live row is listed
	// once, as RUNNING — never twice, never as SUCCEEDED.
	ids, statuses, _ := f.list(t, &orkstackv1.ListRunsRequest{Filters: filters(t, "status!=SUCCEEDED")})
	if want := []string{"ork-l2", "ork-l1", "ork-lag", "ork-a4", "ork-a2"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("status!=SUCCEEDED listed %v, want %v", ids, want)
	}
	if statuses["ork-lag"] != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("status!=SUCCEEDED listed ork-lag as %s, want the lagging RUNNING", statuses["ork-lag"])
	}

	// Paging under filters through the union visits every match once.
	var paged []string
	req := &orkstackv1.ListRunsRequest{PageSize: 1, Filters: filters(t, "orkflow_name^dep", "status!=CANCELED")}
	for page := 0; page < 10; page++ {
		ids, _, token := f.list(t, req)
		paged = append(paged, ids...)
		if token == "" {
			break
		}
		req.PageToken = token
	}
	if want := []string{"ork-l1", "ork-lag", "ork-a2", "ork-a1"}; !reflect.DeepEqual(paged, want) {
		t.Fatalf("pages of one visited %v, want %v", paged, want)
	}

	// The live half is asked with the pushdowns, and only the single
	// equalities: status!= reached it as no status predicate at all.
	queries := f.reader.runQueries()
	for _, q := range queries {
		if q.Keep == nil && q.Status == orkstackv1.Status_STATUS_UNSPECIFIED && q.OrkflowName == "" && q.Since.IsZero() {
			continue // the unfiltered listing
		}
		if q.Keep == nil {
			t.Fatalf("a filtered listing asked the live half %+v without its residual filters", q)
		}
	}
}

func TestListRuns_OriginIDRequiresOriginKind(t *testing.T) {
	svc := api.NewService(api.Config{Runs: engine.NewRunStore()})
	_, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: filters(t, "origin.id=trg-1"),
	}))
	const want = "filters[0]: origin.id needs an equals (=) filter on origin.kind"
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), want) {
		t.Fatalf("origin.id alone = %v, want InvalidArgument %q", err, want)
	}
	for _, text := range []string{"origin.kind!=TRIGGER", "origin.kind=TRIGGER,SUBMIT", "origin.id~trg"} {
		if _, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
			Filters: filters(t, "origin.kind=TRIGGER", text),
		})); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s = %v, want InvalidArgument: origin filters take equals only", text, err)
		}
	}
	if _, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: filters(t, "origin.kind=parent-run", "origin.id=ork-1"),
	})); err != nil {
		t.Fatalf("origin.kind=parent-run with an id = %v, want accepted", err)
	}
}

func TestListRuns_AwaitingIsNotAnAllowedStatus(t *testing.T) {
	svc := api.NewService(api.Config{Runs: engine.NewRunStore()})
	_, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: filters(t, "status=AWAITING"),
	}))
	const want = `filters[0]: "AWAITING" is not a status this list can select; status is one of PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED, SKIPPED`
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), want) {
		t.Fatalf("status=AWAITING = %v, want InvalidArgument %q", err, want)
	}
}

func TestListRuns_OrkflowNameIsHeldToTheNameRule(t *testing.T) {
	svc := api.NewService(api.Config{Runs: engine.NewRunStore()})
	for _, text := range []string{"orkflow_name=Deploy", "orkflow_name!=Deploy", "orkflow_name=deploy,Deploy"} {
		_, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
			Filters: filters(t, text),
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), `"Deploy" is not a slug`) {
			t.Fatalf("%s = %v, want the name rule's refusal", text, err)
		}
	}
	// A fragment is not a name, and the empty value is what an unset name reads as.
	for _, text := range []string{"orkflow_name~Dep", "orkflow_name^D", "orkflow_name="} {
		if _, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
			Filters: filters(t, text),
		})); err != nil {
			t.Fatalf("%s = %v, want accepted", text, err)
		}
	}
}

func TestListRuns_OldTokenIsRefused(t *testing.T) {
	runs := engine.NewRunStore()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	for _, id := range []string{"ork-a", "ork-b"} {
		runs.Save(&orkstackv1.Run{Id: id, OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(at)})
	}
	svc := api.NewService(api.Config{Runs: runs})
	for name, token := range map[string]map[string]any{
		"version 1, the typed filters' shape": {"v": 1, "n": "default", "o": "deploy", "s": 3, "c": at.Format(time.RFC3339Nano), "i": "ork-b"},
		"version 3":                           {"v": 3, "n": "default", "f": "", "c": at.Format(time.RFC3339Nano), "i": "ork-b"},
	} {
		raw, err := json.Marshal(token)
		if err != nil {
			t.Fatal(err)
		}
		_, err = svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
			PageToken: base64.RawURLEncoding.EncodeToString(raw),
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "invalid page_token") {
			t.Fatalf("%s = %v, want invalid page_token", name, err)
		}
	}
	// A minted token is version 2 and binds the namespace and the filter key.
	resp, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
		PageSize: 1, Filters: filters(t, "orkflow_name=deploy"),
	}))
	if err != nil || resp.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, %v", resp, err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(resp.Msg.GetNextPageToken())
	if err != nil {
		t.Fatal(err)
	}
	var minted map[string]any
	if err := json.Unmarshal(raw, &minted); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(minted))
	for k := range minted {
		keys = append(keys, k)
	}
	if minted["v"] != float64(2) || minted["n"] != "default" || minted["f"] == "" || len(minted) != 5 {
		t.Fatalf("minted token = %v (keys %v), want {v:2, n, f, c, i}", minted, keys)
	}
}

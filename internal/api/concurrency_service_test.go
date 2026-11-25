package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func limitsService(t *testing.T, reader *fakeConcReader) *api.Service {
	t.Helper()
	return api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(),
		Starter: &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, ConcurrencyReader: reader})
}

func gateView(key string, holders, queued int) engine.GateView {
	at := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	v := engine.GateView{GateID: engine.GateID("default", "", key), Namespace: "default", Key: key, Answering: true, Epoch: 2,
		Rebuild: engine.GateRebuildNone}
	for i := range holders {
		v.Holders = append(v.Holders, engine.GateEntryView{Run: fmt.Sprintf("ork-h%d", i), GrantedAt: at, Strategy: engine.ConcurrencyStrategyQueue, Limit: 1})
	}
	for i := range queued {
		v.Line = append(v.Line, engine.GateEntryView{Run: fmt.Sprintf("ork-q%d", i), ArrivedAt: at, TimesOutAt: at.Add(2 * time.Hour),
			Strategy: engine.ConcurrencyStrategyQueue, Limit: 1})
	}
	return v
}

// TestGetConcurrencyLimit_ReadsTheKeysLimit: one key's holders and line,
// each cut to api_holders_shown with the totals beside them; a key nothing
// holds is NOT_FOUND, a limit that does not answer says so, and a key that
// breaks the key rule is refused.
func TestGetConcurrencyLimit_ReadsTheKeysLimit(t *testing.T) {
	reader := &fakeConcReader{gates: []engine.GateView{gateView("db:prod", 1, 12)}}
	svc := limitsService(t, reader)
	get := func(key string) (*orkstackv1.ConcurrencyLimit, error) {
		resp, err := svc.GetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.GetConcurrencyLimitRequest{Key: key}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetLimit(), nil
	}
	limit, err := get("db:prod")
	must.OK(t, err)
	shown := engine.DefaultConcurrencySettings().APIHoldersShown
	if !limit.GetAnswering() || limit.GetKey() != "db:prod" || limit.GetEpoch() != 2 || limit.GetHoldersCount() != 1 ||
		limit.GetQueuedCount() != 12 || len(limit.GetLine()) != shown || limit.GetHolders()[0].GetRunId() != "ork-h0" ||
		limit.GetLine()[0].GetTimesOutAt() == nil || limit.GetRebuild() != orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_NONE {
		t.Fatalf("limit = %v; want its holder, %d of its 12 waiting, epoch 2", limit, shown)
	}
	if _, err := get("db:staging"); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a key nothing holds = %v; want not_found", err)
	}
	if _, err := get("ork:mine"); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a reserved key = %v; want invalid_argument", err)
	}
	reader.gatesErr = errors.New("query failed")
	limit, err = get("db:prod")
	must.OK(t, err)
	// The reason is the reader's sentence; the cluster's error is logged,
	// never shown.
	if limit.GetAnswering() || limit.GetNotAnsweringReason() != engine.GateNotAnswering ||
		strings.Contains(limit.GetNotAnsweringReason(), "query failed") || len(limit.GetHolders()) != 0 {
		t.Fatalf("an unanswering limit = %v; want answering false, its sentence, and nothing else", limit)
	}
}

// TestListConcurrencyLimits_PagesAndNarrows: a namespace's keys a page at a
// time, a key prefix filter narrowing each page, one key=VALUE read alone, a
// filter on a field the list does not declare refused, and a page token
// carried to other filters refused.
func TestListConcurrencyLimits_PagesAndNarrows(t *testing.T) {
	old := gateView("db:old", 1, 0)
	old.GateID = engine.GateID("default", "2026-01-01T00:00:00Z", "db:old") // an earlier namespace of this name
	reader := &fakeConcReader{gates: []engine.GateView{gateView("db:prod", 1, 0), gateView("db:staging", 1, 1), old,
		gateView("registry-push", 2, 0), {GateID: "ork-conc-x", Namespace: "default", NotAnswering: engine.GateNotAnswering}}}
	svc := limitsService(t, reader)
	filters := func(texts ...string) []*orkstackv1.FieldFilter {
		out := make([]*orkstackv1.FieldFilter, 0, len(texts))
		for _, text := range texts {
			f, err := listfilter.Parse(text)
			must.OK(t, err)
			out = append(out, f)
		}
		return out
	}
	list := func(req *orkstackv1.ListConcurrencyLimitsRequest) (*orkstackv1.ListConcurrencyLimitsResponse, error) {
		resp, err := svc.ListConcurrencyLimits(context.Background(), connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	var keys []string
	token, carried := "", ""
	for range 4 {
		page, err := list(&orkstackv1.ListConcurrencyLimitsRequest{PageSize: 3, PageToken: token, Filters: filters("key^db:")})
		must.OK(t, err)
		if carried == "" {
			carried = page.GetNextPageToken()
		}
		for _, l := range page.GetLimits() {
			keys = append(keys, l.GetKey()+fmt.Sprint(l.GetAnswering()))
		}
		if token = page.GetNextPageToken(); token == "" {
			break
		}
	}
	if got := strings.Join(keys, ","); got != "db:prodtrue,db:stagingtrue,false" {
		t.Fatalf("listed %s; want the two db: keys and the unanswering limit", got)
	}
	one, err := list(&orkstackv1.ListConcurrencyLimitsRequest{Filters: filters("key=registry-push")})
	must.OK(t, err)
	if len(one.GetLimits()) != 1 || one.GetLimits()[0].GetHoldersCount() != 2 {
		t.Fatalf("one key = %v", one.GetLimits())
	}
	none, err := list(&orkstackv1.ListConcurrencyLimitsRequest{Filters: filters("key=idle")})
	must.OK(t, err)
	if len(none.GetLimits()) != 0 {
		t.Fatalf("an idle key listed %v; want none", none.GetLimits())
	}
	excluded, err := list(&orkstackv1.ListConcurrencyLimitsRequest{Filters: filters("key=registry-push", "key^db:")})
	must.OK(t, err)
	// Two filters on key read the whole list; only the limit whose key is
	// unknown is left.
	if len(excluded.GetLimits()) != 1 || excluded.GetLimits()[0].GetKey() != "" {
		t.Fatalf("a key its other filter excludes listed %v; want only the unanswering limit", excluded.GetLimits())
	}
	if _, err := list(&orkstackv1.ListConcurrencyLimitsRequest{Filters: filters("holders_count=1")}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a filter on an undeclared field = %v; want invalid_argument", err)
	}
	if carried == "" {
		t.Fatal("the first db: page had no next page; the test needs one to carry")
	}
	if _, err := list(&orkstackv1.ListConcurrencyLimitsRequest{PageToken: carried, Filters: filters("key^registry")}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a page token carried to other filters = %v; want invalid_argument", err)
	}
	reader.gatesErr = errors.New("visibility down")
	if _, err := list(&orkstackv1.ListConcurrencyLimitsRequest{}); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("a list the cluster cannot answer = %v; want unavailable", err)
	}
}

// TestConcurrencyLimits_ReadAnOrkflowsOwnKey: the limit of an Orkflow that
// declares no key is read under the key a submit plans for it, by get and by
// list's key=, however long its names are.
func TestConcurrencyLimits_ReadAnOrkflowsOwnKey(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	planner := concService(t, starter, &fakeCapabilities{}, nil)
	must.OK(t, applyDoc(t, planner, limitedYAML("  limit: 1")))
	must.OK(t, submitName(planner, "migrate"))
	long := strings.Repeat("a", 200)
	for _, key := range []string{starter.plans[0].Concurrency.Slots[0].Key, engine.DefaultRunConcurrencyKey(long, long)} {
		svc := limitsService(t, &fakeConcReader{gates: []engine.GateView{gateView(key, 1, 0)}})
		got, err := svc.GetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.GetConcurrencyLimitRequest{Key: key}))
		must.OK(t, err)
		f, err := listfilter.Parse("key=" + key)
		must.OK(t, err)
		list, err := svc.ListConcurrencyLimits(context.Background(), connect.NewRequest(&orkstackv1.ListConcurrencyLimitsRequest{Filters: []*orkstackv1.FieldFilter{f}}))
		must.OK(t, err)
		if got.Msg.GetLimit().GetKey() != key || len(list.Msg.GetLimits()) != 1 || list.Msg.GetLimits()[0].GetHoldersCount() != 1 {
			t.Errorf("%s: got %v, listed %v", key, got.Msg.GetLimit(), list.Msg.GetLimits())
		}
	}
}

// TestConcurrencyLimits_ReadEveryKeyTheListAnswers: each key the list answers
// is read by the same spelling, by get and by key=VALUE, an Orkflow's own key
// among them, at each key's longest. A key no run uses is not found, another
// namespace's Orkflow's included, and one neither OrkStack makes nor an
// author may write is refused.
func TestConcurrencyLimits_ReadEveryKeyTheListAnswers(t *testing.T) {
	longest := strings.Repeat("a", dsl.MaxSlug)
	keys := []string{
		"db:prod",
		strings.Repeat("k", dsl.ConcurrencyKeyMaxBytes),
		engine.DefaultRunConcurrencyKey("default", "migrate"),
		engine.DefaultRunConcurrencyKey("platform", "shared-migrate"), // a shared Orkflow, composed here
		engine.DefaultRunConcurrencyKey(longest, longest),
	}
	reader := &fakeConcReader{}
	for _, key := range keys {
		reader.gates = append(reader.gates, gateView(key, 1, 0))
	}
	svc := limitsService(t, reader)
	list := func(filters ...*orkstackv1.FieldFilter) ([]*orkstackv1.ConcurrencyLimit, error) {
		resp, err := svc.ListConcurrencyLimits(context.Background(), connect.NewRequest(&orkstackv1.ListConcurrencyLimitsRequest{Filters: filters}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetLimits(), nil
	}
	get := func(key string) (*orkstackv1.ConcurrencyLimit, error) {
		resp, err := svc.GetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.GetConcurrencyLimitRequest{Key: key}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetLimit(), nil
	}
	equals := func(key string) *orkstackv1.FieldFilter {
		return &orkstackv1.FieldFilter{Field: "key", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{key}}
	}
	listed, err := list()
	must.OK(t, err)
	if len(listed) != len(keys) {
		t.Fatalf("listed %d limits; want %d", len(listed), len(keys))
	}
	for i, want := range listed {
		if want.GetKey() != keys[i] {
			t.Fatalf("limit %d is %q; want %q", i, want.GetKey(), keys[i])
		}
		got, err := get(want.GetKey())
		if err != nil || !proto.Equal(got, want) {
			t.Errorf("get %.60q = %v, %v; want it as the list answers it", want.GetKey(), got, err)
		}
		one, err := list(equals(want.GetKey()))
		if err != nil || len(one) != 1 || !proto.Equal(one[0], want) {
			t.Errorf("list key=%.60q = %v, %v; want that one limit", want.GetKey(), one, err)
		}
	}
	for _, unused := range []string{
		engine.DefaultRunConcurrencyKey("payments", "migrate"), // another namespace's Orkflow's own key
		engine.DefaultRunConcurrencyKey("default", "never-run"),
		"ork:task:payments/migrate:apply",
		"db:staging",
	} {
		if _, err := get(unused); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("get %q = %v; want not_found", unused, err)
		}
		if one, err := list(equals(unused)); err != nil || len(one) != 0 {
			t.Errorf("list key=%q = %v, %v; want none", unused, one, err)
		}
	}
	for _, refused := range []string{
		"", "ork:mine", "ork:orkflow:", "ork:orkflow:default", "ork:orkflow:default/", "ork:orkflow:/migrate",
		"ork:orkflow:default/migrate/x", "ork:orkflow:Default/migrate", "ork:orkflow:default/migrate ", "ork:task:default/migrate",
		engine.DefaultRunConcurrencyKey(longest+"a", "migrate"), strings.Repeat("k", dsl.ConcurrencyKeyMaxBytes+1), "has space",
	} {
		_, err := get(refused)
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), dsl.ConcurrencyKeyInvalidCode) {
			t.Errorf("get %.60q = %v; want invalid_argument %s", refused, err, dsl.ConcurrencyKeyInvalidCode)
		}
	}
}

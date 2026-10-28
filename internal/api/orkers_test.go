package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// stubFleet states the facts an OrkerFleet would read from the cluster and
// from each Orker, and records what was asked.
type stubFleet struct {
	mu        sync.Mutex
	presence  []engine.OrkerPresence
	observeEr error
	described map[string]*orkstackv1.DescribeOrkerResult
	failures  map[string]error

	observeOrkers int
	observeOrker  []string
	describes     []string
	cordons       []*orkstackv1.CordonOrkerInput
}

func (f *stubFleet) ObserveOrkers(context.Context) ([]engine.OrkerPresence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOrkers++
	return f.presence, f.observeEr
}

func (f *stubFleet) ObserveOrker(_ context.Context, id string) (engine.OrkerPresence, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOrker = append(f.observeOrker, id)
	for _, p := range f.presence {
		if p.Identity == id {
			return p, true, nil
		}
	}
	return engine.OrkerPresence{}, false, nil
}

func (f *stubFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describes = append(f.describes, id)
	if err := f.failures[id]; err != nil {
		return nil, err
	}
	return proto.Clone(f.described[id]).(*orkstackv1.DescribeOrkerResult), nil
}

func (f *stubFleet) CordonOrker(_ context.Context, id string, in *orkstackv1.CordonOrkerInput) (*orkstackv1.DescribeOrkerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cordons = append(f.cordons, in)
	out := proto.Clone(f.described[id]).(*orkstackv1.DescribeOrkerResult)
	out.Cordoned = in.GetCordoned()
	out.CordonReason = ""
	out.CordonedAt = nil
	if in.GetCordoned() {
		out.CordonReason = in.GetReason()
		out.CordonedAt = timestamppb.New(time.Date(2026, 9, 22, 8, 52, 10, 0, time.UTC))
	}
	return out, nil
}

var fleetSeen = time.Date(2026, 9, 22, 9, 14, 2, 0, time.UTC)

func threeOrkerFleet() *stubFleet {
	return &stubFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-c001", LastSeen: fleetSeen},
			{Identity: "orker-91c0", LastSeen: fleetSeen},
			{Identity: "orker-7f3a", LastSeen: fleetSeen},
			{Identity: "orker-g1", LastSeen: fleetSeen},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-91c0": {OrkerId: "orker-91c0", Pool: "build", MaxConcurrentPins: 2},
			"orker-7f3a": {OrkerId: "orker-7f3a", Pool: "build", RunningTasks: 2, OpenPins: 1, MaxConcurrentPins: 2,
				Pins: []*orkstackv1.OpenPin{{RunId: "ork-1", Pin: "build"}}},
			"orker-g1": {OrkerId: "orker-g1", Pool: "gpu"},
		},
		failures: map[string]error{"orker-c001": errors.New("describe orker: context deadline exceeded")},
	}
}

func listOrkers(t *testing.T, svc *api.Service, req *orkstackv1.ListOrkersRequest) []*orkstackv1.Orker {
	t.Helper()
	resp, err := svc.ListOrkers(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("ListOrkers: %v", err)
	}
	return resp.Msg.GetOrkers()
}

func orkerIDs(rows []*orkstackv1.Orker) string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.GetOrkerId())
	}
	return strings.Join(ids, ",")
}

func TestListOrkers_OneRowPerPresenceIdentity_UnansweredIsUnavailableNotZero(t *testing.T) {
	fleet := threeOrkerFleet()
	svc := api.NewService(api.Config{Fleet: fleet})
	rows := listOrkers(t, svc, &orkstackv1.ListOrkersRequest{})
	// Pool, then id; the unreachable row has no pool and sorts first.
	if got := orkerIDs(rows); got != "orker-c001,orker-7f3a,orker-91c0,orker-g1" {
		t.Fatalf("rows = %s", got)
	}
	unreachable := rows[0]
	if unreachable.GetUnavailableReason() == "" || unreachable.GetPool() != "" || unreachable.GetRunningTasks() != 0 ||
		unreachable.GetLastSeen() == nil {
		t.Fatalf("the unanswered row = %v; want unavailable_reason, last_seen, and nothing measured", unreachable)
	}
	busy := rows[1]
	if busy.GetRunningTasks() != 2 || busy.GetOpenPins() != 1 || busy.GetMaxConcurrentPins() != 2 ||
		len(busy.GetPins()) != 1 || !busy.GetLastSeen().AsTime().Equal(fleetSeen) {
		t.Fatalf("the busy row = %v", busy)
	}
	if fleet.observeOrkers != 1 || len(fleet.describes) != 4 {
		t.Fatalf("observed %d times, described %v", fleet.observeOrkers, fleet.describes)
	}
}

func orkersWhere(t *testing.T, texts ...string) *orkstackv1.ListOrkersRequest {
	return &orkstackv1.ListOrkersRequest{Filters: filters(t, texts...)}
}

func TestListOrkers_PoolFilterKeepsUnreachableRows(t *testing.T) {
	svc := api.NewService(api.Config{Fleet: threeOrkerFleet()})
	if got := orkerIDs(listOrkers(t, svc, orkersWhere(t, "pool=gpu"))); got != "orker-c001,orker-g1" {
		t.Fatalf("pool gpu rows = %s; want the gpu Orker and the one that could not be asked", got)
	}
	if got := orkerIDs(listOrkers(t, svc, orkersWhere(t, "pool=build"))); got != "orker-c001,orker-7f3a,orker-91c0" {
		t.Fatalf("pool build rows = %s", got)
	}
	if got := orkerIDs(listOrkers(t, svc, orkersWhere(t, "pool!=build"))); got != "orker-c001,orker-g1" {
		t.Fatalf("pool != build rows = %s", got)
	}
	// orker_id is always measured: the unreachable row filters normally.
	if got := orkerIDs(listOrkers(t, svc, orkersWhere(t, "orker_id^orker-9", "pool=build"))); got != "orker-91c0" {
		t.Fatalf("orker_id^orker-9 rows = %s", got)
	}
}

func TestListOrkers_OrkerIDEqualityAsksOnlyThatOrker(t *testing.T) {
	fleet := threeOrkerFleet()
	svc := api.NewService(api.Config{Fleet: fleet})
	rows := listOrkers(t, svc, orkersWhere(t, "orker_id=orker-7f3a"))
	if got := orkerIDs(rows); got != "orker-7f3a" {
		t.Fatalf("rows = %s; want the one Orker", got)
	}
	if fleet.observeOrkers != 0 || len(fleet.observeOrker) != 1 || len(fleet.describes) != 1 {
		t.Fatalf("ObserveOrkers=%d ObserveOrker=%v describes=%v; want 0, 1, 1",
			fleet.observeOrkers, fleet.observeOrker, fleet.describes)
	}
	if rows := listOrkers(t, svc, orkersWhere(t, "orker_id=orker-gone")); len(rows) != 0 {
		t.Fatalf("an id nothing polls answered %v; want an empty list", rows)
	}
	// Both filters apply: the Orker serves build, not gpu.
	if rows := listOrkers(t, svc, orkersWhere(t, "orker_id=orker-7f3a", "pool=gpu")); len(rows) != 0 {
		t.Fatalf("orker_id=orker-7f3a pool=gpu = %s; want no row, every filter holds", orkerIDs(rows))
	}
	// An in of several ids is no pushdown: the inventory is read.
	before := fleet.observeOrkers
	if got := orkerIDs(listOrkers(t, svc, orkersWhere(t, "orker_id=orker-7f3a,orker-g1"))); got != "orker-7f3a,orker-g1" {
		t.Fatalf("orker_id in = %s", got)
	}
	if fleet.observeOrkers != before+1 {
		t.Fatalf("an in of two ids did not read the inventory")
	}
}

func TestListOrkers_OrkerIDEqualityAnswersAsTheInventoryWould(t *testing.T) {
	for _, id := range []string{"orker-c001", "orker-7f3a", "orker-91c0", "orker-g1", "orker-gone"} {
		for _, extra := range [][]string{nil, {"pool=build"}, {"cordoned=false"}} {
			pushed := api.NewService(api.Config{Fleet: threeOrkerFleet()})
			texts := append([]string{"orker_id=" + id}, extra...)
			got := listOrkers(t, pushed, orkersWhere(t, texts...))
			// The same filter answered from the inventory: an orker_id
			// fragment is never pushed down, and = is the whole value.
			whole := api.NewService(api.Config{Fleet: threeOrkerFleet()})
			var want []*orkstackv1.Orker
			for _, row := range listOrkers(t, whole, orkersWhere(t, extra...)) {
				if row.GetOrkerId() == id {
					want = append(want, row)
				}
			}
			if len(got) != len(want) {
				t.Fatalf("%q: pushdown %s, inventory %s", texts, orkerIDs(got), orkerIDs(want))
			}
			for i := range got {
				if !proto.Equal(got[i], want[i]) {
					t.Errorf("%q: pushdown row %v, inventory row %v", texts, got[i], want[i])
				}
			}
		}
	}
}

func TestListOrkers_MalformedOrkerIDIsRefusedBeforeAnyRead(t *testing.T) {
	for _, text := range []string{"orker_id=a b", "orker_id=", "orker_id!=a b", "orker_id=ok,a b"} {
		fleet := threeOrkerFleet()
		svc := api.NewService(api.Config{Fleet: fleet})
		_, err := svc.ListOrkers(context.Background(), connect.NewRequest(orkersWhere(t, text)))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "orker_id: invalid orker id") {
			t.Fatalf("%s = %v; want InvalidArgument naming the orker id rule", text, err)
		}
		if fleet.observeOrkers != 0 || len(fleet.observeOrker) != 0 || len(fleet.describes) != 0 {
			t.Fatalf("%s read the fleet (%d, %v, %v); a refusal reads nothing", text,
				fleet.observeOrkers, fleet.observeOrker, fleet.describes)
		}
	}
}

func TestListOrkers_NoFleetWiredIsUnavailable(t *testing.T) {
	svc := api.NewService(api.Config{})
	for _, call := range []func() error{
		func() error {
			_, err := svc.ListOrkers(context.Background(), connect.NewRequest(&orkstackv1.ListOrkersRequest{}))
			return err
		},
		func() error {
			_, err := svc.CordonOrker(context.Background(), connect.NewRequest(&orkstackv1.CordonOrkerRequest{OrkerId: "x", Cordoned: true, Reason: "r"}))
			return err
		},
	} {
		err := call()
		if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "cannot list or cordon Orkers") {
			t.Fatalf("no fleet wired = %v; want Unavailable", err)
		}
	}
}

func TestListOrkers_ClusterErrorIsUnavailable(t *testing.T) {
	fleet := threeOrkerFleet()
	fleet.observeEr = errors.New("cluster down")
	svc := api.NewService(api.Config{Fleet: fleet})
	_, err := svc.ListOrkers(context.Background(), connect.NewRequest(&orkstackv1.ListOrkersRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "cluster down") {
		t.Fatalf("cluster error = %v; want Unavailable wrapping it", err)
	}
}

func TestListOrkers_IdentityMismatchIsReportedAsACollision(t *testing.T) {
	fleet := threeOrkerFleet()
	fleet.described["orker-91c0"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-other", Pool: "build"}
	svc := api.NewService(api.Config{Fleet: fleet})
	rows := listOrkers(t, svc, orkersWhere(t, "orker_id=orker-91c0"))
	if len(rows) != 1 || !strings.Contains(rows[0].GetUnavailableReason(), "two Orkers share an id") ||
		rows[0].GetPool() != "" {
		t.Fatalf("a mismatched answer = %v; want an unavailable row naming the collision", rows)
	}
}

func TestCordonOrker_RequiresAReason_BeforeAnyDispatch(t *testing.T) {
	for name, req := range map[string]*orkstackv1.CordonOrkerRequest{
		"no reason":       {OrkerId: "orker-7f3a", Cordoned: true},
		"blank reason":    {OrkerId: "orker-7f3a", Cordoned: true, Reason: "   "},
		"invalid id":      {OrkerId: "a b", Cordoned: true, Reason: "cve"},
		"257-byte reason": {OrkerId: "orker-7f3a", Cordoned: true, Reason: strings.Repeat("x", 257)},
		"control char":    {OrkerId: "orker-7f3a", Cordoned: true, Reason: "a\nb"},
	} {
		fleet := threeOrkerFleet()
		svc := api.NewService(api.Config{Fleet: fleet})
		_, err := svc.CordonOrker(context.Background(), connect.NewRequest(req))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: err = %v; want InvalidArgument", name, err)
		}
		if len(fleet.cordons) != 0 || len(fleet.observeOrker) != 0 {
			t.Errorf("%s: dispatched before refusing (cordons=%d observed=%v)", name, len(fleet.cordons), fleet.observeOrker)
		}
	}
}

func TestCordonOrker_UnknownOrkerIsNotFoundWithoutWaiting(t *testing.T) {
	fleet := threeOrkerFleet()
	svc := api.NewService(api.Config{Fleet: fleet})
	_, err := svc.CordonOrker(context.Background(), connect.NewRequest(&orkstackv1.CordonOrkerRequest{
		OrkerId: "orker-typo", Cordoned: true, Reason: "cve"}))
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), "orkctl orker list") {
		t.Fatalf("unknown id = %v; want NotFound pointing at orkctl orker list", err)
	}
	// "Without waiting" is that nothing was sent to an id nothing polls: the
	// only wait a cordon has is for the Orker's answer (the delivery timeout).
	// Counted, not timed.
	if len(fleet.cordons) != 0 {
		t.Fatal("a cordon was dispatched to an id nothing polls")
	}
}

func TestCordonOrker_ReturnsThePostStateRow(t *testing.T) {
	fleet := threeOrkerFleet()
	svc := api.NewService(api.Config{Fleet: fleet})
	resp, err := svc.CordonOrker(context.Background(), connect.NewRequest(&orkstackv1.CordonOrkerRequest{
		OrkerId: "orker-7f3a", Cordoned: true, Reason: "CVE-2026-1234 runtime patch"}))
	if err != nil {
		t.Fatal(err)
	}
	row := resp.Msg.GetOrker()
	if !row.GetCordoned() || row.GetCordonReason() != "CVE-2026-1234 runtime patch" || row.GetCordonedAt() == nil ||
		!row.GetLastSeen().AsTime().Equal(fleetSeen) || row.GetRunningTasks() != 2 || row.GetPool() != "build" {
		t.Fatalf("post-state row = %v", row)
	}
	if len(fleet.cordons) != 1 || fleet.cordons[0].GetReason() != "CVE-2026-1234 runtime patch" {
		t.Fatalf("delivered %v", fleet.cordons)
	}
	// Uncordon ignores the reason and needs none.
	resp, err = svc.CordonOrker(context.Background(), connect.NewRequest(&orkstackv1.CordonOrkerRequest{OrkerId: "orker-7f3a"}))
	if err != nil || resp.Msg.GetOrker().GetCordoned() {
		t.Fatalf("uncordon = %v, %v", resp, err)
	}
}

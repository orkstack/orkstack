package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestListRuns_ConcurrencyFiltersNeedTheIndexAndAskByGate: queued=,
// queued_on= and holding= are answered from the cluster's index of open runs,
// so a deployment that does not index concurrency limits refuses them before
// it reads anything; one that does hands the reader a key's gate as submit
// computes it for the namespace's incarnation.
func TestListRuns_ConcurrencyFiltersNeedTheIndexAndAskByGate(t *testing.T) {
	created := time.Date(2026, 3, 4, 5, 6, 7, 891011121, time.UTC)
	for _, filter := range []string{"queued=true", "queued_on=db", "holding=db"} {
		t.Run(filter, func(t *testing.T) {
			list := func(indexed bool) (*fakeReader, error) {
				registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments", CreatedAt: timestamppb.New(created)})
				runs := &fakeReader{runs: map[string]*orkstackv1.Run{}}
				svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: registry, Reader: runs,
					Starter: &concStarter{fakeStarter: &fakeStarter{}, indexed: indexed}, Capabilities: &fakeCapabilities{}})
				_, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
					Namespace: "payments", Filters: filters(t, filter)}))
				return runs, err
			}
			runs, err := list(false)
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "does not index concurrency limits") {
				t.Fatalf("unindexed ListRuns = %v; want FailedPrecondition naming the missing index", err)
			}
			if len(runs.listQueries) != 0 {
				t.Errorf("the refused list still read the cluster: %v", runs.listQueries)
			}
			runs, err = list(true)
			if err != nil {
				t.Fatalf("indexed ListRuns: %v", err)
			}
			if len(runs.listQueries) != 1 || runs.listQueries[0].Index == nil || runs.listQueries[0].Index.GateOf == nil {
				t.Fatalf("the reader was asked %v; want one query with its gates", runs.listQueries)
			}
			want := engine.GateID("payments", created.Format(time.RFC3339Nano), "db")
			if got := runs.listQueries[0].Index.GateOf("db"); got != want {
				t.Errorf("GateOf(db) = %q, want the gate submit plans, %q", got, want)
			}
		})
	}
}

package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestListRuns_ResultsRecordedNeedsItsIndex: results_recorded=true is
// answered from the cluster's index, so a deployment whose boot found
// OrkflowResultsRecorded unusable refuses it by name before it reads anything
// (decision RR-1); one that has it hands the reader the filter, needing no
// concurrency index and no gate.
func TestListRuns_ResultsRecordedNeedsItsIndex(t *testing.T) {
	list := func(indexed bool) (*fakeReader, error) {
		runs := &fakeReader{runs: map[string]*orkstackv1.Run{}}
		svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Reader: runs,
			Starter: &fakeStarter{}, Capabilities: &fakeCapabilities{}, ResultsRecordedIndexed: indexed})
		_, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
			Filters: filters(t, "results_recorded=true")}))
		return runs, err
	}
	runs, err := list(false)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "results_recorded") {
		t.Fatalf("unindexed ListRuns = %v; want FailedPrecondition naming results_recorded", err)
	}
	if len(runs.listQueries) != 0 {
		t.Errorf("the refused list still read the cluster: %v", runs.listQueries)
	}
	runs, err = list(true)
	if err != nil {
		t.Fatalf("indexed ListRuns: %v", err)
	}
	if len(runs.listQueries) != 1 || runs.listQueries[0].Index == nil {
		t.Fatalf("the reader was asked %v; want one indexed query", runs.listQueries)
	}
	if got := runs.listQueries[0].Index.Filters; len(got) != 1 || got[0].Path != "results_recorded" || got[0].Value != "true" {
		t.Errorf("index filters = %v; want results_recorded=true", got)
	}
}

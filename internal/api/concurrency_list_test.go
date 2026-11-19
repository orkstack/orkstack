package api_test

import (
	"context"
	"errors"
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

// TestGetRun_ReadsAQueuedRunsPlaceLiveFromItsGate is decision D21: GetRun
// asks the gate of a run that waits for its slot where it stands now, and a
// gate that does not answer, by an error or by the read's bound, leaves the
// place unknown and says so (NOT_ANSWERING) rather than failing the read.
func TestGetRun_ReadsAQueuedRunsPlaceLiveFromItsGate(t *testing.T) {
	created := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	gate := engine.GateID("payments", created.Format(time.RFC3339Nano), "db")
	queued := func() *orkstackv1.Run {
		return &orkstackv1.Run{Id: "ork-me", Namespace: "payments", Status: orkstackv1.Status_STATUS_PENDING,
			Queued: &orkstackv1.ConcurrencyQueued{Key: "db", Limit: 1, PositionAtArrival: 4, Position: 4, PositionKnown: true,
				HoldersCount: 1, HoldersAsOfArrival: true}}
	}
	get := func(t *testing.T, run *orkstackv1.Run, reader *fakeConcReader) *orkstackv1.Run {
		t.Helper()
		registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments", CreatedAt: timestamppb.New(created)})
		runs := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
		svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: registry, Reader: runs,
			Starter: &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, Capabilities: &fakeCapabilities{},
			ConcurrencyReader: reader})
		got, err := svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.GetId(), Namespace: "payments"}))
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		return got.Msg.GetRun()
	}

	t.Run("answering", func(t *testing.T) {
		reader := &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 1, Queued: 2,
			Line:       []engine.GateLineEntry{{Run: "ork-ahead", RunLevel: true}, {Run: "ork-me", RunLevel: true}},
			HolderRuns: []string{"ork-holder"}}}}
		q := get(t, queued(), reader).GetQueued()
		if q.GetPosition() != 2 || !q.GetPositionKnown() || q.GetHoldersAsOfArrival() ||
			len(q.GetHoldersAhead()) != 1 || q.GetHoldersAhead()[0] != "ork-holder" || q.GetPositionAtArrival() != 4 {
			t.Errorf("queued = %v; want 2nd in line now, behind ork-holder", q)
		}
	})
	for _, tc := range []struct {
		name   string
		reader *fakeConcReader
	}{
		{"an error", &fakeConcReader{lineErr: errors.New("frontend unavailable")}},
		{"no answer within the bound", &fakeConcReader{hang: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := get(t, queued(), tc.reader).GetQueued()
			if q.GetPositionKnown() || q.GetNoteCode() != engine.QueuedNoteNotAnswering ||
				!strings.HasPrefix(q.GetNote(), "the limit is not answering; this run stays queued and asks again by ") {
				t.Errorf("queued = %v; want the place unknown and NOT_ANSWERING", q)
			}
		})
	}
	t.Run("a run that is not queued asks nothing", func(t *testing.T) {
		reader := &fakeConcReader{}
		run := queued()
		run.Queued = nil
		get(t, run, reader)
		ended := queued()
		ended.Status = orkstackv1.Status_STATUS_CANCELED
		if got := get(t, ended, reader).GetQueued(); got.GetNoteCode() != "" || !got.GetPositionKnown() {
			t.Errorf("an ended run's record was rewritten: %v", got)
		}
		if asked := reader.questions(); len(asked) != 0 {
			t.Errorf("the gate was asked %v", asked)
		}
	})
}

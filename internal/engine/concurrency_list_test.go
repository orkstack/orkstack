package engine

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestListRuns_AnswersTheConcurrencyFiltersFromTheIndexOfOpenRuns: queued=,
// queued_on= and holding= keep the open runs the cluster indexes so, a key
// standing for its gate, and never a run that has ended, even one whose
// record still carries the attributes it last wrote.
func TestListRuns_AnswersTheConcurrencyFiltersFromTheIndexOfOpenRuns(t *testing.T) {
	created := statsWindowEnd.Add(-10 * time.Minute)
	open := func(id string) statsCorpusRun {
		return statsCorpusRun{id: id, namespace: "alpha", orkflow: "deploy", createdAt: created, status: orkstackv1.Status_STATUS_RUNNING}
	}
	waiting := open("ork-waiting")
	waiting.queued, waiting.queuedOn = true, []string{"gate-db"}
	holder := open("ork-holder")
	holder.holding = []string{"gate-db"}
	other := open("ork-other")
	other.holding = []string{"gate-cache"}
	ended := statsCorpusRun{id: "ork-ended", namespace: "alpha", orkflow: "deploy", createdAt: created,
		closedAt: created.Add(time.Minute), status: orkstackv1.Status_STATUS_SUCCEEDED,
		queued: true, queuedOn: []string{"gate-db"}, holding: []string{"gate-db"}}
	index := statsIndexOver(t, []statsCorpusRun{waiting, holder, other, ended, open("ork-plain")})
	gates := map[string]string{"db": "gate-db", "cache": "gate-cache"}

	for _, tc := range []struct {
		filters   []string
		want      []string
		predicate string
	}{
		{[]string{"queued=true"}, []string{"ork-waiting"}, task.SearchAttrOrkflowRunQueued + ` = "true"`},
		{[]string{"queued_on=db"}, []string{"ork-waiting"}, task.SearchAttrOrkflowConcurrencyQueued + ` = "gate-db"`},
		{[]string{"holding=db"}, []string{"ork-holder"}, task.SearchAttrOrkflowConcurrencyHeld + ` = "gate-db"`},
		{[]string{"holding=cache"}, []string{"ork-other"}, task.SearchAttrOrkflowConcurrencyHeld + ` = "gate-cache"`},
		{[]string{"holding=db", "holding=cache"}, nil, task.SearchAttrOrkflowConcurrencyHeld + ` = "gate-cache"`},
		{[]string{"queued=true", "status=SUCCEEDED"}, nil, ""},
		{[]string{"holding=nobody"}, nil, task.SearchAttrOrkflowConcurrencyHeld + ` = "gate-nobody"`},
	} {
		t.Run(strings.Join(tc.filters, ","), func(t *testing.T) {
			index.queries = nil
			q := RunListPlan(runFilters(t, tc.filters...))
			q.Namespace, q.Complete, q.Limit = "alpha", true, 50
			q.Index.GateOf = func(key string) string {
				if gate, ok := gates[key]; ok {
					return gate
				}
				return "gate-" + key
			}
			reader := newRunReader(index, converter.GetDefaultDataConverter())
			runs, err := reader.ListRuns(context.Background(), q)
			must.OKf(t, err, "ListRuns")
			var got []string
			for _, run := range runs {
				got = append(got, run.GetId())
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("listed %v, want %v", got, tc.want)
			}
			if tc.predicate != "" && !slices.ContainsFunc(index.queries, func(q string) bool { return strings.Contains(q, tc.predicate) }) {
				t.Errorf("no query pushed %q down: %q", tc.predicate, index.queries)
			}
			// Without a status filter the walk asks for live runs only: an
			// ended run's stale attributes are never read.
			if !slices.ContainsFunc(tc.filters, func(f string) bool { return strings.HasPrefix(f, "status") }) &&
				!slices.ContainsFunc(index.queries, func(q string) bool { return livePredicate.MatchString(q) }) {
				t.Errorf("no query asked for live runs only: %q", index.queries)
			}
		})
	}
}

// TestRunListPlan_KeepsNoRunForAnIndexFilterOffTheIndex: the plan's Keep,
// which the archive and the in-memory list apply to rows the index does not
// describe, holds for none under an index filter.
func TestRunListPlan_KeepsNoRunForAnIndexFilterOffTheIndex(t *testing.T) {
	for _, filter := range []string{"queued=true", "queued_on=db", "holding=db"} {
		plan := RunListPlan(runFilters(t, filter))
		if plan.Index == nil || plan.Keep == nil {
			t.Fatalf("%s: plan carries no index query or no Keep", filter)
		}
		archived := &orkstackv1.Run{Id: "ork-archived", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Queued: &orkstackv1.ConcurrencyQueued{Key: "db"}}
		if plan.Keep(RunFilterView(archived)) {
			t.Errorf("%s: Keep holds for an archived run", filter)
		}
	}
	if RunListPlan(runFilters(t, "status=RUNNING")).Index != nil {
		t.Error("a plan without an index filter carries an index query")
	}
}

// TestListRuns_AConcurrencyAttributeItCannotReadLeavesTheClustersMatch: a
// gate list this process cannot decode does not drop the run the cluster's
// own predicate selected.
func TestListRuns_AConcurrencyAttributeItCannotReadLeavesTheClustersMatch(t *testing.T) {
	run := statsCorpusRun{id: "ork-odd", namespace: "alpha", orkflow: "deploy",
		createdAt: statsWindowEnd.Add(-time.Minute), status: orkstackv1.Status_STATUS_RUNNING}
	index := statsIndexOver(t, []statsCorpusRun{run})
	index.records[0].info.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		task.SearchAttrOrkflowConcurrencyHeld: {Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("{not a list")},
	}}
	q := RunListPlan(runFilters(t, "holding=db"))
	q.Namespace, q.Complete, q.Limit = "alpha", true, 50
	q.Index.GateOf = func(key string) string { return "gate-" + key }
	runs, err := newRunReader(index, converter.GetDefaultDataConverter()).ListRuns(context.Background(), q)
	must.OKf(t, err, "ListRuns")
	if len(runs) != 1 || runs[0].GetId() != "ork-odd" {
		t.Errorf("listed %v, want ork-odd, which the cluster matched", runs)
	}
}

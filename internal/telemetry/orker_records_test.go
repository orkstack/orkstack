package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// cachedAttempts runs three cached attempts through obs: a hit, a miss whose
// memo was written, and a miss whose memo write failed.
func cachedAttempts(obs *OrkerObserver, in *orkstackv1.ExecuteTaskInput) {
	for _, cache := range []*orkstackv1.CacheResult{
		{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT},
		{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "no_memo", MemoWritten: true},
		{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, MissReason: "no_memo", MemoWriteError: "store unavailable"},
	} {
		_, a := obs.Begin(context.Background(), in, 1)
		a.End(orker.AttemptOutcome{Cache: cache})
	}
}

// histogramSum is the sum of every point of the float histogram name.
func histogramSum(t *testing.T, reader sdkmetric.Reader, name string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	var sum float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				for _, p := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					sum += p.Sum
				}
			}
		}
	}
	return sum
}

// TestOrkerRecords_AreRunBound: every Orker record that carries a namespace
// goes to the sink the task's run selected, never to the default sink; the
// default sink carries the Orker twins alone, with no namespace or name. An
// attempt whose sink this Orker does not declare still counts its twins.
func TestOrkerRecords_AreRunBound(t *testing.T) {
	def, _, defRead := newObserverProviders(t)
	team, _, teamRead := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(def, map[string]*Providers{"team": team}), nil)
	scheduled := time.Now().Add(-3 * time.Second)
	obs.scheduledAt = func(context.Context) (time.Time, bool) { return scheduled, true }

	in := inputForSink("team", nil)
	cachedAttempts(obs, in)
	_, a := obs.Begin(context.Background(), in, 1)
	a.End(orker.AttemptOutcome{Reason: "CONTAINER_EXIT_NONZERO"})
	_, a = obs.Begin(context.Background(), inputForSink("ghost", nil), 1)
	a.End(orker.AttemptOutcome{})

	defPoints := metricAttrSets(t, defRead)
	for name, sets := range defPoints {
		for _, set := range sets {
			if _, ok := set.Value(AttrNamespace); ok {
				t.Errorf("the default sink carries %s with a namespace: %v", name, set.ToSlice())
			}
			if strings.HasPrefix(name, "ork.deployment.") {
				for _, kv := range set.ToSlice() {
					if !twinLabels[kv.Key] {
						t.Errorf("twin %s carries %s", name, kv.Key)
					}
				}
			}
		}
	}
	for _, name := range []string{DeploymentOrkerTasksMetric, DeploymentOrkerTaskWaitMetric,
		DeploymentOrkerMemoLookupsMetric, DeploymentOrkerMemoWritesMetric} {
		if len(defPoints[name]) == 0 {
			t.Errorf("the default sink has no twin %s", name)
		}
	}
	teamPoints := metricAttrSets(t, teamRead)
	for _, name := range []string{"ork.orker.tasks", "ork.orker.task.duration", OrkerTaskWaitMetric,
		OrkerMemoLookupsMetric, OrkerMemoReusedMetric, OrkerMemoWritesMetric} {
		if len(teamPoints[name]) == 0 {
			t.Errorf("the run's own sink has no %s", name)
		}
		for _, set := range teamPoints[name] {
			if v, _ := set.Value(AttrNamespace); v.AsString() == "" {
				t.Errorf("%s on the run's sink lacks its namespace: %v", name, set.ToSlice())
			}
		}
	}
	for name := range teamPoints {
		if strings.HasPrefix(name, "ork.deployment.") {
			t.Errorf("a named sink carries the twin %s", name)
		}
	}
	var stray int
	for _, set := range defPoints[DeploymentOrkerTasksMetric] {
		if v, _ := set.Value(AttrOutcome); v.AsString() == OutcomeSucceeded {
			stray++
		}
	}
	if stray != 1 {
		t.Errorf("succeeded orker twins = %d; want 1, from the attempt whose sink is undeclared", stray)
	}
}

// TestOrkerObserver_TaskDurationCarriesOutcome: ork.orker.task.duration is
// labelled with the attempt's outcome, so a failed attempt's time is not
// read as a successful one's (F2).
func TestOrkerObserver_TaskDurationCarriesOutcome(t *testing.T) {
	p, _, reader := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(p, nil), nil)
	for _, outcome := range []orker.AttemptOutcome{{}, {Reason: "CONTAINER_EXIT_NONZERO"}, {Canceled: true}} {
		_, a := obs.Begin(context.Background(), observerTestInput(), 1)
		a.End(outcome)
	}
	got := map[string]bool{}
	for _, set := range metricAttrSets(t, reader)["ork.orker.task.duration"] {
		v, _ := set.Value(AttrOutcome)
		got[v.AsString()] = true
	}
	for _, want := range []string{OutcomeSucceeded, OutcomeFailed, OutcomeCanceled} {
		if !got[want] {
			t.Errorf("ork.orker.task.duration has no outcome=%s series (got %v)", want, got)
		}
	}
}

// TestOrkerObserver_MemoCountsFollowTheOutcome: a cached attempt counts one
// lookup by outcome and miss reason, a hit one reuse, and a memo write by
// whether it was written; an attempt that is not cached counts none.
func TestOrkerObserver_MemoCountsFollowTheOutcome(t *testing.T) {
	p, _, reader := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(p, nil), nil)
	in := observerTestInput()
	cachedAttempts(obs, in)
	_, a := obs.Begin(context.Background(), in, 1)
	a.End(orker.AttemptOutcome{})

	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				key := m.Name
				for _, k := range []attribute.Key{AttrCacheOutcome, AttrCacheMissReason, AttrOutcome} {
					if v, ok := dp.Attributes.Value(k); ok {
						key += "/" + v.AsString()
					}
				}
				sums[key] += dp.Value
			}
		}
	}
	for key, want := range map[string]int64{
		OrkerMemoLookupsMetric + "/hit":          1,
		OrkerMemoLookupsMetric + "/miss/no_memo": 2,
		OrkerMemoReusedMetric:                    1,
		OrkerMemoWritesMetric + "/written":       1,
		OrkerMemoWritesMetric + "/failed":        1,
	} {
		if sums[key] != want {
			t.Errorf("%s = %d; want %d (all %v)", key, sums[key], want, sums)
		}
	}
}

// TestOrkerObserver_TaskWaitIsScheduledToBegin: ork.orker.task.wait.duration
// is the time from when the cluster scheduled the attempt to its Begin, and
// nothing outside an activity.
func TestOrkerObserver_TaskWaitIsScheduledToBegin(t *testing.T) {
	p, _, reader := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(p, nil), nil)
	_, a := obs.Begin(context.Background(), observerTestInput(), 1)
	a.End(orker.AttemptOutcome{})
	if n := len(metricAttrSets(t, reader)[OrkerTaskWaitMetric]); n != 0 {
		t.Fatalf("an attempt outside an activity recorded %d wait series", n)
	}
	obs.scheduledAt = func(context.Context) (time.Time, bool) { return time.Now().Add(-2 * time.Second), true }
	_, a = obs.Begin(context.Background(), observerTestInput(), 1)
	a.End(orker.AttemptOutcome{})
	if sum := histogramSum(t, reader, OrkerTaskWaitMetric); sum < 1.9 || sum > 60 {
		t.Fatalf("wait recorded %.2fs; want about 2s", sum)
	}
}

// TestOrkerObserver_APanickingBeginObservesNothing: a panic beginning an
// attempt's observation leaves the attempt unobserved, never panicking into
// the Orker's dispatch.
func TestOrkerObserver_APanickingBeginObservesNothing(t *testing.T) {
	p, _, _ := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(p, nil), nil)
	obs.scheduledAt = func(context.Context) (time.Time, bool) { panic("boom") }
	ctx, a := obs.Begin(context.Background(), observerTestInput(), 1)
	if ctx == nil || a == nil {
		t.Fatal("Begin returned no context or observer after a panic")
	}
	a.Phase("run")
	a.End(orker.AttemptOutcome{})
}

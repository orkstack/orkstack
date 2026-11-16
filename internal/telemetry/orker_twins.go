package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// The Orker's run-bound instruments beyond ork.orker.tasks, and their
// operator twins (twins.go). Every one is recorded from what an attempt's
// Begin and End already carry, so the Orker reads no registry for them
// (docs/DEPLOY.md, "Where a namespace's records go").
const (
	// OrkerTaskWaitMetric is how long an attempt waited between the cluster
	// scheduling it and this Orker beginning it.
	OrkerTaskWaitMetric = "ork.orker.task.wait.duration"
	// OrkerMemoLookupsMetric counts a cached task's memo decisions.
	OrkerMemoLookupsMetric = "ork.orker.memo.lookups"
	// OrkerMemoReusedMetric counts memo hits: results reused with no container.
	OrkerMemoReusedMetric = "ork.orker.memo.reused"
	// OrkerMemoWritesMetric counts memo writes after a miss, written or failed.
	OrkerMemoWritesMetric = "ork.orker.memo.writes"

	DeploymentOrkerTasksMetric       = "ork.deployment.orker.tasks"
	DeploymentOrkerTaskWaitMetric    = "ork.deployment.orker.task.wait.duration"
	DeploymentOrkerMemoLookupsMetric = "ork.deployment.orker.memo.lookups"
	DeploymentOrkerMemoWritesMetric  = "ork.deployment.orker.memo.writes"
)

// orkerRunInstruments are the run-bound instruments beside OrkerInstruments,
// built per sink.
type orkerRunInstruments struct {
	wait        metric.Float64Histogram
	memoLookups metric.Int64Counter
	memoReused  metric.Int64Counter
	memoWrites  metric.Int64Counter
}

func newOrkerRunInstruments(meter metric.Meter, wait, lookups, reused, writes string) (*orkerRunInstruments, error) {
	w, err := meter.Float64Histogram(wait,
		metric.WithDescription("Seconds between the cluster scheduling a task attempt and an Orker beginning it."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	l, err := meter.Int64Counter(lookups,
		metric.WithDescription("A cached task's memo decisions, by outcome and miss reason."),
		metric.WithUnit("{lookup}"))
	if err != nil {
		return nil, err
	}
	var r metric.Int64Counter
	if reused != "" {
		if r, err = meter.Int64Counter(reused,
			metric.WithDescription("Cached tasks whose result was reused from a memo, with no container."),
			metric.WithUnit("{task}")); err != nil {
			return nil, err
		}
	}
	wr, err := meter.Int64Counter(writes,
		metric.WithDescription("Memo writes after a cached task's miss, by outcome: written or failed."),
		metric.WithUnit("{write}"))
	if err != nil {
		return nil, err
	}
	return &orkerRunInstruments{wait: w, memoLookups: l, memoReused: r, memoWrites: wr}, nil
}

// orkerTwins are the Orker's operator twins, on the default sink.
type orkerTwins struct {
	tasks metric.Int64Counter
	*orkerRunInstruments
}

func newOrkerTwins(p *Providers) (*orkerTwins, error) {
	meter := meterFor(p)
	tasks, err := meter.Int64Counter(DeploymentOrkerTasksMetric,
		metric.WithDescription("Completed task attempts in every namespace, by pool, outcome and failure reason."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	run, err := newOrkerRunInstruments(meter, DeploymentOrkerTaskWaitMetric, DeploymentOrkerMemoLookupsMetric, "", DeploymentOrkerMemoWritesMetric)
	if err != nil {
		return nil, err
	}
	return &orkerTwins{tasks: tasks, orkerRunInstruments: run}, nil
}

// activityScheduledAt is when the cluster scheduled the activity ctx runs,
// for its first attempt only: the cluster keeps the first attempt's
// scheduling time across retries, so a retry's "wait" would be its whole
// history, backoff included, and is not measured.
func activityScheduledAt(ctx context.Context) (time.Time, bool) {
	if !activity.IsActivity(ctx) {
		return time.Time{}, false
	}
	info := activity.GetInfo(ctx)
	if info.Attempt != 1 || info.ScheduledTime.IsZero() {
		return time.Time{}, false
	}
	return info.ScheduledTime, true
}

// attemptOutcomeLabel is the outcome ork.orker.tasks labels an attempt with.
func attemptOutcomeLabel(outcome orker.AttemptOutcome) string {
	switch {
	case outcome.Canceled:
		return OutcomeCanceled
	case outcome.CommitAdopted:
		return OutcomeAdopted
	case outcome.Reason != "":
		return OutcomeFailed
	}
	return OutcomeSucceeded
}

// memoWriteOutcome is "written", "failed" or "" (no write was attempted).
func memoWriteOutcome(c *orkstackv1.CacheResult) string {
	switch {
	case c.GetMemoWritten():
		return "written"
	case c.GetMemoWriteError() != "":
		return "failed"
	}
	return ""
}

// recordMemo counts one attempt's memo step on inst, under attrs.
func (r *orkerRunInstruments) recordMemo(ctx context.Context, cache *orkstackv1.CacheResult, attrs []attribute.KeyValue, withReason bool) {
	outcome := CacheOutcomeLabel(cache)
	if r == nil || outcome == "" {
		return
	}
	lookup := append(attrs[:len(attrs):len(attrs)], attribute.String(AttrCacheOutcome, outcome))
	if reason := cache.GetMissReason(); withReason && reason != "" {
		lookup = append(lookup, attribute.String(AttrCacheMissReason, reason))
	}
	r.memoLookups.Add(ctx, 1, metric.WithAttributes(lookup...))
	if outcome == "hit" && r.memoReused != nil {
		r.memoReused.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if w := memoWriteOutcome(cache); w != "" {
		r.memoWrites.Add(ctx, 1, metric.WithAttributes(append(attrs[:len(attrs):len(attrs)], attribute.String(AttrOutcome, w))...))
	}
}

// twinRecorder records one attempt's twins on the default sink. It is built
// in Begin, before the attempt's own sink is known, so an attempt whose sink
// this Orker does not declare still counts them.
type twinRecorder struct {
	twins *orkerTwins
	pool  []attribute.KeyValue
}

func (o *OrkerObserver) twinsFor(in *orkstackv1.ExecuteTaskInput) twinRecorder {
	o.twinsOnce.Do(func() {
		twins, err := newOrkerTwins(o.sinks.Default())
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("telemetry: the Orker's operator twins could not be built; they are not exported", "error", err)
			}
			return
		}
		o.twins = twins
	})
	var pool []attribute.KeyValue
	if p := in.GetPool(); p != "" {
		pool = []attribute.KeyValue{attribute.String(AttrPool, p)}
	}
	return twinRecorder{twins: o.twins, pool: pool}
}

func (t twinRecorder) wait(ctx context.Context, seconds float64) {
	if t.twins != nil {
		t.twins.wait.Record(ctx, seconds, metric.WithAttributes(t.pool...))
	}
}

func (t twinRecorder) end(ctx context.Context, outcome orker.AttemptOutcome) {
	if t.twins == nil {
		return
	}
	t.twins.tasks.Add(ctx, 1, metric.WithAttributes(append(t.pool[:len(t.pool):len(t.pool)],
		attribute.String(AttrOutcome, attemptOutcomeLabel(outcome)),
		attribute.String(AttrFailureReason, outcome.Reason))...))
	t.twins.recordMemo(ctx, outcome.Cache, t.pool, false)
}

// droppedAttempt observes an attempt whose own records are dropped: its sink
// is not declared here. Only its twins are recorded.
type droppedAttempt struct {
	ctx  context.Context
	twin twinRecorder
	once *sync.Once
}

func (droppedAttempt) Phase(string) {}

func (d droppedAttempt) End(outcome orker.AttemptOutcome) {
	d.once.Do(func() { d.twin.end(d.ctx, outcome) })
}

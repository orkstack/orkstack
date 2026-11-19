package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// A census is a periodic read of the cluster or the store that feeds a
// gauge: how many runs are open, how many deliveries are pending. Censuses
// run on every engine replica, each on its own jittered timer, and share one
// budget of requests per minute per replica, so together they cost a bounded
// amount whatever the deployment's size. Over budget a pass is skipped and
// counted; it never retries in line and never touches the submit path.
const (
	CensusRequestsMetric    = "ork.census.requests"
	CensusSkippedMetric     = "ork.census.skipped"
	CensusLastSuccessMetric = "ork.census.last_success"
	CensusFailedMetric      = "ork.census.failed"
	RunsOpenMetric          = "ork.runs.open"

	AttrCensus = "ork.census"

	// DefaultCensusRequestsPerMinute is each replica's census budget.
	DefaultCensusRequestsPerMinute = 60
	// CensusInterval is how often each census runs, and the most its first
	// pass waits after boot.
	CensusInterval = time.Minute
	// censusPassTimeout bounds one pass.
	censusPassTimeout = 30 * time.Second
)

// censusBudget is a token bucket holding at most a minute's requests and
// refilling continuously. It is process-local and shared by every census.
type censusBudget struct {
	perMinute float64
	now       func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newCensusBudget(perMinute int, now func() time.Time) *censusBudget {
	return &censusBudget{perMinute: float64(perMinute), now: now, tokens: float64(perMinute), last: now()}
}

// take spends n requests if the budget holds them, and reports whether it did.
func (b *censusBudget) take(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if float64(n) > b.tokens {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// charge spends n more requests a pass made than it was admitted for. The
// budget may go into debt: later passes are skipped until it is repaid, so
// over any minute the censuses spend no more than the budget plus one pass.
func (b *censusBudget) charge(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.tokens -= float64(n)
}

// capacity is the most the budget holds: a minute's requests.
func (b *censusBudget) capacity() int { return int(b.perMinute) }

func (b *censusBudget) refill() {
	now := b.now()
	b.tokens = min(b.perMinute, b.tokens+now.Sub(b.last).Minutes()*b.perMinute)
	b.last = now
}

// censusMetrics are the census instruments, on the default sink.
type censusMetrics struct {
	requests metric.Int64Counter
	skipped  metric.Int64Counter
	failed   metric.Int64Counter
	logger   *slog.Logger

	mu          sync.Mutex
	lastSuccess map[string]int64
	failing     map[string]bool
	// skips is each census's skipped passes in a row.
	skips map[string]int
}

func newCensusMetrics(p *Providers, logger *slog.Logger) *censusMetrics {
	m := &censusMetrics{logger: logger, lastSuccess: map[string]int64{}, failing: map[string]bool{}, skips: map[string]int{}}
	meter := meterFor(p)
	var err1, err2, err3 error
	m.requests, err1 = meter.Int64Counter(CensusRequestsMetric,
		metric.WithDescription("Requests a census spent from this replica's budget, by census."),
		metric.WithUnit("{request}"))
	m.skipped, err2 = meter.Int64Counter(CensusSkippedMetric,
		metric.WithDescription("Census passes skipped because this replica's budget could not pay for them, by census."),
		metric.WithUnit("{pass}"))
	var err4 error
	m.failed, err4 = meter.Int64Counter(CensusFailedMetric,
		metric.WithDescription("Census passes that ran and could not read their source, by census."),
		metric.WithUnit("{pass}"))
	_, err3 = meter.Int64ObservableGauge(CensusLastSuccessMetric,
		metric.WithDescription("When a census last read its source successfully, unix seconds, by census."),
		metric.WithUnit("s"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			m.mu.Lock()
			defer m.mu.Unlock()
			for name, at := range m.lastSuccess {
				o.Observe(at, metric.WithAttributes(attribute.String(AttrCensus, name)))
			}
			return nil
		}))
	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil && logger != nil {
			logger.Warn("telemetry: a census instrument could not be built; it is not exported", "error", err)
		}
	}
	return m
}

func (m *censusMetrics) spent(ctx context.Context, name string, n int) {
	if m.requests != nil {
		m.requests.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrCensus, name)))
	}
}

// skipWarnAfter is how many passes of one census in a row the budget may
// skip before it is logged at Warn: its gauge has gone that many minutes
// without a reading.
const skipWarnAfter = 5

func (m *censusMetrics) skip(ctx context.Context, name string) {
	if m.skipped != nil {
		m.skipped.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrCensus, name)))
	}
	m.mu.Lock()
	m.skips[name]++
	streak := m.skips[name]
	m.mu.Unlock()
	if streak == skipWarnAfter && m.logger != nil {
		m.logger.Warn("census skipped by its budget pass after pass; its gauge is stale: raise telemetry.census_requests_per_minute",
			"census", name, "skipped_in_a_row", streak)
	}
}

// register starts a census's last_success series at 0, so a census that has
// never read its source is visible as never, not as absent.
func (m *censusMetrics) register(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.lastSuccess[name]; !ok {
		m.lastSuccess[name] = 0
	}
}

func (m *censusMetrics) succeeded(name string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSuccess[name] = at.Unix()
	if m.failing[name] && m.logger != nil {
		m.logger.Info("census reads its source again", "census", name)
	}
	m.failing[name] = false
}

// fail counts a failed pass and logs the first of a run of failures at Warn.
func (m *censusMetrics) fail(ctx context.Context, name string, err error) {
	if m.failed != nil {
		m.failed.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrCensus, name)))
	}
	m.mu.Lock()
	first := !m.failing[name]
	m.failing[name] = true
	m.mu.Unlock()
	if first && m.logger != nil {
		m.logger.Warn("census cannot read its source; its gauge is not reported until it can", "census", name, "error", err)
	}
}

// census is one periodic read: its name, the requests one pass is admitted
// for, the pass itself and, when it can tell, how many requests the pass
// really made (spent, a running total).
type census struct {
	name     string
	requests int
	// budget pays for each pass; nil runs every pass and only records what it
	// spent (the pool backlog, whose cost the operator's settings bound).
	budget *censusBudget
	// interval is how often it runs; zero is CensusInterval.
	interval time.Duration
	metrics  *censusMetrics
	now      func() time.Time
	pass     func(context.Context) error
	spent    func() int64
	// cost, when set, is what the next pass is admitted for, in place of
	// requests: a pass whose price depends on what it read last time.
	cost func() int
}

// once runs one pass if the budget pays for it.
func (c census) once(ctx context.Context) {
	admitted := c.requests
	if c.cost != nil {
		admitted = c.cost()
	}
	// A pass dearer than a whole minute's budget, whose real cost is charged
	// afterwards (spent), is admitted for the whole budget once the budget is
	// full and charged the rest as debt: it runs as often as the budget pays
	// for, never not at all.
	if c.spent != nil && c.budget != nil {
		admitted = min(admitted, c.budget.capacity())
	}
	if c.budget != nil && !c.budget.take(admitted) {
		c.metrics.skip(ctx, c.name)
		return
	}
	var before int64
	if c.spent != nil {
		before = c.spent()
	}
	c.metrics.mu.Lock()
	c.metrics.skips[c.name] = 0
	c.metrics.mu.Unlock()
	passCtx, cancel := context.WithTimeout(ctx, censusPassTimeout)
	defer cancel()
	err := c.runPass(passCtx)
	made := admitted
	if c.spent != nil {
		made = int(c.spent() - before)
		if c.budget != nil {
			c.budget.charge(made - admitted)
		}
	}
	c.metrics.spent(ctx, c.name, made)
	if err != nil {
		if ctx.Err() == nil {
			c.metrics.fail(ctx, c.name, err)
		}
		return
	}
	c.metrics.succeeded(c.name, c.now())
}

// runPass is one pass, with a panic turned into a failed pass: a census runs
// on its own goroutine, and one that panics must not end the engine.
func (c census) runPass(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("census %s panicked: %v", c.name, r)
		}
	}()
	return c.pass(ctx)
}

// run runs a pass after a random delay within the interval, so replicas do
// not ask together, then once per interval until ctx ends.
func (c census) run(ctx context.Context) {
	interval := c.interval
	if interval <= 0 {
		interval = CensusInterval
	}
	jitter := time.NewTimer(rand.N(interval))
	select {
	case <-ctx.Done():
		jitter.Stop()
		return
	case <-jitter.C:
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		c.once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// newRunsOpenGauge registers ork.runs.open and returns the pass that feeds
// it: count is the deployment's open runs, a chain caught continuing as new
// counted once and never as zero. The gauge reports the last good count, and
// nothing after a pass that failed.
func newRunsOpenGauge(p *Providers, logger *slog.Logger, count func(context.Context) (int64, error)) func(context.Context) error {
	var mu sync.Mutex
	var open int64
	known := false
	_, err := meterFor(p).Int64ObservableGauge(RunsOpenMetric,
		metric.WithDescription("Runs open in the deployment, from this replica's last good count."),
		metric.WithUnit("{run}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			mu.Lock()
			defer mu.Unlock()
			if known {
				o.Observe(open)
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: ork.runs.open could not be built; it is not exported", "error", err)
	}
	return func(ctx context.Context) error {
		n, err := count(ctx)
		mu.Lock()
		defer mu.Unlock()
		open, known = n, err == nil
		return err
	}
}

// CensusSources are what the engine's censuses read. A nil source has no
// census.
type CensusSources struct {
	// OpenRuns counts the deployment's open runs (engine RunningRuns), and
	// OpenRunsRequests, when set, is the running total of requests it made
	// (CountingTemporalClient), which the budget charges.
	OpenRuns         func(context.Context) (int64, error)
	OpenRunsRequests func() int64
	// PausedRuns reads the deployment's paused runs by namespace (engine
	// PausedRuns), and PausedRunsRequests the requests it made, charged as
	// OpenRunsRequests is.
	PausedRuns         func(context.Context) (map[string]PausedSummary, error)
	PausedRunsRequests func() int64
	// Policy is the engine's policy cache, refreshed as the telemetry_policy
	// census: the listing and one read per namespace, each minute.
	Policy *PolicyCache
	// WebhookPending counts the webhook deliveries still running that
	// started more than an age ago: two counts a pass.
	WebhookPending func(ctx context.Context, olderThan time.Duration) (int64, error)
	// Schedules is the schedule census: one list and at most half the
	// budget of describes (MaxScheduleDescribes at most) a pass.
	Schedules *ScheduleCensusSource
	// PoolBacklog feeds the pool gauges (ork.pool.backlog.*), as the
	// pool_backlog census: one read per pool each of its own interval. It is
	// outside the budget, because its cost is what the operator configured
	// (engine.pool_metrics.pools and .interval), and a skipped pass would
	// leave an autoscaler without its signal.
	PoolBacklog *PoolBacklogSource
}

// webhookPendingRequests is what one webhook-pending pass is admitted for.
const webhookPendingRequests = 2

// runsOpenRequests is what one ork.runs.open pass is admitted for: its two
// counts. Settling runs that carried on within the hour costs more; that is
// charged after the pass, from what it really made.
const runsOpenRequests = 2

// StartCensuses starts this replica's censuses on p, the default sink, under
// one budget of perMinute requests (DefaultCensusRequestsPerMinute when not
// positive), until ctx ends. The caller starts them only when metrics export.
func StartCensuses(ctx context.Context, p *Providers, perMinute int, sources CensusSources, logger *slog.Logger) {
	if perMinute <= 0 {
		perMinute = DefaultCensusRequestsPerMinute
	}
	budget := newCensusBudget(perMinute, time.Now)
	metrics := newCensusMetrics(p, logger)
	if sources.OpenRuns != nil {
		c := census{name: "runs_open", requests: runsOpenRequests, budget: budget, metrics: metrics, now: time.Now,
			pass: newRunsOpenGauge(p, logger, sources.OpenRuns), spent: sources.OpenRunsRequests}
		metrics.register(c.name)
		go c.run(ctx)
	}
	if sources.PausedRuns != nil {
		c := census{name: "runs_paused", requests: 1, budget: budget, metrics: metrics, now: time.Now,
			pass: newRunsPausedGauges(p, logger, time.Now, sources.PausedRuns), spent: sources.PausedRunsRequests}
		metrics.register(c.name)
		go c.run(ctx)
	}
	if sources.Policy != nil {
		// Half the budget for the policy refresh's documents, so every
		// other census keeps its share however many namespaces there are.
		sources.Policy.SetRefreshBatch(perMinute / 2)
		c := census{name: "telemetry_policy", budget: budget, metrics: metrics, now: time.Now,
			pass: sources.Policy.Refresh, spent: sources.Policy.Requests, cost: sources.Policy.cost}
		metrics.register(c.name)
		go c.run(ctx)
	}
	if sources.WebhookPending != nil {
		c := census{name: "webhook_pending", requests: webhookPendingRequests, budget: budget, metrics: metrics, now: time.Now,
			pass: newWebhookPendingCensus(p, logger, sources.WebhookPending)}
		metrics.register(c.name)
		go c.run(ctx)
	}
	if src := sources.Schedules; src != nil && src.Read != nil {
		sc := newScheduleCensus(p, *src, perMinute, logger)
		// Priced from the last pass's listing; what a pass really made is
		// charged after it.
		c := census{name: "schedules", requests: 1, budget: budget, metrics: metrics, now: time.Now,
			pass: sc.pass, spent: src.Requests, cost: sc.cost}
		metrics.register(c.name)
		go c.run(ctx)
	}
	if src := sources.PoolBacklog; src != nil && len(src.Pools) > 0 && src.Interval > 0 {
		c := census{name: poolBacklogCensus, requests: len(src.Pools), interval: src.Interval, metrics: metrics, now: time.Now,
			pass: newPoolBacklogGauges(p, logger, *src, time.Now)}
		metrics.register(c.name)
		go c.run(ctx)
	}
}

// CountingTemporalClient is c with every visibility request a census makes
// counted (count, list, describe), so the census budget is charged what a
// pass really cost. It changes no request and no answer.
func CountingTemporalClient(c client.Client) (client.Client, func() int64) {
	counted := &countingTemporalClient{Client: c}
	return counted, counted.n.Load
}

type countingTemporalClient struct {
	client.Client
	n atomic.Int64
}

func (c *countingTemporalClient) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	c.n.Add(1)
	return c.Client.CountWorkflow(ctx, request)
}

func (c *countingTemporalClient) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	c.n.Add(1)
	return c.Client.ListWorkflow(ctx, request)
}

// DescribeWorkflowExecution is still what the engine's readers call, through
// interfaces of their own, so it is counted as well as DescribeWorkflow.
func (c *countingTemporalClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.n.Add(1)
	//lint:ignore SA1019 the readers' own interfaces still name this method; counting it is the point of this wrapper
	return c.Client.DescribeWorkflowExecution(ctx, workflowID, runID)
}

func (c *countingTemporalClient) DescribeWorkflow(ctx context.Context, workflowID, runID string) (*client.WorkflowExecutionDescription, error) {
	c.n.Add(1)
	return c.Client.DescribeWorkflow(ctx, workflowID, runID)
}

// RunsPausedMetric and RunsPausedSoonestMetric are the paused-run census's
// gauges: how many live runs of each namespace are paused, and how many
// seconds remain until the soonest of their own pauses ends (absent for a
// namespace whose paused runs are all held by a parent's pause).
const (
	RunsPausedMetric        = "ork.runs.paused"
	RunsPausedSoonestMetric = "ork.runs.paused.soonest_expiry"
)

// PausedSummary is one namespace's paused runs, as the census reads them.
type PausedSummary struct {
	Count   int64
	Soonest time.Time
}

// newRunsPausedGauges registers the paused-run gauges and returns the pass
// that feeds them. Each reports the last good pass and nothing after a pass
// that failed; a namespace with no paused run reads 0 once it has had one.
func newRunsPausedGauges(p *Providers, logger *slog.Logger, now func() time.Time,
	read func(context.Context) (map[string]PausedSummary, error)) func(context.Context) error {
	var mu sync.Mutex
	var last map[string]PausedSummary
	seen := map[string]bool{}
	known := false
	meter := meterFor(p)
	_, err := meter.Int64ObservableGauge(RunsPausedMetric,
		metric.WithDescription("Live runs paused, by namespace, from this replica's last good count."),
		metric.WithUnit("{run}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			mu.Lock()
			defer mu.Unlock()
			if !known {
				return nil
			}
			for ns := range seen {
				o.Observe(last[ns].Count, metric.WithAttributes(attribute.String(AttrNamespace, ns)))
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: "+RunsPausedMetric+" could not be built; it is not exported", "error", err)
	}
	_, err = meter.Float64ObservableGauge(RunsPausedSoonestMetric,
		metric.WithDescription("Seconds until the soonest own pause of a namespace's paused runs ends."),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			mu.Lock()
			defer mu.Unlock()
			if !known {
				return nil
			}
			for ns, s := range last {
				if !s.Soonest.IsZero() {
					o.Observe(s.Soonest.Sub(now()).Seconds(), metric.WithAttributes(attribute.String(AttrNamespace, ns)))
				}
			}
			return nil
		}))
	if err != nil && logger != nil {
		logger.Warn("telemetry: "+RunsPausedSoonestMetric+" could not be built; it is not exported", "error", err)
	}
	return func(ctx context.Context) error {
		got, err := read(ctx)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			known = false
			return err
		}
		last, known = got, true
		for ns := range got {
			seen[ns] = true
		}
		return nil
	}
}

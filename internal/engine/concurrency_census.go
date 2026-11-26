package engine

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
)

// The concurrency census (E88 §9.9.3, §22.6): a Temporal Schedule whose
// short workflow runs one local activity that reads every gate and reports.
// It decides nothing about runs. Its answer is a process-local snapshot in
// the replica that ran it, which the telemetry gauges read.

const (
	// ConcurrencyCensusScheduleID is the census's internal schedule id. It
	// is not of pkg/schedules' shape and carries no schedule-id memo, so
	// OrkStack's schedule listing never shows it.
	ConcurrencyCensusScheduleID   = "ork-conc-census"
	ConcurrencyCensusWorkflowType = "ConcurrencyCensusWorkflow"
	ActivityRunConcurrencyCensus  = "RunConcurrencyCensus"
	// censusDigestNote prefixes the schedule's note, which holds its
	// settings' digest: a replica updates the schedule only when it
	// differs. The note, not the memo, because a schedule's memo cannot be
	// updated (decision G14).
	censusDigestNote = "settings_digest="
)

// CensusSettings are the census's settings (§9.6): the schedule's interval
// and the pass's own bounds, carried as the workflow's input.
type CensusSettings struct {
	Interval time.Duration `json:"census_interval"`
	Timeout  time.Duration `json:"census_timeout"`
	MaxGates int           `json:"census_max_gates"`
	Parallel int           `json:"census_parallel"`
	// ResetIncompleteAfter is how long a reset may run before it is counted
	// incomplete.
	ResetIncompleteAfter time.Duration `json:"reset_incomplete_after"`
}

// DefaultCensusSettings are §9.6's defaults.
func DefaultCensusSettings() CensusSettings {
	return CensusSettings{Interval: time.Minute, Timeout: 5 * time.Minute, MaxGates: 2000, Parallel: 8, ResetIncompleteAfter: 15 * time.Minute}
}

// Check refuses settings the census could not run on, naming the setting:
// an interval under a second (the cluster's own floor for a schedule), a
// timeout or bound below one.
func (s CensusSettings) Check() error {
	switch {
	case s.Interval < time.Second:
		return fmt.Errorf("engine.concurrency.census_interval is %s; it must be at least 1s", s.Interval)
	case s.Timeout <= 0:
		return fmt.Errorf("engine.concurrency.census_timeout is %s; it must be above zero", s.Timeout)
	case s.MaxGates < 1:
		return fmt.Errorf("engine.concurrency.census_max_gates is %d; it must be at least 1", s.MaxGates)
	case s.Parallel < 1:
		return fmt.Errorf("engine.concurrency.census_parallel is %d; it must be at least 1", s.Parallel)
	case s.ResetIncompleteAfter <= 0:
		return fmt.Errorf("engine.concurrency.reset_incomplete_after is %s; it must be above zero", s.ResetIncompleteAfter)
	}
	return nil
}

// Digest is the SHA-256 of the settings' stable JSON (decision G6).
func (s CensusSettings) Digest() (string, error) {
	b, err := stablejson.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("encode the census settings: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ConcurrencyCensusWorkflow is one census fire: one local activity, then
// done. A failed pass fails this execution; the next fire runs a new one.
func ConcurrencyCensusWorkflow(ctx workflow.Context, in CensusSettings) error {
	lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: in.Timeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteLocalActivity(lctx, ActivityRunConcurrencyCensus, in).Get(lctx, nil)
}

// GateStateView is what a gate's state query answers: its state and the
// namespace it holds a key in. The key itself is never reported.
type GateStateView struct {
	Namespace string    `json:"namespace"`
	Key       string    `json:"key,omitempty"`
	State     gateState `json:"state"`
}

// CensusRow is one (namespace, limit, level) aggregate.
type CensusRow struct {
	Namespace  string
	Limit      string // the limit's label (CensusLimitLabel)
	Level      string // "run" or "task"
	SlotsUsed  int
	SlotsLimit int
	QueueDepth int
	// OldestAge and OldestTimeoutFraction describe the oldest queued entry:
	// how long it has waited, and that as a share of its own wait bound.
	OldestAge             time.Duration
	OldestTimeoutFraction float64
}

// CensusKeyRow is one (namespace, limit) aggregate.
type CensusKeyRow struct {
	Namespace      string
	Limit          string
	KeysActive     int
	KeysFull       int
	StalledHolders int
	FenceDegraded  int
}

// CensusSnapshot is one pass's answer.
type CensusSnapshot struct {
	At         time.Time
	Duration   time.Duration
	Rows       []CensusRow
	Keys       []CensusKeyRow
	Rebuilding map[[2]string]int // (namespace, state) → gates
	QueuedRuns map[string]int    // namespace → runs queued
	Partial    map[string]bool   // namespace → a gate of it did not answer
	Gates      int
	Unanswered int
	Truncated  bool
	// Unavailable: the pass could not list the gates, or ran out of time.
	Unavailable bool
	// ResetsIncomplete is how many gates began with a reset that has not
	// finished after reset_incomplete_after.
	ResetsIncomplete int
	// QueuedRunsUnknown: the queued runs could not be counted; QueuedRuns
	// is nil, never a zero.
	QueuedRunsUnknown bool
}

// CensusSink receives each pass's snapshot. The telemetry gauges implement
// it; the engine never reads it back.
type CensusSink interface {
	RecordConcurrencyCensus(CensusSnapshot)
}

// censusLimitLabelBytes is how much of a limit's template a label keeps.
const censusLimitLabelBytes = 64

// CensusLimitLabel is a limit's metric label (§9.9.1): its declared
// template, cut to 64 bytes on a rune boundary, and an 8-hex SHA-256 of the
// whole, so two templates that share a prefix stay apart.
func CensusLimitLabel(template string) string {
	cut := template
	if len(cut) > censusLimitLabelBytes {
		cut = cut[:censusLimitLabelBytes]
		for !utf8.ValidString(cut) {
			cut = cut[:len(cut)-1]
		}
	}
	sum := sha256.Sum256([]byte(template))
	return cut + "#" + hex.EncodeToString(sum[:4])
}

// gateLabel is the label of the template at index ix of g, _other past them.
func gateLabel(g *gateState, ix int) string {
	if ix >= 0 && ix < len(g.Labels) {
		return CensusLimitLabel(g.Labels[ix])
	}
	return "_other"
}

func levelOf(taskIx int) string {
	if taskIx == gateRunLevel {
		return "run"
	}
	return "task"
}

// censusAggregator folds the gates' answers into one snapshot as they
// arrive, so a pass holds one gate's state at a time, never all of them.
// It is pure, so its arithmetic is tested apart from the cluster.
type censusAggregator struct {
	now time.Time
	// resetIncompleteAfter is the setting's; zero counts none.
	resetIncompleteAfter time.Duration

	snap CensusSnapshot
	rows map[censusRowKey]*CensusRow
	keys map[censusKeyKey]*CensusKeyRow
}

type censusRowKey struct{ ns, limit, level string }
type censusKeyKey struct{ ns, limit string }

func newCensusAggregator(now time.Time) *censusAggregator {
	return &censusAggregator{
		now:  now,
		snap: CensusSnapshot{At: now, Rebuilding: map[[2]string]int{}, QueuedRuns: map[string]int{}, Partial: map[string]bool{}},
		rows: map[censusRowKey]*CensusRow{},
		keys: map[censusKeyKey]*CensusKeyRow{},
	}
}

func (c *censusAggregator) row(k censusRowKey) *CensusRow {
	if c.rows[k] == nil {
		c.rows[k] = &CensusRow{Namespace: k.ns, Limit: k.limit, Level: k.level}
	}
	return c.rows[k]
}

// add folds one gate's answer in.
func (c *censusAggregator) add(v GateStateView) {
	if v.Namespace == "" {
		v.Namespace = censusUnknownNamespace
	}
	g, now := &v.State, c.now
	if g.rebuilding() {
		state := "running"
		if g.Rebuild.State == gateRebuildFailing {
			state = "failing"
		}
		c.snap.Rebuilding[[2]string{v.Namespace, state}]++
	}
	if c.resetIncompleteAfter > 0 && g.resetUnfinished() && now.Sub(g.ResetAt) >= c.resetIncompleteAfter {
		c.snap.ResetsIncomplete++
	}
	// A key is labelled by its first entry's template; every entry of one
	// key shares the namespace's declaration.
	var first *gateEntry
	switch {
	case len(g.Holders) > 0:
		first = &g.Holders[0]
	case len(g.Queue) > 0:
		first = &g.Queue[0]
	default:
		return
	}
	label := gateLabel(g, first.LabelIx)
	kk := censusKeyKey{v.Namespace, label}
	kr := c.keys[kk]
	if kr == nil {
		kr = &CensusKeyRow{Namespace: v.Namespace, Limit: label}
		c.keys[kk] = kr
	}
	kr.KeysActive++
	capacity := first.Limit
	if len(g.Holders) > 0 {
		capacity = g.capacity(g.Holders[0].Limit)
	}
	if len(g.Holders) >= capacity {
		kr.KeysFull++
	}
	if now.Before(g.FenceDegradedUntil) {
		kr.FenceDegraded++
	}
	for _, h := range g.Holders {
		if h.Stalled {
			kr.StalledHolders++
		}
		c.row(censusRowKey{v.Namespace, label, levelOf(h.TaskIx)}).SlotsUsed++
	}
	c.row(censusRowKey{v.Namespace, label, levelOf(first.TaskIx)}).SlotsLimit += capacity
	for _, q := range g.Queue {
		r := c.row(censusRowKey{v.Namespace, label, levelOf(q.TaskIx)})
		r.QueueDepth++
		if age := now.Sub(q.ArrivedAt); age > r.OldestAge {
			r.OldestAge, r.OldestTimeoutFraction = age, 0
			if bound := q.WaitUntil.Sub(q.ArrivedAt); bound > 0 {
				r.OldestTimeoutFraction = float64(age) / float64(bound)
			}
		}
	}
}

// finish is the snapshot, rows in a fixed order.
func (c *censusAggregator) finish() CensusSnapshot {
	snap := c.snap
	for _, k := range slices.SortedFunc(maps.Keys(c.rows), func(a, b censusRowKey) int {
		return cmp.Or(cmp.Compare(a.ns, b.ns), cmp.Compare(a.limit, b.limit), cmp.Compare(a.level, b.level))
	}) {
		snap.Rows = append(snap.Rows, *c.rows[k])
	}
	for _, k := range slices.SortedFunc(maps.Keys(c.keys), func(a, b censusKeyKey) int {
		return cmp.Or(cmp.Compare(a.ns, b.ns), cmp.Compare(a.limit, b.limit))
	}) {
		snap.Keys = append(snap.Keys, *c.keys[k])
	}
	return snap
}

// aggregateCensus folds views into one snapshot.
func aggregateCensus(views []GateStateView, now time.Time) CensusSnapshot {
	c := newCensusAggregator(now)
	for _, v := range views {
		c.snap.Gates++
		c.add(v)
	}
	return c.finish()
}

// CensusActivities runs the census pass.
type CensusActivities struct {
	client client.Client
	sink   CensusSink
	now    func() time.Time
}

// NewCensusActivities returns the census pass over c, reporting to sink
// (nil reports nowhere).
func NewCensusActivities(c client.Client, sink CensusSink) *CensusActivities {
	return &CensusActivities{client: c, sink: sink, now: time.Now}
}

// RegisterCensusActivities registers the pass on the engine worker.
func RegisterCensusActivities(w ActivityRegistrar, a *CensusActivities) {
	w.RegisterActivityWithOptions(a.RunConcurrencyCensus, activity.RegisterOptions{Name: ActivityRunConcurrencyCensus})
}

// censusPageSize is how many gates one list page asks for.
const censusPageSize = 200

// RunConcurrencyCensus pages the running gates, queries each one's state,
// at most Parallel at once, each within censusQueryTimeout, and at most
// MaxGates in all, folding each answer in as it arrives; counts the queued
// runs per namespace; and hands the snapshot to the sink. A gate that does
// not answer is counted and marks its namespace partial, one that closed
// in between is not; a queued-runs count that fails is reported unknown,
// never zero. A listing that fails, or a pass that runs out of time,
// reports the census unavailable and fails the pass.
func (a *CensusActivities) RunConcurrencyCensus(ctx context.Context, in CensusSettings) error {
	logger := activity.GetLogger(ctx)
	started := a.now()
	ids, namespaces, truncated, err := a.listGates(ctx, in.MaxGates)
	if err != nil {
		a.record(CensusSnapshot{At: a.now(), Unavailable: true, Duration: a.now().Sub(started)})
		return err
	}
	agg := newCensusAggregator(a.now())
	agg.resetIncompleteAfter = in.ResetIncompleteAfter
	type answer struct {
		i    int
		view GateStateView
		err  error
	}
	answers := make(chan answer)
	sem := make(chan struct{}, max(1, in.Parallel))
	go func() {
		var wg sync.WaitGroup
		for i, id := range ids {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				answers <- answer{i: i, err: ctx.Err()}
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				qctx, cancel := context.WithTimeout(ctx, censusQueryTimeout)
				defer cancel()
				var v GateStateView
				value, err := a.client.QueryWorkflow(qctx, id, "", GateQueryState)
				if err == nil {
					err = value.Get(&v)
				}
				answers <- answer{i: i, view: v, err: err}
			}()
		}
		wg.Wait()
		close(answers)
	}()
	unanswered, closed, cut := 0, 0, 0
	var firstErr error
	for ans := range answers {
		var missing *serviceerror.NotFound
		switch {
		case ans.err == nil:
			agg.add(ans.view)
		case errors.As(ans.err, &missing):
			closed++ // it closed between the listing and the query
		case ctx.Err() != nil:
			cut++ // the pass ran out of time before this gate answered
		default:
			unanswered++
			agg.snap.Partial[namespaces[ans.i]] = true
			if firstErr == nil {
				firstErr = ans.err
			}
		}
	}
	if cut > 0 {
		// Some gates were never read: the snapshot would be a subset
		// presented as a whole.
		a.record(CensusSnapshot{At: a.now(), Unavailable: true, Duration: a.now().Sub(started)})
		return fmt.Errorf("the census pass ran out of time with %d gates unread: %w", cut, ctx.Err())
	}
	if unanswered > 0 {
		logger.Warn("concurrency gates did not answer the census", "unanswered", unanswered, "first_error", firstErr)
	}
	if closed > 0 {
		logger.Info("concurrency gates closed between the census's listing and its queries", "closed", closed)
	}
	snap := agg.finish()
	snap.Gates, snap.Unanswered, snap.Truncated = len(ids)-closed, unanswered, truncated
	queued, err := a.queuedRuns(ctx)
	if err != nil {
		logger.Warn("the census could not count queued runs; it reports them unknown", "error", err)
		snap.QueuedRuns, snap.QueuedRunsUnknown = nil, true
	} else {
		snap.QueuedRuns = queued
	}
	snap.Duration = a.now().Sub(started)
	a.record(snap)
	return nil
}

// censusQueryTimeout bounds one gate's state query, so a gate that hangs
// costs the pass one slot for this long, not its whole budget.
const censusQueryTimeout = 30 * time.Second

// censusUnknownNamespace is the namespace a gate is counted under when its
// own cannot be read.
const censusUnknownNamespace = "_unknown"

func (a *CensusActivities) record(s CensusSnapshot) {
	if a.sink != nil {
		a.sink.RecordConcurrencyCensus(s)
	}
}

// listGates is the running gates' ids and namespaces, at most maxGates.
func (a *CensusActivities) listGates(ctx context.Context, maxGates int) (ids, namespaces []string, truncated bool, err error) {
	query := fmt.Sprintf(`WorkflowType = %q AND ExecutionStatus = "Running"`, ConcurrencyGateWorkflowType)
	var token []byte
	for {
		resp, err := a.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query: query, PageSize: censusPageSize, NextPageToken: token,
		})
		if err != nil {
			return nil, nil, false, fmt.Errorf("list the concurrency gates: %w", err)
		}
		for _, info := range resp.GetExecutions() {
			if len(ids) >= maxGates {
				return ids, namespaces, true, nil
			}
			namespace := censusUnknownNamespace
			if p := info.GetSearchAttributes().GetIndexedFields()[task.SearchAttrOrkflowNamespace]; p != nil {
				var ns string
				if err := DataConverter().FromPayload(p, &ns); err == nil && ns != "" {
					namespace = ns
				}
			}
			ids = append(ids, info.GetExecution().GetWorkflowId())
			namespaces = append(namespaces, namespace)
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return ids, namespaces, false, nil
		}
	}
}

// queuedRuns counts each namespace's queued runs (§7.1's predicate).
func (a *CensusActivities) queuedRuns(ctx context.Context) (map[string]int, error) {
	resp, err := a.client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Query: fmt.Sprintf(`WorkflowType = %q AND %s = "true" AND ExecutionStatus = "Running" GROUP BY %s`,
			RunWorkflowType, task.SearchAttrOrkflowRunQueued, task.SearchAttrOrkflowNamespace),
	})
	if err != nil {
		return nil, fmt.Errorf("count the queued runs: %w", err)
	}
	out := map[string]int{}
	for _, g := range resp.GetGroups() {
		ns := censusUnknownNamespace
		if vals := g.GetGroupValues(); len(vals) == 1 {
			var v string
			if err := DataConverter().FromPayload(vals[0], &v); err == nil && v != "" {
				ns = v
			}
		}
		out[ns] += int(g.GetCount())
	}
	return out, nil
}

// censusScheduler is the slice of the schedule client EnsureCensusSchedule
// uses, so its rules are tested without a cluster.
type censusScheduler interface {
	describe(ctx context.Context) (digest string, found bool, err error)
	create(ctx context.Context, s CensusSettings, digest string) error
	update(ctx context.Context, s CensusSettings, digest string) error
}

// CensusEnsureOutcome is what a boot did to the census schedule.
type CensusEnsureOutcome string

const (
	CensusCreated    CensusEnsureOutcome = "created"
	CensusUnchanged  CensusEnsureOutcome = "unchanged"
	CensusUpdated    CensusEnsureOutcome = "updated"
	CensusUpdateLost CensusEnsureOutcome = "update_lost"
)

// EnsureCensusSchedule makes the census schedule exist with these settings
// (§22.6): created if absent (another replica creating it first is
// success), updated only when its digest differs, and read back after an
// update; still wrong after one retry from a fresh describe, it is left to
// the next boot and reported update_lost. The engine never removes it.
func EnsureCensusSchedule(ctx context.Context, c client.Client, queue string, s CensusSettings) (CensusEnsureOutcome, error) {
	return ensureCensusSchedule(ctx, &temporalCensusScheduler{client: c, queue: queue}, s)
}

func ensureCensusSchedule(ctx context.Context, sch censusScheduler, s CensusSettings) (CensusEnsureOutcome, error) {
	want, err := s.Digest()
	if err != nil {
		return "", err
	}
	got, found, err := sch.describe(ctx)
	if err != nil {
		return "", fmt.Errorf("describe the census schedule: %w", err)
	}
	if !found {
		if err := sch.create(ctx, s, want); err != nil {
			if !errors.Is(err, errCensusExists) {
				return "", fmt.Errorf("create the census schedule: %w", err)
			}
			// Another replica created it first: check what it holds.
			if got, found, err = sch.describe(ctx); err != nil || !found {
				return "", fmt.Errorf("describe the census schedule another replica created: %w", cmp.Or(err, errors.New("not found")))
			}
		} else {
			return CensusCreated, nil
		}
	}
	if got == want {
		return CensusUnchanged, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := sch.update(ctx, s, want); err != nil {
			return "", fmt.Errorf("update the census schedule: %w", err)
		}
		got, found, err = sch.describe(ctx)
		if err != nil {
			return "", fmt.Errorf("read back the census schedule: %w", err)
		}
		if found && got == want {
			return CensusUpdated, nil
		}
	}
	return CensusUpdateLost, nil
}

// errCensusExists is create's answer when the schedule already exists.
var errCensusExists = errors.New("the census schedule already exists")

type temporalCensusScheduler struct {
	client client.Client
	queue  string
}

func (t *temporalCensusScheduler) describe(ctx context.Context) (string, bool, error) {
	desc, err := t.client.ScheduleClient().GetHandle(ctx, ConcurrencyCensusScheduleID).Describe(ctx)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	digest := ""
	if desc.Schedule.State != nil {
		digest, _ = strings.CutPrefix(desc.Schedule.State.Note, censusDigestNote)
	}
	return digest, true, nil
}

func (t *temporalCensusScheduler) options(s CensusSettings, digest string) client.ScheduleOptions {
	return client.ScheduleOptions{
		ID:      ConcurrencyCensusScheduleID,
		Spec:    client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: s.Interval}}},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		Action: &client.ScheduleWorkflowAction{
			ID: ConcurrencyCensusScheduleID, Workflow: ConcurrencyCensusWorkflowType, Args: []any{s},
			TaskQueue: t.queue, WorkflowExecutionTimeout: s.Timeout + time.Minute,
		},
		Note: censusDigestNote + digest,
	}
}

func (t *temporalCensusScheduler) create(ctx context.Context, s CensusSettings, digest string) error {
	_, err := t.client.ScheduleClient().Create(ctx, t.options(s, digest))
	var exists *serviceerror.AlreadyExists
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) || errors.As(err, &exists) || errors.As(err, &started) {
		return fmt.Errorf("%w: %w", errCensusExists, err)
	}
	return err
}

func (t *temporalCensusScheduler) update(ctx context.Context, s CensusSettings, digest string) error {
	opts := t.options(s, digest)
	return t.client.ScheduleClient().GetHandle(ctx, ConcurrencyCensusScheduleID).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sched := in.Description.Schedule
			sched.Spec = &opts.Spec
			sched.Action = opts.Action
			if sched.Policy == nil {
				sched.Policy = &client.SchedulePolicies{}
			}
			sched.Policy.Overlap = opts.Overlap
			if sched.State == nil {
				sched.State = &client.ScheduleState{}
			}
			sched.State.Note = opts.Note
			return &client.ScheduleUpdate{Schedule: &sched}, nil
		},
	})
}

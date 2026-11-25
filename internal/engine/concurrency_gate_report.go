package engine

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// What a concurrency gate's work is counted as (E88 §9.9.2, P4b). The
// activities count what they themselves see: a run they could not check, a
// rebuild attempt, a gate an acquire started. What the gate takes back only
// the gate decides, and it is a workflow, which records no metric: it keeps
// those counts until the end of a check pass, or until it closes or carries
// its state on (one that is cancelled loses them), and hands them to one
// local activity, never when there are none. A count decides nothing: a
// report that fails is dropped, and a call or a workflow task that runs again
// may count twice.

// Why a gate took an entry back.
const (
	gateReclaimClosed      = "closed"
	gateReclaimNotFound    = "not_found"
	gateReclaimStale       = "stale"
	gateReclaimMissingSlot = "missing_slot"
	gateReclaimExpired     = "expired"
)

// How a gate's rebuild ended: skipped on a clean start, ok, or one attempt
// that failed and is tried again.
const (
	GateRebuiltSkipped       = "skipped"
	GateRebuiltOK            = "ok"
	GateRebuiltFailedAttempt = "failed_attempt"
)

// GateReclaimOutcomes and GateRebuiltOutcomes are every outcome a gate's
// work is counted under.
func GateReclaimOutcomes() []string {
	return []string{gateReclaimClosed, gateReclaimNotFound, gateReclaimStale, gateReclaimMissingSlot, gateReclaimExpired}
}

func GateRebuiltOutcomes() []string {
	return []string{GateRebuiltSkipped, GateRebuiltOK, GateRebuiltFailedAttempt}
}

// gateReclaimKey is a reclaimed entry's limit label and outcome.
type gateReclaimKey struct{ limit, outcome string }

// GateReclaim, GateVerifyErrors and GateRebuilt are a report's counts. A
// limit is its label (CensusLimitLabel), never the rendered key.
type GateReclaim struct {
	Limit   string `json:"limit"`
	Outcome string `json:"outcome"`
	Entries int    `json:"entries"`
}

type GateVerifyErrors struct {
	Limit string `json:"limit"`
	Calls int    `json:"calls"`
}

type GateRebuilt struct {
	Outcome string `json:"outcome"`
	Times   int    `json:"times"`
}

// ConcurrencyGateReport is one report of a namespace's gate.
type ConcurrencyGateReport struct {
	Namespace string `json:"namespace"`
	// Started: an acquire started the gate.
	Started      bool               `json:"started,omitempty"`
	Reclaimed    []GateReclaim      `json:"reclaimed,omitempty"`
	VerifyErrors []GateVerifyErrors `json:"verify_errors,omitempty"`
	Rebuilt      []GateRebuilt      `json:"rebuilt,omitempty"`
}

// GateReportObserver counts what gates report.
type GateReportObserver interface {
	ObserveGateReport(ctx context.Context, r ConcurrencyGateReport)
}

// WithGateReportObserver counts each report on o; nil counts none.
func (a *GateActivities) WithGateReportObserver(o GateReportObserver) *GateActivities {
	a.reports = o
	return a
}

// ReportConcurrencyGate hands what a gate took back to this replica's
// observer.
func (a *GateActivities) ReportConcurrencyGate(ctx context.Context, r ConcurrencyGateReport) error {
	a.observe(ctx, r)
	return nil
}

// sendReport reports what the gate took back since its last report, if
// anything, in one attempt whose failure changes nothing.
func (w *gateRun) sendReport(ctx workflow.Context) {
	report := ConcurrencyGateReport{Namespace: w.in.Namespace}
	for _, k := range slices.SortedFunc(maps.Keys(w.reclaimed), func(a, b gateReclaimKey) int {
		return cmp.Or(cmp.Compare(a.limit, b.limit), cmp.Compare(a.outcome, b.outcome))
	}) {
		report.Reclaimed = append(report.Reclaimed, GateReclaim{Limit: k.limit, Outcome: k.outcome, Entries: w.reclaimed[k]})
	}
	w.reclaimed = nil
	if len(report.Reclaimed) == 0 {
		return
	}
	lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: w.g.Tunables.VerifyTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	if err := workflow.ExecuteLocalActivity(lctx, ActivityReportConcurrencyGate, report).Get(lctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("what a concurrency gate took back was not counted; it decides nothing from the count",
			"gate", w.in.GateID, "error", err)
	}
}

// reclaim adds counts to what the next report says: a sum, so the order the
// map is read in changes nothing.
func (w *gateRun) reclaim(counts map[gateReclaimKey]int) {
	if w.reclaimed == nil && len(counts) > 0 {
		w.reclaimed = make(map[gateReclaimKey]int, len(counts))
	}
	for k, n := range counts {
		w.reclaimed[k] += n
	}
}

// limitsOf is the limit labels of each run's entries, each once, for the
// activity that checks the runs to count a failed check under.
func (g *gateState) limitsOf(runs []string) map[string][]string {
	out := make(map[string][]string, len(runs))
	for _, r := range runs {
		out[r] = nil
	}
	for _, e := range slices.Concat(g.Holders, g.Queue) {
		labels, checked := out[e.Run]
		if label := gateLabel(g, e.LabelIx); checked && !slices.Contains(labels, label) {
			out[e.Run] = append(labels, label)
		}
	}
	return out
}

// observe hands r to the observer, unless what it counts was cut short by a
// worker that is stopping, whose work the next worker does again. A call past
// its own deadline is counted: the observer records under a context of its
// own.
func (a *GateActivities) observe(ctx context.Context, r ConcurrencyGateReport) {
	if a.reports == nil || errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	a.reports.ObserveGateReport(context.WithoutCancel(ctx), r)
}

// observeChecks counts the runs a describe could not check, under each limit
// the run holds or waits for.
func (a *GateActivities) observeChecks(ctx context.Context, in DescribeConcurrencyEntriesInput, findings []gateRunFinding) {
	failed := map[string]int{}
	for _, f := range findings {
		if f.Err == "" {
			continue
		}
		for _, limit := range in.Limits[f.Run] {
			failed[limit]++
		}
	}
	if len(failed) == 0 {
		return
	}
	r := ConcurrencyGateReport{Namespace: in.Namespace}
	for _, limit := range slices.Sorted(maps.Keys(failed)) {
		r.VerifyErrors = append(r.VerifyErrors, GateVerifyErrors{Limit: limit, Calls: failed[limit]})
	}
	a.observe(ctx, r)
}

// observeRebuild counts one rebuild attempt by the page that decides it: a
// page that cannot be read, or one past max_entries, fails the attempt (the
// gate's listAll), and the last page completes it.
func (a *GateActivities) observeRebuild(ctx context.Context, in ListConcurrencyEntriesInput, page ListConcurrencyEntriesResult, err error) {
	entries := in.Seen
	for _, r := range page.Records {
		entries += len(r.Entries)
	}
	outcome := ""
	switch {
	case err != nil || entries > in.MaxEntries:
		outcome = GateRebuiltFailedAttempt
	case len(page.NextToken) == 0:
		outcome = GateRebuiltOK
	}
	if outcome != "" {
		a.observe(ctx, ConcurrencyGateReport{Namespace: in.Namespace, Rebuilt: []GateRebuilt{{Outcome: outcome, Times: 1}}})
	}
}

// observeStart counts a gate an acquire started, after a previous execution
// that ended as previous. Two acquires that find no gate at once both count
// one: the cluster does not say which of them started it.
func (a *GateActivities) observeStart(ctx context.Context, namespace, previous string) {
	r := ConcurrencyGateReport{Namespace: namespace, Started: true}
	if previous == GatePreviousClean {
		r.Rebuilt = []GateRebuilt{{Outcome: GateRebuiltSkipped, Times: 1}}
	}
	a.observe(ctx, r)
}

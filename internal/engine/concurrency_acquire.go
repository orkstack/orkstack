package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/task"
)

// ConcurrencyPlan is a run's concurrency limits as its submit decided them
// (E88 §9.6): every number here decides a command, so it is pinned and never
// read from configuration (AGENTS.md §7.2). Nil is a run with no limit,
// whose every path is today's.
type ConcurrencyPlan struct {
	// Slots are the gates the run's limits name, each with its rendered key.
	Slots []ConcurrencySlotPlan `json:"slots"`
	// Orkflow is the run's Orkflow's name, a metric label only.
	Orkflow string `json:"orkflow,omitempty"`
	// Run is the index in Slots of the run-level limit, nil without one.
	Run *int `json:"run,omitempty"`
	// SeqBase is the run's incarnation at its gates: 1 for a fresh run
	// (decision D2); a resumed run's is its resume's.
	SeqBase int64 `json:"seq_base"`
	// GateQueue is the task queue a gate runs on: the engine's own, where
	// every RunWorkflow runs.
	GateQueue string `json:"gate_queue"`
	// Namespace and Incarnation are the gate's identity's parts (GateID).
	Namespace   string `json:"namespace"`
	Incarnation string `json:"incarnation,omitempty"`

	AcquireTimeout       time.Duration `json:"acquire_timeout"`
	AcquireBackoffMin    time.Duration `json:"acquire_backoff_min"`
	AcquireBackoffMax    time.Duration `json:"acquire_backoff_max"`
	RequeueInterval      time.Duration `json:"requeue_interval"`
	RequeueBackoffFactor int           `json:"requeue_backoff_factor"`
	RequeueIntervalMax   time.Duration `json:"requeue_interval_max"`
	PerSlotBytes         int64         `json:"per_slot_bytes"`
	EventHoldersShown    int           `json:"event_holders_shown"`
	// Gate is the tunables a gate this run starts runs on.
	Gate GateTunables `json:"gate"`
}

// ConcurrencySlotPlan is one limit the run declares, rendered.
type ConcurrencySlotPlan struct {
	GateID              string `json:"gate_id"`
	Key                 string `json:"key"`
	Label               string `json:"label"`
	Limit               int    `json:"limit"`
	Strategy            string `json:"strategy"`
	QueueTimeoutSeconds int64  `json:"queue_timeout_seconds"`
	MaxQueued           int    `json:"max_queued,omitempty"`
}

// The submit refusals of a concurrency limit (§7.1): a deployment that
// cannot hold one (cause=index, cause=fleet, or a composed child's limit),
// and a check that could not be made.
const (
	ConcurrencyUnsupportedCode      = "CONCURRENCY_UNSUPPORTED"
	ConcurrencyCheckUnavailableCode = "CONCURRENCY_CHECK_UNAVAILABLE"
	// ConcurrencyLimitReachedCode refuses a refuse or skip limit's run at a
	// full key, before a run id is minted (§6).
	ConcurrencyLimitReachedCode = "CONCURRENCY_LIMIT_REACHED"
)

// The strategies' names in a slot's plan, as the gate reads them.
const (
	ConcurrencyStrategyQueue  = gateStrategyQueue
	ConcurrencyStrategyRefuse = gateStrategyRefuse
	ConcurrencyStrategySkip   = gateStrategySkip
)

// DefaultRunConcurrencyKey is a run-level limit's key when it declares none
// (§3.1): ork:orkflow:<owner>/<name>, owner the namespace that owns the
// definition. The reserved prefix keeps it from any authored key.
func DefaultRunConcurrencyKey(owner, name string) string {
	return "ork:orkflow:" + owner + "/" + name
}

// runSlot is the plan's run-level slot, nil when it has none.
func (p *ConcurrencyPlan) runSlot() *ConcurrencySlotPlan {
	if p == nil || p.Run == nil || *p.Run < 0 || *p.Run >= len(p.Slots) {
		return nil
	}
	return &p.Slots[*p.Run]
}

// RunConcurrencySlot is the plan's run-level slot, false for a plan without.
func (p RunPlan) RunConcurrencySlot() (ConcurrencySlotPlan, bool) {
	slot := p.Concurrency.runSlot()
	if slot == nil {
		return ConcurrencySlotPlan{}, false
	}
	return *slot, true
}

// ActivityAcquireConcurrency is the run's local activity that asks a gate
// for a slot (§5.2.2). It runs on the engine worker, never on ork-tasks.
const ActivityAcquireConcurrency = "AcquireConcurrency"

// AcquireConcurrencyInput is one acquire: the gate, how to start it if it is
// not running, and the request. UpdateID is acq-<D>-<a> (§22.14.1), so a
// retry of one acquire is one Update.
type AcquireConcurrencyInput struct {
	// Orkflow labels the arrival's metric; nothing decides from it.
	Orkflow    string        `json:"orkflow,omitempty"`
	GateID     string        `json:"gate_id"`
	GateQueue  string        `json:"gate_queue"`
	Start      GateStart     `json:"start"`
	Request    gateAcquire   `json:"request"`
	UpdateID   string        `json:"update_id"`
	BackoffMin time.Duration `json:"backoff_min"`
	BackoffMax time.Duration `json:"backoff_max"`
}

// AcquireConcurrencyResult is the gate's answer, or why there is none: a
// protocol refusal, never retried; or no answer within the acquire's time.
type AcquireConcurrencyResult struct {
	Answer      gateAnswer `json:"answer,omitzero"`
	Unsupported string     `json:"unsupported,omitempty"`
	Unavailable string     `json:"unavailable,omitempty"`
}

// permanent reports an error no retry of the acquire can change: a request
// the cluster refuses as malformed or forbidden, a namespace or attribute it
// does not have, or a non-retryable application error.
func permanent(err error) bool {
	var (
		invalid   *serviceerror.InvalidArgument
		denied    *serviceerror.PermissionDenied
		namespace *serviceerror.NamespaceNotFound
	)
	failure := categoryOf(err)
	return errors.As(err, &invalid) || errors.As(err, &denied) || errors.As(err, &namespace) ||
		(failure.nonRetryable() && failure.appType() != GateErrBusy)
}

// acquireUpdateID is acq-<D>-<a>: D the slot's digest, with its ordinal
// only above 0 (the legacy digest byte for byte at 0), a the attempt.
func acquireUpdateID(run string, seqBase int64, taskIx int, n int64, attempt int) string {
	d := gateDigest(run, strconv.FormatInt(seqBase, 10), strconv.Itoa(taskIx))[:16]
	if n > 0 {
		d = gateDigest(run, strconv.FormatInt(seqBase, 10), strconv.Itoa(taskIx), strconv.FormatInt(n, 10))[:16]
	}
	return fmt.Sprintf("acq-%s-%d", d, attempt)
}

// AcquireConcurrency sends the acquire Update to the gate (§5.1.1, §5.2.2):
// a plain Update first; when no gate runs, Update-with-Start, its previous
// execution read from the cluster. A busy gate (draining or frozen), and any
// failure of the cluster, is asked again with back-off until the activity's
// deadline, which is then an Unavailable result, never an error: the run
// decides from the result. A protocol refusal is Unsupported at once.
func (a *GateActivities) AcquireConcurrency(ctx context.Context, in AcquireConcurrencyInput) (AcquireConcurrencyResult, error) {
	res, err := a.acquireConcurrency(ctx, in)
	if err == nil {
		a.observeArrival(ctx, in, res)
	}
	return res, err
}

// ArrivalObserver counts each answered request for a slot (E88 §9.9.2,
// ork.concurrency.arrivals). limit is the declaration's label
// (CensusLimitLabel), never the rendered key.
type ArrivalObserver interface {
	ObserveArrival(ctx context.Context, namespace, orkflow, limit, level, strategy, outcome string)
}

// arrivalOutcome is the arrivals metric's outcome for a result, "" for one
// it does not count (a supersede is counted where it is applied).
func arrivalOutcome(res AcquireConcurrencyResult) string {
	switch {
	case res.Unsupported != "":
		return "unsupported"
	case res.Unavailable != "":
		return "unavailable"
	}
	switch res.Answer.Answer {
	case gateAnswerGranted:
		return "granted"
	case gateAnswerQueued:
		return "queued"
	case gateAnswerFull, gateAnswerFullBound:
		return "full"
	case gateAnswerUnavailable:
		return "unavailable"
	case gateAnswerFenced:
		return "fenced"
	}
	return ""
}

func (a *GateActivities) observeArrival(ctx context.Context, in AcquireConcurrencyInput, res AcquireConcurrencyResult) {
	outcome := arrivalOutcome(res)
	// A re-assert asks again for the place it already has: no arrival.
	if a.arrivals == nil || outcome == "" || !in.Request.ArrivedAt.IsZero() {
		return
	}
	level := "run"
	if in.Request.TaskIx != gateRunLevel {
		level = "task"
	}
	a.arrivals.ObserveArrival(ctx, in.Start.Namespace, in.Orkflow, CensusLimitLabel(in.Request.Label), level, in.Request.Strategy, outcome)
}

func (a *GateActivities) acquireConcurrency(ctx context.Context, in AcquireConcurrencyInput) (AcquireConcurrencyResult, error) {
	backoff := max(in.BackoffMin, 10*time.Millisecond)
	var last error
	var rebuilding *gateAnswer
	base := in.UpdateID
	for ask := 0; ; ask++ {
		if rebuilding != nil {
			// The cluster answers an Update id it has answered with the same
			// answer: each ask after a rebuilding gate's is an id of its own.
			// The gate keys an entry by run, seq_base and task, so an ask
			// that was granted before its answer was lost is answered again.
			in.UpdateID = fmt.Sprintf("%s-r%d", base, ask)
		}
		ans, err := a.acquireOnce(ctx, in)
		if err == nil && ans.Answer == gateAnswerUnavailable {
			// A refuse or skip arrival the gate cannot answer while it
			// rebuilds its record (§5.1.6): asked again within the acquire
			// timeout, so only the final answer counts (§9.9.2).
			rebuilding, last = &ans, errors.New("the limit is rebuilding its record of running runs")
		} else if err == nil {
			return AcquireConcurrencyResult{Answer: ans}, nil
		}
		if failure := categoryOf(err); failure.appType() == GateErrUnsupported {
			return AcquireConcurrencyResult{Unsupported: failure.appMessage()}, nil
		}
		if permanent(err) {
			// Asking again cannot change it: answered at once, its cause
			// kept for the run's message.
			return AcquireConcurrencyResult{Unavailable: fmt.Sprintf("the limit cannot be asked: %v", err)}, nil
		}
		if err != nil {
			last = err
			if activity.IsActivity(ctx) {
				activity.GetLogger(ctx).Warn("a concurrency acquire was not answered; asking again", "gate", in.GateID, "error", err)
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				// Cancelled, as a worker that is stopping cancels it: an
				// error, so the acquire runs again on the next worker and
				// the run is never failed for it.
				return AcquireConcurrencyResult{}, ctx.Err()
			}
			if rebuilding != nil {
				return AcquireConcurrencyResult{Answer: *rebuilding}, nil
			}
			return AcquireConcurrencyResult{Unavailable: fmt.Sprintf("the limit did not answer within the acquire timeout: %v", last)}, nil
		case <-timer.C:
		}
		backoff = min(2*backoff, max(in.BackoffMax, backoff))
	}
}

func (a *GateActivities) acquireOnce(ctx context.Context, in AcquireConcurrencyInput) (gateAnswer, error) {
	update := client.UpdateWorkflowOptions{
		WorkflowID: in.GateID, UpdateID: in.UpdateID, UpdateName: GateUpdateAcquire,
		Args: []any{in.Request}, WaitForStage: client.WorkflowUpdateStageCompleted,
	}
	var ans gateAnswer
	h, err := a.client.UpdateWorkflow(ctx, update)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		previous, perr := a.previousExecution(ctx, in.GateID)
		if perr != nil {
			return ans, perr
		}
		start := in.Start
		start.Previous, start.Resets = previous.ended, previous.resets.inherited(start.Tunables.ResetWindow, time.Now())
		options := client.StartWorkflowOptions{
			ID: in.GateID, TaskQueue: in.GateQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			TypedSearchAttributes: temporal.NewSearchAttributes(
				task.SearchKeyOrkflowNamespace.ValueSet(start.Namespace),
				task.SearchKeyOrkflowGatePhase.ValueSet(gatePhaseActive)),
		}
		if start.Resets != nil {
			options.Memo = map[string]any{gateResetsMemoKey: *start.Resets}
		}
		op := a.client.NewWithStartWorkflowOperation(options, ConcurrencyGateWorkflowType, start)
		update.WorkflowID = ""
		h, err = a.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{StartWorkflowOperation: op, UpdateOptions: update})
		if err == nil {
			a.observeStart(ctx, start.Namespace, previous.ended)
		}
	}
	if err != nil {
		return ans, err
	}
	return ans, h.Get(ctx, &ans)
}

// gatePrevious is a gate id's latest execution, for the start that follows it.
type gatePrevious struct {
	// ended is the start's Previous (§5.1.1): completed is clean, none is
	// absent, anything else unclean.
	ended string
	// open: it is still running.
	open bool
	// runID is the execution itself, "" when absent.
	runID string
	// resets is what its memo says of the key's resets (CR-1).
	resets *gateResetMemo
	// closedAt is when it closed, zero while it is open or when absent.
	closedAt time.Time
}

// previousExecution reads the gate id's latest execution. One started
// meanwhile is joined under USE_EXISTING, and the start's input is then moot.
func (a *GateActivities) previousExecution(ctx context.Context, gateID string) (gatePrevious, error) {
	described, err := a.client.DescribeWorkflow(ctx, gateID, "")
	var missing *serviceerror.NotFound
	switch {
	case errors.As(err, &missing):
		return gatePrevious{ended: GatePreviousAbsent}, nil
	case err != nil:
		return gatePrevious{}, fmt.Errorf("describe gate %s: %w", gateID, err)
	case described == nil || described.RawResponse == nil:
		return gatePrevious{}, fmt.Errorf("describe gate %s: no response", gateID)
	}
	info := described.RawResponse.GetWorkflowExecutionInfo()
	out := gatePrevious{ended: GatePreviousUnclean, open: info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		runID: info.GetExecution().GetRunId(), resets: resetMemoOf(info.GetMemo())}
	if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		out.ended = GatePreviousClean
	}
	if !out.open && info.GetCloseTime() != nil {
		out.closedAt = info.GetCloseTime().AsTime()
	}
	return out, nil
}

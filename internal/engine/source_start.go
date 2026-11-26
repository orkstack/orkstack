package engine

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// SourceStart is what a start of a source's workflow came to.
type SourceStart string

const (
	// SourceSignalled: it was open, and was told.
	SourceSignalled SourceStart = "signalled"
	// SourceStarted: it was not, and was started with the carry it had.
	SourceStarted SourceStart = "started"
	// SourceDeletedCopy: the document is a copy of a source that was deleted.
	SourceDeletedCopy SourceStart = "deleted_copy"
	// SourcePurged: its last execution removed what it kept.
	SourcePurged SourceStart = "purged"
	// SourceNewerCarry: a newer release wrote its carry, and starts it.
	SourceNewerCarry SourceStart = "newer_carry"
	// SourceNotRun: this deployment runs no source.
	SourceNotRun SourceStart = "not_run"
	// SourceUnanswered: whether it was started is not known. It is begun
	// again by whatever finds it so.
	SourceUnanswered SourceStart = "unanswered"
)

const (
	// sourceStartTries is how often a start is begun again in one call.
	sourceStartTries = 3
	// sourceStartBound is how long a start may take from the describe it goes
	// by: one that is later is not made.
	sourceStartBound = 30 * time.Second
)

// sourceClient is what is asked of the cluster about sources' workflows.
type sourceClient interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any, options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error)
}

// SourceStarter starts and tells sources' workflows: there is one way a
// source's workflow is started, whatever starts it.
type SourceStarter struct {
	Client     sourceClient
	Tombstoned func(ctx context.Context, sourceID string) (bool, error)
	Namespaces NamespaceReader
	// DefaultQueue is the channel an unnamed pool resolves to.
	DefaultQueue string
	// Pin is what a source's first execution is pinned with.
	Pin eventsource.Pinned
	// Versioned: this engine runs as a release. One that does not starts no
	// source's workflow.
	Versioned bool
	// Runs: this deployment runs sources. Where it runs none, the workflow of
	// a source that is being deleted is started all the same.
	Runs   bool
	Logger *slog.Logger
	// Converter decodes a memo; nil is the engine's.
	Converter converter.DataConverter
}

// sourceExecution is the latest execution of a source's workflow.
type sourceExecution struct {
	runID  string
	status enumspb.WorkflowExecutionStatus
	carry  jsontext.Value
	now    eventsource.Now
	// task: a task of it is pending, so it has not read what it was last told.
	task bool
	// taskTries is how often that task was tried.
	taskTries int32
	// release is the build it is pinned to.
	release string
	pending []*workflowpb.PendingActivityInfo
}

func (e *sourceExecution) open() bool {
	return e != nil && e.status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// same reports whether nothing ran between the two.
func (e *sourceExecution) same(o *sourceExecution) bool {
	if e == nil || o == nil {
		return e == o
	}
	return e.runID == o.runID && e.status == o.status
}

// outside reports whether the execution was ended and did not end itself.
func (e *sourceExecution) outside() bool {
	switch e.status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return true
	}
	return false
}

// describe is the source's latest execution; nil where it has none.
func (s *SourceStarter) describe(ctx context.Context, sourceID string) (*sourceExecution, error) {
	resp, err := s.Client.DescribeWorkflowExecution(ctx, sourceID, "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("engine: describe event source %s: %w", sourceID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	if info.GetType().GetName() != SourceWorkflowType {
		return nil, fmt.Errorf("engine: the workflow %s is a %q, not an event source's", sourceID, info.GetType().GetName())
	}
	e, err := s.execution(sourceID, info)
	if err != nil {
		return nil, err
	}
	e.task, e.pending = resp.GetPendingWorkflowTask() != nil, resp.GetPendingActivities()
	e.taskTries = resp.GetPendingWorkflowTask().GetAttempt()
	e.release = info.GetVersioningInfo().GetDeploymentVersion().GetBuildId()
	return e, nil
}

func (s *SourceStarter) execution(sourceID string, info *workflowpb.WorkflowExecutionInfo) (*sourceExecution, error) {
	dc := s.Converter
	if dc == nil {
		dc = DataConverter()
	}
	e := &sourceExecution{runID: info.GetExecution().GetRunId(), status: info.GetStatus()}
	fields := info.GetMemo().GetFields()
	if p, ok := fields[sourceMemoCarry]; ok {
		if err := dc.FromPayload(p, &e.carry); err != nil {
			return nil, fmt.Errorf("engine: event source %s's memo %q: %w", sourceID, sourceMemoCarry, err)
		}
	}
	if p, ok := fields[sourceMemoNow]; ok {
		var raw jsontext.Value
		err := dc.FromPayload(p, &raw)
		if err == nil {
			e.now, err = eventsource.DecodeNow(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("engine: event source %s's memo %q: %w", sourceID, sourceMemoNow, err)
		}
	}
	return e, nil
}

// Start makes the one start there is of src's workflow: it tells one that is
// open to load again, and starts one that is not with the carry of the last
// execution there was. src is the document its caller read, which is not read
// again: a start is never withheld because a document is not there.
func (s *SourceStarter) Start(ctx context.Context, src *orkstackv1.EventSource) (SourceStart, error) {
	id, marked := src.GetId(), src.GetDeletingSince() != nil
	for range sourceStartTries {
		last, err := s.describe(ctx, id)
		if err != nil {
			return "", err
		}
		if last.open() {
			err := s.Client.SignalWorkflow(ctx, id, "", SignalSourceSync, nil)
			if _, closed := errors.AsType[*serviceerror.NotFound](err); closed {
				continue
			}
			if err != nil {
				return "", fmt.Errorf("engine: tell event source %s's workflow: %w", id, err)
			}
			return SourceSignalled, nil
		}
		if !s.Versioned || (!marked && !s.Runs) {
			return SourceNotRun, nil
		}
		carry, startedAfter, withheld, err := s.from(ctx, id, marked, last)
		if err != nil || withheld != "" {
			return withheld, err
		}
		source, err := s.source(ctx, src)
		if err != nil {
			return "", err
		}
		// The carry is of the execution that still is the last one, read a
		// moment ago.
		began := time.Now()
		again, err := s.describe(ctx, id)
		if err != nil {
			return "", err
		}
		if !again.same(last) {
			continue
		}
		switch err := s.start(ctx, began, src, carry, startedAfter, source); {
		case err == nil:
			return SourceStarted, nil
		case ctx.Err() != nil:
			return "", ctx.Err()
		case errors.Is(err, errStartUnanswered):
			s.Logger.WarnContext(ctx, "the start of an event source's workflow was not answered; it is begun again", "event_source_id", id, "error", err)
			continue
		default:
			return "", err
		}
	}
	return SourceUnanswered, nil
}

// from is the carry an execution that follows last is started with, what its
// first memo says of how last ended, and why none is started.
func (s *SourceStarter) from(ctx context.Context, id string, marked bool, last *sourceExecution) (jsontext.Value, string, SourceStart, error) {
	var carry eventsource.Carry
	if last != nil && len(last.carry) > 0 {
		var err error
		switch carry, err = eventsource.DecodeCarry(last.carry); {
		case err == nil:
		case !errors.Is(err, eventsource.ErrCarryFormat) && marked:
			// What it kept is removed whatever it remembered.
			s.Logger.WarnContext(ctx, "a deleted event source's carry is damaged; its workflow is started without one", "event_source_id", id, "error", err)
			last = nil
		case !errors.Is(err, eventsource.ErrCarryFormat):
			return nil, "", "", fmt.Errorf("engine: event source %s's carry is damaged, and nothing is started from it (delete the source and create it again): %w", id, err)
		default:
			s.Logger.WarnContext(ctx, "an event source's carry was written by a newer release; that release starts it", "event_source_id", id, "error", err)
			return nil, "", SourceNewerCarry, nil
		}
	}
	if !marked {
		// A failed read is never "no tombstone".
		switch gone, err := s.Tombstoned(ctx, id); {
		case err != nil:
			return nil, "", "", fmt.Errorf("engine: event source %s: %w", id, err)
		case gone:
			return nil, "", SourceDeletedCopy, nil
		}
	} else if carry.Continued != nil && carry.Continued.How == eventsource.EndedPurged {
		return nil, "", SourcePurged, nil
	}
	if last == nil || len(last.carry) == 0 {
		body, err := eventsource.FirstCarry(s.Pin).Encode()
		return body, "", "", err
	}
	startedAfter := ""
	if last.outside() {
		startedAfter = eventsource.EndedAbandoned
		p := carry.Pinned
		if last.now.Events >= p.BudgetEvents*p.ForcedTimes || last.now.Bytes >= p.BudgetBytes*p.ForcedTimes {
			// The cluster ends an execution for its history; its own reason
			// for a termination is not parsed.
			startedAfter = eventsource.EndedLimit
		}
	}
	return last.carry, startedAfter, "", nil
}

// source is what the document asks for, as a memo says it.
func (s *SourceStarter) source(ctx context.Context, src *orkstackv1.EventSource) (eventsource.Source, error) {
	out := eventsource.Source{Namespace: src.GetNamespace(), Name: src.GetName()}
	if src.GetDeletingSince() != nil || src.GetPause() != nil {
		return out, nil
	}
	ns, err := s.Namespaces.Get(ctx, src.GetNamespace())
	if err != nil {
		return out, fmt.Errorf("engine: the namespace of event source %s: %w", src.GetId(), err)
	}
	if ns.GetHold() != nil {
		return out, nil
	}
	e := eventsources.Effective(src, eventsources.Limits{})
	if _, out.Pool, err = sourcePool(e.GetPool(), ns, s.DefaultQueue); err != nil {
		return out, fmt.Errorf("engine: the pool of event source %s: %w", src.GetId(), err)
	}
	out.Instances = e.GetInstances()
	return out, nil
}

// sourcePool is the channel and the name of the pool a source's instances
// wait in: the default pool by another name is one pool.
func sourcePool(pool string, ns *orkstackv1.Namespace, defaultQueue string) (queue, name string, err error) {
	if queue, err = task.QueueForSource(cmp.Or(pool, DefaultPoolFor(ns)), defaultQueue); err != nil {
		return "", "", err
	}
	name, named := task.PoolForSourceQueue(queue)
	if !named {
		name = task.DefaultPool
	}
	return queue, name, nil
}

var errStartUnanswered = errors.New("not answered within its bound")

func (s *SourceStarter) start(ctx context.Context, began time.Time, src *orkstackv1.EventSource, carry jsontext.Value, startedAfter string, source eventsource.Source) error {
	now, err := eventsource.Now{StartedAfter: startedAfter, Source: source, Load: eventsource.LoadIsFailing}.Encode()
	if err != nil {
		return fmt.Errorf("engine: event source %s's first memo: %w", src.GetId(), err)
	}
	call, cancel := context.WithDeadline(ctx, began.Add(sourceStartBound))
	defer cancel()
	in := SourceInput{Namespace: src.GetNamespace(), Name: src.GetName(), ID: src.GetId(), Pool: source.Pool, Instances: source.Instances, Carry: carry, StartedAfter: startedAfter}
	_, err = s.Client.SignalWithStartWorkflow(call, src.GetId(), SignalSourceSync, nil, client.StartWorkflowOptions{
		ID: src.GetId(), TaskQueue: task.DefaultQueue,
		// The id is the source's for good: it is used again after an execution
		// of it closed, and one that is open is told and nothing is started.
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Memo:                     map[string]any{sourceMemoCarry: carry, sourceMemoNow: jsontext.Value(now)},
	}, SourceWorkflowType, in)
	if err != nil && call.Err() != nil {
		return fmt.Errorf("%w: %w", errStartUnanswered, err)
	}
	if err != nil {
		return fmt.Errorf("engine: start event source %s's workflow: %w", src.GetId(), err)
	}
	return nil
}

// Sync tells the workflow of a source that is open to load again.
func (s *SourceStarter) Sync(ctx context.Context, sourceID string) error {
	return s.signal(ctx, sourceID, "", SignalSourceSync, nil)
}

// Report relays what an instance reported, where the workflow waits for it
// and was not told: a report nobody waits for adds nothing to its history.
func (s *SourceStarter) Report(ctx context.Context, ref held.Ref, what orkstackv1.InstanceReport, reason string) error {
	word, ok := sourceReports[what]
	if !ok {
		return fmt.Errorf("engine: a report of kind %d is not one", what)
	}
	if word == eventsource.ReportHealthy && ref.Attempt <= 1 {
		// Only an attempt after the first has a back-off to begin again.
		return nil
	}
	return s.report(ctx, ref, word, reason)
}

// Started relays that an attempt confirmed, where the workflow waits for it.
func (s *SourceStarter) Started(ctx context.Context, ref held.Ref) error {
	return s.report(ctx, ref, eventsource.ReportStarted, "")
}

func (s *SourceStarter) report(ctx context.Context, ref held.Ref, word, reason string) error {
	e, err := s.describe(ctx, ref.SourceID)
	if err != nil {
		return err
	}
	if !e.open() || !awaits(e.now, ref.Index, ref.Epoch, word) {
		return nil
	}
	return s.signal(ctx, ref.SourceID, e.runID, SignalSourceReport, SourceReport{Index: ref.Index, Epoch: ref.Epoch, What: word, Reason: reason})
}

// signal tells a workflow; one that has closed meanwhile is told by what
// starts it again.
func (s *SourceStarter) signal(ctx context.Context, sourceID, runID, name string, arg any) error {
	err := s.Client.SignalWorkflow(ctx, sourceID, runID, name, arg)
	if _, closed := errors.AsType[*serviceerror.NotFound](err); closed || err == nil {
		return nil
	}
	return fmt.Errorf("engine: tell event source %s's workflow %s: %w", sourceID, name, err)
}

// awaits reports whether a workflow of that memo waits for the report.
func awaits(now eventsource.Now, index int32, epoch int64, word string) bool {
	slot, ok := now.Slots[index]
	if !ok || !slices.Contains(slot.Awaits[epoch], word) {
		return false
	}
	_, had := slot.Had[epoch][word]
	return !had
}

var sourceReports = map[orkstackv1.InstanceReport]string{
	orkstackv1.InstanceReport_INSTANCE_REPORT_READY:   eventsource.ReportReady,
	orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID: eventsource.ReportInvalid,
	orkstackv1.InstanceReport_INSTANCE_REPORT_HEALTHY: eventsource.ReportHealthy,
}

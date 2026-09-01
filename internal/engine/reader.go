package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrRunNotFound reports that the cluster holds no execution under a run id.
//
// It wraps artifacts.ErrNotFound so one "not found" test covers both halves of
// the read model: Temporal answers for live runs, the archive for finished ones
// whose history has aged out.
var ErrRunNotFound = fmt.Errorf("engine: run not found: %w", artifacts.ErrNotFound)

const (
	// MemoOrkflowName, MemoOrkflowVersion and MemoNamespace are the memo keys
	// StartRun writes onto an execution. Visibility returns them with the
	// execution, so a list can scope by namespace without reading history.
	//
	// All three carry the orkflow_ prefix, and the third one needs it most: an
	// unqualified `namespace` sitting beside a Temporal client would read as
	// the Temporal namespace, which is exactly the ambiguity AGENTS.md §1.9
	// forbids. The key is written into history, so it is chosen once.
	MemoOrkflowName    = "orkflow_name"
	MemoOrkflowVersion = "orkflow_version"
	MemoNamespace      = "orkflow_namespace"
	// DefaultRunPageSize bounds an unlimited ListRuns.
	DefaultRunPageSize = 200

	// maxRunListPages bounds how far ListRuns pages through visibility looking
	// for matches, so a narrow filter over a large namespace cannot turn one
	// request into an unbounded scan. It applies when every record the walk
	// reads already belongs to the namespace that asked and already carries
	// the status it asked for — the filter left is then the caller's own
	// orkflow name, and how far its own traffic lets it see is its own
	// business.
	maxRunListPages = 20

	// maxRunScanPages bounds the walk taken when the namespace could NOT be
	// pushed into the query, which is the one filter that must not be answered
	// by "as far as the budget got": another tenant's traffic would decide
	// whether this one can see its own runs. That walk reads the index to the
	// end, and this is only the stop that keeps a request finite; hitting it
	// is reported as a failure rather than as a short list.
	maxRunScanPages = 2000
)

// RunQuery selects runs. Zero values mean "no filter".
type RunQuery struct {
	// Namespace narrows the list to one OrkStack namespace. Empty means no
	// namespace filter; callers that mean the built-in default pass "default".
	Namespace   string
	OrkflowName string
	Status      orkstackv1.Status
	// Limit caps the runs RETURNED, newest first. 0 means DefaultRunPageSize,
	// except under Exhaustive, where 0 means every match.
	Limit int
	// Exhaustive makes a safety-sensitive caller fail rather than return a
	// prefix when matching requires more than the ordinary page budget. It is
	// deliberately internal: user-facing lists stay bounded.
	//
	// It governs how completely visibility is WALKED, not how much comes back.
	// A caller may ask for a trustworthy answer and still want one page of it,
	// which is what the UI's "is any run still live?" check does; only a
	// caller that also leaves Limit at 0 receives everything, and that is the
	// deletion gate, which needs true counts for its refusal message.
	Exhaustive bool
}

// runReadClient is the slice of client.Client the reader uses. Narrowing it
// keeps the decoding testable against synthesized history, which is the part
// worth testing without a cluster.
type runReadClient interface {
	GetWorkflowHistory(
		ctx context.Context,
		workflowID, runID string,
		isLongPoll bool,
		filterType enumspb.HistoryEventFilterType,
	) client.HistoryEventIterator
	ListWorkflow(
		ctx context.Context,
		request *workflowservice.ListWorkflowExecutionsRequest,
	) (*workflowservice.ListWorkflowExecutionsResponse, error)
}

// TemporalRunReader answers run reads from Temporal, so any replica can answer
// for any run.
//
// A run's history is its event log. RunWorkflow records each lifecycle Event
// and a small immutable snapshot URI in the PublishEvent activity input. The
// complete Run lives in shared object storage and is fetched once, after the
// history scan, so both history payload size and read cost remain bounded.
//
//	eventId 5  ACTIVITY_TASK_SCHEDULED  PublishEvent  queue ork-engine-events
//	  json/protobuf orkstack.v1.Event  {"runId":"ork-…","sequence":"2",…}
//	  json/plain                       "file:///…/snapshots/…/2.pb"
//	  json/plain                       "ork-…#2"
//
// The in-process RunStore and EventBus are not consulted here and are not a
// cache in front of this: they are write-side helpers that assemble the archive
// body and dedupe webhook notifications.
type TemporalRunReader struct {
	client    runReadClient
	temporal  client.Client
	converter converter.DataConverter
	snapshots SnapshotStore

	// namespaceIndexed reports that this cluster has the OrkflowNamespace
	// search attribute (task.EnsureRunSearchAttributes), so a namespace is a
	// predicate the cluster applies rather than a filter this reader applies
	// afterwards. It is told, at boot, by the process that registered it.
	namespaceIndexed bool
}

// The reader is the archive writer's event source too: PublishEvent assembles
// an archive body from the record rather than from the executing process's own
// bus, which is the only way that body is whole under more than one replica.
var _ RunEventSource = (*TemporalRunReader)(nil)

// NewTemporalRunReader returns a reader over a Temporal client. It reads the
// namespace the client is bound to, which is the hard partition every other
// part of the engine already respects.
func NewTemporalRunReader(c client.Client) *TemporalRunReader {
	r := newRunReader(c, converter.GetDefaultDataConverter())
	r.temporal = c
	return r
}

// WithSnapshotStore resolves the immutable references recorded in run
// history. All replicas use the same configured object store.
func (r *TemporalRunReader) WithSnapshotStore(store SnapshotStore) *TemporalRunReader {
	r.snapshots = store
	return r
}

// WorkflowExists reports whether Temporal still retains workflowID. A timeout
// or transport failure is returned, never interpreted as absence.
func (r *TemporalRunReader) WorkflowExists(ctx context.Context, namespace, workflowID string) (bool, error) {
	if r.temporal == nil {
		return false, fmt.Errorf("engine: workflow existence reader has no Temporal client")
	}
	if namespace == "" {
		_, err := r.temporal.DescribeWorkflowExecution(ctx, workflowID, "")
		return classifyWorkflowExists(err)
	}
	_, err := r.temporal.WorkflowService().DescribeWorkflowExecution(ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
	return classifyWorkflowExists(err)
}

func classifyWorkflowExists(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, err
}

// NamespaceRetention returns Temporal's configured workflow retention period.
func (r *TemporalRunReader) NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error) {
	if r.temporal == nil {
		return 0, fmt.Errorf("engine: namespace retention reader has no Temporal client")
	}
	response, err := r.temporal.WorkflowService().DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	if err != nil {
		return 0, err
	}
	if response.GetConfig().GetWorkflowExecutionRetentionTtl() == nil {
		return 0, fmt.Errorf("engine: namespace %q has no workflow retention", namespace)
	}
	return response.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(), nil
}

// WithNamespaceIndex records whether the cluster indexes a run's OrkStack
// namespace. It defaults to off, which is the safe half: the reader then
// reads the whole index rather than trusting a predicate the cluster would
// reject, and a caller who forgot to say so gets a slower answer, never a
// wrong one.
func (r *TemporalRunReader) WithNamespaceIndex(indexed bool) *TemporalRunReader {
	r.namespaceIndexed = indexed
	return r
}

func newRunReader(c runReadClient, dc converter.DataConverter) *TemporalRunReader {
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	return &TemporalRunReader{client: c, converter: dc}
}

// GetRun returns a run rebuilt from its history: the latest snapshot the
// workflow published, with a terminal status overlaid from the execution's own
// closing event.
//
// The overlay is what covers a **terminated** run. Termination emits no
// lifecycle event at all — the workflow never runs again — so a reader that
// trusted only the last published snapshot would report that run RUNNING
// forever.
func (r *TemporalRunReader) GetRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, fmt.Errorf("engine: get run: run id is required")
	}
	projection, err := r.scan(ctx, runID, false, 0, nil)
	if err != nil {
		return nil, err
	}
	if projection.run == nil {
		return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
	}
	return projection.run, nil
}

// Events returns a run's whole event stream after fromSequence, in sequence
// order, as Temporal recorded it.
func (r *TemporalRunReader) Events(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	projection, err := r.scan(ctx, runID, false, fromSequence, nil)
	if err != nil {
		return nil, err
	}
	return projection.events, nil
}

// StreamEvents delivers a run's events after fromSequence to send, and ends at
// the run's terminal event.
//
// A live run blocks in the iterator rather than polling: GetWorkflowHistory
// with isLongPoll=true does not finish until the workflow does (SDK v1.47.0
// internal/client.go:186-188), so the iterator *is* the stream.
func (r *TemporalRunReader) StreamEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if send == nil {
		return fmt.Errorf("engine: stream run %s: no consumer", runID)
	}
	_, err := r.scan(ctx, runID, true, fromSequence, send)
	return err
}

// runListBudget resolves the three numbers a visibility walk needs, keeping
// them apart on purpose.
//
// `limit` is how many runs the CALLER gets; `batch` is how many records each
// visibility request asks Temporal for. Conflating them made an exhaustive
// query ignore page_size.
//
// Only the explicit Exhaustive flag unlocks an unbounded result. The derived
// walk (an unscoped namespace) must not: that is still a user-facing list and
// keeps its default page.
//
// An explicitly exhaustive caller reads whole pages whatever its Limit is. The
// safety check asks for one run and is willing to read the index to find it;
// at a page size of one, the 2000-page budget would be spent after 2000
// records and the check would start failing on any busy orkflow. The derived
// walk keeps the caller's page size, which is the budget the
// namespace-fallback tests calibrate against.
func runListBudget(q RunQuery, exhaustive bool) (limit, batch, pages int) {
	limit = q.Limit
	if limit <= 0 && !q.Exhaustive {
		limit = DefaultRunPageSize
	}
	batch = limit
	if batch <= 0 || q.Exhaustive {
		batch = DefaultRunPageSize
	}
	pages = maxRunListPages
	if exhaustive {
		pages = maxRunScanPages
	}
	return limit, batch, pages
}

// ListRuns returns runs from Temporal's visibility index, newest first.
//
// A listed run is the shallow projection visibility can give: id, orkflow
// (from the memo StartRun writes), status and times. Task runs, input and
// output live in history and in the archive, and GetRun reads them from there.
//
// The NAMESPACE is a predicate the cluster applies, and the other filters are
// applied over the page. The difference is not convenience, it is what the
// namespace is: a tenancy boundary. Filtered here, over a walk bounded at
// maxRunListPages, the answer to "list my runs" depended on how many runs
// everyone ELSE had — a namespace with one live run behind two hundred of a
// neighbour's was told it had none, and the threshold was set by a stranger.
// An orkflow name inside one namespace is not that, so it stays a filter, and
// its memo stays unindexed.
//
// A cluster without the index (task.EnsureRunSearchAttributes could not
// register it) is answered by reading the index to the end instead — slower,
// and still the tenant's own answer. If even that walk runs out of pages the
// list FAILS rather than returning what it happened to reach: a short list a
// caller cannot tell from a complete one is the defect this paragraph exists
// to close.
func (r *TemporalRunReader) ListRuns(ctx context.Context, q RunQuery) ([]*orkstackv1.Run, error) {
	// A status no execution decodes to is answered without asking.
	// matchesRunQuery would reject every record the walk read, so the result is
	// empty however far it went — the round-trips are the only real thing about
	// it, and the UI's Versions tab spends a whole page budget on one of these
	// per render.
	//
	// This returns BEFORE the exhaustive branches on purpose. Their error exists
	// to stop a SHORT answer being mistaken for a complete one; an answer that is
	// provably empty is complete, so failing here would refuse a question
	// visibility was never going to answer. The archive half, which stores the
	// status it was given, still answers PENDING and SKIPPED.
	if q.Status != orkstackv1.Status_STATUS_UNSPECIFIED && !readerDecodableStatuses[q.Status] {
		return nil, nil
	}

	query, scoped := r.listQuery(q)
	// A safety-sensitive exhaustive walk must either reach the end or fail.
	// The same fail-closed walk is required when the namespace could not be
	// pushed into visibility: another tenant's traffic must not create a short
	// answer for this one.
	exhaustive := q.Exhaustive || (q.Namespace != "" && !scoped)

	limit, batch, pages := runListBudget(q, exhaustive)
	out := make([]*orkstackv1.Run, 0, batch)
	var token []byte
	for page := 0; page < pages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      int32(batch),
			NextPageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("engine: list runs from temporal: %w", err)
		}
		for _, execution := range response.GetExecutions() {
			run := r.listedRun(execution)
			if !matchesRunQuery(run, q) {
				continue
			}
			out = append(out, run)
			// A full quota is a COMPLETE answer to "give me up to N", so the
			// walk can stop even when it was asked to be exhaustive. What it
			// must never do is stop SHORT — that is the ambiguity the error
			// below exists to prevent.
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
	if q.Namespace != "" && !scoped {
		return nil, fmt.Errorf(
			"engine: list runs in namespace %q: visibility does not index %s, so every "+
				"other namespace's runs had to be read and discarded, and %d pages were "+
				"not enough. Register the search attribute (it is provisioned at engine "+
				"boot) and the namespace becomes a query predicate",
			q.Namespace, task.SearchAttrOrkflowNamespace, pages)
	}
	if exhaustive {
		return nil, fmt.Errorf(
			"engine: exhaustive run list for orkflow %q in namespace %q did not reach the "+
				"end of Temporal visibility after %d pages; deletion safety cannot be "+
				"decided from a partial answer",
			q.OrkflowName, q.Namespace, pages)
	}
	return out, nil
}

// listQuery is the visibility query for one RunQuery, and whether the
// namespace went into it.
//
// TWO predicates are pushed down. The namespace goes in when the cluster has
// the custom index. The STATUS goes in unconditionally when it can be spelled,
// because ExecutionStatus is one of Temporal's BUILT-IN attributes and needs
// no registration — which is what keeps "is anything still live?" a query over
// live runs rather than a walk over every run the namespace ever had.
//
// A run started before the index existed carries the memo and no attribute, so
// it is not listed by namespace; it is still readable by id, and once it
// finishes the archive — whose key begins with the namespace — holds it.
//
// The orkflow NAME stays a client-side filter: its memo is unindexed.
func (r *TemporalRunReader) listQuery(q RunQuery) (string, bool) {
	query := fmt.Sprintf("WorkflowType = %q", RunWorkflowType)
	scoped := q.Namespace != "" && r.namespaceIndexed
	if scoped {
		query = fmt.Sprintf("%s AND %s = %q", query, task.SearchAttrOrkflowNamespace,
			artifacts.ResolveNamespace(q.Namespace))
	}
	if predicate, ok := executionStatusPredicate(q.Status); ok {
		query += " AND " + predicate
	}
	return query, scoped
}

// terminalExecutionStatuses is every Temporal execution status that
// statusFromExecution maps onto a TERMINAL OrkStack status.
//
// The live set is deliberately this list's COMPLEMENT rather than a list of
// its own, because statusFromExecution's default arm is what decides it: a
// status a later SDK adds — Paused arrived exactly that way — becomes RUNNING
// there, and a NOT IN predicate keeps it live here with no edit to this file.
// Enumerating the live statuses instead would drop such a run from "is
// anything still live?", which is the one question that must not under-report,
// because an under-report deletes an orkflow out from under a running run.
var terminalExecutionStatuses = []enumspb.WorkflowExecutionStatus{
	enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
}

// readerDecodableStatuses is every OrkStack status a Temporal visibility record
// can decode to.
//
// It is DERIVED from statusFromExecution rather than written out, so it cannot
// drift from it: a status a later SDK adds lands here through that function's
// default arm, in the same move that makes it RUNNING everywhere else. The
// complement — STATUS_PENDING and STATUS_SKIPPED — is what no execution maps
// onto, and asking visibility for one of those is answered without a request.
var readerDecodableStatuses = func() map[orkstackv1.Status]bool {
	decodable := make(map[orkstackv1.Status]bool, len(enumspb.WorkflowExecutionStatus_name))
	for value := range enumspb.WorkflowExecutionStatus_name {
		decodable[statusFromExecution(enumspb.WorkflowExecutionStatus(value))] = true
	}
	return decodable
}()

// executionStatusPredicate is the visibility predicate narrowing a listing to
// the executions statusFromExecution maps onto want, and whether there is one
// to push down at all.
//
// It NARROWS; it never decides. matchesRunQuery still applies the status over
// the page, so a predicate that is correct but too wide cannot change which
// runs come back — only how many records were read to find them. That is the
// property that makes pushing anything down here safe.
//
// STATUS_PENDING and STATUS_SKIPPED push nothing down, because no visibility
// record decodes to either: statusFromExecution cannot return them. Narrowing
// on them would assert something about the index that is not true of it, and
// they are answered by the archive half, which stores the status it was given.
func executionStatusPredicate(want orkstackv1.Status) (string, bool) {
	if want == orkstackv1.Status_STATUS_UNSPECIFIED {
		return "", false
	}
	if want == orkstackv1.Status_STATUS_RUNNING {
		return "ExecutionStatus NOT IN (" +
			quotedExecutionStatuses(terminalExecutionStatuses) + ")", true
	}
	var match []enumspb.WorkflowExecutionStatus
	for _, status := range terminalExecutionStatuses {
		if statusFromExecution(status) == want {
			match = append(match, status)
		}
	}
	if len(match) == 0 {
		return "", false
	}
	return "ExecutionStatus IN (" + quotedExecutionStatuses(match) + ")", true
}

// quotedExecutionStatuses renders statuses the way the query language spells
// them. The SDK's own String() is that spelling, so an SDK upgrade which
// renames a status cannot leave a stale literal behind here.
func quotedExecutionStatuses(statuses []enumspb.WorkflowExecutionStatus) string {
	quoted := make([]string, 0, len(statuses))
	for _, status := range statuses {
		quoted = append(quoted, strconv.Quote(status.String()))
	}
	return strings.Join(quoted, ", ")
}

// runProjection is one run's history, decoded.
type runProjection struct {
	run            *orkstackv1.Run
	events         []*orkstackv1.Event
	snapshotURI    string
	snapshotLoaded bool
}

// scan walks a run's history once and decodes it.
//
// With send set the scan is a stream: each event after fromSequence is handed
// over as it is decoded and the walk stops at the run's terminal event. With
// send nil the whole projection is collected, and the walk continues past the
// terminal event to the execution's closing event, which is the only place a
// terminated run's outcome is recorded.
func (r *TemporalRunReader) scan(
	ctx context.Context,
	runID string,
	follow bool,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) (*runProjection, error) {
	iter := r.client.GetWorkflowHistory(ctx, runID, "", follow,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	projection := &runProjection{}
	var (
		seen         = map[int64]struct{}{}
		lastSequence int64
		read         int
	)
	for iter.HasNext() {
		historyEvent, err := iter.Next()
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) && read == 0 {
				return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
			}
			return nil, fmt.Errorf("engine: read history of run %s: %w", runID, err)
		}
		read++

		if attrs := historyEvent.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			projection.run = mergeRunSnapshot(projection.run, r.startedRun(runID, historyEvent, attrs))
			continue
		}
		if r.isPublishEvent(historyEvent) {
			event, snapshotURI, legacyRun, err := r.decodePublishEvent(runID, historyEvent)
			if err != nil {
				return nil, err
			}
			if snapshotURI != "" {
				projection.snapshotURI = snapshotURI
			}
			if legacyRun != nil {
				// Whichever compatible shape appears last is the complete latest
				// snapshot. This also keeps a mixed upgrade history ordered.
				projection.snapshotURI = ""
				projection.run = mergeRunSnapshot(projection.run, legacyRun)
			}
			if event == nil {
				continue
			}
			if _, duplicate := seen[event.GetSequence()]; duplicate {
				continue
			}
			seen[event.GetSequence()] = struct{}{}
			if event.GetSequence() > lastSequence {
				lastSequence = event.GetSequence()
			}
			if event.GetSequence() <= fromSequence {
				if send != nil && isTerminalRunEvent(event) {
					return projection, nil
				}
				continue
			}
			if send != nil {
				if err := send(event); err != nil {
					return nil, err
				}
			} else {
				projection.events = append(projection.events, event)
			}
			if isTerminalRunEvent(event) && send != nil {
				return projection, nil
			}
			continue
		}
		if status, cause, closed := closedExecution(historyEvent); closed {
			if err := r.loadSnapshot(ctx, runID, projection); err != nil {
				return nil, err
			}
			terminal := r.closeRun(projection, runID, historyEvent, status, cause, lastSequence)
			if terminal == nil || terminal.GetSequence() <= fromSequence {
				continue
			}
			if send != nil {
				if err := send(terminal); err != nil {
					return nil, err
				}
				return projection, nil
			}
			projection.events = append(projection.events, terminal)
		}
	}
	if err := r.loadSnapshot(ctx, runID, projection); err != nil {
		return nil, err
	}
	if projection.run == nil && len(projection.events) == 0 {
		return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
	}
	return projection, nil
}

// closeRun overlays the outcome the execution itself reports, and returns a
// terminal event when the run produced none of its own.
//
// A run that failed or was canceled emits its terminal event first and closes
// afterwards, so the published snapshot is already terminal and richer than
// anything the closing event carries: it is kept. A run that was **terminated**
// emits nothing, and this is the only record of its end — including the
// terminal event, synthesized here so the stream contract still holds.
func (r *TemporalRunReader) closeRun(
	projection *runProjection,
	runID string,
	historyEvent *historypb.HistoryEvent,
	status orkstackv1.Status,
	cause *orkstackv1.Error,
	lastSequence int64,
) *orkstackv1.Event {
	closedAt := cloneTime(historyEvent.GetEventTime())
	if projection.run == nil {
		projection.run = &orkstackv1.Run{Id: runID}
	}
	if isTerminal(projection.run.GetStatus()) {
		if projection.run.GetFinishedAt() == nil {
			projection.run.FinishedAt = closedAt
		}
		return nil
	}
	projection.run.Status = status
	projection.run.FinishedAt = closedAt
	if projection.run.GetError() == nil {
		projection.run.Error = cause
	}
	message := "run ended without a lifecycle event"
	if cause.GetMessage() != "" {
		message = cause.GetMessage()
	}
	return &orkstackv1.Event{
		RunId:     runID,
		Sequence:  lastSequence + 1,
		Type:      terminalEventFor(status),
		Status:    status,
		Message:   message,
		Timestamp: closedAt,
	}
}

// isPublishEvent reports whether a history event is one of the run's lifecycle
// events, recorded as the scheduling of the projection activity.
func (r *TemporalRunReader) isPublishEvent(historyEvent *historypb.HistoryEvent) bool {
	attrs := historyEvent.GetActivityTaskScheduledEventAttributes()
	return attrs != nil && attrs.GetActivityType().GetName() == ActivityPublishEvent
}

// decodePublishEvent decodes the current (Event, snapshotURI, dedupeKey)
// arguments. Histories written before snapshot externalization carried an
// inline Run in payload 1; that shape remains readable during upgrades.
func (r *TemporalRunReader) decodePublishEvent(
	runID string,
	historyEvent *historypb.HistoryEvent,
) (*orkstackv1.Event, string, *orkstackv1.Run, error) {
	payloads := historyEvent.GetActivityTaskScheduledEventAttributes().GetInput().GetPayloads()
	var event *orkstackv1.Event
	if len(payloads) > 0 {
		if err := r.converter.FromPayload(payloads[0], &event); err != nil {
			return nil, "", nil, fmt.Errorf("engine: decode event of run %s at history event %d: %w",
				runID, historyEvent.GetEventId(), err)
		}
	}
	if len(payloads) <= 1 {
		return event, "", nil, nil
	}
	var uri string
	if err := r.converter.FromPayload(payloads[1], &uri); err == nil {
		return event, uri, nil, nil
	}
	var legacyRun *orkstackv1.Run
	if err := r.converter.FromPayload(payloads[1], &legacyRun); err != nil {
		return nil, "", nil, fmt.Errorf("engine: decode snapshot reference of run %s at history event %d: %w",
			runID, historyEvent.GetEventId(), err)
	}
	return event, "", legacyRun, nil
}

func (r *TemporalRunReader) loadSnapshot(
	ctx context.Context,
	runID string,
	projection *runProjection,
) error {
	if projection.snapshotLoaded || projection.snapshotURI == "" {
		return nil
	}
	if r.snapshots == nil {
		return fmt.Errorf("engine: read snapshot of run %s: snapshot store is not configured", runID)
	}
	data, err := r.snapshots.ReadSnapshot(ctx, projection.snapshotURI)
	if err != nil {
		return fmt.Errorf("engine: read snapshot of run %s: %w", runID, err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		return fmt.Errorf("engine: decode snapshot of run %s at %s: %w", runID, projection.snapshotURI, err)
	}
	projection.run = mergeRunSnapshot(projection.run, run)
	projection.snapshotLoaded = true
	return nil
}

// startedRun is the run as it exists before its first lifecycle event: the
// arguments the workflow was started with, dated by the execution's own start
// time. It closes the window between a submission and the first PublishEvent
// reaching history, so a read taken immediately after SubmitRun still answers.
func (r *TemporalRunReader) startedRun(
	runID string,
	historyEvent *historypb.HistoryEvent,
	attrs *historypb.WorkflowExecutionStartedEventAttributes,
) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:        runID,
		Status:    orkstackv1.Status_STATUS_PENDING,
		CreatedAt: cloneTime(historyEvent.GetEventTime()),
	}
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) > 1 {
		var wf *orkstackv1.Orkflow
		if err := r.converter.FromPayload(payloads[1], &wf); err == nil && wf != nil {
			run.OrkflowName = wf.GetName()
			run.OrkflowVersion = wf.GetVersion()
			run.Namespace = wf.GetNamespace()
		}
	}
	if len(payloads) > 2 {
		var input *orkstackv1.Payload
		if err := r.converter.FromPayload(payloads[2], &input); err == nil {
			run.Input = input
		}
	}
	return run
}

// listedRun projects one visibility record onto a Run.
func (r *TemporalRunReader) listedRun(info *workflowpb.WorkflowExecutionInfo) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:        info.GetExecution().GetWorkflowId(),
		Status:    statusFromExecution(info.GetStatus()),
		CreatedAt: cloneTime(info.GetStartTime()),
	}
	if info.GetCloseTime() != nil {
		run.FinishedAt = cloneTime(info.GetCloseTime())
	}
	run.OrkflowName = r.memoString(info.GetMemo(), MemoOrkflowName)
	run.OrkflowVersion = r.memoString(info.GetMemo(), MemoOrkflowVersion)
	run.Namespace = r.memoString(info.GetMemo(), MemoNamespace)
	return run
}

// memoString reads one memo field. A run started by an engine that predates
// the memo simply has none, which is a missing name rather than an error.
func (r *TemporalRunReader) memoString(memo *commonpb.Memo, key string) string {
	payload, ok := memo.GetFields()[key]
	if !ok {
		return ""
	}
	var value string
	if err := r.converter.FromPayload(payload, &value); err != nil {
		return ""
	}
	return value
}

// matchesRunQuery applies the client-side half of a list query.
func matchesRunQuery(run *orkstackv1.Run, q RunQuery) bool {
	if q.Namespace != "" && artifacts.ResolveNamespace(run.GetNamespace()) != artifacts.ResolveNamespace(q.Namespace) {
		return false
	}
	if q.OrkflowName != "" && run.GetOrkflowName() != q.OrkflowName {
		return false
	}
	if q.Status != orkstackv1.Status_STATUS_UNSPECIFIED && run.GetStatus() != q.Status {
		return false
	}
	return true
}

// closedExecution maps a history event that closes an execution onto the run
// status it means.
func closedExecution(
	historyEvent *historypb.HistoryEvent,
) (orkstackv1.Status, *orkstackv1.Error, bool) {
	switch historyEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return orkstackv1.Status_STATUS_SUCCEEDED, nil, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := historyEvent.GetWorkflowExecutionFailedEventAttributes()
		return orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{
			Code:    "RUN_FAILED",
			Message: attrs.GetFailure().GetMessage(),
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{
			Code:    "RUN_CANCELED",
			Message: "run canceled",
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		attrs := historyEvent.GetWorkflowExecutionTerminatedEventAttributes()
		message := attrs.GetReason()
		if message == "" {
			message = "run terminated"
		}
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{
			Code:    "RUN_TERMINATED",
			Message: message,
		}, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{
			Code:    "RUN_TIMED_OUT",
			Message: "run timed out",
		}, true
	default:
		return orkstackv1.Status_STATUS_UNSPECIFIED, nil, false
	}
}

// statusFromExecution maps a visibility status onto a run status.
func statusFromExecution(status enumspb.WorkflowExecutionStatus) orkstackv1.Status {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return orkstackv1.Status_STATUS_SUCCEEDED
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return orkstackv1.Status_STATUS_FAILED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return orkstackv1.Status_STATUS_CANCELED
	default:
		return orkstackv1.Status_STATUS_RUNNING
	}
}

// terminalEventFor is the event type that ends a stream for a given outcome.
func terminalEventFor(status orkstackv1.Status) orkstackv1.EventType {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED:
		return orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	case orkstackv1.Status_STATUS_CANCELED:
		return orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
	default:
		return orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	}
}

// mergeRunSnapshot folds a newer snapshot over an older one. The workflow owns
// everything it publishes; the fields it can omit are filled from what came
// before, so a late snapshot never erases the run's identity.
//
// The namespace is carried for a harder reason than the rest. internal/api
// resolves a run id INSIDE the namespace the caller named and refuses it in
// any other, so a projection that lost the field resolves to `default` and
// hands one tenant's run to another to read, watch and cancel. A dropped
// namespace is a tenancy boundary failing open, not a blank column.
func mergeRunSnapshot(base, next *orkstackv1.Run) *orkstackv1.Run {
	if next == nil {
		return base
	}
	if base == nil {
		return next
	}
	if next.GetCreatedAt() == nil {
		next.CreatedAt = base.GetCreatedAt()
	}
	if next.GetInput() == nil {
		next.Input = base.GetInput()
	}
	if next.GetNamespace() == "" {
		next.Namespace = base.GetNamespace()
	}
	if next.GetOrkflowName() == "" {
		next.OrkflowName = base.GetOrkflowName()
	}
	if next.GetOrkflowVersion() == "" {
		next.OrkflowVersion = base.GetOrkflowVersion()
	}
	return next
}

// cloneTime copies a timestamp out of a history event, so nothing the reader
// returns aliases the protobuf it decoded.
func cloneTime(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(ts.AsTime())
}

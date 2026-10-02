package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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
	// the Temporal namespace, which is exactly the ambiguity AGENTS.md §7
	// forbids. The key is written into history, so it is chosen once.
	MemoOrkflowName    = "orkflow_name"
	MemoOrkflowVersion = "orkflow_version"
	MemoNamespace      = "orkflow_namespace"
	// MemoOriginKind and MemoOriginID carry a run's origin back with each
	// visibility record, as MemoNamespace carries its namespace: a listing
	// row can say "trigger trg-…" without a history read per run. Values are
	// the user-facing slug and the id; absent when the plan carries no origin.
	MemoOriginKind = "orkflow_origin_kind"
	MemoOriginID   = "orkflow_origin_id"
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
	// Since is inclusive and Until exclusive on RunIndexTime.
	Since time.Time
	Until time.Time
	// OriginKind keeps runs whose origin.kind equals it; UNSPECIFIED means no
	// origin filter, and a run recorded before origins existed is matched by
	// no value. OriginID keeps runs whose origin.id equals it, within
	// OriginKind — exact match, never a prefix (ListRunsRequest).
	OriginKind orkstackv1.RunOriginKind
	OriginID   string
	// BeforeCreated/BeforeID continue after a descending cursor. They are
	// selected before Limit so a paged caller does not receive a short page
	// merely because the source's first rows preceded its cursor.
	BeforeCreated time.Time
	BeforeID      string
	// Complete orders a bounded full visibility walk before applying Limit.
	// Temporal's visibility order is not a contract for equal creation times,
	// so API cursor pages set this to retain every created_at/ID tie.
	Complete bool
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
	CountWorkflow(
		ctx context.Context,
		request *workflowservice.CountWorkflowExecutionsRequest,
	) (*workflowservice.CountWorkflowExecutionsResponse, error)
	// DescribeWorkflowExecution is the SDK client's describe, bound to the
	// deployment's partition already; LiveAttempts reads the pending
	// activities and their heartbeat detail from it.
	DescribeWorkflowExecution(
		ctx context.Context,
		workflowID, runID string,
	) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// workflowServiceAdmin is the slice of Temporal's WorkflowService the admin
// retention read/write and the active retirement path need. Narrowing it
// keeps all four testable with a fake and no live cluster, the same way
// internal/task/namespace.go's namespaceRegistrar narrows
// DescribeNamespace/RegisterNamespace for EnsureNamespace.
type workflowServiceAdmin interface {
	DescribeNamespace(
		ctx context.Context,
		in *workflowservice.DescribeNamespaceRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.DescribeNamespaceResponse, error)
	UpdateNamespace(
		ctx context.Context,
		in *workflowservice.UpdateNamespaceRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.UpdateNamespaceResponse, error)
	DescribeWorkflowExecution(
		ctx context.Context,
		in *workflowservice.DescribeWorkflowExecutionRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	DeleteWorkflowExecution(
		ctx context.Context,
		in *workflowservice.DeleteWorkflowExecutionRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.DeleteWorkflowExecutionResponse, error)
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

	// admin is the narrowed seam NamespaceRetention, SetNamespaceRetention,
	// LookupWorkflow and DeleteWorkflow use. It is set from the same Temporal
	// client as everything else on this reader (NewTemporalRunReader), so
	// the admin retention read/write and the active retirement path are
	// never a second connection to the cluster; a unit test sets it directly
	// to a fake, with client and temporal left nil.
	admin workflowServiceAdmin

	// namespaceIndexed reports that this cluster has the OrkflowNamespace
	// search attribute (task.EnsureRunSearchAttributes), so a namespace is a
	// predicate the cluster applies rather than a filter this reader applies
	// afterwards. It is told, at boot, by the process that registered it.
	namespaceIndexed bool

	// originIndexed reports that this cluster has the OrkflowOriginKind and
	// OrkflowOriginId search attributes (task.EnsureRunOriginSearchAttributes),
	// so an origin filter is a predicate the cluster applies. It is a separate
	// answer from namespaceIndexed, told at boot the same way.
	originIndexed bool
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
	r.admin = c.WorkflowService()
	return r
}

// WithSnapshotStore resolves the immutable references recorded in run
// history. All replicas use the same configured object store.
func (r *TemporalRunReader) WithSnapshotStore(store SnapshotStore) *TemporalRunReader {
	r.snapshots = store
	return r
}

// LookupWorkflow reports whether the cluster still holds workflowID and, when
// it does, when the execution closed. A found execution with a zero
// closeTime is still running. A transport failure is returned, never read as
// absence. namespace is the deployment's Temporal partition, never an
// orkstack.v1.Namespace name.
func (r *TemporalRunReader) LookupWorkflow(ctx context.Context, namespace, workflowID string) (found bool, closeTime time.Time, err error) {
	if r.admin == nil {
		return false, time.Time{}, fmt.Errorf("engine: workflow lookup reader has no Temporal client")
	}
	resp, err := r.admin.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, err
	}
	info := resp.GetWorkflowExecutionInfo()
	if ct := info.GetCloseTime(); ct != nil {
		return true, ct.AsTime(), nil
	}
	return true, time.Time{}, nil
}

// DeleteWorkflow removes one finished execution's history and visibility
// record from the cluster now, ahead of the backstop retention. An execution
// the cluster no longer holds is not an error: the outcome asked for is the
// outcome. It must only be called for a closed execution — the cluster would
// terminate a running one first — which LookupWorkflow establishes.
// namespace is the deployment's Temporal partition, never an
// orkstack.v1.Namespace name.
func (r *TemporalRunReader) DeleteWorkflow(ctx context.Context, namespace, workflowID string) error {
	if r.admin == nil {
		return fmt.Errorf("engine: workflow deletion reader has no Temporal client")
	}
	_, err := r.admin.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}
	return nil
}

// NamespaceRetention returns Temporal's configured workflow retention period,
// read live from the cluster (DescribeNamespace). namespace is the TEMPORAL
// namespace, never an orkstack.v1.Namespace name.
func (r *TemporalRunReader) NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error) {
	if r.admin == nil {
		return 0, fmt.Errorf("engine: namespace retention reader has no Temporal client")
	}
	response, err := r.admin.DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	if err != nil {
		return 0, err
	}
	if response.GetConfig().GetWorkflowExecutionRetentionTtl() == nil {
		return 0, fmt.Errorf("engine: namespace %q has no workflow retention", namespace)
	}
	return response.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(), nil
}

// SetNamespaceRetention changes Temporal's workflow-execution retention for
// namespace — the TEMPORAL namespace, never an orkstack.v1.Namespace name —
// through Temporal's real UpdateNamespace RPC, and returns the retention
// Temporal CONFIRMED in its response.
//
// retention must be positive; a non-positive value is refused here, before
// any call reaches the cluster. Temporal's own refusal of a positive value it
// still considers invalid (for example, below the cluster's configured
// minimum) is returned to the caller UNTOUCHED: never swallowed into a
// default, a zero value, or a fabricated success.
func (r *TemporalRunReader) SetNamespaceRetention(
	ctx context.Context,
	namespace string,
	retention time.Duration,
) (time.Duration, error) {
	if r.admin == nil {
		return 0, fmt.Errorf("engine: namespace retention writer has no Temporal client")
	}
	if retention <= 0 {
		return 0, fmt.Errorf("engine: namespace retention must be positive, got %s", retention)
	}
	response, err := r.admin.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
		},
	})
	if err != nil {
		return 0, err
	}
	if response.GetConfig().GetWorkflowExecutionRetentionTtl() == nil {
		return 0, fmt.Errorf("engine: namespace %q update reported no workflow retention", namespace)
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

// WithOriginIndex records whether the cluster indexes a run's origin. Off,
// the reader answers an origin filter by the fail-closed walk the namespace
// filter takes without its index.
func (r *TemporalRunReader) WithOriginIndex(indexed bool) *TemporalRunReader {
	r.originIndexed = indexed
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
	projection, err := r.scan(ctx, runID, false, 0, true, nil)
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
//
// The stream is answered from history alone: no snapshot object is fetched,
// so an event read succeeds even when a snapshot is unreadable or no snapshot
// store is wired at all.
func (r *TemporalRunReader) Events(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	projection, err := r.scan(ctx, runID, false, fromSequence, false, nil)
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
	_, err := r.scan(ctx, runID, true, fromSequence, false, send)
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
	if batch <= 0 || q.Exhaustive || q.Complete {
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

	query, scoped, originScoped := r.listQuery(q)
	// A safety-sensitive exhaustive walk must either reach the end or fail.
	// The same fail-closed walk is required when the namespace could not be
	// pushed into visibility: another tenant's traffic must not create a short
	// answer for this one. An origin filter that could not be pushed down is
	// held to the same rule: "which runs did this trigger start" answered from
	// as far as the budget got is a short list nobody can tell from a whole one.
	originUnscoped := q.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && !originScoped
	exhaustive := q.Exhaustive || q.Complete || (q.Namespace != "" && !scoped) || originUnscoped

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
			if limit > 0 && len(out) >= limit && !q.Complete {
				return out, nil
			}
		}
		if q.Complete && limit > 0 && len(out) > limit {
			out = completeRunPage(out, RunQuery{Limit: limit, Complete: true})
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return completeRunPage(out, q), nil
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
	if originUnscoped {
		return nil, fmt.Errorf(
			"engine: list runs by origin: visibility does not index %s or %s, so every "+
				"other origin's runs had to be read and discarded, and %d pages were not "+
				"enough. Register the search attributes (they are provisioned at engine "+
				"boot) and the origin becomes a query predicate",
			task.SearchAttrOrkflowOriginKind, task.SearchAttrOrkflowOriginId, pages)
	}
	if q.Complete {
		return nil, fmt.Errorf("engine: complete run page for orkflow %q in namespace %q could not determine whether history continues after %d visibility pages", q.OrkflowName, q.Namespace, pages)
	}
	if exhaustive {
		return nil, fmt.Errorf(
			"engine: exhaustive run list for orkflow %q in namespace %q did not reach the "+
				"end of Temporal visibility after %d pages; deletion safety cannot be "+
				"decided from a partial answer",
			q.OrkflowName, q.Namespace, pages)
	}
	return completeRunPage(out, q), nil
}

func completeRunPage(runs []*orkstackv1.Run, q RunQuery) []*orkstackv1.Run {
	if !q.Complete {
		return runs
	}
	sort.SliceStable(runs, func(i, j int) bool {
		left, right := RunIndexTime(runs[i]), RunIndexTime(runs[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return runs[i].GetId() > runs[j].GetId()
	})
	if q.Limit > 0 && len(runs) > q.Limit {
		return runs[:q.Limit]
	}
	return runs
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
//
// The ORIGIN goes in when the cluster has the two origin attributes: the kind
// always, the id only when one was asked for. originScoped reports it went
// in; matchesRunQuery still applies it over the page either way.
func (r *TemporalRunReader) listQuery(q RunQuery) (query string, scoped, originScoped bool) {
	query = fmt.Sprintf("WorkflowType = %q", RunWorkflowType)
	scoped = q.Namespace != "" && r.namespaceIndexed
	if scoped {
		query = fmt.Sprintf("%s AND %s = %q", query, task.SearchAttrOrkflowNamespace,
			artifacts.ResolveNamespace(q.Namespace))
	}
	if predicate, ok := executionStatusPredicate(q.Status); ok {
		query += " AND " + predicate
	}
	if slug, ok := artifacts.OriginKindSlug(q.OriginKind); ok && r.originIndexed {
		originScoped = true
		query = fmt.Sprintf("%s AND %s = %q", query, task.SearchAttrOrkflowOriginKind, slug)
		if q.OriginID != "" {
			query = fmt.Sprintf("%s AND %s = %q", query, task.SearchAttrOrkflowOriginId, q.OriginID)
		}
	}
	return query, scoped, originScoped
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
	// priorSnapshotURI is the snapshot URI referenced by the SECOND-newest
	// lifecycle event. Every publish writes its own snapshot before it
	// publishes (workflow.go's emit), so this object is nearly as complete as
	// the newest one — it is what a live-run miss falls back to rather than
	// dropping straight to startedRun, which carries no TaskRuns at all.
	priorSnapshotURI string
	// sawTerminal records that the run published its own terminal lifecycle
	// event. closeRun consults it so an event read can decide "no synthetic
	// terminal needed" from history alone, without fetching the snapshot the
	// run projection would need.
	sawTerminal bool
	// lastRunStatus is the run-level status of the newest published lifecycle
	// event. It is the degraded answer's status when the referenced snapshot
	// object is gone: history already recorded how far the run got, and a
	// tolerated miss must not report a SUCCEEDED run as still PENDING.
	lastRunStatus orkstackv1.Status
}

// scan walks a run's history once and decodes it.
//
// With send set the scan is a stream: each event after fromSequence is handed
// over as it is decoded and the walk stops at the run's terminal event. With
// send nil the whole projection is collected, and the walk continues past the
// terminal event to the execution's closing event, which is the only place a
// terminated run's outcome is recorded.
//
// wantRun says the caller consumes the run projection, and it alone triggers
// an object-storage fetch of the referenced snapshot. An event read passes
// false: the events are complete in Temporal history, so a missing snapshot
// object — or no configured snapshot store at all — must not destroy an
// answer Temporal already gave. Turning a live run's event read into "not
// found" because one snapshot object was unreadable is exactly the inversion
// LookupRun rules out.
func (r *TemporalRunReader) scan(
	ctx context.Context,
	runID string,
	follow bool,
	fromSequence int64,
	wantRun bool,
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
			event, snapshotURI, err := r.decodePublishEvent(runID, historyEvent)
			if err != nil {
				return nil, err
			}
			if snapshotURI != "" {
				projection.priorSnapshotURI = projection.snapshotURI
				projection.snapshotURI = snapshotURI
			}
			if event == nil {
				continue
			}
			if isTerminalRunEvent(event) {
				projection.sawTerminal = true
			}
			if isRunLevelEvent(event) && event.GetStatus() != orkstackv1.Status_STATUS_UNSPECIFIED {
				projection.lastRunStatus = event.GetStatus()
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
			if wantRun {
				// For a CLOSED run a missing snapshot stays fatal — and stays
				// matching artifacts.ErrNotFound — on purpose: the API layer
				// answers that error from the archive, which holds the finished
				// run's complete body, task runs included. Tolerating the miss
				// here would hand back a partial run and shadow that better
				// answer. The tolerance below is only for runs still executing,
				// which have no archive body yet.
				if err := r.loadSnapshot(ctx, runID, projection); err != nil {
					return nil, err
				}
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
	if wantRun {
		// This load runs for a LIVE run: a closed execution was already handled
		// (and its snapshot loaded) above. A missing snapshot object — an
		// operator lifecycle rule over metadata/snapshots/, or any ordinary
		// object-store 404 — must not erase a run Temporal is actively
		// executing: ErrRunNotFound wraps the same artifacts.ErrNotFound
		// sentinel, so returning the miss would read as "this run does not
		// exist" one layer up, the exact inversion LookupRun rules out. The
		// projection assembled from history still names the run, its orkflow,
		// status and input; only the snapshot overlay is lost. The tolerance
		// lives at this call site rather than inside loadSnapshot because the
		// closed-run site above needs the opposite contract. A transient
		// (non-404) failure stays fatal so it maps to Unavailable, not to a
		// silently degraded answer.
		if err := r.loadSnapshot(ctx, runID, projection); err != nil {
			if !errors.Is(err, artifacts.ErrNotFound) {
				return nil, err
			}
			// The newest snapshot is gone, but every lifecycle event writes its
			// own snapshot before it publishes (workflow.go's emit), so the
			// object written alongside the SECOND-newest event is nearly as
			// complete — in particular it still carries TaskRuns, which
			// startedRun never has at all. One bounded fallback attempt, not a
			// walk further back: the point is to survive one miss on the object
			// an event's own publish just wrote, not to paper over a store that
			// is actually down.
			if prior, priorErr := r.readSnapshotAt(ctx, runID, projection.priorSnapshotURI); priorErr == nil {
				projection.run = mergeRunSnapshot(projection.run, prior)
				projection.snapshotLoaded = true
			}
			// Degrade with what history already recorded: the newest published
			// lifecycle event's run status, so a run that got past PENDING is
			// not reported as if it never started.
			if projection.run != nil && projection.lastRunStatus != orkstackv1.Status_STATUS_UNSPECIFIED {
				projection.run.Status = projection.lastRunStatus
			}
		}
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
	// The run announced its own end either on the loaded run projection or —
	// for an event read that deliberately fetched no snapshot — as a decoded
	// terminal event. Both mean the closing event adds nothing to synthesize.
	if projection.sawTerminal || isTerminal(projection.run.GetStatus()) {
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

// decodePublishEvent decodes the (Event, snapshotURI, dedupeKey) arguments
// the workflow schedules PublishEvent with: the event itself and the URI of
// the run snapshot written alongside it. The Run is never inline in history;
// it is fetched from the snapshot store only when a caller wants it.
func (r *TemporalRunReader) decodePublishEvent(
	runID string,
	historyEvent *historypb.HistoryEvent,
) (*orkstackv1.Event, string, error) {
	payloads := historyEvent.GetActivityTaskScheduledEventAttributes().GetInput().GetPayloads()
	var event *orkstackv1.Event
	if len(payloads) > 0 {
		if err := r.converter.FromPayload(payloads[0], &event); err != nil {
			return nil, "", fmt.Errorf("engine: decode event of run %s at history event %d: %w",
				runID, historyEvent.GetEventId(), err)
		}
	}
	if len(payloads) <= 1 {
		return event, "", nil
	}
	var uri string
	if err := r.converter.FromPayload(payloads[1], &uri); err != nil {
		return nil, "", fmt.Errorf("engine: decode snapshot reference of run %s at history event %d: %w",
			runID, historyEvent.GetEventId(), err)
	}
	return event, uri, nil
}

func (r *TemporalRunReader) loadSnapshot(
	ctx context.Context,
	runID string,
	projection *runProjection,
) error {
	if projection.snapshotLoaded || projection.snapshotURI == "" {
		return nil
	}
	run, err := r.readSnapshotAt(ctx, runID, projection.snapshotURI)
	if err != nil {
		return err
	}
	projection.run = mergeRunSnapshot(projection.run, run)
	projection.snapshotLoaded = true
	return nil
}

// readSnapshotAt fetches and decodes one snapshot object. It is the shared
// primitive behind loadSnapshot's newest-object read and the live-run
// fallback to the prior one; neither call site's own tolerance for a miss
// lives here.
func (r *TemporalRunReader) readSnapshotAt(
	ctx context.Context,
	runID string,
	uri string,
) (*orkstackv1.Run, error) {
	if uri == "" {
		return nil, artifacts.ErrNotFound
	}
	if r.snapshots == nil {
		return nil, fmt.Errorf("engine: read snapshot of run %s: snapshot store is not configured", runID)
	}
	data, err := r.snapshots.ReadSnapshot(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("engine: read snapshot of run %s: %w", runID, err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		return nil, fmt.Errorf("engine: decode snapshot of run %s at %s: %w", runID, uri, err)
	}
	return run, nil
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
	// The plan is the fourth argument and carries the origin decided at
	// submit, so a replica that never saw the submission can say what started
	// the run before its first snapshot exists. A plan that does not decode
	// leaves the origin unset, as an undecodable orkflow leaves the name empty.
	if len(payloads) > 3 {
		var plan RunPlan
		if err := r.converter.FromPayload(payloads[3], &plan); err == nil {
			run.Origin = plan.Origin.Proto()
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
	// A listing row carries the origin's kind and id from the memo; the whole
	// origin comes from GetRun, which reads the snapshot. A memo with no kind
	// (a run started before origins existed) leaves Origin unset.
	if kind, ok := artifacts.ParseOriginKindSlug(r.memoString(info.GetMemo(), MemoOriginKind)); ok {
		run.Origin = &orkstackv1.RunOrigin{Kind: kind, Id: r.memoString(info.GetMemo(), MemoOriginID)}
	}
	return run
}

// memoString reads one memo field; a field the memo does not carry reads as
// an empty string, not an error.
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
	// A run with no origin has kind UNSPECIFIED, which equals no filter value,
	// so it matches no origin filter by construction.
	if q.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && run.GetOrigin().GetKind() != q.OriginKind {
		return false
	}
	if q.OriginID != "" && run.GetOrigin().GetId() != q.OriginID {
		return false
	}
	created := RunIndexTime(run)
	if !q.Since.IsZero() && created.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && !created.Before(q.Until) {
		return false
	}
	if !q.BeforeCreated.IsZero() && !(created.Before(q.BeforeCreated) || (created.Equal(q.BeforeCreated) && run.GetId() < q.BeforeID)) {
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
	if next.GetOrigin() == nil {
		next.Origin = base.GetOrigin()
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

// RunStartRecord is what a run was started with: the planned definition and
// the plan, decoded from its WorkflowExecutionStarted event.
type RunStartRecord struct {
	Orkflow *orkstackv1.Orkflow
	Plan    RunPlan
}

// RunStart reads a run's first history event and decodes its definition and
// plan with the same converter startedRun decodes them with: payloads[1] is
// the Orkflow, payloads[3] the RunPlan. It reads one event and stops. A run
// with no history — unknown, or past retention — is ErrRunNotFound; a payload
// that does not decode is an error naming the run, never an empty record.
//
// It is what lets either await door check a resolution against the PINNED
// definition and verify a callback token against the digest minted at
// submit, on any replica, with no table of pending callbacks anywhere.
func (r *TemporalRunReader) RunStart(ctx context.Context, runID string) (*RunStartRecord, error) {
	if runID == "" {
		return nil, fmt.Errorf("engine: run start: run id is required")
	}
	iter := r.client.GetWorkflowHistory(ctx, runID, "", false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !iter.HasNext() {
		return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
	}
	historyEvent, err := iter.Next()
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
		}
		return nil, fmt.Errorf("engine: read history of run %s: %w", runID, err)
	}
	attrs := historyEvent.GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return nil, fmt.Errorf("engine: run %s: the first history event is %s, not the run's start",
			runID, historyEvent.GetEventType())
	}
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 4 {
		return nil, fmt.Errorf("engine: run %s: its start carries %d arguments, want 4", runID, len(payloads))
	}
	record := &RunStartRecord{}
	if err := r.converter.FromPayload(payloads[1], &record.Orkflow); err != nil || record.Orkflow == nil {
		return nil, fmt.Errorf("engine: run %s: decode the started definition: %v", runID, err)
	}
	if err := r.converter.FromPayload(payloads[3], &record.Plan); err != nil {
		return nil, fmt.Errorf("engine: run %s: decode the started plan: %w", runID, err)
	}
	return record, nil
}

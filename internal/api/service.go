package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunStarter is the seam between the API and Temporal. Tests substitute a fake
// so the Connect surface can be exercised without a cluster.
type RunStarter interface {
	StartRun(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload) error
	CancelRun(ctx context.Context, runID, reason string) error
}

// PluginLister is the seam between the API and a live Orker: ListPlugins
// returns descriptors for every plugin a connected Orker can run. Tests
// substitute a fake; production uses engine.TemporalStarter.
type PluginLister interface {
	ListPlugins(ctx context.Context) ([]plugin.Descriptor, error)
}

// RunArchive is the seam between the API and durable run history. The
// in-process read model only knows the runs this process has seen; the archive
// knows every run that ever finished, which is what makes `run get` and
// `run list` survive an engine restart. Tests substitute a fake.
type RunArchive interface {
	// Archive writes a finished run and its events.
	Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event) error
	// LoadRun returns an archived run, or an error matching
	// artifacts.ErrNotFound when the run was never archived.
	LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
	// LoadRunEvents returns an archived run's events, in sequence order.
	LoadRunEvents(ctx context.Context, runID string) ([]*orkstackv1.Event, error)
	// ListRuns returns archived runs matching q, newest first.
	ListRuns(ctx context.Context, q engine.ArchiveQuery) ([]*orkstackv1.Run, error)
}

// Config wires a Service to its stores and Temporal seams.
type Config struct {
	// Workflows is the durable orkflow store. `ork` wires the S3 store
	// (cmd/ork/main.go); leaving it nil is a unit-test convenience that
	// substitutes the in-memory store, never a fallback for a configured one.
	Workflows engine.OrkflowStore
	Runs      *engine.RunStore
	Bus       *engine.EventBus
	Starter   RunStarter
	Lister    PluginLister

	// Archive is the durable run history. Nil keeps runs in process only.
	Archive RunArchive

	// Plugins is the fallback discovery set for PluginService.ListPlugins
	// when no live Orker lister is configured.
	Plugins []*orkstackv1.PluginInfo

	// Now and NewRunID are injectable for deterministic tests.
	Now      func() time.Time
	NewRunID func() string
}

// Service implements OrkflowService, RunService, and PluginService.
type Service struct {
	workflows engine.OrkflowStore
	runs      *engine.RunStore
	bus       *engine.EventBus
	starter   RunStarter
	lister    PluginLister
	archive   RunArchive
	plugins   []*orkstackv1.PluginInfo
	now       func() time.Time
	newRunID  func() string

	counterMu      sync.Mutex
	idempotencyMu  sync.Mutex
	idempotencyMap map[string]string
	runCounter     int64
}

// NewService builds a Service, filling in defaults for anything omitted.
func NewService(cfg Config) *Service {
	svc := &Service{
		workflows:      cfg.Workflows,
		runs:           cfg.Runs,
		bus:            cfg.Bus,
		starter:        cfg.Starter,
		lister:         cfg.Lister,
		archive:        cfg.Archive,
		plugins:        cfg.Plugins,
		now:            cfg.Now,
		newRunID:       cfg.NewRunID,
		idempotencyMap: map[string]string{},
	}
	// Only an unconfigured Service gets the in-memory store, and only so a
	// unit test can exercise runs without a bucket. A configured store is
	// never bypassed: a failing durable store surfaces as an error.
	if svc.workflows == nil {
		svc.workflows = engine.NewWorkflowStore()
	}
	if svc.runs == nil {
		svc.runs = engine.NewRunStore()
	}
	if svc.bus == nil {
		svc.bus = engine.NewEventBus()
	}
	if svc.now == nil {
		svc.now = time.Now
	}
	if svc.newRunID == nil {
		svc.newRunID = svc.defaultRunID
	}
	return svc
}

// defaultRunID derives a run id from the wall clock and a process counter. It
// runs in the API process, never inside workflow code.
func (s *Service) defaultRunID() string {
	s.counterMu.Lock()
	s.runCounter++
	counter := s.runCounter
	s.counterMu.Unlock()
	return fmt.Sprintf("ork-%d-%d", s.now().UTC().UnixNano(), counter)
}

// Bus exposes the event bus (used by the WebSocket bridge).
func (s *Service) Bus() *engine.EventBus { return s.bus }

// Runs exposes the run read model (used by the WebSocket bridge).
func (s *Service) Runs() *engine.RunStore { return s.runs }

// ApplyOrkflow compiles a DSL document and writes the resulting orkflow to the
// durable store, so it outlives this process.
func (s *Service) ApplyOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.ApplyOrkflowRequest],
) (*connect.Response[orkstackv1.ApplyOrkflowResponse], error) {
	wf, err := dsl.Compile([]byte(req.Msg.GetYaml()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	wf.CreatedAt = timestamppb.New(s.now().UTC())
	if err := s.workflows.Put(ctx, wf); err != nil {
		return nil, orkflowStoreError(err, "store orkflow %s@%s", wf.GetName(), wf.GetVersion())
	}
	return connect.NewResponse(&orkstackv1.ApplyOrkflowResponse{Orkflow: wf}), nil
}

// GetOrkflow reads a stored orkflow; an empty version means the most recently
// applied one.
func (s *Service) GetOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetOrkflowRequest],
) (*connect.Response[orkstackv1.GetOrkflowResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	wf, err := s.workflows.Get(ctx, req.Msg.GetName(), req.Msg.GetVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q",
			req.Msg.GetName(), req.Msg.GetVersion())
	}
	return connect.NewResponse(&orkstackv1.GetOrkflowResponse{Orkflow: wf}), nil
}

// ListOrkflows returns the latest version of every stored orkflow, read from
// the durable store on every call.
func (s *Service) ListOrkflows(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowsRequest],
) (*connect.Response[orkstackv1.ListOrkflowsResponse], error) {
	workflows, err := s.workflows.List(ctx)
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows")
	}
	if size := int(req.Msg.GetPageSize()); size > 0 && size < len(workflows) {
		workflows = workflows[:size]
	}
	return connect.NewResponse(&orkstackv1.ListOrkflowsResponse{Orkflows: workflows}), nil
}

// orkflowStoreError maps a store failure onto a Connect code. A missing
// orkflow is NotFound. Anything else means the durable store could not be
// reached: it is reported as Unavailable, never answered from memory.
func orkflowStoreError(err error, format string, args ...any) error {
	subject := fmt.Sprintf(format, args...)
	if errors.Is(err, artifacts.ErrNotFound) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s not found", subject))
	}
	if errors.Is(err, artifacts.ErrInvalidOrkflow) {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", subject, err))
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", subject, err))
}

// SubmitRun creates a pending run and starts its Temporal workflow.
func (s *Service) SubmitRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.SubmitRunRequest],
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	name := req.Msg.GetWorkflowName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("workflow_name is required"))
	}
	wf, err := s.workflows.Get(ctx, name, req.Msg.GetWorkflowVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q", name, req.Msg.GetWorkflowVersion())
	}
	if s.starter == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no run starter configured"))
	}

	mergedInput := engine.ApplyInputDefaults(wf, req.Msg.GetInput())
	key := req.Msg.GetIdempotencyKey()

	// A keyed submission reserves its key together with the pending run before
	// any event publication or Temporal call. This makes concurrent requests
	// converge on one run and keeps a failed start deduplicated to its recorded
	// failed run.
	if key != "" {
		s.idempotencyMu.Lock()
		if existingID, seen := s.idempotencyMap[key]; seen {
			existing, ok := s.runs.Get(existingID)
			s.idempotencyMu.Unlock()
			if ok {
				return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: existing}), nil
			}
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("idempotency key %q refers to missing run %q", key, existingID))
		}
		runID := s.newRunID()
		run := s.newPendingRun(runID, wf, mergedInput)
		s.runs.Save(run)
		s.idempotencyMap[key] = runID
		s.idempotencyMu.Unlock()
		return s.startPreparedRun(ctx, run, wf, mergedInput)
	}

	runID := s.newRunID()
	run := s.newPendingRun(runID, wf, mergedInput)
	s.runs.Save(run)
	return s.startPreparedRun(ctx, run, wf, mergedInput)
}

func (s *Service) newPendingRun(
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:              runID,
		WorkflowName:    wf.GetName(),
		WorkflowVersion: wf.GetVersion(),
		Status:          orkstackv1.Status_STATUS_PENDING,
		Input:           input,
		CreatedAt:       timestamppb.New(s.now().UTC()),
	}
}

func (s *Service) startPreparedRun(
	ctx context.Context,
	run *orkstackv1.Run,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	runID := run.GetId()
	s.bus.Publish(&orkstackv1.Event{
		RunId:     runID,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:    orkstackv1.Status_STATUS_PENDING,
		Message:   fmt.Sprintf("run created for orkflow %s@%s", wf.GetName(), wf.GetVersion()),
		Timestamp: timestamppb.New(s.now().UTC()),
	}, runID+"#created")

	if err := s.starter.StartRun(ctx, runID, wf, input); err != nil {
		failedAt := timestamppb.New(s.now().UTC())
		run.Status = orkstackv1.Status_STATUS_FAILED
		run.Error = &orkstackv1.Error{Code: "RUN_START_FAILED", Message: err.Error()}
		run.FinishedAt = failedAt
		s.runs.Save(run)
		s.bus.Publish(&orkstackv1.Event{
			RunId:     runID,
			Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
			Status:    orkstackv1.Status_STATUS_FAILED,
			Message:   err.Error(),
			Timestamp: failedAt,
		}, runID+"#failed")
		// A run that never reached Temporal still ends here, so this is the
		// only place that can archive it. The engine's write-through covers
		// every run that did start.
		s.archiveRun(ctx, runID)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	stored, _ := s.runs.Get(runID)
	return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: stored}), nil
}

// archiveRun writes a terminal run to the archive. Archiving is durability, not
// orchestration: a failure here must not turn a recorded outcome into a
// different error for the caller, so it is reported through the run's own
// error and otherwise dropped.
func (s *Service) archiveRun(ctx context.Context, runID string) {
	if s.archive == nil {
		return
	}
	run, ok := s.runs.Get(runID)
	if !ok {
		return
	}
	_ = s.archive.Archive(ctx, run, s.bus.Events(runID, 0))
}

// ArchivedEvents returns the events archived for a run, after fromSequence. It
// is the read path for a run this process never executed, and it reports the
// same Connect errors the live path would: NotFound when nothing was archived
// under that id, Internal when the archive itself is unreachable.
func (s *Service) ArchivedEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	if s.archive == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	}
	events, err := s.archive.LoadRunEvents(ctx, runID)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("read run %q from archive: %w", runID, err))
	}
	after := make([]*orkstackv1.Event, 0, len(events))
	for _, event := range events {
		if event.GetSequence() > fromSequence {
			after = append(after, event)
		}
	}
	return after, nil
}

// GetRun returns a run: the live read model first, because a running run's
// snapshot is fresher there, then the archive, which still holds every run this
// process never saw — including everything from before the last restart.
func (s *Service) GetRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	runID := req.Msg.GetRunId()
	if runID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	if run, ok := s.runs.Get(runID); ok {
		return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
	}
	if s.archive != nil {
		run, err := s.archive.LoadRun(ctx, runID)
		switch {
		case err == nil:
			return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
		case !errors.Is(err, artifacts.ErrNotFound):
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("read run %q from archive: %w", runID, err))
		}
	}
	return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
}

// ListRuns returns runs, newest first, with optional filters. Results merge the
// live read model with the archive, so a query sees runs still executing and
// runs that finished before this process started. A run present in both is
// reported from the live model, which is at least as fresh as the archive.
func (s *Service) ListRuns(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListRunsRequest],
) (*connect.Response[orkstackv1.ListRunsResponse], error) {
	size := int(req.Msg.GetPageSize())
	runs := s.runs.List(engine.ListFilter{
		WorkflowName: req.Msg.GetWorkflowName(),
		Status:       req.Msg.GetStatus(),
	})
	if s.archive != nil {
		archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{
			WorkflowName: req.Msg.GetWorkflowName(),
			Status:       req.Msg.GetStatus(),
			Limit:        size,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("list runs from archive: %w", err))
		}
		runs = mergeRuns(runs, archived)
	}
	if size > 0 && size < len(runs) {
		runs = runs[:size]
	}
	return connect.NewResponse(&orkstackv1.ListRunsResponse{Runs: runs}), nil
}

// mergeRuns folds archived runs into the live ones, newest first. Live wins on
// a duplicate id, and equal timestamps fall back to the id so the order of a
// page never depends on map iteration.
func mergeRuns(live, archived []*orkstackv1.Run) []*orkstackv1.Run {
	merged := make([]*orkstackv1.Run, 0, len(live)+len(archived))
	seen := make(map[string]struct{}, len(live))
	for _, run := range live {
		seen[run.GetId()] = struct{}{}
		merged = append(merged, run)
	}
	for _, run := range archived {
		if _, duplicate := seen[run.GetId()]; duplicate {
			continue
		}
		merged = append(merged, run)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		a, b := engine.RunIndexTime(merged[i]), engine.RunIndexTime(merged[j])
		if !a.Equal(b) {
			return a.After(b)
		}
		return merged[i].GetId() > merged[j].GetId()
	})
	return merged
}

// CancelRun requests cancellation of a run's Temporal workflow.
func (s *Service) CancelRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.CancelRunRequest],
) (*connect.Response[orkstackv1.CancelRunResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, ok := s.runs.Get(req.Msg.GetRunId())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", req.Msg.GetRunId()))
	}
	if s.starter == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no run starter configured"))
	}
	if err := s.starter.CancelRun(ctx, req.Msg.GetRunId(), req.Msg.GetReason()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orkstackv1.CancelRunResponse{Run: run}), nil
}

// StreamEvents server-streams a run's events: the backlog after from_sequence
// followed by live events, ending when the run reaches a terminal event.
//
// A run this process never saw is replayed from the archive instead. It is
// finished by definition — nothing archives before its terminal event — so the
// replay is the whole stream and the call returns when it is sent.
func (s *Service) StreamEvents(
	ctx context.Context,
	req *connect.Request[orkstackv1.StreamEventsRequest],
	stream *connect.ServerStream[orkstackv1.Event],
) error {
	runID := req.Msg.GetRunId()
	if runID == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	if _, ok := s.runs.Get(runID); !ok {
		archived, err := s.ArchivedEvents(ctx, runID, req.Msg.GetFromSequence())
		if err != nil {
			return err
		}
		for _, event := range archived {
			if err := stream.Send(event); err != nil {
				return err
			}
		}
		return nil
	}

	events, cancel, terminalAlreadyReached := s.bus.Subscribe(runID, req.Msg.GetFromSequence())
	defer cancel()
	if terminalAlreadyReached {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, open := <-events:
			if !open {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
			if IsTerminalEvent(event) {
				return nil
			}
		}
	}
}

// IsTerminalEvent reports whether an event ends a run's stream.
func IsTerminalEvent(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	default:
		return false
	}
}

// ListPlugins returns the plugins a live Orker can run, discovered through the
// PluginLister seam (an ExecuteTask-queue activity). When no lister is wired
// (tests, or no Orker), it falls back to the configured static set.
func (s *Service) ListPlugins(
	ctx context.Context,
	_ *connect.Request[orkstackv1.ListPluginsRequest],
) (*connect.Response[orkstackv1.ListPluginsResponse], error) {
	plugins := append([]*orkstackv1.PluginInfo(nil), s.plugins...)
	if s.lister != nil {
		descriptors, err := s.lister.ListPlugins(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("discover plugins: %w", err))
		}
		plugins = plugins[:0]
		for _, d := range descriptors {
			plugins = append(plugins, &orkstackv1.PluginInfo{
				Name:        d.Name,
				Version:     d.Version,
				Description: d.Description,
			})
		}
	}
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].GetName() < plugins[j].GetName() })
	return connect.NewResponse(&orkstackv1.ListPluginsResponse{Plugins: plugins}), nil
}

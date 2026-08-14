package api

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// RunStarter is the seam between the API and Temporal. Tests substitute a fake
// so the Connect surface can be exercised without a cluster.
type RunStarter interface {
	StartRun(ctx context.Context, runID string, wf *orkstackv1.Workflow, input *orkstackv1.Payload) error
	CancelRun(ctx context.Context, runID, reason string) error
}

// Config wires a Service to its stores and Temporal seam.
type Config struct {
	Workflows *engine.WorkflowStore
	Runs      *engine.RunStore
	Bus       *engine.EventBus
	Starter   RunStarter

	// Plugins is the discovery set returned by PluginService.ListPlugins.
	Plugins []*orkstackv1.PluginInfo

	// Now and NewRunID are injectable for deterministic tests.
	Now      func() time.Time
	NewRunID func() string
}

// Service implements WorkflowService, RunService, and PluginService.
type Service struct {
	workflows *engine.WorkflowStore
	runs      *engine.RunStore
	bus       *engine.EventBus
	starter   RunStarter
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
		plugins:        cfg.Plugins,
		now:            cfg.Now,
		newRunID:       cfg.NewRunID,
		idempotencyMap: map[string]string{},
	}
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
	return fmt.Sprintf("run-%d-%d", s.now().UTC().UnixNano(), counter)
}

// Bus exposes the event bus (used by the WebSocket bridge).
func (s *Service) Bus() *engine.EventBus { return s.bus }

// Runs exposes the run read model (used by the WebSocket bridge).
func (s *Service) Runs() *engine.RunStore { return s.runs }

// Apply compiles a DSL document and upserts the resulting workflow.
func (s *Service) Apply(
	_ context.Context,
	req *connect.Request[orkstackv1.ApplyWorkflowRequest],
) (*connect.Response[orkstackv1.ApplyWorkflowResponse], error) {
	wf, err := dsl.Compile([]byte(req.Msg.GetYaml()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	wf.CreatedAt = timestamppb.New(s.now().UTC())
	s.workflows.Put(wf)
	return connect.NewResponse(&orkstackv1.ApplyWorkflowResponse{Workflow: wf}), nil
}

// GetWorkflow returns a stored workflow; an empty version means the most
// recently applied one.
func (s *Service) GetWorkflow(
	_ context.Context,
	req *connect.Request[orkstackv1.GetWorkflowRequest],
) (*connect.Response[orkstackv1.GetWorkflowResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	wf, ok := s.workflows.Get(req.Msg.GetName(), req.Msg.GetVersion())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("workflow %q version %q not found", req.Msg.GetName(), req.Msg.GetVersion()))
	}
	return connect.NewResponse(&orkstackv1.GetWorkflowResponse{Workflow: wf}), nil
}

// ListWorkflows returns the latest version of every stored workflow.
func (s *Service) ListWorkflows(
	_ context.Context,
	req *connect.Request[orkstackv1.ListWorkflowsRequest],
) (*connect.Response[orkstackv1.ListWorkflowsResponse], error) {
	workflows := s.workflows.List()
	if size := int(req.Msg.GetPageSize()); size > 0 && size < len(workflows) {
		workflows = workflows[:size]
	}
	return connect.NewResponse(&orkstackv1.ListWorkflowsResponse{Workflows: workflows}), nil
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
	wf, ok := s.workflows.Get(name, req.Msg.GetWorkflowVersion())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("workflow %q version %q not found", name, req.Msg.GetWorkflowVersion()))
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
	wf *orkstackv1.Workflow,
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
	wf *orkstackv1.Workflow,
	input *orkstackv1.Payload,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	runID := run.GetId()
	s.bus.Publish(&orkstackv1.Event{
		RunId:     runID,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:    orkstackv1.Status_STATUS_PENDING,
		Message:   fmt.Sprintf("run created for workflow %s@%s", wf.GetName(), wf.GetVersion()),
		Timestamp: timestamppb.New(s.now().UTC()),
	}, runID+"#created")

	if err := s.starter.StartRun(ctx, runID, wf, input); err != nil {
		run.Status = orkstackv1.Status_STATUS_FAILED
		run.Error = &orkstackv1.Error{Code: "RUN_START_FAILED", Message: err.Error()}
		run.FinishedAt = timestamppb.New(s.now().UTC())
		s.runs.Save(run)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	stored, _ := s.runs.Get(runID)
	return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: stored}), nil
}

// GetRun returns a run from the read model.
func (s *Service) GetRun(
	_ context.Context,
	req *connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, ok := s.runs.Get(req.Msg.GetRunId())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", req.Msg.GetRunId()))
	}
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
}

// ListRuns returns runs, newest first, with optional filters.
func (s *Service) ListRuns(
	_ context.Context,
	req *connect.Request[orkstackv1.ListRunsRequest],
) (*connect.Response[orkstackv1.ListRunsResponse], error) {
	runs := s.runs.List(engine.ListFilter{
		WorkflowName: req.Msg.GetWorkflowName(),
		Status:       req.Msg.GetStatus(),
	})
	if size := int(req.Msg.GetPageSize()); size > 0 && size < len(runs) {
		runs = runs[:size]
	}
	return connect.NewResponse(&orkstackv1.ListRunsResponse{Runs: runs}), nil
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
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
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

// ListPlugins returns the plugins the engine knows about. M2 has no live Orker
// registry, so this is the configured (possibly empty) set.
func (s *Service) ListPlugins(
	_ context.Context,
	_ *connect.Request[orkstackv1.ListPluginsRequest],
) (*connect.Response[orkstackv1.ListPluginsResponse], error) {
	plugins := append([]*orkstackv1.PluginInfo(nil), s.plugins...)
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].GetName() < plugins[j].GetName() })
	return connect.NewResponse(&orkstackv1.ListPluginsResponse{Plugins: plugins}), nil
}

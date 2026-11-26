package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// handoverMost is how many sources a replica hands over at once.
	handoverMost = 10
	// handoverSlack is how long past a stop's longest a told source is watched.
	handoverSlack = 30 * time.Second
	// handoverClosed is how long after it closed the index still names a
	// workflow that may be between two executions.
	handoverClosed = 10 * time.Minute
	// blockedTries is how often a task of a workflow is tried before its
	// execution is one that cannot run.
	blockedTries = 3
	// eventSourceProfile is the service profile an event source's image is of.
	eventSourceProfile = "event-source"
)

// Why a walk told nobody: what it cannot tell of a workflow with no document
// it counts as taken.
const (
	WithheldIndex    = "index"
	WithheldDescribe = "describe"
	WithheldCount    = "count"
)

const (
	// PassHandover is a source that was told to hand over, and
	// PassHandoverWithheld a walk that told nobody.
	PassHandover         PassDid = "handover"
	PassHandoverWithheld PassDid = "handover_withheld"
)

// SourceIndexed is a source's workflow as the cluster's index names it.
type SourceIndexed struct {
	ID   string
	Open bool
}

// SourceHandover moves sources to the current release as its source hosts
// have room: their workflows are pinned, so each ends itself on the release it
// is on and is started where the deployment sends new work.
type SourceHandover struct {
	// BuildID is this engine's release.
	BuildID  string
	Releases interface {
		ObserveReleases(ctx context.Context) (ReleaseObservation, error)
	}
	Fleet FleetReader
	// Index names the source workflows that are open, or closed since.
	Index func(ctx context.Context, since time.Time) ([]SourceIndexed, error)
	// Spec reads a service spec.
	Spec func(ctx context.Context, id, digest string) (body []byte, found bool, err error)
	// MaxSources bounds how many workflows with no document a walk describes.
	MaxSources int
	// Withheld is told why a walk told nobody, with the ids it could not
	// see; may be nil.
	Withheld func(cause string, ids []string)
}

// seenSource is a source as the walk saw it.
type seenSource struct {
	eventsource.HandoverSource
	src        *orkstackv1.EventSource
	e          *sourceExecution
	carry      eventsource.Carry
	unreadable bool
}

// see keeps what a hand-over's count needs of a source.
func (w *passWalk) see(ctx context.Context, src *orkstackv1.EventSource, e *sourceExecution, failed error) {
	if w.Handover == nil {
		return
	}
	s := seenSource{src: src, e: e, HandoverSource: eventsource.HandoverSource{ID: src.GetId(), Unseen: failed != nil}}
	ns, err := w.namespace(ctx, src.GetNamespace())
	doc := eventsources.Effective(src, eventsources.Limits{})
	if err == nil {
		_, s.Pool, err = sourcePool(doc.GetPool(), ns, w.Starter.DefaultQueue)
	}
	if err != nil {
		w.blind, w.blindErr = append(w.blind, src.GetId()), err
		return
	}
	wants := src.GetPause() == nil && src.GetDeletingSince() == nil && ns.GetHold() == nil
	if failed == nil {
		if e != nil && len(e.carry) > 0 {
			s.carry, err = eventsource.DecodeCarry(e.carry)
			s.unreadable = err != nil
		}
		s.Open = e.open()
		switch {
		case !wants:
		case s.Open:
			s.Wants = int(max(e.now.Wanted, e.now.Source.Instances, 0))
		case s.carry.Loaded != nil:
			s.Wants = int(max(s.carry.Loaded.Instances, 0))
		default:
			s.Wants = int(doc.GetInstances())
		}
		if s.Open {
			s.Release, s.TaskPending, s.Blocked = e.release, e.task, e.taskTries >= blockedTries
			for _, slot := range e.now.Slots {
				if slot.Replacement != nil {
					s.Replacements++
				}
			}
			for _, a := range e.pending {
				s.Activities = append(s.Activities, eventsource.HandoverActivity{Orker: a.GetLastWorkerIdentity(),
					Running: a.GetState() != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED})
			}
		}
	}
	w.seen = append(w.seen, s)
}

// handOver is the walk's last row: it counts, for each pool, what the current
// release's slots are spoken for, from this walk's describes, and tells the
// sources that fit in the rest.
func (w *passWalk) handOver(ctx context.Context) error {
	h := w.Handover
	if h == nil || !w.Starter.Runs {
		return nil
	}
	obs, err := h.Releases.ObserveReleases(ctx)
	if errors.Is(err, ErrNotVersioned) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("the releases: %w", err)
	}
	if !h.mayTell(obs) || !slices.ContainsFunc(w.seen, func(s seenSource) bool { return s.Open && s.Release != h.BuildID }) {
		return nil
	}
	orphans, err := w.orphans(ctx)
	if err != nil || ctx.Err() != nil {
		return err
	}
	if orphans == nil {
		return nil
	}
	// Last of what is read: what it tells acts on reads that are seconds old.
	if obs, err = h.Releases.ObserveReleases(ctx); err != nil {
		return fmt.Errorf("the releases: %w", err)
	}
	if !h.mayTell(obs) {
		return nil
	}
	fleet, err := ReadFleet(ctx, h.Fleet)
	if err != nil {
		return fmt.Errorf("the Orkers: %w", err)
	}
	view := eventsource.HandoverView{Current: h.BuildID, Routed: map[string]bool{h.BuildID: true}, Orphans: *orphans}
	var usable []FleetOrker
	for _, o := range fleet {
		host, ok := w.sourceHost(o)
		if !ok {
			continue
		}
		view.Hosts = append(view.Hosts, host)
		if host.Usable && host.Release == h.BuildID {
			usable = append(usable, o)
		}
	}
	byID := map[string]seenSource{}
	for _, s := range w.seen {
		if s.Open && s.Release != h.BuildID {
			s.Blocked = s.Blocked || obs.EngineReplicas[s.Release] == 0
			why, err := w.cannotCarry(ctx, s, usable)
			if err != nil {
				w.Logger.WarnContext(ctx, "whether this release can carry an event source is not known; it is not handed over in this walk", "event_source_id", s.ID, "error", err)
				s.Unseen = true
			}
			s.CannotCarry = why != ""
		}
		byID[s.ID] = s
		view.Sources = append(view.Sources, s.HandoverSource)
	}
	for _, id := range eventsource.PlanHandover(view, handoverMost-int(w.watching.Load())).Tell {
		s := byID[id]
		w.did(PassHandover)
		w.Logger.InfoContext(ctx, "an event source is handed over to the current release", "event_source_id", id, "from", s.Release, "to", h.BuildID)
		if err := w.Starter.signal(ctx, id, s.e.runID, SignalSourceHandover, nil); err != nil {
			return err
		}
		w.watch(ctx, s)
	}
	return nil
}

// mayTell reports whether this engine's release is the one new work is sent
// to, and no other is being ramped: whose room nobody counted.
func (h *SourceHandover) mayTell(obs ReleaseObservation) bool {
	return obs.Routing.Current == h.BuildID && h.BuildID != "" && (obs.Routing.Ramping == "" || obs.Routing.RampPercent <= 0)
}

// sourceHost is one Orker as a source host; one that does not answer is one
// nobody can use.
func (w *passWalk) sourceHost(o FleetOrker) (eventsource.HandoverHost, bool) {
	d := o.Described
	if o.Err != nil || d.GetServiceHost().GetMaxInstances() <= 0 {
		return eventsource.HandoverHost{}, false
	}
	queue, err := task.QueueForSource(d.GetPool(), w.Starter.DefaultQueue)
	if err != nil {
		return eventsource.HandoverHost{}, false
	}
	pool, named := task.PoolForSourceQueue(queue)
	if !named {
		pool = task.DefaultPool
	}
	s := d.GetServiceHost()
	return eventsource.HandoverHost{Orker: o.Presence.Identity, Pool: pool, Release: d.GetBuildId(), Usable: s.GetFit() && !d.GetCordoned(),
		Slots: int(s.GetMaxInstances()), Free: int(s.GetMaxInstances() - s.GetInstances())}, true
}

// cannotCarry is why this release cannot take the source: "" where it can. A
// source that is being deleted runs no container here.
func (w *passWalk) cannotCarry(ctx context.Context, s seenSource, usable []FleetOrker) (string, error) {
	if s.unreadable {
		return "its carry", nil
	}
	if s.src.GetDeletingSince() != nil {
		return "", nil
	}
	runs := s.carry.Good
	if runs == nil {
		runs = s.carry.Target
	}
	if runs == nil {
		return "", nil
	}
	body, found, err := w.Handover.Spec(ctx, s.ID, runs.Digest)
	if err != nil && !errors.Is(err, eventsources.ErrSpecCorrupt) {
		return "", err
	}
	if err != nil || !found {
		return "its service spec", nil
	}
	spec, err := eventsources.ReadServiceSpec(body)
	if err != nil {
		return "its service spec", nil
	}
	for _, o := range usable {
		host, _ := w.sourceHost(o)
		if host.Pool == s.Pool && hostsImage(o.Described.GetServiceHost(), spec) {
			return "", nil
		}
	}
	return "its image", nil
}

// hostsImage reports whether the host has a contract and a profile version
// the spec's image implements.
func hostsImage(h *orkstackv1.ServiceHost, spec eventsources.ServiceSpec) bool {
	in := func(have, want []int32) bool {
		return slices.ContainsFunc(want, func(v int32) bool { return slices.Contains(have, v) })
	}
	return in(h.GetContractVersions(), spec.Contracts) && slices.ContainsFunc(h.GetProfiles(), func(p *orkstackv1.ServiceProfileVersions) bool {
		return p.GetName() == eventSourceProfile && in(p.GetVersions(), spec.Profiles)
	})
}

// orphans are the open source workflows that no document of the walk accounts
// for; nil where they cannot be told, and nobody is told anything.
func (w *passWalk) orphans(ctx context.Context) (*[]eventsource.HandoverOrphan, error) {
	h := w.Handover
	withhold := func(cause string, ids []string, err error) (*[]eventsource.HandoverOrphan, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		w.did(PassHandoverWithheld)
		w.Logger.ErrorContext(ctx, "no event source is handed over: what the pools' slots are spoken for cannot be told", "cause", cause, "event_source_ids", ids, "error", err)
		if h.Withheld != nil {
			h.Withheld(cause, ids)
		}
		return nil, nil
	}
	if len(w.blind) > 0 {
		return withhold(WithheldDescribe, w.blind, w.blindErr)
	}
	known := map[string]bool{}
	for _, s := range w.seen {
		known[s.ID] = true
	}
	indexed, err := h.Index(ctx, time.Now().Add(-handoverClosed))
	if err != nil {
		return withhold(WithheldIndex, nil, err)
	}
	var ids []string
	for _, i := range indexed {
		if known[i.ID] {
			continue
		}
		known[i.ID] = true
		if !eventsources.ValidID(i.ID) {
			// No source's: whoever started it holds no slot of one.
			if id := eventsources.SafeText(i.ID, maxReportedReason); w.sayOnce(id) {
				w.Logger.WarnContext(ctx, "a workflow of an event source's type has an id that is no source's; it is passed over", "workflow_id", id)
			}
			continue
		}
		if !i.Open {
			switch gone, err := w.Starter.Tombstoned(ctx, i.ID); {
			case err != nil:
				return withhold(WithheldDescribe, []string{i.ID}, err)
			case gone:
				continue
			}
		}
		ids = append(ids, i.ID)
	}
	if len(ids) > h.MaxSources {
		return withhold(WithheldCount, []string{fmt.Sprintf("%d past %d", len(ids)-h.MaxSources, h.MaxSources)}, nil)
	}
	out := []eventsource.HandoverOrphan{}
	for _, id := range ids {
		began := time.Now()
		e, err := w.Starter.describe(ctx, id)
		if err != nil {
			return withhold(WithheldDescribe, []string{id}, err)
		}
		if e.open() {
			out = append(out, eventsource.HandoverOrphan{Pool: e.now.Source.Pool, Instances: max(int(e.now.Source.Instances), 0)})
		}
		if sleep(ctx, time.Until(began.Add(passSpacing))); ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return &out, nil
}

// watch looks at a source that was told once a second, for as long as its stop
// can take, and starts it as soon as it has closed. One that has not closed by
// then is a walk's to start.
func (w *passWalk) watch(ctx context.Context, s seenSource) {
	w.watching.Add(1)
	w.watchers.Go(func() {
		defer w.watching.Add(-1)
		said := false
		until := time.Now().Add(s.carry.Pinned.CancelWait + handoverSlack)
		for time.Now().Before(until) {
			if sleep(ctx, time.Second); ctx.Err() != nil {
				return
			}
			e, err := w.Starter.describe(ctx, s.ID)
			if err == nil && e.open() && e.runID == s.e.runID {
				continue
			}
			// The start goes by the document as the walk read it: the execution
			// it makes loads it again.
			var got SourceStart
			if err == nil {
				got, err = w.Starter.Start(ctx, s.src)
			}
			if err == nil && got != SourceUnanswered {
				if got == SourceStarted {
					w.did(PassStarted)
				}
				return
			}
			if said {
				continue
			}
			said = true
			w.Logger.WarnContext(ctx, "an event source that was handed over could not be started yet; it is tried each second, and then by the next walk", "event_source_id", s.ID, "outcome", string(got), "error", err)
		}
	})
}

// sourceIndexClient is the cluster's index.
type sourceIndexClient interface {
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
}

// SourceIndex reads the source workflows that are open, or closed since, from
// the cluster's index. It decides nothing but what a hand-over counts.
func SourceIndex(c sourceIndexClient, namespace string) func(ctx context.Context, since time.Time) ([]SourceIndexed, error) {
	return func(ctx context.Context, since time.Time) ([]SourceIndexed, error) {
		query := fmt.Sprintf("WorkflowType = '%s' AND (ExecutionStatus = 'Running' OR CloseTime >= '%s')", SourceWorkflowType, since.UTC().Format(time.RFC3339Nano))
		var out []SourceIndexed
		var token []byte
		for {
			resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: namespace, PageSize: 1000, NextPageToken: token, Query: query})
			if err != nil {
				return nil, fmt.Errorf("engine: list event sources' workflows: %w", err)
			}
			for _, info := range resp.GetExecutions() {
				out = append(out, SourceIndexed{ID: info.GetExecution().GetWorkflowId(), Open: info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING})
			}
			if token = resp.GetNextPageToken(); len(token) == 0 {
				return out, nil
			}
		}
	}
}

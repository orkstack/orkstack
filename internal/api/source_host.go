package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// heldCall is one call of EventSourceHostService whose caller holds the
// instance it names.
type heldCall struct {
	result held.Result
	// src is the source's document as this call read it.
	src *orkstackv1.EventSource
	ref held.Ref
}

// SourceHosts is what the host service reads and writes.
type SourceHosts struct {
	// Reader is this replica's one reader of the held check.
	Reader    *held.Reader
	Instances *artifacts.EventSourceInstanceStore
	Claims    *artifacts.MessageClaimStore
	// Parts is what Orkers staged for messages.
	Parts *artifacts.EventSourceRecordStore
	// Artifacts is where runs read what was staged for them.
	Artifacts artifacts.Store
}

// ServeSourceHosts gives the service what its EventSourceHostService works
// on; without it there is none. It is called before the service serves.
func (s *Service) ServeSourceHosts(h SourceHosts) error {
	staged, ok := h.Artifacts.(stagedArtifacts)
	if !ok {
		return fmt.Errorf("the artifact store %T copies no staged parts", h.Artifacts)
	}
	s.heldReader, s.sourceInstances, s.messageClaims, s.startProbe, s.stagedParts, s.staged = h.Reader, h.Instances, h.Claims, h.Claims, h.Parts, staged
	return nil
}

// sourceCall is what every call passes, in order: this replica's own guards,
// an Orker's credential, the ceiling, and the held check. It answers one of a
// call that may go on, the engine's answer, and an error for a caller that
// may not call at all.
func (s *Service) sourceCall(ctx context.Context, purpose held.Purpose, inst *orkstackv1.InstanceRef) (*heldCall, *orkstackv1.DoorAnswer, error) {
	arrived := time.Now()
	switch {
	case !s.enforcingAuth() || authn.ModeFrom(ctx) != authn.ModeEnforce:
		// event_sources.allow_asserted is the user-facing service's and
		// opens nothing here.
		return nil, doorRefused(eventsources.ReasonRequireAuthentication), nil
	case !s.eventSourceSettings.Enabled || s.heldReader == nil:
		return nil, doorRefused(eventsources.ReasonDisabled), nil
	case s.system.BuildID == "":
		return nil, doorRefused(eventsources.ReasonRequireVersioning), nil
	}
	caller := authn.FromContext(ctx)
	switch {
	case caller == nil:
		return nil, nil, connect.NewError(connect.CodeUnauthenticated, errors.New("an event source's host calls with an Orker's verified credential"))
	case caller.OrkerID == "":
		return nil, nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s: the credential of %q names no Orker", eventsources.ReasonHostNotPermitted, caller.ID))
	}
	// What names no source is kept, and logged, for nobody.
	if !eventsources.ValidName(inst.GetNamespace()) || !eventsources.ValidName(inst.GetName()) || !eventsources.ValidID(inst.GetEventSourceId()) {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance names no event source"))
	}
	ref := held.Ref{Namespace: inst.GetNamespace(), Name: inst.GetName(), SourceID: inst.GetEventSourceId(),
		Index: inst.GetIndex(), Epoch: inst.GetEpoch(), Attempt: inst.GetAttempt(), ServiceDigest: inst.GetServiceDigest()}
	if (purpose == held.PurposeOffer || purpose == held.PurposeStart) && !s.sourceCeiling.allow(caller.OrkerID, ref.SourceID) {
		return nil, nil, connect.NewError(connect.CodeResourceExhausted, errors.New("more messages of this source than its rate and instances allow; ask again"))
	}

	result := held.Instance(ctx, s.heldReader, purpose, ref, held.Caller{OrkerID: caller.OrkerID, Subject: subjectOf(caller)}, arrived)
	log := s.logger.With("purpose", purpose.String(), "namespace", ref.Namespace, "event_source", ref.Name, "event_source_id", ref.SourceID,
		"index", ref.Index, "epoch", ref.Epoch, "attempt", ref.Attempt, "orker_id", caller.OrkerID)
	switch result.Kind {
	case held.Held:
		src, ok := result.Document.Source.(*orkstackv1.EventSource)
		if !ok || src == nil {
			log.ErrorContext(ctx, "an event source's document is not one")
			return nil, doorRefused(eventsources.ReasonDocumentUnreadable), nil
		}
		s.sourceCeiling.learn(caller.OrkerID, src)
		return &heldCall{result: result, src: src, ref: ref}, nil, nil
	case held.NotHeld:
		log.InfoContext(ctx, "an Orker called for an instance it does not hold", "why", result.Which)
		return nil, doorStop(eventsources.ReasonNotHeld), nil
	case held.NotPermitted:
		log.ErrorContext(ctx, "an Orker that hosts an event source's instance lacks a permission at its pool", "permission", result.Permission, "pool", result.Pool)
		return nil, nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s: Orker %q lacks %s at pool %q",
			eventsources.ReasonHostNotPermitted, caller.OrkerID, result.Permission, result.Pool))
	case held.Tombstoned:
		return nil, doorStop(eventsources.ReasonDeletedDocumentReappeared), nil
	case held.PausedOrMarked:
		if result.Which == held.Marked {
			return nil, doorStop(eventsources.ReasonDeleting), nil
		}
		return nil, doorStop(eventsources.ReasonPaused), nil
	case held.OtherID, held.NoDocument:
		log.WarnContext(ctx, "this engine finds no document of an event source that runs")
		return nil, doorRefused(eventsources.ReasonDocumentMissing), nil
	}
	// Unanswered, and any kind a later check adds: never a refusal.
	if result.Undecodable {
		log.WarnContext(ctx, "this engine cannot decode an event source's document", "error", result.Err)
		return nil, doorRefused(eventsources.ReasonDocumentUnreadable), nil
	}
	log.WarnContext(ctx, "whether an Orker holds an instance could not be read", "read", string(result.Read), "error", result.Err)
	cause := waitOutage
	if errors.Is(result.Err, context.DeadlineExceeded) {
		cause = waitTimeout
	}
	return nil, doorWait("", cause), nil
}

func doorRefused(reason string) *orkstackv1.DoorAnswer {
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFUSED, Reason: reason}
}

func doorStop(reason string) *orkstackv1.DoorAnswer {
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_STOP, Reason: reason}
}

func doorWait(reason, cause string) *orkstackv1.DoorAnswer {
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_WAIT, Reason: reason, Cause: cause}
}

// maxSourceCeilings bounds the ceilings kept: one an Orker, and one for each
// source it holds an instance of.
const maxSourceCeilings = 4096

// sourceCeiling bounds how fast one Orker offers and starts one source's
// messages on this replica: twice what the source's rate and instances allow,
// which an honest one never meets. It stands before the held check, so what a
// request names keeps nothing: a source has a ceiling of its own once the
// Orker was found to hold an instance of it, and until then its calls are
// the Orker's, at the most any source of this deployment may have. An Orker
// spends its own and no other's.
type sourceCeiling struct {
	now  func() time.Time
	most float64

	mu       sync.Mutex
	byCaller map[string]*rate.Limiter
}

func newSourceCeiling(l eventsources.Limits) *sourceCeiling {
	return &sourceCeiling{now: time.Now, most: 2 * float64(max(l.MaxRate, 1)) * float64(max(l.MaxInstances, 1)), byCaller: map[string]*rate.Limiter{}}
}

// ceilingKey is an Orker's own with no source; a source's id holds no slash.
func ceilingKey(orkerID, sourceID string) string { return orkerID + "/" + sourceID }

func (c *sourceCeiling) allow(orkerID, sourceID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	lim, ok := c.byCaller[ceilingKey(orkerID, sourceID)]
	if !ok {
		lim = c.limiterLocked(ceilingKey(orkerID, ""), c.most)
	}
	return lim.AllowN(c.now(), 1)
}

// learn sets the ceiling of a source the Orker was found to hold, from its
// document.
func (c *sourceCeiling) learn(orkerID string, src *orkstackv1.EventSource) {
	perSecond := src.GetRate()
	if perSecond == 0 {
		perSecond = eventsources.DefaultRate
	}
	ceiling := min(c.most, 2*float64(perSecond)*float64(max(src.GetInstances(), 1)))
	c.mu.Lock()
	defer c.mu.Unlock()
	lim := c.limiterLocked(ceilingKey(orkerID, src.GetId()), ceiling)
	if float64(lim.Limit()) == ceiling {
		return
	}
	lim.SetLimitAt(c.now(), rate.Limit(ceiling))
	lim.SetBurstAt(c.now(), int(ceiling))
}

// limiterLocked is the limiter of key, made at perSecond. It is called with
// c.mu held.
func (c *sourceCeiling) limiterLocked(key string, perSecond float64) *rate.Limiter {
	lim, ok := c.byCaller[key]
	if !ok {
		if len(c.byCaller) >= maxSourceCeilings {
			clear(c.byCaller)
		}
		lim = rate.NewLimiter(rate.Limit(perSecond), int(perSecond))
		c.byCaller[key] = lim
	}
	return lim
}

// HeldDocument is the held check's read of a source's document, validated
// against the store for each call. A source that names no pool has its
// namespace's default one.
func HeldDocument(store eventsources.Store, namespaces engine.NamespaceReader) func(ctx context.Context, namespace, name string) (held.Document, error) {
	return func(ctx context.Context, namespace, name string) (held.Document, error) {
		src, err := store.Get(ctx, namespace, name)
		if errors.Is(err, eventsources.ErrNotFound) {
			return held.Document{}, nil
		}
		if _, ok := errors.AsType[*eventsources.UnreadableError](err); ok {
			return held.Document{}, fmt.Errorf("%w: %w", held.ErrUndecodable, err)
		}
		if err != nil {
			return held.Document{}, err
		}
		pool := src.GetPool()
		if pool == "" {
			ns, err := namespaces.Get(ctx, namespace)
			if err != nil {
				return held.Document{}, fmt.Errorf("the default pool of namespace %q: %w", namespace, err)
			}
			pool = engine.DefaultPoolFor(ns)
		}
		return held.Document{Found: true, ID: src.GetId(), Pool: pool, Paused: src.GetPaused(), Deleting: src.GetDeletingSince() != nil, Source: src}, nil
	}
}

// orkerDescriber asks one Orker for its own account of itself.
type orkerDescriber interface {
	DescribeOrkerByID(ctx context.Context, orkerID string) (*orkstackv1.DescribeOrkerResult, error)
}

// HeldOrkerRow is the held check's read of an Orker: what it says of itself
// as a source host.
func HeldOrkerRow(orkers orkerDescriber) func(ctx context.Context, orkerID string) (held.OrkerRow, error) {
	return func(ctx context.Context, orkerID string) (held.OrkerRow, error) {
		d, err := orkers.DescribeOrkerByID(ctx, orkerID)
		if err != nil {
			return held.OrkerRow{}, err
		}
		pool := d.GetPool()
		if pool == "" {
			pool = task.DefaultPool
		}
		return held.OrkerRow{Pools: []string{pool}, Fit: d.GetServiceHost().GetFit(), MaxInstances: d.GetServiceHost().GetMaxInstances()}, nil
	}
}

// heldAccess is the replica's access document, as the held check asks it.
type heldAccess struct{ policies *PolicyCache }

// HeldAccess is the held check's access document.
func HeldAccess(policies *PolicyCache) held.Access { return heldAccess{policies} }

func (a heldAccess) Held(sub *authz.Subject, perm authz.Permission, pool string) (granted, fresh bool) {
	policy, fresh := a.policies.fresh()
	return fresh && grantedAtPool(policy, sub, perm, pool), fresh
}

func (a heldAccess) Read(ctx context.Context) (held.Decider, error) {
	policy, err := a.policies.PolicyReadAfter(ctx, a.policies.now())
	if err != nil {
		return nil, err
	}
	return func(sub *authz.Subject, perm authz.Permission, pool string) bool {
		return grantedAtPool(policy, sub, perm, pool)
	}, nil
}

func grantedAtPool(policy *authz.Policy, sub *authz.Subject, perm authz.Permission, pool string) bool {
	_, err := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: sub, Resource: authz.Resource{Pool: pool}, Policy: policy})
	return err == nil
}

// sourceInstances records what engines alone record about a source's
// instances.
type sourceInstances interface {
	Confirm(ctx context.Context, a artifacts.InstanceAttempt, r artifacts.ConfirmRecord) (artifacts.ConfirmRecord, error)
}

// SourceWorkflows tells a source's workflow what the door learned. Each is
// best-effort where the workflow finds the same by itself.
type SourceWorkflows interface {
	// Report relays what an instance reported, while the workflow waits for
	// it.
	Report(ctx context.Context, ref held.Ref, what orkstackv1.InstanceReport, reason string) error
	// Sync asks the workflow to load its source again.
	Sync(ctx context.Context, sourceID string) error
}

// pauseSource pauses call's source with one of OrkStack's codes, where it is
// not paused already: a pause that stands is kept as it is. orker is the
// Orker that asked, or none. It writes over the document the call read and no
// other: changed is the document that stands instead, for a caller to judge
// again. With neither changed nor an error the instance ends, for reason: the
// source is paused, or it is being deleted, or is no longer the one the call
// holds, which nothing is written to.
func (s *Service) pauseSource(ctx context.Context, call *heldCall, code, refusalCode, orker string) (reason string, changed *orkstackv1.EventSource, err error) {
	sentence := eventsources.PauseSentence(code)
	if sentence == "" {
		return "", nil, fmt.Errorf("%q is no code a source is paused with", code)
	}
	src := call.src
	ends := func(src *orkstackv1.EventSource) string {
		switch {
		case src.GetId() != call.ref.SourceID:
			return eventsources.ReasonNotHeld
		case src.GetDeletingSince() != nil:
			return eventsources.ReasonDeleting
		case src.GetPause() != nil:
			return eventsources.ReasonPaused
		}
		return ""
	}
	if reason := ends(src); reason != "" {
		return reason, nil, nil
	}
	next := proto.Clone(src).(*orkstackv1.EventSource)
	now := timestamppb.New(s.now().UTC())
	next.Pause = &orkstackv1.EventSourcePause{Cause: orkstackv1.PauseCause_PAUSE_CAUSE_ENGINE, Code: code, RefusalCode: refusalCode,
		Reason: sentence, Since: now, RequestedByOrker: orker}
	next.UpdatedAt, next.UpdatedBy = now, ""
	_, err = s.eventSources.Replace(ctx, next, src.GetEtag())
	switch {
	case errors.Is(err, eventsources.ErrChanged):
		src, err = s.eventSources.Get(ctx, src.GetNamespace(), src.GetName())
		if errors.Is(err, eventsources.ErrNotFound) {
			return eventsources.ReasonNotHeld, nil, nil
		}
		if err != nil {
			return "", nil, err
		}
		if reason := ends(src); reason != eventsources.ReasonPaused {
			if reason == "" {
				return "", src, nil
			}
			return reason, nil, nil
		}
	case err != nil:
		return "", nil, err
	default:
		s.logger.WarnContext(ctx, "an event source was paused", "namespace", src.GetNamespace(), "event_source", src.GetName(),
			"event_source_id", src.GetId(), "code", code, "refusal_code", refusalCode, "requested_by_orker", orker)
	}
	if s.sourceWorkflows != nil {
		if err := s.sourceWorkflows.Sync(ctx, call.ref.SourceID); err != nil {
			s.logger.WarnContext(ctx, "a paused event source's workflow was not told; it finds the pause at its next pass", "event_source_id", call.ref.SourceID, "error", err)
		}
	}
	return eventsources.ReasonPaused, nil, nil
}

// doorPaused stops an instance whose source was to be paused: for reason, and
// with the refusal's code where the source is paused.
func doorPaused(reason, refusalCode string) *orkstackv1.DoorAnswer {
	answer := doorStop(reason)
	if reason == eventsources.ReasonPaused {
		answer.RefusalCode = refusalCode
	}
	return answer
}

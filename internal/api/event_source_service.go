package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/quantity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// EventSourceService stores, reads and evaluates event sources
// (docs/EVENT_SOURCES.md). In this release no source consumes anything: a
// source is written, read and evaluated, and its document is all there is
// of it.
//
// One namespace decides every request: the source's own (docs/ACCESS.md,
// "A request that carries a document"). A create, an update and a
// candidate's evaluation act in the namespace the source message names,
// empty meaning default; the access rows decide them there, and the
// handlers read that one field and no other.

// evaluationsPerNamespace bounds the evaluations one namespace runs at once
// on a replica: a third is refused while two run.
const evaluationsPerNamespace = 2

// inspectsPerMinute bounds the image inspections one namespace asks of a
// replica in a minute: each is an outbound fetch with the engine's registry
// credentials.
const inspectsPerMinute = 30

// maxPauseReasonBytes bounds an operator's pause reason.
const maxPauseReasonBytes = 256

// sourceAuthGuard refuses every RPC of the service where callers are not
// authenticated and the operator has not accepted that.
func (s *Service) sourceAuthGuard() error {
	if s.enforcingAuth() || s.eventSourceSettings.AllowAsserted {
		return nil
	}
	return sourceRefusal(connect.CodeFailedPrecondition, eventsources.ReasonRequireAuthentication,
		"callers of this deployment are not authenticated: enforce authentication (auth.mode: enforce), "+
			"or set event_sources.allow_asserted to write, read and evaluate sources meanwhile")
}

// enforcingAuth reports whether this engine enforces authentication.
func (s *Service) enforcingAuth() bool { return s.access != nil && s.access.enforcing() }

// sourceStore is the registry, or the refusal of an engine that has none.
func (s *Service) sourceStore() (eventsources.Store, error) {
	if s.eventSources == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("this engine has no event-source store configured"))
	}
	return s.eventSources, nil
}

// sourceReadGuard is what every read passes.
func (s *Service) sourceReadGuard() (eventsources.Store, error) {
	if err := s.sourceAuthGuard(); err != nil {
		return nil, err
	}
	return s.sourceStore()
}

// sourceWriteGuard is what every write passes: authentication, then the
// feature, then a versioned deployment.
func (s *Service) sourceWriteGuard() (eventsources.Store, error) {
	if err := s.sourceAuthGuard(); err != nil {
		return nil, err
	}
	if !s.eventSourceSettings.Enabled {
		return nil, sourceRefusal(connect.CodeFailedPrecondition, eventsources.ReasonDisabled,
			"event sources are off on this deployment: set event_sources.enabled")
	}
	if s.system.BuildID == "" {
		return nil, sourceRefusal(connect.CodeFailedPrecondition, eventsources.ReasonRequireVersioning,
			"this deployment runs with no build id: event sources need it run as a release (ORK_BUILD_ID on ork and orker, then orkctl release promote)")
	}
	return s.sourceStore()
}

func sourceRefusal(code connect.Code, reason, sentence string) error {
	return connect.NewError(code, fmt.Errorf("%s: %s", reason, sentence))
}

// sourceNamespace is the one namespace a request is decided and acted in:
// validated by the name rule, never folded; empty is default.
func sourceNamespace(ns string) (string, error) {
	if ns == "" {
		return artifacts.DefaultNamespace, nil
	}
	if !eventsources.ValidName(ns) {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"namespace %q is not a namespace name (^[a-z][a-z0-9-]*$, at most %d bytes)", ns, eventsources.MaxNameBytes))
	}
	return ns, nil
}

// sourceName validates a source's name by the name rule: a request that
// names no source is a bad request, never a store failure.
func sourceName(name string) error {
	if !eventsources.ValidName(name) {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"name %q is not an event source's name (^[a-z][a-z0-9-]*$, at most %d bytes)", name, eventsources.MaxNameBytes))
	}
	return nil
}

// recheckSource decides perm again at the source a handler read: the access
// row decided at the source it read, and a source deleted and created again
// under the name between the two reads may bind another Orkflow.
func (s *Service) recheckSource(ctx context.Context, perm authz.Permission, src *orkstackv1.EventSource) error {
	allowed, err := s.mayAnswer(ctx, perm, authz.Resource{Namespace: src.GetNamespace(), Orkflow: src.GetOrkflowName()})
	if err != nil {
		return sourceStoreError(err, src.GetNamespace(), src.GetName())
	}
	if !allowed {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
			"%s is not granted at Orkflow %q of namespace %q", perm, src.GetOrkflowName(), src.GetNamespace()))
	}
	return nil
}

// sourceError maps a document's refusal to its code.
func sourceError(err error) error {
	var e *eventsources.Error
	if !errors.As(err, &e) {
		var ce *connect.Error
		if errors.As(err, &ce) {
			return err
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	code := connect.CodeInvalidArgument
	switch e.Reason {
	case eventsources.ReasonLimit:
		code = connect.CodeResourceExhausted
	case eventsources.ReasonImageUnsupported, eventsources.ReasonOrkflowUnsupported,
		eventsources.ReasonIdentityUnsupported:
		code = connect.CodeFailedPrecondition
	}
	return connect.NewError(code, e)
}

// sourceStoreError maps a registry failure to its code: nothing a store
// failed at is ever answered as a success or as absent.
func sourceStoreError(err error, namespace, name string) error {
	var unreadable *eventsources.UnreadableError
	switch {
	case errors.Is(err, eventsources.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("event source %q not found in namespace %q", name, namespace))
	case errors.Is(err, eventsources.ErrNameTaken):
		return connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("namespace %q already has an event source named %q", namespace, name))
	case errors.Is(err, eventsources.ErrChanged):
		return connect.NewError(connect.CodeAborted, fmt.Errorf(
			"event source %q in namespace %q changed since it was read: read it again and resend", name, namespace))
	case errors.As(err, &unreadable):
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: event source %q in namespace %q cannot be read by this engine (%s): a newer release's document is read by an engine of that release; "+
				"a damaged one can still be deleted by its id (orkctl event-source delete NAME --id ID)",
			eventsources.ReasonDocumentUnreadable, name, namespace, eventsources.SafeText(unreadable.Cause, 256)))
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	default:
		return connect.NewError(connect.CodeUnavailable, err)
	}
}

// checkedSource is what the write checks found, which the save records.
type checkedSource struct {
	ns     *orkstackv1.Namespace
	image  string
	labels eventsources.Image
}

// checkSource runs a write's checks in the documented order: the
// document's bounds; the Orkflow, its declared inputs and what a source
// cannot give it; the image, its labels, the image's interface and the
// image policy; the pool; the memory a message in hand needs; the secrets.
// The quotas follow in the caller, which knows whether it creates. prev is
// the stored source of an update, nil for a create.
func (s *Service) checkSource(ctx context.Context, src, prev *orkstackv1.EventSource) (*checkedSource, error) {
	limits := s.eventSourceSettings.Limits
	if err := eventsources.Validate(src, limits); err != nil {
		return nil, sourceError(err)
	}
	ns, err := s.namespace(ctx, src.GetNamespace())
	if err != nil {
		return nil, err
	}
	_, inputs, err := s.sourceOrkflow(ctx, ns.GetName(), src)
	if err != nil {
		return nil, err
	}
	if err := eventsources.CheckMapping(src, inputs); err != nil {
		return nil, sourceError(err)
	}
	out := &checkedSource{ns: ns}
	if err := s.checkSourceImage(ctx, ns, src, prev, out); err != nil {
		return nil, err
	}
	if err := s.checkSourcePool(ns, src); err != nil {
		return nil, err
	}
	if memory := src.GetResources().GetMemory(); memory != "" {
		bytes, err := quantity.Memory(memory)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("resources.memory: %w", err))
		}
		e := eventsources.Effective(src, limits)
		if err := eventsources.CheckMemory(int64(bytes), e.GetConcurrency(), e.GetMaxBytes()); err != nil {
			return nil, sourceError(err)
		}
	}
	if cpu := src.GetResources().GetCpu(); cpu != "" {
		if _, err := quantity.CPU(cpu); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("resources.cpu: %w", err))
		}
	}
	if allowed := ns.GetAllowedSecrets(); len(allowed) > 0 {
		for i, secret := range src.GetSecrets() {
			if err := dsl.CheckSecretSource(allowed, secret.GetSource()); err != nil {
				return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
					"secrets[%d] (%s): %w", i, secret.GetName(), err))
			}
		}
	}
	return out, nil
}

// sourceOrkflow reads the Orkflow a source binds, at its pinned version or
// the current one, refuses one a run started by a message cannot be given,
// and returns its declared inputs as a source reads them.
func (s *Service) sourceOrkflow(ctx context.Context, namespace string, src *orkstackv1.EventSource) (*orkstackv1.Orkflow, map[string]eventsources.Input, error) {
	wf, err := s.orkflows.Get(ctx, namespace, src.GetOrkflowName(), src.GetOrkflowVersion())
	if err != nil {
		return nil, nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			src.GetOrkflowName(), src.GetOrkflowVersion(), namespace)
	}
	for _, t := range wf.GetTasks() {
		if t.GetAwait().GetCallback() {
			return nil, nil, sourceError(&eventsources.Error{Reason: eventsources.ReasonOrkflowUnsupported, Field: "orkflow",
				Message: fmt.Sprintf("task %q awaits a callback whose token nobody would receive from a run a message starts", t.GetId())})
		}
		if len(t.GetIdentities()) > 0 || len(t.GetCredentials()) > 0 {
			return nil, nil, sourceError(&eventsources.Error{Reason: eventsources.ReasonOrkflowUnsupported, Field: "orkflow",
				Message: fmt.Sprintf("task %q asks for task identities or credentials, which a run a message starts cannot be given yet", t.GetId())})
		}
	}
	inputs := map[string]eventsources.Input{}
	for name, decl := range wf.GetInputs().GetFields() {
		d := dsl.ReadInput(decl)
		inputs[name] = eventsources.Input{Type: d.Type, Required: d.Required, HasDefault: d.Default != nil}
	}
	return wf, inputs, nil
}

// sourceImagePreflight holds a reference to the namespace's registry
// allow-list before anything is asked of a registry, so no request makes
// the engine reach a registry its namespace may not use.
func (s *Service) sourceImagePreflight(ctx context.Context, ns *orkstackv1.Namespace, field, reference string) (*orkstackv1.ImagePolicySelection, error) {
	sel, err := s.imagePolicySelection(ctx, ns)
	if err != nil {
		return nil, err
	}
	if allowed := sel.GetAllowedRegistries(); len(allowed) > 0 {
		parsed, err := dsl.ParseReference(reference)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %q is not an OCI reference", field, reference))
		}
		if !task.RegistryAllowed(allowed, parsed.Registry, parsed.Repository) {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
				"%s: IMAGE_NOT_PERMITTED: %s/%s is outside the registries namespace %q may use (%s)",
				field, parsed.Registry, parsed.Repository, ns.GetName(), strings.Join(allowed, ", ")))
		}
	}
	return sel, nil
}

// checkSourceImage resolves the source's image, reads its labels and holds
// the source to them and to the namespace's image policy. A registry
// reference is resolved to a digest when its text is new; an update that
// leaves it as it was reads the digest it was resolved to before, so a tag
// that moved changes nothing.
func (s *Service) checkSourceImage(ctx context.Context, ns *orkstackv1.Namespace, src, prev *orkstackv1.EventSource, out *checkedSource) error {
	uses := src.GetUses()
	if eventsources.IsFirstPartyName(uses) {
		return sourceError(&eventsources.Error{Reason: eventsources.ReasonImageUnsupported, Field: "uses", Message: fmt.Sprintf(
			"%q names a first-party source image, and none ships with this release: name an image by its registry reference", uses)})
	}
	if s.resolver == nil {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New(
			"this engine has no image resolver configured, so it cannot read a source image"))
	}
	reference := uses
	if prev != nil && prev.GetUses() == uses && prev.GetImage() != "" {
		reference = prev.GetImage()
	}
	sel, err := s.sourceImagePreflight(ctx, ns, "uses", reference)
	if err != nil {
		return err
	}
	res, err := s.resolver.Inspect(ctx, uses, reference)
	if err != nil {
		return applyResolveError(err)
	}
	img, err := eventsources.ReadServiceLabels(res.ServiceLabels)
	if err != nil {
		return sourceError(err)
	}
	if err := eventsources.CheckImage(img, src); err != nil {
		return sourceError(err)
	}
	probe := &orkstackv1.Orkflow{Name: src.GetOrkflowName(), Tasks: []*orkstackv1.Task{{Id: "event-source", Uses: uses, Image: res.Image}}}
	if err := s.checkImagePolicy(ctx, ns, sel, probe); err != nil {
		return connect.NewError(connect.CodeOf(err), fmt.Errorf("uses: %s",
			strings.ReplaceAll(strings.ReplaceAll(errorText(err), "tasks[0].uses", "uses"), `task "event-source": `, "")))
	}
	out.image = res.Image.GetPinned()
	out.labels = img
	return nil
}

// errorText is a Connect error's message without its code.
func errorText(err error) string {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Message()
	}
	return err.Error()
}

// checkSourcePool holds the source's pool to the namespace's allow-list, in
// the name the pool answers to here.
func (s *Service) checkSourcePool(ns *orkstackv1.Namespace, src *orkstackv1.EventSource) error {
	if s.pools == nil {
		return nil
	}
	probe := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "event-source", Pool: src.GetPool()}}}
	err := s.pools.CheckAllowedPools(ns, probe)
	var refusal *engine.PoolNotAllowedError
	if errors.As(err, &refusal) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"pool: pool %q is not allowed in namespace %q (allowed: %s)",
			refusal.Pool, ns.GetName(), strings.Join(refusal.Allowed, ", ")))
	}
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("pool: %w", err))
	}
	return nil
}

// sourceConsent decides the consent a source's save records, by the rule
// schedules and triggers follow (consent_save.go). binds is a create, or a
// save that changes what the source submits.
func (s *Service) sourceConsent(ctx context.Context, src *orkstackv1.EventSource, binds bool, given string, prior *orkstackv1.ConsentView, by string) (*orkstackv1.ConsentView, error) {
	if !binds && given == "" {
		// Nothing binds and nothing is given: the consent stays as it is,
		// and the Orkflow need not be read.
		return prior, nil
	}
	asks, unknown, err := s.asksForSave(ctx, src.GetNamespace(), src.GetOrkflowName(), src.GetOrkflowVersion())
	if err != nil {
		return nil, err
	}
	c, err := decideConsent(consentSave{Kind: "event-source", Name: src.GetName(), Orkflow: src.GetOrkflowName(), Binds: binds,
		Given: given, Asks: asks, Unknown: unknown, Prior: consentFromView(prior), By: by, Now: s.now().UTC()})
	if err != nil {
		return nil, err
	}
	return c.view(), nil
}

// consentFromView is a stored ConsentView as the consent rule reads it.
func consentFromView(v *orkstackv1.ConsentView) consentRecord {
	c := consentRecord{Digest: v.GetDigest(), Audiences: v.GetAudiences(), Grants: v.GetGrants()}
	if v.GetAt() != nil {
		c.At = v.GetAt().AsTime()
	}
	return c
}

// sourceQuotas holds a write to the namespace's quotas and sums and the
// deployment's count.
func (s *Service) sourceQuotas(ctx context.Context, store eventsources.Store, src *orkstackv1.EventSource) error {
	namespace, skipped, err := store.List(ctx, src.GetNamespace())
	if err != nil {
		return sourceStoreError(err, src.GetNamespace(), src.GetName())
	}
	if len(skipped) > 0 {
		// A source this release cannot read still counts: refusing is the
		// only answer that cannot overshoot a quota.
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: namespace %q holds %d event source(s) this engine cannot read, so its quotas cannot be counted; ask an engine of the release that wrote them",
			eventsources.ReasonDocumentUnreadable, src.GetNamespace(), len(skipped)))
	}
	count, err := store.Count(ctx)
	if err != nil {
		return sourceStoreError(err, src.GetNamespace(), src.GetName())
	}
	if err := eventsources.CheckQuotas(namespace, count, src, s.eventSourceSettings.Limits); err != nil {
		return sourceError(err)
	}
	return nil
}

// recordedWriter is who a save records as its writer and, where it binds,
// as the binder: an asserted caller stays asserted.
func recordedWriter(ctx context.Context, header http.Header) (string, bool) {
	who := authn.Recorded(ctx, header)
	return recordedBinder(who.ID), who.Verified && who.ID != ""
}

// clearOutputOnly drops what a request may not set: a write takes these
// from the stored source or derives them.
func clearOutputOnly(src *orkstackv1.EventSource) {
	src.Id, src.Image = "", ""
	src.Paused, src.Pause = false, nil
	src.CreatedAt, src.UpdatedAt, src.CreatedBy, src.UpdatedBy = nil, nil, "", ""
	src.BoundBy, src.BoundByVerified, src.ConsentStale = "", false, false
	src.AsksFor, src.Consented = nil, nil
	src.ServiceRevision, src.JudgingRevision = 0, 0
}

// CreateEventSource stores a new source after every check, with its
// counters at 1, its writer as its binder, and its first service spec.
func (s *Service) CreateEventSource(ctx context.Context, req *connect.Request[orkstackv1.CreateEventSourceRequest]) (*connect.Response[orkstackv1.CreateEventSourceResponse], error) {
	store, err := s.sourceWriteGuard()
	if err != nil {
		return nil, err
	}
	if req.Msg.GetEventSource() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("event_source is required"))
	}
	src := proto.Clone(req.Msg.GetEventSource()).(*orkstackv1.EventSource)
	if src.Namespace, err = sourceNamespace(src.GetNamespace()); err != nil {
		return nil, err
	}
	clearOutputOnly(src)
	src.Etag = ""
	checked, err := s.checkSource(ctx, src, nil)
	if err != nil {
		return nil, err
	}
	who, verified := recordedWriter(ctx, req.Header())
	src.Namespace = checked.ns.GetName()
	consented, err := s.sourceConsent(ctx, src, true, req.Msg.GetConsent(), nil, who)
	if err != nil {
		return nil, err
	}
	if err := s.sourceQuotas(ctx, store, src); err != nil {
		return nil, err
	}
	id, err := eventsources.NewID()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	now := timestamppb.New(s.now().UTC())
	src.Id = id
	src.Namespace = checked.ns.GetName()
	src.Image = checked.image
	src.ServiceRevision, src.JudgingRevision = 1, 1
	src.CreatedAt, src.UpdatedAt = now, now
	src.CreatedBy, src.UpdatedBy, src.BoundBy, src.BoundByVerified = who, who, who, verified
	src.Consented = consented
	stored, err := store.Create(ctx, src)
	if err != nil {
		return nil, sourceStoreError(err, src.GetNamespace(), src.GetName())
	}
	warnings := s.writeServiceSpec(ctx, store, stored, checked.labels)
	noteAudit(ctx, "id="+stored.GetId()+" sha256="+documentDigest(stored))
	return connect.NewResponse(&orkstackv1.CreateEventSourceResponse{EventSource: s.sourceView(ctx, nil, stored), SaveWarnings: warnings}), nil
}

// GetEventSource returns one source and its status.
func (s *Service) GetEventSource(ctx context.Context, req *connect.Request[orkstackv1.GetEventSourceRequest]) (*connect.Response[orkstackv1.GetEventSourceResponse], error) {
	store, err := s.sourceReadGuard()
	if err != nil {
		return nil, err
	}
	ns, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := sourceName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	src, err := store.Get(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, sourceStoreError(err, ns, req.Msg.GetName())
	}
	if err := s.recheckSource(ctx, authz.PermEventSourceRead, src); err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetEventSourceResponse{EventSource: s.sourceView(ctx, nil, src), Status: s.sourceStatus(src)}), nil
}

// ListEventSources returns a namespace's sources, by name, narrowed by the
// request's filters. A source this engine cannot read refuses the list
// rather than being left out of it, so a list never looks shorter than it
// is.
func (s *Service) ListEventSources(ctx context.Context, req *connect.Request[orkstackv1.ListEventSourcesRequest]) (*connect.Response[orkstackv1.ListEventSourcesResponse], error) {
	store, err := s.sourceReadGuard()
	if err != nil {
		return nil, err
	}
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	ns, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	docs, skipped, err := store.List(ctx, ns)
	if err != nil {
		return nil, sourceStoreError(err, ns, "")
	}
	if len(skipped) > 0 {
		// The count alone: an unreadable source binds no Orkflow this engine
		// can read, so its name could not be filtered by what the caller may
		// see.
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: namespace %q holds %d event source(s) this engine cannot read; ask an engine of the release that wrote them, or delete a damaged one by its id",
			eventsources.ReasonDocumentUnreadable, ns, len(skipped)))
	}
	out := make([]*orkstackv1.EventSource, 0, len(docs))
	memo := asksMemo{}
	for _, src := range docs {
		if row := s.sourceView(ctx, memo, src); set.Match(row) {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return connect.NewResponse(&orkstackv1.ListEventSourcesResponse{EventSources: out}), nil
}

// UpdateEventSource replaces what a user writes of a source under the
// etag it read, after the create's checks, and carries over everything
// that is OrkStack's.
func (s *Service) UpdateEventSource(ctx context.Context, req *connect.Request[orkstackv1.UpdateEventSourceRequest]) (*connect.Response[orkstackv1.UpdateEventSourceResponse], error) {
	store, err := s.sourceWriteGuard()
	if err != nil {
		return nil, err
	}
	if req.Msg.GetEventSource() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("event_source is required"))
	}
	next := proto.Clone(req.Msg.GetEventSource()).(*orkstackv1.EventSource)
	if next.Namespace, err = sourceNamespace(next.GetNamespace()); err != nil {
		return nil, err
	}
	etag := next.GetEtag()
	if etag == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(
			"event_source.etag is required: send back the etag a read of the source returned"))
	}
	stored, err := store.Get(ctx, next.GetNamespace(), next.GetName())
	if err != nil {
		return nil, sourceStoreError(err, next.GetNamespace(), next.GetName())
	}
	if next.GetOrkflowName() != stored.GetOrkflowName() {
		return nil, sourceRefusal(connect.CodeInvalidArgument, eventsources.ReasonNamespaceMismatch, fmt.Sprintf(
			"the source names Orkflow %q in namespace %q, which is not the Orkflow event source %q binds; it is fixed at creation; nothing was written",
			next.GetOrkflowName(), next.GetNamespace(), next.GetName()))
	}
	if etag != stored.GetEtag() {
		return nil, sourceStoreError(eventsources.ErrChanged, next.GetNamespace(), next.GetName())
	}
	clearOutputOnly(next)
	checked, err := s.checkSource(ctx, next, stored)
	if err != nil {
		return nil, err
	}
	// The table of workload-identity §3.2: a save that changes what the
	// source submits binds its caller and needs a consent when the Orkflow
	// asks for an identity; --rebind binds and keeps the consent; a consent
	// given leaves the binder as it is.
	who, verified := recordedWriter(ctx, req.Header())
	binds := eventsources.ChangesWhatItSubmits(stored, next)
	rebinds := req.Msg.GetRebind() || binds
	next.Namespace = stored.GetNamespace()
	consented, err := s.sourceConsent(ctx, next, binds, req.Msg.GetConsent(), stored.GetConsented(), who)
	if err != nil {
		return nil, err
	}
	if err := s.sourceQuotas(ctx, store, next); err != nil {
		return nil, err
	}
	service, judging := eventsources.Classify(stored, next)
	now := timestamppb.New(s.now().UTC())

	next.Id, next.Namespace = stored.GetId(), stored.GetNamespace()
	next.Image = checked.image
	next.Pause = stored.GetPause()
	next.CreatedAt, next.CreatedBy = stored.GetCreatedAt(), stored.GetCreatedBy()
	next.UpdatedAt, next.UpdatedBy = now, who
	next.ServiceRevision, next.JudgingRevision = stored.GetServiceRevision(), stored.GetJudgingRevision()
	if service || next.GetImage() != stored.GetImage() {
		next.ServiceRevision++
	}
	if judging {
		next.JudgingRevision++
	}
	next.BoundBy, next.BoundByVerified, next.Consented = stored.GetBoundBy(), stored.GetBoundByVerified(), consented
	if rebinds {
		next.BoundBy, next.BoundByVerified = who, verified
	}
	saved, err := store.Replace(ctx, next, etag)
	if err != nil {
		return nil, sourceStoreError(err, next.GetNamespace(), next.GetName())
	}
	// The spec is written at every save: it is create-only and named by its
	// digest, so this repairs one an earlier save could not write.
	warnings := s.writeServiceSpec(ctx, store, saved, checked.labels)
	noteAudit(ctx, "id="+saved.GetId()+" sha256="+documentDigest(saved))
	return connect.NewResponse(&orkstackv1.UpdateEventSourceResponse{EventSource: s.sourceView(ctx, nil, saved), SaveWarnings: warnings}), nil
}

// PauseEventSource sets a source's pause with the operator's reason.
func (s *Service) PauseEventSource(ctx context.Context, req *connect.Request[orkstackv1.PauseEventSourceRequest]) (*connect.Response[orkstackv1.PauseEventSourceResponse], error) {
	store, err := s.sourceWriteGuard()
	if err != nil {
		return nil, err
	}
	reason := strings.TrimSpace(req.Msg.GetReason())
	if reason == "" || len(reason) > maxPauseReasonBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("reason is required, at most %d bytes", maxPauseReasonBytes))
	}
	ns, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := sourceName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	who, _ := recordedWriter(ctx, req.Header())
	stored, err := store.Get(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, sourceStoreError(err, ns, req.Msg.GetName())
	}
	if err := s.recheckSource(ctx, authz.PermEventSourceOperate, stored); err != nil {
		return nil, err
	}
	var warnings []string
	saved := stored
	if stored.GetPause() != nil {
		// A pause is kept as it was, who made it and why included: a second
		// pause changes nothing and says so.
		warnings = append(warnings, "source was already paused; nothing changed")
	} else if saved, err = s.replaceSource(ctx, store, stored, who, func(src *orkstackv1.EventSource) {
		src.Pause = &orkstackv1.EventSourcePause{Cause: orkstackv1.PauseCause_PAUSE_CAUSE_OPERATOR,
			Reason: eventsources.SafeText(reason, maxPauseReasonBytes), Since: timestamppb.New(s.now().UTC()), Principal: who}
	}); err != nil {
		return nil, err
	}
	if eventsources.Effective(saved, s.eventSourceSettings.Limits).GetDelivery() == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE {
		warnings = append(warnings, "this source is at-most-once: messages published while it is paused are lost")
	}
	return connect.NewResponse(&orkstackv1.PauseEventSourceResponse{EventSource: s.sourceView(ctx, nil, saved), Warnings: warnings}), nil
}

// ResumeEventSource clears a source's pause after the create's checks other
// than its quotas. It neither rebinds nor consents; a source that is not
// paused is left as it is, and the answer says so.
func (s *Service) ResumeEventSource(ctx context.Context, req *connect.Request[orkstackv1.ResumeEventSourceRequest]) (*connect.Response[orkstackv1.ResumeEventSourceResponse], error) {
	store, err := s.sourceWriteGuard()
	if err != nil {
		return nil, err
	}
	ns, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := sourceName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	stored, err := store.Get(ctx, ns, req.Msg.GetName())
	if err != nil {
		return nil, sourceStoreError(err, ns, req.Msg.GetName())
	}
	if err := s.recheckSource(ctx, authz.PermEventSourceOperate, stored); err != nil {
		return nil, err
	}
	if err := s.recheckSource(ctx, authz.PermRunSubmit, stored); err != nil {
		return nil, err
	}
	if stored.GetPause() == nil {
		return connect.NewResponse(&orkstackv1.ResumeEventSourceResponse{EventSource: s.sourceView(ctx, nil, stored),
			Notice: "source was not paused; nothing changed"}), nil
	}
	if _, err := s.checkSource(ctx, stored, stored); err != nil {
		return nil, err
	}
	who, _ := recordedWriter(ctx, req.Header())
	saved, err := s.replaceSource(ctx, store, stored, who, func(src *orkstackv1.EventSource) { src.Pause = nil })
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.ResumeEventSourceResponse{EventSource: s.sourceView(ctx, nil, saved)}), nil
}

// replaceSource applies change to a source read with its etag and writes it
// under that etag, once: a source that changed meanwhile is Aborted, never
// overwritten.
func (s *Service) replaceSource(ctx context.Context, store eventsources.Store, stored *orkstackv1.EventSource, who string, change func(*orkstackv1.EventSource)) (*orkstackv1.EventSource, error) {
	next := proto.Clone(stored).(*orkstackv1.EventSource)
	change(next)
	next.UpdatedAt, next.UpdatedBy = timestamppb.New(s.now().UTC()), who
	saved, err := store.Replace(ctx, next, stored.GetEtag())
	if err != nil {
		return nil, sourceStoreError(err, stored.GetNamespace(), stored.GetName())
	}
	return saved, nil
}

// DeleteEventSource deletes the source of the id the request names; one of
// another id under the name is refused EVENT_SOURCE_ID_MISMATCH, naming
// it, and nothing is deleted. A request with no id deletes whatever the
// name holds. A delete is never refused for the feature being off: a
// source written before it was turned off can always be removed.
func (s *Service) DeleteEventSource(ctx context.Context, req *connect.Request[orkstackv1.DeleteEventSourceRequest]) (*connect.Response[orkstackv1.DeleteEventSourceResponse], error) {
	store, err := s.sourceReadGuard()
	if err != nil {
		return nil, err
	}
	ns, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	name, id := req.Msg.GetName(), req.Msg.GetId()
	if err := sourceName(name); err != nil {
		return nil, err
	}
	if id != "" && !eventsources.ValidID(id) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("id %q is not an event source's id (evs- and 16 hex digits)", id))
	}
	stored, err := store.Get(ctx, ns, name)
	var unreadable *eventsources.UnreadableError
	switch {
	case errors.As(err, &unreadable) && id != "":
		// A document this engine cannot decode is deleted by the id its
		// caller names, so a damaged one never blocks its namespace.
		// Nothing of it can be read, so it is decided at its namespace,
		// never at an Orkflow a reader resolved before it became unreadable.
		stored = &orkstackv1.EventSource{Id: id, Namespace: ns, Name: name}
		if err := s.recheckSource(ctx, authz.PermEventSourceWrite, stored); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, sourceStoreError(err, ns, name)
	default:
		if err := s.recheckSource(ctx, authz.PermEventSourceWrite, stored); err != nil {
			return nil, err
		}
	}
	if id == "" {
		id = stored.GetId()
	}
	if stored.GetId() != id {
		return nil, sourceRefusal(connect.CodeFailedPrecondition, eventsources.ReasonIDMismatch, fmt.Sprintf(
			"event source %q in namespace %q is %s now, not %s; nothing was deleted", name, ns, stored.GetId(), id))
	}
	err = store.Delete(ctx, ns, name, id)
	if errors.Is(err, eventsources.ErrSpecsLeft) {
		// The source is deleted; what is left under its id admits nothing.
		s.logger.Warn("event source deleted; its service specs were not all removed", "namespace", ns, "name", name, "id", id, "error", err)
		err = nil
	}
	if err != nil {
		if errors.Is(err, eventsources.ErrChanged) {
			return nil, sourceRefusal(connect.CodeFailedPrecondition, eventsources.ReasonIDMismatch, fmt.Sprintf(
				"event source %q in namespace %q became another source while it was being deleted; nothing was deleted", name, ns))
		}
		return nil, sourceStoreError(err, ns, name)
	}
	noteAudit(ctx, "id="+id)
	return connect.NewResponse(&orkstackv1.DeleteEventSourceResponse{}), nil
}

// EvaluateEventSourceMessage runs a stored or candidate source over one
// message, with the package an Orker judges by, and says what would happen.
// It starts nothing and writes nothing.
func (s *Service) EvaluateEventSourceMessage(ctx context.Context, req *connect.Request[orkstackv1.EvaluateEventSourceMessageRequest]) (*connect.Response[orkstackv1.EvaluateEventSourceMessageResponse], error) {
	store, err := s.sourceReadGuard()
	if err != nil {
		return nil, err
	}
	limits := s.eventSourceSettings.Limits
	if int64(len(req.Msg.GetBody())) > limits.MaxMessageBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"body is %d bytes; event_sources.max_message_bytes is %d", len(req.Msg.GetBody()), limits.MaxMessageBytes))
	}
	var src *orkstackv1.EventSource
	switch which := req.Msg.GetSource().(type) {
	case *orkstackv1.EvaluateEventSourceMessageRequest_Name:
		ns, err := sourceNamespace(req.Msg.GetNamespace())
		if err != nil {
			return nil, err
		}
		if err := sourceName(which.Name); err != nil {
			return nil, err
		}
		if src, err = store.Get(ctx, ns, which.Name); err != nil {
			return nil, sourceStoreError(err, ns, which.Name)
		}
		if err := s.recheckSource(ctx, authz.PermEventSourceRead, src); err != nil {
			return nil, err
		}
	case *orkstackv1.EvaluateEventSourceMessageRequest_Candidate:
		if which.Candidate == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("candidate is empty"))
		}
		src = proto.Clone(which.Candidate).(*orkstackv1.EventSource)
		if src.Namespace, err = sourceNamespace(src.GetNamespace()); err != nil {
			return nil, err
		}
		if err := eventsources.Validate(src, limits); err != nil {
			return nil, sourceError(err)
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name a stored source or give a candidate"))
	}
	release, ok := s.evaluations.take(src.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"namespace %q already has %d evaluations running on this engine; try again", src.GetNamespace(), evaluationsPerNamespace))
	}
	defer release()
	ns, err := s.namespace(ctx, src.GetNamespace())
	if err != nil {
		return nil, err
	}
	wf, inputs, err := s.sourceOrkflow(ctx, ns.GetName(), src)
	if err != nil {
		return nil, err
	}
	if err := eventsources.CheckMapping(src, inputs); err != nil {
		return nil, sourceError(err)
	}
	res := eventsources.Evaluate(src, messageOf(req.Msg), eventsources.Judge{
		Inputs: inputs, MaxInputBytes: limits.MaxInputBytes, MaxMessageBytes: limits.MaxMessageBytes})
	out := &orkstackv1.EvaluateEventSourceMessageResponse{Outcome: res.Outcome, Reason: res.Reason,
		Detail: res.Detail, Artifacts: res.Artifacts, OrkflowVersion: wf.GetVersion()}
	for _, t := range res.Tests {
		out.Tests = append(out.Tests, &orkstackv1.MatchResult{Index: int32(t.Index), Selector: t.Selector, Held: t.Held, Selected: t.Selected})
	}
	if res.Input != nil {
		if out.Input, err = mappedInputPayload(res.Input); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(out), nil
}

// messageOf is the request's message as the package reads one.
func messageOf(req *orkstackv1.EvaluateEventSourceMessageRequest) eventsources.Message {
	md := req.GetMetadata()
	msg := eventsources.Message{ID: md.GetId(), Subject: md.GetSubject(), Group: md.GetGroup(), Body: req.GetBody()}
	if md.GetPublishedAt() != nil {
		msg.PublishedAt = md.GetPublishedAt().AsTime()
	}
	if len(md.GetAttributes()) > 0 {
		msg.Attributes = make(map[string][]string, len(md.GetAttributes()))
		for k, v := range md.GetAttributes() {
			msg.Attributes[k] = v.GetValues()
		}
	}
	return msg
}

func mappedInputPayload(input map[string]jsontext.Value) (*orkstackv1.Payload, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode the mapped input: %w", err)
	}
	data := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, data); err != nil {
		return nil, fmt.Errorf("decode the mapped input: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// InspectSourceImage reads what one image declares. Asked with no
// reference it lists the first-party source images this release ships,
// which are none.
func (s *Service) InspectSourceImage(ctx context.Context, req *connect.Request[orkstackv1.InspectSourceImageRequest]) (*connect.Response[orkstackv1.InspectSourceImageResponse], error) {
	if _, err := s.sourceReadGuard(); err != nil {
		return nil, err
	}
	nsName, err := sourceNamespace(req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	reference := req.Msg.GetReference()
	if reference == "" {
		return connect.NewResponse(&orkstackv1.InspectSourceImageResponse{}), nil
	}

	if eventsources.IsFirstPartyName(reference) {
		return nil, sourceError(&eventsources.Error{Reason: eventsources.ReasonImageUnsupported, Field: "reference",
			Message: fmt.Sprintf("%q names a first-party source image, and none ships with this release", reference)})
	}
	if s.resolver == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(
			"this engine has no image resolver configured, so it cannot read a source image"))
	}
	// The namespace's image policy is held before and after the registry is
	// asked: a caller who may read sources makes the engine reach only a
	// registry the namespace may use.
	ns, err := s.namespace(ctx, nsName)
	if err != nil {
		return nil, err
	}
	// Counted only for a namespace that exists, so the limiter's keys are
	// the registry's namespaces and never a caller's invention.
	if !s.inspections.allow(ns.GetName(), s.now()) {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"namespace %q has asked this engine for %d image inspections in the last minute; try again shortly", ns.GetName(), inspectsPerMinute))
	}
	sel, err := s.sourceImagePreflight(ctx, ns, "reference", reference)
	if err != nil {
		return nil, err
	}
	res, err := s.resolver.Inspect(ctx, reference, reference)
	if err != nil {
		return nil, applyResolveError(err)
	}
	probe := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "event-source", Uses: reference, Image: res.Image}}}
	if err := s.checkImagePolicy(ctx, ns, sel, probe); err != nil {
		return nil, connect.NewError(connect.CodeOf(err), fmt.Errorf("reference: %s", errorText(err)))
	}
	img, err := eventsources.ReadServiceLabels(res.ServiceLabels)
	if err != nil {
		return nil, sourceError(err)
	}
	info := &orkstackv1.SourceImage{Reference: reference, Image: res.Image.GetPinned(),
		ContractVersions: img.Contracts, ProfileVersions: img.Profiles, Interface: img.Interface}
	return connect.NewResponse(&orkstackv1.InspectSourceImageResponse{Images: []*orkstackv1.SourceImage{info}}), nil
}

// sourceView is a stored source as a caller reads it, with what is derived
// on read.
func (s *Service) sourceView(ctx context.Context, memo asksMemo, src *orkstackv1.EventSource) *orkstackv1.EventSource {
	out := proto.Clone(src).(*orkstackv1.EventSource)
	out.Paused = out.GetPause() != nil
	// What its Orkflow asks for now; unset when it cannot be read, so "could
	// not tell" is never "asks for nothing".
	asks, err := s.asksForMemo(ctx, memo, src.GetNamespace(), src.GetOrkflowName(), src.GetOrkflowVersion())
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("what an event source's Orkflow asks for could not be read; it is not shown",
				slog.String("namespace", src.GetNamespace()), slog.String("event_source", src.GetName()), slog.String("error", err.Error()))
		}
		return out
	}
	out.AsksFor = &orkstackv1.AsksFor{Digest: asks.Digest, Audiences: asks.Audiences, Grants: asks.Grants}
	out.ConsentStale = consentStale(out.GetConsented(), out.GetAsksFor())
	return out
}

// sourceStatus is a source's state, derived on read. No source runs in this
// release, so a source is disabled, paused, or stored and waiting for a
// release that runs it.
func (s *Service) sourceStatus(src *orkstackv1.EventSource) *orkstackv1.EventSourceStatus {
	st := &orkstackv1.EventSourceStatus{Delivery: eventsources.Effective(src, s.eventSourceSettings.Limits).GetDelivery()}
	set := func(state orkstackv1.EventSourceState, reason, message string) *orkstackv1.EventSourceStatus {
		st.State, st.Reason, st.Message = state, reason, message
		return st
	}
	switch {
	case !s.eventSourceSettings.Enabled:
		return set(orkstackv1.EventSourceState_EVENT_SOURCE_STATE_DISABLED, eventsources.ReasonDisabled,
			"event sources are off on this deployment")
	case s.system.BuildID == "":
		return set(orkstackv1.EventSourceState_EVENT_SOURCE_STATE_DISABLED, eventsources.ReasonRequireVersioning,
			"this deployment does not run as a release, which event sources need")
	case !s.enforcingAuth():
		return set(orkstackv1.EventSourceState_EVENT_SOURCE_STATE_DISABLED, eventsources.ReasonRequireAuthentication,
			"no instance runs until the deployment enforces authentication")
	case src.GetPause() != nil:
		return set(orkstackv1.EventSourceState_EVENT_SOURCE_STATE_PAUSED, eventsources.ReasonPaused,
			"paused: "+src.GetPause().GetReason())
	default:
		return set(orkstackv1.EventSourceState_EVENT_SOURCE_STATE_STARTING, eventsources.ReasonNotConverged,
			"stored; this release of OrkStack runs no event source yet, so it consumes nothing")
	}
}

// writeServiceSpec stores the service spec of the source's current service
// revision under its id and digest. A spec that cannot be written is a
// warning: the document is the truth, and a spec it calls for is rebuilt
// from it.
func (s *Service) writeServiceSpec(ctx context.Context, store eventsources.Store, src *orkstackv1.EventSource, img eventsources.Image) []string {
	body, digest, err := serviceSpec(src, img, s.eventSourceSettings.Limits)
	if err == nil {
		err = store.PutSpec(ctx, src.GetId(), digest, body)
	}
	if err != nil {
		s.logger.Error("event source: write its service spec", "namespace", src.GetNamespace(), "name", src.GetName(), "id", src.GetId(), "error", err)
		return []string{"the source was saved; its service spec could not be stored, and the next save of the source writes it again"}
	}
	return nil
}

// serviceSpec is a source's service spec: what its containers are started
// with, and the digest that names it (eventsources.BuildServiceSpec). The
// park is the default: event_sources.park arrives with the source workflow,
// which then builds the spec with it here and there alike.
func serviceSpec(src *orkstackv1.EventSource, img eventsources.Image, l eventsources.Limits) ([]byte, string, error) {
	_, body, digest, err := eventsources.BuildServiceSpec(src, img, l, 0)
	return body, digest, err
}

// documentDigest is the SHA-256 of a stored document, for its audit
// record.
func documentDigest(src *orkstackv1.EventSource) string {
	view := proto.Clone(src).(*orkstackv1.EventSource)
	view.Etag = ""
	raw, err := protojson.Marshal(view)
	if err != nil {
		return "unencodable"
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// namespaceSlots bounds how many of something run at once for each
// namespace, on this replica only: process memory, never shared.
type namespaceSlots struct {
	mu    sync.Mutex
	limit int
	held  map[string]int
}

func newNamespaceSlots(limit int) *namespaceSlots {
	return &namespaceSlots{limit: limit, held: map[string]int{}}
}

// take holds a slot of ns, or reports that every slot is held.
func (n *namespaceSlots) take(ns string) (release func(), ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.held[ns] >= n.limit {
		return nil, false
	}
	n.held[ns]++
	var once sync.Once
	return func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.held[ns]--; n.held[ns] <= 0 {
				delete(n.held, ns)
			}
		})
	}, true
}

// namespaceRate admits at most limit events a minute for each namespace, on
// this replica only: process memory, never shared, gone with the process.
type namespaceRate struct {
	mu     sync.Mutex
	limit  int
	window map[string][]time.Time
}

func newNamespaceRate(limit int) *namespaceRate {
	return &namespaceRate{limit: limit, window: map[string][]time.Time{}}
}

// allow records one event of ns at now, or reports that the minute's are
// spent.
func (r *namespaceRate) allow(ns string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Every namespace's window is pruned on each call, and an empty one is
	// dropped, so what this holds is bounded by the namespaces asking.
	for key, times := range r.window {
		if key == ns {
			continue
		}
		if len(times) == 0 || now.Sub(times[len(times)-1]) >= time.Minute {
			delete(r.window, key)
		}
	}
	kept := r.window[ns][:0]
	for _, at := range r.window[ns] {
		if now.Sub(at) < time.Minute {
			kept = append(kept, at)
		}
	}
	if len(kept) >= r.limit {
		r.window[ns] = kept
		return false
	}
	r.window[ns] = append(kept, now)
	return true
}

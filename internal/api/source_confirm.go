package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// maxDoorRequest bounds a request of EventSourceHostService.
	maxDoorRequest = 256 << 10
	// maxReportReason bounds the reason of an instance's report.
	maxReportReason = 256
	// sourcePolicyAge is how long what a service spec was judged against may
	// admit it again. It never refuses.
	sourcePolicyAge = time.Minute
	// maxSourcePolicies bounds the specs one replica keeps that for.
	maxSourcePolicies = 1024
)

// checkDoorRequest refuses a request over its bound.
func checkDoorRequest(req proto.Message) error {
	if n := proto.Size(req); n > maxDoorRequest {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("a request of an event source's host is at most %d bytes; this is %d", maxDoorRequest, n))
	}
	return nil
}

// judgedSpec is a service spec that may run now, and what it was judged
// against.
type judgedSpec struct {
	body        []byte
	imagePolicy *orkstackv1.ImagePolicySelection
	telemetry   *orkstackv1.TelemetrySelection
}

// sourcePolicies keeps what confirmations admitted, for sourcePolicyAge: a
// busy source confirms for every filtered message, and its spec's registry
// and signature need not be asked for each.
type sourcePolicies struct {
	now  func() time.Time
	mu   sync.Mutex
	kept map[string]keptPolicy
}

type keptPolicy struct {
	at   time.Time
	spec judgedSpec
}

func newSourcePolicies() *sourcePolicies {
	return &sourcePolicies{now: time.Now, kept: map[string]keptPolicy{}}
}

func (p *sourcePolicies) get(key string) (judgedSpec, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, ok := p.kept[key]
	age := p.now().Sub(k.at)
	return k.spec, ok && age >= 0 && age <= sourcePolicyAge
}

func (p *sourcePolicies) put(key string, at time.Time, spec judgedSpec) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.kept) >= maxSourcePolicies {
		clear(p.kept)
	}
	p.kept[key] = keptPolicy{at: at, spec: spec}
}

// errSpecUnread is a service spec that is not there, or not one this release
// reads.
var errSpecUnread = errors.New("the source's service spec cannot be read")

// judgeSourceSpec holds the service spec a call presents to what is in force
// now: the image policy of the deployment and the namespace, and the
// namespace's pools and secret sources. Its error is classified; one that
// wraps errSpecUnread is the spec's own.
func (s *Service) judgeSourceSpec(ctx context.Context, call *heldCall) (judgedSpec, error) {
	key := call.ref.Namespace + "/" + call.ref.SourceID + "/" + call.ref.ServiceDigest
	if kept, ok := s.sourcePolicies.get(key); ok {
		return kept, nil
	}
	at := s.sourcePolicies.now()
	body, found, err := s.eventSources.ReadSpec(ctx, call.ref.SourceID, call.ref.ServiceDigest)
	if errors.Is(err, eventsources.ErrSpecCorrupt) {
		return judgedSpec{}, fmt.Errorf("%w: %w", errSpecUnread, err)
	}
	if err != nil {
		return judgedSpec{}, outage(err)
	}
	if !found {
		return judgedSpec{}, errSpecUnread
	}
	spec, err := eventsources.ReadServiceSpec(body)
	if err != nil {
		return judgedSpec{}, fmt.Errorf("%w: %w", errSpecUnread, err)
	}
	if spec.Source.ID != call.ref.SourceID {
		return judgedSpec{}, fmt.Errorf("%w: it is of source %s", errSpecUnread, spec.Source.ID)
	}
	ns, err := s.namespace(ctx, call.ref.Namespace)
	if err != nil {
		return judgedSpec{}, err
	}
	sel, err := s.imagePolicySelection(ctx, ns)
	if err != nil {
		return judgedSpec{}, err
	}
	parsed, err := dsl.ParseReference(spec.Image)
	if err != nil {
		return judgedSpec{}, fmt.Errorf("%w: %w", errSpecUnread, err)
	}
	image := &orkstackv1.ImageRef{Reference: spec.Image, Registry: parsed.Registry, Repository: parsed.Repository, IndexDigest: parsed.Digest, Pinned: spec.Image}
	probe := &orkstackv1.Orkflow{Name: call.src.GetOrkflowName(), Tasks: []*orkstackv1.Task{{Id: "event-source", Uses: spec.Image, Image: image, Pool: spec.Pool}}}
	if err := s.checkImagePolicy(ctx, ns, sel, probe); err != nil {
		return judgedSpec{}, err
	}
	if s.pools != nil {
		if err := s.pools.CheckAllowedPools(ns, probe); err != nil {
			return judgedSpec{}, err
		}
	}
	if allowed := ns.GetAllowedSecrets(); len(allowed) > 0 {
		for _, secret := range spec.Secrets {
			if err := dsl.CheckSecretSource(allowed, secret.Source); err != nil {
				return judgedSpec{}, refused(refusalPause, "SECRET_NOT_ALLOWED", err)
			}
		}
	}
	judged := judgedSpec{body: body, imagePolicy: sel}
	if judged.imagePolicy == nil {
		judged.imagePolicy = &orkstackv1.ImagePolicySelection{}
	}
	if t := ns.GetTelemetry(); t != nil {
		judged.telemetry = &orkstackv1.TelemetrySelection{Sink: t.GetSink(), TaskLogs: t.GetTaskLogs(), Attributes: t.GetAttributes()}
	}
	s.sourcePolicies.put(key, at, judged)
	return judged, nil
}

// heldRevokePending is the wait of an attempt whose save a revoke names.
const heldRevokePending = "held=revoke-pending"

// ConfirmInstance confirms that the caller holds the instance and that what
// it runs may still run: the source is judged against what is in force now.
// An attempt's first confirmation is put on record, whatever it is answered.
func (s *Service) ConfirmInstance(ctx context.Context, req *connect.Request[orkstackv1.ConfirmInstanceRequest]) (*connect.Response[orkstackv1.ConfirmInstanceResponse], error) {
	answered := func(a *orkstackv1.DoorAnswer) (*connect.Response[orkstackv1.ConfirmInstanceResponse], error) {
		return connect.NewResponse(&orkstackv1.ConfirmInstanceResponse{Answer: a}), nil
	}
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, doorTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeConfirm, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer != nil {
		return answered(answer)
	}
	caller := authn.FromContext(ctx)
	if !call.result.Confirmed {
		attempt := artifacts.InstanceAttempt{SourceID: call.ref.SourceID, Index: int(call.ref.Index), Epoch: call.ref.Epoch, Attempt: int(call.ref.Attempt)}
		record, err := s.sourceInstances.Confirm(ctx, attempt, artifacts.ConfirmRecord{OrkerID: caller.OrkerID, ConfirmedAt: s.now(),
			ServiceDigest: call.result.Epoch.Digest, AdmissionSave: call.result.Epoch.AdmissionSave})
		if err != nil {
			// No container starts on an attempt the engine could not write down.
			s.logger.WarnContext(ctx, "an instance's attempt could not be put on record", "event_source_id", call.ref.SourceID, "index", call.ref.Index,
				"epoch", call.ref.Epoch, "attempt", call.ref.Attempt, "error", err)
			return answered(doorWait("", outageVerdict(err).waitCause()))
		}
		if record.OrkerID != caller.OrkerID || record.ServiceDigest != call.ref.ServiceDigest {
			return answered(doorStop(eventsources.ReasonNotHeld))
		}
		if s.sourceWorkflows != nil {
			if err := s.sourceWorkflows.Started(ctx, call.ref); err != nil {
				s.logger.WarnContext(ctx, "an instance's workflow was not told that it was taken; it finds out at its next pass", "event_source_id", call.ref.SourceID,
					"index", call.ref.Index, "epoch", call.ref.Epoch, "error", err)
			}
		}
	}
	// A revoke of the attempt's save holds it before it starts. One that
	// waited is on record already, so that it has none is not what tells: it
	// asks for its spec for as long as it has not started.
	if call.result.RevokePending && (!call.result.Confirmed || req.Msg.GetWantServiceSpec()) {
		return answered(doorWait(heldRevokePending, waitAdmission))
	}
	judged, err := s.judgeSourceSpec(ctx, call)
	if err != nil {
		return answered(s.confirmRefused(ctx, call, err))
	}
	out := &orkstackv1.ConfirmInstanceResponse{Answer: &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_OK},
		ImagePolicy: judged.imagePolicy, Telemetry: judged.telemetry}
	if req.Msg.GetWantServiceSpec() {
		out.Service = &orkstackv1.ServiceSpec{Spec: judged.body, Digest: call.ref.ServiceDigest}
	}
	if req.Msg.GetInstance().GetJudgingRevision() != call.src.GetJudgingRevision() {
		if out.Judging, err = s.judgingSpec(call.src); err != nil {
			s.logger.ErrorContext(ctx, "a source's judging spec could not be made", "event_source_id", call.ref.SourceID, "error", err)
			return answered(doorRefused(eventsources.ReasonDocumentUnreadable))
		}
	}
	return connect.NewResponse(out), nil
}

// judgingSpec is how src's messages are judged now.
func (s *Service) judgingSpec(src *orkstackv1.EventSource) (*orkstackv1.JudgingSpec, error) {
	_, body, digest, err := eventsources.BuildJudgingSpec(src, s.eventSourceSettings.Limits)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.JudgingSpec{Spec: body, Digest: digest}, nil
}

// confirmRefused answers a confirmation whose source may not run now: a wait
// where that may pass, and where it cannot, a pause of the source with the
// refusal's code. A spec that cannot be read is a wait of the engine's own
// judgment.
func (s *Service) confirmRefused(ctx context.Context, call *heldCall, err error) *orkstackv1.DoorAnswer {
	log := s.logger.With("namespace", call.ref.Namespace, "event_source", call.ref.Name, "event_source_id", call.ref.SourceID, "service_digest", call.ref.ServiceDigest)
	if errors.Is(err, errSpecUnread) {
		log.ErrorContext(ctx, "an event source's service spec cannot be read", "error", err)
		return doorWait("unread=spec", waitOutage)
	}
	v := classifyRefusal(err)
	switch v.class {
	case refusalPause:
		log.WarnContext(ctx, "an event source may no longer run what it runs", "code", v.code, "error", err)
		switch reason, changed, err := s.pauseSource(ctx, call, eventsources.PauseInstanceFailed, v.code, ""); {
		case err != nil:
			log.ErrorContext(ctx, "an event source could not be paused", "code", v.code, "error", err)
			return doorWait("", outageVerdict(err).waitCause())
		case changed != nil:
			// Judged again under the document that stands, at the next call.
			return doorWait("changed=document", waitOutage)
		default:
			return doorPaused(reason, v.code)
		}
	case refusalWait:
		log.WarnContext(ctx, "whether an event source may run could not be judged now", "cause", v.cause, "error", err)
		return doorWait("", v.cause)
	}
	// Uncertain, at a confirmation, is a wait too: the source's messages are
	// not what it is about, and the Orker bounds how long it goes unjudged.
	log.ErrorContext(ctx, "whether an event source may run could not be judged", "code", v.code, "error", err)
	return doorWait("unread=policy", waitOutage)
}

// waitCause is what a wait for v waits for.
func (v verdict) waitCause() string {
	if v.class == refusalWait {
		return v.cause
	}
	return waitOutage
}

// ReportInstance passes an instance's report on to its source's workflow,
// which takes it while it waits for one.
func (s *Service) ReportInstance(ctx context.Context, req *connect.Request[orkstackv1.ReportInstanceRequest]) (*connect.Response[orkstackv1.ReportInstanceResponse], error) {
	answered := func(a *orkstackv1.DoorAnswer) (*connect.Response[orkstackv1.ReportInstanceResponse], error) {
		return connect.NewResponse(&orkstackv1.ReportInstanceResponse{Answer: a}), nil
	}
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	what := req.Msg.GetWhat()
	if _, known := orkstackv1.InstanceReport_name[int32(what)]; !known || what == orkstackv1.InstanceReport_INSTANCE_REPORT_UNSPECIFIED {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("a report of kind %d is not one", what))
	}
	ctx, cancel := context.WithTimeout(ctx, doorTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeReport, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer != nil {
		return answered(answer)
	}
	log := s.logger.With("event_source_id", call.ref.SourceID, "index", call.ref.Index, "epoch", call.ref.Epoch, "what", what.String())
	if s.sourceWorkflows == nil {
		log.WarnContext(ctx, "an instance's report reached no workflow: this engine tells none")
		return answered(doorWait("", waitOutage))
	}
	reason := ""
	if what == orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID {
		reason = eventsources.SafeText(req.Msg.GetReason(), maxReportReason)
	}
	if err := s.sourceWorkflows.Report(ctx, call.ref, what, reason); err != nil {
		log.WarnContext(ctx, "an instance's report did not reach its source's workflow", "error", err)
		return answered(doorWait("", outageVerdict(err).waitCause()))
	}
	return answered(&orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_OK})
}

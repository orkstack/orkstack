package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// How long a call, other than a start, and its steps may take. The whole is
// under the 30 s its Orker waits for, so a slow step is answered as one.
const (
	doorTimeout    = 25 * time.Second
	claimTimeout   = 10 * time.Second
	resolveTimeout = 10 * time.Second
	// tidyTimeout bounds what is done best-effort once an answer stands.
	tidyTimeout = 3 * time.Second
)

// messageClaims binds messages to run ids. A test substitutes a store that
// fails.
type messageClaims interface {
	Get(ctx context.Context, scope artifacts.ClaimScope, identity string) (artifacts.MessageClaim, bool, error)
	Create(ctx context.Context, scope artifacts.ClaimScope, identity, runID string) (artifacts.MessageClaim, error)
	MarkStarted(ctx context.Context, scope artifacts.ClaimScope, identity, runID string, at time.Time) error
}

// stagedParts is what Orkers staged for messages.
type stagedParts interface {
	RemoveStaged(ctx context.Context, namespace, sourceID, identity string) error
}

var bodyDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// doorLog is the log of one call about one message. A message's identity is
// its publisher's text and is in no engine log.
func (s *Service) doorLog(call *heldCall) *slog.Logger {
	return s.logger.With("namespace", call.ref.Namespace, "event_source", call.ref.Name, "event_source_id", call.ref.SourceID,
		"index", call.ref.Index, "epoch", call.ref.Epoch)
}

// doorReject rejects the message.
func doorReject(reason string) *orkstackv1.DoorAnswer {
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REJECT, Reason: reason}
}

// doorRefresh has the message judged again by how src's are judged now.
func (s *Service) doorRefresh(ctx context.Context, call *heldCall, src *orkstackv1.EventSource) *orkstackv1.DoorAnswer {
	spec, err := s.judgingSpec(src)
	if err != nil {
		s.doorLog(call).ErrorContext(ctx, "a source's judging spec could not be made", "error", err)
		return doorRefused(eventsources.ReasonDocumentUnreadable)
	}
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFRESH, Judging: spec}
}

// doorUnread answers a read or a write that was not had: a wait, or, where it
// would fail again however often it is tried, uncertain.
func (s *Service) doorUnread(ctx context.Context, call *heldCall, what string, err error) *orkstackv1.DoorAnswer {
	return s.doorVerdict(ctx, call, what, outageVerdict(err), err)
}

// doorRefusal answers why a message's run cannot be started.
func (s *Service) doorRefusal(ctx context.Context, call *heldCall, what string, err error) *orkstackv1.DoorAnswer {
	return s.doorVerdict(ctx, call, what, classifyRefusal(err), err)
}

func (s *Service) doorVerdict(ctx context.Context, call *heldCall, what string, v verdict, err error) *orkstackv1.DoorAnswer {
	log := s.doorLog(call).With("step", what, "code", v.code, "cause", v.cause)
	if v.class == refusalReject {
		// What a message is refused for may quote its values: its code alone
		// is logged.
		log.InfoContext(ctx, "a message can never be accepted")
		return doorReject(v.code)
	}
	log = log.With("error", err)
	switch v.class {
	case refusalWait:
		log.WarnContext(ctx, "a message's run cannot be started now")
		return doorWait("", v.cause)
	case refusalPause:
		switch reason, changed, err := s.pauseSource(ctx, call, eventsources.PauseRefused, v.code, ""); {
		case err != nil:
			log.ErrorContext(ctx, "an event source could not be paused", "pause_error", err)
			return doorWait("", outageVerdict(err).waitCause())
		case changed != nil:
			// What was judged is not what stands.
			return s.doorRefresh(ctx, call, changed)
		default:
			return doorPaused(reason, v.code)
		}
	}
	log.ErrorContext(ctx, "whether a message's run can be started is not classed")
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_UNCERTAIN, RefusalCode: v.code}
}

// envelopeMessage is the request's message as the package reads one, with no
// body.
func envelopeMessage(e *orkstackv1.MessageEnvelope) eventsources.Message {
	msg := eventsources.Message{ID: e.GetId(), Subject: e.GetSubject(), Group: e.GetGroup()}
	if e.GetPublishedAt().IsValid() {
		msg.PublishedAt = e.GetPublishedAt().AsTime()
	}
	if len(e.GetAttributes()) > 0 {
		msg.Attributes = make(map[string][]string, len(e.GetAttributes()))
		for name, v := range e.GetAttributes() {
			msg.Attributes[name] = v.GetValues()
		}
	}
	return msg
}

// validIdentity reports whether id can name a message's claim.
func validIdentity(id string) bool {
	return id != "" && eventsources.CheckEnvelope(&orkstackv1.EventSource{}, eventsources.Message{ID: id}, 0, 0) == nil
}

// claimScope is what a message's identity is unique in: the engine's own
// words for the source the caller holds.
func (c *heldCall) claimScope() artifacts.ClaimScope {
	return artifacts.ClaimScope{Namespace: c.src.GetNamespace(), Source: c.src.GetName(), Orkflow: c.src.GetOrkflowName()}
}

// messageRun is what a message's claim says: the run id it is bound to, and
// whether that run exists.
type messageRun struct {
	runID string
	// marked is a run whose started marker was read; nothing else was asked
	// about it.
	marked bool
	// known is what the cluster or the archive says of a run that is not
	// marked, where it is the source's namespace's.
	known knownRun
}

func (r messageRun) exists() bool { return r.marked || r.known.found }

// claimedRun reads the message's claim and, where no marker answers for it,
// asks the cluster and then the archive whether the run it names exists: a
// run archived and retired between the two is found. A message with no claim
// has no run id.
func (s *Service) claimedRun(ctx context.Context, call *heldCall, identity string) (messageRun, *orkstackv1.DoorAnswer) {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	log := s.doorLog(call)
	claim, found, err := s.messageClaims.Get(ctx, call.claimScope(), identity)
	switch {
	case err != nil:
		return messageRun{}, s.claimRefused(ctx, call, err)
	case !found:
		return messageRun{}, nil
	}
	run := messageRun{runID: claim.RunID, marked: !claim.Started.IsZero()}
	if run.marked {
		return run, nil
	}
	if claim.MarkerUnreadable {
		log.ErrorContext(ctx, "a message's started marker cannot be read; its run is looked for instead", "run_id", claim.RunID)
	}
	known, err := s.readKnownRun(ctx, claim.RunID)
	switch {
	case errors.Is(err, engine.ErrRunRecordUndecodable):
		log.ErrorContext(ctx, "the record of a message's run does not decode, and never will", "run_id", claim.RunID, "error", err)
		return messageRun{}, doorReject(eventsources.ReasonMessageUnprocessable)
	case err != nil:
		return messageRun{}, s.doorUnread(ctx, call, "run", err)
	case known.found && known.namespace == artifacts.ResolveNamespace(call.src.GetNamespace()):
		run.known = known
	}
	return run, nil
}

// claimRefused answers a claim that cannot be read, or written.
func (s *Service) claimRefused(ctx context.Context, call *heldCall, err error) *orkstackv1.DoorAnswer {
	log := s.doorLog(call)
	switch {
	case errors.Is(err, artifacts.ErrClaimVersion):
		log.WarnContext(ctx, "a message's claim was written by a newer engine", "error", err)
		return doorWait("", waitRollout)
	case errors.Is(err, artifacts.ErrClaimCorrupt):
		log.ErrorContext(ctx, "a message's claim does not decode, and never will", "error", err)
		return doorReject(eventsources.ReasonMessageUnprocessable)
	case errors.Is(err, artifacts.ErrClaimIncomplete):
		// No store writes half an object, so another read does not change it.
		log.ErrorContext(ctx, "a message's claim is not a whole document; a person must look at it", "error", err)
		return doorWait("unread=claim", waitOutage)
	}
	return s.doorUnread(ctx, call, "claim", err)
}

// knownRun is what the cluster, or else the archive, says of a run id.
type knownRun struct {
	// found is a run either has; archived one only the archive has.
	found, archived bool
	namespace       string
	origin          *orkstackv1.RunOrigin
}

// startedBy reports whether the run is the one a message of identity started
// in namespace.
func (r knownRun) startedBy(namespace, identity string) bool {
	return r.found && r.namespace == artifacts.ResolveNamespace(namespace) &&
		r.origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE && r.origin.GetReference() == identity
}

// readKnownRun asks the cluster, by the run's start event alone, and then the
// archive: a run archived and retired between the two is found.
func (s *Service) readKnownRun(ctx context.Context, runID string) (knownRun, error) {
	binding, err := s.router.Locate(ctx, runID)
	if err != nil {
		return knownRun{}, err
	}
	if binding.Reader == nil {
		return knownRun{}, errors.New("this engine reads no runs")
	}
	start, err := binding.Reader.RunStart(ctx, runID)
	switch {
	case err == nil:
		return knownRun{found: true, namespace: artifacts.ResolveNamespace(start.Orkflow.GetNamespace()), origin: start.Plan.Origin.Proto()}, nil
	case !errors.Is(err, artifacts.ErrNotFound):
		return knownRun{}, err
	case s.archive == nil:
		return knownRun{}, errors.New("this engine reads no run archive")
	}
	run, err := s.archive.LoadRun(ctx, runID)
	switch {
	case err == nil:
		return knownRun{found: true, archived: true, namespace: artifacts.ResolveNamespace(run.GetNamespace()), origin: run.GetOrigin()}, nil
	case errors.Is(err, artifacts.ErrNotFound):
		return knownRun{}, nil
	}
	return knownRun{}, err
}

// markStarted writes the marker of a message's run.
func (s *Service) markStarted(ctx context.Context, call *heldCall, identity, runID string) error {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	return s.messageClaims.MarkStarted(ctx, call.claimScope(), identity, runID, s.now())
}

// removeStaged removes what is staged for a message that has its run. What
// it leaves, the prefix's lifecycle rule removes.
func (s *Service) removeStaged(ctx context.Context, call *heldCall, identity string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tidyTimeout)
	defer cancel()
	if err := s.stagedParts.RemoveStaged(ctx, call.src.GetNamespace(), call.src.GetId(), identity); err != nil {
		s.doorLog(call).WarnContext(ctx, "what was staged for a message that has its run was left; it expires by itself", "error", err)
	}
}

// resolvedSource is the version a source starts now, and how its messages'
// values are judged by it.
type resolvedSource struct {
	orkflow *orkstackv1.Orkflow
	judge   eventsources.Judge
}

// resolveSource reads the version the source's own rule gives now and holds
// its mapping to it. Its error is classified.
func (s *Service) resolveSource(ctx context.Context, call *heldCall) (resolvedSource, error) {
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	wf, inputs, err := s.sourceOrkflow(ctx, call.src.GetNamespace(), call.src)
	if err != nil {
		return resolvedSource{}, err
	}
	if err := eventsources.CheckMapping(call.src, inputs); err != nil {
		return resolvedSource{}, err
	}
	limits := s.eventSourceSettings.Limits
	return resolvedSource{orkflow: wf, judge: eventsources.Judge{Inputs: inputs, MaxInputBytes: limits.MaxInputBytes, MaxMessageBytes: limits.MaxMessageBytes}}, nil
}

// partsToStage are the parts of a message the source binds, in their inputs'
// order.
func partsToStage(src *orkstackv1.EventSource) []*orkstackv1.PartToUpload {
	var parts []*orkstackv1.PartToUpload
	for _, input := range slices.Sorted(maps.Keys(src.GetArtifacts())) {
		part := src.GetArtifacts()[input]
		parts = append(parts, &orkstackv1.PartToUpload{Input: input, Part: part, ContentType: partContentType(src, part)})
	}
	return parts
}

func partContentType(src *orkstackv1.EventSource, part orkstackv1.MessagePart) string {
	if part == orkstackv1.MessagePart_MESSAGE_PART_BODY && src.GetFormat() != orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON {
		return "application/octet-stream"
	}
	return "application/json"
}

// checkOffer refuses what no Orker's offer holds.
func checkOffer(req *orkstackv1.OfferMessageRequest) error {
	switch {
	case req.GetPreRejection() != "" && !eventsources.IsPreRejection(req.GetPreRejection()):
		return fmt.Errorf("pre_rejection %q is no reason an Orker rejects a message with", req.GetPreRejection())
	case req.GetBodyBytes() < 0 || req.GetDeliveryCount() < 0 || req.GetMaxDeliveries() < 0:
		return errors.New("a size and a count are not negative")
	case req.GetBodySha256() != "" && !bodyDigest.MatchString(req.GetBodySha256()):
		return errors.New("body_sha256 is 64 characters of lowercase hex")
	}
	return nil
}

// OfferMessage judges one message before anything is staged for it. It starts
// nothing and claims nothing: all it may write is the marker of a run that
// exists. Nothing about the message or its source is refused before the
// message's claim was read.
func (s *Service) OfferMessage(ctx context.Context, req *connect.Request[orkstackv1.OfferMessageRequest]) (*connect.Response[orkstackv1.OfferMessageResponse], error) {
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := checkOffer(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ctx, cancel := context.WithTimeout(ctx, doorTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeOffer, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer == nil {
		if answer, err = s.offer(ctx, call, req.Msg); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&orkstackv1.OfferMessageResponse{Answer: answer}), nil
}

func (s *Service) offer(ctx context.Context, call *heldCall, req *orkstackv1.OfferMessageRequest) (*orkstackv1.DoorAnswer, error) {
	if req.GetInstance().GetJudgingRevision() != call.src.GetJudgingRevision() {
		return s.doorRefresh(ctx, call, call.src), nil
	}
	msg := envelopeMessage(req.GetMessage())
	limits := s.eventSourceSettings.Limits
	rejection := ""
	if err := eventsources.CheckEnvelope(call.src, msg, req.GetBodyBytes(), limits.MaxMessageBytes); err != nil {
		rejection = err.Reason
	}
	if req.GetPreRejection() != "" {
		rejection = req.GetPreRejection()
	}
	// Only a body over its bound was never read, and has no digest.
	if req.GetBodySha256() == "" && rejection == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("body_sha256 is empty for a body within its bound"))
	}
	if !validIdentity(msg.ID) || rejection == eventsources.ReasonMessageIdentityInvalid {
		// What is no identity names no claim.
		return doorReject(eventsources.ReasonMessageIdentityInvalid), nil
	}

	run, answer := s.claimedRun(ctx, call, msg.ID)
	switch {
	case answer != nil:
		return answer, nil
	case run.exists():
		if !run.marked {
			if err := s.markStarted(ctx, call, msg.ID, run.runID); err != nil {
				s.doorLog(call).WarnContext(ctx, "a message's run was found and its marker not written; its next delivery looks for it again", "run_id", run.runID, "error", err)
			}
		}
		s.removeStaged(ctx, call, msg.ID)
		return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_DUPLICATE, RunId: run.runID, RetiredRun: run.known.archived}, nil
	case rejection != "":
		return doorReject(rejection), nil
	}

	resolved, err := s.resolveSource(ctx, call)
	if err != nil {
		return s.doorRefusal(ctx, call, "resolve", err), nil
	}
	if _, err := eventsources.JudgeExtracted(call.src, req.GetExtracted(), resolved.judge); err != nil {
		return doorReject(err.Reason), nil
	}
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_PROCEED, Parts: partsToStage(call.src),
		OrkflowVersion: resolved.orkflow.GetVersion()}, nil
}

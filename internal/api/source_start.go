package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// How long a message's start and its steps may take. The whole is under the
// 180 s its Orker waits for.
const (
	messageStartTimeout = 165 * time.Second
	prepareTimeout      = 30 * time.Second
	copyTimeout         = 30 * time.Second
	runObjectsTimeout   = 30 * time.Second
	runStartTimeout     = 15 * time.Second
)

// maxMetadataPart bounds a message's metadata document, whose bounded facts
// JSON may write six times as long.
const maxMetadataPart = 1 << 20

// maxUploadedParts is the most artifact inputs an Orkflow declares.
const maxUploadedParts = 16

// stagedArtifacts puts what an Orker staged where a run reads it, at the
// reference it predicts.
type stagedArtifacts interface {
	artifacts.StagedCopier
	artifacts.RefPredictor
}

// checkStart refuses what no Orker's start holds.
func checkStart(req *orkstackv1.StartMessageRunRequest) error {
	if n := len(req.GetParts()); n > maxUploadedParts {
		return fmt.Errorf("%d parts; a message has at most %d", n, maxUploadedParts)
	}
	seen := map[string]bool{}
	for _, part := range req.GetParts() {
		switch {
		case seen[part.GetInput()]:
			return fmt.Errorf("input %q is staged twice", part.GetInput())
		case !bodyDigest.MatchString(part.GetSha256()):
			return errors.New("a part's sha256 is 64 characters of lowercase hex")
		case part.GetSizeBytes() < 0:
			return errors.New("a part's size is not negative")
		}
		seen[part.GetInput()] = true
	}
	return nil
}

// StartMessageRun starts the message's run from what was staged, or finds it.
// Everything the run could be refused for is asked before a run id exists and
// before anything is written; the claim then binds the message to its id, and
// any delivery may complete a start that was not.
func (s *Service) StartMessageRun(ctx context.Context, req *connect.Request[orkstackv1.StartMessageRunRequest]) (*connect.Response[orkstackv1.StartMessageRunResponse], error) {
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := checkStart(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ctx, cancel := context.WithTimeout(ctx, messageStartTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeStart, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer == nil {
		if answer, err = s.startMessage(ctx, call, req.Msg); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&orkstackv1.StartMessageRunResponse{Answer: answer}), nil
}

func (s *Service) startMessage(ctx context.Context, call *heldCall, req *orkstackv1.StartMessageRunRequest) (*orkstackv1.DoorAnswer, error) {
	if req.GetInstance().GetJudgingRevision() != call.src.GetJudgingRevision() {
		return s.doorRefresh(ctx, call, call.src), nil
	}
	msg := envelopeMessage(req.GetMessage())
	if !validIdentity(msg.ID) {
		return doorReject(eventsources.ReasonMessageIdentityInvalid), nil
	}
	if err := s.boundParts(call.src, req.GetParts()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	run, answer := s.claimedRun(ctx, call, msg.ID)
	switch {
	case answer != nil:
		return answer, nil
	case run.exists():
		return s.foundRun(ctx, call, req, run), nil
	}
	if err := eventsources.CheckEnvelope(call.src, msg, 0, 0); err != nil {
		return doorReject(err.Reason), nil
	}

	sub, answer := s.prepareMessage(ctx, call, msg.ID, req)
	if answer != nil {
		return answer, nil
	}
	if run.runID == "" {
		if run.runID, answer = s.claimMessage(ctx, call, msg.ID); answer != nil {
			return answer, nil
		}
	}
	// From here the message's run is admitted and has its id: what fails is
	// made again by the message's next try, under the same id.
	if sub.plan.InputArtifacts, answer = s.copyParts(ctx, call, msg.ID, run.runID, req.GetParts()); answer != nil {
		return answer, nil
	}
	if err := s.writeMessageObjects(ctx, run.runID, sub); err != nil {
		return s.doorUnread(ctx, call, "objects", err), nil
	}
	err := startBounded(ctx, run.runID, sub)
	switch {
	case err == nil:
		if s.acceptedRuns != nil {
			s.acceptedRuns.NoteAccepted(run.runID, time.Now())
		}
		return s.startedRun(ctx, call, msg.ID, run.runID, orkstackv1.Disposition_DISPOSITION_ACCEPTED), nil
	case errors.Is(err, engine.ErrRunIDTaken):
		return s.takenRun(ctx, call, req, run.runID), nil
	}
	// It may have started, or not: the claim names it, and the next try looks.
	return s.doorUnread(ctx, call, "start", err), nil
}

// boundParts holds what was staged to what the source binds. The judging spec
// the caller has is the document's, so an honest one's agree.
func (s *Service) boundParts(src *orkstackv1.EventSource, parts []*orkstackv1.UploadedPart) error {
	if len(parts) != len(src.GetArtifacts()) {
		return fmt.Errorf("%d parts are staged, and the source binds %d", len(parts), len(src.GetArtifacts()))
	}
	for _, part := range parts {
		bound, ok := src.GetArtifacts()[part.GetInput()]
		if !ok {
			return errors.New("a part is staged for an input the source does not bind")
		}
		limit := int64(maxMetadataPart)
		if bound == orkstackv1.MessagePart_MESSAGE_PART_BODY {
			limit = eventsources.MaxBodyBytes(src, s.eventSourceSettings.Limits.MaxMessageBytes)
		}
		if part.GetSizeBytes() > limit {
			return fmt.Errorf("the part of input %q is %d bytes, over %d", part.GetInput(), part.GetSizeBytes(), limit)
		}
	}
	return nil
}

// prepareMessage asks everything the message's run could be refused for, with
// the size of what will be started, and writes nothing.
func (s *Service) prepareMessage(ctx context.Context, call *heldCall, identity string, req *orkstackv1.StartMessageRunRequest) (submission, *orkstackv1.DoorAnswer) {
	ctx, cancel := context.WithTimeout(ctx, prepareTimeout)
	defer cancel()
	resolved, err := s.resolveSource(ctx, call)
	if err != nil {
		return submission{}, s.doorRefusal(ctx, call, "resolve", err)
	}
	mapped, rejection := eventsources.JudgeExtracted(call.src, req.GetExtracted(), resolved.judge)
	if rejection != nil {
		return submission{}, doorReject(rejection.Reason)
	}
	input, err := mappedInputPayload(mapped)
	if err != nil {
		return submission{}, s.doorRefusal(ctx, call, "input", err)
	}
	// Any new run id has one length, so the references predicted for one
	// minted and thrown away are of the size of the ones that will be sent.
	sub, err := s.prepareWith(ctx, messageSubmit(call.src, resolved, input), messageOrigin(call.src, identity),
		prepareOptions{received: s.predictParts(call.src, s.newRunID(), req.GetParts()), pools: s.doorPools})
	if err != nil {
		return submission{}, s.doorRefusal(ctx, call, "prepare", err)
	}
	if sub.admission != nil || unadmittedPlan(sub.plan) {
		// A run a message starts holds no identity yet, so one that would is
		// never started.
		return submission{}, s.doorRefusal(ctx, call, "prepare", refused(refusalPause, eventsources.ReasonOrkflowUnsupported,
			errors.New("the run would hold task identities, which a message's run is not given")))
	}
	return sub, nil
}

// messageSubmit is what a message's start asks prepare.
func messageSubmit(src *orkstackv1.EventSource, resolved resolvedSource, input *orkstackv1.Payload) *orkstackv1.SubmitRunRequest {
	return &orkstackv1.SubmitRunRequest{Namespace: src.GetNamespace(), OrkflowName: src.GetOrkflowName(),
		OrkflowVersion: resolved.orkflow.GetVersion(), Input: input}
}

// messageOrigin is the origin of the run a message of identity starts.
func messageOrigin(src *orkstackv1.EventSource, identity string) *orkstackv1.RunOrigin {
	return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE, Id: src.GetId(), Reference: identity,
		Description: boundOriginAssertion(src.GetName())}
}

// partProvenance is where a message's parts stand under its run.
func partProvenance(src *orkstackv1.EventSource, runID string) artifacts.Provenance {
	return artifacts.Provenance{Orkflow: src.GetOrkflowName(), RunID: runID, TaskID: dsl.InputsProducer}
}

func receivedPart(runID, input string, ref artifacts.ArtifactRef) engine.NamedArtifactPlan {
	return engine.NamedArtifactPlan{Name: input, RunID: runID, TaskID: dsl.InputsProducer, Artifact: engine.ArtifactRefPlan{
		Name: ref.Name, URI: ref.URI, ContentType: ref.ContentType, SizeBytes: ref.SizeBytes, SHA256: ref.SHA256, Metadata: maps.Clone(ref.Metadata)}}
}

// inInputOrder is the parts by the inputs they become.
func inInputOrder(parts []*orkstackv1.UploadedPart) []*orkstackv1.UploadedPart {
	return slices.SortedFunc(slices.Values(parts), func(a, b *orkstackv1.UploadedPart) int { return cmp.Compare(a.GetInput(), b.GetInput()) })
}

// predictParts are the references a copy of the parts under runID will have.
func (s *Service) predictParts(src *orkstackv1.EventSource, runID string, parts []*orkstackv1.UploadedPart) []engine.NamedArtifactPlan {
	var out []engine.NamedArtifactPlan
	for _, part := range inInputOrder(parts) {
		ref := s.staged.RefFor(partProvenance(src, runID), part.GetInput(), part.GetSha256(), part.GetSizeBytes(),
			partContentType(src, src.GetArtifacts()[part.GetInput()]))
		out = append(out, receivedPart(runID, part.GetInput(), ref))
	}
	return out
}

// copyParts puts the parts where the run reads them. A part that is not
// there, or is not what was said of it, is staged again: the bad one is
// removed first, since staging never writes over what is there.
func (s *Service) copyParts(ctx context.Context, call *heldCall, identity, runID string, parts []*orkstackv1.UploadedPart) ([]engine.NamedArtifactPlan, *orkstackv1.DoorAnswer) {
	var out []engine.NamedArtifactPlan
	for _, part := range inInputOrder(parts) {
		ref, err := s.copyPart(ctx, call, identity, runID, part)
		switch {
		case errors.Is(err, artifacts.ErrStagedMissing), errors.Is(err, artifacts.ErrStagedMismatch):
			s.doorLog(call).WarnContext(ctx, "a message's staged part is asked for again", "run_id", runID, "input", part.GetInput(), "error", err)
			if errors.Is(err, artifacts.ErrStagedMismatch) {
				s.removeStaged(ctx, call, identity)
			}
			return nil, &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN}
		case err != nil:
			return nil, s.doorUnread(ctx, call, "copy", err)
		}
		out = append(out, receivedPart(runID, part.GetInput(), ref))
	}
	return out, nil
}

func (s *Service) copyPart(ctx context.Context, call *heldCall, identity, runID string, part *orkstackv1.UploadedPart) (artifacts.ArtifactRef, error) {
	ctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()
	src := call.src
	return s.staged.CopyStaged(ctx, artifacts.StagedPart{Namespace: src.GetNamespace(), SourceID: src.GetId(), Identity: identity,
		Digest: part.GetSha256(), Size: part.GetSizeBytes()}, partProvenance(src, runID), part.GetInput(),
		partContentType(src, src.GetArtifacts()[part.GetInput()]))
}

// claimMessage binds the message to a freshly minted run id, or answers the
// id another delivery bound it to first.
func (s *Service) claimMessage(ctx context.Context, call *heldCall, identity string) (string, *orkstackv1.DoorAnswer) {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	claim, err := s.messageClaims.Create(ctx, call.claimScope(), identity, s.newRunID())
	if err != nil {
		return "", s.claimRefused(ctx, call, err)
	}
	return claim.RunID, nil
}

// writeMessageObjects writes what else the start references.
func (s *Service) writeMessageObjects(ctx context.Context, runID string, sub submission) error {
	ctx, cancel := context.WithTimeout(ctx, runObjectsTimeout)
	defer cancel()
	return s.writeRunObjects(ctx, runID, sub)
}

func startBounded(ctx context.Context, runID string, sub submission) error {
	ctx, cancel := context.WithTimeout(ctx, runStartTimeout)
	defer cancel()
	return sub.binding.Starter.StartRun(ctx, runID, sub.startDefinition(), sub.input, sub.plan)
}

// atMostOnce is a source whose broker keeps no copy: no delivery comes back
// to look for a run.
func (c *heldCall) atMostOnce() bool {
	return c.src.GetDelivery() == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE
}

// startedRun answers a message whose run is there, once its marker is: a
// message is settled only for a run its next delivery would find. A source
// nothing is delivered to again is answered without one, since its marker
// serves nobody and its message would be lost waiting for it.
func (s *Service) startedRun(ctx context.Context, call *heldCall, identity, runID string, as orkstackv1.Disposition) *orkstackv1.DoorAnswer {
	if err := s.markStarted(ctx, call, identity, runID); err != nil {
		if !call.atMostOnce() {
			return s.doorUnread(ctx, call, "marker", err)
		}
		s.doorLog(call).WarnContext(ctx, "the marker of an at-most-once message's run was not written", "run_id", runID, "error", err)
	}
	s.removeStaged(ctx, call, identity)
	return &orkstackv1.DoorAnswer{Disposition: as, RunId: runID}
}

// foundRun answers a start whose message had its run already.
func (s *Service) foundRun(ctx context.Context, call *heldCall, req *orkstackv1.StartMessageRunRequest, run messageRun) *orkstackv1.DoorAnswer {
	identity := req.GetMessage().GetId()
	as := orkstackv1.Disposition_DISPOSITION_DUPLICATE
	if req.GetRetryOfOwnStart() {
		// This exchange's start, its answer lost. A record that is never to be
		// had leaves a duplicate, which settles the message just the same.
		known := run.known
		if run.marked {
			readCtx, cancel := context.WithTimeout(ctx, claimTimeout)
			var err error
			known, err = s.readKnownRun(readCtx, run.runID)
			cancel()
			if err != nil && !errors.Is(err, engine.ErrRunRecordUndecodable) {
				return s.doorUnread(ctx, call, "run", err)
			}
		}
		if known.startedBy(call.src.GetNamespace(), identity) {
			as = orkstackv1.Disposition_DISPOSITION_ACCEPTED
		}
	}
	if run.marked {
		s.removeStaged(ctx, call, identity)
		return &orkstackv1.DoorAnswer{Disposition: as, RunId: run.runID}
	}
	answer := s.startedRun(ctx, call, identity, run.runID, as)
	answer.RetiredRun = run.known.archived && answer.GetDisposition() == orkstackv1.Disposition_DISPOSITION_DUPLICATE
	return answer
}

// takenRun answers a start whose id the cluster holds: by this message's run,
// which another delivery or a lost answer started, or by another's.
func (s *Service) takenRun(ctx context.Context, call *heldCall, req *orkstackv1.StartMessageRunRequest, runID string) *orkstackv1.DoorAnswer {
	readCtx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	known, err := s.readKnownRun(readCtx, runID)
	switch {
	case errors.Is(err, engine.ErrRunRecordUndecodable):
		s.doorLog(call).ErrorContext(ctx, "the record of the run that holds a message's id does not decode, and never will", "run_id", runID, "error", err)
		return doorReject(eventsources.ReasonMessageUnprocessable)
	case err != nil:
		return s.doorUnread(ctx, call, "run", err)
	case !known.found:
		// The cluster refused the id and then held no run of it.
		return doorWait("", waitOutage)
	case !known.startedBy(call.src.GetNamespace(), req.GetMessage().GetId()):
		s.doorLog(call).ErrorContext(ctx, "a message's run id is held by a run no message of its identity started", "run_id", runID)
		return doorReject(eventsources.ReasonMessageRunIDConflict)
	}
	as := orkstackv1.Disposition_DISPOSITION_DUPLICATE
	if req.GetRetryOfOwnStart() {
		as = orkstackv1.Disposition_DISPOSITION_ACCEPTED
	}
	return s.startedRun(ctx, call, req.GetMessage().GetId(), runID, as)
}

package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// probeAge is how long one probe's answer serves every instance that
	// asks for the same.
	probeAge = 10 * time.Second
	// probeTimeout bounds a probe, under the 30 s its Orker waits for, and
	// probeSideTimeout one side of it.
	probeTimeout     = 20 * time.Second
	probeSideTimeout = 5 * time.Second
	// maxKeptProbes bounds what is kept.
	maxKeptProbes = 1024
)

// startProbe writes a few bytes where the engine writes a message's claim,
// and removes them.
type startProbe interface {
	ProbeWrite(ctx context.Context) error
}

// keptProbes runs each probe at most once in probeAge, refused or not, and
// shares its answer with whoever asks meanwhile.
type keptProbes struct {
	now  func() time.Time
	mu   sync.Mutex
	kept map[string]*probeAnswer
}

// probeAnswer is one probe's answer; its turn is held while it runs.
type probeAnswer struct {
	turn chan struct{}
	at   time.Time
	err  error
}

func newKeptProbes(now func() time.Time) *keptProbes {
	return &keptProbes{now: now, kept: map[string]*probeAnswer{}}
}

// run answers the kept answer, or probe's. One that waits for another's leaves
// with ctx.
func (k *keptProbes) run(ctx context.Context, key string, probe func() error) error {
	k.mu.Lock()
	p, ok := k.kept[key]
	if !ok {
		if len(k.kept) >= maxKeptProbes {
			clear(k.kept)
		}
		p = &probeAnswer{turn: make(chan struct{}, 1)}
		k.kept[key] = p
	}
	k.mu.Unlock()
	select {
	case p.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.turn }()
	at := k.now()
	if age := at.Sub(p.at); !p.at.IsZero() && age >= 0 && age < probeAge {
		return p.err
	}
	p.err = probe()
	p.at = at
	return p.err
}

// ProbeEventSource asks whether a start could complete now, for an instance
// that waits and holds no message: what prepare would refuse, a write where a
// claim is written, and a read on the path a start takes. A fault that only a
// real start meets passes it.
func (s *Service) ProbeEventSource(ctx context.Context, req *connect.Request[orkstackv1.ProbeEventSourceRequest]) (*connect.Response[orkstackv1.ProbeEventSourceResponse], error) {
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, doorTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeProbe, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer == nil {
		answer = s.probeSource(ctx, call)
	}
	return connect.NewResponse(&orkstackv1.ProbeEventSourceResponse{Answer: answer}), nil
}

func (s *Service) probeSource(ctx context.Context, call *heldCall) *orkstackv1.DoorAnswer {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	// A probe's answer is shared, so one caller that leaves does not end it.
	asking, done := context.WithTimeout(context.WithoutCancel(ctx), probeTimeout)
	defer done()
	src := call.src
	// The document's revision is in the key: a source that changed is asked
	// about afresh.
	input, err := proto.MarshalOptions{Deterministic: true}.Marshal(src.GetInput())
	if err != nil {
		return s.doorRefusal(ctx, call, "input", err)
	}
	key := strings.Join([]string{src.GetNamespace(), src.GetOrkflowName(), src.GetOrkflowVersion(), fmt.Sprintf("%x", sha256.Sum256(input)), src.GetId(), src.GetEtag()}, "\x1f")
	err = s.probes.run(ctx, key, func() error { return s.probePlan(asking, call) })
	if probeRefuses(err) {
		answer := s.doorRefusal(ctx, call, "probe", err)
		if answer.GetDisposition() == orkstackv1.Disposition_DISPOSITION_REFRESH {
			// It holds no message to judge again; its next probe asks afresh.
			return doorWait("changed=document", waitOutage)
		}
		return answer
	}
	err = s.probes.run(ctx, "write", func() error {
		ctx, cancel := context.WithTimeout(asking, probeSideTimeout)
		defer cancel()
		return s.startProbe.ProbeWrite(ctx)
	})
	if err != nil {
		return s.doorUnread(ctx, call, "probe write", err)
	}
	err = s.probes.run(ctx, "cluster", func() error { return s.probeCluster(asking) })
	if err != nil {
		return s.doorUnread(ctx, call, "probe cluster", err)
	}
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_OK}
}

// probeRefuses reports a refusal a probe answers. What a message's own values
// are refused for it does not: it has none of them.
func probeRefuses(err error) bool {
	return err != nil && classifyRefusal(err).class != refusalReject
}

// probePlan asks what a message's start would be refused for, with the
// source's constants for its input and parts of nothing.
func (s *Service) probePlan(ctx context.Context, call *heldCall) error {
	resolved, err := s.resolveSource(ctx, call)
	if err != nil {
		return err
	}
	src := call.src
	parts := make([]*orkstackv1.UploadedPart, 0, len(src.GetArtifacts()))
	for _, part := range partsToStage(src) {
		parts = append(parts, &orkstackv1.UploadedPart{Input: part.GetInput(), Sha256: strings.Repeat("0", sha256.Size*2)})
	}
	_, err = s.prepareWith(ctx, messageSubmit(src, resolved, src.GetInput()), messageOrigin(src, ""), prepareOptions{received: s.predictParts(src, s.newRunID(), parts), pools: s.doorPools})
	return err
}

// probeCluster reads a run id nobody has: that there is none is the answer
// of a cluster that answers.
func (s *Service) probeCluster(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, probeSideTimeout)
	defer cancel()
	runID := s.newRunID()
	binding, err := s.router.Locate(ctx, runID)
	if err != nil {
		return err
	}
	if binding.Reader == nil {
		return errors.New("this engine reads no runs")
	}
	if _, err := binding.Reader.RunStart(ctx, runID); err != nil && !errors.Is(err, artifacts.ErrNotFound) {
		return err
	}
	return nil
}

// breakers are the codes an Orker may ask a pause with: what it counts.
var breakers = map[string]bool{eventsources.PauseRefused: true, eventsources.PauseAllRejected: true, eventsources.PauseSettleFailing: true, eventsources.PauseSettleUnanswered: true}

// uncertainCodes are the refusal codes an answer is uncertain with
// (doorVerdict): the only ones an Orker can have counted.
var uncertainCodes = map[string]bool{codeUnclassified: true, codeStoreRefused: true, codeClusterRefused: true,
	engine.ReasonProfileUnavailable: true, "RESOURCES_UNSATISFIABLE": true, "DEVICES_NOT_A_PROFILE": true, "GPU_WITHOUT_PROFILE": true}

// maxPauseTries bounds the writes of one pause against a document that keeps
// changing.
const maxPauseTries = 3

// RequestEventSourcePause pauses the source for a breaker the Orker counted.
// It is not counted again: what is checked is that the caller holds the
// instance and that the code is one an Orker counts.
func (s *Service) RequestEventSourcePause(ctx context.Context, req *connect.Request[orkstackv1.RequestEventSourcePauseRequest]) (*connect.Response[orkstackv1.RequestEventSourcePauseResponse], error) {
	if err := checkDoorRequest(req.Msg); err != nil {
		return nil, err
	}
	code, refusal := req.Msg.GetCode(), req.Msg.GetRefusalCode()
	switch {
	case !breakers[code]:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("code is no breaker an event source's host counts"))
	case code == eventsources.PauseRefused && !uncertainCodes[refusal]:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("refusal_code is none this service answers"))
	case code != eventsources.PauseRefused:
		// It says nothing of another breaker's pause.
		refusal = ""
	}
	ctx, cancel := context.WithTimeout(ctx, doorTimeout)
	defer cancel()
	call, answer, err := s.sourceCall(ctx, held.PurposeRequestPause, req.Msg.GetInstance())
	if err != nil {
		return nil, err
	}
	if answer == nil {
		answer = s.pauseForBreaker(ctx, call, code, refusal, authn.FromContext(ctx).OrkerID)
	}
	return connect.NewResponse(&orkstackv1.RequestEventSourcePauseResponse{Answer: answer}), nil
}

// pauseForBreaker pauses the source whatever else of its document changed
// meanwhile: a breaker is counted from what the instance met, not from a
// document.
func (s *Service) pauseForBreaker(ctx context.Context, call *heldCall, code, refusal, orker string) *orkstackv1.DoorAnswer {
	for range maxPauseTries {
		reason, changed, err := s.pauseSource(ctx, call, code, refusal, orker)
		if err != nil {
			s.doorLog(call).ErrorContext(ctx, "an event source could not be paused", "code", code, "error", err)
			return doorWait("", outageVerdict(err).waitCause())
		}
		if changed == nil {
			return doorPaused(reason, refusal)
		}
		call = &heldCall{result: call.result, src: changed, ref: call.ref}
	}
	return doorWait("changed=document", waitOutage)
}

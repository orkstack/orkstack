package sourcehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// ErrNoAnswer is a call that got no answer of the engine's: nothing is decided
// about a message or a source from it.
var ErrNoAnswer = errors.New("the engine gave no answer")

// ErrHostNotPermitted is, beside ErrNoAnswer, this Orker's credential lacking a
// permission at the source's pool: the attempt ends with that reason.
var ErrHostNotPermitted = errors.New("this Orker's credential does not hold what hosting this source needs")

// RefusedError is a call that got no answer because each replica it reached said
// that it cannot serve it; Reason is why, where all said the same, and one of
// their own guards where each said one.
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string {
	return fmt.Sprintf("%v: the replica reached on each of %d connections cannot serve the call (%s)", ErrNoAnswer, 1+refusedConnections, e.Reason)
}

func (e *RefusedError) Unwrap() error { return ErrNoAnswer }

const (
	// refusedConnections is how many new connections in a row a call is made
	// on again while each replica it reaches answers that it cannot serve it.
	refusedConnections = 8
	// callBound and startBound are what the engine gives one call, of which
	// its further connections are part.
	callBound  = 30 * time.Second
	startBound = 180 * time.Second

	maxServiceSpecBytes = 64 << 10
	maxJudgingSpecBytes = 128 << 10
)

// HostConn is this Orker's connection to the engine's host service, which every
// instance it hosts calls through.
type HostConn struct {
	dial func() (orkstackv1connect.EventSourceHostServiceClient, func())

	mu  sync.Mutex
	cur *hostClient
}

type hostClient struct {
	orkstackv1connect.EventSourceHostServiceClient
	close func()
}

// NewHostConn calls through what dial gives: a client whose connections are its
// own, and what closes them.
func NewHostConn(dial func() (orkstackv1connect.EventSourceHostServiceClient, func())) *HostConn {
	return &HostConn{dial: dial}
}

// Close closes the connection held.
func (h *HostConn) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cur != nil {
		h.cur.close()
		h.cur = nil
	}
}

func (h *HostConn) client() *hostClient {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cur == nil {
		c, done := h.dial()
		h.cur = &hostClient{EventSourceHostServiceClient: c, close: done}
	}
	return h.cur
}

// drop closes c, unless a call beside this one already replaced it.
func (h *HostConn) drop(c *hostClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cur == c {
		h.cur = nil
	}
	c.close()
}

// DoorClient is the door as one attempt of one instance calls it. It decides
// only from a typed answer, and from a connection's code nothing but that this
// Orker lacks a permission.
type DoorClient struct {
	Conn *HostConn
	// Instance names the attempt; its judging revision is each call's own.
	Instance *orkstackv1.InstanceRef
	// MaxDeliveries is the broker's delivery limit as last reported; nil is
	// not known yet.
	MaxDeliveries func() int
	Now           func() time.Time

	mu           sync.Mutex
	answered     time.Time
	notPermitted string
	// refusal is the last refusal code an answer gave, for a pause asked for.
	refusal string
}

var _ Door = (*DoorClient)(nil)

// LastAnswer is when the engine last answered a call of this attempt.
func (c *DoorClient) LastAnswer() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.answered
}

// NotPermitted is what the engine last said this Orker lacks, until a call is
// answered otherwise.
func (c *DoorClient) NotPermitted() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.notPermitted
}

func (c *DoorClient) ref(revision int64) *orkstackv1.InstanceRef {
	ref := proto.CloneOf(c.Instance)
	ref.JudgingRevision = revision
	return ref
}

// doorResponse is a response that carries a DoorAnswer.
type doorResponse interface{ GetAnswer() *orkstackv1.DoorAnswer }

// call makes one call within bound, on a new connection each time the replica
// reached answers that it cannot serve it. Only an answer of one of wants is
// one.
func call[R doorResponse](ctx context.Context, c *DoorClient, bound time.Duration, rpc func(context.Context, orkstackv1connect.EventSourceHostServiceClient) (R, error),
	wants ...orkstackv1.Disposition) (R, error) {
	var none R
	var last error
	var refused []string
	within, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	for range 1 + refusedConnections {
		client := c.Conn.client()
		resp, err := rpc(within, client)
		if ctx.Err() != nil {
			return none, ctx.Err()
		}
		if err != nil {
			if last == nil || connect.IsWireError(err) || within.Err() != nil {
				return none, c.noAnswer(err)
			}
			// Dialled for a replica's refusal, and not answered: another is
			// asked.
			c.Conn.drop(client)
			last = err
			continue
		}
		got := resp.GetAnswer().GetDisposition()
		if got == orkstackv1.Disposition_DISPOSITION_REFUSED {
			c.Conn.drop(client)
			refused = append(refused, resp.GetAnswer().GetReason())
			last = errors.New("the replica reached cannot serve the call")
			continue
		}
		if !slices.Contains(wants, got) {
			return none, fmt.Errorf("%w: it answered %v", ErrNoAnswer, got)
		}
		c.mu.Lock()
		c.answered, c.notPermitted = c.Now(), ""
		c.mu.Unlock()
		return resp, nil
	}
	if len(refused) == 1+refusedConnections {
		if slices.ContainsFunc(refused, func(r string) bool { return r != refused[0] && !(isGuard(r) && isGuard(refused[0])) }) {
			return none, &RefusedError{}
		}
		return none, &RefusedError{Reason: refused[0]}
	}
	return none, fmt.Errorf("%w on %d new connections: %v", ErrNoAnswer, refusedConnections, last)
}

// noAnswer is err as no answer, and where it is the engine's word that this
// Orker lacks a permission, as that too.
func (c *DoorClient) noAnswer(err error) error {
	var ce *connect.Error
	if errors.As(err, &ce) && ce.Code() == connect.CodePermissionDenied {
		word, _, _ := strings.Cut(ce.Message(), ":")
		if strings.TrimSpace(word) == eventsources.ReasonHostNotPermitted {
			c.mu.Lock()
			c.answered, c.notPermitted = c.Now(), ce.Message()
			c.mu.Unlock()
			return fmt.Errorf("%w: %w: %s", ErrNoAnswer, ErrHostNotPermitted, ce.Message())
		}
	}
	return fmt.Errorf("%w: %v", ErrNoAnswer, err)
}

// ConfirmKind is what a confirmation decided.
type ConfirmKind int

const (
	// ConfirmAccepted: the instance may run.
	ConfirmAccepted ConfirmKind = iota + 1
	// ConfirmWait: the engine judges nothing now; ask again.
	ConfirmWait
	// ConfirmStop: the instance ends.
	ConfirmStop
)

// Confirmed is a confirmation's answer.
type Confirmed struct {
	Kind   ConfirmKind
	Reason string
	Cause  string
	// Service is the service spec, where it was asked for and the instance
	// may run.
	Service *eventsources.ServiceSpec
	// Revision is the judging revision the source is judged by now; Source is
	// its spec where it is not the one the caller holds.
	Revision    int64
	Source      *orkstackv1.EventSource
	ImagePolicy *orkstackv1.ImagePolicySelection
	Telemetry   *orkstackv1.TelemetrySelection
}

// ConfirmInstance confirms the instance, holding judging revision (0 for none),
// and with wantService asks for its service spec.
func (c *DoorClient) ConfirmInstance(ctx context.Context, revision int64, wantService bool) (Confirmed, error) {
	resp, err := call(ctx, c, callBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.ConfirmInstanceResponse, error) {
		r, err := h.ConfirmInstance(ctx, connect.NewRequest(&orkstackv1.ConfirmInstanceRequest{Instance: c.ref(revision), WantServiceSpec: wantService}))
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP)
	if err != nil {
		return Confirmed{}, err
	}
	a := resp.GetAnswer()
	out := Confirmed{Reason: a.GetReason(), Cause: a.GetCause(), Revision: revision}
	switch a.GetDisposition() {
	case orkstackv1.Disposition_DISPOSITION_WAIT:
		out.Kind = ConfirmWait
		return out, nil
	case orkstackv1.Disposition_DISPOSITION_STOP:
		out.Kind = ConfirmStop
		return out, nil
	}
	out.Kind, out.ImagePolicy, out.Telemetry = ConfirmAccepted, resp.GetImagePolicy(), resp.GetTelemetry()
	if out.ImagePolicy == nil {
		return Confirmed{}, fmt.Errorf("%w: a confirmation that says nothing of the image policy", ErrNoAnswer)
	}
	if wantService {
		if out.Service, err = c.service(resp.GetService()); err != nil {
			return Confirmed{}, err
		}
	}
	switch {
	case resp.GetJudging() != nil:
		if out.Source, err = c.judging(resp.GetJudging()); err != nil {
			return Confirmed{}, err
		}
		out.Revision = out.Source.GetJudgingRevision()
	case revision == 0:
		return Confirmed{}, fmt.Errorf("%w: no judging spec, and none is held", ErrNoAnswer)
	}
	return out, nil
}

// service reads a service spec: its digest, which is this instance's, is its
// bytes', and it is this source's.
func (c *DoorClient) service(s *orkstackv1.ServiceSpec) (*eventsources.ServiceSpec, error) {
	if err := digested("service", s.GetSpec(), s.GetDigest(), maxServiceSpecBytes); err != nil {
		return nil, err
	}
	if s.GetDigest() != c.Instance.GetServiceDigest() {
		return nil, fmt.Errorf("%w: the service spec of another digest", ErrNoAnswer)
	}
	spec, err := eventsources.ReadServiceSpec(s.GetSpec())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoAnswer, err)
	}
	if spec.Source.ID != c.Instance.GetEventSourceId() || spec.Source.Namespace != c.Instance.GetNamespace() || spec.Source.Name != c.Instance.GetName() {
		return nil, fmt.Errorf("%w: the service spec of another source", ErrNoAnswer)
	}
	return &spec, nil
}

// judging reads a judging spec, as service does.
func (c *DoorClient) judging(s *orkstackv1.JudgingSpec) (*orkstackv1.EventSource, error) {
	if err := digested("judging", s.GetSpec(), s.GetDigest(), maxJudgingSpecBytes); err != nil {
		return nil, err
	}
	_, src, err := eventsources.ReadJudgingSpec(s.GetSpec())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoAnswer, err)
	}
	if src.GetId() != c.Instance.GetEventSourceId() || src.GetNamespace() != c.Instance.GetNamespace() || src.GetName() != c.Instance.GetName() {
		return nil, fmt.Errorf("%w: the judging spec of another source", ErrNoAnswer)
	}
	return src, nil
}

func digested(what string, body []byte, digest string, limit int) error {
	if len(body) == 0 || len(body) > limit {
		return fmt.Errorf("%w: a %s spec of %d bytes", ErrNoAnswer, what, len(body))
	}
	if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("%w: the %s spec is not its digest", ErrNoAnswer, what)
	}
	return nil
}

// Confirm is Door's: which judging revision is the document's now. A wait the
// engine answers is no answer of that.
func (c *DoorClient) Confirm(ctx context.Context, revision int64) (Confirmation, error) {
	got, err := c.ConfirmInstance(ctx, revision, false)
	switch {
	case err != nil:
		return Confirmation{}, err
	case got.Kind == ConfirmWait:
		return Confirmation{}, fmt.Errorf("%w: it waits: %s", ErrNoAnswer, got.Reason)
	}
	return Confirmation{Revision: got.Revision, Source: got.Source, Stop: got.Kind == ConfirmStop, Reason: got.Reason}, nil
}

// Report reports what the instance is. stop is that it ends.
func (c *DoorClient) Report(ctx context.Context, what orkstackv1.InstanceReport, reason string) (stop bool, err error) {
	resp, err := call(ctx, c, callBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.ReportInstanceResponse, error) {
		r, err := h.ReportInstance(ctx, connect.NewRequest(&orkstackv1.ReportInstanceRequest{Instance: c.ref(0), What: what, Reason: reason}))
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_STOP)
	if err != nil {
		return false, err
	}
	return resp.GetAnswer().GetDisposition() == orkstackv1.Disposition_DISPOSITION_STOP, nil
}

// Probe is Door's.
func (c *DoorClient) Probe(ctx context.Context) error {
	_, err := call(ctx, c, callBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.ProbeEventSourceResponse, error) {
		r, err := h.ProbeEventSource(ctx, connect.NewRequest(&orkstackv1.ProbeEventSourceRequest{Instance: c.ref(0)}))
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_OK)
	return err
}

// RequestPause is Door's: nil once the pause is written.
func (c *DoorClient) RequestPause(ctx context.Context, code string) error {
	req := &orkstackv1.RequestEventSourcePauseRequest{Instance: c.ref(0), Code: code}
	c.mu.Lock()
	req.RefusalCode = c.refusal
	c.mu.Unlock()
	_, err := call(ctx, c, callBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.RequestEventSourcePauseResponse, error) {
		r, err := h.RequestEventSourcePause(ctx, connect.NewRequest(req))
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_STOP)
	return err
}

// Offer is Door's.
func (c *DoorClient) Offer(ctx context.Context, o Offer) (Answer, error) {
	values, err := extracted(o)
	if err != nil {
		return Answer{}, err
	}
	req := &orkstackv1.OfferMessageRequest{Instance: c.ref(o.Revision), Message: envelope(o.Message), DeliveryCount: int32(o.Message.DeliveryCount),
		BodySha256: o.BodyDigest, BodyBytes: o.BodySize, Extracted: values, PreRejection: o.PreRejection}
	if o.Message.Oversize {
		req.BodySha256 = ""
	}
	if c.MaxDeliveries != nil {
		req.MaxDeliveries = int32(c.MaxDeliveries())
	}
	resp, err := call(ctx, c, callBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.OfferMessageResponse, error) {
		r, err := h.OfferMessage(ctx, connect.NewRequest(req))
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_PROCEED, orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_REJECT,
		orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFRESH)
	if err != nil {
		return Answer{}, err
	}
	return c.answer(resp.GetAnswer())
}

// Start is Door's.
func (c *DoorClient) Start(ctx context.Context, s Start) (Answer, error) {
	values, err := extracted(s.Offer)
	if err != nil {
		return Answer{}, err
	}
	req := &orkstackv1.StartMessageRunRequest{Instance: c.ref(s.Offer.Revision), Message: envelope(s.Offer.Message), Extracted: values, RetryOfOwnStart: s.Retry}
	for _, p := range s.Staged {
		req.Parts = append(req.Parts, &orkstackv1.UploadedPart{Input: p.Input, Sha256: p.Digest, SizeBytes: p.Size})
	}
	resp, err := call(ctx, c, startBound, func(ctx context.Context, h orkstackv1connect.EventSourceHostServiceClient) (*orkstackv1.StartMessageRunResponse, error) {
		r, err := h.StartMessageRun(ctx, connect.NewRequest(req))
		if err != nil {
			// It may have started the run: made again, it is its own retry.
			req.RetryOfOwnStart = true
		}
		return message(r, err)
	}, orkstackv1.Disposition_DISPOSITION_ACCEPTED, orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_REJECT,
		orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFRESH, orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN)
	if err != nil {
		return Answer{}, err
	}
	return c.answer(resp.GetAnswer())
}

// answer is a message call's answer as the judge takes it.
func (c *DoorClient) answer(a *orkstackv1.DoorAnswer) (Answer, error) {
	out := Answer{Reason: a.GetReason(), Cause: a.GetCause(), RunID: a.GetRunId(), At: c.Now()}
	c.mu.Lock()
	// Empty but for a refusal: what is told with a pause is the last answer's.
	c.refusal = a.GetRefusalCode()
	c.mu.Unlock()
	switch a.GetDisposition() {
	case orkstackv1.Disposition_DISPOSITION_PROCEED:
		out.Kind = AnswerProceed
		for _, p := range a.GetParts() {
			part := Part{Input: p.GetInput(), ContentType: p.GetContentType()}
			switch p.GetPart() {
			case orkstackv1.MessagePart_MESSAGE_PART_BODY:
				part.Kind = PartBody
			case orkstackv1.MessagePart_MESSAGE_PART_METADATA:
				part.Kind = PartMetadata
			default:
				return Answer{}, fmt.Errorf("%w: it asks for a part of kind %v", ErrNoAnswer, p.GetPart())
			}
			out.Parts = append(out.Parts, part)
		}
	case orkstackv1.Disposition_DISPOSITION_ACCEPTED:
		out.Kind = AnswerAccepted
	case orkstackv1.Disposition_DISPOSITION_DUPLICATE:
		out.Kind = AnswerDuplicate
	case orkstackv1.Disposition_DISPOSITION_REJECT:
		out.Kind = AnswerReject
	case orkstackv1.Disposition_DISPOSITION_WAIT:
		out.Kind = AnswerWait
	case orkstackv1.Disposition_DISPOSITION_UNCERTAIN:
		out.Kind, out.Reason = AnswerUncertain, a.GetRefusalCode()
	case orkstackv1.Disposition_DISPOSITION_STOP:
		out.Kind = AnswerStop
	case orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN:
		out.Kind = AnswerUploadAgain
	case orkstackv1.Disposition_DISPOSITION_REFRESH:
		src, err := c.judging(a.GetJudging())
		if err != nil {
			return Answer{}, err
		}
		out.Kind, out.Source = AnswerRefresh, src
	}
	if out.RunID == "" && (out.Kind == AnswerAccepted || out.Kind == AnswerDuplicate) {
		return Answer{}, fmt.Errorf("%w: a message taken by no run", ErrNoAnswer)
	}
	return out, nil
}

// extracted is an offer's mapped values as the wire holds them. Prejudge has
// already rejected a message whose number would be changed by it.
func extracted(o Offer) (map[string]*structpb.Value, error) {
	out := make(map[string]*structpb.Value, len(o.Values))
	for name, raw := range o.Values {
		v := &structpb.Value{}
		if err := protojson.Unmarshal(raw, v); err != nil {
			return nil, fmt.Errorf("sourcehost: the value mapped to %q: %w", name, err)
		}
		out[name] = v
	}
	return out, nil
}

// envelope is a message as the engine is told of it: its body and its binary
// attributes stay here.
func envelope(m HandedMessage) *orkstackv1.MessageEnvelope {
	out := &orkstackv1.MessageEnvelope{Id: m.ID, Subject: m.Subject, Group: m.Group, Broker: m.Broker}
	for name, values := range m.Attributes {
		if out.Attributes == nil {
			out.Attributes = make(map[string]*orkstackv1.AttributeValues, len(m.Attributes))
		}
		out.Attributes[name] = &orkstackv1.AttributeValues{Values: values}
	}
	if !m.PublishedAt.IsZero() {
		out.PublishedAt = timestamppb.New(m.PublishedAt)
	}
	return out
}

// message is a response's message, or its error.
func message[T any](r *connect.Response[T], err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return r.Msg, nil
}

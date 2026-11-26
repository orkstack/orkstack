package sourcehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// faultDoor is the engine's side of the door for the answers a real engine does
// not give on demand: a refusal per replica, an unknown disposition, a lost
// answer, a spec that is not its digest. It decides nothing of its own.
type faultDoor struct {
	orkstackv1connect.UnimplementedEventSourceHostServiceHandler
	mu      sync.Mutex
	conns   map[string]bool
	calls   int
	answer  func(call int) (*orkstackv1.DoorAnswer, error)
	confirm func(*orkstackv1.ConfirmInstanceRequest, *orkstackv1.ConfirmInstanceResponse)
	last    proto.Message
	// bounds is how long each call was given, as it was sent.
	bounds []time.Duration
}

func (f *faultDoor) next(peer string, req proto.Message) (*orkstackv1.DoorAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conns == nil {
		f.conns = map[string]bool{}
	}
	f.conns[peer] = true
	f.calls++
	f.last = req
	return f.answer(f.calls)
}

func (f *faultDoor) ConfirmInstance(_ context.Context, r *connect.Request[orkstackv1.ConfirmInstanceRequest]) (*connect.Response[orkstackv1.ConfirmInstanceResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	if err != nil {
		return nil, err
	}
	resp := &orkstackv1.ConfirmInstanceResponse{Answer: a}
	if f.confirm != nil {
		f.confirm(r.Msg, resp)
	}
	return connect.NewResponse(resp), nil
}

func (f *faultDoor) ReportInstance(_ context.Context, r *connect.Request[orkstackv1.ReportInstanceRequest]) (*connect.Response[orkstackv1.ReportInstanceResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	return connect.NewResponse(&orkstackv1.ReportInstanceResponse{Answer: a}), err
}

func (f *faultDoor) OfferMessage(_ context.Context, r *connect.Request[orkstackv1.OfferMessageRequest]) (*connect.Response[orkstackv1.OfferMessageResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	return connect.NewResponse(&orkstackv1.OfferMessageResponse{Answer: a}), err
}

func (f *faultDoor) StartMessageRun(_ context.Context, r *connect.Request[orkstackv1.StartMessageRunRequest]) (*connect.Response[orkstackv1.StartMessageRunResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	return connect.NewResponse(&orkstackv1.StartMessageRunResponse{Answer: a}), err
}

func (f *faultDoor) ProbeEventSource(_ context.Context, r *connect.Request[orkstackv1.ProbeEventSourceRequest]) (*connect.Response[orkstackv1.ProbeEventSourceResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	return connect.NewResponse(&orkstackv1.ProbeEventSourceResponse{Answer: a}), err
}

func (f *faultDoor) RequestEventSourcePause(_ context.Context, r *connect.Request[orkstackv1.RequestEventSourcePauseRequest]) (*connect.Response[orkstackv1.RequestEventSourcePauseResponse], error) {
	a, err := f.next(r.Peer().Addr, r.Msg)
	return connect.NewResponse(&orkstackv1.RequestEventSourcePauseResponse{Answer: a}), err
}

const (
	doorSourceID = "evs-0123456789abcdef"
	doorDigest   = "d1gest"
)

// testSource is a source a spec can be built of.
func testSource(t *testing.T) *orkstackv1.EventSource {
	t.Helper()
	src := judgingSource("PAID")
	src.Image = "ghcr.io/acme/source@sha256:" + strings.Repeat("a", 64)
	src.Pool = "main"
	return src
}

// doorTo is a client of f, each connection of it a new one.
func doorTo(t *testing.T, f *faultDoor) *DoorClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewEventSourceHostServiceHandler(f))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.Header.Get("Connect-Timeout-Ms"))
		f.mu.Lock()
		f.bounds = append(f.bounds, time.Duration(ms)*time.Millisecond)
		f.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	conn := NewHostConn(func() (orkstackv1connect.EventSourceHostServiceClient, func()) {
		tr := &http.Transport{Protocols: new(http.Protocols)}
		tr.Protocols.SetUnencryptedHTTP2(true)
		return orkstackv1connect.NewEventSourceHostServiceClient(&http.Client{Transport: tr}, server.URL), tr.CloseIdleConnections
	})
	t.Cleanup(conn.Close)
	return &DoorClient{Conn: conn, Now: time.Now, Instance: &orkstackv1.InstanceRef{
		Namespace: "payments", Name: "orders", EventSourceId: doorSourceID, Index: 1, Epoch: 7, Attempt: 2, ServiceDigest: doorDigest}}
}

func ok(int) (*orkstackv1.DoorAnswer, error) {
	return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_OK}, nil
}

func disposition(d orkstackv1.Disposition) func(int) (*orkstackv1.DoorAnswer, error) {
	return func(int) (*orkstackv1.DoorAnswer, error) {
		return &orkstackv1.DoorAnswer{Disposition: d, RunId: "ork-1"}, nil
	}
}

// TestDoorClient_AMessageAnswer: each disposition is what it says of an offer or
// a start; one the call has no such answer, one this release does not know, or
// none at all is no answer. An offer starts nothing, so it is never accepted.
func TestDoorClient_AMessageAnswer(t *testing.T) {
	for d, want := range map[orkstackv1.Disposition]struct{ offer, start AnswerKind }{
		orkstackv1.Disposition_DISPOSITION_PROCEED:      {offer: AnswerProceed},
		orkstackv1.Disposition_DISPOSITION_ACCEPTED:     {start: AnswerAccepted},
		orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN: {start: AnswerUploadAgain},
		orkstackv1.Disposition_DISPOSITION_DUPLICATE:    {AnswerDuplicate, AnswerDuplicate},
		orkstackv1.Disposition_DISPOSITION_REJECT:       {AnswerReject, AnswerReject},
		orkstackv1.Disposition_DISPOSITION_WAIT:         {AnswerWait, AnswerWait},
		orkstackv1.Disposition_DISPOSITION_UNCERTAIN:    {AnswerUncertain, AnswerUncertain},
		orkstackv1.Disposition_DISPOSITION_STOP:         {AnswerStop, AnswerStop},
		orkstackv1.Disposition_DISPOSITION_UNSPECIFIED:  {}, orkstackv1.Disposition_DISPOSITION_OK: {}, 99: {},
	} {
		c := doorTo(t, &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) {
			return &orkstackv1.DoorAnswer{Disposition: d, Reason: "MESSAGE_X", Cause: "pool", RunId: "ork-1", RefusalCode: "POOL_NOT_ALLOWED",
				Parts: []*orkstackv1.PartToUpload{{Input: "order", Part: orkstackv1.MessagePart_MESSAGE_PART_BODY, ContentType: "application/json"}}}, nil
		}})
		offer := Offer{Message: HandedMessage{ID: "m-1"}, BodyDigest: "ab", BodySize: 2, Revision: 5}
		check := func(call string, a Answer, err error, want AnswerKind) {
			if want == 0 {
				if !errors.Is(err, ErrNoAnswer) {
					t.Errorf("%s: %v: %+v, %v; want no answer", call, d, a, err)
				}
				return
			}
			if err != nil || a.Kind != want {
				t.Errorf("%s: %v: %+v, %v; want kind %v", call, d, a, err, want)
			}
			if want == AnswerProceed && (len(a.Parts) != 1 || a.Parts[0] != Part{Input: "order", Kind: PartBody, ContentType: "application/json"}) {
				t.Errorf("parts = %+v", a.Parts)
			}
			if want == AnswerUncertain && a.Reason != "POOL_NOT_ALLOWED" {
				t.Errorf("an uncertain refusal's code = %q", a.Reason)
			}
		}
		a, err := c.Offer(t.Context(), offer)
		check("offer", a, err, want.offer)
		a, err = c.Start(t.Context(), Start{Offer: offer})
		check("start", a, err, want.start)
	}
	// What acknowledges a message names the run it was acknowledged for.
	for _, d := range []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_ACCEPTED, orkstackv1.Disposition_DISPOSITION_DUPLICATE} {
		f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) { return &orkstackv1.DoorAnswer{Disposition: d}, nil }}
		if a, err := doorTo(t, f).Start(t.Context(), Start{}); !errors.Is(err, ErrNoAnswer) {
			t.Errorf("%v of no run: %+v, %v", d, a, err)
		}
	}
	// A part of no kind this release stages is no answer, never staged as another.
	f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) {
		return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_PROCEED, Parts: []*orkstackv1.PartToUpload{{Input: "x", Part: 9}}}, nil
	}}
	if _, err := doorTo(t, f).Offer(t.Context(), Offer{}); !errors.Is(err, ErrNoAnswer) {
		t.Errorf("an unknown part: %v", err)
	}
}

// TestDoorClient_WhatAnOfferCarries: the instance, the revision judged by, the
// envelope with its text attributes only, the delivery limit as last reported,
// and each mapped value as it was.
func TestDoorClient_WhatAnOfferCarries(t *testing.T) {
	f := &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_DUPLICATE)}
	c := doorTo(t, f)
	c.MaxDeliveries = func() int { return 9 }
	at := time.Date(2026, 11, 25, 1, 2, 3, 4, time.UTC)
	_, err := c.Offer(t.Context(), Offer{Revision: 31, BodyDigest: strings.Repeat("a", 64), BodySize: 12, PreRejection: "MESSAGE_NOT_JSON",
		Message: HandedMessage{ID: "m-1", Subject: "s", Attributes: map[string][]string{"k": {"v", "w"}}, Binary: map[string][]byte{"b": {0}},
			PublishedAt: at, DeliveryCount: 3, Group: "g", Broker: map[string]string{"stream": "ORDERS"}},
		Values: map[string]jsontext.Value{"n": jsontext.Value(`1.5`), "o": jsontext.Value(`{"a":[null,true,"x"]}`)}})
	must.OK(t, err)
	got := f.last.(*orkstackv1.OfferMessageRequest)
	o, _ := structpb.NewValue(map[string]any{"a": []any{nil, true, "x"}})
	want := &orkstackv1.OfferMessageRequest{
		Instance: &orkstackv1.InstanceRef{Namespace: "payments", Name: "orders", EventSourceId: doorSourceID, Index: 1, Epoch: 7, Attempt: 2,
			ServiceDigest: doorDigest, JudgingRevision: 31},
		Message: &orkstackv1.MessageEnvelope{Id: "m-1", Subject: "s", Group: "g", Broker: map[string]string{"stream": "ORDERS"},
			Attributes: map[string]*orkstackv1.AttributeValues{"k": {Values: []string{"v", "w"}}}, PublishedAt: got.GetMessage().GetPublishedAt()},
		DeliveryCount: 3, MaxDeliveries: 9, BodySha256: strings.Repeat("a", 64), BodyBytes: 12, PreRejection: "MESSAGE_NOT_JSON",
		Extracted: map[string]*structpb.Value{"n": structpb.NewNumberValue(1.5), "o": o},
	}
	if !proto.Equal(got, want) || !got.GetMessage().GetPublishedAt().AsTime().Equal(at) {
		t.Fatalf("sent %v\nwant %v", got, want)
	}
	// An over-long body was never read: its digest is not sent, its size is.
	if _, err := c.Offer(t.Context(), Offer{BodyDigest: "e3b0", BodySize: 99, Message: HandedMessage{Oversize: true, OversizeBytes: 99}}); err != nil {
		t.Fatal(err)
	}
	if got := f.last.(*orkstackv1.OfferMessageRequest); got.GetBodySha256() != "" || got.GetBodyBytes() != 99 {
		t.Fatalf("an oversize message sent %v", got)
	}
}

// TestDoorClient_RefusedIsOneReplicas: a REFUSED answer is made again at once on
// a new connection, up to eight of them; a replica that answers ends it, and the
// connection that answered is the one kept.
func TestDoorClient_RefusedIsOneReplicas(t *testing.T) {
	for name, tc := range map[string]struct{ refusals, wantCalls int }{
		"the third replica answers":  {2, 3},
		"the eighth new one answers": {8, 9},
		"no replica answers":         {100, 9},
	} {
		t.Run(name, func(t *testing.T) {
			f := &faultDoor{answer: func(call int) (*orkstackv1.DoorAnswer, error) {
				if call <= tc.refusals {
					return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFUSED}, nil
				}
				return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_ACCEPTED, RunId: "ork-1"}, nil
			}}
			c := doorTo(t, f)
			a, err := c.Start(t.Context(), Start{})
			answered := tc.refusals < 9
			if answered != (err == nil && a.Kind == AnswerAccepted) || !answered && !errors.Is(err, ErrNoAnswer) {
				t.Fatalf("%+v, %v", a, err)
			}
			if f.calls != tc.wantCalls || len(f.conns) != tc.wantCalls {
				t.Fatalf("%d calls on %d connections, want %d on as many", f.calls, len(f.conns), tc.wantCalls)
			}
			if answered {
				if _, err := c.Start(t.Context(), Start{}); err != nil || len(f.conns) != tc.wantCalls {
					t.Fatalf("the next call: %v on %d connections", err, len(f.conns))
				}
				if c.LastAnswer().IsZero() {
					t.Error("an answer left no time of it")
				}
			} else if !c.LastAnswer().IsZero() {
				t.Error("a refusal counted as an answer")
			}
		})
	}
}

// TestDoorClient_WhyEveryReplicaRefused: the reason is given only where every
// replica reached gave it; one that was not heard, or said another, gives none.
func TestDoorClient_WhyEveryReplicaRefused(t *testing.T) {
	const off = "EVENT_SOURCES_DISABLED"
	for name, tc := range map[string]struct {
		say     func(call int) string // "!" is no answer
		refused bool
		want    string
	}{
		"all":       {func(int) string { return off }, true, off},
		"one other": {func(n int) string { return map[bool]string{true: "EVENT_SOURCE_DOCUMENT_MISSING", false: off}[n == 5] }, true, ""},
		"each a guard": {func(n int) string {
			return map[bool]string{true: "EVENT_SOURCES_REQUIRE_VERSIONING", false: off}[n == 5]
		}, true, off},
		"one unheard":  {func(n int) string { return map[bool]string{true: "!", false: off}[n == 5] }, false, ""},
		"none says it": {func(int) string { return "" }, true, ""},
	} {
		f := &faultDoor{answer: func(call int) (*orkstackv1.DoorAnswer, error) {
			reason := tc.say(call)
			if reason == "!" {
				panic(http.ErrAbortHandler)
			}
			return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFUSED, Reason: reason}, nil
		}}
		_, err := doorTo(t, f).ConfirmInstance(t.Context(), 1, false)
		got, refused := errors.AsType[*RefusedError](err)
		if !errors.Is(err, ErrNoAnswer) || refused != tc.refused || refused && got.Reason != tc.want {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestDoorClient_OneTryIsBounded: a call has the engine's own bound for it, and
// its further connections share it: a start is never cut short of the engine's
// three minutes, and no call takes a multiple of its own.
func TestDoorClient_OneTryIsBounded(t *testing.T) {
	const refusing = 20 * time.Millisecond
	for name, tc := range everyCall {
		want := 30 * time.Second
		if name == "StartMessageRun" {
			want = 180 * time.Second
		}
		f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) {
			time.Sleep(refusing)
			return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFUSED}, nil
		}}
		if err := tc.call(t.Context(), doorTo(t, f)); !errors.Is(err, ErrNoAnswer) {
			t.Errorf("%s: %v", name, err)
		}
		for n, got := range f.bounds {
			most := want - time.Duration(n)*refusing
			// How much of it is left when it is sent is the host's; that a start
			// has more than any other call's whole is not.
			if got > most || n == 0 && (got <= 0 || want == 180*time.Second && got <= 30*time.Second) {
				t.Errorf("%s: connection %d was given %v; want at most %v", name, n+1, got, most)
			}
		}
	}
}

// TestDoorClient_AnUnansweredConnection: on a connection dialled for a refusal, a
// replica that does not answer is left for another, and a start made again says
// it may be its own; what a replica does answer ends the call.
func TestDoorClient_AnUnansweredConnection(t *testing.T) {
	seq := func(starts *[]bool, steps ...any) *faultDoor {
		f := &faultDoor{}
		f.answer = func(call int) (*orkstackv1.DoorAnswer, error) {
			if r, isStart := f.last.(*orkstackv1.StartMessageRunRequest); isStart {
				*starts = append(*starts, r.GetRetryOfOwnStart())
			}
			switch step := steps[call-1].(type) {
			case orkstackv1.Disposition:
				return &orkstackv1.DoorAnswer{Disposition: step, RunId: "ork-1"}, nil
			case error:
				if step == http.ErrAbortHandler {
					panic(step)
				}
				return nil, step
			}
			panic("no step")
		}
		return f
	}
	const refused, accepted = orkstackv1.Disposition_DISPOSITION_REFUSED, orkstackv1.Disposition_DISPOSITION_ACCEPTED
	var retries []bool
	f := seq(&retries, refused, http.ErrAbortHandler, accepted)
	if a, err := doorTo(t, f).Start(t.Context(), Start{}); err != nil || a.Kind != AnswerAccepted || !slices.Equal(retries, []bool{false, false, true}) {
		t.Errorf("a start left unanswered: %+v, %v, retries %v", a, err, retries)
	}
	// The first connection's silence is a wait: nothing is dialled for it.
	f = seq(&retries, http.ErrAbortHandler)
	if _, err := doorTo(t, f).Start(t.Context(), Start{}); !errors.Is(err, ErrNoAnswer) || f.calls != 1 {
		t.Errorf("an engine that does not answer: %v after %d calls", err, f.calls)
	}
	f = seq(&retries, refused, connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED: x")))
	if _, err := doorTo(t, f).Start(t.Context(), Start{}); !errors.Is(err, ErrHostNotPermitted) || f.calls != 2 {
		t.Errorf("a second replica's denial: %v after %d calls", err, f.calls)
	}
	f = seq(&retries, refused, connect.NewError(connect.CodeResourceExhausted, errors.New("x")))
	if _, err := doorTo(t, f).Start(t.Context(), Start{}); !errors.Is(err, ErrNoAnswer) || f.calls != 2 {
		t.Errorf("a second replica's ceiling: %v after %d calls", err, f.calls)
	}
}

// TestDoorClient_NoAnswer: the closed set of what a connection says: nothing but
// a permission this Orker lacks, named as such, is more than "no answer", and
// no other code or message decides anything.
func TestDoorClient_NoAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		err          error
		notPermitted bool
	}{
		"not permitted":                      {err: connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED: eventsource.host at pool gpu")), notPermitted: true},
		"another permission denial":          {err: connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))},
		"the words elsewhere":                {err: connect.NewError(connect.CodePermissionDenied, errors.New("no SOURCE_HOST_NOT_PERMITTED here"))},
		"a longer word":                      {err: connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED_X"))},
		"the words under another code":       {err: connect.NewError(connect.CodeInternal, errors.New("SOURCE_HOST_NOT_PERMITTED"))},
		"overloaded":                         {err: connect.NewError(connect.CodeResourceExhausted, errors.New("x"))},
		"unauthenticated":                    {err: connect.NewError(connect.CodeUnauthenticated, errors.New("x"))},
		"an engine that has no such service": {err: connect.NewError(connect.CodeUnimplemented, errors.New("x"))},
		"a failed precondition":              {err: connect.NewError(connect.CodeFailedPrecondition, errors.New("EVENT_SOURCE_NOT_HELD"))},
	} {
		t.Run(name, func(t *testing.T) {
			f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) { return nil, tc.err }}
			c := doorTo(t, f)
			_, err := c.Offer(t.Context(), Offer{})
			if !errors.Is(err, ErrNoAnswer) || errors.Is(err, ErrHostNotPermitted) != tc.notPermitted {
				t.Fatalf("%v", err)
			}
			if f.calls != 1 {
				t.Fatalf("%d calls", f.calls)
			}
			if got := c.NotPermitted(); (got != "") != tc.notPermitted {
				t.Fatalf("NotPermitted = %q", got)
			}
			if tc.notPermitted == c.LastAnswer().IsZero() {
				t.Fatalf("LastAnswer = %v: a denial is the engine's answer, nothing else here is", c.LastAnswer())
			}
		})
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	url := "http://" + l.Addr().String()
	must.OK(t, l.Close())
	conn := NewHostConn(func() (orkstackv1connect.EventSourceHostServiceClient, func()) {
		return orkstackv1connect.NewEventSourceHostServiceClient(http.DefaultClient, url), func() {}
	})
	defer conn.Close()
	if _, err := (&DoorClient{Conn: conn, Now: time.Now, Instance: &orkstackv1.InstanceRef{}}).Offer(t.Context(), Offer{}); !errors.Is(err, ErrNoAnswer) {
		t.Fatalf("an engine that is not there: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := doorTo(t, &faultDoor{answer: ok}).Offer(ctx, Offer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a caller that stopped: %v", err)
	}
}

// TestDoorClient_Confirm: what a confirmation gives is read only where its digest
// is its bytes' and it is this source's and this instance's; anything else is no
// answer, and an attempt is never run on a spec it could not read.
func TestDoorClient_Confirm(t *testing.T) {
	src := testSource(t)
	_, service, digest, err := eventsources.BuildServiceSpec(src, eventsources.Image{Contracts: []int32{1}, Profiles: []int32{1}}, eventsources.Limits{}, 0)
	must.OK(t, err)
	judging, judgingBytes, judgingDigest, err := eventsources.BuildJudgingSpec(src, eventsources.Limits{})
	must.OK(t, err)
	give := func(_ *orkstackv1.ConfirmInstanceRequest, r *orkstackv1.ConfirmInstanceResponse) {
		r.Service = &orkstackv1.ServiceSpec{Spec: service, Digest: digest}
		r.Judging = &orkstackv1.JudgingSpec{Spec: judgingBytes, Digest: judgingDigest}
		r.ImagePolicy = &orkstackv1.ImagePolicySelection{RequireDigest: true}
	}
	client := func(f *faultDoor) *DoorClient {
		c := doorTo(t, f)
		c.Instance.Namespace, c.Instance.Name, c.Instance.EventSourceId, c.Instance.ServiceDigest = src.GetNamespace(), src.GetName(), src.GetId(), digest
		return c
	}
	f := &faultDoor{answer: ok, confirm: give}
	got, err := client(f).ConfirmInstance(t.Context(), 0, true)
	if err != nil || got.Kind != ConfirmAccepted || got.Service == nil || got.Service.Image != src.GetImage() || got.Revision != judging.Revision ||
		got.Source.GetId() != src.GetId() || !got.ImagePolicy.GetRequireDigest() {
		t.Fatalf("%+v, %v", got, err)
	}
	if req := f.last.(*orkstackv1.ConfirmInstanceRequest); !req.GetWantServiceSpec() || req.GetInstance().GetJudgingRevision() != 0 {
		t.Fatalf("asked %v", req)
	}
	for name, spoil := range map[string]func(*orkstackv1.ConfirmInstanceResponse){
		"a service spec that is not its digest": func(r *orkstackv1.ConfirmInstanceResponse) { r.Service.Spec = append([]byte(" "), r.Service.Spec...) },
		"another service spec of this source": func(r *orkstackv1.ConfirmInstanceResponse) {
			other := proto.Clone(src).(*orkstackv1.EventSource)
			other.Pool = "another"
			_, b, d, err := eventsources.BuildServiceSpec(other, eventsources.Image{Contracts: []int32{1}, Profiles: []int32{1}}, eventsources.Limits{}, 0)
			must.OK(t, err)
			r.Service = &orkstackv1.ServiceSpec{Spec: b, Digest: d}
		},
		"a service spec of a later release": func(r *orkstackv1.ConfirmInstanceResponse) {
			r.Service = sum([]byte(strings.Replace(string(service), `"format":1`, `"format":2`, 1)))
		},
		"no service spec though one was asked":  func(r *orkstackv1.ConfirmInstanceResponse) { r.Service = nil },
		"no judging spec though none is held":   func(r *orkstackv1.ConfirmInstanceResponse) { r.Judging = nil },
		"a judging spec that is not its digest": func(r *orkstackv1.ConfirmInstanceResponse) { r.Judging.Digest = strings.Repeat("0", 64) },
		"another source's judging spec": func(r *orkstackv1.ConfirmInstanceResponse) {
			other := proto.Clone(src).(*orkstackv1.EventSource)
			other.Name = "another"
			_, b, d, err := eventsources.BuildJudgingSpec(other, eventsources.Limits{})
			must.OK(t, err)
			r.Judging = &orkstackv1.JudgingSpec{Spec: b, Digest: d}
		},
		"a service spec over its bound": func(r *orkstackv1.ConfirmInstanceResponse) { r.Service = sum(make([]byte, 64<<10+1)) },
		"no image policy":               func(r *orkstackv1.ConfirmInstanceResponse) { r.ImagePolicy = nil },
	} {
		f := &faultDoor{answer: ok, confirm: func(q *orkstackv1.ConfirmInstanceRequest, r *orkstackv1.ConfirmInstanceResponse) {
			give(q, r)
			spoil(r)
		}}
		if got, err := client(f).ConfirmInstance(t.Context(), 0, true); !errors.Is(err, ErrNoAnswer) {
			t.Errorf("%s: %+v, %v; want no answer", name, got, err)
		}
	}
	// A later confirmation asks for no spec and is given none: the revision held stands.
	f = &faultDoor{answer: ok, confirm: func(_ *orkstackv1.ConfirmInstanceRequest, r *orkstackv1.ConfirmInstanceResponse) {
		r.ImagePolicy = &orkstackv1.ImagePolicySelection{}
	}}
	if got, err := client(f).ConfirmInstance(t.Context(), 31, false); err != nil || got.Kind != ConfirmAccepted || got.Revision != 31 || got.Source != nil {
		t.Errorf("a later confirmation: %+v, %v", got, err)
	}
	for d, want := range map[orkstackv1.Disposition]ConfirmKind{
		orkstackv1.Disposition_DISPOSITION_STOP: ConfirmStop, orkstackv1.Disposition_DISPOSITION_WAIT: ConfirmWait,
	} {
		f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) {
			return &orkstackv1.DoorAnswer{Disposition: d, Reason: "unread=spec", Cause: "outage"}, nil
		}}
		if got, err := client(f).ConfirmInstance(t.Context(), 0, true); err != nil || got.Kind != want || got.Reason != "unread=spec" || got.Service != nil {
			t.Errorf("%v: %+v, %v", d, got, err)
		}
	}
	for _, d := range []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_PROCEED, orkstackv1.Disposition_DISPOSITION_ACCEPTED, 0, 99} {
		if _, err := client(&faultDoor{answer: disposition(d), confirm: give}).ConfirmInstance(t.Context(), 0, true); !errors.Is(err, ErrNoAnswer) {
			t.Errorf("%v confirmed: %v", d, err)
		}
	}
}

func sum(b []byte) *orkstackv1.ServiceSpec {
	d := sha256.Sum256(b)
	return &orkstackv1.ServiceSpec{Spec: b, Digest: hex.EncodeToString(d[:])}
}

// TestDoorClient_ProbeReportAndPause: a probe passes only on OK; a report and a
// pause request say what they are and take what they are answered.
func TestDoorClient_ProbeReportAndPause(t *testing.T) {
	for d, pass := range map[orkstackv1.Disposition]bool{
		orkstackv1.Disposition_DISPOSITION_OK: true, orkstackv1.Disposition_DISPOSITION_WAIT: false, orkstackv1.Disposition_DISPOSITION_UNCERTAIN: false,
		orkstackv1.Disposition_DISPOSITION_STOP: false, orkstackv1.Disposition_DISPOSITION_ACCEPTED: false, 99: false,
	} {
		if err := doorTo(t, &faultDoor{answer: disposition(d)}).Probe(t.Context()); (err == nil) != pass {
			t.Errorf("a probe answered %v: %v", d, err)
		}
	}
	f := &faultDoor{answer: ok}
	if stop, err := doorTo(t, f).Report(t.Context(), orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID, "OBJECT_MISSING"); stop || err != nil {
		t.Errorf("Report = %v, %v", stop, err)
	}
	if got := f.last.(*orkstackv1.ReportInstanceRequest); got.GetWhat() != orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID || got.GetReason() != "OBJECT_MISSING" {
		t.Errorf("reported %v", got)
	}
	if stop, err := doorTo(t, &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_STOP)}).Report(t.Context(), orkstackv1.InstanceReport_INSTANCE_REPORT_READY, ""); !stop || err != nil {
		t.Errorf("a report answered STOP = %v, %v", stop, err)
	}
	if _, err := doorTo(t, &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_WAIT)}).Report(t.Context(), orkstackv1.InstanceReport_INSTANCE_REPORT_READY, ""); !errors.Is(err, ErrNoAnswer) {
		t.Errorf("a report answered WAIT = %v; it is made again", err)
	}
	f = &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_STOP)}
	c := doorTo(t, f)
	f.answer = func(int) (*orkstackv1.DoorAnswer, error) {
		return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_UNCERTAIN, RefusalCode: "POOL_NOT_ALLOWED"}, nil
	}
	if _, err := c.Offer(t.Context(), Offer{Message: HandedMessage{ID: "m"}}); err != nil {
		t.Fatal(err)
	}
	f.answer = disposition(orkstackv1.Disposition_DISPOSITION_STOP)
	if err := c.RequestPause(t.Context(), "EVENT_SOURCE_REFUSED"); err != nil {
		t.Errorf("RequestPause = %v", err)
	}
	if got := f.last.(*orkstackv1.RequestEventSourcePauseRequest); got.GetCode() != "EVENT_SOURCE_REFUSED" || got.GetRefusalCode() != "POOL_NOT_ALLOWED" {
		t.Errorf("asked %v", got)
	}
	if err := doorTo(t, &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_WAIT)}).RequestPause(t.Context(), "EVENT_SOURCE_ALL_REJECTED"); !errors.Is(err, ErrNoAnswer) {
		t.Errorf("a pause that was not written = %v", err)
	}
}

// TestDoorClient_StartAndRefresh: a start carries the staged parts and says it is
// a retry of its own; a refresh carries a judging spec this source's or is no
// answer.
func TestDoorClient_StartAndRefresh(t *testing.T) {
	f := &faultDoor{answer: disposition(orkstackv1.Disposition_DISPOSITION_ACCEPTED)}
	_, err := doorTo(t, f).Start(t.Context(), Start{Offer: Offer{Revision: 4, Message: HandedMessage{ID: "m"}}, Retry: true,
		Staged: []StagedPart{{Input: "order", Digest: strings.Repeat("b", 64), Size: 3}}})
	must.OK(t, err)
	got := f.last.(*orkstackv1.StartMessageRunRequest)
	if !got.GetRetryOfOwnStart() || len(got.GetParts()) != 1 || got.GetParts()[0].GetSizeBytes() != 3 || got.GetInstance().GetJudgingRevision() != 4 || got.GetMessage().GetId() != "m" {
		t.Fatalf("started with %v", got)
	}
	src := testSource(t)
	_, b, d, err := eventsources.BuildJudgingSpec(src, eventsources.Limits{})
	must.OK(t, err)
	refresh := func(spec *orkstackv1.JudgingSpec) *faultDoor {
		return &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) {
			return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_REFRESH, Judging: spec}, nil
		}}
	}
	c := doorTo(t, refresh(&orkstackv1.JudgingSpec{Spec: b, Digest: d}))
	c.Instance.Namespace, c.Instance.Name, c.Instance.EventSourceId = src.GetNamespace(), src.GetName(), src.GetId()
	if a, err := c.Offer(t.Context(), Offer{}); err != nil || a.Kind != AnswerRefresh || a.Source.GetId() != src.GetId() {
		t.Fatalf("%+v, %v", a, err)
	}
	for _, spec := range []*orkstackv1.JudgingSpec{nil, {Spec: b, Digest: "x"}} {
		if a, err := doorTo(t, refresh(spec)).Offer(t.Context(), Offer{}); !errors.Is(err, ErrNoAnswer) {
			t.Fatalf("a refresh with %v: %+v, %v", spec, a, err)
		}
	}
}

// everyCall is each call of the door, and the dispositions that answer it.
var everyCall = map[string]struct {
	call    func(context.Context, *DoorClient) error
	answers []orkstackv1.Disposition
}{
	"ConfirmInstance": {func(ctx context.Context, c *DoorClient) error {
		_, err := c.ConfirmInstance(ctx, 1, false)
		return err
	}, []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP}},
	"ReportInstance": {func(ctx context.Context, c *DoorClient) error {
		_, err := c.Report(ctx, orkstackv1.InstanceReport_INSTANCE_REPORT_READY, "")
		return err
	}, []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_STOP}},
	"OfferMessage": {func(ctx context.Context, c *DoorClient) error {
		_, err := c.Offer(ctx, Offer{})
		return err
	}, []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_PROCEED, orkstackv1.Disposition_DISPOSITION_DUPLICATE,
		orkstackv1.Disposition_DISPOSITION_REJECT, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN,
		orkstackv1.Disposition_DISPOSITION_STOP, orkstackv1.Disposition_DISPOSITION_REFRESH}},
	"StartMessageRun": {func(ctx context.Context, c *DoorClient) error {
		_, err := c.Start(ctx, Start{})
		return err
	}, []orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_ACCEPTED, orkstackv1.Disposition_DISPOSITION_DUPLICATE,
		orkstackv1.Disposition_DISPOSITION_REJECT, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN,
		orkstackv1.Disposition_DISPOSITION_STOP, orkstackv1.Disposition_DISPOSITION_REFRESH, orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN}},
	"ProbeEventSource": {func(ctx context.Context, c *DoorClient) error { return c.Probe(ctx) },
		[]orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_OK}},
	"RequestEventSourcePause": {func(ctx context.Context, c *DoorClient) error {
		return c.RequestPause(ctx, "EVENT_SOURCE_ALL_REJECTED")
	},
		[]orkstackv1.Disposition{orkstackv1.Disposition_DISPOSITION_STOP}},
}

// TestDoorClient_EveryCallTakesOnlyItsAnswers: on every call, a disposition that
// is none of its answers, one this release does not know and one that is not set
// are no answer, made once and kept as no time of one; REFUSED is made again on
// eight new connections.
func TestDoorClient_EveryCallTakesOnlyItsAnswers(t *testing.T) {
	src := testSource(t)
	_, b, d, err := eventsources.BuildJudgingSpec(src, eventsources.Limits{})
	must.OK(t, err)
	var all []orkstackv1.Disposition
	for n := range orkstackv1.Disposition_name {
		all = append(all, orkstackv1.Disposition(n))
	}
	all = append(all, 99, -1)
	for name, tc := range everyCall {
		t.Run(name, func(t *testing.T) {
			for _, got := range all {
				f := &faultDoor{
					answer: func(int) (*orkstackv1.DoorAnswer, error) {
						return &orkstackv1.DoorAnswer{Disposition: got, RunId: "ork-1", Judging: &orkstackv1.JudgingSpec{Spec: b, Digest: d}}, nil
					},
					confirm: func(_ *orkstackv1.ConfirmInstanceRequest, r *orkstackv1.ConfirmInstanceResponse) {
						r.ImagePolicy = &orkstackv1.ImagePolicySelection{}
					},
				}
				c := doorTo(t, f)
				c.Instance.Namespace, c.Instance.Name, c.Instance.EventSourceId = src.GetNamespace(), src.GetName(), src.GetId()
				err := tc.call(t.Context(), c)
				want, calls := slices.Contains(tc.answers, got), 1
				if got == orkstackv1.Disposition_DISPOSITION_REFUSED {
					calls = 1 + refusedConnections
				}
				if want != (err == nil) || !want && !errors.Is(err, ErrNoAnswer) || f.calls != calls || c.LastAnswer().IsZero() == want {
					t.Errorf("%v: %v after %d calls, answered at %v; want an answer %v after %d", got, err, f.calls, c.LastAnswer(), want, calls)
				}
			}
			f := &faultDoor{answer: func(int) (*orkstackv1.DoorAnswer, error) { return nil, nil }}
			if err := tc.call(t.Context(), doorTo(t, f)); !errors.Is(err, ErrNoAnswer) {
				t.Errorf("a response with no answer in it: %v", err)
			}
		})
	}
}

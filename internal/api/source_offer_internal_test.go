package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const offeredID = "m-1"

// offerWorld is confirmWorld with a source that maps and binds, its Orkflow,
// the claims and what is staged, the stores real ones.
type offerWorld struct {
	*confirmWorld
	dir     string
	claims  *artifacts.MessageClaimStore
	records *artifacts.EventSourceRecordStore
	starts  *startsOf
	archive *archivedRuns
}

// startsOf is a cluster's runs by their start events, or its failure.
type startsOf struct {
	engine.RunReader
	runs map[string]*engine.RunStartRecord
	err  error
	asks int
}

func (r *startsOf) RunStart(_ context.Context, id string) (*engine.RunStartRecord, error) {
	r.asks++
	if r.err != nil {
		return nil, r.err
	}
	if rec, ok := r.runs[id]; ok {
		return rec, nil
	}
	return nil, fmt.Errorf("run %s: %w", id, engine.ErrRunNotFound)
}

// archivedRuns is the archive, or its failure.
type archivedRuns struct {
	RunArchive
	runs map[string]*orkstackv1.Run
	err  error
	asks int
}

func (a *archivedRuns) LoadRun(_ context.Context, id string) (*orkstackv1.Run, error) {
	a.asks++
	if a.err != nil {
		return nil, a.err
	}
	if run, ok := a.runs[id]; ok {
		return run, nil
	}
	return nil, fmt.Errorf("run %s: %w", id, artifacts.ErrNotFound)
}

func newOfferWorld(t *testing.T) *offerWorld {
	t.Helper()
	w := &offerWorld{confirmWorld: newConfirmWorld(t), dir: t.TempDir(), starts: &startsOf{runs: map[string]*engine.RunStartRecord{}},
		archive: &archivedRuns{runs: map[string]*orkstackv1.Run{}}}
	var err error
	if w.claims, err = artifacts.NewLocalMessageClaimStore(w.dir); err != nil {
		t.Fatal(err)
	}
	if w.records, err = artifacts.NewLocalEventSourceRecordStore(w.dir); err != nil {
		t.Fatal(err)
	}
	inputs, err := structpb.NewStruct(map[string]any{
		"order_id": map[string]any{"type": "string", "required": true},
		"order":    map[string]any{"type": "artifact", "required": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.put(t, &orkstackv1.Orkflow{Namespace: "default", Name: "deploy", Version: "1.0.0", Inputs: inputs,
		Tasks: []*orkstackv1.Task{{Id: "apply", Uses: "ghcr.io/acme/apply:1", InputArtifacts: []string{"order"},
			Image: &orkstackv1.ImageRef{Reference: "ghcr.io/acme/apply:1", Pinned: "ghcr.io/acme/apply@sha256:" + strings.Repeat("b", 64)}}}})
	w.save(t, func(src *orkstackv1.EventSource) {
		src.Format = orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON
		src.Map = map[string]*orkstackv1.MapRule{"order_id": {Pointer: "/id"}}
		src.Artifacts = map[string]orkstackv1.MessagePart{"order": orkstackv1.MessagePart_MESSAGE_PART_BODY}
	})
	w.s.messageClaims, w.s.stagedParts = w.claims, w.records
	w.s.router = engine.NewSingleRouter(engine.Binding{Reader: w.starts})
	w.s.archive = w.archive
	return w
}

func (w *offerWorld) put(t *testing.T, wf *orkstackv1.Orkflow) {
	t.Helper()
	if _, _, err := w.s.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
}

func (w *offerWorld) save(t *testing.T, edit func(*orkstackv1.EventSource)) {
	t.Helper()
	src := w.stored(t)
	edit(src)
	if _, err := w.store.Replace(context.Background(), src, src.GetEtag()); err != nil {
		t.Fatal(err)
	}
}

func (w *offerWorld) scope() artifacts.ClaimScope {
	return artifacts.ClaimScope{Namespace: "default", Source: "orders", Orkflow: "deploy"}
}

func (w *offerWorld) claim(t *testing.T, runID string, started bool) {
	t.Helper()
	ctx := context.Background()
	if _, err := w.claims.Create(ctx, w.scope(), offeredID, runID); err != nil {
		t.Fatal(err)
	}
	if started {
		if err := w.claims.MarkStarted(ctx, w.scope(), offeredID, runID, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func (w *offerWorld) started(t *testing.T) bool {
	t.Helper()
	c, found, err := w.claims.Get(context.Background(), w.scope(), offeredID)
	if err != nil || !found {
		t.Fatalf("%v, %v", found, err)
	}
	return !c.Started.IsZero()
}

// spoil overwrites the message's claim with body.
func (w *offerWorld) spoil(t *testing.T, body string) {
	t.Helper()
	w.claim(t, "ork-claimed", false)
	found, err := filepath.Glob(filepath.Join(w.dir, artifacts.MessageClaimPrefix, "*", "*.json"))
	if err != nil || len(found) != 1 {
		t.Fatalf("%v, %v", found, err)
	}
	if err := os.WriteFile(found[0], []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (w *offerWorld) request() *orkstackv1.OfferMessageRequest {
	return &orkstackv1.OfferMessageRequest{Instance: w.ref(4), Message: &orkstackv1.MessageEnvelope{Id: offeredID, Subject: "orders.paid"},
		DeliveryCount: 1, BodySha256: strings.Repeat("a", 64), BodyBytes: 12,
		Extracted: map[string]*structpb.Value{"order_id": structpb.NewStringValue("o-1")}}
}

func (w *offerWorld) offer(t *testing.T, req *orkstackv1.OfferMessageRequest) *orkstackv1.DoorAnswer {
	t.Helper()
	resp, err := w.s.OfferMessage(asHost(hostOrker), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	return answered(t, "OfferMessage", resp.Msg.GetAnswer())
}

// startOf is a run of this namespace as its start event says.
func startOf(namespace string) *engine.RunStartRecord {
	return &engine.RunStartRecord{Orkflow: &orkstackv1.Orkflow{Namespace: namespace, Name: "deploy"}}
}

// TestOfferMessage_Proceeds: a message that has no run and can have one is
// told what to stage and the version it would run, and nothing is written.
func TestOfferMessage_Proceeds(t *testing.T) {
	w := newOfferWorld(t)
	got := w.offer(t, w.request())
	if got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_PROCEED || got.GetOrkflowVersion() != "1.0.0" || got.GetRunId() != "" || len(got.GetParts()) != 1 {
		t.Fatalf("%v", got)
	}
	if p := got.GetParts()[0]; p.GetInput() != "order" || p.GetPart() != orkstackv1.MessagePart_MESSAGE_PART_BODY || p.GetContentType() != "application/json" {
		t.Fatalf("%v", p)
	}
	if _, found, err := w.claims.Get(context.Background(), w.scope(), offeredID); found || err != nil {
		t.Fatalf("an offer claimed: %v, %v", found, err)
	}
	if w.starts.asks+w.archive.asks != 0 {
		t.Fatal("a message with no claim was looked for")
	}

	w.save(t, func(src *orkstackv1.EventSource) {
		src.Format = orkstackv1.MessageFormat_MESSAGE_FORMAT_BYTES
		src.Map = nil
		src.Input = &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{"order_id": structpb.NewStringValue("o")}}}
		src.Artifacts = map[string]orkstackv1.MessagePart{"order": orkstackv1.MessagePart_MESSAGE_PART_METADATA}
	})
	req := w.request()
	req.Extracted = nil
	if p := w.offer(t, req).GetParts(); len(p) != 1 || p[0].GetContentType() != "application/json" || p[0].GetPart() != orkstackv1.MessagePart_MESSAGE_PART_METADATA {
		t.Fatalf("%v", p)
	}
	w.save(t, func(src *orkstackv1.EventSource) {
		src.Artifacts = map[string]orkstackv1.MessagePart{"order": orkstackv1.MessagePart_MESSAGE_PART_BODY}
	})
	if p := w.offer(t, req).GetParts(); len(p) != 1 || p[0].GetContentType() != "application/octet-stream" {
		t.Fatalf("%v", p)
	}
}

// TestOfferMessage_TheClaimIsReadFirst: what the message's claim says comes
// before anything the message or its source is judged by, pre-rejections
// included.
func TestOfferMessage_TheClaimIsReadFirst(t *testing.T) {
	const dup, proceed, reject, wait = orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_PROCEED, orkstackv1.Disposition_DISPOSITION_REJECT,
		orkstackv1.Disposition_DISPOSITION_WAIT
	run := "ork-claimed"
	for name, c := range map[string]struct {
		set     func(*testing.T, *offerWorld)
		want    orkstackv1.Disposition
		reason  string
		cause   string
		run     string
		retired bool
		marked  bool
	}{
		"a started run": {func(t *testing.T, w *offerWorld) {
			w.claim(t, run, true)
			w.starts.err, w.archive.err = errFault, errFault // neither is asked
		}, dup, "", "", run, false, true},
		"a run the cluster holds": {func(t *testing.T, w *offerWorld) { w.claim(t, run, false); w.starts.runs[run] = startOf("default") }, dup, "", "", run, false, true},
		"a run the archive holds": {func(t *testing.T, w *offerWorld) {
			w.claim(t, run, false)
			w.archive.runs[run] = &orkstackv1.Run{Namespace: "default"}
		}, dup, "", "", run, true, true},
		"a run neither holds":     {func(t *testing.T, w *offerWorld) { w.claim(t, run, false) }, proceed, "", "", "", false, false},
		"another namespace's run": {func(t *testing.T, w *offerWorld) { w.claim(t, run, false); w.starts.runs[run] = startOf("payments") }, proceed, "", "", "", false, false},
		"the cluster down":        {func(t *testing.T, w *offerWorld) { w.claim(t, run, false); w.starts.err = errFault }, wait, "", waitOutage, "", false, false},
		"the cluster slow":        {func(t *testing.T, w *offerWorld) { w.claim(t, run, false); w.starts.err = context.DeadlineExceeded }, wait, "", waitTimeout, "", false, false},
		"the archive down":        {func(t *testing.T, w *offerWorld) { w.claim(t, run, false); w.archive.err = errFault }, wait, "", waitOutage, "", false, false},
		"a start event that does not decode": {func(t *testing.T, w *offerWorld) {
			w.claim(t, run, false)
			w.starts.err = fmt.Errorf("x: %w", engine.ErrRunRecordUndecodable)
		}, reject, eventsources.ReasonMessageUnprocessable, "", "", false, false},
		"an archive line that does not decode": {func(t *testing.T, w *offerWorld) {
			w.claim(t, run, false)
			w.archive.err = fmt.Errorf("x: %w", engine.ErrRunRecordUndecodable)
		}, reject, eventsources.ReasonMessageUnprocessable, "", "", false, false},
		"a claim that does not decode": {func(t *testing.T, w *offerWorld) { w.spoil(t, `{"schemaVersion":1,"identity":4}`) }, reject, eventsources.ReasonMessageUnprocessable, "", "", false, false},
		"a newer engine's claim":       {func(t *testing.T, w *offerWorld) { w.spoil(t, `{"schemaVersion":2}`) }, wait, "", waitRollout, "", false, false},
		"half a claim":                 {func(t *testing.T, w *offerWorld) { w.spoil(t, `{"schemaVersion":1,`) }, wait, "unread=claim", waitOutage, "", false, false},
	} {
		t.Run(name, func(t *testing.T) {
			w := newOfferWorld(t)
			c.set(t, w)
			got := w.offer(t, w.request())
			if got.GetDisposition() != c.want || got.GetReason() != c.reason || got.GetCause() != c.cause || got.GetRunId() != c.run || got.GetRetiredRun() != c.retired {
				t.Fatalf("%v; want %s %s %s %s %t", got, c.want, c.reason, c.cause, c.run, c.retired)
			}
			if c.marked && !w.started(t) {
				t.Fatal("the run that was found has no marker")
			}
			// A pre-rejection changes nothing a claim answered, and is what is
			// answered where it answered nothing.
			req := w.request()
			req.PreRejection = eventsources.ReasonMessageNotJSON
			want, reason := c.want, c.reason
			if c.want == proceed {
				want, reason = reject, eventsources.ReasonMessageNotJSON
			}
			if again := w.offer(t, req); again.GetDisposition() != want || again.GetReason() != reason {
				t.Fatalf("with a pre-rejection: %v; want %s %s", again, want, reason)
			}
		})
	}
}

// TestOfferMessage_ADuplicate: a marker that cannot be written
// leaves the answer, and what was staged is removed.
func TestOfferMessage_ADuplicate(t *testing.T) {
	w := newOfferWorld(t)
	w.claim(t, "ork-claimed", false)
	w.starts.runs["ork-claimed"] = startOf("default")
	sum := sha256.Sum256([]byte("x"))
	key, err := artifacts.StagingKey("default", hostSource, offeredID, hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.records.PutStaged(context.Background(), key, []byte("x")); err != nil {
		t.Fatal(err)
	}
	w.s.messageClaims = unmarkedClaims{w.claims}
	if got := w.offer(t, w.request()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_DUPLICATE {
		t.Fatalf("%v", got)
	}
	if left, err := os.ReadDir(filepath.Dir(filepath.Join(w.dir, filepath.FromSlash(key)))); err == nil && len(left) > 0 {
		t.Fatalf("staged parts were left: %v", left)
	}
	w.s.stagedParts = unremovedParts{}
	if got := w.offer(t, w.request()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_DUPLICATE {
		t.Fatalf("what could not be removed changed the answer: %v", got)
	}
}

// unmarkedClaims is a claims store that cannot be written, which a local
// directory is not.
type unmarkedClaims struct{ *artifacts.MessageClaimStore }

func (unmarkedClaims) MarkStarted(context.Context, artifacts.ClaimScope, string, string, time.Time) error {
	return errFault
}

// unreadClaims is a claims store that does not answer.
type unreadClaims struct {
	*artifacts.MessageClaimStore
	err error
}

func (c unreadClaims) Get(context.Context, artifacts.ClaimScope, string) (artifacts.MessageClaim, bool, error) {
	return artifacts.MessageClaim{}, false, c.err
}

type unremovedParts struct{}

func (unremovedParts) RemoveStaged(context.Context, string, string, string) error { return errFault }

// TestOfferMessage_WhatIsTheMessages: a message that can never be accepted is
// rejected with its reason, and its source is left alone.
func TestOfferMessage_WhatIsTheMessages(t *testing.T) {
	for name, c := range map[string]struct {
		edit   func(*orkstackv1.OfferMessageRequest)
		reason string
	}{
		"a field missing": {func(r *orkstackv1.OfferMessageRequest) { r.Extracted = nil }, eventsources.ReasonMessageFieldMissing},
		"a field's type":  {func(r *orkstackv1.OfferMessageRequest) { r.Extracted["order_id"] = structpb.NewBoolValue(true) }, eventsources.ReasonMessageFieldType},
		"the input too large": {func(r *orkstackv1.OfferMessageRequest) {
			r.Extracted["order_id"] = structpb.NewStringValue(strings.Repeat("x", 20<<10))
		}, eventsources.ReasonMessageInputTooLarge},
		"the body too large": {func(r *orkstackv1.OfferMessageRequest) { r.BodyBytes, r.BodySha256 = 64<<20, "" }, eventsources.ReasonMessageTooLarge},
		"a subject":          {func(r *orkstackv1.OfferMessageRequest) { r.Message.Subject = strings.Repeat("s", 2<<10) }, eventsources.ReasonMessageTextInvalid},
		"no identity":        {func(r *orkstackv1.OfferMessageRequest) { r.Message.Id = "" }, eventsources.ReasonMessageIdentityInvalid},
		"an identity":        {func(r *orkstackv1.OfferMessageRequest) { r.Message.Id = "a\nb" }, eventsources.ReasonMessageIdentityInvalid},
		"an identity, and the body too large": {func(r *orkstackv1.OfferMessageRequest) {
			r.Message.Id, r.BodyBytes, r.BodySha256, r.PreRejection = strings.Repeat("i", 300), 64<<20, "", eventsources.ReasonMessageTooLarge
		}, eventsources.ReasonMessageIdentityInvalid},
		"a pre-rejection over its bound": {func(r *orkstackv1.OfferMessageRequest) {
			r.BodyBytes, r.BodySha256, r.PreRejection = 64<<20, "", eventsources.ReasonMessageTooLarge
		}, eventsources.ReasonMessageTooLarge},
		"an identity pre-rejected": {func(r *orkstackv1.OfferMessageRequest) {
			r.Message.Id, r.PreRejection = "cut", eventsources.ReasonMessageIdentityInvalid
		}, eventsources.ReasonMessageIdentityInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			w := newOfferWorld(t)
			req := w.request()
			c.edit(req)
			if got := w.offer(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REJECT || got.GetReason() != c.reason {
				t.Fatalf("%v; want %s", got, c.reason)
			}
			if w.stored(t).GetPause() != nil {
				t.Fatal("a message paused its source")
			}
		})
	}
	// An identity that is none names no claim, so none is read for it.
	w := newOfferWorld(t)
	w.claim(t, "ork-claimed", true)
	req := w.request()
	req.PreRejection = eventsources.ReasonMessageIdentityInvalid
	if got := w.offer(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REJECT {
		t.Fatalf("%v", got)
	}
}

// TestDoorVerdict_AMessagesValuesAreNotLogged: a refusal of a message may
// quote its values, which are a publisher's; its code alone is logged.
func TestDoorVerdict_AMessagesValuesAreNotLogged(t *testing.T) {
	w := newOfferWorld(t)
	var logged bytes.Buffer
	w.s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	call := &heldCall{src: w.stored(t), ref: held.Ref{Namespace: "default", Name: "orders", SourceID: hostSource}}
	w.s.doorRefusal(context.Background(), call, "prepare", refused(refusalReject, eventsources.ReasonMessageConcurrencyKeyInvalid, errors.New("key \"s3cret\"")))
	if strings.Contains(logged.String(), "s3cret") || !strings.Contains(logged.String(), eventsources.ReasonMessageConcurrencyKeyInvalid) {
		t.Fatal(logged.String())
	}
}

// TestOfferMessage_NoArchiveIsNoAnswer: an engine that reads no archive does
// not know that a claimed run never started.
func TestOfferMessage_NoArchiveIsNoAnswer(t *testing.T) {
	w := newOfferWorld(t)
	w.claim(t, "ork-claimed", false)
	w.s.archive = nil
	if got := w.offer(t, w.request()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT {
		t.Fatalf("%v", got)
	}
}

// TestOfferMessage_AnOrkersWordIsChecked: what no honest Orker sends is no
// answer about the message.
func TestOfferMessage_AnOrkersWordIsChecked(t *testing.T) {
	for name, edit := range map[string]func(*orkstackv1.OfferMessageRequest){
		"a digest":                 func(r *orkstackv1.OfferMessageRequest) { r.BodySha256 = "abc" },
		"a digest in capitals":     func(r *orkstackv1.OfferMessageRequest) { r.BodySha256 = strings.Repeat("A", 64) },
		"no digest":                func(r *orkstackv1.OfferMessageRequest) { r.BodySha256 = "" },
		"a negative size":          func(r *orkstackv1.OfferMessageRequest) { r.BodyBytes = -1 },
		"a negative count":         func(r *orkstackv1.OfferMessageRequest) { r.DeliveryCount = -1 },
		"a reason of the engine's": func(r *orkstackv1.OfferMessageRequest) { r.PreRejection = eventsources.ReasonMessageFieldMissing },
		"a reason of nobody's":     func(r *orkstackv1.OfferMessageRequest) { r.PreRejection = "SOMETHING" },
		"a request over its bound": func(r *orkstackv1.OfferMessageRequest) {
			r.Message.Broker = map[string]string{"a": strings.Repeat("x", maxDoorRequest)}
		},
	} {
		w := newOfferWorld(t)
		req := w.request()
		edit(req)
		_, err := w.s.OfferMessage(asHost(hostOrker), connect.NewRequest(req))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestOfferMessage_WhatIsTheSources: what every message of the source would
// meet pauses it, once the message's claim was read.
func TestOfferMessage_WhatIsTheSources(t *testing.T) {
	for name, c := range map[string]struct {
		set  func(*testing.T, *offerWorld)
		code string
	}{
		"its Orkflow is gone": {func(t *testing.T, w *offerWorld) {
			w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "nothing" })
		}, "ORKFLOW_NOT_FOUND"},
		"its version is gone": {func(t *testing.T, w *offerWorld) {
			w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowVersion = "9.9.9" })
		}, "ORKFLOW_NOT_FOUND"},
		"its mapping": {func(t *testing.T, w *offerWorld) {
			w.save(t, func(src *orkstackv1.EventSource) { src.Map["nothing"] = &orkstackv1.MapRule{Pointer: "/x"} })
		}, eventsources.ReasonMappingInvalid},
		"an Orkflow it cannot start": {func(t *testing.T, w *offerWorld) {
			w.put(t, &orkstackv1.Orkflow{Namespace: "default", Name: "asks", Version: "1.0.0",
				Tasks: []*orkstackv1.Task{{Id: "gate", Await: &orkstackv1.AwaitSpec{Callback: true}}}})
			w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "asks" })
		}, eventsources.ReasonOrkflowUnsupported},
	} {
		t.Run(name, func(t *testing.T) {
			w := newOfferWorld(t)
			c.set(t, w)
			got := w.offer(t, w.request())
			if got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP || got.GetReason() != eventsources.ReasonPaused || got.GetRefusalCode() != c.code {
				t.Fatalf("%v; want a stop of %s", got, c.code)
			}
			if p := w.stored(t).GetPause(); p.GetCode() != eventsources.PauseRefused || p.GetRefusalCode() != c.code || p.GetCause() != orkstackv1.PauseCause_PAUSE_CAUSE_ENGINE {
				t.Fatalf("%v", p)
			}
			if len(w.workflows.synced) != 1 {
				t.Fatalf("the source's workflow was told %v", w.workflows.synced)
			}
		})
	}
}

// TestOfferMessage_ASourceThatChangedWhileItWasJudged: a refusal judged under
// a document somebody has replaced pauses nothing; the message is judged
// again under the new one.
func TestOfferMessage_ASourceThatChangedWhileItWasJudged(t *testing.T) {
	w := newOfferWorld(t)
	w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "nothing" })
	w.s.messageClaims = changingClaims{w.claims, func() {
		w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName, src.JudgingRevision = "deploy", 5 })
	}}
	got := w.offer(t, w.request())
	if got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REFRESH || len(got.GetJudging().GetSpec()) == 0 {
		t.Fatalf("%v", got)
	}
	if w.stored(t).GetPause() != nil {
		t.Fatal("a source that was changed was paused for what it was")
	}
}

// changingClaims changes the world between the held check and the judgment.
type changingClaims struct {
	*artifacts.MessageClaimStore
	change func()
}

func (c changingClaims) Get(ctx context.Context, scope artifacts.ClaimScope, id string) (artifacts.MessageClaim, bool, error) {
	c.change()
	return c.MessageClaimStore.Get(ctx, scope, id)
}

// TestOfferMessage_WhatCouldNotBeRead: a read that was not had is a wait, and
// one the store would refuse again is uncertain.
func TestOfferMessage_WhatCouldNotBeRead(t *testing.T) {
	for name, c := range map[string]struct {
		set   func(*offerWorld)
		want  orkstackv1.Disposition
		cause string
		code  string
	}{
		"the claim":            {func(w *offerWorld) { w.s.messageClaims = unreadClaims{w.claims, errors.New("503")} }, orkstackv1.Disposition_DISPOSITION_WAIT, waitOutage, ""},
		"the claim, in time":   {func(w *offerWorld) { w.s.messageClaims = unreadClaims{w.claims, context.DeadlineExceeded} }, orkstackv1.Disposition_DISPOSITION_WAIT, waitTimeout, ""},
		"the claim, refused":   {func(w *offerWorld) { w.s.messageClaims = unreadClaims{w.claims, s3Status(403)} }, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, "", codeStoreRefused},
		"the Orkflow":          {func(w *offerWorld) { w.s.orkflows = unreadOrkflows{w.s.orkflows, errors.New("503")} }, orkstackv1.Disposition_DISPOSITION_WAIT, waitOutage, ""},
		"the Orkflow, refused": {func(w *offerWorld) { w.s.orkflows = unreadOrkflows{w.s.orkflows, s3Status(403)} }, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, "", codeStoreRefused},
	} {
		t.Run(name, func(t *testing.T) {
			w := newOfferWorld(t)
			c.set(w)
			if got := w.offer(t, w.request()); got.GetDisposition() != c.want || got.GetCause() != c.cause || got.GetRefusalCode() != c.code {
				t.Fatalf("%v; want %s %s %s", got, c.want, c.cause, c.code)
			}
			if w.stored(t).GetPause() != nil {
				t.Fatal("a read that was not had paused the source")
			}
		})
	}
}

// unreadOrkflows is an Orkflow store that does not answer.
type unreadOrkflows struct {
	engine.OrkflowStore
	err error
}

func (o unreadOrkflows) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, o.err
}

// TestOfferMessage_Refresh: a message judged under another judging spec is
// judged again, before its claim is read, under the current one.
func TestOfferMessage_Refresh(t *testing.T) {
	w := newOfferWorld(t)
	w.s.messageClaims = unreadClaims{w.claims, errFault}
	req := w.request()
	req.Instance.JudgingRevision = 3
	got := w.offer(t, req)
	if got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REFRESH {
		t.Fatalf("%v", got)
	}
	if _, _, err := eventsources.ReadJudgingSpec(got.GetJudging().GetSpec()); err != nil {
		t.Fatal(err)
	}
	// What the held check answers comes before all of it.
	w.save(t, func(src *orkstackv1.EventSource) { src.Pause = &orkstackv1.EventSourcePause{} })
	if got := w.offer(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP || got.GetReason() != eventsources.ReasonPaused {
		t.Fatalf("%v", got)
	}
}

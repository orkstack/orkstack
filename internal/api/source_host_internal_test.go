package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
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

const (
	hostOrker  = "orker-a"
	hostSource = "evs-0123456789abcdef"
	hostDigest = "sha256-of-a-spec"
)

// hostWorld is a cluster, a store and a fleet in which orker-a holds one
// instance of a source, each read an error can be put in.
type hostWorld struct {
	wf     held.Workflow
	doc    held.Document
	rec    artifacts.ConfirmRecord
	row    held.OrkerRow
	tomb   bool
	grant  bool
	err    error
	failOn string
	reads  int
}

func newHostWorld() *hostWorld {
	src := &orkstackv1.EventSource{Namespace: "payments", Name: "orders", Id: hostSource, Pool: "main", Rate: 10, Instances: 2, JudgingRevision: 4}
	return &hostWorld{
		wf: held.Workflow{Open: true, Pending: []held.Pending{{ActivityID: held.ActivityID(0, 7), State: held.Started, Attempt: 1, LastWorker: hostOrker}},
			Memo: held.Memo{Slots: map[int32]held.Slot{0: {Current: held.SlotEpoch{Epoch: 7, Digest: hostDigest, AdmissionSave: "save-1"}}}}},
		doc:   held.Document{Found: true, ID: hostSource, Pool: "main", Source: src},
		rec:   artifacts.ConfirmRecord{OrkerID: hostOrker, ServiceDigest: hostDigest, AdmissionSave: "save-1"},
		row:   held.OrkerRow{Pools: []string{"main"}, Fit: true, MaxInstances: 2},
		grant: true,
	}
}

func (w *hostWorld) read(kind string) error {
	w.reads++
	if w.failOn == kind {
		return w.err
	}
	return nil
}

// Held and Read are the replica's access document.
func (w *hostWorld) Held(*authz.Subject, authz.Permission, string) (granted, fresh bool) {
	return w.grant, true
}

func (w *hostWorld) Read(context.Context) (held.Decider, error) {
	if err := w.read("access"); err != nil {
		return nil, err
	}
	return func(*authz.Subject, authz.Permission, string) bool { return w.grant }, nil
}

func (w *hostWorld) service(t *testing.T) *Service {
	t.Helper()
	s := newAccessService(t)
	s.access, _ = accessFor(t, authn.ModeEnforce, nil)
	s.logger = slog.New(slog.DiscardHandler)
	s.eventSourceSettings = eventsources.DefaultSettings()
	s.eventSourceSettings.Enabled = true
	s.sourceCeiling = newSourceCeiling(s.eventSourceSettings.Limits)
	s.system.BuildID = "b1"
	s.heldReader = held.NewReader(held.Clients{
		Describe:   func(context.Context, string) (held.Workflow, error) { return w.wf, w.read("describe") },
		Tombstoned: func(context.Context, string) (bool, error) { return w.tomb, w.read("tombstone") },
		Document: func(context.Context, string, string) (held.Document, error) {
			return w.doc, w.read("document")
		},
		Confirmed: func(context.Context, artifacts.InstanceAttempt) (artifacts.ConfirmRecord, bool, error) {
			return w.rec, true, w.read("confirm")
		},
		Orker:  func(context.Context, string) (held.OrkerRow, error) { return w.row, w.read("orker") },
		Access: w,
	}, held.Options{InventoryAge: time.Minute})
	return s
}

func hostRef() *orkstackv1.InstanceRef {
	return &orkstackv1.InstanceRef{Namespace: "payments", Name: "orders", EventSourceId: hostSource, Index: 0, Epoch: 7, Attempt: 1,
		ServiceDigest: hostDigest, JudgingRevision: 4}
}

// asHost is a context of a caller whose credential names id.
func asHost(id string) context.Context {
	return authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), &authn.Principal{ID: "service:orker", OrkerID: id})
}

// TestSourceCall_TheHeldCheckSaysWhatAnOrkerIsAnswered: each of the check's
// results has one answer. Nothing about the calling Orker ever pauses or
// stops a source, an absence is one replica's and never a stop, and a read
// that was not had is a wait.
func TestSourceCall_TheHeldCheckSaysWhatAnOrkerIsAnswered(t *testing.T) {
	const stop, refuse, wait = orkstackv1.Disposition_DISPOSITION_STOP, orkstackv1.Disposition_DISPOSITION_REFUSED, orkstackv1.Disposition_DISPOSITION_WAIT
	for name, c := range map[string]struct {
		edit   func(*hostWorld)
		want   orkstackv1.Disposition
		reason string
		cause  string
	}{
		"held":                {func(*hostWorld) {}, orkstackv1.Disposition_DISPOSITION_OK, "", ""},
		"not held":            {func(w *hostWorld) { w.wf.Pending = nil }, stop, eventsources.ReasonNotHeld, ""},
		"another Orker's":     {func(w *hostWorld) { w.wf.Pending[0].LastWorker = "orker-b" }, stop, eventsources.ReasonNotHeld, ""},
		"no source host":      {func(w *hostWorld) { w.row.Fit = false }, stop, eventsources.ReasonNotHeld, ""},
		"deleted":             {func(w *hostWorld) { w.tomb = true }, stop, eventsources.ReasonDeletedDocumentReappeared, ""},
		"paused":              {func(w *hostWorld) { w.doc.Paused = true }, stop, eventsources.ReasonPaused, ""},
		"marked":              {func(w *hostWorld) { w.doc.Deleting = true }, stop, eventsources.ReasonDeleting, ""},
		"another source's":    {func(w *hostWorld) { w.doc.ID = "evs-fedcba9876543210" }, refuse, eventsources.ReasonDocumentMissing, ""},
		"no document":         {func(w *hostWorld) { w.doc = held.Document{} }, refuse, eventsources.ReasonDocumentMissing, ""},
		"cannot decode":       {func(w *hostWorld) { w.failOn, w.err = "document", fmt.Errorf("x: %w", held.ErrUndecodable) }, refuse, eventsources.ReasonDocumentUnreadable, ""},
		"the cluster down":    {func(w *hostWorld) { w.failOn, w.err = "describe", errors.New("unavailable") }, wait, "", waitOutage},
		"the cluster slow":    {func(w *hostWorld) { w.failOn, w.err = "describe", context.DeadlineExceeded }, wait, "", waitTimeout},
		"the store down":      {func(w *hostWorld) { w.failOn, w.err = "tombstone", errors.New("503") }, wait, "", waitOutage},
		"a refusing store":    {func(w *hostWorld) { w.failOn, w.err = "document", s3Status(403) }, wait, "", waitOutage},
		"the access document": {func(w *hostWorld) { w.grant, w.failOn, w.err = false, "access", errors.New("503") }, wait, "", waitOutage},
	} {
		t.Run(name, func(t *testing.T) {
			w := newHostWorld()
			c.edit(w)
			call, answer, err := w.service(t).sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef())
			if err != nil {
				t.Fatal(err)
			}
			if c.want == orkstackv1.Disposition_DISPOSITION_OK {
				if answer != nil || call == nil || call.src.GetName() != "orders" {
					t.Fatalf("%+v, %+v", call, answer)
				}
				return
			}
			if call != nil || answer.GetDisposition() != c.want || answer.GetReason() != c.reason || answer.GetCause() != c.cause ||
				answer.GetRunId() != "" || answer.GetRefusalCode() != "" {
				t.Fatalf("%+v, %+v; want %v %s %s and nothing else", call, answer, c.want, c.reason, c.cause)
			}
		})
	}
}

// TestSourceCall_NotPermittedIsTheOrkersOwn: a role that lacks the permission
// fails that Orker's attempt, named, and is neither a stop nor a pause.
func TestSourceCall_NotPermittedIsTheOrkersOwn(t *testing.T) {
	w := newHostWorld()
	w.grant = false
	call, answer, err := w.service(t).sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef())
	if call != nil || answer != nil || connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("%+v, %+v, %v", call, answer, err)
	}
	for _, want := range []string{eventsources.ReasonHostNotPermitted, hostOrker, string(authz.PermEventSourceHost), `"main"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%v does not name %s", err, want)
		}
	}
}

// TestSourceCall_Guards: a replica that may not serve the service refuses
// before it reads anything, so that another is asked; a caller that is no
// Orker is refused as a caller.
func TestSourceCall_Guards(t *testing.T) {
	orker := func(edit func(*Service)) func(*Service) context.Context {
		return func(s *Service) context.Context { edit(s); return asHost(hostOrker) }
	}
	for name, c := range map[string]struct {
		edit   func(*Service) context.Context
		reason string
		code   connect.Code
	}{
		"off":               {orker(func(s *Service) { s.eventSourceSettings.Enabled = false }), eventsources.ReasonDisabled, 0},
		"no build id":       {orker(func(s *Service) { s.system.BuildID = "" }), eventsources.ReasonRequireVersioning, 0},
		"no reader":         {orker(func(s *Service) { s.heldReader = nil }), eventsources.ReasonDisabled, 0},
		"nobody is checked": {orker(func(s *Service) { s.access = nil }), eventsources.ReasonRequireAuthentication, 0},
		// event_sources.allow_asserted never opens this door.
		"asserted callers": {orker(func(s *Service) { s.access = nil; s.eventSourceSettings.AllowAsserted = true }), eventsources.ReasonRequireAuthentication, 0},
		"a request's mode": {func(*Service) context.Context {
			return authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeObserve), &authn.Principal{ID: "service:orker", OrkerID: hostOrker})
		}, eventsources.ReasonRequireAuthentication, 0},
		"no caller": {func(*Service) context.Context { return authn.WithMode(context.Background(), authn.ModeEnforce) }, "", connect.CodeUnauthenticated},
		"a person":  {func(*Service) context.Context { return asHost("") }, eventsources.ReasonHostNotPermitted, connect.CodePermissionDenied},
	} {
		t.Run(name, func(t *testing.T) {
			w := newHostWorld()
			s := w.service(t)
			ctx := c.edit(s)
			call, answer, err := s.sourceCall(ctx, held.PurposeOffer, hostRef())
			if call != nil || w.reads != 0 {
				t.Fatalf("%+v after %d reads", call, w.reads)
			}
			if c.code != 0 {
				if connect.CodeOf(err) != c.code || !strings.Contains(err.Error(), c.reason) {
					t.Fatalf("%v; want %v", err, c.code)
				}
				return
			}
			if err != nil || answer.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REFUSED || answer.GetReason() != c.reason {
				t.Fatalf("%+v, %v; want REFUSED %s", answer, err, c.reason)
			}
		})
	}
}

// TestSourceCall_TheCeiling: offers and starts of one source past twice its
// rate and instances a second are refused before anything is read, and only
// that source's; a confirmation is never counted.
func TestSourceCall_TheCeiling(t *testing.T) {
	w := newHostWorld()
	s := w.service(t)
	now := time.Now()
	s.sourceCeiling.now = func() time.Time { return now }
	// The first call is let through by the deployment's own most, and says
	// what the source's is: 2 x 10 x 2 a second.
	if _, answer, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef()); err != nil || answer != nil {
		t.Fatalf("%+v, %v", answer, err)
	}
	now = now.Add(time.Second)
	var refused, passed int
	for range 200 {
		before := w.reads
		_, _, err := s.sourceCall(asHost(hostOrker), held.PurposeStart, hostRef())
		switch {
		case err == nil:
			passed++
		case connect.CodeOf(err) == connect.CodeResourceExhausted && w.reads == before:
			refused++
		default:
			t.Fatalf("%v after %d reads", err, w.reads-before)
		}
	}
	if passed != 40 || refused != 160 {
		t.Fatalf("%d passed and %d refused within a moment; want 40 and 160", passed, refused)
	}
	if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeConfirm, hostRef()); err != nil {
		t.Fatalf("a confirmation past the ceiling: %v", err)
	}
	other := hostRef()
	other.EventSourceId = "evs-fedcba9876543210"
	if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, other); err != nil {
		t.Fatalf("another source is refused for this one's: %v", err)
	}
	now = now.Add(time.Second)
	if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef()); err != nil {
		t.Fatalf("a second later: %v", err)
	}
	// What names no source is refused before anything is kept for it.
	kept := len(s.sourceCeiling.byCaller)
	long := hostRef()
	long.EventSourceId = strings.Repeat("e", 100<<10)
	name := hostRef()
	name.Name = strings.Repeat("n", 100<<10)
	for _, ref := range []*orkstackv1.InstanceRef{nil, long, name, {Namespace: "a/b", Name: "orders", EventSourceId: hostSource}} {
		if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, ref); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("a request that names no source: %v", err)
		}
	}
	if n := len(s.sourceCeiling.byCaller); n != kept {
		t.Fatalf("%d ceilings were kept for what names no source", n-kept)
	}
	// Another Orker spends its own ceiling, and none of this one's.
	for range 200 {
		_, _, _ = s.sourceCall(asHost("orker-b"), held.PurposeOffer, hostRef())
	}
	if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef()); err != nil {
		t.Fatalf("refused for another Orker's calls: %v", err)
	}
}

// A caller that makes up sources grows nothing without bound.
// TestSourceCall_ASourceHasItsOwnCeilingFromItsFirstHeldCall: on a replica that
// knows nothing of an Orker, the first call that finds it holding a source, a
// confirmation included, gives that source a ceiling of its own, which what
// the Orker spends on others does not touch.
func TestSourceCall_ASourceHasItsOwnCeilingFromItsFirstHeldCall(t *testing.T) {
	w := newHostWorld()
	s := w.service(t)
	now := time.Now()
	s.sourceCeiling.now = func() time.Time { return now }
	if _, answer, err := s.sourceCall(asHost(hostOrker), held.PurposeConfirm, hostRef()); err != nil || answer != nil {
		t.Fatalf("%+v, %v", answer, err)
	}
	for i := range 3 * int(s.sourceCeiling.most) {
		s.sourceCeiling.allow(hostOrker, fmt.Sprintf("evs-%016x", i))
	}
	for i := range 40 {
		if _, _, err := s.sourceCall(asHost(hostOrker), held.PurposeOffer, hostRef()); err != nil {
			t.Fatalf("call %d of a source it holds is refused for what it spent on others: %v", i, err)
		}
	}
}

func TestSourceCeiling_IsBounded(t *testing.T) {
	c := newSourceCeiling(eventsources.DefaultSettings().Limits)
	now := time.Now()
	c.now = func() time.Time { return now }
	src := &orkstackv1.EventSource{Id: hostSource, Rate: 1, Instances: 1}
	c.learn(hostOrker, src)
	for range 2 {
		if !c.allow(hostOrker, hostSource) {
			t.Fatal("refused within its ceiling")
		}
	}
	// Any number of sources an Orker does not hold keeps one ceiling, the
	// Orker's own, which they all spend, and resets none.
	passed := 0
	for i := range 3 * maxSourceCeilings {
		if c.allow(hostOrker, fmt.Sprintf("evs-%016x", i)) {
			passed++
		}
	}
	if n := len(c.byCaller); n != 2 || passed != int(c.most) {
		t.Fatalf("%d ceilings kept, %d passed; want 2 and %d", n, passed, int(c.most))
	}
	if c.allow(hostOrker, hostSource) {
		t.Fatal("what an Orker does not hold reset the ceiling of what it does")
	}
	for i := range 3 * maxSourceCeilings {
		c.allow(fmt.Sprintf("orker-%d", i), hostSource)
	}
	if n := len(c.byCaller); n > maxSourceCeilings {
		t.Fatalf("%d ceilings kept", n)
	}
}

// TestHeldReads: what the check reads of the store, the fleet and the access
// document, each told apart: a document that is not there from one this
// release cannot read, an empty pool from none, and a grant from a document
// that is not fresh.
func TestHeldReads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalEventSourceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	named := &orkstackv1.Namespace{Name: "default"}
	read := HeldDocument(store, namespacesOf{named})
	if d, err := read(ctx, "default", "orders"); err != nil || d.Found {
		t.Fatalf("no document: %+v, %v", d, err)
	}
	src := &orkstackv1.EventSource{Namespace: "default", Name: "orders", Id: hostSource, OrkflowName: "deploy", DeletingSince: timestamppb.Now()}
	if _, err := store.Create(ctx, src); err != nil {
		t.Fatal(err)
	}
	d, err := read(ctx, "default", "orders")
	if err != nil || !d.Found || d.ID != hostSource || d.Pool != task.DefaultPool || !d.Deleting || d.Paused || d.Source.(*orkstackv1.EventSource).GetOrkflowName() != "deploy" {
		t.Fatalf("%+v, %v", d, err)
	}
	named.DefaultPool = "gpu"
	if d, err := read(ctx, "default", "orders"); err != nil || d.Pool != "gpu" {
		t.Fatalf("the namespace's default pool: %+v, %v", d, err)
	}
	if _, err := HeldDocument(store, unreadNamespaces{})(ctx, "default", "orders"); err == nil || errors.Is(err, held.ErrUndecodable) {
		t.Fatalf("a namespace that cannot be read: %v", err)
	}
	damageDocument(t, dir)
	if _, err := read(ctx, "default", "orders"); !errors.Is(err, held.ErrUndecodable) {
		t.Fatalf("a damaged document: %v", err)
	}

	row := HeldOrkerRow(orkerAnswers{res: &orkstackv1.DescribeOrkerResult{ServiceHost: &orkstackv1.ServiceHost{Fit: true, MaxInstances: 3}}})
	if r, err := row(ctx, hostOrker); err != nil || len(r.Pools) != 1 || r.Pools[0] != task.DefaultPool || !r.Fit || r.MaxInstances != 3 {
		t.Fatalf("%+v, %v", r, err)
	}
	if r, err := HeldOrkerRow(orkerAnswers{res: &orkstackv1.DescribeOrkerResult{Pool: "gpu"}})(ctx, hostOrker); err != nil || r.Fit || r.MaxInstances != 0 || r.Pools[0] != "gpu" {
		t.Fatalf("an Orker that hosts nothing: %+v, %v", r, err)
	}
	if _, err := HeldOrkerRow(orkerAnswers{err: errors.New("no answer")})(ctx, hostOrker); err == nil {
		t.Fatal("an Orker that does not answer has a row")
	}
}

type unreadNamespaces struct{}

func (unreadNamespaces) Get(context.Context, string) (*orkstackv1.Namespace, error) {
	return nil, errors.New("503")
}

type orkerAnswers struct {
	res *orkstackv1.DescribeOrkerResult
	err error
}

func (o orkerAnswers) DescribeOrkerByID(context.Context, string) (*orkstackv1.DescribeOrkerResult, error) {
	return o.res, o.err
}

// TestHeldAccess: a grant may come from the copy the replica holds while it
// is fresh; past that it grants nothing until a read answers, and a read that
// fails decides nothing.
func TestHeldAccess(t *testing.T) {
	sub := &authz.Subject{ID: "serviceaccount:o", Kind: "service", Issuer: "serviceaccount"}
	cas := artifacts.NewMemoryCAS()
	docs := artifacts.NewAccessStoreOn(cas)
	binding := func(subject string, anyID bool) *orkstackv1.Binding {
		return &orkstackv1.Binding{Name: "b", Role: authz.RoleOrker, Scope: "pool:main", Subjects: []string{subject}, OrkerIdsAny: anyID}
	}
	var head *artifacts.AccessHead
	write := func(b *orkstackv1.Binding) {
		t.Helper()
		var err error
		if head, err = docs.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{b}}, head, artifacts.AccessWrite{At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	write(binding(sub.ID, false))
	now := time.Now()
	policies := NewPolicyCache(docs, PolicyCacheOptions{Now: func() time.Time { return now }})
	a := HeldAccess(policies)
	if granted, fresh := a.Held(sub, authz.PermEventSourceHost, "main"); granted || fresh {
		t.Fatalf("before any read: %v, %v", granted, fresh)
	}
	ask := func() bool {
		t.Helper()
		decide, err := a.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return decide(sub, authz.PermEventSourceHost, "main")
	}
	if !ask() {
		t.Fatal("the grant is not read")
	}
	if granted, fresh := a.Held(sub, authz.PermEventSourceHost, "main"); !granted || !fresh {
		t.Fatalf("the copy just read: %v, %v", granted, fresh)
	}
	if granted, _ := a.Held(sub, authz.PermEventSourceHost, "gpu"); granted {
		t.Fatal("granted at another pool")
	}
	// A read made now sees what was written since the copy.
	write(binding(sub.ID, true))
	now = now.Add(time.Second)
	if ask() {
		t.Fatal("a binding that accepts any Orker id hosts a source")
	}
	now = now.Add(time.Hour)
	if granted, fresh := a.Held(sub, authz.PermEventSourceHost, "main"); granted || fresh {
		t.Fatalf("a copy past its bound: %v, %v", granted, fresh)
	}
	cas.SetFailure(errors.New("down"))
	if _, err := a.Read(context.Background()); err == nil {
		t.Fatal("a store that does not answer decided")
	}
}

// confirmWorld is hostWorld with a stored source, its service spec and the
// engine's records, the stores real ones.
type confirmWorld struct {
	*hostWorld
	s         *Service
	store     *artifacts.BlobEventSourceStore
	instances *artifacts.EventSourceInstanceStore
	workflows *toldWorkflows
	digest    string
}

// toldWorkflows is a source's workflow as the door tells it.
type toldWorkflows struct {
	started []held.Ref
	reports []string
	synced  []string
	err     error
}

func (w *toldWorkflows) Report(_ context.Context, ref held.Ref, what orkstackv1.InstanceReport, reason string) error {
	w.reports = append(w.reports, fmt.Sprintf("%d/%d %s %s", ref.Index, ref.Epoch, what, reason))
	return w.err
}

func (w *toldWorkflows) Started(_ context.Context, ref held.Ref) error {
	w.started = append(w.started, ref)
	return w.err
}

func (w *toldWorkflows) Start(context.Context, *orkstackv1.EventSource) (engine.SourceStart, error) {
	return engine.SourceSignalled, w.err
}

func (w *toldWorkflows) Sync(_ context.Context, id string) error {
	w.synced = append(w.synced, id)
	return w.err
}

func newConfirmWorld(t *testing.T) *confirmWorld {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalEventSourceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	instances, err := artifacts.NewLocalEventSourceInstanceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := &orkstackv1.EventSource{Namespace: "default", Name: "orders", Id: hostSource, OrkflowName: "deploy", Pool: "main", Rate: 10, Instances: 2, ServiceRevision: 1, JudgingRevision: 4,
		Uses: "ghcr.io/acme/source:1", Image: "ghcr.io/acme/source@sha256:" + strings.Repeat("a", 64),
		Secrets: []*orkstackv1.SecretRef{{Name: "token", Source: "env:BROKER_TOKEN"}}}
	if _, err := store.Create(ctx, src); err != nil {
		t.Fatal(err)
	}
	_, body, digest, err := eventsources.BuildServiceSpec(src, eventsources.Image{Contracts: []int32{1}, Profiles: []int32{1}}, eventsources.DefaultSettings().Limits, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutSpec(ctx, hostSource, digest, body); err != nil {
		t.Fatal(err)
	}
	w := &confirmWorld{hostWorld: newHostWorld(), store: store, instances: instances, workflows: &toldWorkflows{}, digest: digest}
	w.wf.Memo.Slots[0] = held.Slot{Current: held.SlotEpoch{Epoch: 7, Digest: digest, AdmissionSave: "save-1"}}
	w.s = w.hostWorld.service(t)
	w.s.heldReader = held.NewReader(held.Clients{
		Describe:   func(context.Context, string) (held.Workflow, error) { return w.wf, w.read("describe") },
		Tombstoned: instances.Tombstoned,
		Document:   HeldDocument(store, namespacesOf{&orkstackv1.Namespace{Name: "default"}}),
		Confirmed:  instances.Confirmed,
		Orker:      func(context.Context, string) (held.OrkerRow, error) { return w.row, nil },
		Access:     w.hostWorld,
	}, held.Options{})
	w.s.eventSources, w.s.sourceInstances, w.s.sourceWorkflows = store, instances, w.workflows
	return w
}

func (w *confirmWorld) ref(revision int64) *orkstackv1.InstanceRef {
	r := hostRef()
	r.Namespace, r.ServiceDigest, r.JudgingRevision = "default", w.digest, revision
	return r
}

func (w *confirmWorld) confirm(t *testing.T, want bool, revision int64) *orkstackv1.ConfirmInstanceResponse {
	t.Helper()
	resp, err := w.s.ConfirmInstance(asHost(hostOrker), connect.NewRequest(&orkstackv1.ConfirmInstanceRequest{Instance: w.ref(revision), WantServiceSpec: want}))
	if err != nil {
		t.Fatal(err)
	}
	answered(t, "ConfirmInstance", resp.Msg.GetAnswer())
	return resp.Msg
}

// doorSets are what each call may answer: what an Orker reads of it
// (internal/orker/sourcehost), and nothing else is an answer.
var doorSets = map[string][]orkstackv1.Disposition{
	"ConfirmInstance": {orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFUSED},
	"ReportInstance": {orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFUSED},
	"OfferMessage": {orkstackv1.Disposition_DISPOSITION_PROCEED, orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_REJECT,
		orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFRESH, orkstackv1.Disposition_DISPOSITION_REFUSED},
	"StartMessageRun": {orkstackv1.Disposition_DISPOSITION_ACCEPTED, orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_REJECT,
		orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_REFRESH, orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN, orkstackv1.Disposition_DISPOSITION_REFUSED},
	"ProbeEventSource": {orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN,
		orkstackv1.Disposition_DISPOSITION_STOP, orkstackv1.Disposition_DISPOSITION_REFUSED},
	"RequestEventSourcePause": {orkstackv1.Disposition_DISPOSITION_STOP, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_REFUSED},
}

// answered holds an answer to its call's set and to what each disposition
// carries: every answer the door's tests get goes through it.
func answered(t *testing.T, rpc string, a *orkstackv1.DoorAnswer) *orkstackv1.DoorAnswer {
	t.Helper()
	if !slices.Contains(doorSets[rpc], a.GetDisposition()) {
		t.Fatalf("%s answered %v", rpc, a)
	}
	carries := func(is bool, what string, has bool) {
		t.Helper()
		if is != has {
			t.Fatalf("%s: %s of %v", rpc, what, a)
		}
	}
	d := a.GetDisposition()
	carries(d == orkstackv1.Disposition_DISPOSITION_ACCEPTED || d == orkstackv1.Disposition_DISPOSITION_DUPLICATE, "the run id", a.GetRunId() != "")
	carries(d == orkstackv1.Disposition_DISPOSITION_WAIT, "the cause", a.GetCause() != "")
	carries(d == orkstackv1.Disposition_DISPOSITION_REFRESH, "the judging spec", len(a.GetJudging().GetSpec()) > 0)
	carries(d == orkstackv1.Disposition_DISPOSITION_PROCEED, "the version", a.GetOrkflowVersion() != "")
	if d == orkstackv1.Disposition_DISPOSITION_REJECT && !strings.HasPrefix(a.GetReason(), "MESSAGE_") {
		t.Fatalf("%s rejects for %q", rpc, a.GetReason())
	}
	if d == orkstackv1.Disposition_DISPOSITION_UNCERTAIN && !uncertainCodes[a.GetRefusalCode()] {
		t.Fatalf("%s is uncertain of %q", rpc, a.GetRefusalCode())
	}
	if (d == orkstackv1.Disposition_DISPOSITION_STOP || d == orkstackv1.Disposition_DISPOSITION_REFUSED) && a.GetReason() == "" {
		t.Fatalf("%s: %v says nothing of why", rpc, a)
	}
	return a
}

func (w *confirmWorld) record(t *testing.T) (artifacts.ConfirmRecord, bool) {
	t.Helper()
	r, found, err := w.instances.Confirmed(context.Background(), artifacts.InstanceAttempt{SourceID: hostSource, Index: 0, Epoch: 7, Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	return r, found
}

func (w *confirmWorld) stored(t *testing.T) *orkstackv1.EventSource {
	t.Helper()
	src, err := w.store.Get(context.Background(), "default", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// TestConfirmInstance_FirstAndLater: a first confirmation puts the attempt on
// record with what the workflow scheduled, and gives the spec its digest
// names and how messages are judged; a later one gives what the caller lacks.
func TestConfirmInstance_FirstAndLater(t *testing.T) {
	w := newConfirmWorld(t)
	first := w.confirm(t, true, 0)
	if first.GetAnswer().GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK || first.GetImagePolicy() == nil {
		t.Fatalf("%+v", first)
	}
	spec, err := eventsources.ReadServiceSpec(first.GetService().GetSpec())
	if err != nil || first.GetService().GetDigest() != w.digest || spec.Source.ID != hostSource {
		t.Fatalf("service %+v, %v", first.GetService(), err)
	}
	judging, src, err := eventsources.ReadJudgingSpec(first.GetJudging().GetSpec())
	if err != nil || judging.Revision != 4 || src.GetOrkflowName() != "deploy" {
		t.Fatalf("judging %+v, %+v, %v", judging, src, err)
	}
	r, found := w.record(t)
	if !found || r.OrkerID != hostOrker || r.ServiceDigest != w.digest || r.AdmissionSave != "save-1" {
		t.Fatalf("the record %+v, %v", r, found)
	}
	later := w.confirm(t, false, 4)
	if later.GetAnswer().GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK || later.GetService() != nil || later.GetJudging() != nil || later.GetImagePolicy() == nil {
		t.Fatalf("%+v", later)
	}
	if again, _ := w.record(t); again != r {
		t.Fatalf("the record changed: %+v, was %+v", again, r)
	}
	if len(w.workflows.synced)+len(w.workflows.reports) != 0 || len(w.workflows.started) != 1 || w.workflows.started[0].Attempt != 1 || w.stored(t).GetPause() != nil {
		t.Fatalf("a confirmation told %+v", w.workflows)
	}
	// What the workflow is not told of does not hold the confirmation.
	w = newConfirmWorld(t)
	w.workflows.err = errors.New("unreachable")
	if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK || len(w.workflows.started) != 1 {
		t.Fatalf("%+v, told %+v", a, w.workflows)
	}
}

// TestConfirmInstance_TheRecord: no attempt is confirmed that could not be put
// on record, and one another Orker holds is not this one's.
func TestConfirmInstance_TheRecord(t *testing.T) {
	w := newConfirmWorld(t)
	w.s.sourceInstances = recordsOf{err: errors.New("down")}
	if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || a.GetCause() != waitOutage {
		t.Fatalf("%+v", a)
	}
	w.s.sourceInstances = recordsOf{r: artifacts.ConfirmRecord{OrkerID: "orker-b", ServiceDigest: w.digest}}
	if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP || a.GetReason() != eventsources.ReasonNotHeld {
		t.Fatalf("another Orker's record: %+v", a)
	}
}

// recordsOf is an instance store that fails, or whose key another engine
// wrote first.
type recordsOf struct {
	r   artifacts.ConfirmRecord
	err error
}

func (r recordsOf) Confirm(context.Context, artifacts.InstanceAttempt, artifacts.ConfirmRecord) (artifacts.ConfirmRecord, error) {
	return r.r, r.err
}

// TestConfirmInstance_ARevokeHoldsWhatHasNotStarted: an attempt on the save a
// revoke names waits before it starts, however often it asks, and is on
// record meanwhile; one that runs goes on, and one on another save is free.
func TestConfirmInstance_ARevokeHoldsWhatHasNotStarted(t *testing.T) {
	w := newConfirmWorld(t)
	w.wf.Memo.Revoke = &held.Revoke{Save: "save-1"}
	for range 2 {
		if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || a.GetReason() != heldRevokePending {
			t.Fatalf("%+v", a)
		}
	}
	if _, found := w.record(t); !found {
		t.Fatal("an attempt that waits is not on record")
	}
	if a := w.confirm(t, false, 4).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("a running instance: %+v", a)
	}
	free := newConfirmWorld(t)
	free.wf.Memo.Revoke = &held.Revoke{Save: "save-0"}
	if a := free.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("another save's revoke: %+v", a)
	}
}

// TestConfirmInstance_WhatIsInForceNow: a spec the namespace no longer permits
// pauses its source with the refusal's code, once, and stops it; what could
// not be judged is a wait, and pauses nothing.
func TestConfirmInstance_WhatIsInForceNow(t *testing.T) {
	permitted := func(edit func(*orkstackv1.Namespace)) func(*confirmWorld) {
		return func(w *confirmWorld) {
			ns := &orkstackv1.Namespace{Name: "default"}
			edit(ns)
			w.s.pools = &engine.PoolResolver{Namespaces: namespacesOf{ns}}
		}
	}
	for name, c := range map[string]struct {
		edit   func(*confirmWorld)
		want   orkstackv1.Disposition
		reason string
		code   string
	}{
		"its secret's source": {permitted(func(ns *orkstackv1.Namespace) { ns.AllowedSecrets = []string{"env:OTHER_*"} }),
			orkstackv1.Disposition_DISPOSITION_STOP, eventsources.ReasonPaused, "SECRET_NOT_ALLOWED"},
		"its pool": {permitted(func(ns *orkstackv1.Namespace) { ns.AllowedPools, ns.DefaultPool = []string{"gpu"}, "gpu" }),
			orkstackv1.Disposition_DISPOSITION_STOP, eventsources.ReasonPaused, "POOL_NOT_ALLOWED"},
		"the spec is gone": {func(w *confirmWorld) { w.s.eventSources = specsOf{Store: w.store} }, orkstackv1.Disposition_DISPOSITION_WAIT, "unread=spec", ""},
		"the spec is another's": {func(w *confirmWorld) {
			w.s.eventSources = specsOf{Store: w.store, body: []byte(`{"format":9}`), found: true}
		},
			orkstackv1.Disposition_DISPOSITION_WAIT, "unread=spec", ""},
		"the store is down": {func(w *confirmWorld) { w.s.eventSources = specsOf{Store: w.store, err: errors.New("503")} }, orkstackv1.Disposition_DISPOSITION_WAIT, "", ""},
		"the store refuses": {func(w *confirmWorld) { w.s.eventSources = specsOf{Store: w.store, err: s3Status(403)} }, orkstackv1.Disposition_DISPOSITION_WAIT, "unread=policy", ""},
	} {
		t.Run(name, func(t *testing.T) {
			w := newConfirmWorld(t)
			c.edit(w)
			a := w.confirm(t, true, 0).GetAnswer()
			if a.GetDisposition() != c.want || a.GetReason() != c.reason || a.GetRefusalCode() != c.code {
				t.Fatalf("%+v; want %v %s %s", a, c.want, c.reason, c.code)
			}
			pause := w.stored(t).GetPause()
			if (pause != nil) != (c.code != "") {
				t.Fatalf("the pause is %+v", pause)
			}
			if pause == nil {
				return
			}
			if pause.GetCause() != orkstackv1.PauseCause_PAUSE_CAUSE_ENGINE || pause.GetCode() != eventsources.PauseInstanceFailed || pause.GetRefusalCode() != c.code ||
				pause.GetReason() != eventsources.PauseSentence(eventsources.PauseInstanceFailed) || pause.GetPrincipal() != "" || pause.GetRequestedByOrker() != "" {
				t.Fatalf("%+v", pause)
			}
			if len(w.workflows.synced) != 1 {
				t.Fatalf("its workflow was told %v", w.workflows.synced)
			}
			since := pause.GetSince().AsTime()
			if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP || a.GetReason() != eventsources.ReasonPaused {
				t.Fatalf("once paused: %+v", a)
			}
			if again := w.stored(t).GetPause(); !again.GetSince().AsTime().Equal(since) || len(w.workflows.synced) != 1 {
				t.Fatalf("paused again: %+v", again)
			}
		})
	}
}

// namespacesOf is a registry of one namespace.
type namespacesOf struct{ ns *orkstackv1.Namespace }

func (n namespacesOf) Get(context.Context, string) (*orkstackv1.Namespace, error) { return n.ns, nil }

// specsOf is a source store whose specs answer as a test says: none, another
// release's, or a store that fails.
type specsOf struct {
	eventsources.Store
	body  []byte
	found bool
	err   error
}

func (s specsOf) ReadSpec(context.Context, string, string) ([]byte, bool, error) {
	return s.body, s.found, s.err
}

// TestConfirmInstance_WhatWasJudgedIsKeptAMinute: what admitted a spec admits
// it again for a minute without a read; nothing kept refuses, and past the
// minute it is judged anew.
func TestConfirmInstance_WhatWasJudgedIsKeptAMinute(t *testing.T) {
	w := newConfirmWorld(t)
	now := time.Now()
	w.s.sourcePolicies.now = func() time.Time { return now }
	if a := w.confirm(t, true, 0).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("%+v", a)
	}
	w.s.eventSources = specsOf{Store: w.store, err: errors.New("503")}
	now = now.Add(59 * time.Second)
	if a := w.confirm(t, false, 4).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("within the minute: %+v", a)
	}
	now = now.Add(2 * time.Second)
	if a := w.confirm(t, false, 4).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT {
		t.Fatalf("past it: %+v", a)
	}
	w.s.eventSources = w.store
	if a := w.confirm(t, false, 4).GetAnswer(); a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("a refusal was kept: %+v", a)
	}
}

// TestPauseSource: a pause that stands is kept as it is, the document of
// another source under the name is never paused, and one that changed
// meanwhile is not paused for what it was.
func TestPauseSource(t *testing.T) {
	ctx := context.Background()
	w := newConfirmWorld(t)
	stale := w.stored(t)
	call := &heldCall{src: stale, ref: held.Ref{Namespace: "default", Name: "orders", SourceID: hostSource}}
	changed := w.stored(t)
	changed.Description = "changed meanwhile"
	if _, err := w.store.Replace(ctx, changed, changed.GetEtag()); err != nil {
		t.Fatal(err)
	}
	_, now, err := w.s.pauseSource(ctx, call, eventsources.PauseAllRejected, "", hostOrker)
	if err != nil || now.GetDescription() != "changed meanwhile" || w.stored(t).GetPause() != nil || len(w.workflows.synced) != 0 {
		t.Fatalf("%v, %v; stored %v, told %v", now, err, w.stored(t), w.workflows.synced)
	}
	call.src = now
	reason, now, err := w.s.pauseSource(ctx, call, eventsources.PauseAllRejected, "", hostOrker)
	if now != nil || err != nil || reason != eventsources.ReasonPaused {
		t.Fatalf("%s, %v, %v", reason, now, err)
	}
	if got := w.stored(t); got.GetDescription() != "changed meanwhile" || got.GetPause().GetCode() != eventsources.PauseAllRejected || got.GetPause().GetRequestedByOrker() != hostOrker {
		t.Fatalf("%+v", got)
	}
	// From a document that is behind the pause, and from the paused one.
	for _, src := range []*orkstackv1.EventSource{stale, w.stored(t)} {
		call.src = src
		if reason, now, err = w.s.pauseSource(ctx, call, eventsources.PauseRefused, "X", ""); now != nil || err != nil || reason != eventsources.ReasonPaused {
			t.Fatalf("%s, %v, %v", reason, now, err)
		}
		if again := w.stored(t).GetPause(); again.GetCode() != eventsources.PauseAllRejected {
			t.Fatalf("a pause that stood was replaced: %+v", again)
		}
	}
	if _, _, err := w.s.pauseSource(ctx, call, "NOT_A_CODE", "", ""); err == nil {
		t.Fatal("a source was paused with no code of OrkStack's")
	}
	other := newConfirmWorld(t)
	call = &heldCall{src: other.stored(t), ref: held.Ref{Namespace: "default", Name: "orders", SourceID: "evs-fedcba9876543210"}}
	if reason, now, err := other.s.pauseSource(ctx, call, eventsources.PauseAllRejected, "", hostOrker); now != nil || err != nil || reason != eventsources.ReasonNotHeld || other.stored(t).GetPause() != nil {
		t.Fatalf("another source was paused: %s, %v, %v, %+v", reason, now, err, other.stored(t).GetPause())
	}

	// One that is being deleted meanwhile, and one that is gone.
	for want, change := range map[string]func(*confirmWorld){
		eventsources.ReasonDeleting: func(w *confirmWorld) {
			src := w.stored(t)
			src.DeletingSince = timestamppb.Now()
			if _, err := w.store.Replace(ctx, src, src.GetEtag()); err != nil {
				t.Fatal(err)
			}
		},
		eventsources.ReasonNotHeld: func(w *confirmWorld) {
			src := w.stored(t)
			if err := w.store.Remove(ctx, "default", "orders", src.GetEtag()); err != nil {
				t.Fatal(err)
			}
		},
	} {
		w := newConfirmWorld(t)
		call := &heldCall{src: w.stored(t), ref: held.Ref{Namespace: "default", Name: "orders", SourceID: hostSource}}
		change(w)
		if reason, now, err := w.s.pauseSource(ctx, call, eventsources.PauseAllRejected, "", hostOrker); reason != want || now != nil || err != nil {
			t.Errorf("%s: %s, %v, %v", want, reason, now, err)
		}
	}
}

// TestReportInstance: a report reaches the source's workflow, with a reason
// only where it is invalid; one that did not is asked for again.
func TestReportInstance(t *testing.T) {
	w := newConfirmWorld(t)
	report := func(what orkstackv1.InstanceReport, reason string) (*orkstackv1.DoorAnswer, error) {
		resp, err := w.s.ReportInstance(asHost(hostOrker), connect.NewRequest(&orkstackv1.ReportInstanceRequest{Instance: w.ref(4), What: what, Reason: reason}))
		if err != nil {
			return nil, err
		}
		return answered(t, "ReportInstance", resp.Msg.GetAnswer()), nil
	}
	for _, c := range []struct {
		what   orkstackv1.InstanceReport
		reason string
	}{
		{orkstackv1.InstanceReport_INSTANCE_REPORT_READY, "ignored"},
		{orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID, "CONFIG_INVALID\nx"},
		{orkstackv1.InstanceReport_INSTANCE_REPORT_HEALTHY, ""},
	} {
		if a, err := report(c.what, c.reason); err != nil || a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
			t.Fatalf("%v: %+v, %v", c.what, a, err)
		}
	}
	if len(w.workflows.reports) != 3 || !strings.HasSuffix(w.workflows.reports[0], "READY ") || strings.Contains(w.workflows.reports[1], "\n") || !strings.Contains(w.workflows.reports[1], "CONFIG_INVALID") {
		t.Fatalf("%q", w.workflows.reports)
	}
	for _, what := range []orkstackv1.InstanceReport{0, 99} {
		if _, err := report(what, ""); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("a report of kind %d: %v", what, err)
		}
	}
	w.workflows.err = errors.New("unreachable")
	if a, err := report(orkstackv1.InstanceReport_INSTANCE_REPORT_READY, ""); err != nil || a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT {
		t.Fatalf("a report that did not arrive: %+v, %v", a, err)
	}
	w.s.sourceWorkflows = nil
	if a, err := report(orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID, "x"); err != nil || a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT {
		t.Fatalf("a report there is nobody to take: %+v, %v", a, err)
	}
	w.s.sourceWorkflows = w.workflows
	// Reported while its cancellation is asked, it is still held.
	w.workflows.err = nil
	w.wf.Pending[0].State = held.CancelRequested
	if a, err := report(orkstackv1.InstanceReport_INSTANCE_REPORT_READY, ""); err != nil || a.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("a cancelled instance's report: %+v, %v", a, err)
	}
}

// A request over its bound is refused before anything is read.
func TestDoorRequest_IsBounded(t *testing.T) {
	w := newConfirmWorld(t)
	big := w.ref(4)
	big.OrkerId = strings.Repeat("x", maxDoorRequest)
	if _, err := w.s.ConfirmInstance(asHost(hostOrker), connect.NewRequest(&orkstackv1.ConfirmInstanceRequest{Instance: big})); connect.CodeOf(err) != connect.CodeInvalidArgument || w.reads != 0 {
		t.Fatalf("%v after %d reads", err, w.reads)
	}
	if _, err := w.s.ReportInstance(asHost(hostOrker), connect.NewRequest(&orkstackv1.ReportInstanceRequest{Instance: big, What: 1})); connect.CodeOf(err) != connect.CodeInvalidArgument || w.reads != 0 {
		t.Fatalf("%v after %d reads", err, w.reads)
	}
}

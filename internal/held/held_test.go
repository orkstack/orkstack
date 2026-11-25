package held

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	srcID  = "evs-0123456789abcdef"
	digest = "d-run"
	save   = "save-1"
	orker  = "orker-a"
	pool   = "main"
)

// world is what the check reads, as the raw clients answer it, counting each
// read. The test moves its clock.
type world struct {
	mu       sync.Mutex
	now      time.Time
	wf       Workflow
	wfErr    error
	tomb     bool
	tombErr  error
	doc      Document
	docErr   error
	rec      artifacts.ConfirmRecord
	recFound bool
	recErr   error
	row      OrkerRow
	rowErr   error
	perms    map[authz.Permission]string // the pool each is granted at
	docPerms map[authz.Permission]string // as the replica's held document says
	fresh    bool
	permErr  error
	reads    map[string]int
	onRead   func(kind string)
}

func newWorld() *world {
	w := &world{now: time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC), fresh: true, reads: map[string]int{},
		wf: Workflow{Open: true, Pending: []Pending{{ActivityID: ActivityID(3, 12), State: Started, Attempt: 2, LastWorker: orker}},
			Memo: Memo{Slots: map[int32]Slot{3: {Current: SlotEpoch{Epoch: 12, Digest: digest, AdmissionSave: save}}}}},
		doc:   Document{Found: true, ID: srcID, Pool: pool},
		rec:   artifacts.ConfirmRecord{OrkerID: orker, ServiceDigest: digest, AdmissionSave: save},
		row:   OrkerRow{Pools: []string{pool}, Fit: true, MaxInstances: 4},
		perms: map[authz.Permission]string{authz.PermEventSourceHost: pool, authz.PermIdentityTokenMint: pool}}
	w.recFound = true
	w.docPerms = w.perms
	return w
}

func (w *world) read(kind string) {
	w.mu.Lock()
	w.reads[kind]++
	f := w.onRead
	w.mu.Unlock()
	if f != nil {
		f(kind)
	}
}

func (w *world) count(kind string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reads[kind]
}

func (w *world) set(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

func (w *world) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *world) advance(d time.Duration) { w.set(func() { w.now = w.now.Add(d) }) }

func at[V any](w *world, v *V) V {
	w.mu.Lock()
	defer w.mu.Unlock()
	return *v
}

func (w *world) reader() *Reader {
	return NewReader(Clients{
		Describe: func(context.Context, string) (Workflow, error) {
			// As the cluster says when it is asked, however late it answers.
			wf, err := at(w, &w.wf), at(w, &w.wfErr)
			w.read("describe")
			return wf, err
		},
		Tombstoned: func(context.Context, string) (bool, error) {
			w.read("tombstone")
			return at(w, &w.tomb), at(w, &w.tombErr)
		},
		Document: func(context.Context, string, string) (Document, error) {
			w.read("document")
			return at(w, &w.doc), at(w, &w.docErr)
		},
		Confirmed: func(context.Context, artifacts.InstanceAttempt) (artifacts.ConfirmRecord, bool, error) {
			w.read("confirm")
			return at(w, &w.rec), at(w, &w.recFound), at(w, &w.recErr)
		},
		Orker: func(context.Context, string) (OrkerRow, error) {
			w.read("orker")
			return at(w, &w.row), at(w, &w.rowErr)
		},
		Access: access{w},
	}, Options{InventoryAge: 10 * time.Minute, Now: w.clock})
}

type access struct{ w *world }

func (a access) Held(_ *authz.Subject, p authz.Permission, pool string) (granted, fresh bool) {
	a.w.mu.Lock()
	defer a.w.mu.Unlock()
	return a.w.docPerms[p] == pool, a.w.fresh
}

func (a access) Read(context.Context) (Decider, error) {
	a.w.read("access")
	if err := at(a.w, &a.w.permErr); err != nil {
		return nil, err
	}
	perms := at(a.w, &a.w.perms)
	return func(_ *authz.Subject, p authz.Permission, pool string) bool { return perms[p] == pool }, nil
}

func ref() Ref {
	return Ref{Namespace: "payments", Name: "orders", SourceID: srcID, Index: 3, Epoch: 12, Attempt: 2, ServiceDigest: digest}
}

func caller() Caller { return Caller{OrkerID: orker, Subject: &authz.Subject{}} }

var purposes = []Purpose{PurposeConfirm, PurposeReport, PurposeOffer, PurposeStart, PurposeProbe, PurposeRequestPause, PurposeMint}

func ask(w *world, r *Reader, p Purpose) Result {
	return Instance(context.Background(), r, p, ref(), caller(), w.clock())
}

func TestActivityID_OneSpelling(t *testing.T) {
	if got := ActivityID(3, 12); got != "evs/3/12" {
		t.Fatalf("%q", got)
	}
	if got := ActivityID(0, 1234567890123); got != "evs/0/1234567890123" {
		t.Fatalf("%q", got)
	}
}

// TestInstance_BothPurposes is the one function's table: every check, for
// each of the door's six calls and for the mint.
func TestInstance_BothPurposes(t *testing.T) {
	boom := errors.New("no answer")
	pending := func(edit func(*Pending)) func(*world) {
		return func(w *world) {
			p := w.wf.Pending[0]
			edit(&p)
			w.wf.Pending = []Pending{p}
		}
	}
	cancelHeld := func(p Purpose) bool { return p == PurposeStart || p == PurposeReport }
	tests := []struct {
		name string
		set  func(*world)
		want func(Purpose) Result
	}{
		{"it holds", func(*world) {}, held},
		{"the workflow is not open", func(w *world) { w.wf.Open = false }, notHeld},
		{"no such activity", func(w *world) { w.wf.Pending = nil }, notHeld},
		{"another epoch's activity", pending(func(p *Pending) { p.ActivityID = ActivityID(3, 13) }), notHeld},
		{"it is not started", pending(func(p *Pending) { p.State = Scheduled }), notHeld},
		{"another attempt", pending(func(p *Pending) { p.Attempt = 3 }), notHeld},
		{"another Orker's", pending(func(p *Pending) { p.LastWorker = "orker-b" }), notHeld},
		{"a cancellation is asked", pending(func(p *Pending) { p.State = CancelRequested }), func(p Purpose) Result {
			if cancelHeld(p) {
				return held(p)
			}
			return notHeld(p)
		}},
		{"another spec's digest than its record", func(w *world) { w.rec.ServiceDigest = "d-other" }, notHeld},
		{"never confirmed, the epoch's digest", func(w *world) { w.recFound = false }, func(p Purpose) Result {
			if p == PurposeMint {
				return Result{Kind: Unanswered, Read: ReadConfirm}
			}
			return held(p)
		}},
		{"never confirmed, a roll's target's digest", func(w *world) {
			w.recFound = false
			s := w.wf.Memo.Slots[3]
			w.wf.Memo.Slots = map[int32]Slot{3: {Current: SlotEpoch{Epoch: 12, Digest: "d-old"}, Replacement: &SlotEpoch{Epoch: 13, Digest: s.Current.Digest}}}
		}, func(p Purpose) Result {
			if p == PurposeMint {
				return Result{Kind: Unanswered, Read: ReadConfirm}
			}
			return notHeld(p)
		}},
		{"the replacement of its slot", func(w *world) {
			s := w.wf.Memo.Slots[3].Current
			w.wf.Memo.Slots = map[int32]Slot{3: {Current: SlotEpoch{Epoch: 11, Digest: "d-old"}, Replacement: &s}}
		}, held},
		{"an epoch the memo has no slot for", func(w *world) { w.recFound = false; w.wf.Memo.Slots = nil }, func(p Purpose) Result {
			if p == PurposeMint {
				return Result{Kind: Unanswered, Read: ReadConfirm}
			}
			return notHeld(p)
		}},
		{"a revoke of its save", func(w *world) { w.wf.Memo.Revoke = &Revoke{Save: save} }, func(p Purpose) Result {
			return Result{Kind: Held, RevokePending: p == PurposeConfirm || p == PurposeMint}
		}},
		{"a revoke of another save", func(w *world) { w.wf.Memo.Revoke = &Revoke{Save: "save-2"} }, held},
		{"a tombstone, whatever the name holds", func(w *world) { w.tomb = true; w.doc.ID = "evs-other" }, kind(Tombstoned)},
		{"a tombstone and a paused document", func(w *world) { w.tomb = true; w.doc.Paused = true }, kind(Tombstoned)},
		{"no document", func(w *world) { w.doc = Document{} }, kind(NoDocument)},
		{"another source's document", func(w *world) { w.doc.ID = "evs-other" }, kind(OtherID)},
		{"a paused document", func(w *world) { w.doc.Paused = true }, func(Purpose) Result { return Result{Kind: PausedOrMarked, Which: "paused"} }},
		{"a marked document", func(w *world) { w.doc.Deleting = true }, func(Purpose) Result { return Result{Kind: PausedOrMarked, Which: "marked"} }},
		{"the host permission is at another pool", func(w *world) {
			w.perms = map[authz.Permission]string{authz.PermEventSourceHost: "other", authz.PermIdentityTokenMint: pool}
			w.docPerms = w.perms
		}, func(Purpose) Result {
			return Result{Kind: NotPermitted, Permission: authz.PermEventSourceHost, Pool: pool}
		}},
		{"the mint permission is at another pool", func(w *world) {
			w.perms = map[authz.Permission]string{authz.PermEventSourceHost: pool, authz.PermIdentityTokenMint: "other"}
			w.docPerms = w.perms
		}, func(p Purpose) Result {
			if p == PurposeMint {
				return Result{Kind: NotPermitted, Permission: authz.PermIdentityTokenMint, Pool: pool}
			}
			return held(p)
		}},
		{"a binding just revoked", func(w *world) { w.perms, w.docPerms = nil, nil }, func(Purpose) Result {
			return Result{Kind: NotPermitted, Permission: authz.PermEventSourceHost, Pool: pool}
		}},
		{"a binding just added", func(w *world) { w.docPerms = nil }, held},
		{"an Orker that is not fit", func(w *world) { w.row.Fit = false }, notHeld},
		{"an Orker of another pool", func(w *world) { w.row.Pools = []string{"other"} }, notHeld},
		{"an Orker that hosts none", func(w *world) { w.row.MaxInstances = 0 }, notHeld},
		{"the describe does not answer", func(w *world) { w.wfErr = boom }, unanswered(ReadDescribe)},
		{"the confirm record does not answer", func(w *world) { w.recErr = boom }, unanswered(ReadConfirm)},
		{"the tombstone does not answer", func(w *world) { w.tombErr = boom }, unanswered(ReadTombstone)},
		{"the document does not answer", func(w *world) { w.docErr = boom }, unanswered(ReadDocument)},
		{"a document this engine cannot decode", func(w *world) { w.docErr = fmt.Errorf("%w: x", ErrUndecodable) }, func(Purpose) Result {
			return Result{Kind: Unanswered, Read: ReadDocument, Undecodable: true}
		}},
		{"the inventory does not answer", func(w *world) { w.rowErr = boom }, unanswered(ReadInventory)},
		{"the permission does not answer", func(w *world) { w.docPerms = nil; w.permErr = boom }, unanswered(ReadPermission)},
		{"a stale access document that cannot be revalidated", func(w *world) { w.fresh = false; w.permErr = boom }, unanswered(ReadPermission)},
	}
	for _, tt := range tests {
		for _, p := range purposes {
			t.Run(tt.name+"/"+p.String(), func(t *testing.T) {
				w := newWorld()
				tt.set(w)
				got, want := ask(w, w.reader(), p), tt.want(p)
				if got.Kind != want.Kind || got.RevokePending != want.RevokePending || got.Permission != want.Permission ||
					got.Pool != want.Pool || got.Read != want.Read || got.Undecodable != want.Undecodable ||
					(want.Kind == PausedOrMarked && got.Which != want.Which) {
					t.Fatalf("%+v; want %+v", got, want)
				}
				if got.Kind == NotHeld && got.Which == "" {
					t.Fatal("a refusal that does not say which, for the log")
				}
				if got.Kind == Held && (got.Document.Pool != pool || got.Epoch.Digest != digest || got.Epoch.AdmissionSave != save) {
					t.Fatalf("what it holds: %+v", got)
				}
			})
		}
	}
}

func held(Purpose) Result    { return Result{Kind: Held} }
func notHeld(Purpose) Result { return Result{Kind: NotHeld} }
func kind(k Kind) func(Purpose) Result {
	return func(Purpose) Result { return Result{Kind: k} }
}
func unanswered(r Read) func(Purpose) Result {
	return func(Purpose) Result { return Result{Kind: Unanswered, Read: r} }
}

func TestInstance_BadRequestIsNotHeld(t *testing.T) {
	w := newWorld()
	r := w.reader()
	for name, edit := range map[string]func(*Ref, *Caller){
		"source":   func(f *Ref, _ *Caller) { f.SourceID = "evs-../x" },
		"index":    func(f *Ref, _ *Caller) { f.Index = -1 },
		"epoch":    func(f *Ref, _ *Caller) { f.Epoch = -1 },
		"attempt":  func(f *Ref, _ *Caller) { f.Attempt = 0 },
		"no Orker": func(_ *Ref, c *Caller) { c.OrkerID = "" },
		"digest":   func(f *Ref, _ *Caller) { f.ServiceDigest = "" },
		"indices":  func(f *Ref, _ *Caller) { f.Index = 100000 },
		"epochs":   func(f *Ref, _ *Caller) { f.Epoch = 1000000000000 },
		"attempts": func(f *Ref, _ *Caller) { f.Attempt = 10000 },
		"name":     func(f *Ref, _ *Caller) { f.Name = "a/b" },
		"empty":    func(f *Ref, _ *Caller) { f.Name = "" },
		"space":    func(f *Ref, _ *Caller) { f.Namespace = "Pay ments" },
	} {
		f, c := ref(), caller()
		edit(&f, &c)
		if got := Instance(context.Background(), r, PurposeOffer, f, c, w.clock()); got.Kind != NotHeld {
			t.Errorf("%s: %+v", name, got)
		}
	}
	if len(w.reads) != 0 {
		t.Errorf("a malformed request cost reads: %v", w.reads)
	}
	if got := Instance(context.Background(), r, Purpose(99), ref(), caller(), w.clock()); got.Kind != NotHeld {
		t.Errorf("a purpose there is none of: %+v", got)
	}
}

// Nothing is read for an attempt the describe does not show the caller
// holding: an Orker that makes attempts up costs its capped describes and no
// other read.
func TestInstance_MadeUpAttemptsCostNoOtherRead(t *testing.T) {
	w := newWorld()
	r := w.reader()
	for i := range 200 {
		f := ref()
		f.Index, f.Epoch, f.Attempt = int32(i%50), int64(1000+i), int32(1+i%9)
		w.advance(time.Microsecond)
		if got := Instance(context.Background(), r, PurposeMint, f, caller(), w.clock()); got.Kind != NotHeld && got.Kind != Unanswered {
			t.Fatalf("%+v", got)
		}
	}
	for _, kind := range []string{"confirm", "tombstone", "document", "access", "orker"} {
		if n := w.count(kind); n != 0 {
			t.Errorf("%d reads of the %s for attempts nobody holds", n, kind)
		}
	}
	if n := w.count("describe"); n > describesPerSource {
		t.Errorf("%d describes", n)
	}
}

// A kept describe is renewed past the caps only for a caller it shows
// holding the attempt asked about: keys an Orker had read cost it no more
// than its caps when it asks again.
func TestReader_RenewalIsTheHoldersOnly(t *testing.T) {
	w := newWorld()
	w.wf.Pending = nil
	r := w.reader()
	for second := range 20 {
		for i := range 40 {
			f := ref()
			f.SourceID = fmt.Sprintf("evs-%016x", i)
			w.advance(time.Microsecond)
			Instance(context.Background(), r, PurposeOffer, f, caller(), w.clock())
		}
		if n, most := w.count("describe"), describesPerOrker*(second+2); n > most {
			t.Fatalf("after %d s one Orker cost %d describes of sources it does not hold; at most %d", second, n, most)
		}
		w.advance(time.Second)
	}
	// The control: a holder's busy sources are renewed once in the kept one's
	// life each, however many they are.
	busy := newWorld()
	br := busy.reader()
	for range 3 {
		for i := range 4 {
			f := ref()
			f.SourceID = fmt.Sprintf("evs-%016x", i)
			busy.advance(time.Microsecond)
			if got := Instance(context.Background(), br, PurposeOffer, f, caller(), busy.clock()); got.Kind != OtherID {
				t.Fatalf("source %d: %+v", i, got)
			}
		}
		busy.advance(describeAge + 100*time.Millisecond)
	}
	if n := busy.count("describe"); n != 12 {
		t.Fatalf("%d describes of four busy sources over three lives; want 12", n)
	}
}

// An Orker that held a source once is let through to renew it once: the fresh
// read shows it holds nothing, and from then on the caps bound it.
func TestReader_AFormerHolderRenewsOnce(t *testing.T) {
	w := newWorld()
	r := w.reader()
	if got := ask(w, r, PurposeOffer); got.Kind != Held {
		t.Fatalf("%+v", got)
	}
	w.set(func() { w.wf.Pending = nil })
	before := w.count("describe")
	w.advance(describeAge + 100*time.Millisecond)
	for range 100 {
		w.advance(time.Microsecond)
		if got := ask(w, r, PurposeOffer); got.Kind == Held {
			t.Fatalf("a former holder: %+v", got)
		}
	}
	if n := w.count("describe") - before; n > describesPerSource+1 {
		t.Fatalf("%d describes for a former holder; at most %d", n, describesPerSource+1)
	}
}

// A call that does not say when it arrived, or says later than now, arrived
// now: it is answered, and never refused from a read made before it.
func TestReader_AnInstantThatCannotBe(t *testing.T) {
	for name, arrived := range map[string]func(*world) time.Time{
		"none":  func(*world) time.Time { return time.Time{} },
		"later": func(w *world) time.Time { return w.clock().Add(time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld()
			pending := w.wf.Pending
			w.wf.Pending = nil
			r := w.reader()
			if got := ask(w, r, PurposeOffer); got.Kind != NotHeld {
				t.Fatalf("%+v", got)
			}
			w.set(func() { w.wf.Pending = pending })
			w.advance(time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if got := Instance(ctx, r, PurposeOffer, ref(), caller(), arrived(w)); got.Kind != Held {
				t.Fatalf("%+v", got)
			}
			if n := w.count("describe"); n != 2 {
				t.Fatalf("%d describes; want 2", n)
			}
		})
	}
}

// A clock that steps back makes nothing kept look younger than it is.
func TestReader_AClockThatStepsBack(t *testing.T) {
	w := newWorld()
	r := w.reader()
	if got := ask(w, r, PurposeOffer); got.Kind != Held {
		t.Fatalf("%+v", got)
	}
	w.advance(-time.Hour)
	w.set(func() { w.tomb = true })
	if got := ask(w, r, PurposeOffer); got.Kind != Tombstoned {
		t.Fatalf("a none kept from later than now admitted: %+v", got)
	}
}

// Documents and the access document are never kept: nothing is answered from
// them later, and neither is bounded by what the reader counts.
func TestReader_KeepsNoDocument(t *testing.T) {
	w := newWorld()
	w.docPerms = nil
	r := w.reader()
	if got := ask(w, r, PurposeOffer); got.Kind != Held {
		t.Fatalf("%+v", got)
	}
	for key := range r.kept {
		if strings.HasPrefix(key, "document/") || key == "access" {
			t.Errorf("%s is kept", key)
		}
	}
}

// A read that panics, or answers with nothing, is a read that gave no
// answer: the caller is told and nobody waits for ever.
func TestReader_APanicIsNoAnswer(t *testing.T) {
	for _, kind := range []string{"describe", "confirm", "tombstone", "document", "orker", "access"} {
		w := newWorld()
		w.docPerms = nil
		r := w.reader()
		var once sync.Once
		w.onRead = func(k string) {
			if k == kind {
				once.Do(func() { panic("boom") })
			}
		}
		if got := ask(w, r, PurposeOffer); got.Kind != Unanswered || got.Err == nil {
			t.Errorf("%s: %+v", kind, got)
		}
		w.advance(2 * time.Second)
		if got := ask(w, r, PurposeOffer); got.Kind != Held {
			t.Errorf("%s, the next call: %+v", kind, got)
		}
	}
	w := newWorld()
	w.docPerms = nil
	r := w.reader()
	r.clients.Access = nilAccess{}
	if got := ask(w, r, PurposeOffer); got.Kind != Unanswered || got.Read != ReadPermission {
		t.Errorf("a nil decider: %+v", got)
	}
}

type nilAccess struct{}

func (nilAccess) Held(*authz.Subject, authz.Permission, string) (granted, fresh bool) {
	return false, true
}
func (nilAccess) Read(context.Context) (Decider, error) { return nil, nil }

// A memo a kept describe shows may not refuse an attempt that has not
// confirmed: its epoch may have been scheduled since.
func TestInstance_AKeptMemoNeverRefusesAFirstConfirmation(t *testing.T) {
	w := newWorld()
	w.recFound = false
	s := w.wf.Memo.Slots
	w.wf.Memo.Slots = nil
	r := w.reader()
	if got := ask(w, r, PurposeConfirm); got.Kind != NotHeld {
		t.Fatalf("%+v", got)
	}
	w.set(func() { w.wf.Memo.Slots = s })
	w.advance(100 * time.Millisecond)
	if got := ask(w, r, PurposeConfirm); got.Kind != Held || w.count("describe") != 2 {
		t.Fatalf("%+v, %d describes", got, w.count("describe"))
	}
}

// A kept describe confirms and never refuses, for the door's calls; one that
// would refuse is made again after the request arrived.
func TestReader_KeptDescribeConfirmsAndNeverRefuses(t *testing.T) {
	w := newWorld()
	r := w.reader()
	for p := PurposeConfirm; p < PurposeMint; p++ {
		w.advance(time.Millisecond)
		if got := ask(w, r, p); got.Kind != Held {
			t.Fatalf("purpose %d: %+v", p, got)
		}
	}
	if n := w.count("describe"); n != 1 {
		t.Fatalf("%d describes for the door's six calls within the kept one's life; want 1", n)
	}
	w.advance(describeAge - time.Second)
	w.set(func() {
		w.wf.Pending = []Pending{{ActivityID: ActivityID(3, 12), State: Started, Attempt: 3, LastWorker: "orker-b"}}
	})
	if got := ask(w, r, PurposeOffer); got.Kind != Held || w.count("describe") != 1 {
		t.Fatalf("within its life the kept one confirms: %+v, %d describes", got, w.count("describe"))
	}
	f := ref()
	f.Attempt = 3
	c := caller()
	c.OrkerID = "orker-b"
	w.set(func() { w.recFound = false })
	if got := Instance(context.Background(), r, PurposeConfirm, f, c, w.clock()); got.Kind != Held || w.count("describe") != 2 {
		t.Fatalf("a new attempt the kept one cannot know: %+v, %d describes", got, w.count("describe"))
	}
	w.advance(describeAge + time.Second)
	if got := ask(w, r, PurposeOffer); got.Kind != NotHeld || w.count("describe") != 3 {
		t.Fatalf("past its life: %+v, %d describes", got, w.count("describe"))
	}
}

// The mint gives a credential, so it never rests on a kept describe, whoever
// made it and however young it is: an instance that lost its slot a moment
// ago mints nothing, and one that holds it costs a describe a call.
func TestReader_TheMintNeverUsesAKeptDescribe(t *testing.T) {
	for p := PurposeConfirm; p <= PurposeMint; p++ {
		t.Run(fmt.Sprintf("kept by purpose %d", p), func(t *testing.T) {
			w := newWorld()
			r := w.reader()
			if got := ask(w, r, p); got.Kind != Held {
				t.Fatalf("%+v", got)
			}
			w.advance(time.Nanosecond)
			if got := ask(w, r, PurposeMint); got.Kind != Held || w.count("describe") != 2 {
				t.Fatalf("a holder's mint: %+v, %d describes; want 2", got, w.count("describe"))
			}
			w.set(func() { w.wf.Pending = nil })
			w.advance(time.Nanosecond)
			if got := ask(w, r, PurposeMint); got.Kind != NotHeld || w.count("describe") != 3 {
				t.Fatalf("a mint for a slot just lost: %+v, %d describes; want 3", got, w.count("describe"))
			}
		})
	}
	// Past the caps it waits: a kept one that would confirm is still not used.
	w := newWorld()
	r := w.reader()
	kinds := map[Kind]int{}
	for range 40 {
		w.advance(time.Microsecond)
		got := ask(w, r, PurposeMint)
		kinds[got.Kind]++
		if got.Kind == Unanswered && got.Read != ReadDescribe {
			t.Fatalf("%+v", got)
		}
	}
	if kinds[Held] != w.count("describe") || kinds[Held]+kinds[Unanswered] != 40 || kinds[Held] > describesPerSource+1 {
		t.Fatalf("%v for %d describes", kinds, w.count("describe"))
	}
}

// An Orker that does not hold the source cannot keep the one that does from
// minting: a holder's reads are its own, one a second, and no more than that.
func TestReader_AFloodKeepsNoHolderFromMinting(t *testing.T) {
	w := newWorld()
	r := w.reader()
	if got := ask(w, r, PurposeOffer); got.Kind != Held {
		t.Fatalf("%+v", got)
	}
	other := caller()
	other.OrkerID = "orker-flood"
	const seconds = 10
	for second := range seconds {
		for range 100 {
			w.advance(10 * time.Millisecond)
			if got := Instance(context.Background(), r, PurposeOffer, ref(), other, w.clock()); got.Kind == Held {
				t.Fatalf("the flood holds it: %+v", got)
			}
		}
		w.advance(time.Millisecond)
		if got := ask(w, r, PurposeMint); got.Kind != Held {
			t.Fatalf("second %d: the holder's mint: %+v", second, got)
		}
	}
	if n, most := w.count("describe"), (describesPerSource+freshPerHolder)*(seconds+2); n > most {
		t.Fatalf("%d describes; at most %d", n, most)
	}
	// The control: one that held it once and mints on costs one read more than
	// the caps allow, and gets nothing.
	w.set(func() { w.wf.Pending = nil })
	w.advance(2 * time.Second)
	before := w.count("describe")
	for range 100 {
		w.advance(time.Microsecond)
		if got := ask(w, r, PurposeMint); got.Kind == Held {
			t.Fatalf("a former holder's mint: %+v", got)
		}
	}
	if n := w.count("describe") - before; n > describesPerSource+1 {
		t.Fatalf("%d describes for a former holder's mints; at most %d", n, describesPerSource+1)
	}
}

// An attempt's first confirmation sees a revoke as it stands: it makes the
// record a credential rests on. A confirmed one's later calls may be answered
// from a kept describe.
func TestReader_AFirstConfirmationSeesTheRevoke(t *testing.T) {
	w := newWorld()
	w.recFound = false
	r := w.reader()
	if got := ask(w, r, PurposeReport); got.Kind != Held || got.Confirmed {
		t.Fatalf("%+v", got)
	}
	w.set(func() { w.wf.Memo.Revoke = &Revoke{Save: save} })
	w.advance(time.Second)
	if got := ask(w, r, PurposeConfirm); got.Kind != Held || !got.RevokePending || w.count("describe") != 2 {
		t.Fatalf("%+v, %d describes", got, w.count("describe"))
	}
	w.set(func() { w.recFound, w.wf.Memo.Revoke = true, nil })
	w.advance(time.Second)
	if got := ask(w, r, PurposeConfirm); got.Kind != Held || !got.Confirmed || w.count("describe") != 2 {
		t.Fatalf("a confirmed one: %+v, %d describes", got, w.count("describe"))
	}
}

// A mint that arrives while the door's describe is in flight waits for it and
// is not answered from it; the next one, sent since, answers it.
func TestReader_TheMintSharesOnlyAReadSentSinceItArrived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		sent, land := make(chan struct{}), make(chan struct{})
		var first atomic.Bool
		w.onRead = func(kind string) {
			if kind == "describe" && first.CompareAndSwap(false, true) {
				close(sent)
				<-land
			}
		}
		a, b := make(chan Result, 1), make(chan Result, 1)
		go func() { a <- ask(w, r, PurposeOffer) }()
		<-sent
		w.advance(10 * time.Millisecond)
		arrived := w.clock()
		go func() { b <- Instance(context.Background(), r, PurposeMint, ref(), caller(), arrived) }()
		synctest.Wait()
		close(land)
		if got := <-a; got.Kind != Held {
			t.Fatalf("%+v", got)
		}
		if got := <-b; got.Kind != Held || w.count("describe") != 2 {
			t.Fatalf("%+v, %d describes; want 2", got, w.count("describe"))
		}
	})
}

// A read in flight serves a request only if it was sent after the request
// arrived, or if it confirms; the instant is the send, not the answer.
func TestReader_TheInstantIsTheSend(t *testing.T) {
	for _, confirms := range []bool{true, false} {
		t.Run(fmt.Sprintf("confirms %v", confirms), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newWorld()
				r := w.reader()
				sent, land := make(chan struct{}), make(chan struct{})
				var first atomic.Bool
				w.onRead = func(kind string) {
					if kind == "describe" && first.CompareAndSwap(false, true) {
						close(sent)
						<-land
					}
				}
				if !confirms {
					w.wf.Pending = nil
				}
				a := make(chan Result, 1)
				go func() { a <- ask(w, r, PurposeOffer) }()
				<-sent
				w.advance(10 * time.Millisecond) // the second request arrives after the send
				b := make(chan Result, 1)
				arrived := w.clock()
				go func() { b <- Instance(context.Background(), r, PurposeOffer, ref(), caller(), arrived) }()
				synctest.Wait() // the second call waits for the first's describe
				w.advance(10 * time.Millisecond)
				if !confirms {
					// What the cluster says by the time the second asks.
					w.set(func() {
						w.wf.Pending = []Pending{{ActivityID: ActivityID(3, 12), State: Started, Attempt: 2, LastWorker: orker}}
					})
				}
				close(land)
				ra, rb := <-a, <-b
				if confirms && (ra.Kind != Held || rb.Kind != Held) {
					t.Fatalf("%+v, %+v", ra, rb)
				}
				if !confirms && rb.Kind != Held {
					t.Fatalf("a describe sent before it arrived refused it: %+v (first %+v)", rb, ra)
				}
				if want := map[bool]int{true: 1, false: 2}[confirms]; w.count("describe") != want {
					t.Fatalf("%d describes; want %d", w.count("describe"), want)
				}
			})
		})
	}
}

// Calls that need a read while none of theirs is in flight share one.
func TestReader_OneReadARoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		gate := make(chan struct{})
		w.onRead = func(kind string) {
			if kind == "document" {
				<-gate
			}
		}
		const n = 50
		var wg sync.WaitGroup
		results := make([]Result, n)
		for i := range n {
			wg.Go(func() { results[i] = ask(w, r, PurposeOffer) })
		}
		// Every call is waiting for the one read by now.
		synctest.Wait()
		if got := w.count("document"); got != 1 {
			t.Fatalf("%d reads of one document at once", got)
		}
		close(gate)
		wg.Wait()
		for i, got := range results {
			if got.Kind != Held {
				t.Fatalf("call %d: %+v", i, got)
			}
		}
		if got := w.count("document"); got != 1 {
			t.Fatalf("%d reads for %d calls that came together", got, n)
		}
	})
}

func TestReader_Ages(t *testing.T) {
	t.Run("the document is read for every call", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		for range 3 {
			w.advance(time.Millisecond)
			ask(w, r, PurposeOffer)
		}
		if n := w.count("document"); n != 3 {
			t.Fatalf("%d reads for 3 calls", n)
		}
	})
	t.Run("a none is kept a second, a found an hour and may refuse", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeOffer)
		w.advance(900 * time.Millisecond)
		ask(w, r, PurposeOffer)
		if n := w.count("tombstone"); n != 1 {
			t.Fatalf("%d reads of none within a second", n)
		}
		w.set(func() { w.tomb = true })
		w.advance(200 * time.Millisecond)
		if got := ask(w, r, PurposeOffer); got.Kind != Tombstoned || w.count("tombstone") != 2 {
			t.Fatalf("a none older than a second was used: %+v", got)
		}
		w.set(func() { w.tombErr = errors.New("down") })
		w.advance(59 * time.Minute)
		if got := ask(w, r, PurposeOffer); got.Kind != Tombstoned || w.count("tombstone") != 2 {
			t.Fatalf("a found one within the hour: %+v, %d reads", got, w.count("tombstone"))
		}
		w.advance(2 * time.Minute)
		if got := ask(w, r, PurposeOffer); got.Kind != Unanswered || got.Read != ReadTombstone {
			t.Fatalf("past the hour, the store down: %+v", got)
		}
	})
	t.Run("a row confirms for the interval and never refuses", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeOffer)
		w.set(func() { w.row.Fit = false })
		w.advance(9 * time.Minute)
		if got := ask(w, r, PurposeOffer); got.Kind != Held || w.count("orker") != 1 {
			t.Fatalf("a kept row: %+v, %d reads", got, w.count("orker"))
		}
		w.advance(2 * time.Minute)
		if got := ask(w, r, PurposeOffer); got.Kind != NotHeld || w.count("orker") != 2 {
			t.Fatalf("past the interval: %+v, %d reads", got, w.count("orker"))
		}
		w.set(func() { w.row.Fit = true })
		w.advance(2 * time.Second)
		if got := ask(w, r, PurposeOffer); got.Kind != Held || w.count("orker") != 3 {
			t.Fatalf("a kept refusal refused: %+v, %d reads", got, w.count("orker"))
		}
	})
	t.Run("a confirm record found is kept for its attempt", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeMint)
		w.set(func() { w.recErr = errors.New("down") })
		w.advance(3 * time.Hour)
		if got := ask(w, r, PurposeMint); got.Kind != Held || w.count("confirm") != 1 {
			t.Fatalf("%+v, %d reads", got, w.count("confirm"))
		}
	})
	t.Run("a grant from the held document costs no read, a refusal one", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeOffer)
		if n := w.count("access"); n != 0 {
			t.Fatalf("a grant cost %d reads", n)
		}
		w.set(func() { w.docPerms = nil })
		w.advance(time.Millisecond)
		if got := ask(w, r, PurposeOffer); got.Kind != Held || w.count("access") != 1 {
			t.Fatalf("%+v, %d reads", got, w.count("access"))
		}
	})
}

// Past a cap the result is unanswered, never not-held, and one source or one
// Orker cannot spend what the others need.
func TestReader_Caps(t *testing.T) {
	t.Run("one source", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeOffer)
		w.set(func() { w.wf.Pending = nil })
		kinds := map[Kind]int{}
		for range 40 {
			w.advance(time.Microsecond)
			f := ref()
			f.Attempt = 9
			kinds[Instance(context.Background(), r, PurposeOffer, f, caller(), w.clock()).Kind]++
		}
		if n := w.count("describe") - 1; n > 5 || kinds[NotHeld] > 5 || kinds[Unanswered] != 40-kinds[NotHeld] {
			t.Fatalf("%d describes, %v", n, kinds)
		}
		w.advance(time.Second)
		f := ref()
		f.Attempt = 9
		if got := Instance(context.Background(), r, PurposeOffer, f, caller(), w.clock()); got.Kind != NotHeld {
			t.Fatalf("a second later: %+v", got)
		}
	})
	t.Run("one Orker over many sources, and the replica over many Orkers", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		for i := range 60 {
			f := ref()
			f.SourceID = fmt.Sprintf("evs-%016x", i)
			w.advance(time.Microsecond)
			Instance(context.Background(), r, PurposeOffer, f, caller(), w.clock())
		}
		if n := w.count("describe"); n > 5 {
			t.Fatalf("one Orker cost %d describes of sources it does not hold", n)
		}
		for i := range 300 {
			f, c := ref(), caller()
			f.SourceID = fmt.Sprintf("evs-%016x", 1000+i)
			c.OrkerID = fmt.Sprintf("orker-%d", i)
			w.advance(time.Microsecond)
			Instance(context.Background(), r, PurposeOffer, f, c, w.clock())
		}
		if n := w.count("describe"); n > 55 {
			t.Fatalf("the replica made %d describes in a moment", n)
		}
	})
	t.Run("a known source whose describe fails", func(t *testing.T) {
		w := newWorld()
		r := w.reader()
		ask(w, r, PurposeOffer)
		w.set(func() { w.wfErr = errors.New("down") })
		w.advance(describeAge + time.Second)
		for range 40 {
			w.advance(time.Microsecond)
			if got := ask(w, r, PurposeOffer); got.Kind != Unanswered || got.Read != ReadDescribe {
				t.Fatalf("%+v", got)
			}
		}
		if n := w.count("describe") - 1; n > 6 {
			t.Fatalf("%d describes of one source that fails", n)
		}
	})
	t.Run("an Orker's row", func(t *testing.T) {
		w := newWorld()
		w.row.Fit = false
		r := w.reader()
		kinds := map[Kind]int{}
		for range 10 {
			w.advance(time.Microsecond)
			kinds[ask(w, r, PurposeOffer).Kind]++
		}
		if n := w.count("orker"); n > 1 || kinds[NotHeld] > 1 || kinds[Unanswered] != 10-kinds[NotHeld] {
			t.Fatalf("%d reads, %v", n, kinds)
		}
	})
}

// What the reader keeps is bounded: an entry it evicts is read again, and no
// answer but held or unanswered changes.
func TestReader_Bound(t *testing.T) {
	w := newWorld()
	w.wf.Memo.Slots = map[int32]Slot{3: {Current: SlotEpoch{Epoch: 12, Digest: digest, AdmissionSave: save}}}
	r := w.reader()
	r.maxBytes = 4 << 10
	for i := range 200 {
		f, c := ref(), caller()
		f.SourceID = fmt.Sprintf("evs-%016x", i)
		f.Name = strings.Repeat("n", 100)
		w.advance(300 * time.Millisecond)
		got := Instance(context.Background(), r, PurposeOffer, f, c, w.clock())
		if got.Kind != OtherID && got.Kind != Unanswered {
			t.Fatalf("source %d: %+v", i, got)
		}
		if size := r.keptBytes(); size > r.maxBytes {
			t.Fatalf("%d bytes are kept; at most %d", size, r.maxBytes)
		}
	}
	if r.keptBytes() == 0 {
		t.Fatal("nothing is kept")
	}
}

// A caller that gives up does not take a shared read away from the others.
func TestReader_ACallerLeavingEndsNoSharedRead(t *testing.T) {
	w := newWorld()
	r := w.reader()
	sent, land := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	w.onRead = func(kind string) {
		if kind == "document" && first.CompareAndSwap(false, true) {
			close(sent)
			<-land
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := make(chan Result, 1)
	arrived := w.clock()
	go func() { a <- Instance(ctx, r, PurposeOffer, ref(), caller(), arrived) }()
	<-sent
	cancel()
	if got := <-a; got.Kind != Unanswered || got.Read != ReadDocument {
		t.Fatalf("the caller that left: %+v", got)
	}
	b := make(chan Result, 1)
	go func() { b <- Instance(context.Background(), r, PurposeOffer, ref(), caller(), arrived) }()
	close(land)
	if got := <-b; got.Kind != Held {
		t.Fatalf("the other: %+v", got)
	}
}

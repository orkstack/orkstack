package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// passWorld is a pass over documents in a real store, against a cluster as a
// test gives it.
type passWorld struct {
	*SourcePass
	c         *sourceCluster
	docs      *artifacts.BlobEventSourceStore
	src       *orkstackv1.EventSource
	confirmed map[string]bool
	reads     int
	confErr   error
	mu        sync.Mutex
	did       []PassDid
}

func newPassWorld(t *testing.T, edit func(*orkstackv1.EventSource)) *passWorld {
	t.Helper()
	starter, c := newStarter(t)
	docs, err := artifacts.NewLocalEventSourceStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := startSource()
	src.Uses, src.ServiceRevision, src.ServiceDigest, src.ResumeSeq = "ghcr.io/acme/s:1", 3, strings.Repeat("a", 64), 1
	if edit != nil {
		edit(src)
	}
	w := &passWorld{c: c, docs: docs, confirmed: map[string]bool{}}
	if w.src, err = docs.Create(context.Background(), src); err != nil {
		t.Fatal(err)
	}
	w.SourcePass = &SourcePass{Starter: starter, Documents: docs, Interval: time.Minute, Readmit: 5 * time.Minute, Logger: slog.New(slog.DiscardHandler),
		Did: func(d PassDid) {
			w.mu.Lock()
			w.did = append(w.did, d)
			w.mu.Unlock()
		},
		Confirmed: func(_ context.Context, a artifacts.InstanceAttempt) (artifacts.ConfirmRecord, bool, error) {
			w.reads++
			return artifacts.ConfirmRecord{}, w.confirmed[fmt.Sprintf("%d/%d/%d", a.Index, a.Epoch, a.Attempt)], w.confErr
		}}
	return w
}

// open makes the source's workflow one that loaded what the document says,
// and runs both its instances, ready.
func (w *passWorld) open(t *testing.T, carry func(*eventsource.Carry), now func(*eventsource.Now)) {
	t.Helper()
	c := eventsource.FirstCarry(sourcePinned())
	c.Loaded = &eventsource.LoadedFacts{Revision: 3, Instances: 2, ResumeSeq: 1}
	n := eventsource.Now{Load: "ok", Wanted: 2, Running: 2, Source: eventsource.Source{Namespace: "team", Name: "orders", Pool: "gpu", Instances: 2}, Slots: map[int32]eventsource.SlotMemo{}}
	for i := range int32(2) {
		epoch := int64(7 + i)
		n.Slots[i] = eventsource.SlotMemo{Current: eventsource.SlotEpoch{Epoch: epoch, Digest: "d", Phase: eventsource.PhaseReady},
			Awaits: map[int64][]string{epoch: {"healthy"}}, Had: map[int64]map[string]string{epoch: {"started": "", "ready": ""}}}
		w.confirmed[fmt.Sprintf("%d/%d/1", i, epoch)] = true
	}
	if carry != nil {
		carry(&c)
	}
	if now != nil {
		now(&n)
	}
	cb, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	nb, err := n.Encode()
	if err != nil {
		t.Fatal(err)
	}
	w.c.is("r1", running, string(cb), string(nb))
	w.c.pending = []*workflowpb.PendingActivityInfo{passActivity(0, 7, "active"), passActivity(1, 8, "active")}
}

func passActivity(index int32, epoch int64, state string) *workflowpb.PendingActivityInfo {
	details := eventsources.HeartbeatDetails{Epoch: epoch, State: state, Reason: "CONFIG_INVALID", Since: time.Now().Add(-time.Hour)}.Encode()
	return &workflowpb.PendingActivityInfo{ActivityId: held.ActivityID(index, epoch), State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1,
		LastWorkerIdentity: "orker-a", LastStartedTime: timestamppb.New(time.Now().Add(-time.Hour)),
		HeartbeatDetails: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: details}}}}
}

// once is one walk, and what it sent.
func (w *passWorld) once(t *testing.T) []string {
	t.Helper()
	w.did, w.c.signals = nil, nil
	if err := w.walk(t.Context()); err != nil {
		t.Fatal(err)
	}
	var sent []string
	for _, s := range w.c.signals {
		sent = append(sent, strings.TrimPrefix(s, workSource+" "))
	}
	return sent
}

// A walk over a deployment in which nothing has changed sends nothing.
func TestSourcePass_NothingChangedSendsNothing(t *testing.T) {
	w := newPassWorld(t, nil)
	w.open(t, nil, nil)
	for range 3 {
		if sent := w.once(t); len(sent) != 0 || len(w.c.started) != 0 || !slices.Equal(w.did, []PassDid{PassDescribed}) {
			t.Fatalf("sent %v, did %v", sent, w.did)
		}
	}
	// What was seen confirmed is not read again.
	if w.reads != 2 {
		t.Errorf("%d reads of the confirmations", w.reads)
	}
}

// Rows 0 and 1: what has no open workflow is started, and a source whose
// workflow removed what it kept is removed.
func TestSourcePass_StartsAndRemoves(t *testing.T) {
	marked := func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) }
	for name, tc := range map[string]struct {
		edit          func(*orkstackv1.EventSource)
		world         func(*passWorld)
		started, gone bool
		did           []PassDid
	}{
		"no workflow":                  {started: true, did: []PassDid{PassDescribed, PassStarted}},
		"one that closed":              {world: func(w *passWorld) { w.c.is("r1", sourceTerminated, carried(t, ""), "") }, started: true, did: []PassDid{PassDescribed, PassStarted}},
		"marked, with no workflow":     {edit: marked, started: true, did: []PassDid{PassDescribed, PassStarted}},
		"marked, and closed otherwise": {edit: marked, world: func(w *passWorld) { w.c.is("r1", completed, carried(t, "idle"), "") }, started: true, did: []PassDid{PassDescribed, PassStarted}},
		"marked and purged":            {edit: marked, world: func(w *passWorld) { w.c.is("r1", completed, carried(t, "purged"), "") }, gone: true, did: []PassDid{PassDescribed, PassRemoved}},
		"a copy that came back": {world: func(w *passWorld) {
			w.c.tombs[workSource] = true
			w.c.is("r1", completed, carried(t, "purged"), "")
		}, did: []PassDid{PassDescribed}},
		"a newer release's carry":     {world: func(w *passWorld) { w.c.is("r1", completed, `{"format":9}`, "") }, did: []PassDid{PassDescribed}},
		"a damaged carry":             {world: func(w *passWorld) { w.c.is("r1", running, `{"format":1,"good":7}`, "") }, did: []PassDid{PassDescribed}},
		"marked, and a damaged carry": {edit: marked, world: func(w *passWorld) { w.c.is("r1", completed, `{"format":1,"good":7}`, "") }, started: true, did: []PassDid{PassDescribed, PassStarted}},
		"a describe that fails":       {world: func(w *passWorld) { w.c.fail["describe1"] = errWorkStore }, did: []PassDid{PassDescribeFailed}},
		"a start that fails":          {world: func(w *passWorld) { w.c.fail["start1"] = errWorkStore }, did: []PassDid{PassDescribed}},
	} {
		t.Run(name, func(t *testing.T) {
			w := newPassWorld(t, tc.edit)
			if tc.world != nil {
				tc.world(w)
			}
			if sent := w.once(t); len(sent) != 0 || (len(w.c.started) > 0) != tc.started || !slices.Equal(w.did, tc.did) {
				t.Errorf("sent %v, %d started, did %v", sent, len(w.c.started), w.did)
			}
			_, err := w.docs.Get(t.Context(), "team", "orders")
			if errors.Is(err, eventsources.ErrNotFound) != tc.gone {
				t.Errorf("its document: %v", err)
			}
		})
	}

	// A document that changed since it was read is not removed.
	w := newPassWorld(t, marked)
	w.c.is("r1", completed, carried(t, "purged"), "")
	w.Documents = &changedSince{PassDocuments: w.docs, store: w.docs}
	w.once(t)
	if _, err := w.docs.Get(t.Context(), "team", "orders"); err != nil || !slices.Equal(w.did, []PassDid{PassDescribed}) {
		t.Errorf("%v, did %v", err, w.did)
	}
}

// changedSince changes the document between its read and what follows.
type changedSince struct {
	PassDocuments
	store *artifacts.BlobEventSourceStore
}

func (c *changedSince) Remove(ctx context.Context, ns, name, etag string) error {
	src, err := c.store.Get(ctx, ns, name)
	if err != nil {
		return err
	}
	src.Description = "changed"
	if _, err := c.store.Replace(ctx, src, src.GetEtag()); err != nil {
		return err
	}
	return c.PassDocuments.Remove(ctx, ns, name, etag)
}

// Rows 2 and 3: a workflow that has not read what it was last told is told
// nothing; one whose load differs from the document is told to load.
func TestSourcePass_SyncsWhatDiffers(t *testing.T) {
	for name, tc := range map[string]struct {
		edit  func(*orkstackv1.EventSource)
		carry func(*eventsource.Carry)
		now   func(*eventsource.Now)
		world func(*passWorld)
		want  bool
	}{
		"as it is":              {},
		"a revision":            {edit: func(s *orkstackv1.EventSource) { s.ServiceRevision = 4 }, want: true},
		"a pause":               {edit: func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{} }, want: true},
		"a resume":              {carry: func(c *eventsource.Carry) { c.Loaded.Paused = true }, want: true},
		"instances":             {edit: func(s *orkstackv1.EventSource) { s.Instances = 3 }, want: true},
		"the resume's sequence": {edit: func(s *orkstackv1.EventSource) { s.ResumeSeq = 2 }, want: true},
		"a hold":                {world: func(w *passWorld) { w.c.ns.Hold = &orkstackv1.NamespaceHold{} }, want: true},
		"a hold lifted":         {carry: func(c *eventsource.Carry) { c.Loaded.Held = true }, want: true},
		"a mark":                {edit: func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) }, want: true},
		"a mark that was loaded": {edit: func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow); s.ServiceRevision = 9 },
			carry: func(c *eventsource.Carry) { c.Loaded.Deleting = true }},
		"a copy of what is being deleted": {carry: func(c *eventsource.Carry) { c.Loaded.Deleting = true }},
		"its namespace's default pool":    {edit: func(s *orkstackv1.EventSource) { s.Pool = "" }, world: func(w *passWorld) { w.c.ns.DefaultPool = "arm" }, want: true},
		"the same default pool":           {edit: func(s *orkstackv1.EventSource) { s.Pool = "" }, world: func(w *passWorld) { w.c.ns.DefaultPool = "gpu" }},
		"a judging change":                {edit: func(s *orkstackv1.EventSource) { s.JudgingRevision = 9 }},
		"a load that fails":               {edit: func(s *orkstackv1.EventSource) { s.ServiceRevision = 4 }, now: func(n *eventsource.Now) { n.Load = "failing" }},
		"a document that is missing":      {edit: func(s *orkstackv1.EventSource) { s.ServiceRevision = 4 }, now: func(n *eventsource.Now) { n.Load = "missing" }},
		"nothing loaded yet":              {carry: func(c *eventsource.Carry) { c.Loaded = nil }},
		"a task pending":                  {edit: func(s *orkstackv1.EventSource) { s.ServiceRevision = 4 }, world: func(w *passWorld) { w.c.task = true }},
		"a namespace that cannot be read": {world: func(w *passWorld) { w.c.nsErr = errWorkStore }},
	} {
		t.Run(name, func(t *testing.T) {
			w := newPassWorld(t, tc.edit)
			w.open(t, tc.carry, tc.now)
			if tc.world != nil {
				tc.world(w)
			}
			want := []string(nil)
			if tc.want {
				want = []string{"r1 sync <nil>"}
			}
			if sent := w.once(t); !slices.Equal(sent, want) || len(w.c.started) != 0 {
				t.Errorf("sent %v; want %v", sent, want)
			}
		})
	}
}

// Row 5: what was taken long ago and never confirmed is asked to be scheduled
// again, at most six times a day, and never where the record cannot be read.
func TestSourcePass_ReroutesWhatNeverConfirmed(t *testing.T) {
	fresh := func(n int, ago time.Duration) func(*eventsource.Carry) {
		return func(c *eventsource.Carry) {
			c.Index = map[int32]eventsource.IndexFacts{0: {Fresh: slices.Repeat([]time.Time{time.Now().Add(-ago)}, n)}}
		}
	}
	for name, tc := range map[string]struct {
		carry func(*eventsource.Carry)
		world func(*passWorld)
		want  bool
	}{
		"never confirmed":                {want: true},
		"confirmed":                      {world: func(w *passWorld) { w.confirmed["0/7/1"] = true }},
		"an earlier attempt confirmed":   {world: func(w *passWorld) { w.confirmed["0/7/1"] = true; w.c.pending[0].Attempt = 2 }, want: true},
		"taken a moment ago":             {world: func(w *passWorld) { w.c.pending[0].LastStartedTime = timestamppb.New(time.Now().Add(-9 * time.Minute)) }},
		"not taken":                      {world: func(w *passWorld) { w.c.pending[0].State = enumspb.PENDING_ACTIVITY_STATE_SCHEDULED }},
		"being cancelled":                {world: func(w *passWorld) { w.c.pending[0].State = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED }},
		"not known when":                 {world: func(w *passWorld) { w.c.pending[0].LastStartedTime = nil }},
		"a record that cannot be read":   {world: func(w *passWorld) { w.confErr = errWorkStore }},
		"six times today":                {carry: fresh(6, time.Hour)},
		"six times, the first a day ago": {carry: fresh(6, 25*time.Hour), want: true},
		"five times today":               {carry: fresh(5, time.Hour), want: true},
	} {
		t.Run(name, func(t *testing.T) {
			w := newPassWorld(t, nil)
			w.open(t, tc.carry, func(n *eventsource.Now) {
				s := n.Slots[0]
				s.Current.Phase = eventsource.PhaseScheduled
				s.Awaits, s.Had = map[int64][]string{7: {"started", "ready"}}, nil
				n.Slots[0] = s
			})
			delete(w.confirmed, "0/7/1")
			w.c.pending[0].HeartbeatDetails = nil
			if tc.world != nil {
				tc.world(w)
			}
			want := []string(nil)
			if tc.want {
				want = []string{fmt.Sprintf("r1 reroute %+v", SourceReroute{Index: 0, Epoch: 7})}
			}
			sent := slices.DeleteFunc(w.once(t), func(s string) bool { return strings.Contains(s, " report ") })
			if !slices.Equal(sent, want) {
				t.Errorf("sent %v; want %v", sent, want)
			}
		})
	}
}

// Row 6: a report the workflow waits for, and was not given, is sent again as
// the instance's beat shows it.
func TestSourcePass_ReportsWhatWasLost(t *testing.T) {
	awaits := func(words ...string) func(*eventsource.Now) {
		return func(n *eventsource.Now) {
			n.Slots[0] = eventsource.SlotMemo{Current: eventsource.SlotEpoch{Epoch: 7, Digest: "d", Phase: eventsource.PhaseScheduled}, Awaits: map[int64][]string{7: words}}
		}
	}
	for name, tc := range map[string]struct {
		now     func(*eventsource.Now)
		state   string
		attempt int32
		beat    func(*workflowpb.PendingActivityInfo)
		want    []string
	}{
		"started and ready":        {now: awaits("started", "ready", "invalid", "healthy"), state: "active", want: []string{"started", "ready"}},
		"waiting is ready":         {now: awaits("ready"), state: "waiting", want: []string{"ready"}},
		"starting":                 {now: awaits("started", "ready", "invalid", "healthy"), state: "starting", want: []string{"started"}},
		"invalid":                  {now: awaits("ready", "invalid"), state: "invalid", want: []string{"invalid CONFIG_INVALID"}},
		"unreachable":              {now: awaits("ready", "invalid"), state: "unreachable"},
		"healthy":                  {now: awaits("healthy"), state: "active", attempt: 2, want: []string{"healthy"}},
		"healthy, a first attempt": {now: awaits("healthy"), state: "active"},
		"healthy, not for long": {now: awaits("healthy"), state: "active", attempt: 2, beat: func(a *workflowpb.PendingActivityInfo) {
			a.HeartbeatDetails.Payloads[0].Data = eventsources.HeartbeatDetails{Epoch: 7, State: "active", Since: time.Now().Add(-9 * time.Minute)}.Encode()
		}},
		"nothing waited for": {now: awaits(), state: "active"},
		"another epoch's beat": {now: awaits("ready"), state: "active", beat: func(a *workflowpb.PendingActivityInfo) {
			a.HeartbeatDetails.Payloads[0].Data = eventsources.HeartbeatDetails{Epoch: 6, State: "active"}.Encode()
		}},
		"a beat that does not read": {now: awaits("ready"), state: "active", beat: func(a *workflowpb.PendingActivityInfo) { a.HeartbeatDetails.Payloads[0].Data = []byte("x") }},
		"a beat of two parts": {now: awaits("ready"), state: "active", beat: func(a *workflowpb.PendingActivityInfo) {
			a.HeartbeatDetails.Payloads = append(a.HeartbeatDetails.Payloads, &commonpb.Payload{})
		}},
		"no beat": {now: awaits("started", "ready"), state: "active", beat: func(a *workflowpb.PendingActivityInfo) { a.HeartbeatDetails = nil }, want: []string{"started"}},
		"a replacement": {now: func(n *eventsource.Now) {
			s := n.Slots[0]
			s.Replacement = &eventsource.SlotEpoch{Epoch: 9, Digest: "d2", Phase: eventsource.PhaseScheduled}
			s.Awaits[9] = []string{"started", "ready"}
			n.Slots[0] = s
		}, state: "active", want: []string{"9 started", "9 ready"}},
	} {
		t.Run(name, func(t *testing.T) {
			w := newPassWorld(t, nil)
			w.open(t, nil, tc.now)
			attempt := max(tc.attempt, 1)
			a := passActivity(0, 7, tc.state)
			a.Attempt = attempt
			w.confirmed[fmt.Sprintf("0/7/%d", attempt)], w.confirmed["0/9/1"] = true, true
			if tc.beat != nil {
				tc.beat(a)
			}
			w.c.pending = []*workflowpb.PendingActivityInfo{a, passActivity(1, 8, "active"), passActivity(0, 9, "active")}
			var want []string
			for _, r := range tc.want {
				epoch := int64(7)
				if rest, ok := strings.CutPrefix(r, "9 "); ok {
					epoch, r = 9, rest
				}
				what, reason, _ := strings.Cut(r, " ")
				want = append(want, fmt.Sprintf("r1 report %+v", SourceReport{Index: 0, Epoch: epoch, What: what, Reason: reason}))
			}
			if sent := w.once(t); !slices.Equal(sent, want) {
				t.Errorf("sent %v; want %v", sent, want)
			}
		})
	}
}

// A walk starts twenty workflows at most, at ten sources a second, and looks
// at every source once.
func TestSourcePass_Rate(t *testing.T) {
	w := newPassWorld(t, nil)
	for i := range 29 {
		src := startSource()
		src.Id, src.Name, src.Uses = fmt.Sprintf("evs-%016x", i+1), fmt.Sprintf("s%d", i), "ghcr.io/acme/s:1"
		if _, err := w.docs.Create(t.Context(), src); err != nil {
			t.Fatal(err)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		began := time.Now()
		if err := w.walk(t.Context()); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(began); took != 30*passSpacing {
			t.Errorf("a walk of 30 sources took %s", took)
		}
	})
	if len(w.c.started) != passStarts {
		t.Errorf("%d started", len(w.c.started))
	}
	ids := map[string]bool{}
	for _, in := range w.c.inputs {
		ids[in.ID] = true
	}
	if len(ids) != passStarts {
		t.Errorf("%d sources started", len(ids))
	}
}

// It runs at its interval, goes on past a store that is away, and does
// nothing where the deployment is not versioned.
func TestSourcePass_Runs(t *testing.T) {
	w := newPassWorld(t, nil)
	w.open(t, nil, nil)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute+time.Second)
		defer cancel()
		away := &awayNames{PassDocuments: w.docs, away: 1}
		w.Documents = away
		walked := 0
		w.Walked = func(time.Time) { walked++ }
		w.Run(ctx)
		if walked != 3 || away.asked != 4 {
			t.Errorf("walked %d times, asked %d", walked, away.asked)
		}
	})
	w = newPassWorld(t, nil)
	w.Starter.Versioned = false
	w.Run(t.Context())
	if len(w.c.calls) != 0 {
		t.Errorf("an engine that is no release asked %v", w.c.calls)
	}
}

type awayNames struct {
	PassDocuments
	away, asked int
}

func (a *awayNames) Names(ctx context.Context) ([]eventsources.Key, error) {
	if a.asked++; a.asked <= a.away {
		return nil, errWorkStore
	}
	return a.PassDocuments.Names(ctx)
}

// The states a beat is read by are the ones an instance shows.
func TestSourcePass_States(t *testing.T) {
	if instanceActive != sourcehost.StateActive || instanceWaiting != sourcehost.StateWaiting || instanceInvalid != sourcehost.StateInvalid {
		t.Error("an instance shows other states")
	}
}

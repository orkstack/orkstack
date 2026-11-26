package engine

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const workSource = "evs-0123456789abcdef"

var workNow = time.Unix(1_800_000_000, 0).UTC()

type workWorld struct {
	*SourceWorker
	docs *artifacts.BlobEventSourceStore
	dir  string
	ns   *orkstackv1.Namespace
	err  error
}

// Get is the namespace registry, with a failure a registry does not give at
// will.
func (w *workWorld) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	if name != "team" {
		return nil, artifacts.ErrNotFound
	}
	return w.ns, w.err
}

func newWorkWorld(t *testing.T) *workWorld {
	t.Helper()
	dir := t.TempDir()
	docs, err := artifacts.NewLocalEventSourceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := artifacts.NewLocalEventSourceInstanceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w := &workWorld{docs: docs, dir: dir, ns: &orkstackv1.Namespace{Name: "team"}}
	w.SourceWorker = &SourceWorker{Documents: docs, Records: records, Namespaces: w,
		Pin: sourcePinned(), MaxEvents: 51200, MaxBytes: 50 << 20, Now: func() time.Time { return workNow }, Logger: slog.New(slog.DiscardHandler)}
	return w
}

func (w *workWorld) create(t *testing.T, edit func(*orkstackv1.EventSource)) *orkstackv1.EventSource {
	t.Helper()
	src := &orkstackv1.EventSource{Id: workSource, Namespace: "team", Name: "orders", OrkflowName: "flow", Uses: "ghcr.io/acme/s:1", Pool: "gpu", Instances: 3,
		ServiceRevision: 4, JudgingRevision: 1, ServiceDigest: strings.Repeat("a", 64), ResumeSeq: 2, TrialCode: "X",
		Resources: &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "64Mi"}}
	if edit != nil {
		edit(src)
	}
	out, err := w.docs.Create(t.Context(), src)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

var workAsk = SourceLoadAsk{SourceRef: SourceRef{Namespace: "team", Name: "orders", ID: workSource}, Revision: 4}

func TestSourceWorker_LoadsWhatTheDocumentAsks(t *testing.T) {
	found := eventsource.Load{Outcome: eventsource.LoadFound, Pool: "gpu", Instances: 3, ResumeSeq: 2, TrialCode: "X", MaxEvents: 51200, MaxBytes: 50 << 20,
		Target: eventsource.Spec{Revision: 4, Digest: strings.Repeat("a", 64), Queue: "ork-source-gpu", CPU: "500m", Memory: "64Mi"}}
	for name, tc := range map[string]struct {
		edit func(*orkstackv1.EventSource)
		ask  func(*SourceLoadAsk)
		held bool
		pool string
		want func(*eventsource.Load)
	}{
		"as it is":     {},
		"paused":       {edit: func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{} }, want: func(l *eventsource.Load) { l.Paused = true }},
		"held":         {held: true, want: func(l *eventsource.Load) { l.Held = true }},
		"in place":     {edit: func(s *orkstackv1.EventSource) { s.InPlaceRevision = 4 }, want: func(l *eventsource.Load) { l.InPlace = true }},
		"was in place": {edit: func(s *orkstackv1.EventSource) { s.InPlaceRevision = 3 }},
		"restarted": {edit: func(s *orkstackv1.EventSource) { s.RestartRevision = 4 }, ask: func(a *SourceLoadAsk) { a.Revision = 3 },
			want: func(l *eventsource.Load) { p := sourcePinned(); l.Repin = &p }},
		"its namespace's default pool": {pool: "arm", edit: func(s *orkstackv1.EventSource) { s.Pool = "" }, want: func(l *eventsource.Load) {
			l.Pool, l.Target.Queue = "arm", "ork-source-arm"
		}},
		"restarted before": {edit: func(s *orkstackv1.EventSource) { s.RestartRevision = 4 }},
		"the defaults": {edit: func(s *orkstackv1.EventSource) { s.Pool, s.Instances, s.Resources = "", 0, nil }, want: func(l *eventsource.Load) {
			l.Pool, l.Instances = "main", 1
			l.Target.Queue, l.Target.CPU, l.Target.Memory = "ork-source-main", "", ""
		}},
	} {
		t.Run(name, func(t *testing.T) {
			w, ask, want := newWorkWorld(t), workAsk, found
			w.create(t, tc.edit)
			if tc.held {
				w.ns.Hold = &orkstackv1.NamespaceHold{}
			}
			w.ns.DefaultPool = tc.pool
			if tc.ask != nil {
				tc.ask(&ask)
			}
			if tc.want != nil {
				tc.want(&want)
			}
			if got, err := w.LoadEventSource(t.Context(), ask); err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("%+v, %v; want %+v", got, err, want)
			}
		})
	}
}

// TestSourceWorker_TheDocumentRule: only a delete's own mark, in the document
// or under the id, says that a source was deleted. An absent document says
// nothing, in a store that holds something of the source, and a store that
// holds nothing of it is not its store.
func TestSourceWorker_TheDocumentRule(t *testing.T) {
	put := func(t *testing.T, w *workWorld, spec bool, other func(*orkstackv1.EventSource), documents, tombstone bool) {
		t.Helper()
		if documents {
			w.create(t, other)
		}
		if spec {
			if err := w.docs.PutSpec(t.Context(), workSource, strings.Repeat("a", 64), []byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
		if tombstone {
			if err := w.Records.Tombstone(t.Context(), workSource, "team", "orders", workNow); err != nil {
				t.Fatal(err)
			}
		}
	}
	marked := func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) }
	another := func(s *orkstackv1.EventSource) { s.Id = "evs-0123456789abcde0" }
	for name, tc := range map[string]struct {
		spec, document, tombstone bool
		edit                      func(*orkstackv1.EventSource)
		want                      eventsource.LoadOutcome
	}{
		"marked":                             {document: true, edit: marked, spec: true, want: eventsource.LoadDeleting},
		"marked, with its tombstone":         {document: true, edit: marked, tombstone: true, want: eventsource.LoadDeleting},
		"a tombstone alone":                  {tombstone: true, want: eventsource.LoadDeleted},
		"a copy that came back":              {document: true, tombstone: true, spec: true, want: eventsource.LoadDeleted},
		"no document":                        {spec: true, want: eventsource.LoadMissing},
		"another's document under the name":  {document: true, edit: another, spec: true, want: eventsource.LoadMissing},
		"another's, and a tombstone":         {document: true, edit: another, spec: true, tombstone: true, want: eventsource.LoadDeleted},
		"nothing":                            {want: eventsource.LoadFailed},
		"another's document and nothing":     {document: true, edit: another, want: eventsource.LoadFailed},
		"another's marked document":          {document: true, edit: func(s *orkstackv1.EventSource) { another(s); marked(s) }, spec: true, want: eventsource.LoadMissing},
		"a document that names no spec":      {document: true, edit: func(s *orkstackv1.EventSource) { s.ServiceDigest = "" }, want: eventsource.LoadFailed},
		"a document whose pool is no pool's": {document: true, edit: func(s *orkstackv1.EventSource) { s.Pool = "Not A Pool" }, want: eventsource.LoadFailed},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorkWorld(t)
			put(t, w, tc.spec, tc.edit, tc.document, tc.tombstone)
			got, err := w.LoadEventSource(t.Context(), workAsk)
			if got.Outcome != tc.want || (err != nil) != (tc.want == eventsource.LoadFailed) {
				t.Errorf("%+v, %v", got, err)
			}
		})
	}

	// A read that fails is never an answer.
	t.Run("a document that cannot be read", func(t *testing.T) {
		w := newWorkWorld(t)
		put(t, w, true, nil, true, false)
		if err := os.WriteFile(filepath.Join(w.dir, "metadata", "event-sources", "team", "orders.json"), []byte(`{"format":9}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := w.LoadEventSource(t.Context(), workAsk); err == nil {
			t.Errorf("%+v", got)
		}
	})
	for name, records := range map[string]failingRecords{"its tombstone": {tombstoned: true}, "its specs": {specs: true}} {
		t.Run(name+" cannot be read", func(t *testing.T) {
			w := newWorkWorld(t)
			put(t, w, true, nil, name == "its tombstone", false)
			records.SourceRecords = w.Records
			w.Records = records
			if got, err := w.LoadEventSource(t.Context(), workAsk); !errors.Is(err, errWorkStore) {
				t.Errorf("%+v, %v", got, err)
			}
		})
	}
	t.Run("its namespace cannot be read", func(t *testing.T) {
		w := newWorkWorld(t)
		put(t, w, true, nil, true, false)
		w.err = errWorkStore
		if got, err := w.LoadEventSource(t.Context(), workAsk); !errors.Is(err, errWorkStore) {
			t.Errorf("%+v, %v", got, err)
		}
	})
}

var errWorkStore = errors.New("the store is down")

// failingRecords fails the reads it names, which a store a test runs does not
// do one at a time.
type failingRecords struct {
	SourceRecords
	tombstoned, specs bool
}

func (f failingRecords) Tombstoned(ctx context.Context, id string) (bool, error) {
	if f.tombstoned {
		return false, errWorkStore
	}
	return f.SourceRecords.Tombstoned(ctx, id)
}

func (f failingRecords) KeepsSpecs(ctx context.Context, id string) (bool, error) {
	if f.specs {
		return false, errWorkStore
	}
	return f.SourceRecords.KeepsSpecs(ctx, id)
}

func (f failingRecords) Tombstone(ctx context.Context, id, ns, name string, at time.Time) error {
	if f.tombstoned {
		return errWorkStore
	}
	return f.SourceRecords.Tombstone(ctx, id, ns, name, at)
}

// A purge removes nothing before the tombstone is there.
func TestSourceWorker_Purges(t *testing.T) {
	w := newWorkWorld(t)
	for _, digit := range []string{"a", "b", "c"} {
		if err := w.docs.PutSpec(t.Context(), workSource, strings.Repeat(digit, 64), []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	records := w.Records
	w.Records = failingRecords{SourceRecords: records, tombstoned: true}
	if left, err := w.PurgeEventSource(t.Context(), workAsk.SourceRef, 2); !errors.Is(err, errWorkStore) || left {
		t.Fatalf("%v, %v", left, err)
	}
	if kept, err := records.KeepsSpecs(t.Context(), workSource); !kept || err != nil {
		t.Fatalf("its specs are gone before its tombstone is there: %v", err)
	}
	w.Records = records
	for _, want := range []bool{true, false, false} {
		if left, err := w.PurgeEventSource(t.Context(), workAsk.SourceRef, 2); err != nil || left != want {
			t.Fatalf("left %v, %v", left, err)
		}
	}
	if gone, err := records.Tombstoned(t.Context(), workSource); !gone || err != nil {
		t.Fatalf("no tombstone: %v", err)
	}
}

func TestSourceWorker_Pauses(t *testing.T) {
	ask := SourcePauseAsk{SourceRef: workAsk.SourceRef, Code: eventsources.PauseInstanceFailed, Refusal: "IMAGE_NOT_PERMITTED", Detail: "IMAGE_NOT_SIGNED", Revision: 4, ResumeSeq: 2}
	for name, tc := range map[string]struct {
		edit    func(*orkstackv1.EventSource)
		absent  bool
		paused  bool
		written bool
	}{
		"running":                {paused: true, written: true},
		"paused before":          {edit: func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{Reason: "mine"} }, paused: true},
		"marked by a delete":     {edit: func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) }, paused: true},
		"changed since":          {edit: func(s *orkstackv1.EventSource) { s.ServiceRevision = 5 }},
		"resumed since":          {edit: func(s *orkstackv1.EventSource) { s.ResumeSeq = 3 }},
		"another under its name": {edit: func(s *orkstackv1.EventSource) { s.Id = "evs-0123456789abcde0" }},
		"not there":              {absent: true},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorkWorld(t)
			var before *orkstackv1.EventSource
			if !tc.absent {
				before = w.create(t, tc.edit)
			}
			if paused, err := w.PauseEventSource(t.Context(), ask); err != nil || paused != tc.paused {
				t.Fatalf("%v, %v", paused, err)
			}
			if tc.absent {
				return
			}
			after, err := w.docs.Get(t.Context(), "team", "orders")
			if err != nil {
				t.Fatal(err)
			}
			if !tc.written {
				if !proto.Equal(before, after) {
					t.Errorf("it was written: %v", after)
				}
				return
			}
			p := after.GetPause()
			if p.GetCause() != orkstackv1.PauseCause_PAUSE_CAUSE_ENGINE || p.GetCode() != ask.Code || p.GetRefusalCode() != ask.Refusal || p.GetRequestedByOrker() != "" ||
				p.GetReason() != eventsources.PauseSentence(ask.Code)+" (IMAGE_NOT_SIGNED)" || !p.GetSince().AsTime().Equal(workNow) || after.GetUpdatedBy() != "" {
				t.Errorf("paused %v", after)
			}
		})
	}

	// A document that changes under the write is read and judged again.
	w := newWorkWorld(t)
	w.create(t, nil)
	races := &racingDocuments{SourceDocuments: w.docs, change: func(s *orkstackv1.EventSource) { s.Rate = 7 }, times: 1}
	w.Documents = races
	if paused, err := w.PauseEventSource(t.Context(), ask); err != nil || !paused {
		t.Fatalf("%v, %v", paused, err)
	}
	if src, err := w.docs.Get(t.Context(), "team", "orders"); err != nil || src.GetPause() == nil || src.GetRate() != 7 {
		t.Fatalf("%v, %v", src, err)
	}
	w = newWorkWorld(t)
	w.create(t, nil)
	w.Documents = &racingDocuments{SourceDocuments: w.docs, change: func(s *orkstackv1.EventSource) { s.Rate++ }, times: 99}
	if paused, err := w.PauseEventSource(t.Context(), ask); err == nil || paused {
		t.Fatalf("%v, %v", paused, err)
	}
	ask.Code = "NO_CODE_OF_A_PAUSE"
	if paused, err := w.PauseEventSource(t.Context(), ask); err == nil || paused {
		t.Fatalf("%v, %v", paused, err)
	}
	long := eventsources.EnginePause(eventsources.PauseInstanceFailed, "X", strings.Repeat("é", 4000), "", workNow)
	if len(long.GetReason()) > 1024 {
		t.Errorf("a pause's reason is %d bytes", len(long.GetReason()))
	}
}

// racingDocuments changes the document between a read and the write over it.
type racingDocuments struct {
	SourceDocuments
	change func(*orkstackv1.EventSource)
	times  int
}

func (r *racingDocuments) Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error) {
	if r.times > 0 {
		r.times--
		now, err := r.Get(ctx, src.GetNamespace(), src.GetName())
		if err != nil {
			return nil, err
		}
		r.change(now)
		if _, err := r.SourceDocuments.Replace(ctx, now, now.GetEtag()); err != nil {
			return nil, err
		}
	}
	return r.SourceDocuments.Replace(ctx, src, etag)
}

// A missing document is logged by the load that first finds it so, and once a
// day after.
func TestSourceWorker_SaysWhatIsMissing(t *testing.T) {
	var log strings.Builder
	w := newWorkWorld(t)
	w.Logger = slog.New(slog.NewTextHandler(&log, nil))
	if err := w.docs.PutSpec(t.Context(), workSource, strings.Repeat("a", 64), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	for said, want := range map[time.Time]bool{{}: true, workNow.Add(-time.Hour): false, workNow.Add(-24 * time.Hour): true} {
		log.Reset()
		ask := workAsk
		ask.MissingSaid = said
		if l, err := w.LoadEventSource(t.Context(), ask); err != nil || l.Outcome != eventsource.LoadMissing {
			t.Fatalf("%+v, %v", l, err)
		}
		if strings.Contains(log.String(), "EVENT_SOURCE_DOCUMENT_MISSING") != want {
			t.Errorf("said at %s: %q", said, log.String())
		}
	}
}

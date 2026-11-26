package archive

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"google.golang.org/protobuf/types/known/durationpb"
)

// world is one in-memory deployment the GC tests drive: the object store, the
// cluster, the archive bodies and the sweep, all on one fake clock. The store
// clock is world.now; an engine clock may be skewed against it.
//
// It exists for the fault cases a real store and cluster cannot produce on
// demand: a clock skewed by an exact amount, a call that fails at one chosen
// point, a listing that interleaves with a delete, a chain caught mid-switch,
// a forged sidecar. The path a deployment takes is proved against real
// SeaweedFS and Temporal (internal/engine TestIntegration_ArtifactGC_*).
type world struct {
	t   *testing.T
	mu  sync.Mutex
	now time.Time
	// skew is the engine's clock minus the store's.
	skew time.Duration

	objects    map[string]int64
	markers    map[string]map[string]bool
	lists      map[string][]string
	sidecars   map[string]time.Time
	bodies     map[string]RunBody
	executions map[string]bool
	commits    map[string]int
	// attested is how many engine records lie under a run's attested prefix.
	attested map[string]int
	records  map[string]artifacts.ReclaimRecord
	index    []artifacts.RunIndexEntry

	// live and closed are what the low-water walk answers.
	live    map[string]time.Time
	closed  map[string]time.Time
	walkErr error
	cause   string

	sweep     SweepPass
	hasSweep  bool
	local     bool
	retention time.Duration
	accepted  *acceptedRun

	// probeDelay is how long reading the store's clock takes: both clocks
	// run on while the probe is in flight.
	probeDelay time.Duration
	// fail makes a named store or cluster call fail.
	fail map[string]error
	// removed records every object the GC deleted.
	removed []string
	// onList runs before ListObjectReferences or ListUnder, with how many
	// times that key has been listed: an injected interleaving point.
	onList func(key string, n int)
	listed map[string]int
	// writes counts every write the GC made to the store.
	writes int
}

type acceptedRun struct {
	id string
	at time.Time
}

func newWorld(t *testing.T) *world {
	t.Helper()
	return &world{
		t: t, now: time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC),
		objects: map[string]int64{}, markers: map[string]map[string]bool{}, lists: map[string][]string{},
		sidecars: map[string]time.Time{}, bodies: map[string]RunBody{}, executions: map[string]bool{},
		commits: map[string]int{}, attested: map[string]int{}, records: map[string]artifacts.ReclaimRecord{},
		live: map[string]time.Time{}, closed: map[string]time.Time{},
		sweep: SweepPass{Clean: true}, hasSweep: true,
		retention: 24 * time.Hour, fail: map[string]error{}, listed: map[string]int{},
	}
}

// objectID renders an S3-layout object produced by run.
func objectID(run, name string) string {
	return fmt.Sprintf("orkflows/flow/%s/task/%s/%064x", run, name, len(name)+len(run))
}

// produce records a finished, archived run that produced objects, with
// markers and a list as PutRun writes them.
func (w *world) produce(run string, created, finished time.Time, names ...string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var ids []string
	body := RunBody{RunID: run, Namespace: "default", Terminal: true, FinishedAt: finished}
	for _, n := range names {
		id := objectID(run, n)
		ids = append(ids, id)
		w.objects[id] = 10
		body.Objects = append(body.Objects, BodyObject{ID: id, Class: ClassArtifact})
		w.mark(id, run)
	}
	w.lists[run] = append(w.lists[run], ids...)
	w.bodies[run] = body
	w.index = append(w.index, artifacts.RunIndexEntry{RunID: run, Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: created})
	return ids
}

// reference records an archived run that references existing objects (a
// memo hit), as PutRun writes it.
func (w *world) reference(run string, created, finished time.Time, ids ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	body := RunBody{RunID: run, Namespace: "default", Terminal: true, FinishedAt: finished}
	for _, id := range ids {
		body.Objects = append(body.Objects, BodyObject{ID: id, Class: ClassArtifact})
		w.mark(id, run)
	}
	w.lists[run] = append(w.lists[run], ids...)
	w.bodies[run] = body
	w.index = append(w.index, artifacts.RunIndexEntry{RunID: run, Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: created})
}

func (w *world) mark(id, run string) {
	if w.markers[id] == nil {
		w.markers[id] = map[string]bool{}
	}
	w.markers[id][run] = true
}

func (w *world) advance(d time.Duration) {
	w.mu.Lock()
	w.now = w.now.Add(d)
	w.mu.Unlock()
}

func (w *world) exists(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.objects[id]
	return ok
}

// --- ReclaimStore ---

func (w *world) Local() bool { return w.local }

func (w *world) StoreNow(context.Context) (time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["StoreNow"]; err != nil {
		return time.Time{}, err
	}
	w.now = w.now.Add(w.probeDelay)
	return w.now, nil
}

func (w *world) ListRuns(_ context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["ListRuns"]; err != nil {
		return nil, err
	}
	entries := append([]artifacts.RunIndexEntry(nil), w.index...)
	sort.Slice(entries, func(i, j int) bool {
		a, _ := artifacts.RunIndexKeyForEntry(entries[i])
		b, _ := artifacts.RunIndexKeyForEntry(entries[j])
		return a < b
	})
	var out []artifacts.RunIndexEntry
	for _, e := range entries {
		key, _ := artifacts.RunIndexKeyForEntry(e)
		if key <= q.StartAfterKey || (!q.Until.IsZero() && !e.Time.Before(q.Until)) {
			continue
		}
		out = append(out, e)
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (w *world) HasRun(_ context.Context, run string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["HasRun"]; err != nil {
		return false, err
	}
	_, ok := w.bodies[run]
	return ok, nil
}

func (w *world) CleanupSidecar(_ context.Context, run string) (artifacts.RunCleanupState, time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["CleanupSidecar"]; err != nil {
		return artifacts.RunCleanupState{}, time.Time{}, err
	}
	at, ok := w.sidecars[run]
	if !ok {
		return artifacts.RunCleanupState{}, time.Time{}, artifacts.ErrNotFound
	}
	return artifacts.RunCleanupState{RunID: run, ArtifactsCleaned: true}, at, nil
}

func (w *world) RunObjectList(_ context.Context, run string) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["RunObjectList"]; err != nil {
		return nil, err
	}
	ids, ok := w.lists[run]
	if !ok {
		return nil, artifacts.ErrNotFound
	}
	return append([]string(nil), ids...), nil
}

func (w *world) hook(key string) {
	w.mu.Lock()
	n := w.listed[key] + 1
	w.listed[key] = n
	hook := w.onList
	w.mu.Unlock()
	if hook != nil {
		hook(key, n)
	}
}

func (w *world) ListUnder(_ context.Context, prefix string, limit int) ([]artifacts.StoredObject, bool, error) {
	w.hook(prefix)
	w.mu.Lock()
	defer w.mu.Unlock()
	if limit <= 0 {
		w.t.Errorf("unbounded listing of %q", prefix)
	}
	if err := w.fail["ListUnder"]; err != nil {
		return nil, false, err
	}
	if err := w.fail["ListUnder "+prefix]; err != nil {
		return nil, false, err
	}
	var keys []string
	if strings.HasPrefix(prefix, artifacts.CommitPrefix+"/") {
		for run, n := range w.commits {
			p, _ := artifacts.RunCommitPrefix("default", run)
			if p == prefix {
				for i := 0; i < n; i++ {
					keys = append(keys, fmt.Sprintf("%s%d/commit.json", p, i))
				}
			}
		}
	} else if strings.HasPrefix(prefix, artifacts.AttestedPrefix+"/") {
		for run, n := range w.attested {
			p, _ := artifacts.AttestedRunPrefix("default", run)
			if p == prefix {
				for i := 0; i < n; i++ {
					keys = append(keys, fmt.Sprintf("%s%d/commit.json", p, i))
				}
			}
		}
	} else {
		for id := range w.objects {
			if strings.HasPrefix(id, prefix) {
				keys = append(keys, id)
			}
		}
	}
	sort.Strings(keys)
	var out []artifacts.StoredObject
	for _, k := range keys {
		if len(out) == limit {
			return out, true, nil
		}
		out = append(out, artifacts.StoredObject{Key: k, Size: w.objects[k]})
	}
	return out, false, nil
}

func (w *world) StatObject(_ context.Context, key string) (artifacts.StoredObject, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["StatObject"]; err != nil {
		return artifacts.StoredObject{}, err
	}
	size, ok := w.objects[key]
	if !ok {
		return artifacts.StoredObject{}, artifacts.ErrNotFound
	}
	return artifacts.StoredObject{Key: key, Size: size}, nil
}

func (w *world) ListObjectReferences(_ context.Context, id string) ([]string, error) {
	w.hook(id)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["ListObjectReferences"]; err != nil {
		return nil, err
	}
	if len(w.markers[id]) > artifacts.MaxObjectReferences {
		return nil, artifacts.ErrTooManyReferences
	}
	var runs []string
	for run := range w.markers[id] {
		runs = append(runs, run)
	}
	sort.Strings(runs)
	return runs, nil
}

func (w *world) RemoveReclaimedObject(_ context.Context, id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["RemoveReclaimedObject"]; err != nil {
		return err
	}
	w.writes++
	delete(w.objects, id)
	w.removed = append(w.removed, id)
	return nil
}

func (w *world) RemoveObjectReferences(_ context.Context, id string, runs []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["RemoveObjectReferences"]; err != nil {
		return err
	}
	w.writes++
	for _, run := range runs {
		delete(w.markers[id], run)
	}
	return nil
}

func (w *world) RemoveRunObjectList(_ context.Context, run string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["RemoveRunObjectList"]; err != nil {
		return err
	}
	w.writes++
	delete(w.lists, run)
	return nil
}

func (w *world) CreateReclaimRecord(_ context.Context, rec artifacts.ReclaimRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["CreateReclaimRecord"]; err != nil {
		return err
	}
	w.writes++
	if _, ok := w.records[rec.RunID]; !ok {
		w.records[rec.RunID] = rec
	}
	return nil
}

func (w *world) EachReclaimRecord(_ context.Context, visit func(string) error) error {
	w.mu.Lock()
	if err := w.fail["EachReclaimRecord"]; err != nil {
		w.mu.Unlock()
		return err
	}
	var runs []string
	for run := range w.records {
		runs = append(runs, run)
	}
	w.mu.Unlock()
	sort.Strings(runs)
	for _, run := range runs {
		if err := visit(run); err != nil {
			return err
		}
	}
	return nil
}

// --- RunBodyReader ---

func (w *world) ReclaimBody(_ context.Context, run string) (RunBody, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["ReclaimBody"]; err != nil {
		return RunBody{}, err
	}
	body, ok := w.bodies[run]
	if !ok {
		return RunBody{}, fmt.Errorf("body of %q: %w", run, artifacts.ErrNotFound)
	}
	return body, nil
}

// --- ReclaimCluster ---

func (w *world) LookupWorkflow(_ context.Context, _, run string) (bool, time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail["LookupWorkflow"]; err != nil {
		return false, time.Time{}, err
	}
	return w.executions[run], time.Time{}, nil
}

func (w *world) WalkLowWater(_ context.Context, _ time.Time) (LowWaterWalk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.walkErr != nil {
		return LowWaterWalk{}, w.walkErr
	}
	walk := LowWaterWalk{Cause: w.cause, Seen: map[string]struct{}{}}
	for run, created := range w.live {
		walk.Chains++
		walk.Seen[run] = struct{}{}
		if walk.Earliest.IsZero() || created.Before(walk.Earliest) {
			walk.Earliest, walk.EarliestRunID = created, run
		}
	}
	for run, created := range w.closed {
		walk.Seen[run] = struct{}{}
		walk.Closed = append(walk.Closed, ClosedRun{RunID: run, Created: created})
	}
	return walk, nil
}

// --- SweepRecord, AcceptedRuns ---

func (w *world) LastCompletedSweep() (SweepPass, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.hasSweep {
		return SweepPass{}, false
	}
	pass := w.sweep
	if pass.Started.IsZero() {
		pass.Started, pass.Finished = w.now.Add(-time.Minute), w.now
	}
	return pass, true
}

func (w *world) NewestAccepted() (string, time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.accepted == nil {
		return "", time.Time{}, false
	}
	return w.accepted.id, w.accepted.at, true
}

// reclaimer builds a GC over the world: grace 1 h, interval 1 h.
func (w *world) reclaimer(mode ReclaimMode) *Reclaimer {
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(w.retention)}}
	retention := NewRetentionController(policies, nil, "ork-main", time.Hour, testLogger())
	r := NewReclaimer(ReclaimConfig{Mode: mode, Grace: time.Hour, Batch: 1000, Interval: time.Hour},
		w, w, w, w, &fakeNamespaces{}, retention, "ork-main", testLogger())
	return r.WithClock(func() time.Time {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.now.Add(w.skew)
	}).WithAcceptedRuns(w).WithResumes(NewEndingRecords(&endingStore{}))
}

// warm runs the first pass after boot, which never deletes, and fails the
// test if it did.
func (w *world) warm(r *Reclaimer) {
	w.t.Helper()
	before := len(w.removed)
	report := r.RunOnce(context.Background())
	if report.Err != nil {
		w.t.Fatalf("warming pass: %v", report.Err)
	}
	if len(w.removed) != before {
		w.t.Fatalf("the warming pass deleted %v", w.removed[before:])
	}
}

func tally(report ReclaimReport, outcome, reason string) int64 {
	var n int64
	for k, v := range report.Objects {
		if k.Outcome == outcome && (reason == "" || k.Reason == reason) {
			n += v.Objects
		}
	}
	return n
}

var errStore = errors.New("store unavailable")

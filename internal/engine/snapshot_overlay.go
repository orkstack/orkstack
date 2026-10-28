package engine

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Snapshot overlays (docs/DEPLOY.md §1, engine.snapshot_overlays;
// docs/STORAGE.md). Every lifecycle event a run publishes names a snapshot
// of the whole run, and for a run of thousands of rows writing each one in
// full is most of what the run writes. A run whose plan enables overlays,
// and which declares more rows than its policy's floor, writes for each
// non-terminal event an OVERLAY instead: {seq}.ovl, the rows that differ
// from the last full snapshot this generation wrote, which it names by URI
// and by the SHA-256 of the bytes it was diffed against. A reader takes at
// most two GETs — the overlay and its base — and verifies the base before it
// uses a row of it.
//
// The diff runs in the snapshot's local activity, live only: workflow code
// decides only WHICH activity runs (the policy, the gate, the run's declared
// rows and the event type, all pinned or derived), and follows the base the
// activity reports from its recorded result. A replay reads the markers and
// never diffs.

// SnapshotOverlayChange is the workflow.GetVersion change id of overlays. It
// is asked LAZILY, at the first non-terminal snapshot write of a run that
// uses overlays, and carried (2.3): a history recorded before it replays its
// DefaultVersion branch, which writes every snapshot in full.
const SnapshotOverlayChange = "snapshot-overlay"

// ActivityWriteSnapshotOrOverlay is the registered name of the local
// activity that writes a non-terminal event's snapshot for a run that uses
// overlays: an overlay when it can, a full snapshot otherwise.
const ActivityWriteSnapshotOrOverlay = "WriteSnapshotOrOverlay"

// SnapshotBase is the last full snapshot a generation wrote for one of its
// events, the SHA-256 of the bytes it wrote, and how many overlays have
// followed it. The zero value — a generation's first write — has none, and
// the write is full.
type SnapshotBase struct {
	URI      string `json:"uri,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Overlays int32  `json:"overlays,omitempty"`
}

// SnapshotWritten is what WriteSnapshotOrOverlay wrote: its URI, and for a
// full snapshot the SHA-256 of its bytes (empty for an overlay).
type SnapshotWritten struct {
	URI    string `json:"uri"`
	SHA256 string `json:"sha256,omitempty"`
	Full   bool   `json:"full,omitempty"`
}

// ErrSnapshotOverlayInvalid reports a snapshot overlay that could not be
// assembled into a run: the overlay or the full snapshot it names is
// missing, unreadable, of another run, or no longer the bytes the overlay
// was written against. It never matches artifacts.ErrNotFound — the run
// exists; its state at that event cannot be read right now — so a client
// sees Unavailable, never a degraded answer.
var ErrSnapshotOverlayInvalid = errors.New("the run's snapshot at this event cannot be assembled")

// DeclaredRows is how many rows a run of wf declares: one per task, plus
// every fan-out's max_items. It decides whether a run uses snapshot
// overlays, at submit (the run tree's largest) and in workflow code (the
// run's own); a pure function of the definition.
func DeclaredRows(wf *orkstackv1.Orkflow) int64 {
	return int64(len(wf.GetTasks())) + int64(dsl.EachItemsDeclared(wf))
}

// overlayPolicyFor is the policy this event's snapshot is written under, and
// whether it is: the plan's policy, valid, for a non-terminal event of a run
// that declares more rows than the policy's floor, once the lazy gate says
// so. A terminal run event is always full — the archive and every later
// read depend on it, and none may depend on overlay support.
func (s *runState) overlayPolicyFor(ctx workflow.Context, eventType orkstackv1.EventType) (OverlayPolicy, bool) {
	policy := s.plan.SnapshotOverlay
	if !policy.valid() || isTerminalRunEventType(eventType) || s.declaredRows <= policy.MinRows {
		return OverlayPolicy{}, false
	}
	return *policy, s.version(ctx, SnapshotOverlayChange, 1) >= 1
}

// snapshotLocalActivityOptions are the options of every event snapshot's
// local activity: one attempt's budget, the whole write's, three attempts.
func snapshotLocalActivityOptions() workflow.LocalActivityOptions {
	return workflow.LocalActivityOptions{
		StartToCloseTimeout:    snapshotAttemptTimeout,
		ScheduleToCloseTimeout: snapshotActivityTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	}
}

// writeEventSnapshot writes the snapshot of the event numbered s.eventSeq,
// of eventType, and returns its URI. Both write sites call it — emit's
// (emitWithSnapshot) and a fan-out's fresh item snapshot (emitItem) — so it
// alone decides which activity runs and alone updates s.snapBase: the
// activity type a non-terminal snapshot records depends only on the rule in
// overlayPolicyFor, never on which site wrote it, and an overlay always
// names the run's last full snapshot, whichever site wrote that.
//
// A local activity is essential here: Temporal does not persist its input,
// while a regular activity would put this complete snapshot straight back
// into ActivityTaskScheduled.input and retain quadratic history growth. Its
// only recorded result is a small reference.
//
// The activity reads the run's live rows, lent to it for the call and taken
// back as soon as Get returns, whatever it returned: Get can return while
// the activity is still encoding (a cancel, an attempt's deadline), and the
// failure path then changes those rows (rows_loan.go).
func (s *runState) writeEventSnapshot(ctx workflow.Context, eventType orkstackv1.EventType) (string, error) {
	localCtx := workflow.WithLocalActivityOptions(ctx, snapshotLocalActivityOptions())
	policy, overlay := s.overlayPolicyFor(ctx, eventType)
	snapshot := s.snapshot()
	loan := lendRows(snapshot)
	if overlay {
		var written SnapshotWritten
		err := workflow.ExecuteLocalActivity(localCtx, ActivityWriteSnapshotOrOverlay,
			snapshot, int64(s.eventSeq), s.snapBase, policy).Get(localCtx, &written)
		loan.takeBack()
		if err != nil {
			return "", err
		}
		s.followSnapshotBase(written)
		return written.URI, nil
	}
	var uri string
	err := workflow.ExecuteLocalActivity(localCtx, ActivityWriteSnapshot, snapshot, int64(s.eventSeq)).
		Get(localCtx, &uri)
	loan.takeBack()
	if err != nil {
		return "", err
	}
	return uri, nil
}

// followSnapshotBase moves s.snapBase by what a write recorded: a full
// snapshot becomes the new base, with no overlay after it; an overlay is
// one more after the same base. A pure function of the markers.
func (s *runState) followSnapshotBase(written SnapshotWritten) {
	if written.Full {
		s.snapBase = SnapshotBase{URI: written.URI, SHA256: written.SHA256}
	} else {
		s.snapBase.Overlays++
	}
	if snapshotBaseHookForTest != nil {
		snapshotBaseHookForTest(s.snapBase)
	}
}

// snapshotBaseHookForTest, when set, sees s.snapBase after every write that
// moved it: how a replay test checks the base follows the recorded results.
// TEST-ONLY (SetSnapshotBaseHookForTest); never set in production.
var snapshotBaseHookForTest func(SnapshotBase)

// ---------------------------------------------------------------- activity

// WriteSnapshotOrOverlay writes one non-terminal event's snapshot of a run
// that uses overlays. It writes a full {seq}.pb, and reports its digest,
// when the generation has no base yet, after policy.MaxChain overlays in a
// row, when the base cannot be read or its bytes no longer hash to the
// digest the run recorded (a failed attempt's late write replaced it), when
// more than policy.MaxRows rows differ, or when the overlay would not
// reproduce the run exactly. Otherwise it writes {seq}.ovl naming the base.
//
// The run it is handed is the workflow's own state, passed in-process, its
// rows lent for the encode only (rows_loan.go): the workflow is blocked on this
// activity while it runs — and when a cancel frees it early, it takes the rows
// back, which waits for the encode to end. So everything the workflow goes on
// changing — the rows and the messages the run points at — is read inside one
// readLent, and nothing in it waits on the network: the base is read before
// it, the object is written after it. The run's namespace and id are the Run's
// own fields, set once when s.snapshot() builds it and never written again, so
// they are read once, here, outside the loan.
func (a *SnapshotActivities) WriteSnapshotOrOverlay(
	ctx context.Context,
	run *orkstackv1.Run,
	sequence int64,
	base SnapshotBase,
	policy OverlayPolicy,
) (SnapshotWritten, error) {
	if run == nil {
		return SnapshotWritten{}, fmt.Errorf("engine: write snapshot: run is required")
	}
	namespace, runID := run.GetNamespace(), run.GetId()
	var baseRun *orkstackv1.Run
	if base.URI != "" && int64(base.Overlays) < policy.MaxChain &&
		artifacts.OverlayBaseURIMatches(base.URI, namespace, runID) {
		baseRun = a.verifiedBase(ctx, base)
	}
	encoded, err := readLent(run, sequence, func() (encodedSnapshot, error) {
		return encodeSnapshot(run, baseRun, base, policy)
	})
	if err != nil {
		return SnapshotWritten{}, err
	}
	if encoded.overlay {
		uri, err := a.store.WriteSnapshotOverlay(ctx, namespace, runID, sequence, encoded.data)
		if err != nil {
			return SnapshotWritten{}, fmt.Errorf("engine: persist snapshot overlay for run %s: %w", runID, err)
		}
		return SnapshotWritten{URI: uri}, nil
	}
	uri, err := a.store.WriteSnapshot(ctx, namespace, runID, sequence, encoded.data)
	if err != nil {
		return SnapshotWritten{}, fmt.Errorf("engine: persist snapshot for run %s: %w", runID, err)
	}
	sum := artifacts.ContinuationSHA256(encoded.data)
	a.bases.put(uri, sum, encoded.data)
	return SnapshotWritten{URI: uri, SHA256: sum, Full: true}, nil
}

// encodedSnapshot is a snapshot's bytes, and whether they are an overlay.
type encodedSnapshot struct {
	data    []byte
	overlay bool
}

// encodeSnapshot is the bytes of run's snapshot: an overlay against baseRun
// (the verified full snapshot base names) when there is one and the overlay
// reproduces run exactly, else the full run. It reads run's rows and nothing
// else; callers hold them on loan.
func encodeSnapshot(run, baseRun *orkstackv1.Run, base SnapshotBase, policy OverlayPolicy) (encodedSnapshot, error) {
	if baseRun != nil {
		if overlay, ok := diffSnapshot(baseRun, run, base, policy.MaxRows); ok {
			data, err := proto.Marshal(overlay)
			if err != nil {
				return encodedSnapshot{}, fmt.Errorf("engine: marshal snapshot overlay for run %s: %w", run.GetId(), err)
			}
			return encodedSnapshot{data: data, overlay: true}, nil
		}
	}
	data, err := proto.Marshal(run)
	if err != nil {
		return encodedSnapshot{}, fmt.Errorf("engine: marshal snapshot for run %s: %w", run.GetId(), err)
	}
	return encodedSnapshot{data: data}, nil
}

// verifiedBase is the full snapshot base names, decoded, from the base cache
// or read and verified against base.SHA256; nil when it cannot be had —
// gone, unreadable, or no longer those bytes — and the caller then writes a
// full snapshot, starting a new chain from bytes it knows.
func (a *SnapshotActivities) verifiedBase(ctx context.Context, base SnapshotBase) *orkstackv1.Run {
	if run := a.bases.get(base.URI, base.SHA256); run != nil {
		return run
	}
	data, err := a.store.ReadSnapshot(ctx, base.URI)
	if err != nil || artifacts.ContinuationSHA256(data) != base.SHA256 {
		return nil
	}
	return a.bases.put(base.URI, base.SHA256, data)
}

// overlayDiffs counts diffSnapshot's calls, process-wide: the evidence that
// a replay — which reads the activity's marker — never diffs
// (TestSnapshotOverlay_DiffRunsInTheLocalActivityOnly).
var overlayDiffs atomic.Int64

// itemKey is an item row's key: its fan-out's task id and its index.
type itemKey struct {
	task string
	item int32
}

// diffSnapshot is the overlay of run against base: the header, every task
// row (items cleared) and every item row that is not proto.Equal to the
// base's row with the same key. It reports false — write a full snapshot —
// when base is another run's, when more than maxRows rows differ, or when
// the overlay applied to base is not exactly run.
func diffSnapshot(base, run *orkstackv1.Run, ref SnapshotBase, maxRows int64) (*orkstackv1.RunSnapshotOverlay, bool) {
	overlayDiffs.Add(1)
	if base.GetId() != run.GetId() || base.GetNamespace() != run.GetNamespace() {
		return nil, false
	}
	baseRows := make(map[string]*orkstackv1.TaskRun, len(base.GetTaskRuns()))
	baseItems := map[itemKey]*orkstackv1.TaskRun{}
	for _, row := range base.GetTaskRuns() {
		baseRows[row.GetTaskId()] = row
		for _, item := range row.GetItems() {
			baseItems[itemKey{row.GetTaskId(), item.GetItem()}] = item
		}
	}
	overlay := &orkstackv1.RunSnapshotOverlay{BaseUri: ref.URI, BaseSha256: ref.SHA256, Header: runHeader(run)}
	changed := int64(0)
	for _, row := range run.GetTaskRuns() {
		header := rowHeader(row)
		if b, ok := baseRows[row.GetTaskId()]; !ok || !proto.Equal(rowHeader(b), header) {
			overlay.TaskRuns = append(overlay.TaskRuns, header)
			changed++
		}
		for _, item := range row.GetItems() {
			if b, ok := baseItems[itemKey{row.GetTaskId(), item.GetItem()}]; !ok || !proto.Equal(b, item) {
				overlay.Items = append(overlay.Items, item)
				changed++
			}
		}
		if changed > maxRows {
			return nil, false
		}
	}
	if !proto.Equal(ApplySnapshotOverlay(base, overlay), run) {
		return nil, false
	}
	return overlay, true
}

// ApplySnapshotOverlay is the run overlay describes over base: the overlay's
// header; base's task rows in base order, each replaced by the overlay's
// row with the same task id; each row's items in base order, each replaced
// by the overlay's item with the same (task id, index); an overlay item no
// base row holds appended to its task's items, and an overlay task row no
// base holds appended to the rows, in overlay order. It shares messages with
// both arguments and copies none: a caller that hands the result on clones
// it first.
func ApplySnapshotOverlay(base *orkstackv1.Run, overlay *orkstackv1.RunSnapshotOverlay) *orkstackv1.Run {
	out := &orkstackv1.Run{}
	if header := overlay.GetHeader(); header != nil {
		out = shallowCopy(header, out, runTaskRunsField)
	}
	rows := make(map[string]*orkstackv1.TaskRun, len(overlay.GetTaskRuns()))
	for _, row := range overlay.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	items := make(map[itemKey]*orkstackv1.TaskRun, len(overlay.GetItems()))
	extra := map[string][]*orkstackv1.TaskRun{}
	baseItems := map[itemKey]bool{}
	baseRows := make(map[string]bool, len(base.GetTaskRuns()))
	for _, row := range base.GetTaskRuns() {
		baseRows[row.GetTaskId()] = true
		for _, item := range row.GetItems() {
			baseItems[itemKey{row.GetTaskId(), item.GetItem()}] = true
		}
	}
	for _, item := range overlay.GetItems() {
		key := itemKey{item.GetTaskId(), item.GetItem()}
		items[key] = item
		if !baseItems[key] {
			extra[item.GetTaskId()] = append(extra[item.GetTaskId()], item)
		}
	}
	assemble := func(header *orkstackv1.TaskRun, baseRow *orkstackv1.TaskRun) *orkstackv1.TaskRun {
		var merged []*orkstackv1.TaskRun
		for _, item := range baseRow.GetItems() {
			if replaced, ok := items[itemKey{baseRow.GetTaskId(), item.GetItem()}]; ok {
				merged = append(merged, replaced)
			} else {
				merged = append(merged, item)
			}
		}
		merged = append(merged, extra[header.GetTaskId()]...)
		return rowWithItems(header, merged)
	}
	for _, b := range base.GetTaskRuns() {
		header := rowHeader(b)
		if replaced, ok := rows[b.GetTaskId()]; ok {
			header = replaced
		}
		out.TaskRuns = append(out.TaskRuns, assemble(header, b))
	}
	for _, row := range overlay.GetTaskRuns() {
		if !baseRows[row.GetTaskId()] {
			out.TaskRuns = append(out.TaskRuns, assemble(row, nil))
		}
	}
	return out
}

// The fields the shallow copies leave out, read from the descriptors.
var (
	runTaskRunsField  = (&orkstackv1.Run{}).ProtoReflect().Descriptor().Fields().ByName("task_runs").Number()
	taskRunItemsField = (&orkstackv1.TaskRun{}).ProtoReflect().Descriptor().Fields().ByName("items").Number()
)

// shallowCopy copies every populated field of src except skip into a new
// message of the same type, sharing nested messages and lists: read-only.
func shallowCopy[M proto.Message](src M, fresh M, skip protoreflect.FieldNumber) M {
	dst := fresh.ProtoReflect()
	src.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Number() != skip {
			dst.Set(fd, v)
		}
		return true
	})
	return fresh
}

// runHeader is run without its task rows: run itself when it has none.
func runHeader(run *orkstackv1.Run) *orkstackv1.Run {
	if run == nil || len(run.GetTaskRuns()) == 0 {
		return run
	}
	return shallowCopy(run, &orkstackv1.Run{}, runTaskRunsField)
}

// rowHeader is row without its items: row itself when it has none.
func rowHeader(row *orkstackv1.TaskRun) *orkstackv1.TaskRun {
	if row == nil || len(row.GetItems()) == 0 {
		return row
	}
	return shallowCopy(row, &orkstackv1.TaskRun{}, taskRunItemsField)
}

// rowWithItems is header with exactly items: header itself when both have
// none.
func rowWithItems(header *orkstackv1.TaskRun, items []*orkstackv1.TaskRun) *orkstackv1.TaskRun {
	if len(items) == 0 {
		return rowHeader(header)
	}
	row := shallowCopy(header, &orkstackv1.TaskRun{}, taskRunItemsField)
	row.Items = items
	return row
}

// ---------------------------------------------------------------- readers

// materializeSnapshot reads the snapshot at uri as the Run it stands for. A
// {seq}.pb, a switch snapshot or a checkpoint is read as before — this is
// only the plain-snapshot path, so a missing one still matches
// artifacts.ErrNotFound. A {seq}.ovl is read, its base is checked to be a
// full snapshot of the same run under the same root
// (artifacts.OverlayBaseURIMatches) and read through bases by (URI, SHA-256)
// — verified against the digest on a miss — and its rows replace the
// base's by key: at most two GETs. Every failure of an overlay, its own
// object missing included, wraps ErrSnapshotOverlayInvalid and never
// matches NotFound. The Run returned is the caller's own.
func materializeSnapshot(ctx context.Context, store SnapshotStore, bases *SnapshotBaseCache, uri string) (*orkstackv1.Run, error) {
	data, err := store.ReadSnapshot(ctx, uri)
	if !artifacts.IsSnapshotOverlayURI(uri) {
		if err != nil {
			return nil, err
		}
		run := &orkstackv1.Run{}
		if err := proto.Unmarshal(data, run); err != nil {
			return nil, fmt.Errorf("decode snapshot %s: %w", uri, err)
		}
		return run, nil
	}
	if err != nil {
		return nil, overlayInvalid("its snapshot could not be read: %v", err)
	}
	overlay := &orkstackv1.RunSnapshotOverlay{}
	if err := proto.Unmarshal(data, overlay); err != nil {
		return nil, overlayInvalid("its snapshot does not decode: %v", err)
	}
	header := overlay.GetHeader()
	baseURI := overlay.GetBaseUri()
	if header.GetId() == "" || !artifacts.OverlayBaseURIMatches(baseURI, header.GetNamespace(), header.GetId()) ||
		uriDir(baseURI) != uriDir(uri) {
		return nil, overlayInvalid("its snapshot names a base that is not a full snapshot of this run")
	}
	base := bases.get(baseURI, overlay.GetBaseSha256())
	if base == nil {
		baseData, err := store.ReadSnapshot(ctx, baseURI)
		if err != nil {
			return nil, overlayInvalid("the full snapshot it is written against could not be read: %v", err)
		}
		if artifacts.ContinuationSHA256(baseData) != overlay.GetBaseSha256() {
			return nil, overlayInvalid("the full snapshot it is written against has changed since")
		}
		if base = bases.put(baseURI, overlay.GetBaseSha256(), baseData); base == nil {
			return nil, overlayInvalid("the full snapshot it is written against does not decode")
		}
	}
	if base.GetId() != header.GetId() || base.GetNamespace() != header.GetNamespace() {
		return nil, overlayInvalid("its snapshot names a base of another run")
	}
	return proto.Clone(ApplySnapshotOverlay(base, overlay)).(*orkstackv1.Run), nil
}

// overlayInvalid is one assembly failure; its text never wraps the store's
// error, so it can never read as NotFound.
func overlayInvalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrSnapshotOverlayInvalid, fmt.Sprintf(format, args...))
}

// uriDir is uri up to and including its last slash.
func uriDir(uri string) string {
	return uri[:strings.LastIndex(uri, "/")+1]
}

// ---------------------------------------------------------------- base cache

// The base cache's bounds: at most this many bases, holding at most this
// many encoded bytes together, per engine process (docs/DEPLOY.md §5).
const (
	snapshotBaseCacheEntries       = 8
	snapshotBaseCacheBytes   int64 = 128 << 20
)

// SnapshotBaseCache is one engine process's LRU of full snapshots that
// overlays name, keyed by (URI, SHA-256), shared by the writing activity and
// the readers (the projection worker, the run reader). An entry is the
// verified bytes of that digest, kept decoded, so it is never stale: a
// rewritten {seq}.pb has another digest and is another key. A disposable
// process-local cache (AGENTS.md §13): a miss reads the object store. The
// decoded run is shared read-only; nothing that holds one mutates it. A nil
// cache caches nothing.
type SnapshotBaseCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	bytes      int64
	order      *list.List // front is the most recently used
	entries    map[baseCacheKey]*list.Element
}

type baseCacheKey struct{ uri, sha256 string }

type baseCacheEntry struct {
	key  baseCacheKey
	size int64 // the encoded bytes
	run  *orkstackv1.Run
}

// NewSnapshotBaseCache returns a cache of at most 8 bases or 128 MiB.
func NewSnapshotBaseCache() *SnapshotBaseCache {
	return newSnapshotBaseCache(snapshotBaseCacheEntries, snapshotBaseCacheBytes)
}

func newSnapshotBaseCache(entries int, bytes int64) *SnapshotBaseCache {
	return &SnapshotBaseCache{maxEntries: entries, maxBytes: bytes, order: list.New(),
		entries: map[baseCacheKey]*list.Element{}}
}

// get is the decoded base at (uri, sha256), or nil.
func (c *SnapshotBaseCache) get(uri, sha256 string) *orkstackv1.Run {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[baseCacheKey{uri, sha256}]
	if !ok {
		return nil
	}
	c.order.MoveToFront(element)
	return element.Value.(*baseCacheEntry).run
}

// put keeps data — bytes the caller verified hash to sha256 — decoded, as
// the base at (uri, sha256), evicting the least recently used until both
// bounds hold (counted in encoded bytes), and returns it (nil when it does
// not decode). A base larger than the byte bound is returned, never kept.
func (c *SnapshotBaseCache) put(uri, sha256 string, data []byte) *orkstackv1.Run {
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		return nil
	}
	if c == nil {
		return run
	}
	size := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	key := baseCacheKey{uri, sha256}
	if element, ok := c.entries[key]; ok {
		c.order.MoveToFront(element)
		return element.Value.(*baseCacheEntry).run
	}
	if size > c.maxBytes {
		return run
	}
	for c.order.Len() > 0 && (c.order.Len() >= c.maxEntries || c.bytes+size > c.maxBytes) {
		c.remove(c.order.Back())
	}
	c.entries[key] = c.order.PushFront(&baseCacheEntry{key: key, size: size, run: run})
	c.bytes += size
	return run
}

func (c *SnapshotBaseCache) remove(element *list.Element) {
	entry := c.order.Remove(element).(*baseCacheEntry)
	delete(c.entries, entry.key)
	c.bytes -= entry.size
}

// len and size are the cache's entry count and bytes (tests).
func (c *SnapshotBaseCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *SnapshotBaseCache) size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

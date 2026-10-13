package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The archive body is JSON Lines. Line one is the run's terminal snapshot;
// every line after it is one event, in sequence order:
//
//	{"kind":"run","run":{"id":"ork-…","status":"STATUS_SUCCEEDED",…}}
//	{"kind":"event","event":{"runId":"ork-…","sequence":"1",…}}
//	{"kind":"event","event":{"runId":"ork-…","sequence":"2",…}}
//
// Events alone cannot rebuild a run — they carry no orkflow name, no task
// outputs and no artifacts — so the snapshot leads the file. Reading the first
// line is enough to answer `run get`; reading the rest replays the run's whole
// history. Each line is canonical protojson compacted to a single line, so the
// body round-trips exactly and stays greppable.
//
// A run whose stream lost part of itself before it was archived — an earlier
// generation's event segment missing from object storage, or altered — has
// one gap line per lost range, in sequence order among the events, so the
// archive records the loss explicitly rather than dropping the run:
//
//	{"kind":"gap","gap":{"first":1201,"last":2400,"reason":"segment object missing"}}
const (
	// ArchiveKindRun marks the snapshot line.
	ArchiveKindRun = "run"
	// ArchiveKindEvent marks an event line.
	ArchiveKindEvent = "event"
	// ArchiveKindGap marks a line recording events lost before archiving.
	ArchiveKindGap = "gap"
	// DefaultArchivePageSize bounds an unlimited archive query, so a list never
	// fetches an unbounded number of bodies.
	DefaultArchivePageSize = 200
	// archiveFetchConcurrency bounds parallel body reads for one query.
	archiveFetchConcurrency = 8
)

// archiveLine is one JSONL record. The payloads stay raw so protojson owns the
// encoding of every protobuf message.
type archiveLine struct {
	Kind  string          `json:"kind"`
	Run   json.RawMessage `json:"run,omitempty"`
	Event json.RawMessage `json:"event,omitempty"`
	Gap   *EventGap       `json:"gap,omitempty"`
}

// EventGap is a range of a run's event sequence that was lost from object
// storage before the run was archived: an earlier generation's event segment
// that was missing or failed verification. First and Last are inclusive.
type EventGap struct {
	First  int64  `json:"first"`
	Last   int64  `json:"last"`
	Reason string `json:"reason"`
}

// ErrArchivedEventsLost reports an archived run whose stream records a gap.
// The run's outcome and its other events are intact.
var ErrArchivedEventsLost = errors.New("events of the run were lost from object storage before it was archived")

// ArchivedEventsLostError names every range an archived run lost. It wraps
// ErrArchivedEventsLost.
type ArchivedEventsLostError struct {
	RunID string
	Gaps  []EventGap
}

func (e *ArchivedEventsLostError) Error() string {
	ranges := make([]string, 0, len(e.Gaps))
	for _, gap := range e.Gaps {
		ranges = append(ranges, fmt.Sprintf("%d–%d (%s)", gap.First, gap.Last, gap.Reason))
	}
	return fmt.Sprintf("events %s of run %s were lost from object storage before the run was archived",
		strings.Join(ranges, ", "), e.RunID)
}

func (e *ArchivedEventsLostError) Unwrap() error { return ErrArchivedEventsLost }

// MarshalRunArchive renders a run and its events as the archive body, with
// one gap line per lost range, placed after the events that precede it.
func MarshalRunArchive(run *orkstackv1.Run, events []*orkstackv1.Event, gaps ...EventGap) ([]byte, error) {
	if run.GetId() == "" {
		return nil, fmt.Errorf("engine: archive run: run id is required")
	}
	var body bytes.Buffer
	runJSON, err := marshalArchiveMessage(run)
	if err != nil {
		return nil, fmt.Errorf("engine: archive run %q: %w", run.GetId(), err)
	}
	if err := appendArchiveLine(&body, archiveLine{Kind: ArchiveKindRun, Run: runJSON}); err != nil {
		return nil, fmt.Errorf("engine: archive run %q: %w", run.GetId(), err)
	}
	pending := append([]EventGap(nil), gaps...)
	sort.SliceStable(pending, func(i, j int) bool { return pending[i].First < pending[j].First })
	writeGapsBefore := func(sequence int64) error {
		for len(pending) > 0 && pending[0].First < sequence {
			gap := pending[0]
			pending = pending[1:]
			if err := appendArchiveLine(&body, archiveLine{Kind: ArchiveKindGap, Gap: &gap}); err != nil {
				return fmt.Errorf("engine: archive run %q: gap %d–%d: %w", run.GetId(), gap.First, gap.Last, err)
			}
		}
		return nil
	}
	for _, event := range events {
		if err := writeGapsBefore(event.GetSequence()); err != nil {
			return nil, err
		}
		eventJSON, err := marshalArchiveMessage(event)
		if err != nil {
			return nil, fmt.Errorf("engine: archive run %q: event %d: %w",
				run.GetId(), event.GetSequence(), err)
		}
		if err := appendArchiveLine(&body, archiveLine{Kind: ArchiveKindEvent, Event: eventJSON}); err != nil {
			return nil, fmt.Errorf("engine: archive run %q: event %d: %w",
				run.GetId(), event.GetSequence(), err)
		}
	}
	if err := writeGapsBefore(math.MaxInt64); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

// UnmarshalRunArchiveGaps returns the gap lines of an archive body, in the
// order they were written; nil for a body that lost nothing.
func UnmarshalRunArchiveGaps(body []byte) ([]EventGap, error) {
	var gaps []EventGap
	for number, raw := range bytes.Split(body, []byte("\n")) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		var record archiveLine
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("engine: read archive line %d: %w", number+1, err)
		}
		if record.Kind != ArchiveKindGap {
			continue
		}
		if record.Gap == nil || record.Gap.First < 1 || record.Gap.Last < record.Gap.First {
			return nil, fmt.Errorf("engine: read archive line %d: malformed gap", number+1)
		}
		gaps = append(gaps, *record.Gap)
	}
	return gaps, nil
}

// UnmarshalRunArchive reverses MarshalRunArchive. A body with no run line is
// rejected: the archive would not be able to answer `run get`.
func UnmarshalRunArchive(body []byte) (*orkstackv1.Run, []*orkstackv1.Event, error) {
	var (
		run    *orkstackv1.Run
		events []*orkstackv1.Event
		opts   = protojson.UnmarshalOptions{DiscardUnknown: true}
	)
	for number, raw := range bytes.Split(body, []byte("\n")) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		var record archiveLine
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, nil, fmt.Errorf("engine: read archive line %d: %w", number+1, err)
		}
		switch record.Kind {
		case ArchiveKindRun:
			decoded := &orkstackv1.Run{}
			if err := opts.Unmarshal(record.Run, decoded); err != nil {
				return nil, nil, fmt.Errorf("engine: read archive line %d: run: %w", number+1, err)
			}
			run = decoded
		case ArchiveKindEvent:
			decoded := &orkstackv1.Event{}
			if err := opts.Unmarshal(record.Event, decoded); err != nil {
				return nil, nil, fmt.Errorf("engine: read archive line %d: event: %w", number+1, err)
			}
			events = append(events, decoded)
		case ArchiveKindGap:
			// Read by UnmarshalRunArchiveGaps; the run and its events are
			// whole without it.
		default:
			return nil, nil, fmt.Errorf("engine: read archive line %d: unknown kind %q", number+1, record.Kind)
		}
	}
	if run == nil {
		return nil, nil, fmt.Errorf("engine: read archive: no %q line", ArchiveKindRun)
	}
	return run, events, nil
}

// UnmarshalRunLine decodes only the archive body's run line: what `run get`
// and a list page need. MarshalRunArchive writes it first, so the read stops
// at line one and no event line is decoded; a body whose run line is not the
// first is still read correctly, with every line before it skipped unparsed
// beyond its kind. A body with no run line is the error UnmarshalRunArchive
// returns.
func UnmarshalRunLine(body []byte) (*orkstackv1.Run, error) {
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	for number := 1; len(body) > 0; number++ {
		raw := body
		if i := bytes.IndexByte(body, '\n'); i >= 0 {
			raw, body = body[:i], body[i+1:]
		} else {
			body = nil
		}
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		var kind struct {
			Kind string          `json:"kind"`
			Run  json.RawMessage `json:"run,omitempty"`
		}
		if err := json.Unmarshal(line, &kind); err != nil {
			return nil, fmt.Errorf("engine: read archive line %d: %w", number, err)
		}
		switch kind.Kind {
		case ArchiveKindRun:
			decoded := &orkstackv1.Run{}
			if err := opts.Unmarshal(kind.Run, decoded); err != nil {
				return nil, fmt.Errorf("engine: read archive line %d: run: %w", number, err)
			}
			return decoded, nil
		case ArchiveKindEvent, ArchiveKindGap:
		default:
			return nil, fmt.Errorf("engine: read archive line %d: unknown kind %q", number, kind.Kind)
		}
	}
	return nil, fmt.Errorf("engine: read archive: no %q line", ArchiveKindRun)
}

func appendArchiveLine(out *bytes.Buffer, line archiveLine) error {
	encoded, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("encode %s line: %w", line.Kind, err)
	}
	out.Write(encoded)
	out.WriteByte('\n')
	return nil
}

// marshalArchiveMessage renders one protobuf message as compact single-line
// JSON. protojson deliberately varies its whitespace, so the output is
// re-compacted: an archive line must be one line, and identical input must
// produce identical bytes.
func marshalArchiveMessage(m proto.Message) (json.RawMessage, error) {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode %T: %w", m, err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("compact %T: %w", m, err)
	}
	return compact.Bytes(), nil
}

// ArchiveQuery selects archived runs. Zero values mean "no filter".
type ArchiveQuery struct {
	// Namespace selects one namespace. It is the index key's outermost
	// segment, so naming one narrows the scan instead of filtering it.
	Namespace   string
	OrkflowName string
	Status      orkstackv1.Status
	// Since is the inclusive lower bound, Until the exclusive upper bound, on
	// the run's creation time.
	Since         time.Time
	Until         time.Time
	BeforeCreated time.Time
	BeforeID      string
	// OriginKind selects the origin index instead of the run index; OriginID
	// narrows it to one originating object (an origin.kind and origin.id
	// equals filter). A run archived with no origin is matched by neither.
	OriginKind orkstackv1.RunOriginKind
	OriginID   string
	// Keep is the residual filter set. It is evaluated on indexProjection of
	// each index entry during the key scan, so a key it rejects is never
	// hydrated; nil keeps every entry the fields above keep.
	Keep func(*orkstackv1.Run) bool
	// Limit caps the runs returned, newest first. 0 means DefaultArchivePageSize
	// unless Exhaustive is set, in which case zero means every matching entry.
	Limit      int
	Exhaustive bool
}

// RunArchiver is the engine's view of the run archive: it owns the protobuf
// encoding, while pkg/artifacts owns object keys, prefix scans and storage.
type RunArchiver struct {
	archive artifacts.RunArchive
}

// NewRunArchiver wraps a storage archive. A nil archive yields a nil archiver,
// which every caller treats as "archiving is switched off".
func NewRunArchiver(archive artifacts.RunArchive) *RunArchiver {
	if archive == nil {
		return nil
	}
	return &RunArchiver{archive: archive}
}

// Archive writes a finished run and all of its events. It is safe to call more
// than once for the same run: the stored body is immutable, so a retry, a
// replay or a second writer converges on the first archived copy.
//
// The index time comes from the run itself (created, else started, else
// finished), never from the clock, so repeated calls address the same keys.
//
// gaps are the ranges of the stream lost before archiving (EventsForArchive);
// each becomes a gap line, so the archive says what it is missing.
func (a *RunArchiver) Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event, gaps ...EventGap) error {
	if a == nil {
		return nil
	}
	if run.GetOrkflowName() == "" {
		return fmt.Errorf("engine: archive run %q: orkflow name is required", run.GetId())
	}
	body, err := MarshalRunArchive(run, events, gaps...)
	if err != nil {
		return err
	}
	record := artifacts.RunRecord{
		RunID:     run.GetId(),
		Namespace: run.GetNamespace(),
		Orkflow:   run.GetOrkflowName(),
		Status:    archiveStatus(run.GetStatus()),
		Time:      RunIndexTime(run),
		JSONL:     body,
		ObjectIDs: runObjectIDs(run),
		// The origin row is written from the snapshot's own origin, so the
		// index carries exactly what the archived body says.
		OriginKind: run.GetOrigin().GetKind(),
		OriginID:   run.GetOrigin().GetId(),
	}
	if record.Status == "" {
		return fmt.Errorf("engine: archive run %q: status is unspecified", run.GetId())
	}
	if err := a.archive.PutRun(ctx, record); err != nil {
		return fmt.Errorf("engine: archive run %q: %w", run.GetId(), err)
	}
	return nil
}

func runObjectIDs(run *orkstackv1.Run) []string {
	var ids []string
	appendRef := func(ref *orkstackv1.ArtifactRef) {
		if objectID, ok := artifacts.ObjectIDFromURI(ref.GetUri()); ok {
			ids = append(ids, objectID)
		}
	}
	appendRow := func(taskRun *orkstackv1.TaskRun) {
		for _, ref := range taskRun.GetArtifacts() {
			appendRef(ref)
		}
		for _, ref := range taskRun.GetSystemArtifacts() {
			appendRef(ref)
		}
		appendRef(taskRun.GetValuesRef())
	}
	for _, taskRun := range run.GetTaskRuns() {
		appendRow(taskRun)
		// A fan-out's items hold the references; the node holds none. Every
		// item's objects get their markers, so provenance and the cleanup
		// worker's reference accounting see them (docs/DSL.md "Fan-out").
		for _, item := range taskRun.GetItems() {
			appendRow(item)
		}
	}
	return ids
}

// ErrNoObjectIndex reports an archive backend that cannot list object
// references. Neither shipped backend is one; a fake may be.
var ErrNoObjectIndex = fmt.Errorf("run archive has no object reference index")

// ObjectReferences lists the run ids holding a marker on objectID: the
// reverse index PutRun maintains for cleanup refcounting, read keys only.
//
// A nil archiver is ErrNoRunArchive, not an empty list, on TallyRuns's terms:
// "no archive" and "no reference" are opposite facts and must not render
// alike.
func (a *RunArchiver) ObjectReferences(ctx context.Context, objectID string) ([]string, error) {
	if a == nil {
		return nil, ErrNoRunArchive
	}
	lister, ok := a.archive.(artifacts.ObjectReferenceLister)
	if !ok {
		return nil, ErrNoObjectIndex
	}
	return lister.ListObjectReferences(ctx, objectID)
}

// CleanupState returns a run's immutable cleanup record, or an error
// matching artifacts.ErrNotFound when cleanup has not released it.
func (a *RunArchiver) CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error) {
	if a == nil {
		return artifacts.RunCleanupState{}, ErrNoRunArchive
	}
	return a.archive.CleanupState(ctx, runID)
}

// Load returns the archived run and its events.
func (a *RunArchiver) Load(ctx context.Context, runID string) (*orkstackv1.Run, []*orkstackv1.Event, error) {
	if a == nil {
		return nil, nil, fmt.Errorf("engine: load run %q: %w", runID, artifacts.ErrNotFound)
	}
	body, err := a.archive.GetRun(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	run, events, err := UnmarshalRunArchive(body)
	if err != nil {
		return nil, nil, fmt.Errorf("engine: load run %q: %w", runID, err)
	}
	return run, events, nil
}

// LoadRunBody returns the archive body exactly as stored: the JSONL document
// MarshalRunArchive wrote, one run line then one line per event. It is Load's
// raw half, for a reader that must carry the body byte-for-byte (run evidence)
// rather than re-marshal what Load decoded. A nil archiver and an unknown run
// both answer an error matching artifacts.ErrNotFound, as Load does.
func (a *RunArchiver) LoadRunBody(ctx context.Context, runID string) ([]byte, error) {
	if a == nil {
		return nil, fmt.Errorf("engine: load run body %q: %w", runID, artifacts.ErrNotFound)
	}
	return a.archive.GetRun(ctx, runID)
}

// LoadRun returns just the archived run snapshot. It decodes the body's run
// line only (UnmarshalRunLine): the event lines are fetched with the body but
// never decoded.
func (a *RunArchiver) LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	body, err := a.LoadRunBody(ctx, runID)
	if err != nil {
		return nil, err
	}
	run, err := UnmarshalRunLine(body)
	if err != nil {
		return nil, fmt.Errorf("engine: load run %q: %w", runID, err)
	}
	return run, nil
}

// LoadRunEvents returns just the archived event log, in sequence order. When
// the body records gaps it returns the events it holds AND an
// *ArchivedEventsLostError (matching ErrArchivedEventsLost) naming every
// range, so no caller can mistake a gapped stream for a whole one.
func (a *RunArchiver) LoadRunEvents(ctx context.Context, runID string) ([]*orkstackv1.Event, error) {
	body, err := a.LoadRunBody(ctx, runID)
	if err != nil {
		return nil, err
	}
	_, events, err := UnmarshalRunArchive(body)
	if err != nil {
		return nil, fmt.Errorf("engine: load run %q: %w", runID, err)
	}
	gaps, err := UnmarshalRunArchiveGaps(body)
	if err != nil {
		return nil, fmt.Errorf("engine: load run %q: %w", runID, err)
	}
	if len(gaps) > 0 {
		return events, &ArchivedEventsLostError{RunID: runID, Gaps: gaps}
	}
	return events, nil
}

// ListRuns answers a query: one scan over index keys, then one body read per
// run on the resulting page, decoding only each body's run line. Filtering
// never reads a body. A bounded page of one namespace walks the newest-first
// index and stops at the page, so its cost is set by the page size, not by how
// much history the namespace holds.
func (a *RunArchiver) ListRuns(ctx context.Context, q ArchiveQuery) ([]*orkstackv1.Run, error) {
	if a == nil {
		return nil, nil
	}
	limit := q.Limit
	if limit <= 0 && !q.Exhaustive {
		limit = DefaultArchivePageSize
	}
	entries, err := a.archive.ListRuns(ctx, artifacts.RunQuery{
		Namespace:   q.Namespace,
		Orkflow:     q.OrkflowName,
		Status:      archiveStatus(q.Status),
		Since:       q.Since,
		Until:       q.Until,
		BeforeTime:  q.BeforeCreated,
		BeforeRunID: q.BeforeID,
		OriginKind:  q.OriginKind,
		OriginID:    q.OriginID,
		Keep:        archiveKeep(q.Keep),
		Limit:       limit,
		// A bounded page of one namespace walks the newest-first family and
		// stops at the page; the backend keeps the narrower family where one
		// serves the query better (artifacts.RunQuery.Recent).
		Recent: true,
	})
	if err != nil {
		return nil, err
	}

	runs := make([]*orkstackv1.Run, len(entries))
	errs := make([]error, len(entries))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, archiveFetchConcurrency)
	)
	for i, entry := range entries {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, runID string) {
			defer wg.Done()
			defer func() { <-slots }()
			runs[i], errs[i] = a.LoadRun(ctx, runID)
		}(i, entry.RunID)
	}
	wg.Wait()

	out := make([]*orkstackv1.Run, 0, len(entries))
	for i, run := range runs {
		if errs[i] != nil {
			// An index key whose body is missing is the crash window between
			// the two writes, not a query failure: skip it.
			if errors.Is(errs[i], artifacts.ErrNotFound) {
				continue
			}
			return nil, errs[i]
		}
		out = append(out, run)
	}
	return out, nil
}

// archiveKeep adapts a residual run filter to the index scan: the filter sees
// indexProjection(entry), never a body.
func archiveKeep(keep func(*orkstackv1.Run) bool) func(artifacts.RunIndexEntry) bool {
	if keep == nil {
		return nil
	}
	return func(entry artifacts.RunIndexEntry) bool { return keep(indexProjection(entry)) }
}

// indexProjection is the Run an archive index key describes: its id,
// namespace, Orkflow, status (parsed from the key's lowercase segment),
// created_at (the key's stamp, which the archiver wrote from RunIndexTime, so
// it is already RunFilterView's created_at) and, when the entry came from the
// origin index, its origin kind and id. A key whose status names no Status
// projects UNSPECIFIED, which no status filter selects.
func indexProjection(entry artifacts.RunIndexEntry) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:          entry.RunID,
		Namespace:   entry.Namespace,
		OrkflowName: entry.Orkflow,
		Status:      orkstackv1.Status(orkstackv1.Status_value["STATUS_"+strings.ToUpper(entry.Status)]),
	}
	if !entry.Time.IsZero() {
		run.CreatedAt = timestamppb.New(entry.Time)
	}
	if entry.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
		run.Origin = &orkstackv1.RunOrigin{Kind: entry.OriginKind, Id: entry.OriginID}
	}
	return run
}

// archiveStatus renders a Status the way an index key spells it: lowercase and
// unprefixed. An unspecified status renders empty, which means "any".
func archiveStatus(status orkstackv1.Status) string {
	if status == orkstackv1.Status_STATUS_UNSPECIFIED {
		return ""
	}
	return artifacts.NormalizeRunStatus(status.String())
}

// RunIndexTime is the instant a run is ordered and indexed by: when it was
// created, so a date range means "runs submitted in this window". It never
// reads the clock — a retried archive write must address the same index key as
// the first attempt, and list ordering must not drift between calls.
//
// Truncated to microseconds because the SAME run reports two different
// precisions depending on which source answers: freshly submitted, its
// CreatedAt carries the full nanosecond value ork stamped at SubmitRun; once
// archived, the value has round-tripped through Temporal's own persistence,
// which keeps only microseconds. Without normalizing, a run that transitions
// from "live only" to "also archived" between two page fetches compares as
// strictly earlier than the cursor built from its own live-precision
// timestamp, and reappears as a duplicate on the next page.
func RunIndexTime(run *orkstackv1.Run) time.Time {
	for _, ts := range []*timestamppb.Timestamp{
		run.GetCreatedAt(), run.GetStartedAt(), run.GetFinishedAt(),
	} {
		if ts != nil {
			return ts.AsTime().Truncate(time.Microsecond)
		}
	}
	return time.Time{}
}

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
const (
	// ArchiveKindRun marks the snapshot line.
	ArchiveKindRun = "run"
	// ArchiveKindEvent marks an event line.
	ArchiveKindEvent = "event"
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
}

// MarshalRunArchive renders a run and its events as the archive body.
func MarshalRunArchive(run *orkstackv1.Run, events []*orkstackv1.Event) ([]byte, error) {
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
	for _, event := range events {
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
	return body.Bytes(), nil
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
		default:
			return nil, nil, fmt.Errorf("engine: read archive line %d: unknown kind %q", number+1, record.Kind)
		}
	}
	if run == nil {
		return nil, nil, fmt.Errorf("engine: read archive: no %q line", ArchiveKindRun)
	}
	return run, events, nil
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
func (a *RunArchiver) Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event) error {
	if a == nil {
		return nil
	}
	if run.GetOrkflowName() == "" {
		return fmt.Errorf("engine: archive run %q: orkflow name is required", run.GetId())
	}
	body, err := MarshalRunArchive(run, events)
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
	for _, taskRun := range run.GetTaskRuns() {
		for _, ref := range taskRun.GetArtifacts() {
			appendRef(ref)
		}
		for _, ref := range taskRun.GetSystemArtifacts() {
			appendRef(ref)
		}
		appendRef(taskRun.GetValuesRef())
	}
	return ids
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

// LoadRun returns just the archived run snapshot.
func (a *RunArchiver) LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	run, _, err := a.Load(ctx, runID)
	return run, err
}

// LoadRunEvents returns just the archived event log, in sequence order.
func (a *RunArchiver) LoadRunEvents(ctx context.Context, runID string) ([]*orkstackv1.Event, error) {
	_, events, err := a.Load(ctx, runID)
	return events, err
}

// ListRuns answers a query: one prefix scan over index keys, then one body read
// per run on the resulting page. Filtering never reads a body, so the cost is
// set by the page size, not by how much history the archive holds.
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
		Limit:       limit,
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
			run, _, err := a.Load(ctx, runID)
			runs[i], errs[i] = run, err
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

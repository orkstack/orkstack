package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The control-plane audit trail is one immutable object per call of a write
// RPC, written by the engine at the moment the handler returned:
//
//	metadata/audit/{yyyy}/{mm}/{dd}/{id}.json        one AuditRecord
//
// It is a RECORD, never a read path: nothing in OrkStack consults one to make
// a decision, so the trail cannot become a second source of truth about
// anything (AGENTS.md §2.2). Records are create-only and the store has no
// update and no delete, so a record, once written, is immutable by
// construction rather than by convention.
//
// The layout is DATE-FIRST, unlike the namespace-first run archive, because
// the trail's primary read is the deployment's writes over a window — and a
// deployment-wide write (retention, webhooks) has no namespace at all. One
// newest-first day walk with body filtering is the simpler reader; its cost
// is bounded by AuditScanMaxObjects, and the volume is operator-shaped.
const (
	// AuditPrefix roots every audit record. It sits under metadata/, beside
	// metadata/attempts and metadata/triggers, and never under orkflows/ or
	// objects/, so a workload artifact can never collide with a record and an
	// operator can express one lifecycle rule over the prefix.
	AuditPrefix = "metadata/audit"
	// AuditSchemaVersion is the only AuditRecord schema this build writes or
	// returns. A reader skips a record of any other version rather than
	// guessing at its fields.
	AuditSchemaVersion = 1
	// AuditContentType is what an audit record is stored as.
	AuditContentType = "application/json"
	// AuditObjectSuffix is the fixed leaf suffix of every audit key.
	AuditObjectSuffix = ".json"

	// AuditPrincipalMaxBytes bounds principal: it is a header value a caller
	// controls.
	AuditPrincipalMaxBytes = 256
	// AuditTargetMaxBytes bounds target, and every other short coordinate
	// (namespace, procedure, source address) a refused request may carry.
	AuditTargetMaxBytes = 512
	// AuditDetailMaxBytes bounds detail.
	AuditDetailMaxBytes = 1 << 10
	// AuditMaxBytes bounds the whole serialized record.
	AuditMaxBytes = 8 << 10

	// AuditScanMaxObjects is how many records one listing may examine (key
	// listed and body fetched) before it has filled its page. Past it the
	// listing is REFUSED with ErrAuditScanBudget, never answered from the
	// subset it read: a partial audit listing that looks complete is worse
	// than none.
	AuditScanMaxObjects = 5000

	auditIDPrefix = "aud-"
	auditIDHex    = 16
)

var (
	// ErrAuditRecordExists reports a create-only put onto a key that already
	// holds a record. The existing record is never overwritten.
	ErrAuditRecordExists = errors.New("audit record already exists")
	// ErrAuditScanBudget reports a listing that examined AuditScanMaxObjects
	// records without filling its page.
	ErrAuditScanBudget = fmt.Errorf("audit scan examined more than %d records before filling a page", AuditScanMaxObjects)
	// ErrInvalidAuditRecord reports a record that cannot be addressed or
	// stored.
	ErrInvalidAuditRecord = errors.New("invalid audit record")

	auditIDPattern = regexp.MustCompile(`^aud-\d{8}T\d{6}\.\d{9}Z-[0-9a-f]{16}$`)
)

// AuditStore writes and reads control-plane audit records. Implementations
// hold no cache and no per-process state: the replica that recorded a write
// and the replica asked about it later are different processes, and any
// replica must answer for any write (AGENTS.md §7).
type AuditStore interface {
	// PutAuditRecord writes one record, create-only. The record is bounded
	// (AuditMaxBytes) before it is stored. An existing key is
	// ErrAuditRecordExists, never overwritten.
	PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error
	// ListAuditRecords returns one page, newest first, and the key to
	// continue BEFORE (exclusive). A scan that examines more than
	// AuditScanMaxObjects before filling the page returns ErrAuditScanBudget.
	ListAuditRecords(ctx context.Context, q AuditQuery) (AuditPage, error)
}

// AuditQuery narrows a listing. Since is inclusive, Until exclusive; both are
// required here — the RPC resolves the defaults, so the store never guesses
// a window. Namespace "" means every namespace. BeforeKey is the exclusive
// newest-first cursor (a full object key the store itself handed back).
type AuditQuery struct {
	Since, Until time.Time
	Namespace    string
	ActionPrefix string
	Principal    string
	BeforeKey    string
	Limit        int
}

// Matches reports whether rec passes the query's filters. The window is not
// part of it: the walk applies the window from the key before a body is
// fetched, and again from the body's own time.
//
// Namespace "" keeps everything, deployment-wide records included; a name
// keeps only records of exactly that namespace, so a deployment-wide record —
// which carries no namespace — matches no name, "default" included.
func (q AuditQuery) Matches(rec *orkstackv1.AuditRecord) bool {
	if q.Namespace != "" && rec.GetNamespace() != q.Namespace {
		return false
	}
	if q.ActionPrefix != "" && !strings.HasPrefix(rec.GetAction(), q.ActionPrefix) {
		return false
	}
	if q.Principal != "" && rec.GetPrincipal() != q.Principal {
		return false
	}
	return true
}

// AuditPage is one page. NextKey is the key of the last record delivered,
// which the caller hands back as BeforeKey; empty means the window is
// exhausted.
type AuditPage struct {
	Records []*orkstackv1.AuditRecord
	NextKey string
}

// NewAuditID renders an opaque record id: "aud-" + RunStamp(at) + "-" + 16
// hex characters from crypto/rand. The stamp makes ids sort by time within a
// day's prefix; nothing parses it for meaning — the record's own `at` is the
// time.
func NewAuditID(at time.Time) string {
	var raw [auditIDHex / 2]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand does not fail on a supported platform; a zero suffix
		// still yields a valid id, and create-only publication turns a
		// collision into a refused write rather than an overwrite.
		clear(raw[:])
	}
	return auditIDPrefix + RunStamp(at) + "-" + hex.EncodeToString(raw[:])
}

// ValidAuditID reports whether id is shaped like an id NewAuditID mints. It is
// checked before any path operation.
func ValidAuditID(id string) bool { return auditIDPattern.MatchString(id) }

// auditIDTime returns the stamp embedded in a valid id.
func auditIDTime(id string) (time.Time, bool) {
	if !ValidAuditID(id) {
		return time.Time{}, false
	}
	stamp := id[len(auditIDPrefix) : len(auditIDPrefix)+len(RunStampLayout)]
	at, err := time.Parse(RunStampLayout, stamp)
	if err != nil {
		return time.Time{}, false
	}
	return at.UTC(), true
}

// auditDayPrefix renders metadata/audit/{yyyy}/{mm}/{dd}/ for day's UTC date.
func auditDayPrefix(day time.Time) string {
	return AuditPrefix + "/" + day.UTC().Format("2006/01/02") + "/"
}

// AuditKey renders the object key of one record from its `at` (UTC day) and
// its id: metadata/audit/{yyyy}/{mm}/{dd}/{id}.json.
func AuditKey(rec *orkstackv1.AuditRecord) (string, error) {
	if rec == nil {
		return "", fmt.Errorf("artifacts: audit key: %w: nil record", ErrInvalidAuditRecord)
	}
	if !ValidAuditID(rec.GetId()) {
		return "", fmt.Errorf("artifacts: audit key: %w: id %q", ErrInvalidAuditRecord, rec.GetId())
	}
	if rec.GetAt() == nil || !rec.GetAt().IsValid() {
		return "", fmt.Errorf("artifacts: audit key %q: %w: no valid time", rec.GetId(), ErrInvalidAuditRecord)
	}
	return auditDayPrefix(rec.GetAt().AsTime()) + rec.GetId() + AuditObjectSuffix, nil
}

// ParseAuditKey reverses AuditKey. A key this package did not generate —
// including one whose day segment disagrees with the stamp in its id —
// reports ok=false rather than an error, so a scan skips it instead of
// failing on it.
func ParseAuditKey(key string) (id string, day time.Time, ok bool) {
	rest, found := strings.CutPrefix(key, AuditPrefix+"/")
	if !found {
		return "", time.Time{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 4 {
		return "", time.Time{}, false
	}
	day, err := time.Parse("2006/01/02", parts[0]+"/"+parts[1]+"/"+parts[2])
	if err != nil {
		return "", time.Time{}, false
	}
	id, cut := strings.CutSuffix(parts[3], AuditObjectSuffix)
	if !cut {
		return "", time.Time{}, false
	}
	stamp, valid := auditIDTime(id)
	if !valid || !sameUTCDay(stamp, day) {
		return "", time.Time{}, false
	}
	return id, day.UTC(), true
}

func sameUTCDay(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

// MarshalAuditRecord renders the exact bytes a record is stored as, after
// bounding it. The caller's message is never modified.
func MarshalAuditRecord(rec *orkstackv1.AuditRecord) ([]byte, error) {
	if rec == nil {
		return nil, fmt.Errorf("artifacts: encode audit record: %w: nil record", ErrInvalidAuditRecord)
	}
	bounded := BoundAuditRecord(rec)
	document, err := marshalAuditExact(bounded)
	if err != nil {
		return nil, err
	}
	// The ladder gives up the least useful text first. Each step re-measures,
	// because the encoder's escaping decides what a string costs.
	for _, shrink := range []func(*orkstackv1.AuditRecord){
		func(r *orkstackv1.AuditRecord) { r.Detail = markerOnly(r.GetDetail()) },
		func(r *orkstackv1.AuditRecord) { r.Target = markerOnly(r.GetTarget()) },
		func(r *orkstackv1.AuditRecord) { r.Principal = markerOnly(r.GetPrincipal()) },
	} {
		if len(document) <= AuditMaxBytes {
			return document, nil
		}
		shrink(bounded)
		if document, err = marshalAuditExact(bounded); err != nil {
			return nil, err
		}
	}
	if len(document) > AuditMaxBytes {
		return nil, fmt.Errorf("artifacts: encode audit record %q: %d bytes, limit is %d: %w",
			rec.GetId(), len(document), AuditMaxBytes, ErrInvalidAuditRecord)
	}
	return document, nil
}

// BoundAuditRecord returns a copy of rec with every string one printable line
// and within its bound. Control characters (a newline, a tab, an escape) are
// replaced by a space, so a header value cannot break a tabwriter table or a
// log line; invalid UTF-8 is replaced, so a hostile header cannot make the
// record unencodable and therefore unrecorded.
func BoundAuditRecord(rec *orkstackv1.AuditRecord) *orkstackv1.AuditRecord {
	bounded, _ := proto.Clone(rec).(*orkstackv1.AuditRecord)
	bounded.Principal = truncateUTF8(oneLine(bounded.GetPrincipal()), AuditPrincipalMaxBytes)
	bounded.Target = truncateUTF8(oneLine(bounded.GetTarget()), AuditTargetMaxBytes)
	bounded.Detail = truncateUTF8(oneLine(bounded.GetDetail()), AuditDetailMaxBytes)
	// The remaining strings are the engine's own vocabulary on a success, but
	// a refused request can carry any namespace a caller typed, so they are
	// bounded too.
	bounded.Namespace = truncateUTF8(oneLine(bounded.GetNamespace()), AuditTargetMaxBytes)
	bounded.Procedure = truncateUTF8(oneLine(bounded.GetProcedure()), AuditTargetMaxBytes)
	bounded.SourceAddress = truncateUTF8(oneLine(bounded.GetSourceAddress()), AuditTargetMaxBytes)
	bounded.Action = truncateUTF8(oneLine(bounded.GetAction()), AuditTargetMaxBytes)
	bounded.ErrorCode = truncateUTF8(oneLine(bounded.GetErrorCode()), AuditTargetMaxBytes)
	return bounded
}

// oneLine replaces invalid UTF-8 and every control character with a space.
func oneLine(s string) string {
	s = strings.ToValidUTF8(s, "�")
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
}

func marshalAuditExact(rec *orkstackv1.AuditRecord) ([]byte, error) {
	raw, err := protojson.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode audit record %q: %w", rec.GetId(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode audit record %q: %w", rec.GetId(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// UnmarshalAuditRecord decodes a stored record. Unknown fields are discarded,
// so an engine can read what a newer engine wrote within one schema version.
func UnmarshalAuditRecord(document []byte) (*orkstackv1.AuditRecord, error) {
	rec := &orkstackv1.AuditRecord{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, rec); err != nil {
		return nil, fmt.Errorf("artifacts: decode audit record: %w", err)
	}
	return rec, nil
}

// encodeAuditForStore is the one place a record becomes a key and bytes, so
// no backend can drift into a different bound or a different key.
func encodeAuditForStore(rec *orkstackv1.AuditRecord) (string, []byte, error) {
	key, err := AuditKey(rec)
	if err != nil {
		return "", nil, err
	}
	document, err := MarshalAuditRecord(rec)
	if err != nil {
		return "", nil, err
	}
	return key, document, nil
}

// auditSource is what a backend contributes to a listing: the keys under one
// day's prefix, and one body. The walk — window, cursor, filters, budget and
// order — is shared, so the backends cannot answer the same query
// differently.
type auditSource interface {
	// listAuditDay returns every key under dayPrefix (keys only), in any
	// order. A day with nothing in it is an empty list, never an error.
	listAuditDay(ctx context.Context, dayPrefix string) ([]string, error)
	// fetchAudit returns one body; found=false when it vanished between the
	// listing and the read.
	fetchAudit(ctx context.Context, key string) (body []byte, found bool, err error)
}

// walkAudit answers one page: days from the newest in the window (or the
// cursor's day) down to since's day; within a day, keys newest first; each
// candidate inside the window and before the cursor is fetched, checked, and
// matched, until Limit records match or the window is exhausted.
func walkAudit(ctx context.Context, src auditSource, q AuditQuery) (AuditPage, error) {
	if q.Limit <= 0 {
		return AuditPage{}, fmt.Errorf("artifacts: list audit records: limit %d is not positive", q.Limit)
	}
	if !q.Until.After(q.Since) {
		return AuditPage{}, fmt.Errorf("artifacts: list audit records: until %s is not after since %s",
			q.Until.UTC().Format(time.RFC3339Nano), q.Since.UTC().Format(time.RFC3339Nano))
	}
	since, until := q.Since.UTC(), q.Until.UTC()
	// The newest instant that can be in the window. until is exclusive, so a
	// window ending exactly at midnight never visits the empty next day.
	last := until.Add(-time.Nanosecond)
	if q.BeforeKey != "" {
		_, cursorDay, ok := ParseAuditKey(q.BeforeKey)
		if !ok {
			return AuditPage{}, fmt.Errorf("artifacts: list audit records: cursor %q is not an audit key: %w",
				q.BeforeKey, ErrInvalidAuditRecord)
		}
		endOfCursorDay := cursorDay.Add(24*time.Hour - time.Nanosecond)
		if endOfCursorDay.Before(last) {
			last = endOfCursorDay
		}
	}
	examined := 0
	var page AuditPage
	err := walkDaysNewestFirst(ctx, last, since, func(day time.Time) (bool, error) {
		keys, err := src.listAuditDay(ctx, auditDayPrefix(day))
		if err != nil {
			return false, err
		}
		sort.Strings(keys)
		for i := len(keys) - 1; i >= 0; i-- {
			key := keys[i]
			if q.BeforeKey != "" && key >= q.BeforeKey {
				continue
			}
			id, _, ok := ParseAuditKey(key)
			if !ok {
				continue
			}
			stamp, _ := auditIDTime(id)
			if stamp.Before(since) {
				// Keys in a day sort by stamp: everything further down is older
				// still, and older than the window.
				break
			}
			if !stamp.Before(until) {
				continue
			}
			if examined >= AuditScanMaxObjects {
				return false, ErrAuditScanBudget
			}
			examined++
			rec, ok, err := readAuditCandidate(ctx, src, key, day)
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
			at := rec.GetAt().AsTime()
			if at.Before(since) || !at.Before(until) || !q.Matches(rec) {
				continue
			}
			page.Records = append(page.Records, rec)
			if len(page.Records) == q.Limit {
				page.NextKey = key
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return AuditPage{}, err
	}
	return page, nil
}

// readAuditCandidate fetches and checks one body. A body that vanished, does
// not decode, carries an unknown schema version, or claims a time outside its
// key's day is skipped (ok=false) and logged — never returned, and never the
// reason the whole listing fails.
func readAuditCandidate(
	ctx context.Context, src auditSource, key string, day time.Time,
) (*orkstackv1.AuditRecord, bool, error) {
	body, found, err := src.fetchAudit(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	rec, err := UnmarshalAuditRecord(body)
	if err != nil {
		slog.Default().Warn("audit record skipped: undecodable", "key", key, "error", err)
		return nil, false, nil
	}
	if rec.GetSchemaVersion() != AuditSchemaVersion {
		slog.Default().Warn("audit record skipped: unknown schema version",
			"key", key, "schema_version", rec.GetSchemaVersion())
		return nil, false, nil
	}
	if rec.GetAt() == nil || !rec.GetAt().IsValid() || !sameUTCDay(rec.GetAt().AsTime(), day) {
		slog.Default().Warn("audit record skipped: time is not in its key's day", "key", key)
		return nil, false, nil
	}
	return rec, true, nil
}

func truncateToDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// NewAuditStore selects the same shared backend as artifacts, run history,
// attempts and triggers — one ORK_ARTIFACT_BACKEND, no setting of its own.
func NewAuditStore(ctx context.Context, cfg Config) (AuditStore, error) {
	switch cfg.Backend {
	case "", BackendLocal:
		return NewLocalAuditStore(localDir(cfg))
	case BackendS3:
		return NewS3AuditStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: audit store backend %q: %w", cfg.Backend, ErrUnknownBackend)
	}
}

// MemoryAuditStore is an in-memory AuditStore for tests. It bounds, encodes
// and walks exactly as the durable backends do. It is NOT durable and must
// never be selected by a deployment.
type MemoryAuditStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

var _ AuditStore = (*MemoryAuditStore)(nil)

// NewMemoryAuditStore returns an empty in-memory audit store.
func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{objects: make(map[string][]byte)}
}

// PutAuditRecord stores one record, create-only.
func (s *MemoryAuditStore) PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	key, document, err := encodeAuditForStore(rec)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[key]; exists {
		return fmt.Errorf("artifacts: memory audit record %q: %w", key, ErrAuditRecordExists)
	}
	s.objects[key] = document
	return nil
}

// ListAuditRecords walks the stored records.
func (s *MemoryAuditStore) ListAuditRecords(ctx context.Context, q AuditQuery) (AuditPage, error) {
	return walkAudit(ctx, s, q)
}

// Documents returns a copy of every stored document by key, so a test can
// assert on the exact bytes a backend would hold.
func (s *MemoryAuditStore) Documents() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.objects))
	for key, document := range s.objects {
		out[key] = append([]byte(nil), document...)
	}
	return out
}

func (s *MemoryAuditStore) listAuditDay(ctx context.Context, dayPrefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, dayPrefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (s *MemoryAuditStore) fetchAudit(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	document, ok := s.objects[key]
	return document, ok, nil
}

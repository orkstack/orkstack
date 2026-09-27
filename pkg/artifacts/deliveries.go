package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// A webhook delivery record is one immutable object per SETTLED delivery —
// landed, or given up after its retry ladder — written by the engine after
// the dispatcher returned:
//
//	metadata/deliveries/{yyyy}/{mm}/{dd}/{id}/{namespace}/{run-id}/{webhook-id}/{event}/{outcome}.json
//
// It is a LOG, never a queue: nothing in OrkStack reads a record to decide
// what to send, there is no pending state, no lease and no due time, and the
// store has no update and no delete. The record carries every attempt, the
// signature that was sent and the SHA-256 of the body — never the body, which
// would be a second copy of the run (AGENTS.md §2.2).
//
// The layout is date-first, like the audit trail, so one bucket lifecycle
// rule over the prefix expires records by age. The segments after the id are
// the record's own coordinates, so every filter of a listing is decided from
// the key alone and a body is fetched only for a record that is returned.
const (
	// DeliveryPrefix roots every delivery record, under metadata/ beside the
	// attempt, trigger and audit families.
	DeliveryPrefix = "metadata/deliveries"
	// DeliverySchemaVersion is the only Delivery schema this build writes or
	// returns. A reader skips a record of any other version.
	DeliverySchemaVersion = 1
	// DeliveryContentType is what a record is stored as.
	DeliveryContentType = "application/json"
	// DeliveryObjectSuffix is the fixed leaf suffix of every record key.
	DeliveryObjectSuffix = ".json"

	// DeliveryOutcomeDeliveredSegment and DeliveryOutcomeFailedSegment are the
	// key's {outcome} leaf.
	DeliveryOutcomeDeliveredSegment = "delivered"
	DeliveryOutcomeFailedSegment    = "failed"

	// DeliveryScanMaxKeys bounds the keys one ListDeliveries call may examine.
	// Keys are cheap (1000 per list request) and every filter is decided
	// from the key, so the budget is spent listing, never fetching.
	DeliveryScanMaxKeys = 100_000
	// Bounds applied by MarshalDelivery before a byte is stored.
	DeliveryErrorMaxBytes = 1 << 10  // DeliveryAttempt.error
	DeliveryURLMaxBytes   = 2 << 10  // Delivery.url
	DeliveryMaxBytes      = 16 << 10 // the whole document

	// deliveryKeySegments is the fixed segment count of a record key.
	deliveryKeySegments = 11
	deliveryIDPrefix    = "dlv-"
)

var (
	// ErrDeliveryExists reports a create-only put onto a key that already
	// holds a record. The existing record is never overwritten.
	ErrDeliveryExists = errors.New("artifacts: delivery record already exists")
	// ErrDeliveryScanBudget reports a listing that examined more than
	// DeliveryScanMaxKeys keys before filling its page.
	ErrDeliveryScanBudget = errors.New("artifacts: delivery scan exceeded its key budget")
	// ErrInvalidDeliveryKey reports a record that cannot be addressed, or a
	// body that disagrees with the key it was found under.
	ErrInvalidDeliveryKey = errors.New("artifacts: invalid delivery record key")
	// ErrUnknownDeliverySchema reports a record whose schema_version this
	// build does not know. A listing skips such a record.
	ErrUnknownDeliverySchema = errors.New("artifacts: unknown delivery record schema version")
)

// DeliveryStore writes and reads webhook delivery records. Implementations
// hold no cache and no per-process state: the replica that made a delivery
// and the replica asked about it are different processes (AGENTS.md §7).
//
// There is no update, no delete, no claim and no lease: a record has exactly
// one state, written.
type DeliveryStore interface {
	// PutDelivery writes one record, create-only. It is bounded
	// (DeliveryMaxBytes) before it is stored. An existing key is
	// ErrDeliveryExists, never overwritten.
	PutDelivery(ctx context.Context, rec *orkstackv1.Delivery) error
	// GetDelivery returns the record with this id: one keys-only prefix list
	// under the id's day, then one get. A key whose id segment matches but
	// whose body is unreadable is an error, never "not found".
	GetDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, bool, error)
	// ListDeliveries returns one page, newest first. A walk that examines
	// more than DeliveryScanMaxKeys keys before filling the page returns
	// ErrDeliveryScanBudget.
	ListDeliveries(ctx context.Context, q DeliveryQuery) (DeliveryPage, error)
}

// DeliveryQuery narrows a listing. Since is inclusive, Until exclusive,
// both against created_at; both are required here — the RPC resolves the
// defaults, so the store never guesses a window. Empty string / UNSPECIFIED
// fields match every value. BeforeKey is the exclusive newest-first cursor:
// a full object key the store itself handed back.
type DeliveryQuery struct {
	Since, Until time.Time
	WebhookID    string
	RunID        string
	Namespace    string
	Event        orkstackv1.EventType
	Outcome      orkstackv1.DeliveryOutcome
	BeforeKey    string
	Limit        int
}

// matches reports whether a key's coordinates pass every non-empty filter.
func (q DeliveryQuery) matches(k DeliveryKeyFields) bool {
	switch {
	case q.WebhookID != "" && k.WebhookID != q.WebhookID,
		q.RunID != "" && k.RunID != q.RunID,
		q.Namespace != "" && k.Namespace != q.Namespace,
		q.Event != orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED && k.Event != q.Event,
		q.Outcome == orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED && !k.Delivered,
		q.Outcome == orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_FAILED && k.Delivered:
		return false
	}
	return true
}

// DeliveryPage is one page. NextKey is the key of the last record delivered,
// which the caller hands back as BeforeKey; empty means the window is
// exhausted.
type DeliveryPage struct {
	Deliveries []*orkstackv1.Delivery
	NextKey    string
}

// DeliveryKeyFields are the coordinates a record key carries.
type DeliveryKeyFields struct {
	ID        string
	CreatedAt time.Time
	Namespace string
	RunID     string
	WebhookID string
	Event     orkstackv1.EventType
	Delivered bool
}

// deliveryIDTime returns the instant a valid delivery id's stamp carries. It
// is the store's coordinate for the record's day; no client derives anything
// from it.
func deliveryIDTime(id string) (time.Time, bool) {
	if !webhooks.ValidDeliveryID(id) {
		return time.Time{}, false
	}
	stamp := id[len(deliveryIDPrefix) : len(deliveryIDPrefix)+len(webhooks.DeliveryIDLayout)]
	at, err := time.Parse(webhooks.DeliveryIDLayout, stamp)
	if err != nil {
		return time.Time{}, false
	}
	return at.UTC(), true
}

// deliveryDayPrefix renders metadata/deliveries/{yyyy}/{mm}/{dd}/.
func deliveryDayPrefix(day time.Time) string {
	return DeliveryPrefix + "/" + day.UTC().Format("2006/01/02") + "/"
}

// DeliveryIDPrefix returns metadata/deliveries/{yyyy}/{mm}/{dd}/{id}/ — the
// day from the id's stamp — which is how GetDelivery finds one record with
// one keys-only prefix list and no scan.
func DeliveryIDPrefix(id string) (string, error) {
	at, ok := deliveryIDTime(id)
	if !ok {
		return "", fmt.Errorf("%w: delivery id %q", ErrInvalidDeliveryKey, id)
	}
	return deliveryDayPrefix(at) + id + "/", nil
}

// subscribableEvent reports whether event is one a subscription can receive,
// and so one a delivery can carry.
func subscribableEvent(event orkstackv1.EventType) bool {
	if event == orkstackv1.EventType_EVENT_TYPE_UNSPECIFIED {
		return false
	}
	_, err := webhooks.ParseEvent(event.String())
	return err == nil
}

// DeliveryKey derives a record's key from the record. It refuses an id that
// is not a delivery id, a created_at that is not the id's stamp instant to
// the nanosecond, an empty webhook_id, run_id or namespace, and an event no
// subscription can receive. The recorder builds every one of those from the
// same Result, so the check guards against a hand-written record.
func DeliveryKey(rec *orkstackv1.Delivery) (string, error) {
	if rec == nil {
		return "", fmt.Errorf("%w: nil record", ErrInvalidDeliveryKey)
	}
	stamp, ok := deliveryIDTime(rec.GetId())
	if !ok {
		return "", fmt.Errorf("%w: delivery id %q", ErrInvalidDeliveryKey, rec.GetId())
	}
	if rec.GetCreatedAt() == nil || !rec.GetCreatedAt().IsValid() || !rec.GetCreatedAt().AsTime().Equal(stamp) {
		return "", fmt.Errorf("%w: delivery %q: created_at does not equal the id's stamp %s",
			ErrInvalidDeliveryKey, rec.GetId(), stamp.Format(time.RFC3339Nano))
	}
	if !subscribableEvent(rec.GetEvent()) {
		return "", fmt.Errorf("%w: delivery %q: %s is not a subscribable event",
			ErrInvalidDeliveryKey, rec.GetId(), rec.GetEvent())
	}
	segments := make([]string, 0, 3)
	for _, field := range []struct{ name, value string }{
		{"namespace", rec.GetNamespace()}, {"run_id", rec.GetRunId()}, {"webhook_id", rec.GetWebhookId()},
	} {
		if field.value == "" {
			return "", fmt.Errorf("%w: delivery %q: empty %s", ErrInvalidDeliveryKey, rec.GetId(), field.name)
		}
		encoded, err := encodeArchiveSegment(field.value)
		if err != nil {
			return "", fmt.Errorf("%w: delivery %q: %s: %v", ErrInvalidDeliveryKey, rec.GetId(), field.name, err)
		}
		segments = append(segments, encoded)
	}
	outcome := DeliveryOutcomeFailedSegment
	if rec.GetDelivered() {
		outcome = DeliveryOutcomeDeliveredSegment
	}
	return deliveryDayPrefix(stamp) + rec.GetId() + "/" + strings.Join(segments, "/") + "/" +
		strings.TrimPrefix(rec.GetEvent().String(), "EVENT_TYPE_") + "/" + outcome + DeliveryObjectSuffix, nil
}

// ParseDeliveryKey reverses DeliveryKey. A foreign key under the prefix, a
// wrong segment count, an undecodable segment, or a day that disagrees with
// the id's stamp is ok=false, so a walk skips it rather than fails.
func ParseDeliveryKey(key string) (DeliveryKeyFields, bool) {
	rest, found := strings.CutPrefix(key, DeliveryPrefix+"/")
	if !found {
		return DeliveryKeyFields{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts)+2 != deliveryKeySegments {
		return DeliveryKeyFields{}, false
	}
	day, err := time.Parse("2006/01/02", parts[0]+"/"+parts[1]+"/"+parts[2])
	if err != nil {
		return DeliveryKeyFields{}, false
	}
	id := parts[3]
	stamp, ok := deliveryIDTime(id)
	if !ok || !sameUTCDay(stamp, day) {
		return DeliveryKeyFields{}, false
	}
	fields := DeliveryKeyFields{ID: id, CreatedAt: stamp}
	for i, target := range []*string{&fields.Namespace, &fields.RunID, &fields.WebhookID} {
		decoded, ok := decodeArchiveSegment(parts[4+i])
		if !ok {
			return DeliveryKeyFields{}, false
		}
		*target = decoded
	}
	value, known := orkstackv1.EventType_value["EVENT_TYPE_"+parts[7]]
	if !known || !subscribableEvent(orkstackv1.EventType(value)) {
		return DeliveryKeyFields{}, false
	}
	fields.Event = orkstackv1.EventType(value)
	switch parts[8] {
	case DeliveryOutcomeDeliveredSegment + DeliveryObjectSuffix:
		fields.Delivered = true
	case DeliveryOutcomeFailedSegment + DeliveryObjectSuffix:
	default:
		return DeliveryKeyFields{}, false
	}
	return fields, true
}

// MarshalDelivery renders the exact bytes a record is stored as, after
// bounding it: url and every attempt's error are truncated with the marker,
// control characters in an error become spaces so a tabwriter row cannot be
// broken by a transport message, and schema_version is set. A document still
// over DeliveryMaxBytes is refused. The caller's message is never modified.
func MarshalDelivery(rec *orkstackv1.Delivery) ([]byte, error) {
	if rec == nil {
		return nil, fmt.Errorf("%w: nil record", ErrInvalidDeliveryKey)
	}
	bounded, _ := proto.Clone(rec).(*orkstackv1.Delivery)
	bounded.SchemaVersion = DeliverySchemaVersion
	bounded.Url = truncateUTF8(bounded.GetUrl(), DeliveryURLMaxBytes)
	for _, attempt := range bounded.GetAttempts() {
		attempt.Error = truncateUTF8(oneLine(attempt.GetError()), DeliveryErrorMaxBytes)
	}
	raw, err := protojson.Marshal(bounded)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode delivery record %q: %w", rec.GetId(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode delivery record %q: %w", rec.GetId(), err)
	}
	out.WriteByte('\n')
	if out.Len() > DeliveryMaxBytes {
		return nil, fmt.Errorf("artifacts: encode delivery record %q: %d bytes, limit is %d: %w",
			rec.GetId(), out.Len(), DeliveryMaxBytes, ErrInvalidDeliveryKey)
	}
	return out.Bytes(), nil
}

// UnmarshalDelivery decodes a stored record found under key. Unknown fields
// are discarded, so an engine reads what a newer engine wrote within one
// schema version; an unknown schema version is ErrUnknownDeliverySchema; and
// a body whose own coordinates derive a different key than the one it was
// found under is ErrInvalidDeliveryKey — a hand-edited record cannot be
// listed under one coordinate and read as another.
func UnmarshalDelivery(key string, document []byte) (*orkstackv1.Delivery, error) {
	rec := &orkstackv1.Delivery{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, rec); err != nil {
		return nil, fmt.Errorf("artifacts: decode delivery record %q: %w", key, err)
	}
	if rec.GetSchemaVersion() != DeliverySchemaVersion {
		return nil, fmt.Errorf("artifacts: decode delivery record %q: schema_version %d: %w",
			key, rec.GetSchemaVersion(), ErrUnknownDeliverySchema)
	}
	derived, err := DeliveryKey(rec)
	if err != nil {
		return nil, fmt.Errorf("artifacts: decode delivery record %q: %w", key, err)
	}
	if derived != key {
		return nil, fmt.Errorf("artifacts: decode delivery record %q: its fields address %q: %w",
			key, derived, ErrInvalidDeliveryKey)
	}
	return rec, nil
}

// encodeDeliveryForStore is the one place a record becomes a key and bytes,
// so no backend can drift into a different bound or a different key.
func encodeDeliveryForStore(rec *orkstackv1.Delivery) (string, []byte, error) {
	key, err := DeliveryKey(rec)
	if err != nil {
		return "", nil, err
	}
	document, err := MarshalDelivery(rec)
	if err != nil {
		return "", nil, err
	}
	return key, document, nil
}

// deliverySource is what a backend contributes to a read: keys under a
// prefix and one body. The walk — window, cursor, filters, budget and order —
// is shared, so the backends cannot answer the same query differently.
type deliverySource interface {
	// listDeliveryRange returns the keys under dayPrefix that sort strictly
	// after from and strictly before to, in any order, and stops once it has
	// more than max of them (the caller then refuses). A day with nothing in
	// it is an empty list, never an error.
	listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error)
	// listDeliveryID returns every key under one id's prefix (keys only).
	listDeliveryID(ctx context.Context, idPrefix string) ([]string, error)
	// fetchDelivery returns one body; found=false when it vanished between
	// the listing and the read.
	fetchDelivery(ctx context.Context, key string) (body []byte, found bool, err error)
}

// getDelivery is GetDelivery on any source: one prefix list, then one get.
func getDelivery(ctx context.Context, src deliverySource, id string) (*orkstackv1.Delivery, bool, error) {
	prefix, err := DeliveryIDPrefix(id)
	if err != nil {
		return nil, false, err
	}
	keys, err := src.listDeliveryID(ctx, prefix)
	if err != nil {
		return nil, false, err
	}
	switch len(keys) {
	case 0:
		return nil, false, nil
	case 1:
	default:
		sort.Strings(keys)
		// PutDelivery cannot produce this; a store that shows it must not be
		// answered from.
		return nil, false, fmt.Errorf("artifacts: delivery %q has more than one record: %q and %q",
			id, keys[0], keys[1])
	}
	body, found, err := src.fetchDelivery(ctx, keys[0])
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	rec, err := UnmarshalDelivery(keys[0], body)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

// walkDeliveries answers one page: days from the newest in the window (or the
// cursor's day) down to since's day; within a day, keys newest first, every
// filter decided from the key; a body is fetched only for a record that is
// returned.
func walkDeliveries(ctx context.Context, src deliverySource, q DeliveryQuery) (DeliveryPage, error) {
	if q.Limit <= 0 {
		return DeliveryPage{}, fmt.Errorf("artifacts: list delivery records: limit %d is not positive", q.Limit)
	}
	if !q.Until.After(q.Since) {
		return DeliveryPage{}, fmt.Errorf("artifacts: list delivery records: until %s is not after since %s",
			q.Until.UTC().Format(time.RFC3339Nano), q.Since.UTC().Format(time.RFC3339Nano))
	}
	since, until := q.Since.UTC(), q.Until.UTC()
	last := until.Add(-time.Nanosecond)
	if q.BeforeKey != "" {
		cursor, ok := ParseDeliveryKey(q.BeforeKey)
		if !ok {
			return DeliveryPage{}, fmt.Errorf("artifacts: list delivery records: cursor %q is not a delivery key: %w",
				q.BeforeKey, ErrInvalidDeliveryKey)
		}
		if endOfCursorDay := truncateToDay(cursor.CreatedAt).Add(24*time.Hour - time.Nanosecond); endOfCursorDay.Before(last) {
			last = endOfCursorDay
		}
	}
	examined := 0
	var page DeliveryPage
	err := walkDaysNewestFirst(ctx, last, since, func(day time.Time) (bool, error) {
		prefix := deliveryDayPrefix(day)
		// Keys under a day sort by the id's stamp, so the window is a key
		// range: after since's stamp, before until's (and the cursor).
		from := prefix + deliveryIDPrefix + since.Format(webhooks.DeliveryIDLayout)
		to := prefix + deliveryIDPrefix + until.Format(webhooks.DeliveryIDLayout)
		if q.BeforeKey != "" && q.BeforeKey < to {
			to = q.BeforeKey
		}
		keys, err := src.listDeliveryRange(ctx, prefix, from, to, DeliveryScanMaxKeys-examined)
		if err != nil {
			return false, err
		}
		examined += len(keys)
		if examined > DeliveryScanMaxKeys {
			return false, ErrDeliveryScanBudget
		}
		sort.Strings(keys)
		for i := len(keys) - 1; i >= 0; i-- {
			key := keys[i]
			if q.BeforeKey != "" && key >= q.BeforeKey {
				continue
			}
			fields, ok := ParseDeliveryKey(key)
			if !ok || fields.CreatedAt.Before(since) || !fields.CreatedAt.Before(until) || !q.matches(fields) {
				continue
			}
			rec, ok, err := readDeliveryCandidate(ctx, src, key)
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
			page.Deliveries = append(page.Deliveries, rec)
			if len(page.Deliveries) == q.Limit {
				page.NextKey = key
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return DeliveryPage{}, err
	}
	return page, nil
}

// readDeliveryCandidate fetches and checks one body. A body that vanished (a
// lifecycle rule), carries an unknown schema version, or disagrees with its
// key is skipped and logged — never returned, and never the reason the whole
// listing fails.
func readDeliveryCandidate(ctx context.Context, src deliverySource, key string) (*orkstackv1.Delivery, bool, error) {
	body, found, err := src.fetchDelivery(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	rec, err := UnmarshalDelivery(key, body)
	if err != nil {
		slog.Default().Warn("delivery record skipped", "key", key, "error", err)
		return nil, false, nil
	}
	return rec, true, nil
}

// walkDaysNewestFirst visits every UTC day from last's down to first's,
// inclusive, until visit reports done or fails. It is the one day walk in
// this package: the audit trail and the delivery records both page through
// a date-first layout newest first.
func walkDaysNewestFirst(ctx context.Context, last, first time.Time, visit func(day time.Time) (done bool, err error)) error {
	firstDay := truncateToDay(first)
	for day := truncateToDay(last); !day.Before(firstDay); day = day.AddDate(0, 0, -1) {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := visit(day)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

// NewDeliveryStore selects the same shared backend as artifacts, attempts,
// triggers and the audit trail — one ORK_ARTIFACT_BACKEND, no setting of its
// own.
func NewDeliveryStore(ctx context.Context, cfg Config) (DeliveryStore, error) {
	switch cfg.Backend {
	case "", BackendLocal:
		return NewLocalDeliveryStore(localDir(cfg))
	case BackendS3:
		return NewS3DeliveryStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: delivery store backend %q: %w", cfg.Backend, ErrUnknownBackend)
	}
}

// MemoryDeliveryStore is an in-memory DeliveryStore for tests. It bounds,
// encodes and walks exactly as the durable backends do. It is NOT durable and
// must never be selected by a deployment.
type MemoryDeliveryStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	// failPut, when set, is what every PutDelivery answers with.
	failPut error
}

var _ DeliveryStore = (*MemoryDeliveryStore)(nil)

// NewMemoryDeliveryStore returns an empty in-memory delivery store.
func NewMemoryDeliveryStore() *MemoryDeliveryStore {
	return &MemoryDeliveryStore{objects: make(map[string][]byte)}
}

// FailPuts makes every later PutDelivery fail with err (nil restores
// writes), so a test can prove a store failure is never a delivery failure.
func (s *MemoryDeliveryStore) FailPuts(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPut = err
}

// PutDelivery stores one record, create-only.
func (s *MemoryDeliveryStore) PutDelivery(ctx context.Context, rec *orkstackv1.Delivery) error {
	key, document, err := encodeDeliveryForStore(rec)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failPut != nil {
		return fmt.Errorf("artifacts: memory delivery record %q: %w", key, s.failPut)
	}
	if _, exists := s.objects[key]; exists {
		return fmt.Errorf("artifacts: memory delivery record %q: %w", key, ErrDeliveryExists)
	}
	s.objects[key] = document
	return nil
}

// GetDelivery returns one record by id.
func (s *MemoryDeliveryStore) GetDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, bool, error) {
	return getDelivery(ctx, s, id)
}

// ListDeliveries walks the stored records.
func (s *MemoryDeliveryStore) ListDeliveries(ctx context.Context, q DeliveryQuery) (DeliveryPage, error) {
	return walkDeliveries(ctx, s, q)
}

// Documents returns a copy of every stored document by key, so a test can
// assert on the exact bytes a backend would hold.
func (s *MemoryDeliveryStore) Documents() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.objects))
	for key, document := range s.objects {
		out[key] = append([]byte(nil), document...)
	}
	return out
}

func (s *MemoryDeliveryStore) listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, dayPrefix) && key > from && key < to {
			keys = append(keys, key)
			if len(keys) > max {
				break
			}
		}
	}
	return keys, nil
}

func (s *MemoryDeliveryStore) listDeliveryID(ctx context.Context, idPrefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, idPrefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (s *MemoryDeliveryStore) fetchDelivery(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	document, ok := s.objects[key]
	return document, ok, nil
}

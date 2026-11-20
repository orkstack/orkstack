package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The record of what was issued (docs/TASK_IDENTITY.md, "The record of
// what was issued"): one object for each granted mint, written before the
// answer, under the run's identity prefix, holding the caller, the attempt
// and each token's claims. Never a token.
const (
	// MaxIssuedTokens is the most tokens one mint grants, so the most one
	// record holds.
	MaxIssuedTokens = 16
	// MaxIssuedPage is the most records one listing page answers.
	MaxIssuedPage = 100
	// DefaultIssuedPage is a page's size when the query names none.
	DefaultIssuedPage = 50

	issuedFormat      = 1
	maxIssuedBytes    = 32 << 10
	maxIssuedField    = 1024
	maxIssuedAttempt  = 99999
	issuedStampLayout = "20060102T150405.000000000Z"
	issuedReadsAtOnce = 8
)

var (
	issuedKeys   = []string{"v", "namespace", "run_id", "task_id", "item", "attempt", "principal", "orker_id", "issued_at", "tokens"}
	issuedIDName = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// IssuedRecord is what one granted mint issued.
type IssuedRecord struct {
	Namespace, RunID, TaskID string
	Item                     Item
	Attempt                  int32
	// Principal and OrkerID are the caller's, as the engine verified them.
	Principal, OrkerID string
	IssuedAt           time.Time
	Tokens             []IssuedEntry
}

// IssuedEntry is one token's claims: never the token.
type IssuedEntry struct {
	Name, Audience, Subject, JTI, KID string
	IssuedAtUnix, ExpiresAtUnix       int64
}

// IssuedRecordKey renders the key of one record:
// metadata/snapshots/{ns}/{run-id}/identity/issued/{task}/{item or "-"}/{attempt:05}/{stamp}-{id}.json,
// the stamp the record's time in UTC to the nanosecond, so key order is
// time order within an attempt, and id 16 lowercase hex digits.
func IssuedRecordKey(namespace, runID, taskID string, item Item, attempt int32, at time.Time, id string) (string, error) {
	prefix, err := issuedAttemptPrefix(namespace, runID, taskID, item, attempt)
	if err != nil {
		return "", err
	}
	if at.IsZero() || !issuedIDName.MatchString(id) {
		return "", fmt.Errorf("artifacts: issued record key: a record needs a time and an id of 16 hex digits: %w", ErrInvalidRecordKey)
	}
	return prefix + at.UTC().Format(issuedStampLayout) + "-" + id + ".json", nil
}

func issuedAttemptPrefix(namespace, runID, taskID string, item Item, attempt int32) (string, error) {
	if attempt < 1 || attempt > maxIssuedAttempt {
		return "", fmt.Errorf("artifacts: issued record key: attempt %d is not 1 to %d: %w", attempt, maxIssuedAttempt, ErrInvalidRecordKey)
	}
	prefix, err := issuedTaskPrefix(namespace, runID, taskID, item)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%05d/", prefix, attempt), nil
}

// issuedTaskPrefix is the prefix of a task's records, or of one item's
// when item is set.
func issuedTaskPrefix(namespace, runID, taskID string, item Item) (string, error) {
	if err := validCommitTaskID(taskID); err != nil {
		return "", fmt.Errorf("artifacts: issued record key: task id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	if item.Set && item.Index < 0 {
		return "", fmt.Errorf("artifacts: issued record key: item %d is no item: %w", item.Index, ErrInvalidRecordKey)
	}
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + "issued/" + taskID + "/" + issuedItemSegment(item) + "/", nil
}

func issuedItemSegment(item Item) string {
	if !item.Set {
		return "-"
	}
	return strconv.Itoa(int(item.Index))
}

// WriteIssued writes one record under a key no other mint can have, and
// answers that key. A record that cannot be stored is ErrInvalidRecordKey,
// before the store is asked; a store that does not answer is
// RECORD_STORE_UNANSWERED, and the mint is then IDENTITY_UNAVAILABLE.
func (s *RecordStore) WriteIssued(ctx context.Context, r IssuedRecord) (string, error) {
	body, err := encodeIssued(r)
	if err != nil {
		return "", err
	}
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("artifacts: issued record id: %w", err)
	}
	key, err := IssuedRecordKey(r.Namespace, r.RunID, r.TaskID, r.Item, r.Attempt, r.IssuedAt, hex.EncodeToString(id[:]))
	if err != nil {
		return "", err
	}
	if err := s.createIn(ctx, key, body); err != nil {
		if errors.Is(err, errBlobExists) {
			// 64 random bits under one attempt's instant: never another
			// mint's, so a store that says so has not answered this write.
			return "", &RecordStoreUnanswered{Class: UnansweredUnavailable, Err: fmt.Errorf("artifacts: issued record %s already exists", key)}
		}
		return "", err
	}
	return key, nil
}

// IssuedQuery narrows a listing of one run's records to a task, and to
// one of its items, and continues it after a key a previous page ended on.
type IssuedQuery struct {
	Namespace, RunID, TaskID string
	Item                     Item
	After                    string
	Limit                    int
}

// ListIssued lists a run's records in key order, at most Limit (by default
// DefaultIssuedPage), and answers the key to continue after, or "" at the
// end. Each listed object is a RecordRead: an object that is not a record
// of its key is RecordInvalid, naming it, never dropped. A query it cannot
// answer is ErrInvalidRecordKey, before the store is asked; a store that
// does not answer is RECORD_STORE_UNANSWERED.
func (s *RecordStore) ListIssued(ctx context.Context, q IssuedQuery) ([]RecordRead[IssuedRecord], string, error) {
	limit := q.Limit
	if limit == 0 {
		limit = DefaultIssuedPage
	}
	if limit < 1 || limit > MaxIssuedPage {
		return nil, "", fmt.Errorf("artifacts: issued records: a page of %d is not 1 to %d: %w", limit, MaxIssuedPage, ErrInvalidRecordKey)
	}
	var prefix string
	switch {
	case q.TaskID != "":
		p, err := issuedTaskPrefix(q.Namespace, q.RunID, q.TaskID, q.Item)
		if err != nil {
			return nil, "", err
		}
		prefix = p
		if !q.Item.Set {
			prefix = strings.TrimSuffix(prefix, "-/")
		}
	case q.Item.Set:
		return nil, "", fmt.Errorf("artifacts: issued records: an item needs its task: %w", ErrInvalidRecordKey)
	default:
		p, err := identitySnapshotPrefix(q.Namespace, q.RunID)
		if err != nil {
			return nil, "", err
		}
		prefix = p + "issued/"
	}
	if q.After != "" && !strings.HasPrefix(q.After, prefix) {
		return nil, "", fmt.Errorf("artifacts: issued records: the page does not continue this listing: %w", ErrInvalidRecordKey)
	}
	release, err := s.reader.slots.acquireWithin(ctx, SlotList, s.reader.spacing.AttemptMargin)
	if err != nil {
		return nil, "", err
	}
	page, err := s.b.list(ctx, blobListInput{Prefix: prefix, StartAfter: q.After, MaxKeys: int32(limit)})
	release()
	if err != nil {
		return nil, "", unansweredIn(ctx, err)
	}
	if page.Truncated && len(page.Objects) == 0 {
		return nil, "", &RecordStoreUnanswered{Class: UnansweredUnavailable, Err: fmt.Errorf("artifacts: a listing of %s was cut short with nothing in it", prefix)}
	}
	objects := page.Objects
	if len(objects) > limit {
		objects = objects[:limit]
	}
	out := make([]RecordRead[IssuedRecord], len(objects))
	errs := make([]error, len(objects))
	var wg sync.WaitGroup
	gate := make(chan struct{}, issuedReadsAtOnce)
	for i, o := range objects {
		wg.Add(1)
		gate <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-gate }()
			out[i], errs[i] = s.readIssued(ctx, o.Key)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, "", err
	}
	next := ""
	if page.Truncated || len(page.Objects) > limit {
		next = objects[len(objects)-1].Key
	}
	return out, next, nil
}

// readIssued reads one listed record and holds it to its key.
func (s *RecordStore) readIssued(ctx context.Context, key string) (RecordRead[IssuedRecord], error) {
	release, err := s.reader.slots.acquireWithin(ctx, SlotList, s.reader.spacing.AttemptMargin)
	if err != nil {
		return RecordRead[IssuedRecord]{}, err
	}
	defer release()
	raw, _, err := s.b.get(ctx, key, maxIssuedBytes+1)
	if errors.Is(err, errBlobNotFound) {
		// Listed, then gone: only the run's removal deletes a record.
		return RecordRead[IssuedRecord]{}, &RecordStoreUnanswered{Class: UnansweredUnavailable, Err: fmt.Errorf("artifacts: issued record %s was listed and is gone", key)}
	}
	if err != nil {
		return RecordRead[IssuedRecord]{}, unansweredIn(ctx, err)
	}
	r, answer, reason := decodeIssued(raw)
	switch answer {
	case RecordUnsupported:
		return RecordRead[IssuedRecord]{Answer: RecordUnsupported, Object: key}, nil
	case RecordInvalid:
		return invalidRead[IssuedRecord](key, "%s", reason), nil
	}
	leaf := key[strings.LastIndexByte(key, '/')+1:]
	id := strings.TrimSuffix(leaf[strings.LastIndexByte(leaf, '-')+1:], ".json")
	if want, err := IssuedRecordKey(r.Namespace, r.RunID, r.TaskID, r.Item, r.Attempt, r.IssuedAt, id); err != nil || want != key {
		return invalidRead[IssuedRecord](key, "is not the record of its key"), nil
	}
	return RecordRead[IssuedRecord]{Answer: RecordFound, Value: r}, nil
}

type issuedWire struct {
	V         int64             `json:"v"`
	Namespace string            `json:"namespace"`
	RunID     string            `json:"run_id"`
	TaskID    string            `json:"task_id"`
	Item      string            `json:"item"`
	Attempt   int32             `json:"attempt"`
	Principal string            `json:"principal"`
	OrkerID   string            `json:"orker_id"`
	IssuedAt  string            `json:"issued_at"`
	Tokens    []issuedEntryWire `json:"tokens"`
}

type issuedEntryWire struct {
	Name     string `json:"name"`
	Audience string `json:"audience"`
	Subject  string `json:"subject"`
	JTI      string `json:"jti"`
	KID      string `json:"kid"`
	IAT      int64  `json:"iat"`
	EXP      int64  `json:"exp"`
}

// checkIssued says why r cannot be stored, or "".
func checkIssued(r IssuedRecord) string {
	if _, err := issuedAttemptPrefix(r.Namespace, r.RunID, r.TaskID, r.Item, r.Attempt); err != nil {
		return err.Error()
	}
	if r.IssuedAt.IsZero() || !bounded(r.Principal) || !bounded(r.OrkerID) {
		return "a record needs a time, a principal and an Orker"
	}
	if len(r.Tokens) < 1 || len(r.Tokens) > MaxIssuedTokens {
		return fmt.Sprintf("%d tokens is not 1 to %d", len(r.Tokens), MaxIssuedTokens)
	}
	for i, t := range r.Tokens {
		if !bounded(t.Name) || !bounded(t.Audience) || !bounded(t.Subject) || !bounded(t.JTI) || !bounded(t.KID) ||
			t.IssuedAtUnix <= 0 || t.ExpiresAtUnix <= t.IssuedAtUnix {
			return fmt.Sprintf("token %d needs a name, audience, subject, jti, kid and an exp after its iat", i)
		}
	}
	return ""
}

func bounded(s string) bool { return s != "" && len(s) <= maxIssuedField }

// encodeIssued is a record's stored bytes.
func encodeIssued(r IssuedRecord) ([]byte, error) {
	if reason := checkIssued(r); reason != "" {
		return nil, fmt.Errorf("artifacts: issued record: %s: %w", reason, ErrInvalidRecordKey)
	}
	w := issuedWire{V: issuedFormat, Namespace: ResolveNamespace(r.Namespace), RunID: r.RunID, TaskID: r.TaskID,
		Item: issuedItemSegment(r.Item), Attempt: r.Attempt, Principal: r.Principal, OrkerID: r.OrkerID,
		IssuedAt: r.IssuedAt.UTC().Format(time.RFC3339Nano), Tokens: make([]issuedEntryWire, len(r.Tokens))}
	for i, t := range r.Tokens {
		w.Tokens[i] = issuedEntryWire{Name: t.Name, Audience: t.Audience, Subject: t.Subject, JTI: t.JTI, KID: t.KID, IAT: t.IssuedAtUnix, EXP: t.ExpiresAtUnix}
	}
	raw, err := json.Marshal(w, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("artifacts: issued record: %w", err)
	}
	if len(raw)+1 > maxIssuedBytes {
		return nil, fmt.Errorf("artifacts: issued record: %d bytes is over %d: %w", len(raw)+1, maxIssuedBytes, ErrInvalidRecordKey)
	}
	return append(raw, '\n'), nil
}

// decodeIssued reads a record. Only the exact bytes encodeIssued writes
// are a record: anything else of format 1 is invalid, and a later format
// is unsupported.
func decodeIssued(raw []byte) (IssuedRecord, RecordAnswer, string) {
	if len(raw) > maxIssuedBytes {
		return IssuedRecord{}, RecordInvalid, "too large"
	}
	version, bad := decodeVersioned(raw, issuedFormat, issuedKeys)
	switch {
	case bad != "":
		return IssuedRecord{}, RecordInvalid, bad
	case version > issuedFormat:
		return IssuedRecord{}, RecordUnsupported, ""
	case version != issuedFormat:
		return IssuedRecord{}, RecordInvalid, fmt.Sprintf("format %d is no format", version)
	}
	var w issuedWire
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return IssuedRecord{}, RecordInvalid, "does not decode: " + err.Error()
	}
	at, err := time.Parse(time.RFC3339Nano, w.IssuedAt)
	if err != nil {
		return IssuedRecord{}, RecordInvalid, "issued_at is not a time"
	}
	item := NoItem
	if w.Item != "-" {
		n, err := strconv.ParseInt(w.Item, 10, 32)
		if err != nil {
			return IssuedRecord{}, RecordInvalid, "item is not an index or -"
		}
		item = ItemAt(int32(n))
	}
	r := IssuedRecord{Namespace: w.Namespace, RunID: w.RunID, TaskID: w.TaskID, Item: item, Attempt: w.Attempt,
		Principal: w.Principal, OrkerID: w.OrkerID, IssuedAt: at.UTC(), Tokens: make([]IssuedEntry, len(w.Tokens))}
	for i, t := range w.Tokens {
		r.Tokens[i] = IssuedEntry{Name: t.Name, Audience: t.Audience, Subject: t.Subject, JTI: t.JTI, KID: t.KID, IssuedAtUnix: t.IAT, ExpiresAtUnix: t.EXP}
	}
	again, err := encodeIssued(r)
	if err != nil {
		return IssuedRecord{}, RecordInvalid, strings.TrimPrefix(err.Error(), "artifacts: issued record: ")
	}
	if !bytes.Equal(again, raw) {
		return IssuedRecord{}, RecordInvalid, "is not as a record is written"
	}
	return r, RecordFound, ""
}

package payloadcrypt

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// MemoField is the memo field of a sealed run, and of a sealed schedule's
// action, that carries its key wrapped under an engines' payload key. It is
// plaintext (Plain), like every memo field.
const MemoField = "orkflow_payload_key"

const (
	wrapLabel       = "ork-wrap-v1"
	keyMemoVersion  = 1
	maxMemoIDLength = 256
	maxMemoBytes    = 2048
)

// ErrKeyless is a run, or a fire, whose memo carries a key field that does
// not yield a key: cut short, of an unknown version, or a wrap that does not
// open with the binding the memo states. It is the same on every engine that
// holds the file and on every replay.
var ErrKeyless = errors.New("RUN_START_UNREADABLE")

// ErrEngineKeyMissing is a memo naming an engines' payload key this process
// does not hold: a fact about this process's configuration and none about
// the run. Workflow code panics on it, so the workflow task fails and an
// engine that holds the file takes it.
var ErrEngineKeyMissing = errors.New("PAYLOAD_ENGINE_KEY_MISSING: this process does not hold the engines' payload key the run's memo names")

func keyless(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrKeyless}, args...)...)
}

func derive(secret *Key, fields ...string) (*Key, error) {
	if secret == nil {
		return nil, errors.New("payloadcrypt: derive from no key")
	}
	info, err := Info(fields...)
	if err != nil {
		return nil, err
	}
	raw, err := hkdf.Key(sha256.New, secret.b[:], nil, string(info), KeySize)
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: derive: %w", err)
	}
	defer clear(raw)
	return NewKey(raw)
}

// DeriveRunTreeKey is the key of the run tree rooted at rootRunID, from a
// namespace's data key: one key for a run and every run it composes.
func DeriveRunTreeKey(dataKey *Key, rootRunID string) (*Key, error) {
	return derive(dataKey, payloadLabel, KindRun, rootRunID)
}

// DeriveScheduleKey is the key of a schedule, by the id the cluster holds it
// under. A schedule and a run tree never share a key.
func DeriveScheduleKey(dataKey *Key, scheduleID string) (*Key, error) {
	return derive(dataKey, payloadLabel, KindSchedule, scheduleID)
}

// KeyMemo is the memo field that carries a run tree's or a schedule's key,
// wrapped under an engines' payload key, beside everything the wrap is bound
// to. A schedule's id is not in it: a fire takes it from the cluster's own
// stamp, never from a memo that can be copied with its action.
type KeyMemo struct {
	Version     int    `json:"v"`
	Kind        string `json:"k"`
	EngineKeyID string `json:"ek"`
	DataKeyID   string `json:"dk"`
	Namespace   string `json:"ns"`
	NamespaceID string `json:"nsid"`
	RootRunID   string `json:"root,omitempty"`
	Wrap        []byte `json:"w"`
}

var engineKeyIDShape = regexp.MustCompile(`^[0-9a-f]{16}$`)

func boundedID(name, v string) error {
	if v == "" || len(v) > maxMemoIDLength {
		return fmt.Errorf("%s of %d bytes", name, len(v))
	}
	return nil
}

func (m KeyMemo) check() error {
	if m.Version != keyMemoVersion {
		return keyless("a key field of version %d", m.Version)
	}
	if !engineKeyIDShape.MatchString(m.EngineKeyID) {
		return keyless("an engines' key id of no known shape")
	}
	for _, f := range []struct{ name, v string }{
		{"a data key id", m.DataKeyID}, {"a namespace", m.Namespace}, {"a namespace id", m.NamespaceID},
	} {
		if err := boundedID(f.name, f.v); err != nil {
			return keyless("%v", err)
		}
	}
	switch m.Kind {
	case KindRun:
		if m.RootRunID == "" || len(m.RootRunID) > MaxContextID {
			return keyless("a root run id of %d bytes", len(m.RootRunID))
		}
	case KindSchedule:
		if m.RootRunID != "" {
			return keyless("a schedule's key field names a run")
		}
	default:
		return keyless("a key field of no known kind")
	}
	if len(m.Wrap) != nonceSize+KeySize+tagSize {
		return keyless("a wrap of %d bytes", len(m.Wrap))
	}
	return nil
}

// binding is what the wrap is bound to, in its key's derivation and its
// AAD. id is the root run id, or the schedule id the cluster stamped.
func (m KeyMemo) binding(id string) ([]byte, error) {
	return Info(wrapLabel, m.Kind, m.Namespace, m.NamespaceID, id, m.DataKeyID, m.EngineKeyID)
}

func (m KeyMemo) boundID(scheduleID string) string {
	if m.Kind == KindSchedule {
		return scheduleID
	}
	return m.RootRunID
}

// ParseKeyMemo reads a key field's JSON strictly: an unknown field, trailing
// data, an unknown version or kind, or a field out of shape is ErrKeyless.
func ParseKeyMemo(raw []byte) (KeyMemo, error) {
	if len(raw) > maxMemoBytes {
		return KeyMemo{}, keyless("a key field of %d bytes", len(raw))
	}
	var m KeyMemo
	// json/v2 refuses trailing data, duplicate names and, with this option,
	// unknown ones; names match exactly.
	if err := json.Unmarshal(raw, &m, json.RejectUnknownMembers(true)); err != nil {
		return KeyMemo{}, keyless("the key field does not parse")
	}
	if err := m.check(); err != nil {
		return KeyMemo{}, err
	}
	// Only the canonical form this package writes: no other spelling of a
	// field's name, no duplicate, no other order or whitespace.
	if canon, err := json.Marshal(m); err != nil || !bytes.Equal(canon, raw) {
		return KeyMemo{}, keyless("a key field not in its canonical form")
	}
	return m, nil
}

// Open unwraps the key m carries with the engines' key it names.
// scheduleID is the id the cluster stamped on a fire, and is not read for a
// run. A memo naming an engines' key keys does not hold is
// ErrEngineKeyMissing; anything else that does not yield the key is
// ErrKeyless.
func (m KeyMemo) Open(keys *EngineKeys, scheduleID string) (*Key, error) {
	if err := m.check(); err != nil {
		return nil, err
	}
	ek, ok := keys.Get(m.EngineKeyID)
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrEngineKeyMissing, m.EngineKeyID)
	}
	id := m.boundID(scheduleID)
	if id == "" || len(id) > MaxContextID {
		return nil, keyless("a bound id of %d bytes", len(id))
	}
	info, err := m.binding(id)
	if err != nil {
		return nil, keyless("%v", err)
	}
	wk, err := derive(ek, wrapLabel, string(info))
	if err != nil {
		return nil, keyless("%v", err)
	}
	aead, err := gcmOf(wk)
	if err != nil {
		return nil, keyless("%v", err)
	}
	raw, err := aead.Open(nil, m.Wrap[:nonceSize], m.Wrap[nonceSize:], info)
	if err != nil {
		return nil, keyless("the wrap does not open")
	}
	defer clear(raw)
	return NewKey(raw)
}

func wrap(engine *Key, m KeyMemo, key *Key, id string, random io.Reader) (KeyMemo, error) {
	m.Version = keyMemoVersion
	m.EngineKeyID = engine.ID()
	m.Wrap = make([]byte, nonceSize+KeySize+tagSize) // its final size, for check
	if id == "" || len(id) > MaxContextID {
		return KeyMemo{}, fmt.Errorf("payloadcrypt: a bound id of %d bytes", len(id))
	}
	if err := m.check(); err != nil {
		// A caller's mistake at submit, never a run's keyless start.
		return KeyMemo{}, fmt.Errorf("payloadcrypt: an invalid key field: %v", err)
	}
	info, err := m.binding(id)
	if err != nil {
		return KeyMemo{}, err
	}
	wk, err := derive(engine, wrapLabel, string(info))
	if err != nil {
		return KeyMemo{}, err
	}
	aead, err := gcmOf(wk)
	if err != nil {
		return KeyMemo{}, err
	}
	nonce := m.Wrap[:nonceSize]
	if _, err := io.ReadFull(random, nonce); err != nil {
		return KeyMemo{}, fmt.Errorf("payloadcrypt: nonce: %w", err)
	}
	m.Wrap = aead.Seal(nonce, nonce, key.b[:], info)
	return m, nil
}

// DataKeySource gives an engine its current data key for a namespace: made
// at the namespace's provider (P20b), or a fixed one in tests.
type DataKeySource interface {
	DataKey(ctx context.Context, namespace string) (id string, key *Key, err error)
}

// StaticDataKeys is a DataKeySource of one fixed data key, for tests.
type StaticDataKeys struct {
	id  string
	key *Key
}

// NewStaticDataKeys is a source that answers id and key for every namespace.
func NewStaticDataKeys(id string, key *Key) *StaticDataKeys {
	return &StaticDataKeys{id: id, key: key}
}

// DataKey answers the fixed key.
func (s *StaticDataKeys) DataKey(context.Context, string) (string, *Key, error) {
	return s.id, s.key, nil
}

// NewRunTree derives the key of a new run tree rooted at rootRunID from the
// namespace's current data key, and wraps it under engine for the run's
// memo. Each run of the tree carries the same memo field.
func NewRunTree(ctx context.Context, src DataKeySource, engine *Key, namespace, namespaceID, rootRunID string) (*Key, KeyMemo, error) {
	dkID, dk, err := src.DataKey(ctx, namespace)
	if err != nil {
		return nil, KeyMemo{}, fmt.Errorf("payloadcrypt: data key of %s: %w", namespace, err)
	}
	if dk == nil || engine == nil {
		return nil, KeyMemo{}, errors.New("payloadcrypt: a run tree needs a data key and an engines' key")
	}
	tree, err := DeriveRunTreeKey(dk, rootRunID)
	if err != nil {
		return nil, KeyMemo{}, err
	}
	m, err := wrap(engine, KeyMemo{Kind: KindRun, DataKeyID: dkID, Namespace: namespace, NamespaceID: namespaceID, RootRunID: rootRunID}, tree, rootRunID, rand.Reader)
	if err != nil {
		return nil, KeyMemo{}, err
	}
	return tree, m, nil
}

// NewSchedule derives a schedule's key, by the id the cluster holds it
// under, and wraps it for its action's memo.
func NewSchedule(ctx context.Context, src DataKeySource, engine *Key, namespace, namespaceID, scheduleID string) (*Key, KeyMemo, error) {
	dkID, dk, err := src.DataKey(ctx, namespace)
	if err != nil {
		return nil, KeyMemo{}, fmt.Errorf("payloadcrypt: data key of %s: %w", namespace, err)
	}
	if dk == nil || engine == nil {
		return nil, KeyMemo{}, errors.New("payloadcrypt: a schedule needs a data key and an engines' key")
	}
	key, err := DeriveScheduleKey(dk, scheduleID)
	if err != nil {
		return nil, KeyMemo{}, err
	}
	m, err := wrap(engine, KeyMemo{Kind: KindSchedule, DataKeyID: dkID, Namespace: namespace, NamespaceID: namespaceID}, key, scheduleID, rand.Reader)
	if err != nil {
		return nil, KeyMemo{}, err
	}
	return key, m, nil
}

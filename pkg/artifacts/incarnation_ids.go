package artifacts

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/typeid"
)

// IncarnationPrefix holds the immutable ids of namespaces and Orkflows
// (docs/ACCESS.md, "Pinned bindings"). A name can be made
// again after a delete; an id cannot. So each record is bound to one
// incarnation of what it names, and a record bound to another is ignored:
// a namespace's to its document's created_at, an Orkflow's to its head's
// since. Both lie under the prefix only an engine's credential writes, and
// neither lives in the namespace document or the head: an engine of an
// earlier release rewrites those with unknown fields dropped, and an id that
// could be erased and minted again is not an id.
//
//	metadata/identity/namespaces/{namespace}.json
//	metadata/identity/orkflows/{namespace}/{name}.json
const IncarnationPrefix = IdentityPrefix

// DefaultNamespaceID is the id of `default`, which is never stored and never
// deleted.
const DefaultNamespaceID = "ns_default"

const (
	// NamespaceIDPrefix and OrkflowIDPrefix begin every minted id. An id is
	// opaque: nothing parses one.
	NamespaceIDPrefix = "ns_"
	OrkflowIDPrefix   = "of_"

	incarnationFormat   = 1
	incarnationTries    = 5
	incarnationMaxBytes = 64 << 10
)

// ErrIncarnationUnreadable is a stored id record that does not decode, is of
// a format this release does not read, or names another thing. It is never
// read as "no id" and never overwritten: an operator removes the object, and
// the next need writes a new id, which leaves every binding pinned to the
// old one stale until `--repin`.
var ErrIncarnationUnreadable = errors.New("the id record is unreadable")

// ErrIncarnationContended is an Ensure that lost its conditional write every
// time.
var ErrIncarnationContended = errors.New("the id record is being changed by another engine; try again")

// incarnationRecord is one stored id.
type incarnationRecord struct {
	V    int    `json:"v"`
	Kind string `json:"kind"`
	// Namespace is the Orkflow's namespace; empty for a namespace's record.
	Namespace string `json:"namespace,omitzero"`
	Name      string `json:"name"`
	ID        string `json:"id"`
	// BoundTo is the incarnation the id belongs to: the namespace
	// document's created_at, or the Orkflow head's since (zero for a head
	// written before heads recorded one).
	BoundTo  time.Time `json:"boundTo"`
	MintedAt time.Time `json:"mintedAt"`
	// Policy is the namespace's identity policy, decoded on its own so a
	// policy this release cannot read never makes the id unreadable
	// (namespace_identity.go). An Orkflow's record holds none.
	Policy jsontext.Value `json:"policy,omitzero"`
}

// incarnationRef names one record.
type incarnationRef struct {
	kind, prefix, namespace, name, key string
}

func (r incarnationRef) String() string {
	if r.namespace != "" {
		return r.kind + " " + r.namespace + "/" + r.name
	}
	return r.kind + " " + r.name
}

// IncarnationStore keeps the ids of namespaces and Orkflows.
type IncarnationStore struct {
	blobBase
	now func() time.Time
}

// NewIncarnationStore opens the store on the deployment's backend.
func NewIncarnationStore(ctx context.Context, cfg Config) (*IncarnationStore, error) {
	b, err := openBlobs(ctx, cfg, "id store", IncarnationPrefix)
	if err != nil {
		return nil, err
	}
	return &IncarnationStore{blobBase: blobBase{b}, now: time.Now}, nil
}

func namespaceRef(namespace string) (incarnationRef, error) {
	if err := validNamespaceName(namespace); err != nil {
		return incarnationRef{}, fmt.Errorf("artifacts: namespace id: %w", err)
	}
	return incarnationRef{kind: "namespace", prefix: NamespaceIDPrefix, name: namespace,
		key: IncarnationPrefix + "/namespaces/" + namespace + ".json"}, nil
}

func orkflowRef(namespace, name string) (incarnationRef, error) {
	if _, err := orkflowNamePrefix(namespace, name); err != nil {
		return incarnationRef{}, fmt.Errorf("artifacts: orkflow id: %w", err)
	}
	return incarnationRef{kind: "orkflow", prefix: OrkflowIDPrefix, namespace: namespace, name: name,
		key: IncarnationPrefix + "/orkflows/" + namespace + "/" + name + ".json"}, nil
}

// NamespaceID reads a namespace's id for the incarnation created at
// createdAt. No record, or one bound to another incarnation, answers ok
// false; a read never writes one.
func (s *IncarnationStore) NamespaceID(ctx context.Context, namespace string, createdAt time.Time) (string, bool, error) {
	if namespace == DefaultNamespace {
		return DefaultNamespaceID, true, nil
	}
	ref, err := namespaceRef(namespace)
	if err != nil {
		return "", false, err
	}
	return s.read(ctx, ref, createdAt)
}

// EnsureNamespaceID answers a namespace's id for the incarnation created at
// createdAt, minting and storing one when none is stored for it.
func (s *IncarnationStore) EnsureNamespaceID(ctx context.Context, namespace string, createdAt time.Time) (string, error) {
	if namespace == DefaultNamespace {
		return DefaultNamespaceID, nil
	}
	ref, err := namespaceRef(namespace)
	if err != nil {
		return "", err
	}
	return s.ensure(ctx, ref, createdAt)
}

// DeleteNamespaceID removes the record of the namespace incarnation created
// at createdAt. A missing record is not an error, and one bound to another
// incarnation (the name was made again meanwhile) is kept. `default` has
// none to remove.
func (s *IncarnationStore) DeleteNamespaceID(ctx context.Context, namespace string, createdAt time.Time) error {
	if namespace == DefaultNamespace {
		return fmt.Errorf("artifacts: namespace %q has a constant id and nothing to delete", namespace)
	}
	ref, err := namespaceRef(namespace)
	if err != nil {
		return err
	}
	return s.remove(ctx, ref, createdAt)
}

// OrkflowID reads an Orkflow's id for the incarnation whose head began at
// since. No record, or one bound to another incarnation, answers ok false.
func (s *IncarnationStore) OrkflowID(ctx context.Context, namespace, name string, since time.Time) (string, bool, error) {
	ref, err := orkflowRef(namespace, name)
	if err != nil {
		return "", false, err
	}
	return s.read(ctx, ref, since)
}

// EnsureOrkflowID answers an Orkflow's id for the incarnation whose head
// began at since, minting and storing one when none is stored for it.
func (s *IncarnationStore) EnsureOrkflowID(ctx context.Context, namespace, name string, since time.Time) (string, error) {
	ref, err := orkflowRef(namespace, name)
	if err != nil {
		return "", err
	}
	return s.ensure(ctx, ref, since)
}

// DeleteOrkflowID removes the record of the Orkflow incarnation that began
// at since, as DeleteNamespaceID does.
func (s *IncarnationStore) DeleteOrkflowID(ctx context.Context, namespace, name string, since time.Time) error {
	ref, err := orkflowRef(namespace, name)
	if err != nil {
		return err
	}
	return s.remove(ctx, ref, since)
}

// get reads ref's record and its version; a missing one is found false.
func (s *IncarnationStore) get(ctx context.Context, ref incarnationRef) (rec incarnationRecord, version string, found bool, err error) {
	raw, version, err := s.b.get(ctx, ref.key, incarnationMaxBytes+1)
	if errors.Is(err, errBlobNotFound) {
		return incarnationRecord{}, "", false, nil
	}
	if err != nil {
		return incarnationRecord{}, "", false, fmt.Errorf("artifacts: read %s: %w", s.b.uri(ref.key), err)
	}
	if len(raw) > incarnationMaxBytes {
		return incarnationRecord{}, "", false, fmt.Errorf("artifacts: %s is larger than %d bytes: %w", s.b.uri(ref.key), incarnationMaxBytes, ErrIncarnationUnreadable)
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return incarnationRecord{}, "", false, fmt.Errorf("artifacts: decode %s: %w: %w", s.b.uri(ref.key), ErrIncarnationUnreadable, err)
	}
	if rec.V != incarnationFormat || rec.Kind != ref.kind || rec.Namespace != ref.namespace || rec.Name != ref.name || rec.ID == "" {
		return incarnationRecord{}, "", false, fmt.Errorf("artifacts: %s names %s %q of namespace %q (format %d), not the %s: %w",
			s.b.uri(ref.key), rec.Kind, rec.Name, rec.Namespace, rec.V, ref, ErrIncarnationUnreadable)
	}
	return rec, version, true, nil
}

func (s *IncarnationStore) read(ctx context.Context, ref incarnationRef, boundTo time.Time) (string, bool, error) {
	rec, _, found, err := s.get(ctx, ref)
	if err != nil || !found || !rec.BoundTo.Equal(boundTo) {
		return "", false, err
	}
	return rec.ID, true, nil
}

func (s *IncarnationStore) ensure(ctx context.Context, ref incarnationRef, boundTo time.Time) (string, error) {
	for range incarnationTries {
		rec, version, found, err := s.get(ctx, ref)
		if err != nil {
			return "", err
		}
		if found && rec.BoundTo.Equal(boundTo) {
			return rec.ID, nil
		}
		next := incarnationRecord{V: incarnationFormat, Kind: ref.kind, Namespace: ref.namespace, Name: ref.name,
			ID: ref.prefix + typeid.New(), BoundTo: boundTo.UTC(), MintedAt: s.now().UTC()}
		body, err := stablejson.Marshal(next)
		if err != nil {
			return "", fmt.Errorf("artifacts: encode %s: %w", s.b.uri(ref.key), err)
		}
		if found {
			// A record of an earlier incarnation: replaced only while it is
			// still the one read.
			err = s.b.replace(ctx, ref.key, "application/json", body, version)
		} else {
			err = s.b.create(ctx, ref.key, "application/json", body)
		}
		if errors.Is(err, errBlobExists) || errors.Is(err, errBlobChanged) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("artifacts: write %s: %w", s.b.uri(ref.key), err)
		}
		return next.ID, nil
	}
	return "", fmt.Errorf("artifacts: %s: %w", ref, ErrIncarnationContended)
}

// remove deletes ref's record when it is bound to boundTo. A record a newer
// incarnation wrote is kept: the delete that would remove it is of the
// incarnation before. The read and the remove are two requests, so a
// re-create landing between them can still lose its record, which the next
// need writes again under a new id; the window is one request wide.
func (s *IncarnationStore) remove(ctx context.Context, ref incarnationRef, boundTo time.Time) error {
	rec, _, found, err := s.get(ctx, ref)
	if err != nil {
		return err
	}
	if !found || !rec.BoundTo.Equal(boundTo) {
		return nil
	}
	if err := s.b.remove(ctx, ref.key); err != nil {
		return fmt.Errorf("artifacts: delete %s: %w", s.b.uri(ref.key), err)
	}
	return nil
}

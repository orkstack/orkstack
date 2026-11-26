package eventsources

import (
	"context"
	"errors"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Store is the durable registry of event sources: one document a source,
// keyed by its namespace and name, and the service specs written under its
// id. Every operation reads the shared backend, so any replica answers for a
// source another wrote. Implementations live in pkg/artifacts.
type Store interface {
	// Create writes a new source's document only where its name holds
	// none; a taken name is ErrNameTaken. Every answer carries the
	// document's etag in its Etag field.
	Create(ctx context.Context, src *orkstackv1.EventSource) (*orkstackv1.EventSource, error)
	// Get reads one source. A missing one is ErrNotFound; one this release
	// cannot read is an *UnreadableError.
	Get(ctx context.Context, namespace, name string) (*orkstackv1.EventSource, error)
	// List reads a namespace's sources, by name. A document this release
	// cannot read is listed as an *UnreadableError in skipped, never
	// dropped in silence.
	List(ctx context.Context, namespace string) (docs []*orkstackv1.EventSource, skipped []*UnreadableError, err error)
	// Names is every source's name, by its namespace and name, with no read
	// of a document.
	Names(ctx context.Context) ([]Key, error)
	// Count is how many sources the deployment holds, readable or not.
	Count(ctx context.Context) (int, error)
	// Replace writes a source's document while it still carries etag; a
	// source that changed or vanished is ErrChanged.
	Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error)
	// PutSpec writes a service spec under the source's id and its digest,
	// create-only; one already there is success, since the digest names
	// its bytes.
	PutSpec(ctx context.Context, id, digest string, body []byte) error
	// RemoveSpec removes one service spec; one that is not there is success.
	RemoveSpec(ctx context.Context, id, digest string) error
	// ReadSpec reads a service spec's bytes, which hash to its digest; found
	// is false only when the store said it is not there.
	ReadSpec(ctx context.Context, id, digest string) (body []byte, found bool, err error)
	// Remove removes a document while it still carries etag; one that
	// changed is ErrChanged, and one that is gone is success. What the
	// source keeps under its id is its workflow's to remove.
	Remove(ctx context.Context, namespace, name, etag string) error
	// NamespaceHolds reports whether a namespace holds a source.
	NamespaceHolds(ctx context.Context, namespace string) (bool, error)
}

// Key names a source's document.
type Key struct{ Namespace, Name string }

var (
	// ErrNotFound is a name that holds no source.
	ErrNotFound = errors.New("eventsources: no such event source")
	// ErrNameTaken is a create under a name that holds a source.
	ErrNameTaken = errors.New("eventsources: the name holds an event source")
	// ErrSpecCorrupt is a service spec whose bytes are not its digest's.
	ErrSpecCorrupt = errors.New("its bytes are not its digest's")
	// ErrChanged is a conditional write or delete whose source changed,
	// vanished, or is another source, since it was read.
	ErrChanged = errors.New("eventsources: the event source changed since it was read")
)

// UnreadableError is a stored document this release cannot read: written by
// a newer release, or damaged. It names the source and holds nothing of it
// but, where it can be read and the document is its key's own, its id.
type UnreadableError struct {
	Namespace string
	Name      string
	Cause     string
	ID        string
	Etag      string
}

func (e *UnreadableError) Error() string {
	return ReasonDocumentUnreadable + ": event source " + e.Namespace + "/" + e.Name + ": " + e.Cause
}

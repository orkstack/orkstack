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
	// Count is how many sources the deployment holds, readable or not.
	Count(ctx context.Context) (int, error)
	// Replace writes a source's document while it still carries etag; a
	// source that changed or vanished is ErrChanged.
	Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error)
	// PutSpec writes a service spec under the source's id and its digest,
	// create-only; one already there is success, since the digest names
	// its bytes.
	PutSpec(ctx context.Context, id, digest string, body []byte) error
	// ReadSpec reads a service spec's bytes, which hash to its digest; found
	// is false only when the store said it is not there.
	ReadSpec(ctx context.Context, id, digest string) (body []byte, found bool, err error)
	// Delete removes what the source of this id keeps under its id, then
	// its document. A document of another id under the name is
	// ErrChanged and nothing is removed.
	Delete(ctx context.Context, namespace, name, id string) error
	// NamespaceHolds reports whether a namespace holds a source.
	NamespaceHolds(ctx context.Context, namespace string) (bool, error)
}

var (
	// ErrNotFound is a name that holds no source.
	ErrNotFound = errors.New("eventsources: no such event source")
	// ErrNameTaken is a create under a name that holds a source.
	ErrNameTaken = errors.New("eventsources: the name holds an event source")
	// ErrSpecsLeft is a delete that removed the document and could not
	// remove every spec under its id: the source is deleted, and what is
	// left admits nothing.
	ErrSpecsLeft = errors.New("eventsources: the source is deleted; some of its service specs were not removed")
	// ErrSpecCorrupt is a service spec whose bytes are not its digest's.
	ErrSpecCorrupt = errors.New("its bytes are not its digest's")
	// ErrChanged is a conditional write or delete whose source changed,
	// vanished, or is another source, since it was read.
	ErrChanged = errors.New("eventsources: the event source changed since it was read")
)

// UnreadableError is a stored document this release cannot read: written by
// a newer release, or damaged. It names the source and holds nothing of it.
type UnreadableError struct {
	Namespace string
	Name      string
	Cause     string
}

func (e *UnreadableError) Error() string {
	return ReasonDocumentUnreadable + ": event source " + e.Namespace + "/" + e.Name + ": " + e.Cause
}

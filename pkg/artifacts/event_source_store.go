package artifacts

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The event-source registry (docs/STORAGE.md): one document a source, keyed
// by its namespace and name, so a conditional create is what keeps a name
// unique in its namespace; and the immutable service specs, keyed by the
// source's id and their digest, so what one source keeps is never another's
// whatever is later created under its name.
const (
	EventSourcePrefix     = "metadata/event-sources"
	EventSourceSpecPrefix = "metadata/event-source-specs"
	// eventSourceFormat is the one document format this release writes and
	// reads. A document of another format, or with a field this release
	// does not know, was written by a newer release and is not read.
	eventSourceFormat = 1
	// maxEventSourceRecordBytes bounds a stored document as read: twice the
	// document bound, for its counters and its framing.
	maxEventSourceRecordBytes = 2 * eventsources.MaxDocumentBytes
)

var (
	eventSourceSlug   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,199}$`)
	eventSourceDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func eventSourceKey(namespace, name string) (string, error) {
	if !eventSourceSlug.MatchString(namespace) || !eventSourceSlug.MatchString(name) {
		return "", fmt.Errorf("artifacts: event source %q/%q: a namespace and a name are slugs", namespace, name)
	}
	return EventSourcePrefix + "/" + namespace + "/" + name + ".json", nil
}

func eventSourceSpecKey(id, digest string) (string, error) {
	if !eventsources.ValidID(id) || !eventSourceDigest.MatchString(digest) {
		return "", fmt.Errorf("artifacts: event source spec %q/%q: an id is evs- and 16 hex digits, a digest 64", id, digest)
	}
	return EventSourceSpecPrefix + "/" + id + "/" + digest + ".json", nil
}

// eventSourceRecord is a document as stored.
type eventSourceRecord struct {
	Format      int            `json:"format"`
	EventSource jsontext.Value `json:"eventSource"`
}

func marshalEventSource(src *orkstackv1.EventSource) ([]byte, error) {
	stored := proto.Clone(src).(*orkstackv1.EventSource)
	// The etag is the object's version, never part of its bytes; the paused
	// flag is derived from the pause on read.
	stored.Etag = ""
	stored.Paused = false
	body, err := protojson.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode event source %q: %w", src.GetName(), err)
	}
	return json.Marshal(eventSourceRecord{Format: eventSourceFormat, EventSource: body})
}

// UnmarshalEventSourceRecord reads a stored document strictly: a format or
// a field this release does not know is refused, never read in part.
func UnmarshalEventSourceRecord(raw []byte) (*orkstackv1.EventSource, error) {
	// One value, keys matched exactly, and no key this release does not
	// know: encoding/json/v2 refuses trailing data by itself.
	var rec eventSourceRecord
	if err := json.Unmarshal(raw, &rec, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("the record does not decode")
	}
	if rec.Format != eventSourceFormat {
		return nil, fmt.Errorf("format %d; this release reads %d", rec.Format, eventSourceFormat)
	}
	src := &orkstackv1.EventSource{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(rec.EventSource, src); err != nil {
		return nil, fmt.Errorf("the document has a field this release does not read")
	}
	src.Paused = src.GetPause() != nil
	return src, nil
}

// BlobEventSourceStore is the event-source registry over the object store
// every replica reads.
type BlobEventSourceStore struct {
	blobBase
}

var _ eventsources.Store = (*BlobEventSourceStore)(nil)

// NewEventSourceStore opens the registry on the backend cfg selects.
func NewEventSourceStore(ctx context.Context, cfg Config) (*BlobEventSourceStore, error) {
	b, err := openBlobs(ctx, cfg, "event source store", EventSourcePrefix, EventSourceSpecPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobEventSourceStore{blobBase: blobBase{b}}, nil
}

// NewLocalEventSourceStore opens (creating if needed) the registry under
// dir.
func NewLocalEventSourceStore(dir string) (*BlobEventSourceStore, error) {
	b, err := openLocalBlobs("event source store", dir, EventSourcePrefix, EventSourceSpecPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobEventSourceStore{blobBase: blobBase{b}}, nil
}

// Create writes a new document create-only.
func (s *BlobEventSourceStore) Create(ctx context.Context, src *orkstackv1.EventSource) (*orkstackv1.EventSource, error) {
	key, err := eventSourceKey(src.GetNamespace(), src.GetName())
	if err != nil {
		return nil, err
	}
	body, err := marshalEventSource(src)
	if err != nil {
		return nil, err
	}
	if err := s.b.create(ctx, key, "application/json", body); err != nil {
		if errors.Is(err, errBlobExists) {
			return nil, eventsources.ErrNameTaken
		}
		return nil, fmt.Errorf("artifacts: create event source %s/%s: %w", src.GetNamespace(), src.GetName(), err)
	}
	return s.written(ctx, src), nil
}

// written is a document this store has just written, as a caller reads
// it: with its etag when a read of it right after the write finds exactly
// what was written, and with none otherwise. The write happened either way,
// so a read that fails, or that finds another writer's document, is never
// answered as a failed write.
func (s *BlobEventSourceStore) written(ctx context.Context, src *orkstackv1.EventSource) *orkstackv1.EventSource {
	out := proto.Clone(src).(*orkstackv1.EventSource)
	out.Etag, out.Paused = "", out.GetPause() != nil
	read, err := s.Get(ctx, src.GetNamespace(), src.GetName())
	if err != nil {
		return out
	}
	etag := read.GetEtag()
	read.Etag = ""
	if !proto.Equal(read, out) {
		return out
	}
	out.Etag = etag
	return out
}

// Get reads one document with its etag.
func (s *BlobEventSourceStore) Get(ctx context.Context, namespace, name string) (*orkstackv1.EventSource, error) {
	key, err := eventSourceKey(namespace, name)
	if err != nil {
		return nil, err
	}
	return s.read(ctx, key, namespace, name)
}

func (s *BlobEventSourceStore) read(ctx context.Context, key, namespace, name string) (*orkstackv1.EventSource, error) {
	raw, version, err := s.b.get(ctx, key, maxEventSourceRecordBytes+1)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, eventsources.ErrNotFound
		}
		return nil, fmt.Errorf("artifacts: read event source %s/%s: %w", namespace, name, err)
	}
	if len(raw) > maxEventSourceRecordBytes {
		return nil, &eventsources.UnreadableError{Namespace: namespace, Name: name,
			Cause: fmt.Sprintf("the document is over %d bytes", maxEventSourceRecordBytes)}
	}
	src, err := UnmarshalEventSourceRecord(raw)
	if err != nil {
		return nil, &eventsources.UnreadableError{Namespace: namespace, Name: name, Cause: err.Error()}
	}
	if src.GetNamespace() != namespace || src.GetName() != name {
		return nil, &eventsources.UnreadableError{Namespace: namespace, Name: name,
			Cause: "the document names another source than its key"}
	}
	src.Etag = version
	return src, nil
}

// List reads one namespace's documents, by name.
func (s *BlobEventSourceStore) List(ctx context.Context, namespace string) ([]*orkstackv1.EventSource, []*eventsources.UnreadableError, error) {
	if !eventSourceSlug.MatchString(namespace) {
		return nil, nil, fmt.Errorf("artifacts: list event sources: namespace %q is not a slug", namespace)
	}
	prefix := EventSourcePrefix + "/" + namespace + "/"
	var docs []*orkstackv1.EventSource
	var skipped []*eventsources.UnreadableError
	err := listObjects(ctx, s.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		name, ok := strings.CutSuffix(strings.TrimPrefix(object.Key, prefix), ".json")
		if !ok || !eventSourceSlug.MatchString(name) {
			// An object under the prefix no source could have written is
			// reported, never left out of a count it is in.
			skipped = append(skipped, &eventsources.UnreadableError{Namespace: namespace, Name: strings.TrimPrefix(object.Key, prefix),
				Cause: "the key names no event source"})
			return false, nil
		}
		src, err := s.read(ctx, object.Key, namespace, name)
		var unreadable *eventsources.UnreadableError
		switch {
		case errors.As(err, &unreadable):
			skipped = append(skipped, unreadable)
		case errors.Is(err, eventsources.ErrNotFound):
			// Deleted between the listing and the read.
		case err != nil:
			return true, err
		default:
			docs = append(docs, src)
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("artifacts: list event sources of %q: %w", namespace, err)
	}
	return docs, skipped, nil
}

// Count is how many documents the deployment holds.
func (s *BlobEventSourceStore) Count(ctx context.Context) (int, error) {
	n := 0
	err := listObjects(ctx, s.b, blobListInput{Prefix: EventSourcePrefix + "/"}, func(object blobObject) (bool, error) {
		if strings.HasSuffix(object.Key, ".json") {
			n++
		}
		return false, nil
	})
	if err != nil {
		return 0, fmt.Errorf("artifacts: count event sources: %w", err)
	}
	return n, nil
}

// NamespaceHolds reports whether a namespace holds a document.
func (s *BlobEventSourceStore) NamespaceHolds(ctx context.Context, namespace string) (bool, error) {
	if !eventSourceSlug.MatchString(namespace) {
		return false, fmt.Errorf("artifacts: namespace %q is not a slug", namespace)
	}
	page, err := s.b.list(ctx, blobListInput{Prefix: EventSourcePrefix + "/" + namespace + "/", MaxKeys: 1})
	if err != nil {
		return false, fmt.Errorf("artifacts: does namespace %q hold an event source: %w", namespace, err)
	}
	return len(page.Objects) > 0, nil
}

// Replace writes a document while it still carries etag.
func (s *BlobEventSourceStore) Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error) {
	key, err := eventSourceKey(src.GetNamespace(), src.GetName())
	if err != nil {
		return nil, err
	}
	if etag == "" {
		return nil, eventsources.ErrChanged
	}
	body, err := marshalEventSource(src)
	if err != nil {
		return nil, err
	}
	if err := s.b.replace(ctx, key, "application/json", body, etag); err != nil {
		if errors.Is(err, errBlobChanged) {
			return nil, eventsources.ErrChanged
		}
		return nil, fmt.Errorf("artifacts: write event source %s/%s: %w", src.GetNamespace(), src.GetName(), err)
	}
	return s.written(ctx, src), nil
}

// PutSpec writes a service spec create-only.
func (s *BlobEventSourceStore) PutSpec(ctx context.Context, id, digest string, body []byte) error {
	key, err := eventSourceSpecKey(id, digest)
	if err != nil {
		return err
	}
	if err := s.b.create(ctx, key, "application/json", body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write event source spec %s/%s: %w", id, digest, err)
	}
	return nil
}

// Delete removes the document of the source of this id, then the specs
// under that id. A document this release cannot decode is still deleted by
// the id its caller names, where its own id cannot be read or is that id:
// an unreadable document is never a source nobody can remove. Specs are
// removed only for an id the document itself carried, so a caller's id
// never removes another source's. Just before the document is removed its
// version is read again, and a document that changed meanwhile is
// ErrChanged with nothing removed; the remove itself is not conditional, so
// a delete and a create of the same name racing in that last instant are
// not excluded (no source runs in this release, and the next release
// replaces this delete with a mark). Specs that cannot all be removed after
// the document was are ErrSpecsLeft: the source is deleted.
func (s *BlobEventSourceStore) Delete(ctx context.Context, namespace, name, id string) error {
	key, err := eventSourceKey(namespace, name)
	if err != nil {
		return err
	}
	if !eventsources.ValidID(id) {
		return fmt.Errorf("artifacts: delete event source %s/%s: id %q is not an event source's id", namespace, name, id)
	}
	raw, version, err := s.b.get(ctx, key, maxEventSourceRecordBytes+1)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return eventsources.ErrNotFound
		}
		return fmt.Errorf("artifacts: read event source %s/%s: %w", namespace, name, err)
	}
	stored := ""
	if src, err := UnmarshalEventSourceRecord(raw); err == nil {
		stored = src.GetId()
	} else {
		stored = tolerantRecordID(raw)
	}
	if stored != "" && stored != id {
		return eventsources.ErrChanged
	}
	if _, again, err := s.b.get(ctx, key, maxEventSourceRecordBytes+1); err != nil {
		if errors.Is(err, errBlobNotFound) {
			return eventsources.ErrNotFound
		}
		return fmt.Errorf("artifacts: read event source %s/%s: %w", namespace, name, err)
	} else if again != version {
		return eventsources.ErrChanged
	}
	if err := s.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: delete event source %s/%s: %w", namespace, name, err)
	}
	if stored == "" {
		return nil
	}
	keys, err := listKeys(ctx, s.b, EventSourceSpecPrefix+"/"+id+"/")
	if err != nil {
		return fmt.Errorf("%w: list %s: %v", eventsources.ErrSpecsLeft, id, err)
	}
	for start := 0; start < len(keys); start += blobRemoveBatch {
		end := min(start+blobRemoveBatch, len(keys))
		failed, err := s.b.removeKeys(ctx, keys[start:end])
		if err != nil {
			return fmt.Errorf("%w: remove the specs of %s: %v", eventsources.ErrSpecsLeft, id, err)
		}
		for key, cause := range failed {
			return fmt.Errorf("%w: remove %s: %v", eventsources.ErrSpecsLeft, key, cause)
		}
	}
	return nil
}

// tolerantRecordID reads the id of a record this release cannot decode in
// full, "" when even that cannot be read.
func tolerantRecordID(raw []byte) string {
	var rec struct {
		EventSource struct {
			ID string `json:"id"`
		} `json:"eventSource"`
	}
	if json.Unmarshal(raw, &rec) != nil || !eventsources.ValidID(rec.EventSource.ID) {
		return ""
	}
	return rec.EventSource.ID
}

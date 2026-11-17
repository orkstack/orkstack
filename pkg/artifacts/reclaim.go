package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// The artifact GC's store primitives (docs/STORAGE.md, "Reclaiming space").
// Nothing here decides anything: internal/archive's Reclaimer reads these
// facts, decides, and calls the removals. Each removal is idempotent, so two
// replicas reclaiming one run converge, and a killed pass is simply redone.

const (
	// ReclaimRecordNamespace prefixes the create-only GC records: one per run
	// whose objects the GC has finished with, in delete mode only.
	ReclaimRecordNamespace = "metadata/cleanup/objects"
	// maxReclaimRecord bounds one GC record read.
	maxReclaimRecord = 4 << 20
	// maxObjectList bounds one run-objects list read: a fan-out at the item
	// ceiling lists a few hundred thousand ids, well under this.
	maxObjectList = 256 << 20
)

// ErrStoreRefused reports a delete the store refused on purpose: access
// denied, or an object lock. The object stays.
var ErrStoreRefused = errors.New("artifacts: the store refused the delete")

// refusal wraps err in ErrStoreRefused when the store refused the request
// itself (403, AccessDenied, an Object Lock refusal), not when it failed.
func refusal(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "AllAccessDisabled", "InvalidObjectState", "ObjectLockConfigurationNotFoundError":
			return fmt.Errorf("%w: %w", ErrStoreRefused, err)
		}
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusForbidden {
		return fmt.Errorf("%w: %w", ErrStoreRefused, err)
	}
	return err
}

// ProducingRunFromObjectKey returns the run that produced an S3-layout
// object: the run segment of orkflows/{orkflow}/{run}/{task}/{name}/{digest}.
// Only a clean segment is the run id byte for byte; a sanitized segment, the
// unattributed segment, the local layout and anything malformed are not
// parseable, and the GC keeps such an object (unparseable_key).
func ProducingRunFromObjectKey(objectID string) (string, bool) {
	segments, ok := splitS3ObjectKey(objectID)
	if !ok {
		return "", false
	}
	run := segments[2]
	if run == s3UnattributedComponent || !isCleanS3Component(run) {
		return "", false
	}
	return run, true
}

// ObjectProducerPrefix is the prefix every object one run produced under
// one orkflow segment lies under, taken from one of its own object ids:
// orkflows/{orkflow}/{run}/. ok is false for an id ProducingRunFromObjectKey
// cannot parse.
func ObjectProducerPrefix(objectID string) (string, bool) {
	if _, ok := ProducingRunFromObjectKey(objectID); !ok {
		return "", false
	}
	segments, _ := splitS3ObjectKey(objectID)
	return path.Join(segments[0], segments[1], segments[2]) + "/", true
}

// ObjectTaskAndName reads an S3-layout object id's task and artifact
// segments: the exact name when the segment is clean, the readable head of a
// sanitized one otherwise. ok is false for any other id.
func ObjectTaskAndName(objectID string) (task, name string, ok bool) {
	segments, ok := splitS3ObjectKey(objectID)
	if !ok {
		return "", "", false
	}
	return segments[3], segments[4], true
}

// StoredObject is one object as a listing or a stat reports it. Modified is
// the store's own clock.
type StoredObject struct {
	Key      string
	Size     int64
	Modified time.Time
}

// ReclaimRecord is the create-only record that the GC finished with one run:
// its objects are deleted, or kept for a stated reason no pass revisits.
type ReclaimRecord struct {
	RunID          string    `json:"run_id"`
	ReclaimedAt    time.Time `json:"reclaimed_at"`
	ObjectsDeleted int64     `json:"objects_deleted,omitempty"`
	BytesDeleted   int64     `json:"bytes_deleted,omitempty"`
	// LateCommit says a commit appeared under the run's commit prefix after
	// it was retired: nothing the run produced is ever deleted.
	LateCommit bool `json:"late_commit,omitempty"`
	// Kept lists the run's objects kept for good (a counted leak), so a read
	// of one never answers ARTIFACT_RECLAIMED.
	Kept []string `json:"kept,omitempty"`
}

// ReclaimRecordKey renders a run's GC record key.
func ReclaimRecordKey(runID string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: reclaim record key: run id: %w", err)
	}
	return path.Join(ReclaimRecordNamespace, segment) + ".json", nil
}

func decodeReclaimRecord(runID string, body []byte) (ReclaimRecord, error) {
	var rec ReclaimRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return ReclaimRecord{}, fmt.Errorf("decode: %w", err)
	}
	if rec.RunID != runID {
		return ReclaimRecord{}, fmt.Errorf("record names run %q", rec.RunID)
	}
	return rec, nil
}

// StoreVersioned reports whether the bucket keeps versions, where a delete
// only adds a delete marker. A local directory never does.
func (a *BlobRunArchive) StoreVersioned(ctx context.Context) (bool, error) {
	s3b, ok := a.b.(*s3Blobs)
	if !ok {
		return false, nil
	}
	out, err := s3b.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s3b.bucket)})
	if err != nil {
		return false, fmt.Errorf("artifacts: read bucket versioning: %w", err)
	}
	return out.Status == s3types.BucketVersioningStatusEnabled || out.Status == s3types.BucketVersioningStatusSuspended, nil
}

// Local reports whether this archive is a local directory, where the GC
// reclaims nothing: a local object key carries no producing run.
func (a *BlobRunArchive) Local() bool { return a.local }

// StoreNow reads the store's clock with the one store-clock probe
// (StoreClock.Read) and returns a lower bound of the store's now, so every
// age the GC reads from it reads late, never early.
func (a *BlobRunArchive) StoreNow(ctx context.Context) (time.Time, error) {
	clock := StoreClock{blobBase: a.blobBase, now: time.Now}
	r, err := clock.Read(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return r.EarliestStoreNow(time.Now()), nil
}

// CleanupSidecar returns a run's cleanup sidecar and the latest store time
// its last-modified stamp stands for: when the run was retired, on the
// store's clock, never earlier than it was.
// A sidecar that names another run is an error, never a retirement.
func (a *BlobRunArchive) CleanupSidecar(ctx context.Context, runID string) (RunCleanupState, time.Time, error) {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return RunCleanupState{}, time.Time{}, err
	}
	info, err := a.b.stat(ctx, key)
	if errors.Is(err, errBlobNotFound) {
		return RunCleanupState{}, time.Time{}, fmt.Errorf("artifacts: cleanup sidecar %q: %w", runID, ErrNotFound)
	}
	if err != nil {
		return RunCleanupState{}, time.Time{}, fmt.Errorf("artifacts: cleanup sidecar %q: stat: %w", runID, err)
	}
	state, err := a.CleanupState(ctx, runID)
	if err != nil {
		return RunCleanupState{}, time.Time{}, err
	}
	if state.RunID != runID {
		return RunCleanupState{}, time.Time{}, fmt.Errorf("artifacts: cleanup sidecar %q names run %q", runID, state.RunID)
	}
	if info.Modified.IsZero() {
		return RunCleanupState{}, time.Time{}, fmt.Errorf("artifacts: cleanup sidecar %q: the store reported no last-modified time", runID)
	}
	// The latest instant the stamp stands for: a retirement is never read
	// as older than it is.
	return state, LatestInstant(info.Modified.UTC()), nil
}

// RunObjectList reads a run's run-objects list. A run with none is
// ErrNotFound.
func (a *BlobRunArchive) RunObjectList(ctx context.Context, runID string) ([]string, error) {
	key, err := RunObjectListKey(runID)
	if err != nil {
		return nil, err
	}
	ids, found, err := a.readObjectList(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("artifacts: run objects of %q: %w", runID, err)
	}
	if !found {
		return nil, fmt.Errorf("artifacts: run objects of %q: %w", runID, ErrNotFound)
	}
	return ids, nil
}

// readObjectList reads one list; found is false when there is none.
func (a *BlobRunArchive) readObjectList(ctx context.Context, key string) ([]string, bool, error) {
	body, _, err := a.b.get(ctx, key, maxObjectList+1)
	if errors.Is(err, errBlobNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %w", key, err)
	}
	if len(body) > maxObjectList {
		return nil, false, fmt.Errorf("read %q: larger than %d bytes", key, maxObjectList)
	}
	var ids []string
	if err := json.Unmarshal(body, &ids); err != nil {
		return nil, false, fmt.Errorf("decode %q: %w", key, err)
	}
	return ids, true, nil
}

// ListUnder lists at most limit objects under prefix, in key order, and
// whether more follow. A non-positive limit is refused: every GC listing is
// bounded.
func (a *BlobRunArchive) ListUnder(ctx context.Context, prefix string, limit int) ([]StoredObject, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("artifacts: list %q: a listing needs a positive bound", prefix)
	}
	var (
		out  []StoredObject
		more bool
	)
	err := listObjects(ctx, a.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		if len(out) == limit {
			more = true
			return true, nil
		}
		out = append(out, StoredObject{Key: object.Key, Size: object.Size, Modified: object.Modified.UTC()})
		return false, nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("artifacts: list %q: %w", prefix, err)
	}
	return out, more, nil
}

// StatObject reports one object's size and the store's last-modified time.
// A missing object is ErrNotFound.
func (a *BlobRunArchive) StatObject(ctx context.Context, key string) (StoredObject, error) {
	info, err := a.b.stat(ctx, key)
	if errors.Is(err, errBlobNotFound) {
		return StoredObject{}, fmt.Errorf("artifacts: stat %q: %w", key, ErrNotFound)
	}
	if err != nil {
		return StoredObject{}, fmt.Errorf("artifacts: stat %q: %w", key, err)
	}
	return StoredObject{Key: key, Size: info.Size, Modified: info.Modified.UTC()}, nil
}

// ObjectCompanionKey is the record that goes with a content-addressed object
// (LocalStore's sidecar, or S3Store's name manifest for a sanitized name), ""
// when there is none. ok is false for an id that is not canonical here.
func (a *BlobRunArchive) ObjectCompanionKey(objectID string) (string, bool) {
	s3Segments, ok := a.canonicalObjectID(objectID)
	if !ok || hasDotSegment(objectID) {
		return "", false
	}
	return a.objectCompanion(objectID, s3Segments), true
}

func hasDotSegment(key string) bool {
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." || segment == "" {
			return true
		}
	}
	return false
}

// RemoveReclaimedObject deletes one content-addressed object and then its
// companion. Only a canonical object id is ever removed, so no caller can
// turn a malformed id into a delete of a metadata key. A missing object is
// not an error.
func (a *BlobRunArchive) RemoveReclaimedObject(ctx context.Context, objectID string) error {
	// The GC deletes only S3-layout objects, whose key names their producer:
	// never a local-layout key, on either backend.
	if _, s3Layout := ProducingRunFromObjectKey(objectID); a.local || !s3Layout {
		return fmt.Errorf("%w: %q is not an object the artifact GC may delete", ErrInvalidURI, objectID)
	}
	companion, ok := a.ObjectCompanionKey(objectID)
	if !ok {
		return fmt.Errorf("%w: %q is not a canonical object id", ErrInvalidURI, objectID)
	}
	if err := a.b.remove(ctx, objectID); err != nil {
		return fmt.Errorf("artifacts: reclaim %q: %w", objectID, refusal(err))
	}
	if companion != "" {
		if err := a.b.remove(ctx, companion); err != nil {
			return fmt.Errorf("artifacts: reclaim %q: delete %s: %w", objectID, a.companionName(), refusal(err))
		}
	}
	return nil
}

// RemoveObjectReferences deletes the markers runIDs hold on objectID.
func (a *BlobRunArchive) RemoveObjectReferences(ctx context.Context, objectID string, runIDs []string) error {
	if _, ok := a.canonicalObjectID(objectID); !ok {
		return fmt.Errorf("%w: %q is not a canonical object id", ErrInvalidURI, objectID)
	}
	keys := make([]string, 0, len(runIDs))
	for _, runID := range runIDs {
		key, err := RunObjectRefKey(objectID, runID)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}
	for start := 0; start < len(keys); start += blobRemoveBatch {
		batch := keys[start:min(start+blobRemoveBatch, len(keys))]
		if failed, err := a.b.removeKeys(ctx, batch); err != nil || len(failed) > 0 {
			return fmt.Errorf("artifacts: remove references of %q: %w", objectID, errors.Join(err, joinDeleteFailures(failed)))
		}
	}
	return nil
}

// RemoveRunObjectList deletes a run's run-objects list. A missing list is
// not an error.
func (a *BlobRunArchive) RemoveRunObjectList(ctx context.Context, runID string) error {
	key, err := RunObjectListKey(runID)
	if err != nil {
		return err
	}
	if err := a.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: remove run objects of %q: %w", runID, err)
	}
	return nil
}

// CreateReclaimRecord writes a run's GC record once: one already there is
// the winner and is kept.
func (a *BlobRunArchive) CreateReclaimRecord(ctx context.Context, rec ReclaimRecord) error {
	key, err := ReclaimRecordKey(rec.RunID)
	if err != nil {
		return err
	}
	rec.ReclaimedAt = rec.ReclaimedAt.UTC()
	body, err := stablejson.Marshal(rec)
	if err != nil {
		return fmt.Errorf("artifacts: reclaim record %q: encode: %w", rec.RunID, err)
	}
	if err := a.b.create(ctx, key, RunCleanupContentType, body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: reclaim record %q: %w", rec.RunID, err)
	}
	return nil
}

// ReclaimRecord reads a run's GC record. A run with none is ErrNotFound.
func (a *BlobRunArchive) ReclaimRecord(ctx context.Context, runID string) (ReclaimRecord, error) {
	key, err := ReclaimRecordKey(runID)
	if err != nil {
		return ReclaimRecord{}, err
	}
	body, _, err := a.b.get(ctx, key, maxReclaimRecord+1)
	if errors.Is(err, errBlobNotFound) {
		return ReclaimRecord{}, fmt.Errorf("artifacts: reclaim record %q: %w", runID, ErrNotFound)
	}
	if err != nil {
		return ReclaimRecord{}, fmt.Errorf("artifacts: reclaim record %q: %w", runID, err)
	}
	if len(body) > maxReclaimRecord {
		return ReclaimRecord{}, fmt.Errorf("artifacts: reclaim record %q: larger than %d bytes", runID, maxReclaimRecord)
	}
	rec, err := decodeReclaimRecord(runID, body)
	if err != nil {
		return ReclaimRecord{}, fmt.Errorf("artifacts: reclaim record %q: %w", runID, err)
	}
	return rec, nil
}

// EachReclaimRecord visits the run id of every GC record, keys only, in key
// order. A key this package did not write is skipped.
func (a *BlobRunArchive) EachReclaimRecord(ctx context.Context, visit func(runID string) error) error {
	prefix := ReclaimRecordNamespace + "/"
	err := listObjects(ctx, a.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		name, ok := strings.CutSuffix(strings.TrimPrefix(object.Key, prefix), ".json")
		if !ok || strings.Contains(name, "/") {
			return false, nil
		}
		runID, ok := decodeArchiveSegment(name)
		if !ok {
			return false, nil
		}
		return false, visit(runID)
	})
	if err != nil {
		return fmt.Errorf("artifacts: list reclaim records: %w", err)
	}
	return nil
}

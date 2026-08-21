package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// An orkflow's versions are immutable, so something else has to say which one
// an empty version means. That is the current pointer:
//
//	s3://{bucket}/metadata/orkflows/{namespace}/{name}/@current.json
//
// It holds ONLY the version string and when it moved — never a copy of the
// document. A pointer that carried the definition would be a second place the
// definition lives, and the two would drift the first time somebody wrote one
// and not the other.
const (
	// orkflowMetaPrefix reserves a basename shape for per-orkflow metadata
	// stored beside the versions.
	//
	// A version component must already start alphanumeric
	// (cleanComponentPattern), so "@current.json" could never have been parsed
	// as one. The reservation is explicit anyway: it states the rule where a
	// reader of parseOrkflowKey looks for it, and it keeps the shelf clear for
	// the next such document rather than leaving the guarantee to a regex two
	// screens away.
	orkflowMetaPrefix = "@"

	// currentPointerBasename is the pointer object's name.
	currentPointerBasename = orkflowMetaPrefix + "current" + orkflowSuffix

	// setCurrentAttempts bounds the compare-and-swap retry. One retry is
	// enough for the property that matters: the loser of a race re-reads the
	// etag the winner just wrote and swaps against it. Retrying forever would
	// let two clients starve each other while both believe they are promoting.
	setCurrentAttempts = 2
)

// OrkflowCurrentKey renders the object key holding one orkflow's current
// pointer: metadata/orkflows/{namespace}/{name}/@current.json.
//
// Both coordinates are validated exactly as a version key's are, so a pointer
// can no more escape its prefix than a definition can.
func OrkflowCurrentKey(namespace, name string) (string, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return "", err
	}
	return prefix + currentPointerBasename, nil
}

// ParseOrkflowCurrentKey reverses OrkflowCurrentKey. A key that is not a
// pointer reports ok=false, so one prefix scan can sort definitions from
// pointers without a second request.
func ParseOrkflowCurrentKey(key string) (namespace, name string, ok bool) {
	return parseOrkflowMetaKey(key, currentPointerBasename)
}

// currentPointer is the pointer document's body.
type currentPointer struct {
	Version   string    `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// marshalCurrentPointer renders the pointer with the same normalization the
// definition documents use, so a human reading the bucket sees one shape.
func marshalCurrentPointer(pointer currentPointer) ([]byte, error) {
	document, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode current pointer: %w", err)
	}
	return append(document, '\n'), nil
}

// unmarshalCurrentPointer decodes a pointer document. Unknown fields are
// discarded, so an engine can read what a newer engine wrote.
func unmarshalCurrentPointer(document []byte) (currentPointer, error) {
	var pointer currentPointer
	if err := json.Unmarshal(document, &pointer); err != nil {
		return currentPointer{}, fmt.Errorf("decode current pointer: %w", err)
	}
	return pointer, nil
}

// CurrentVersion returns the version an orkflow's pointer names, or "" when no
// pointer was ever written for it.
//
// "" is not an error: it is the honest answer for an orkflow nobody has
// promoted, and it is what makes resolution fall back to the most recently
// applied version rather than refusing to run.
func (s *S3OrkflowStore) CurrentVersion(ctx context.Context, namespace, name string) (string, error) {
	pointer, _, err := s.readCurrent(ctx, namespace, name)
	if err != nil {
		return "", err
	}
	return pointer.Version, nil
}

// readCurrent reads the pointer and the etag naming the bytes it read. A
// missing pointer is the zero value and no error, with an empty etag — which
// is what tells the writer to swap with IfAbsent rather than IfMatch.
func (s *S3OrkflowStore) readCurrent(
	ctx context.Context,
	namespace, name string,
) (currentPointer, string, error) {
	key, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return currentPointer{}, "", err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return currentPointer{}, "", nil
		}
		return currentPointer{}, "", fmt.Errorf("artifacts: get current version %q: %w",
			s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return currentPointer{}, "", fmt.Errorf("artifacts: get current version %q: read body: %w",
			s.uri(key), err)
	}
	pointer, err := unmarshalCurrentPointer(document)
	if err != nil {
		return currentPointer{}, "", fmt.Errorf("artifacts: get current version %q: %w",
			s.uri(key), err)
	}
	return pointer, aws.ToString(object.ETag), nil
}

// SetCurrentVersion points an orkflow at an existing version and returns the
// version the pointer named before ("" when it was never set).
//
// Two things make this safe with two replicas writing at once, and both are
// load-bearing:
//
//  1. The target is READ first. A pointer is only ever written after its
//     target has been seen in the bucket, and versions are immutable and never
//     deleted, so a validated target cannot become invalid afterwards. That is
//     what makes "a reader never observes a pointer to a version that does not
//     exist" a property of the store rather than a hope about timing.
//
//  2. The write is a compare-and-swap (IfAbsent for the first pointer,
//     IfMatch afterwards), which S3 and MinIO both answer natively. Two
//     concurrent promotions therefore serialize: one lands, the other is
//     refused with PreconditionFailed, re-reads the etag the winner wrote and
//     swaps against it. The final value is one of the two versions asked for,
//     never a mixture and never a lost write silently reported as success.
//
// The retry deliberately does NOT re-validate the target: a version cannot
// stop existing, so re-reading it would cost a request to learn what is
// already known.
func (s *S3OrkflowStore) SetCurrentVersion(
	ctx context.Context,
	namespace, name, version string,
) (string, error) {
	versionKey, err := OrkflowKey(namespace, name, version)
	if err != nil {
		return "", err
	}
	pointerKey, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return "", err
	}
	if _, err := s.read(ctx, versionKey); err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", fmt.Errorf(
				"artifacts: cannot promote orkflow %q in namespace %q to version %q: "+
					"that version is not stored: %w",
				name, ResolveNamespace(namespace), version, ErrNotFound)
		}
		return "", err
	}

	var lastErr error
	for attempt := range setCurrentAttempts {
		existing, etag, err := s.readCurrent(ctx, namespace, name)
		if err != nil {
			return "", err
		}
		document, err := marshalCurrentPointer(currentPointer{
			Version:   version,
			UpdatedAt: time.Now().UTC(),
		})
		if err != nil {
			return "", err
		}
		input := &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(pointerKey),
			Body:          bytes.NewReader(document),
			ContentLength: aws.Int64(int64(len(document))),
			ContentType:   aws.String(orkflowContentType),
		}
		if etag == "" {
			input.IfNoneMatch = aws.String("*")
		} else {
			input.IfMatch = aws.String(etag)
		}
		if _, err := s.client.PutObject(ctx, input); err != nil {
			if isPreconditionFailed(err) {
				// Somebody else promoted between the read and the write. Their
				// value is in the bucket and is a real version; this attempt
				// swaps against the etag they just created.
				lastErr = fmt.Errorf(
					"artifacts: promote orkflow %q in namespace %q to version %q: "+
						"another writer moved the pointer first (attempt %d): %w",
					name, ResolveNamespace(namespace), version, attempt+1, ErrPreconditionFailed)
				continue
			}
			return "", fmt.Errorf("artifacts: promote orkflow %q to version %q (%s): %w",
				name, version, s.uri(pointerKey), err)
		}
		return existing.Version, nil
	}
	return "", lastErr
}

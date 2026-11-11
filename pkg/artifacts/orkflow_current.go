package artifacts

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
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
	document, err := stablejson.MarshalIndent(pointer, "  ")
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
	pointer, _, etag, err := s.readCurrentRaw(ctx, namespace, name)
	return pointer, etag, err
}

// readCurrentRaw is readCurrent with the pointer's bytes as stored, which a
// promotion moving the pointer back writes again exactly.
func (s *S3OrkflowStore) readCurrentRaw(
	ctx context.Context,
	namespace, name string,
) (currentPointer, []byte, string, error) {
	key, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return currentPointer{}, nil, "", err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return currentPointer{}, nil, "", nil
		}
		return currentPointer{}, nil, "", fmt.Errorf("artifacts: get current version %q: %w",
			s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return currentPointer{}, nil, "", fmt.Errorf("artifacts: get current version %q: read body: %w",
			s.uri(key), err)
	}
	pointer, err := unmarshalCurrentPointer(document)
	if err != nil {
		return currentPointer{}, nil, "", fmt.Errorf("artifacts: get current version %q: %w",
			s.uri(key), err)
	}
	return pointer, document, aws.ToString(object.ETag), nil
}

// ErrCurrentMoved reports a promotion whose expected current pointer was no
// longer the pointer when the store went to swap it. It wraps
// ErrPreconditionFailed, so a caller that only knew "lost a race" still
// recognises it.
var ErrCurrentMoved = fmt.Errorf("orkflow current pointer moved under this promotion: %w",
	ErrPreconditionFailed)

// CurrentMovedError names both pointers, so a refusal can say what the
// promotion compared against and what is there now.
type CurrentMovedError struct {
	Namespace, Name string
	Expected        string // "" = the pointer was expected to be unset
	Current         string // "" = the pointer is unset
}

func (e *CurrentMovedError) Error() string {
	return fmt.Sprintf("artifacts: orkflow %q in namespace %q: the current pointer names %s, "+
		"not %s as this promotion expected: %v",
		e.Name, e.Namespace, pointerWords(e.Current), pointerWords(e.Expected), ErrCurrentMoved)
}

// Unwrap makes a CurrentMovedError errors.Is both ErrCurrentMoved and
// ErrPreconditionFailed.
func (e *CurrentMovedError) Unwrap() error { return ErrCurrentMoved }

// pointerWords renders one side of a CurrentMovedError for a reader.
func pointerWords(version string) string {
	if version == "" {
		return "no version (it was never set)"
	}
	return fmt.Sprintf("%q", version)
}

// CurrentExpectation is what one promotion claims about the pointer it
// replaces. A type, not a string, for HeadExpectation's reason: "claims
// nothing" and "claims the pointer is unset" are different claims and ""
// would have to mean both.
type CurrentExpectation struct {
	version string
	stated  bool
}

// NoCurrentExpectation is the unconditional promotion (tests, tools).
func NoCurrentExpectation() CurrentExpectation { return CurrentExpectation{} }

// ExpectCurrent commits only while the pointer names version; "" means
// "only while no pointer has ever been written".
func ExpectCurrent(version string) CurrentExpectation {
	return CurrentExpectation{version: version, stated: true}
}

// Stated reports whether this expectation claims anything at all.
func (e CurrentExpectation) Stated() bool { return e.stated }

// Version is the pointer this expectation claims; meaningful only when Stated.
func (e CurrentExpectation) Version() string { return e.version }

// SatisfiedBy reports whether current is the pointer this promotion claims.
func (e CurrentExpectation) SatisfiedBy(current string) bool {
	return !e.stated || e.version == current
}

// SetCurrentVersion points an orkflow at an existing version and returns the
// version the pointer named before ("" when it was never set).
//
// Two things make this safe with two replicas writing at once, and both are
// load-bearing:
//
//  1. The target is READ first, and read again once the pointer names it. A
//     pointer is only ever written after its target has been seen in the
//     bucket, and versions are immutable; the one path that deletes one — Put
//     rolling back a refused apply's version, which is listed meanwhile — can
//     race a promotion of it, and each side reads the other back and puts the
//     version back: the rollback when its re-read finds the pointer naming it,
//     and a promotion whose target is gone while the pointer is still its own
//     write (confirmPromoted). Whichever restores first, the other finds the
//     version there, so every order ends with the pointer on a stored version;
//     a reader can see it name a missing version only until then.
//
//  2. The write is a compare-and-swap (IfAbsent for the first pointer,
//     IfMatch afterwards), which S3 and MinIO both answer natively. Two
//     concurrent promotions therefore serialize: one lands, the other is
//     refused with PreconditionFailed, re-reads the etag the winner wrote and
//     swaps against it. The final value is one of the two versions asked for,
//     never a mixture and never a lost write silently reported as success.
//
//  3. The expectation is checked on EVERY read of the pointer, inside the
//     loop, against the version that read returned. A stated expectation the
//     winner of a race broke is therefore a refusal (*CurrentMovedError), not
//     an overwrite: a promotion that compared its target against one pointer
//     never lands on top of another. An unstated expectation retries exactly
//     as point 2 describes, and a winner that re-wrote the SAME version (the
//     etag moved, the version did not) still lets the retry land.
//
//  4. A refusal is believed only once a read shows the swap's own write is not
//     there (putConfirmed). The SDK resends a swap whose answer it lost, and a
//     resend whose If-Match meets the first send's stored write answers 412,
//     or the SDK gives up, for a promotion that landed. The pointer document
//     carries its write's instant to the nanosecond, so the promotion is
//     reported as what it did: landed, over the version it read — or, when
//     the read fails too, an outcome not known (errOutcomeUnknown), never a
//     refusal and never a pointer somebody else moved.
//
// The retry deliberately does NOT re-validate the target: a version cannot
// stop existing, so re-reading it would cost a request to learn what is
// already known.
func (s *S3OrkflowStore) SetCurrentVersion(
	ctx context.Context,
	namespace, name, version string,
	expected CurrentExpectation,
) (string, error) {
	versionKey, err := OrkflowKey(namespace, name, version)
	if err != nil {
		return "", err
	}
	pointerKey, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return "", err
	}
	target, raw, _, err := s.readRaw(ctx, versionKey)
	if err != nil {
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
		existing, existingRaw, etag, err := s.readCurrentRaw(ctx, namespace, name)
		if err != nil {
			return "", err
		}
		if !expected.SatisfiedBy(existing.Version) {
			return "", &CurrentMovedError{Namespace: ResolveNamespace(namespace),
				Name: name, Expected: expected.Version(), Current: existing.Version}
		}
		written := currentPointer{Version: version, UpdatedAt: time.Now().UTC()}
		document, err := marshalCurrentPointer(written)
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
		// Equal bytes after a lost answer are this swap: the pointer carries
		// updated_at to the nanosecond. Were they another's, the pointer names
		// exactly the version this promotion meant it to.
		if _, err := putConfirmed(ctx, s.client, input, document); err != nil {
			if input.IfMatch != nil && isNotFound(err) {
				// The pointer this promotion read is gone, and only a Delete of
				// the whole orkflow removes it: writing it again from nothing
				// would bring a deleted orkflow back.
				return "", fmt.Errorf(
					"artifacts: cannot promote orkflow %q in namespace %q to version %q: "+
						"%s: %w",
					name, ResolveNamespace(namespace), version, deletedAsPromoted, ErrNotFound)
			}
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
		if err := s.confirmPromoted(ctx, namespace, name, versionKey, existing, existingRaw, written, target, raw); err != nil {
			return "", err
		}
		return existing.Version, nil
	}
	return "", lastErr
}

// confirmPromoted reads the target back once the pointer names it, and holds
// it to the document this promotion validated. Another document stored under
// the version since — a refused apply's removed, and the version created
// again with other content — is one this promotion's interface check never
// compared, and a promotion is never an unchecked move: while the pointer is
// still this promotion's own write, it is moved back to what it named before
// (cleared when it named nothing), under that write's etag, and the promotion
// is refused. The version is stored, so no rollback or restore is in play; a
// pointer another promotion has moved since is left alone.
//
// The one path that deletes a version alone — Put rolling back a refused
// apply's version — can remove it between this promotion's validation and its
// swap. The rollback's re-read of the pointers covers a swap that lands before
// that read; this covers one that lands after it. While the pointer is still
// this promotion's own write — its version at its instant, to the nanosecond —
// the version's exact bytes as this promotion read them are put back, created
// only if absent and held to that document (restoreVersion), and the promotion
// stands. The rollback restores the same bytes under the same condition, so
// whichever lands first, the other finds the version there.
//
// A Delete of the orkflow removes versions too, and a promotion landing inside
// one loses to it: the target gone with no head at all is a Delete, because a
// refused apply's rollback never leaves the head absent (its refusal is
// another apply's head, or a Delete's). This promotion then removes its own
// pointer, under that pointer's etag, puts nothing back, and is refused as the
// orkflow deleted. A pointer that is not this promotion's own any more is left
// alone: another promotion moved it on, or the Delete took it.
func (s *S3OrkflowStore) confirmPromoted(
	ctx context.Context,
	namespace, name, versionKey string,
	previous currentPointer,
	previousRaw []byte,
	written currentPointer,
	target *orkstackv1.Orkflow,
	raw []byte,
) error {
	namespace, version := ResolveNamespace(namespace), written.Version
	refused := func(why string, cause error) error {
		return fmt.Errorf("artifacts: cannot promote orkflow %q in namespace %q to version %q: %s: %w",
			name, namespace, version, why, cause)
	}
	switch found, _, _, err := s.readRaw(ctx, versionKey); {
	case err == nil && SameStoredOrkflow(found, target):
		return nil
	case err == nil:
		const another = "another document is stored under this version now (the one this promotion " +
			"compared was removed and the version created again)"
		what, err := s.unpromote(ctx, namespace, name, previous, previousRaw, written)
		if err != nil {
			return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: %s, and moving the "+
				"current pointer back failed: %w", name, namespace, version, another, err)
		}
		return refused(another+"; "+what, ErrVersionExists)
	case !errors.Is(err, ErrNotFound):
		return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: the version could not "+
			"be read back: %w", name, namespace, version, err)
	}
	pointer, etag, err := s.readCurrent(ctx, namespace, name)
	switch {
	case err != nil:
		return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: the version was removed "+
			"as it was promoted, and the current pointer could not be read again: %w", name, namespace, version, err)
	case etag == "":
		return refused(s.deletedOrRecreated(ctx, namespace, name), ErrNotFound)
	case pointer.Version != version || !pointer.UpdatedAt.Equal(written.UpdatedAt):
		return refused(fmt.Sprintf("the version was removed as it was promoted, and the current pointer has "+
			"moved since, to %q; nothing was put back", pointer.Version), ErrNotFound)
	}
	// A Delete, not a rollback, removed the target when there is no head at
	// all, or when the target was created before the head's incarnation
	// began: a refused apply's version is always stamped after a head of its
	// own incarnation. Either way this promotion removes its own pointer and
	// puts nothing back. A head without since says nothing, and the restore
	// below stands, as it did before the field.
	switch head, headETag, err := s.readHead(ctx, namespace, name); {
	case err != nil:
		return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: the version was removed "+
			"as it was promoted, and the head could not be read to tell a Delete from a refused apply: %w",
			name, namespace, version, err)
	case headETag == "":
		return s.loseToDelete(ctx, namespace, name, version, etag, deletedAsPromoted)
	case !head.Since.IsZero() && target.GetCreatedAt().AsTime().Before(head.Since):
		return s.loseToDelete(ctx, namespace, name, version, etag, recreatedAsPromoted)
	}
	if err := s.restoreVersion(ctx, versionKey, raw, target); err != nil {
		return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: the current pointer "+
			"names it, a refused apply removed it as it was promoted, and putting it back failed: %w",
			name, namespace, version, err)
	}
	return nil
}

// loseToDelete removes this promotion's own current pointer, under its etag,
// and refuses the promotion as one a Delete beat, for the reason given.
func (s *S3OrkflowStore) loseToDelete(ctx context.Context, namespace, name, version, etag, why string) error {
	pointerKey, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket),
		Key: aws.String(pointerKey), IfMatch: aws.String(etag)}); err != nil &&
		!isPreconditionFailed(err) && !isNotFound(err) {
		return fmt.Errorf("artifacts: promote orkflow %q in namespace %q to version %q: %s, and removing "+
			"this promotion's current pointer failed, so it still names the removed version (promote again or "+
			"delete the orkflow to clear it): %w", name, namespace, version, why, err)
	}
	return fmt.Errorf("artifacts: cannot promote orkflow %q in namespace %q to version %q: %s: %w",
		name, namespace, version, why, ErrNotFound)
}

// recreatedAsPromoted is what a promotion of a version a Delete removed is
// told once another apply has re-created the orkflow.
const recreatedAsPromoted = "the orkflow was deleted as it was promoted, and another apply has re-created it " +
	"since; this version is gone"

// deletedOrRecreated is what a promotion whose pointer a Delete took is told:
// deletedAsPromoted while the orkflow has no head, and, while a head is there,
// that another apply has re-created it — deleting it again would delete the
// re-created one — or that a Delete between its two pointer deletes is still
// removing it.
func (s *S3OrkflowStore) deletedOrRecreated(ctx context.Context, namespace, name string) string {
	if _, etag, err := s.readHead(ctx, namespace, name); err == nil && etag != "" {
		return "the orkflow was deleted as it was promoted, and another apply has re-created it since, or a " +
			"Delete is still removing it; this version is gone"
	}
	return deletedAsPromoted
}

// deletedAsPromoted is what a promotion that lost to a Delete of its orkflow is
// told: a Delete that stopped part way, before its versions, reads the same.
const deletedAsPromoted = "the orkflow was deleted as it was promoted, or a Delete of it stopped part way " +
	"(deleting it again finishes it)"

// unpromote moves the current pointer back to previous — its exact bytes, so
// the promotion that wrote them still knows its own write — or clears it, when
// previous named nothing, while it is still the write this promotion made,
// under that write's etag, and says what it did. A pointer moved since is
// another writer's, and is left alone.
func (s *S3OrkflowStore) unpromote(
	ctx context.Context,
	namespace, name string,
	previous currentPointer,
	previousRaw []byte,
	written currentPointer,
) (string, error) {
	pointer, etag, err := s.readCurrent(ctx, namespace, name)
	switch {
	case err != nil:
		return "", err
	case etag == "" || pointer.Version != written.Version || !pointer.UpdatedAt.Equal(written.UpdatedAt):
		return "the current pointer has moved since, and was left alone", nil
	}
	pointerKey, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return "", err
	}
	if previous.Version == "" {
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket),
			Key: aws.String(pointerKey), IfMatch: aws.String(etag)})
		switch {
		case err == nil:
			return "the current pointer was cleared, as it was before", nil
		case isPreconditionFailed(err) || isNotFound(err):
			// Refused, or a resend of a delete that landed meeting nothing: the
			// pointer as it is now says which.
			_, now, readErr := s.readCurrent(ctx, namespace, name)
			switch {
			case readErr != nil:
				return "", fmt.Errorf("its clear was refused, and the current pointer could not be read again: %w", readErr)
			case now == "":
				return "the current pointer was cleared, as it was before", nil
			}
			return "the current pointer has moved since, and was left alone", nil
		}
		return "", err
	}
	// The version it named before must still be stored, or moving back would
	// leave the pointer on nothing: then it is cleared instead.
	previousKey, err := OrkflowKey(namespace, name, previous.Version)
	if err != nil {
		return "", err
	}
	switch _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket),
		Key: aws.String(previousKey)}); {
	case isNotFound(err):
		cleared, err := s.unpromote(ctx, namespace, name, currentPointer{}, nil, written)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%q, which it named before, is no longer stored, so %s", previous.Version,
			strings.Replace(cleared, ", as it was before", "", 1)), nil
	case err != nil:
		return "", fmt.Errorf("the version %q it named before could not be read: %w", previous.Version, err)
	}
	document := previousRaw
	if len(document) == 0 {
		if document, err = marshalCurrentPointer(previous); err != nil {
			return "", err
		}
	}
	input := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(pointerKey),
		Body: bytes.NewReader(document), ContentLength: aws.Int64(int64(len(document))),
		ContentType: aws.String(orkflowContentType), IfMatch: aws.String(etag)}
	// Equal bytes after a lost answer are this move: the document carries its
	// instant to the nanosecond.
	switch _, err := putConfirmed(ctx, s.client, input, document); {
	case err == nil:
		return fmt.Sprintf("the current pointer was moved back to %q", previous.Version), nil
	case refusedCondition(err, input):
		return "the current pointer has moved since, and was left alone", nil
	default:
		return "", err
	}
}

// removeDanglingPointer removes the pointer at key when it names a version
// that is not stored, or none it can be read as, under the etag it read: a
// pointer moved since is another writer's, and is left alone.
func (s *S3OrkflowStore) removeDanglingPointer(
	ctx context.Context,
	namespace, name, key string,
	decode func([]byte) (string, error),
) error {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	switch {
	case isNotFound(err):
		return nil
	case err != nil:
		return fmt.Errorf("artifacts: read orkflow pointer %q after its versions: %w", s.uri(key), err)
	}
	document, err := io.ReadAll(object.Body)
	object.Body.Close()
	if err != nil {
		return fmt.Errorf("artifacts: read orkflow pointer %q after its versions: %w", s.uri(key), err)
	}
	if version, err := decode(document); err == nil {
		if versionKey, err := OrkflowKey(namespace, name, version); err == nil {
			_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(versionKey)})
			switch {
			case err == nil:
				return nil // a re-created orkflow's pointer, naming a stored version
			case !isNotFound(err):
				return fmt.Errorf("artifacts: read orkflow version %q a pointer names: %w", s.uri(versionKey), err)
			}
		}
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key),
		IfMatch: object.ETag})
	if err != nil && !isPreconditionFailed(err) && !isNotFound(err) {
		return fmt.Errorf("artifacts: delete orkflow pointer %q: %w", s.uri(key), err)
	}
	return nil
}

// Delete permanently removes every version and both pointers for one orkflow.
// A name with neither versions nor pointers returns ErrNotFound.
func (s *S3OrkflowStore) Delete(ctx context.Context, namespace, name string) error {
	namespace = ResolveNamespace(namespace)
	scan, err := s.scan(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("artifacts: scan orkflow %q in namespace %q for deletion: %w",
			name, namespace, err)
	}

	currentKey, err := OrkflowCurrentKey(namespace, name)
	if err != nil {
		return err
	}
	headKey, err := OrkflowHeadKey(namespace, name)
	if err != nil {
		return err
	}
	found := len(scan.versions) > 0 || scan.current[name] != "" || scan.head[name] != ""

	// Remove pointers before definitions. If object storage fails part way
	// through, readers can still derive a usable head from the definitions that
	// remain; they must never observe a pointer naming a definition already
	// removed. The unconditional cleanup also repairs a previous partial delete
	// that left only pointers behind.
	for _, key := range []string{currentKey, headKey} {
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		}); err != nil {
			return fmt.Errorf("artifacts: delete orkflow pointer %q: %w", s.uri(key), err)
		}
	}

	for _, wf := range scan.versions {
		key, err := OrkflowKey(namespace, name, wf.GetVersion())
		if err != nil {
			return fmt.Errorf("artifacts: address orkflow %q version %q for deletion: %w",
				name, wf.GetVersion(), err)
		}
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		}); err != nil {
			return fmt.Errorf("artifacts: delete orkflow version %q: %w", s.uri(key), err)
		}
	}
	// The claims go last, so a delete that fails part way still maps a document
	// to the versions that remain, and re-applying it lands on one of them. A
	// claim with nothing else stored (its apply never landed, or an earlier
	// delete stopped here) is cleaned up too, and the orkflow is still not found.
	for _, key := range scan.claimKeys {
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		}); err != nil {
			return fmt.Errorf("artifacts: delete derived claim %q: %w", s.uri(key), err)
		}
	}
	// Both pointers once more: a promotion or apply landing inside this Delete
	// loses to it, and a pointer one wrote while the versions were going would
	// otherwise outlive the Delete naming a version it removed. So a pointer
	// naming a version that is not stored is removed, under the etag read;
	// one naming a version that is stored — an apply that re-created the
	// orkflow with a version this Delete never scanned — is the re-created
	// orkflow's, and stays. One that lands after this sees its version gone
	// and removes its own pointer (confirmPromoted, confirmHeadFromNothing).
	for _, pointer := range []struct {
		key    string
		decode func([]byte) (string, error)
	}{
		{currentKey, func(b []byte) (string, error) { p, err := unmarshalCurrentPointer(b); return p.Version, err }},
		{headKey, func(b []byte) (string, error) { p, err := unmarshalHeadPointer(b); return p.Version, err }},
	} {
		if err := s.removeDanglingPointer(ctx, namespace, name, pointer.key, pointer.decode); err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("artifacts: delete orkflow %q in namespace %q: %w",
			name, namespace, ErrNotFound)
	}
	return nil
}

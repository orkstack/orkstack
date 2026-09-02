package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// An orkflow's HEAD is the version most recently applied, and it is the token
// every edit is checked against: an editor fetches a version, says which head
// it saw, and its apply is refused when that head has moved underneath it.
//
// It is a pointer object beside the versions, moved by compare-and-swap:
//
//	s3://{bucket}/metadata/orkflows/{namespace}/{name}/@head.json
//
// Two things forced it, and each one is a bug that a derived head has:
//
//  1. It cannot be READ off the documents' own created_at. Every API replica
//     stamps that from its own clock (internal/api.Service.now), so two
//     replicas a few seconds apart disagree about which apply was last, and
//     the apply that really landed last is not the head. Object timestamps are
//     no better: S3 reports them to the second, so two applies in one second
//     tie and the tie-break decides, not the order.
//
//  2. It cannot be CHECKED before the write and then trusted. An edit's
//     expected head is a compare-and-set or it is nothing: between a check in
//     the service and a write in the store, a competing apply lands and the
//     stale edit still succeeds. Moving the pointer IS the apply's commit
//     point, so the competing apply changes the etag the stale edit swaps
//     against, and the swap fails.
//
// The pointer also carries the stamp the head was applied at, and that stamp
// is MONOTONIC: an apply never stamps its version earlier than the head it
// extends (AppliedStamp). One replica an hour behind another therefore still
// produces a catalog whose "newest first" order is the order things were
// applied in, because the store — not the caller's clock — has the last word.
//
// It holds no copy of the definition, for the reason @current.json holds none:
// a second place for the document is a second place for it to be wrong.
const (
	// headPointerBasename is the head pointer object's name. It shares
	// @current.json's reserved prefix, so it can never be read as a version.
	headPointerBasename = orkflowMetaPrefix + "head" + orkflowSuffix

	// headSwapAttempts bounds the compare-and-swap retry for an apply that
	// states no expected head. Such an apply cannot be refused for losing —
	// nothing it claimed turned out to be false — so it re-reads what the
	// winner wrote and swaps against that. Two rounds are enough for the
	// property that matters, and a bound is what stops two writers starving
	// each other while both believe they are applying.
	headSwapAttempts = 3
)

// ErrHeadMoved reports an apply whose expected head was no longer the head
// when the store went to commit it. It is a FailedPrecondition at the API,
// never a retry: what to do about it is the caller's decision, because only
// the caller knows what its user typed into the editor.
var ErrHeadMoved = errors.New("orkflow head moved under this apply")

// HeadMovedError names both heads, so a refusal can tell an editor the version
// it was working against and the version that is there now without the API
// having to re-read the store to find out.
type HeadMovedError struct {
	Namespace string
	Name      string
	// Expected is the head the apply claimed.
	Expected string
	// Head is the head the store found, empty when nothing is stored.
	Head string
}

func (e *HeadMovedError) Error() string {
	return fmt.Sprintf(
		"artifacts: orkflow %q in namespace %q: this apply expected the head to be %q "+
			"but it is %q: %v",
		e.Name, e.Namespace, e.Expected, e.Head, ErrHeadMoved)
}

func (e *HeadMovedError) Unwrap() error { return ErrHeadMoved }

// headMoved is the refusal an apply gets when the head it named is not the
// head the store found.
func headMoved(namespace, name string, expected HeadExpectation, head string) error {
	return &HeadMovedError{
		Namespace: namespace,
		Name:      name,
		Expected:  expected.Version(),
		Head:      head,
	}
}

// HeadExpectation is what one apply claims about the head it extends: the
// version the editor saw when it fetched the document it is now applying.
//
// It is a type rather than a version string because the store has to tell
// "I claim nothing" — a plain `orkctl apply file.yaml` — from a claim about a
// particular version, and one string cannot say both while "" is also the
// honest answer for an orkflow nothing was ever applied to.
type HeadExpectation struct {
	version string
	stated  bool
}

// NoHeadExpectation is the unchecked apply: store this version, whatever the
// head is.
func NoHeadExpectation() HeadExpectation { return HeadExpectation{} }

// ExpectHead is an apply that commits only while version is still the head.
func ExpectHead(version string) HeadExpectation {
	return HeadExpectation{version: version, stated: true}
}

// Stated reports whether this apply claims a head at all.
func (e HeadExpectation) Stated() bool { return e.stated }

// Version is the head claimed. It means nothing unless Stated.
func (e HeadExpectation) Version() string { return e.version }

// SatisfiedBy reports whether head is the version this apply claims. An apply
// that claims nothing is satisfied by every head.
func (e HeadExpectation) SatisfiedBy(head string) bool {
	return !e.stated || e.version == head
}

// OrkflowHeadKey renders the object key holding one orkflow's head pointer:
// metadata/orkflows/{namespace}/{name}/@head.json. Both coordinates are
// validated exactly as a version key's are.
func OrkflowHeadKey(namespace, name string) (string, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return "", err
	}
	return prefix + headPointerBasename, nil
}

// ParseOrkflowHeadKey reverses OrkflowHeadKey, so one prefix scan can sort
// definitions from the metadata beside them without a second request.
func ParseOrkflowHeadKey(key string) (namespace, name string, ok bool) {
	return parseOrkflowMetaKey(key, headPointerBasename)
}

// parseOrkflowMetaKey reverses the key of one per-orkflow metadata document.
// The pointer keys differ only in their basename, and having each parse itself
// is how the two drifted apart in the first place.
func parseOrkflowMetaKey(key, basename string) (namespace, name string, ok bool) {
	rest, found := strings.CutPrefix(key, OrkflowPrefix+"/")
	if !found {
		return "", "", false
	}
	namespace, rest, found = strings.Cut(rest, "/")
	if !found {
		return "", "", false
	}
	name, file, found := strings.Cut(rest, "/")
	if !found || file != basename {
		return "", "", false
	}
	if validOrkflowNamespace(namespace) != nil || validOrkflowComponent("name", name) != nil {
		return "", "", false
	}
	return namespace, name, true
}

// headPointer is the head document's body: which version was applied last, and
// the stamp that apply carries.
type headPointer struct {
	Version   string    `json:"version"`
	AppliedAt time.Time `json:"applied_at"`
}

func marshalHeadPointer(pointer headPointer) ([]byte, error) {
	document, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode head pointer: %w", err)
	}
	return append(document, '\n'), nil
}

// unmarshalHeadPointer decodes a head document. Unknown fields are discarded,
// so an engine can read what a newer engine wrote.
func unmarshalHeadPointer(document []byte) (headPointer, error) {
	var pointer headPointer
	if err := json.Unmarshal(document, &pointer); err != nil {
		return headPointer{}, fmt.Errorf("decode head pointer: %w", err)
	}
	return pointer, nil
}

// AppliedStamp is the stamp one apply carries: the caller's own, unless that is
// not after the head's, in which case it is one nanosecond past the head's.
//
// This is the whole of the clock discipline, and it is deliberately not "use
// the store's clock" — there is no store clock; MinIO and S3 hand out no
// timestamp a writer can order against. What there is instead is the stamp
// already committed, which every replica reads before it writes. Ordering
// against THAT makes the catalog's order the order applies committed in, on a
// deployment whose replicas' clocks disagree by any amount.
//
// It is exported because the in-memory store applies the same rule
// (engine.MemoryOrkflowStore), and one rule spelled twice is two rules.
func AppliedStamp(caller, head time.Time) time.Time {
	if caller.IsZero() {
		// Nothing said when this was applied — a programmatically built
		// orkflow rather than one that came through ApplyOrkflow. Wall clock,
		// then, held to the same rule below.
		caller = time.Now().UTC()
	}
	if caller.After(head) {
		return caller
	}
	return head.Add(time.Nanosecond)
}

// readHead reads one orkflow's head pointer and the etag naming the bytes it
// read. A missing pointer is the zero value and no error, with an empty etag —
// which is what tells the writer to swap with IfAbsent rather than IfMatch.
func (s *S3OrkflowStore) readHead(
	ctx context.Context,
	namespace, name string,
) (headPointer, string, error) {
	key, err := OrkflowHeadKey(namespace, name)
	if err != nil {
		return headPointer{}, "", err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return headPointer{}, "", nil
		}
		return headPointer{}, "", fmt.Errorf("artifacts: get head %q: %w", s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return headPointer{}, "", fmt.Errorf("artifacts: get head %q: read body: %w", s.uri(key), err)
	}
	pointer, err := unmarshalHeadPointer(document)
	if err != nil {
		return headPointer{}, "", fmt.Errorf("artifacts: get head %q: %w", s.uri(key), err)
	}
	return pointer, aws.ToString(object.ETag), nil
}

// effectiveHead is the head an apply is checked against and swaps against: the
// committed POINTER when there is one, and the head DERIVED from the stored
// documents when there is not.
//
// The derived answer exists because the bucket can hold version documents
// and no pointer: Delete removes both pointers before the definitions, so a
// Delete that fails part way leaves exactly that, and Put writes a version
// before it moves the pointer, so a writer killed on an orkflow's first apply
// leaves it too. A listing answers such a catalog "head 1.0.0"
// (orkflowScan.headOf). Reading the absent pointer as a head of "" HERE —
// while the listing says 1.0.0 — would make the catalog readable and not
// editable: the service checks the derived head, accepts the edit, and the
// store then refuses it for expecting 1.0.0 of a head it calls "". One rule,
// in both places, or the two disagree exactly where a user is trying to work.
//
// The etag is empty in the derived case, which is the honest thing to swap
// against: no pointer object exists yet, so the swap is IfAbsent and the first
// apply to install the pointer wins. The stamp comes from the derived head's
// own document, so the version this apply publishes is stamped after it
// (AppliedStamp) and the catalog's order still reads as the order applies
// committed in.
//
// It costs a prefix scan only while the pointer is missing: the first apply
// writes one, and every apply after that is the single GetObject readHead
// makes.
func (s *S3OrkflowStore) effectiveHead(
	ctx context.Context,
	namespace, name string,
) (headPointer, string, error) {
	pointer, etag, err := s.readHead(ctx, namespace, name)
	if err != nil || etag != "" {
		return pointer, etag, err
	}
	scan, err := s.scan(ctx, namespace, name)
	if err != nil {
		return headPointer{}, "", err
	}
	derived := scan.derivedHeadOf(name)
	if derived == nil {
		// Nothing is stored at all: an orkflow's first apply. The zero
		// pointer is the truthful answer, and the zero stamp is what
		// AppliedStamp reads as "no head to be after".
		return headPointer{}, "", nil
	}
	return headPointer{
		Version:   derived.GetVersion(),
		AppliedAt: derived.GetCreatedAt().AsTime(),
	}, "", nil
}

// swapHead moves the head pointer, conditional on the bytes the caller read.
// A caller holding no etag is claiming the pointer does not exist yet, which
// IfAbsent is the object store's word for.
//
// ErrPreconditionFailed here means somebody else's apply committed between the
// read and this write. That is not an error about the bucket; it is the answer
// to the question the swap asks, and Put decides what it means.
func (s *S3OrkflowStore) swapHead(
	ctx context.Context,
	namespace, name, etag string,
	pointer headPointer,
) error {
	key, err := OrkflowHeadKey(namespace, name)
	if err != nil {
		return err
	}
	document, err := marshalHeadPointer(pointer)
	if err != nil {
		return err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
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
			return fmt.Errorf("artifacts: move head of orkflow %q in namespace %q to %q: %w",
				name, ResolveNamespace(namespace), pointer.Version, ErrPreconditionFailed)
		}
		return fmt.Errorf("artifacts: move head of orkflow %q to %q (%s): %w",
			name, pointer.Version, s.uri(key), err)
	}
	return nil
}

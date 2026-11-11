package artifacts

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// A document that pins no `version:` is given one when it is first applied: a
// time-ordered id minted on the apply path. Re-applying the unchanged document
// must land on that same version and write nothing, so something has to say
// which version a document's content was given. That is the derived claim, one
// write-once object per content hash beside the versions:
//
//	s3://{bucket}/metadata/orkflows/{namespace}/{name}/@derived-{contentHash}.json
//
// Being an object is what makes it hold across replicas: no process remembers
// anything, and a replica that never saw the first apply reads the same answer.
// Being conditionally created is what settles a race: two replicas applying one
// new document each mint a candidate, the object store lets one create the
// claim, and the other reads the winner's version. Both then store one version,
// exactly as two applies of a pinned version do.
//
// It is an immutable record and not a lock: nobody holds it, nothing waits on
// it, it never expires and it decides no execution. A claim whose version never
// landed (the writer died, or its apply was refused after minting) is inert, and
// the next apply of that content simply stores the version it names.
//
// Nothing has to edit one to unstick an apply (AGENTS.md §15). A claim that
// cannot be decoded answers as no claim at all, and the next apply replaces it by
// compare-and-swap on the bytes it read, so replicas converge exactly as they do
// on a create. That content then lands as a new version: which version the
// damaged claim named cannot be known, and a duplicate is correct where a refusal
// nobody can clear is not.
//
// A stored version from before minting existed is its own content hash, so such
// a catalog needs no claims: FindDerivedVersion answers from the version key.
const (
	// derivedClaimPrefix begins a claim's basename. It shares @current.json's
	// reserved prefix, so a claim can never be read as a version.
	derivedClaimPrefix = orkflowMetaPrefix + "derived-"

	// derivedClaimAttempts bounds the create-or-read loop. The loser of a create
	// reads what the winner wrote, so one round settles a race; the rounds after
	// it cover a backend reporting the conflict before the winner's write is
	// readable, and a Delete of the claim between the refusal and the read.
	derivedClaimAttempts = 5

	// derivedClaimBackoff is the wait before the second attempt, doubling after
	// it: at most 300ms across all of them, on a request a person is waiting for.
	derivedClaimBackoff = 20 * time.Millisecond
)

// OrkflowDerivedKey renders the object key holding one document's claim. All
// three coordinates are validated exactly as a version key's are.
func OrkflowDerivedKey(namespace, name, contentHash string) (string, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return "", err
	}
	if err := validOrkflowComponent("content hash", contentHash); err != nil {
		return "", err
	}
	return prefix + derivedClaimPrefix + contentHash + orkflowSuffix, nil
}

// ParseOrkflowDerivedKey reverses OrkflowDerivedKey. A key that is not a claim
// reports ok=false, so one prefix scan can sort claims from definitions without
// a second request.
func ParseOrkflowDerivedKey(key string) (namespace, name, contentHash string, ok bool) {
	rest, found := strings.CutPrefix(key, OrkflowPrefix+"/")
	if !found {
		return "", "", "", false
	}
	namespace, rest, found = strings.Cut(rest, "/")
	if !found {
		return "", "", "", false
	}
	name, file, found := strings.Cut(rest, "/")
	if !found {
		return "", "", "", false
	}
	file, found = strings.CutPrefix(file, derivedClaimPrefix)
	if !found {
		return "", "", "", false
	}
	contentHash, found = strings.CutSuffix(file, orkflowSuffix)
	if !found {
		return "", "", "", false
	}
	if validOrkflowNamespace(namespace) != nil ||
		validOrkflowComponent("name", name) != nil ||
		validOrkflowComponent("content hash", contentHash) != nil {
		return "", "", "", false
	}
	return namespace, name, contentHash, true
}

// derivedClaim is the claim document's body. It holds the version and nothing
// else: the key already says which content, and a copy of the definition here
// would be a second place for it to be wrong.
type derivedClaim struct {
	Version string `json:"version"`
}

func marshalDerivedClaim(claim derivedClaim) ([]byte, error) {
	document, err := stablejson.MarshalIndent(claim, "  ")
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode derived claim: %w", err)
	}
	return append(document, '\n'), nil
}

// unmarshalDerivedClaim decodes a claim read back from the bucket. A claim
// naming no addressable version is refused here, never handed to an apply as
// the version to store under; readDerived decides what a refusal means.
func unmarshalDerivedClaim(document []byte) (derivedClaim, error) {
	var claim derivedClaim
	if err := json.Unmarshal(document, &claim); err != nil {
		return derivedClaim{}, fmt.Errorf("decode derived claim: %w", err)
	}
	if err := validOrkflowComponent("version", claim.Version); err != nil {
		return derivedClaim{}, fmt.Errorf("decode derived claim: %w", err)
	}
	return claim, nil
}

// FindDerivedVersion reports the version the content hash was given, "" when it
// was never given one or its claim cannot be decoded. It only reads, so a
// validation can ask.
//
// A claim answers first. Without one, a version stored under the hash itself
// answers: every derived version an earlier release stored is its content's
// hash, and re-applying that document must stay the no-op it always was.
func (s *S3OrkflowStore) FindDerivedVersion(
	ctx context.Context,
	namespace, name, contentHash string,
) (string, error) {
	version, _, err := s.readDerived(ctx, namespace, name, contentHash)
	return version, err
}

// readDerived is FindDerivedVersion with the one thing a claim needs beyond the
// answer: damaged is the ETag of a claim that could not be decoded, the bytes a
// replacement must swap against, and empty when there is none.
//
// Only a body that does not decode counts as damaged. A store that cannot be
// reached, or a body that cannot be read, is an error: that says nothing about
// what the claim holds, and treating it as absent would mint a second version
// whenever storage blinked.
func (s *S3OrkflowStore) readDerived(
	ctx context.Context,
	namespace, name, contentHash string,
) (version, damaged string, err error) {
	key, err := OrkflowDerivedKey(namespace, name, contentHash)
	if err != nil {
		return "", "", err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	switch {
	case err == nil:
		defer object.Body.Close()
		document, err := io.ReadAll(object.Body)
		if err != nil {
			return "", "", fmt.Errorf("artifacts: get derived claim %q: read body: %w", s.uri(key), err)
		}
		claim, err := unmarshalDerivedClaim(document)
		if err == nil {
			return claim.Version, "", nil
		}
		damaged = aws.ToString(object.ETag)
	case !isNotFound(err):
		return "", "", fmt.Errorf("artifacts: get derived claim %q: %w", s.uri(key), err)
	}

	versionKey, err := OrkflowKey(namespace, name, contentHash)
	if err != nil {
		return "", "", err
	}
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(versionKey),
	}); err != nil {
		if isNotFound(err) {
			return "", damaged, nil
		}
		return "", "", fmt.Errorf("artifacts: head orkflow %q: %w", s.uri(versionKey), err)
	}
	return contentHash, "", nil
}

// ClaimDerivedVersion gives the content hash a version: the one it already has,
// or candidate if it has none. Every caller for one content hash gets one
// answer, whatever replica it runs on and however the calls interleave.
func (s *S3OrkflowStore) ClaimDerivedVersion(
	ctx context.Context,
	namespace, name, contentHash, candidate string,
) (string, error) {
	if err := validOrkflowComponent("version", candidate); err != nil {
		return "", err
	}
	key, err := OrkflowDerivedKey(namespace, name, contentHash)
	if err != nil {
		return "", err
	}
	document, err := marshalDerivedClaim(derivedClaim{Version: candidate})
	if err != nil {
		return "", err
	}
	for attempt := range derivedClaimAttempts {
		if attempt > 0 {
			// A backend may refuse with a conflict while the winner's write is
			// still landing, so the next read is not asked for at once.
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("artifacts: claim %q: %w", s.uri(key), ctx.Err())
			case <-time.After(derivedClaimBackoff << (attempt - 1)):
			}
		}
		existing, damaged, err := s.readDerived(ctx, namespace, name, contentHash)
		if err != nil {
			return "", err
		}
		if existing != "" {
			return existing, nil
		}
		input := &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(document),
			ContentLength: aws.Int64(int64(len(document))),
			ContentType:   aws.String(orkflowContentType),
		}
		if damaged != "" {
			// Only the bytes that could not be decoded are replaced: a replica
			// that got there first has changed them, and is read next round.
			input.IfMatch = aws.String(damaged)
		} else {
			// "*" is the S3 spelling of "only if nothing is there".
			input.IfNoneMatch = aws.String("*")
		}
		// Equal bytes after a lost answer (putConfirmed) name candidate, the
		// version every caller for this hash is answered with: whoever wrote
		// them, candidate is the answer.
		if _, err = putConfirmed(ctx, s.client, input, document); err == nil {
			return candidate, nil
		}
		if !refusedCondition(err, input) {
			return "", fmt.Errorf("artifacts: put derived claim %q: %w", s.uri(key), err)
		}
	}
	return "", fmt.Errorf("artifacts: orkflow %q in namespace %q: no version could be claimed for "+
		"this document in %d attempts: %w", name, ResolveNamespace(namespace), derivedClaimAttempts,
		ErrPreconditionFailed)
}

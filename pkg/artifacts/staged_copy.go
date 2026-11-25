package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	// ErrStagedMissing is a staged part that is not there, at the check or
	// at the copy: the Orker stages it again.
	ErrStagedMissing = errors.New("artifacts: the staged part is not there")
	// ErrStagedMismatch is a staged part that is there and is not what was
	// said of it: another size, or other bytes than were checked.
	ErrStagedMismatch = errors.New("artifacts: the staged part is not what was said of it")
	// errCopyNotStored is a copy the store answered as made and that is not
	// there to read.
	errCopyNotStored = errors.New("artifacts: the store answered a copy it did not store")
	// errCopyWrong is a copy whose bytes are not the part's, of a part whose
	// own bytes are: the store's failure, which another try may not meet.
	errCopyWrong = errors.New("artifacts: the store's copy is not of the staged part's bytes")
)

// maxStagedPart is the largest part a store copies in one request.
const maxStagedPart = 5 << 30

// StagedPart names one bound part of a message as an Orker staged it, and
// the size the Orker gave for it.
type StagedPart struct {
	Namespace string
	SourceID  string
	Identity  string
	Digest    string
	Size      int64
}

func (p StagedPart) key() (string, error) {
	key, err := StagingKey(p.Namespace, p.SourceID, p.Identity, p.Digest)
	if err == nil && (p.Size < 0 || p.Size > maxStagedPart) {
		return "", fmt.Errorf("artifacts: staging: a part is 0 to %d bytes, got %d", int64(maxStagedPart), p.Size)
	}
	return key, err
}

// StagedCopier puts a staged part where a run reads it, inside the store.
type StagedCopier interface {
	// CopyStaged copies the staged part to the artifact RefFor predicts for
	// name under prov, and returns that reference. It is ErrStagedMissing
	// when the part is not there and ErrStagedMismatch when it is not of the
	// size or the digest given. A reference is returned only for an object
	// whose own bytes were read back and hash to the digest: neither the
	// Orker's word nor the store's is taken for it. A copy made again is the
	// same object, and one that is already there and good is left alone.
	CopyStaged(ctx context.Context, part StagedPart, prov Provenance, name, contentType string) (ArtifactRef, error)
}

var (
	_ StagedCopier = (*S3Store)(nil)
	_ StagedCopier = (*LocalStore)(nil)
)

// checkStagedCopy is what both backends refuse before they read anything.
func checkStagedCopy(part StagedPart, prov Provenance, contentType string) (key string, err error) {
	if key, err = part.key(); err != nil {
		return "", err
	}
	if prov.Orkflow == "" || prov.RunID == "" || prov.TaskID == "" {
		return "", errors.New("artifacts: staged copy: the run and the producer it is copied to are required")
	}
	if contentType == "" {
		return "", errors.New("artifacts: staged copy: a content type is required")
	}
	return key, nil
}

// CopyStaged implements StagedCopier with CopyObject.
func (s *S3Store) CopyStaged(ctx context.Context, part StagedPart, prov Provenance, name, contentType string) (ArtifactRef, error) {
	key, err := checkStagedCopy(part, prov, contentType)
	if err != nil {
		return ArtifactRef{}, err
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	switch {
	case isNotFound(err):
		return ArtifactRef{}, s.stagedMissing(ctx, key)
	case err != nil:
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 stat %q: %w", s.uri(key), err)
	case aws.ToInt64(head.ContentLength) != part.Size:
		return ArtifactRef{}, fmt.Errorf("%w: %s is %d bytes, not %d", ErrStagedMismatch, s.uri(key), aws.ToInt64(head.ContentLength), part.Size)
	}
	dest := s3ObjectKey(prov, name, part.Digest)
	if state, err := s.readCopy(ctx, dest, part); err != nil {
		return ArtifactRef{}, err
	} else if state == copyGood {
		return s.copiedRef(ctx, dest, part, prov, name, contentType)
	}
	stored := prov.Metadata(nil)
	stored[metaContentSHA256] = part.Digest
	_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dest),
		CopySource: aws.String((&url.URL{Path: s.bucket + "/" + key}).EscapedPath()),
		// What is copied is what was checked, where the store holds a copy
		// to that. Nothing rests on it: the copy is read back below.
		CopySourceIfMatch: head.ETag,
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata:          stored,
		ContentType:       aws.String(contentType),
	})
	switch {
	case isNotFound(err):
		return ArtifactRef{}, fmt.Errorf("%w: %s went as it was copied", ErrStagedMissing, s.uri(key))
	case err != nil:
		// A refused condition (the part was staged again as it was copied) and
		// a conflict with another delivery's copy among them: the next try
		// checks afresh, and only a part's own size and digest judge it.
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 copy %q to %q: %w", s.uri(key), s.uri(dest), err)
	}
	switch state, err := s.readCopy(ctx, dest, part); {
	case err != nil:
		return ArtifactRef{}, err
	case state == copyAbsent:
		return ArtifactRef{}, fmt.Errorf("%w: %s", errCopyNotStored, s.uri(dest))
	case state == copyWrong:
		return ArtifactRef{}, s.wrongCopy(ctx, key, dest, part)
	}
	return s.copiedRef(ctx, dest, part, prov, name, contentType)
}

// stagedMissing is the error of a part HeadObject found absent. That answer
// has no body, so it says nothing apart from a bucket that is not there: one
// byte is asked for, whose refusal names what is missing. Staging the part
// again helps only when it is the part.
func (s *S3Store) stagedMissing(ctx context.Context, key string) error {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Range: aws.String("bytes=0-0")})
	if err == nil {
		_ = out.Body.Close()
		return fmt.Errorf("artifacts: s3 stat %q: the part came as it was asked for", s.uri(key))
	}
	if isNotFound(err) {
		return fmt.Errorf("%w: %s", ErrStagedMissing, s.uri(key))
	}
	return fmt.Errorf("artifacts: s3 stat %q: %w", s.uri(key), err)
}

// wrongCopy says whose fault a copy of other bytes is, from the staged part's
// own: the part's, or the store's. What is at the copy's key is left: this
// call references nothing, a good copy replaces it, and removing it could
// take away one that just did.
func (s *S3Store) wrongCopy(ctx context.Context, key, dest string, part StagedPart) error {
	switch state, err := s.readCopy(ctx, key, part); {
	case err != nil:
		return err
	case state == copyAbsent:
		return fmt.Errorf("%w: %s", ErrStagedMissing, s.uri(key))
	case state == copyWrong:
		return fmt.Errorf("%w: %s is not of digest %s", ErrStagedMismatch, s.uri(key), part.Digest)
	}
	return fmt.Errorf("%w: %s", errCopyWrong, s.uri(dest))
}

// copiedRef is the reference of a copy that was read back and is good. As
// upload: the manifest follows the object, and only a name the key cannot
// hold needs one.
func (s *S3Store) copiedRef(ctx context.Context, dest string, part StagedPart, prov Provenance, name, contentType string) (ArtifactRef, error) {
	if !isCleanS3Component(name) {
		if err := s.writeNameManifest(ctx, dest, name); err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 copy %q: %w", name, err)
		}
	}
	return s.RefFor(prov, name, part.Digest, part.Size, contentType), nil
}

// copyState is what is at a copy's key.
type copyState int

const (
	copyAbsent copyState = iota
	copyWrong            // an object of another size or digest
	copyGood             // the part's own bytes
)

// readCopy reads the object at key and says whether its bytes are part's.
// It hashes what it reads: no length, ETag or checksum the store gives is
// taken for the bytes.
func (s *S3Store) readCopy(ctx context.Context, key string, part StagedPart) (copyState, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if isNotFound(err) {
		return copyAbsent, nil
	}
	if err != nil {
		return copyAbsent, fmt.Errorf("artifacts: s3 read copy %q: %w", s.uri(key), err)
	}
	defer out.Body.Close()
	hasher := sha256.New()
	// One byte more than the part's is enough to see that it is longer.
	n, err := io.Copy(hasher, io.LimitReader(out.Body, part.Size+1))
	if err != nil {
		return copyAbsent, fmt.Errorf("artifacts: s3 read copy %q: %w", s.uri(key), err)
	}
	if n != part.Size || hex.EncodeToString(hasher.Sum(nil)) != part.Digest {
		return copyWrong, nil
	}
	return copyGood, nil
}

// CopyStaged implements StagedCopier with a file copy inside the store's
// root.
func (s *LocalStore) CopyStaged(ctx context.Context, part StagedPart, prov Provenance, name, contentType string) (ArtifactRef, error) {
	key, err := checkStagedCopy(part, prov, contentType)
	if err != nil {
		return ArtifactRef{}, err
	}
	file, err := s.root.Open(key)
	if errors.Is(err, fs.ErrNotExist) {
		return ArtifactRef{}, fmt.Errorf("%w: %s", ErrStagedMissing, key)
	}
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local open %q: %w", key, err)
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, io.LimitReader(&ctxReader{ctx: ctx, r: file}, part.Size+1))
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local read %q: %w", key, err)
	}
	if size != part.Size || hex.EncodeToString(hasher.Sum(nil)) != part.Digest {
		return ArtifactRef{}, fmt.Errorf("%w: %s is %d bytes of another digest or size", ErrStagedMismatch, key, size)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: local rewind %q: %w", key, err)
	}
	return s.Put(ctx, name, file, PutOptions{ContentType: contentType, Metadata: prov.Metadata(nil),
		ExpectedSHA256: part.Digest, ExpectedSize: part.Size})
}

// RemoveStaged removes every staged part of one message, once its run has
// been started or found. The engine's, best-effort to its caller: what it
// leaves, the prefix's lifecycle rule removes. The Orker's credential cannot.
func (s *EventSourceRecordStore) RemoveStaged(ctx context.Context, namespace, sourceID, identity string) error {
	// A key of any digest names the message's own directory.
	key, err := StagingKey(namespace, sourceID, identity, strings.Repeat("0", sha256.Size*2))
	if err != nil {
		return err
	}
	prefix := key[:len(key)-sha256.Size*2]
	keys, err := listKeys(ctx, s.b, prefix)
	if err != nil {
		return fmt.Errorf("artifacts: list staged parts %s: %w", s.b.uri(prefix), err)
	}
	var failed []error
	for batch := range slices.Chunk(keys, blobRemoveBatch) {
		refused, err := s.b.removeKeys(ctx, batch)
		if err != nil {
			return fmt.Errorf("artifacts: remove staged parts %s: %w", s.b.uri(prefix), err)
		}
		for _, err := range refused {
			failed = append(failed, err)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("artifacts: remove staged parts %s: %w", s.b.uri(prefix), errors.Join(failed...))
	}
	return nil
}

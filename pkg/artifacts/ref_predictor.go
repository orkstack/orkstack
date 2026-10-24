package artifacts

import "strings"

// RefPredictor computes the reference a Put WOULD return, without performing
// any I/O.
//
// It exists because §5.4 step 4 has to bound the commit before step 5 uploads a
// byte. A projected ActivityCommit must contain the same ArtifactRefs step 5
// will produce or measuring it proves nothing — and a ref's uri is the longest
// field in it. artifacts.Store cannot supply that projection: it is
// Put/Get/Stat/List/Presign/Delete, nothing predicts a ref, Stat needs a uri the
// Orker does not yet have, and both key builders (objectKey, s3ObjectKey) are
// unexported.
//
// The keys are already pure functions of data the Orker holds after step 3 — the
// logical name plus the content digest for the local backend, provenance plus
// name plus digest for S3 once the content-addressed sixth segment lands — so
// the answer is to export the projection rather than to add I/O.
//
// Every Store implementation is expected to also implement RefPredictor. The
// two are separate interfaces rather than one because a caller that only needs
// the projection (the dispatcher's step-4 bound) should not have to hold a
// store that can delete objects.
type RefPredictor interface {
	// RefFor returns exactly the ArtifactRef a Put of these bytes under this
	// name and provenance would return. It performs no I/O and MUST agree
	// byte-for-byte with Put's result.
	//
	// contentType is used VERBATIM. Put sniffs when its caller supplies none,
	// and a prediction cannot sniff bytes it has not been given — so a caller
	// that needs the two to agree on that field passes the same explicit
	// content type to both. Every §5.4 caller does: it knows it is uploading
	// values.json, a log, or a scanned output file whose type it resolved
	// during the step-2 scan.
	//
	// sha256 is the lowercase hex digest computed in step 3.
	RefFor(p Provenance, name, sha256 string, sizeBytes int64, contentType string) ArtifactRef
}

// MemoryRefPredictor projects refs for the in-memory/testing path, using the
// content-addressed layout the local backend already uses: a key names bytes,
// not a slot, so predicting one needs nothing the caller does not have.
//
// It is the companion to MemoryCommitStore — together they let the
// OCIDispatcher's step-4 bound and step-8 commit be exercised end to end with no
// bucket and no filesystem.
type MemoryRefPredictor struct {
	// Root is the uri prefix every predicted ref is composed against, e.g.
	// "mem://artifacts". An empty Root uses DefaultMemoryRefRoot.
	Root string
}

// DefaultMemoryRefRoot is what MemoryRefPredictor composes against when its
// Root is empty.
const DefaultMemoryRefRoot = "mem://artifacts"

// compile-time contract check.
var _ RefPredictor = MemoryRefPredictor{}

// RefFor projects the reference for these bytes.
//
// The layout is objectKey's: objects/n-<b64(name)>[/c-…]/<sha256>. Provenance
// does not enter the key, exactly as it does not for LocalStore.Put, but it
// does travel in Metadata so a predicted ref carries the same coordinates a
// real one does.
func (p MemoryRefPredictor) RefFor(prov Provenance, name, sha256 string, sizeBytes int64, contentType string) ArtifactRef {
	root := p.Root
	if root == "" {
		root = DefaultMemoryRefRoot
	}
	return ArtifactRef{
		Name:        name,
		URI:         strings.TrimSuffix(root, "/") + "/" + objectKey(name, sha256),
		ContentType: contentType,
		SizeBytes:   sizeBytes,
		SHA256:      sha256,
		Metadata:    prov.Metadata(nil),
	}
}

// RefFor predicts the content-addressed local reference returned by Put.
func (s *LocalStore) RefFor(prov Provenance, name, sha256 string, sizeBytes int64, contentType string) ArtifactRef {
	return ArtifactRef{Name: name, URI: s.uri(objectKey(name, sha256)), ContentType: contentType,
		SizeBytes: sizeBytes, SHA256: sha256, Metadata: prov.Metadata(nil)}
}

// RefFor predicts the sixth-segment content-addressed S3 reference returned by Put.
func (s *S3Store) RefFor(prov Provenance, name, sha256 string, sizeBytes int64, contentType string) ArtifactRef {
	return ArtifactRef{Name: name, URI: s.uri(s3ObjectKey(prov, name, sha256)), ContentType: contentType,
		SizeBytes: sizeBytes, SHA256: sha256, Metadata: prov.Metadata(nil)}
}

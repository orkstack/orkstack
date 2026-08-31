package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// OrkflowStore is the system of record for orkflow definitions. Runs live in
// Temporal; orkflows live here. Every read goes to the backing store, so a
// restarted engine sees exactly what the bucket holds and nothing is served
// from process memory.
//
// An orkflow is addressed by three coordinates, not two: a name is unique
// WITHIN a namespace, so two namespaces may each hold a "deploy" and never see
// each other's documents.
//
// A stored version is immutable. The one thing that moves is a per-orkflow
// POINTER (SetCurrentVersion), which decides what an empty version resolves
// to; the versions it points at are written once and never rewritten.
//
// internal/engine declares the same method set as the interface the API
// depends on; Go's structural typing lets this package satisfy it without
// importing internal/.
type OrkflowStore interface {
	// Put writes one orkflow version into the namespace the message names,
	// and returns the stored document plus whether this call created it.
	//
	// A version is written once: re-applying the identical document is a
	// no-op that writes nothing, moves nothing and returns what is stored
	// (created=false), and a DIFFERENT document under an existing version is
	// refused with an error matching ErrVersionExists. Other versions, and
	// every other namespace, are never touched.
	//
	// expected is what this apply claims about the HEAD it extends. A stated
	// expectation is a compare-and-SET, not a check: the version lands only
	// while the head is still the version named, and an apply that loses that
	// race is refused with an error matching ErrHeadMoved. Passing
	// NoHeadExpectation() is the unchecked apply — `orkctl apply file.yaml`,
	// which claims nothing and so cannot be told it was wrong.
	Put(ctx context.Context, wf *orkstackv1.Orkflow, expected HeadExpectation) (*orkstackv1.Orkflow, bool, error)
	// Get returns one orkflow version from one namespace. An empty version
	// resolves through the current pointer when one has been set, and to the
	// most recently applied version when none ever was; an empty namespace
	// means DefaultNamespace. A missing orkflow returns an error matching
	// ErrNotFound.
	Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error)
	// List returns the version every orkflow in one namespace currently
	// resolves to, ordered by name.
	List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error)
	// Versions returns one orkflow's whole version catalog: every stored
	// version newest-applied first, the pointer's target, and the head.
	Versions(ctx context.Context, namespace, name string) (OrkflowVersions, error)
	// SetCurrentVersion points the orkflow at an EXISTING version and returns
	// the version the pointer named before ("" when it was never set).
	// Promoting to a version that is not stored returns ErrNotFound, so no
	// reader can observe a pointer to a version that does not exist.
	SetCurrentVersion(ctx context.Context, namespace, name, version string) (string, error)
	// Delete permanently removes every version and both pointers for one
	// orkflow. A missing orkflow returns ErrNotFound.
	Delete(ctx context.Context, namespace, name string) error
}

// OrkflowVersions is one orkflow's version catalog.
//
// Current and Head are two different questions and are deliberately not one
// field: Head is the edit lineage's tip (what an editor last read, and the
// base_version it sends back), Current is execution policy (what an empty
// version resolves to). They coincide until somebody promotes an older
// version, which is exactly when conflating them would run the wrong code.
type OrkflowVersions struct {
	// All holds every stored version, most recently applied first.
	All []*orkstackv1.Orkflow
	// Current is the pointer's target, empty when nobody ever promoted this
	// orkflow. Empty does not mean "nothing runs": resolution then falls back
	// to Head.
	Current string
	// Head is the most recently applied version, empty when none are stored.
	Head string
}

// Resolved is the version an empty version resolves to: the pointer's target
// when one is set, otherwise the most recently applied version.
func (v OrkflowVersions) Resolved() string {
	if v.Current != "" {
		return v.Current
	}
	return v.Head
}

const (
	// OrkflowPrefix roots orkflow definitions in the bucket:
	//
	//	s3://{bucket}/metadata/orkflows/{namespace}/{name}/{version}.json
	//
	// The namespace comes first so listing one namespace is a single prefix
	// scan, which is what makes namespace isolation a property of the key
	// rather than a filter applied after reading everyone's documents.
	//
	// It is disjoint from the run-artifact hierarchy (`orkflows/`) and from
	// artifact name manifests (`manifests/`), so a lifecycle rule that expires
	// one orkflow's artifacts never deletes the definitions.
	OrkflowPrefix = "metadata/orkflows"

	// DefaultNamespace holds every orkflow that names no namespace. It always
	// resolves and is never created: a deployment that registered nothing
	// still has one namespace.
	DefaultNamespace = "default"

	// orkflowSuffix ends every definition object. The document is JSON.
	orkflowSuffix = ".json"

	// orkflowContentType is what a definition object is served as.
	orkflowContentType = "application/json"

	// maxOrkflowComponent bounds a namespace, name or version so a key stays
	// far below S3's 1024-byte limit and is a legal path component everywhere.
	maxOrkflowComponent = 200
)

// ErrInvalidOrkflow reports coordinates this store cannot address.
var ErrInvalidOrkflow = errors.New("invalid orkflow coordinates")

// ErrVersionExists reports a write that would have changed a stored version.
//
// A version is immutable: a run pinned to one reads its document long after it
// was applied, so rewriting it would change what an already-started run
// executes. The fix is never "force"; it is a new version.
var ErrVersionExists = errors.New("orkflow version already exists with different content")

// ResolveNamespace turns an unset namespace into DefaultNamespace. It is the
// ONE place "empty means default" happens: the DSL keeps an absent namespace
// empty and the proto documents it as empty, so the rule is applied where a
// name becomes a key and nowhere else.
func ResolveNamespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

// OrkflowKey renders the object key holding one orkflow version:
// metadata/orkflows/{namespace}/{name}/{version}.json.
//
// All three coordinates are validated rather than sanitized: a component that
// is not already a safe single path component (a DSL slug and a version string
// always are) is rejected, so no input can escape the prefix, climb out of its
// namespace, or collide with another orkflow's key.
func OrkflowKey(namespace, name, version string) (string, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return "", err
	}
	if err := validOrkflowComponent("version", version); err != nil {
		return "", err
	}
	return prefix + version + orkflowSuffix, nil
}

// OrkflowNamespacePrefix is the key prefix holding every orkflow of one
// namespace. Listing a namespace is exactly this one prefix scan.
func OrkflowNamespacePrefix(namespace string) (string, error) {
	namespace = ResolveNamespace(namespace)
	if err := validOrkflowNamespace(namespace); err != nil {
		return "", err
	}
	return OrkflowPrefix + "/" + namespace + "/", nil
}

// validOrkflowNamespace holds the namespace component of an orkflow key to the
// registry's name rule, which is the compiler's. The other two components have
// their own rules — a version is a semver or a content hash, not a slug — but
// a namespace is the same concept here as in metadata/namespaces/, and one
// concept gets one rule (design §2.5). Without this, an orkflow could be
// stored under a namespace the registry can never hold and no document can
// name: reachable by key, absent from every listing of namespaces.
//
// The error matches ErrInvalidNamespace and ErrInvalidOrkflow both, because it
// is truthfully each, and a caller checking either should see it.
func validOrkflowNamespace(namespace string) error {
	if err := validNamespaceName(namespace); err != nil {
		return fmt.Errorf("%w: %w", err, ErrInvalidOrkflow)
	}
	return nil
}

// orkflowNamePrefix is the key prefix holding every version of one orkflow.
func orkflowNamePrefix(namespace, name string) (string, error) {
	prefix, err := OrkflowNamespacePrefix(namespace)
	if err != nil {
		return "", err
	}
	if err := validOrkflowComponent("name", name); err != nil {
		return "", err
	}
	return prefix + name + "/", nil
}

// parseOrkflowKey reverses OrkflowKey. Keys that are not definitions (a future
// document beside them, a stray object) report false and are skipped by List.
func parseOrkflowKey(key string) (namespace, name, version string, ok bool) {
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
	// Reserved metadata beside the versions (@current.json) is not a version,
	// and is skipped rather than decoded as one.
	if strings.HasPrefix(file, orkflowMetaPrefix) {
		return "", "", "", false
	}
	version, found = strings.CutSuffix(file, orkflowSuffix)
	if !found {
		return "", "", "", false
	}
	if validOrkflowNamespace(namespace) != nil ||
		validOrkflowComponent("name", name) != nil ||
		validOrkflowComponent("version", version) != nil {
		return "", "", "", false
	}
	return namespace, name, version, true
}

func validOrkflowComponent(kind, value string) error {
	switch {
	case value == "":
		return fmt.Errorf("artifacts: orkflow %s is empty: %w", kind, ErrInvalidOrkflow)
	case len(value) > maxOrkflowComponent:
		return fmt.Errorf("artifacts: orkflow %s %q exceeds %d bytes: %w",
			kind, value, maxOrkflowComponent, ErrInvalidOrkflow)
	case !cleanComponentPattern.MatchString(value):
		return fmt.Errorf("artifacts: orkflow %s %q must match %s: %w",
			kind, value, cleanComponentPattern, ErrInvalidOrkflow)
	}
	return nil
}

// namespacedOrkflow returns wf with its namespace resolved, so a stored
// document always names the namespace it lives in and a reader needs no key to
// know where it came from. The input message is never mutated: it belongs to
// the caller.
func namespacedOrkflow(wf *orkstackv1.Orkflow) *orkstackv1.Orkflow {
	if wf == nil || wf.GetNamespace() == ResolveNamespace(wf.GetNamespace()) {
		return wf
	}
	stamped := proto.Clone(wf).(*orkstackv1.Orkflow)
	stamped.Namespace = ResolveNamespace(wf.GetNamespace())
	return stamped
}

// marshalOrkflow renders the stored document: the orkflow message as canonical
// protojson, indented for a human reading it in a bucket browser.
//
// protojson deliberately perturbs its own insignificant whitespace, so the
// bytes are normalized through encoding/json. Storing the same definition twice
// then produces identical objects, which is what makes "this version was not
// rewritten" checkable by comparing bytes.
func marshalOrkflow(wf *orkstackv1.Orkflow) ([]byte, error) {
	if wf == nil {
		return nil, fmt.Errorf("artifacts: encode orkflow: %w: nil orkflow", ErrInvalidOrkflow)
	}
	raw, err := protojson.Marshal(wf)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode orkflow %q: %w", wf.GetName(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode orkflow %q: %w", wf.GetName(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// unmarshalOrkflow decodes a stored document. Unknown fields are discarded, so
// an engine can read a document a newer engine wrote.
func unmarshalOrkflow(document []byte) (*orkstackv1.Orkflow, error) {
	wf := &orkstackv1.Orkflow{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, wf); err != nil {
		return nil, fmt.Errorf("decode orkflow document: %w", err)
	}
	return wf, nil
}

// latestOrkflow picks the version applied most recently: the greatest
// created_at, ties broken by the greater version string so the answer is
// deterministic even when two versions carry the same stamp.
func latestOrkflow(versions []*orkstackv1.Orkflow) *orkstackv1.Orkflow {
	var latest *orkstackv1.Orkflow
	for _, candidate := range versions {
		if candidate == nil {
			continue
		}
		if latest == nil || moreRecentOrkflow(candidate, latest) {
			latest = candidate
		}
	}
	return latest
}

func moreRecentOrkflow(candidate, incumbent *orkstackv1.Orkflow) bool {
	a, b := candidate.GetCreatedAt().AsTime(), incumbent.GetCreatedAt().AsTime()
	if a.Equal(b) {
		return candidate.GetVersion() > incumbent.GetVersion()
	}
	return a.After(b)
}

// resolveByName reduces one prefix scan to the one version each name
// currently resolves to, ordered by name.
//
// It is the pointer's rule applied to a whole namespace: a name somebody
// promoted resolves to the promoted version, and a name nobody promoted
// resolves to its head. Listing had to learn this too — a list that showed
// the head while `run submit` executed the promoted version would report a
// version the deployment does not run.
func resolveByName(scan orkflowScan) []*orkstackv1.Orkflow {
	byName := map[string][]*orkstackv1.Orkflow{}
	names := make([]string, 0, len(scan.versions))
	for _, wf := range scan.versions {
		if _, seen := byName[wf.GetName()]; !seen {
			names = append(names, wf.GetName())
		}
		byName[wf.GetName()] = append(byName[wf.GetName()], wf)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Orkflow, 0, len(names))
	for _, name := range names {
		if pinned := pinnedOrkflow(byName[name], scan.current[name]); pinned != nil {
			out = append(out, pinned)
			continue
		}
		if head := pinnedOrkflow(byName[name], scan.headOf(name)); head != nil {
			out = append(out, head)
		}
	}
	return out
}

// pinnedOrkflow returns the named version out of one orkflow's versions, or
// nil when the pointer is unset or names a version this scan did not see.
//
// A pointer that names nothing present cannot happen through this store —
// SetCurrentVersion validates the target, and the one path that deletes a
// version object (Put, rolling back a version it wrote and could not commit)
// deletes a version nobody was ever handed — so falling back to the head here
// is not a policy about dangling pointers; it is what keeps a listing
// answerable when a version arrives between the LIST and the pointer's write.
// Get is stricter, because Get is the read a run is started from.
func pinnedOrkflow(versions []*orkstackv1.Orkflow, version string) *orkstackv1.Orkflow {
	if version == "" {
		return nil
	}
	for _, wf := range versions {
		if wf.GetVersion() == version {
			return wf
		}
	}
	return nil
}

// sameStoredOrkflow reports whether re-applying incoming would store exactly
// what stored already holds, which is what makes a repeat a no-op instead of a
// rewrite.
//
// created_at is excluded because it is stamped per apply: comparing it would
// make every re-apply "different" and every apply a rewrite — the defect this
// check exists to remove.
//
// The design's rule was "compare source_yaml". This compares the whole
// document (with created_at cleared) because source_yaml alone is empty for a
// programmatically built orkflow — engine tests and any future non-DSL writer
// — and two genuinely different definitions would then both compare as "" and
// the second would be silently dropped. Comparing everything is strictly
// safer: it never treats a difference as a repeat.
//
// It is also the reason a DERIVED version covers the source bytes rather than
// the document's semantic content (internal/dsl.sourceHash): this comparison
// is what "the same document" means, so a derivation that ignored comments
// would hand one version two documents and get the second refused below, with
// no version left for the author to publish it under.
func sameStoredOrkflow(stored, incoming *orkstackv1.Orkflow) bool {
	if stored.GetSourceYaml() != incoming.GetSourceYaml() {
		return false
	}
	a := proto.Clone(stored).(*orkstackv1.Orkflow)
	b := proto.Clone(incoming).(*orkstackv1.Orkflow)
	a.CreatedAt, b.CreatedAt = nil, nil
	return proto.Equal(a, b)
}

// S3OrkflowStore keeps orkflow definitions in an S3-compatible bucket, one
// immutable JSON object per version:
//
//	s3://{bucket}/metadata/orkflows/{namespace}/{name}/{version}.json
//
// It holds no cache. Apply writes the object; get and list read it back. A
// restarted engine therefore reloads every orkflow simply by reading, and two
// engines pointed at one bucket see the same catalog.
type S3OrkflowStore struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var _ OrkflowStore = (*S3OrkflowStore)(nil)

// NewS3OrkflowStore builds the orkflow store from the same ORK_S3_* config the
// artifact store uses.
func NewS3OrkflowStore(ctx context.Context, cfg Config) (*S3OrkflowStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3OrkflowStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the definitions.
func (s *S3OrkflowStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns. Binaries log it so
// an operator can see where orkflows are being kept.
func (s *S3OrkflowStore) Location() string { return s.uri(OrkflowPrefix + "/") }

// URI is the s3:// URI of one orkflow version's document.
func (s *S3OrkflowStore) URI(namespace, name, version string) (string, error) {
	key, err := OrkflowKey(namespace, name, version)
	if err != nil {
		return "", err
	}
	return s.uri(key), nil
}

func (s *S3OrkflowStore) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// Put writes one orkflow version's document, moves the head onto it, and
// returns the stored document with whether this call created it.
//
// The version is part of the key, so publishing a new version adds an object
// and leaves every earlier version byte for byte as it was: a run pinned to a
// version always reads the definition it pinned.
//
// A version already stored is never rewritten. Re-applying the identical
// document writes NOTHING and hands back what is there — the object keeps its
// original created_at, which is what makes "compare the stored bytes before
// and after" a real check rather than a comparison of two fresh timestamps —
// and it moves nothing either: a no-op is a no-op, the head included.
// A different document under the same version is ErrVersionExists: the fix is
// a new version, because rewriting this one would change what a pinned run
// reads.
//
// Both writes are conditional, and each condition answers a different race:
//
//   - the version object is created IfAbsent, so two replicas applying the
//     same new version at one instant produce one write and one refusal,
//     settled on the immutability rule above rather than by overwriting;
//   - the head pointer is compare-and-SWAPPED against the bytes this call
//     read, and THAT swap is the apply's commit point. An apply that stated
//     an expected head and loses the swap is refused with ErrHeadMoved — the
//     window a check before the write leaves open, closed. An apply that
//     stated nothing cannot be told it was wrong, so it re-reads what the
//     winner wrote and swaps against that.
//
// The document is written BEFORE the head moves, so no reader ever sees a
// head naming a version that is not in the bucket. That order leaves a window
// in the other direction — a version object whose head swap then fails — and
// **a Put that returns an error removes what it wrote**: the object it
// created in this call is deleted before the refusal is returned. Without
// that, a refused edit left its version addressable, listed and promotable
// though nobody was ever told it landed, and — worse — un-retryable: the
// user's retargeted retry of the same edit would find its own bytes already
// stored, take the identical-document no-op above, and never move the head.
// The compensation is the whole reason this is the ONE code path that deletes
// a version object, and it only ever deletes the object this call created,
// under a version nobody has been handed.
//
// Two objects without a transaction cannot be made atomic against a process
// that dies mid-apply, and that residue stays honest rather than hidden: a
// killed writer leaves a stored version that never became the head, inert and
// addressable, and the apply it belonged to answered nobody.
func (s *S3OrkflowStore) Put(
	ctx context.Context,
	wf *orkstackv1.Orkflow,
	expected HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	key, err := OrkflowKey(wf.GetNamespace(), wf.GetName(), wf.GetVersion())
	if err != nil {
		return nil, false, err
	}
	incoming := namespacedOrkflow(wf)
	namespace, name := incoming.GetNamespace(), incoming.GetName()

	stored, created, err := s.commitVersion(ctx, key, namespace, name, incoming, expected)
	if err == nil {
		return stored, created, nil
	}
	if !created {
		return nil, false, err
	}
	if removeErr := s.removeVersion(ctx, key); removeErr != nil {
		// The refusal is still the caller's answer — it is what happened —
		// but the residue it was supposed to clean up is back, so say so.
		return nil, false, fmt.Errorf(
			"%w (and the version this apply wrote could not be removed from %s: %v)",
			err, s.uri(key), removeErr)
	}
	return nil, false, err
}

// commitVersion is Put's body: write the version object, then move the head
// onto it under the condition the apply named.
//
// It reports whether THIS call created the version object on every path,
// error paths included, because that is what tells Put whether there is
// anything to remove.
func (s *S3OrkflowStore) commitVersion(
	ctx context.Context,
	key, namespace, name string,
	incoming *orkstackv1.Orkflow,
	expected HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	var (
		stored  *orkstackv1.Orkflow
		created bool
	)
	for range headSwapAttempts {
		pointer, etag, err := s.effectiveHead(ctx, namespace, name)
		if err != nil {
			return nil, created, err
		}
		if !expected.SatisfiedBy(pointer.Version) {
			return nil, created, headMoved(namespace, name, expected, pointer.Version)
		}
		if stored == nil {
			if stored, created, err = s.writeVersion(ctx, key,
				stampedOrkflow(incoming, pointer.AppliedAt)); err != nil {
				return nil, false, err
			}
			if !created {
				return stored, false, nil
			}
		}
		applied := stored.GetCreatedAt().AsTime()
		if !applied.After(pointer.AppliedAt) {
			// Somebody else's apply is stamped after ours and is already the
			// head. Theirs IS the newer apply; leaving the pointer where they
			// put it is what keeps the head and the catalog's order one
			// answer.
			return stored, created, nil
		}
		switch err := s.swapHead(ctx, namespace, name, etag, headPointer{
			Version:   stored.GetVersion(),
			AppliedAt: applied,
		}); {
		case err == nil:
			return stored, created, nil
		case errors.Is(err, ErrPreconditionFailed):
			if expected.Stated() {
				head, _, readErr := s.effectiveHead(ctx, namespace, name)
				if readErr != nil {
					return nil, created, readErr
				}
				return nil, created, headMoved(namespace, name, expected, head.Version)
			}
		default:
			return nil, created, err
		}
	}
	return nil, created, fmt.Errorf(
		"artifacts: orkflow %q in namespace %q: another apply moved the head on each of "+
			"%d attempts to publish version %q: %w",
		name, namespace, headSwapAttempts, incoming.GetVersion(), ErrPreconditionFailed)
}

// removeVersion deletes one version object. It is called on exactly one path —
// Put rolling back the object it just created and could not commit — and the
// comment on Put is where the reasoning lives.
func (s *S3OrkflowStore) removeVersion(ctx context.Context, key string) error {
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("artifacts: remove orkflow %q: %w", s.uri(key), err)
	}
	return nil
}

// stampedOrkflow returns wf carrying the stamp its apply lands with: its own,
// unless that is not after the head's (AppliedStamp). The caller's message is
// never mutated — it belongs to the caller — and an unchanged stamp costs no
// copy.
func stampedOrkflow(wf *orkstackv1.Orkflow, head time.Time) *orkstackv1.Orkflow {
	var caller time.Time
	if ts := wf.GetCreatedAt(); ts != nil {
		caller = ts.AsTime()
	}
	applied := AppliedStamp(caller, head)
	if applied.Equal(caller) {
		return wf
	}
	stamped := proto.Clone(wf).(*orkstackv1.Orkflow)
	stamped.CreatedAt = timestamppb.New(applied)
	return stamped
}

// writeVersion creates one version's object and reports whether this call
// created it. A version already stored is settled on the immutability rule.
func (s *S3OrkflowStore) writeVersion(
	ctx context.Context,
	key string,
	incoming *orkstackv1.Orkflow,
) (*orkstackv1.Orkflow, bool, error) {
	document, err := marshalOrkflow(incoming)
	if err != nil {
		return nil, false, err
	}
	switch stored, err := s.read(ctx, key); {
	case err == nil:
		return s.settle(stored, incoming, key)
	case errors.Is(err, ErrNotFound):
		// Nothing there: create it below.
	default:
		return nil, false, err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(orkflowContentType),
		// "*" is the S3 spelling of "only if nothing is there".
		IfNoneMatch: aws.String("*"),
	}); err != nil {
		if isPreconditionFailed(err) {
			// Another writer created this version between the read and the
			// write. Whatever they wrote is now the stored document, and the
			// immutability rule decides this call exactly as it would have
			// decided a second apply a minute later.
			stored, readErr := s.read(ctx, key)
			if readErr != nil {
				return nil, false, readErr
			}
			return s.settle(stored, incoming, key)
		}
		return nil, false, fmt.Errorf("artifacts: put orkflow %q: %w", s.uri(key), err)
	}
	return incoming, true, nil
}

// settle answers a Put whose version is already stored: an identical document
// is the repeat it looks like, a different one is refused.
func (s *S3OrkflowStore) settle(
	stored, incoming *orkstackv1.Orkflow,
	key string,
) (*orkstackv1.Orkflow, bool, error) {
	if sameStoredOrkflow(stored, incoming) {
		return stored, false, nil
	}
	return nil, false, fmt.Errorf(
		"artifacts: orkflow %q version %q in namespace %q is already stored with different "+
			"content (%s); a version is immutable, so publish a new version: %w",
		incoming.GetName(), incoming.GetVersion(), incoming.GetNamespace(),
		s.uri(key), ErrVersionExists)
}

// Get reads one orkflow version from one namespace. An empty version resolves
// through the current pointer when one is set, and to the most recently
// applied version when none is — never into another namespace.
//
// A pointer naming a version that is not stored is an ERROR here, not a quiet
// fall back to the head. Nothing in this store can produce that state
// (SetCurrentVersion validates its target, and the only path that deletes a
// version — Put rolling back a version it could not commit — deletes one that
// was never promotable because nobody was told it existed), so reaching it
// means the bucket was edited underneath the engine — and silently running a
// different version than the pointer names is the one answer nobody could
// debug.
func (s *S3OrkflowStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	if version == "" {
		current, err := s.CurrentVersion(ctx, namespace, name)
		if err != nil {
			return nil, err
		}
		if current != "" {
			key, err := OrkflowKey(namespace, name, current)
			if err != nil {
				return nil, err
			}
			wf, err := s.read(ctx, key)
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf(
					"artifacts: orkflow %q in namespace %q points at version %q, "+
						"which is not stored: %w",
					name, ResolveNamespace(namespace), current, ErrNotFound)
			}
			return wf, err
		}
		scan, err := s.scan(ctx, namespace, name)
		if err != nil {
			return nil, err
		}
		head := scan.headOf(name)
		if head == "" {
			return nil, fmt.Errorf("artifacts: get orkflow %q in namespace %q: %w",
				name, ResolveNamespace(namespace), ErrNotFound)
		}
		wf := pinnedOrkflow(scan.versions, head)
		if wf == nil {
			return nil, fmt.Errorf(
				"artifacts: orkflow %q in namespace %q has head %q, which is not stored: %w",
				name, ResolveNamespace(namespace), head, ErrNotFound)
		}
		return wf, nil
	}
	key, err := OrkflowKey(namespace, name, version)
	if err != nil {
		return nil, err
	}
	return s.read(ctx, key)
}

// List returns the version every orkflow in one namespace resolves to, ordered
// by name. It is one prefix scan: another namespace's documents are never
// read, let alone filtered out afterwards, and the pointers come out of the
// same listing as the definitions — a promoted orkflow costs no extra request.
func (s *S3OrkflowStore) List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	prefix, err := OrkflowNamespacePrefix(namespace)
	if err != nil {
		return nil, err
	}
	scan, err := s.readPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return resolveByName(scan), nil
}

// Versions returns one orkflow's whole version catalog in one prefix scan.
//
// Both pointers live under the same prefix as the versions, so Current and
// Head come back from the listing this call already makes: neither costs an
// extra round trip, and neither is guessed from a timestamp.
func (s *S3OrkflowStore) Versions(
	ctx context.Context,
	namespace, name string,
) (OrkflowVersions, error) {
	scan, err := s.scan(ctx, namespace, name)
	if err != nil {
		return OrkflowVersions{}, err
	}
	sortOrkflowsNewestFirst(scan.versions)
	return OrkflowVersions{
		All:     scan.versions,
		Current: scan.current[name],
		Head:    scan.headOf(name),
	}, nil
}

// sortOrkflowsNewestFirst orders a version catalog by application time, newest
// first, with the same tie-break latestOrkflow uses.
//
// The stamps are safe to order by because the store, not the caller, has the
// last word on them: an apply never stamps its version earlier than the head
// it extends (AppliedStamp), so the order here is the order applies committed
// in even when the replicas that made them disagree about the time. The head
// itself is still the pointer's, never this order's first element — the two
// agree, and where they could not, the pointer is the one that was committed.
func sortOrkflowsNewestFirst(versions []*orkstackv1.Orkflow) {
	sort.SliceStable(versions, func(i, j int) bool {
		return moreRecentOrkflow(versions[i], versions[j])
	})
}

// scan reads everything stored for one orkflow in one namespace.
func (s *S3OrkflowStore) scan(ctx context.Context, namespace, name string) (orkflowScan, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return orkflowScan{}, err
	}
	return s.readPrefix(ctx, prefix)
}

// orkflowScan is what one prefix scan found: every definition under it, and
// the two pointers that live beside them, each keyed by orkflow name.
type orkflowScan struct {
	versions []*orkstackv1.Orkflow
	current  map[string]string
	head     map[string]string
}

// headOf is the version one orkflow's head names, and derivedHeadOf is the
// document it is.
//
// The pointer decides, because moving the pointer is what an apply commits.
// The documents' own stamps answer only when the pointer cannot: an orkflow
// last applied by an engine built before the head became a pointer, or — and
// this is why the check is on membership rather than on emptiness — a pointer
// naming a version this scan did not see. Put writes the document BEFORE it
// moves the pointer, so that state should be unreachable; falling back rather
// than reporting a head nothing holds is what keeps a listing answerable if it
// ever is, exactly as pinnedOrkflow does for the current pointer.
//
// The derived answer is not a listing's private convenience: S3OrkflowStore.Put
// checks an apply's expected head against the SAME rule (effectiveHead), or a
// catalog stored before @head.json existed would be readable and not editable
// — an editor shown 1.0.0 as the head, and an apply naming 1.0.0 refused
// against a head of "".
func (scan orkflowScan) headOf(name string) string {
	if head := scan.derivedHeadOf(name); head != nil {
		return head.GetVersion()
	}
	return ""
}

func (scan orkflowScan) derivedHeadOf(name string) *orkstackv1.Orkflow {
	versions := scan.versionsOf(name)
	if head := pinnedOrkflow(versions, scan.head[name]); head != nil {
		return head
	}
	return latestOrkflow(versions)
}

// versionsOf narrows a scan to one orkflow's versions. A per-name scan holds
// only one orkflow's documents; a namespace-wide one holds every orkflow's.
func (scan orkflowScan) versionsOf(name string) []*orkstackv1.Orkflow {
	versions := make([]*orkstackv1.Orkflow, 0, len(scan.versions))
	for _, wf := range scan.versions {
		if wf.GetName() == name {
			versions = append(versions, wf)
		}
	}
	return versions
}

// readPrefix lists a prefix once and reads what is under it: every definition,
// and both pointers beside them (name -> version).
//
// One scan answers all three because they share the prefix. Orkflow catalogs
// are small (one document per version), and reading the documents is what
// makes a listing exact rather than a guess from S3 timestamps.
func (s *S3OrkflowStore) readPrefix(ctx context.Context, prefix string) (orkflowScan, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	scan := orkflowScan{current: map[string]string{}, head: map[string]string{}}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return orkflowScan{}, fmt.Errorf("artifacts: list orkflows %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if namespace, name, ok := ParseOrkflowCurrentKey(key); ok {
				pointer, _, err := s.readCurrent(ctx, namespace, name)
				if err != nil {
					return orkflowScan{}, err
				}
				if pointer.Version != "" {
					scan.current[name] = pointer.Version
				}
				continue
			}
			if namespace, name, ok := ParseOrkflowHeadKey(key); ok {
				pointer, _, err := s.readHead(ctx, namespace, name)
				if err != nil {
					return orkflowScan{}, err
				}
				if pointer.Version != "" {
					scan.head[name] = pointer.Version
				}
				continue
			}
			if _, _, _, ok := parseOrkflowKey(key); !ok {
				continue
			}
			wf, err := s.read(ctx, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between the listing and the read
				}
				return orkflowScan{}, err
			}
			scan.versions = append(scan.versions, wf)
		}
	}
	return scan, nil
}

func (s *S3OrkflowStore) read(ctx context.Context, key string) (*orkstackv1.Orkflow, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get orkflow %q: read body: %w", s.uri(key), err)
	}
	wf, err := unmarshalOrkflow(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), err)
	}
	return wf, nil
}

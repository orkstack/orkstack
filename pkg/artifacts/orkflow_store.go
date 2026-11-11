package artifacts

import (
	"bytes"
	"context"
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
	"github.com/orkstack/orkstack/internal/stablejson"
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
	// NoHeadExpectation() is the unchecked apply — `orkctl orkflow create file.yaml`,
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
	//
	// expected is what the promotion claims the pointer names now: a stated
	// expectation that no longer holds when the swap is made is refused with a
	// *CurrentMovedError (errors.Is ErrCurrentMoved), never an overwrite.
	SetCurrentVersion(ctx context.Context, namespace, name, version string,
		expected CurrentExpectation) (string, error)
	// Delete permanently removes every version, both pointers and every
	// derived claim for one orkflow. A missing orkflow returns ErrNotFound.
	Delete(ctx context.Context, namespace, name string) error
	// FindDerivedVersion reports the version a document that pins none was
	// given, by its content hash, "" when it was never given one. It only
	// reads, and it answers for a version stored under the hash itself.
	FindDerivedVersion(ctx context.Context, namespace, name, contentHash string) (string, error)
	// ClaimDerivedVersion gives a content hash its version: the one it already
	// has, or candidate. Every caller for one hash gets one answer.
	ClaimDerivedVersion(ctx context.Context, namespace, name, contentHash, candidate string) (string, error)
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
// their own rules — a version is a semver, a minted id or a content hash, not a slug — but
// a namespace is the same concept here as in metadata/namespaces/, and one
// concept gets one rule (AGENTS.md §7). Without this, an orkflow could be
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
	if err := stablejson.Indent(&out, raw, "  "); err != nil {
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
// A pointer that names nothing present happens through this store only for
// the moment a rollback (Put, removing a version it wrote and could not
// commit) races a pointer moved onto that version, until one side reads the
// other back and repairs it (Get says how). Falling back to the head here is
// not a policy about dangling pointers; it is what keeps a listing answerable
// then, and when a version arrives between the LIST and the pointer's write.
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

// SameStoredOrkflow reports whether storing incoming would store what stored
// holds: the same source bytes, and every other field equal but created_at.
//
// This is what makes a repeat a no-op instead of a rewrite. created_at is
// excluded because it is stamped per apply: comparing it would make every
// re-apply "different" and every apply a rewrite — the defect this check
// exists to remove.
//
// The design's rule was "compare source_yaml". This compares the whole
// document (with created_at cleared) because source_yaml alone is empty for a
// programmatically built orkflow — engine tests and any future non-DSL writer
// — and two genuinely different definitions would then both compare as "" and
// the second would be silently dropped. Comparing everything is strictly
// safer: it never treats a difference as a repeat.
//
// It is also the reason a document's content hash covers the whole source rather
// than its semantic content (internal/dsl.ContentHash): this comparison is what
// "the same document" means, so a hash that ignored comments would hand one
// version two documents and get the second refused below, with no version left
// for the author to publish it under. The engine's in-memory store calls this
// function too, so there is one rule.
func SameStoredOrkflow(stored, incoming *orkstackv1.Orkflow) bool {
	if !bytes.Equal([]byte(stored.GetSourceYaml()), []byte(incoming.GetSourceYaml())) {
		return false
	}
	a, b := proto.Clone(stored).(*orkstackv1.Orkflow), proto.Clone(incoming).(*orkstackv1.Orkflow)
	a.CreatedAt, b.CreatedAt = nil, nil
	a.SourceYaml, b.SourceYaml = "", ""
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

	stored, document, created, known, err := s.commitVersion(ctx, key, namespace, name, incoming, expected)
	if err == nil {
		return stored, created, nil
	}
	var deleted *deletedAsAppliedError
	if !created || errors.As(err, &deleted) {
		// A Delete that beat this apply took its version too: there is nothing
		// to roll back, and nothing more to say.
		return nil, false, err
	}
	if !known {
		// The create's answer was lost and only a read found this exact
		// document there — this apply's, or an identical apply's that may
		// already be the head — or the head swap's outcome is unknown, so the
		// head may name it. Removing it could leave a head naming nothing, so
		// this apply leaves it as it is and says so; applying the document
		// again completes its commit (commitVersion). An identical apply that
		// is refused decides for itself what it wrote.
		return nil, false, fmt.Errorf(
			"%w (this apply left the version it wrote at %s as it is: its create or its head swap lost "+
				"its answer, so the version may be an identical apply's, or the head's)", err, s.uri(key))
	}
	// A refused apply leaves nothing stored: its version is not addressable,
	// listed or promotable, and a retargeted retry of the same document
	// creates it afresh. Except that a version a pointer names — the head, or
	// the current pointer a promotion of the listed version moved — is never
	// removed, nor one whose pointers cannot be read.
	version := incoming.GetVersion()
	switch names, readErr := s.pointersName(ctx, namespace, name, version); {
	case readErr != nil:
		return nil, false, fmt.Errorf(
			"%w (the version stays stored at %s: its pointers could not be read, so whether one names it "+
				"is not known: %v)", err, s.uri(key), readErr)
	case names:
		return nil, false, fmt.Errorf(
			"%w (the version stays stored at %s: the head or the current pointer names it)", err, s.uri(key))
	}
	written, raw, etag, readErr := s.readRaw(ctx, key)
	switch {
	case errors.Is(readErr, ErrNotFound):
		// A Delete of the orkflow took it first: nothing is stored.
		return nil, false, err
	case readErr != nil:
		return nil, false, fmt.Errorf("%w (the version stays stored at %s: it could not be read: %v)",
			err, s.uri(key), readErr)
	case !bytes.Equal(raw, document):
		// Not the document this apply wrote: a Delete of the orkflow removed
		// it, and another apply created this version afresh, whose own commit
		// decides it. Its document carries its own apply's instant to the
		// nanosecond, so equal bytes would be this apply's.
		return nil, false, fmt.Errorf(
			"%w (another document is stored under this version at %s now, and is left alone: a Delete "+
				"removed the one this apply wrote, and another apply created it again)", err, s.uri(key))
	}
	if removeErr := s.removeVersion(ctx, key, etag); removeErr != nil {
		// The refusal is still the caller's answer — it is what happened —
		// but the residue it was supposed to clean up is back, so say so.
		return nil, false, fmt.Errorf(
			"%w (and the version this apply wrote could not be removed from %s: %v)",
			err, s.uri(key), removeErr)
	}
	// The read above and the removal are two requests. An identical apply that
	// found this version stored ahead of the head may move the head onto it,
	// and a promotion of the listed version may move the current pointer onto
	// it, at any point: before the read, which keeps the version; between the
	// read and the re-read below, which puts it back; or after the re-read,
	// when that apply's or promotion's own check puts it back (ensureHeadVersion,
	// confirmPromoted). Each restore is the same bytes, created only if absent,
	// so whichever lands first the other finds there. Until then a reader can find a
	// pointer naming a version that is not there, and is told so. A pointer
	// that cannot be read is not guessed at — putting the version back then
	// could undo a Delete of the whole orkflow — and the caller is told. (An
	// identical apply that answered "already stored" with the head past this
	// version can still find it gone, as with every rollback: explicitly not
	// found, never another version. A Delete landing between a re-read and its
	// restore can leave one version object behind it, and the orkflow then
	// reappears with that version until it is deleted again.)
	names, readErr := s.pointersName(ctx, namespace, name, version)
	if readErr != nil {
		return nil, false, fmt.Errorf(
			"%w (the version this apply wrote was removed from %s, and its pointers could not be read "+
				"afterwards: %v; if one names this version, applying this document again against the head as "+
				"it is now stores it again)",
			err, s.uri(key), readErr)
	}
	if names {
		if restoreErr := s.restoreVersion(ctx, key, raw, written); restoreErr != nil {
			return nil, false, fmt.Errorf(
				"%w (a pointer names the version this apply removed from %s, and putting it back "+
					"failed: %v; applying this document again against the head as it is now stores it again)",
				err, s.uri(key), restoreErr)
		}
		return nil, false, fmt.Errorf(
			"%w (the version stays stored at %s: an identical apply made it the head, or a promotion "+
				"made it current)", err, s.uri(key))
	}
	return nil, false, err
}

// pointersName reports whether the head or the current pointer names version.
func (s *S3OrkflowStore) pointersName(ctx context.Context, namespace, name, version string) (bool, error) {
	head, _, err := s.readHead(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	current, err := s.CurrentVersion(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	return head.Version == version || current == version, nil
}

// commitVersion is Put's body: write the version object, then move the head
// onto it under the condition the apply named.
//
// It reports whether THIS call created the version object on every path,
// error paths included, because that is what tells Put whether there is
// anything to remove, and whether it knows it did (writeVersion), with the
// bytes it wrote, so the removal takes only that document.
func (s *S3OrkflowStore) commitVersion(
	ctx context.Context,
	key, namespace, name string,
	incoming *orkstackv1.Orkflow,
	expected HeadExpectation,
) (*orkstackv1.Orkflow, []byte, bool, bool, error) {
	var (
		stored         *orkstackv1.Orkflow
		raw            []byte
		created, known bool
		// The incarnation of the first head pointer this apply read. A later
		// round that finds another incarnation's head read an orkflow a Delete
		// removed; one that finds no head at all after a refused swap — which
		// proves a head existed — read one a Delete took.
		seenPresent bool
		seenSince   time.Time
		looped      bool
		// Whether the first read found a head pointer: only then can a later
		// round's incarnation be the one this apply's version was read under.
		sawPointerFirst bool
	)
	for attempt := range headSwapAttempts {
		pointer, etag, err := s.effectiveHead(ctx, namespace, name)
		if err != nil {
			return nil, raw, created, known, err
		}
		if attempt == 0 {
			sawPointerFirst = etag != ""
		}
		switch {
		case looped && etag == "":
			return nil, raw, created, known, fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted as "+
				"version %q was applied, or a Delete of it stopped part way (deleting it again finishes it): %w",
				name, ResolveNamespace(namespace), incoming.GetVersion(), ErrNotFound)
		case seenPresent && !pointer.Since.IsZero() && !pointer.Since.Equal(seenSince):
			return nil, raw, created, known, fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted as "+
				"version %q was applied, and another apply has re-created it since: %w",
				name, ResolveNamespace(namespace), incoming.GetVersion(), ErrNotFound)
		case !seenPresent && etag != "":
			seenPresent, seenSince = true, pointer.Since
		}
		if !expected.SatisfiedBy(pointer.Version) {
			return nil, raw, created, known, headMoved(namespace, name, expected, pointer.Version)
		}
		if stored == nil {
			if stored, raw, created, known, err = s.writeVersion(ctx, key,
				stampedOrkflow(incoming, pointer.AppliedAt)); err != nil {
				return nil, nil, false, false, err
			}
			if !created && !stored.GetCreatedAt().AsTime().After(pointer.AppliedAt) {
				// The repeat it looks like: the version is stored, and the
				// head is at it or past it.
				return stored, raw, false, false, nil
			}
			// Created now, or stored ahead of the head by an apply whose head
			// swap never landed — it lost the swap after a create whose answer
			// was lost, or its process ended between the two writes. This
			// apply of the same document completes that commit; it stored no
			// version, so it still reports created=false.
		}
		// After a refused swap, this apply's version is read again before
		// anything else: a Delete — or, for a version it does not know it
		// created, a refused apply's rollback — may have taken it. Gone, the apply is
		// refused without touching the head, which may be another apply's
		// that re-created the orkflow; only a version a rollback removed
		// within this incarnation goes on, for the swap to put it back.
		rolledBack := false
		if looped {
			var err error
			// The incarnation is the one this apply read before its refused
			// swap only when that read was a pointer head with the since this
			// round finds.
			sameIncarnation := sawPointerFirst && etag != "" && !pointer.Since.IsZero() &&
				pointer.Since.Equal(seenSince)
			if rolledBack, err = s.versionStillStored(ctx, namespace, name, key, stored, known, pointer,
				sameIncarnation); err != nil {
				return nil, raw, created, known, err
			}
		}
		applied := stored.GetCreatedAt().AsTime()
		if !applied.After(pointer.AppliedAt) {
			// Somebody else's apply is stamped after ours and is already the
			// head. Theirs IS the newer apply; leaving the pointer where they
			// put it is what keeps the head and the catalog's order one
			// answer. A version a rollback removed is then not this apply's
			// to put back: no pointer will name it.
			if rolledBack {
				return nil, raw, created, known, removedSinceFound(namespace, name, stored.GetVersion())
			}
			return stored, raw, created, known, nil
		}
		// A head swapped in from nothing begins an incarnation; every swap
		// after copies its since, or copies none from a head without one.
		since := pointer.Since
		if etag == "" {
			since = applied
		}
		switch err := s.swapHead(ctx, namespace, name, etag, headPointer{
			Version:   stored.GetVersion(),
			AppliedAt: applied,
			Since:     since,
		}); {
		case err == nil && etag == "":
			// A head swapped in from nothing: a Delete of the orkflow may have
			// taken the head this apply read as absent, and then its version.
			if err := s.confirmHeadFromNothing(ctx, namespace, name, key, stored, applied, known); err != nil {
				return nil, raw, created, known, err
			}
			return stored, raw, created, known, nil
		case err == nil && !known:
			// This apply moved the head onto a version it does not know it
			// created — one it found stored, or one only a read showed landed:
			// the refused apply that did create it may be rolling it back. Its
			// re-read of the head covers a move that lands before that read;
			// this check covers a move that lands after it.
			if err := s.ensureHeadVersion(ctx, namespace, name, key, stored, raw); err != nil {
				return nil, nil, false, false, err
			}
			return stored, raw, created, known, nil
		case err == nil && looped:
			// A swap after a refused one: a Delete in between may have taken
			// this apply's version, and the head would then name nothing.
			if err := s.confirmHeadFromNothing(ctx, namespace, name, key, stored, applied, known); err != nil {
				return nil, raw, created, known, err
			}
			return stored, raw, created, known, nil
		case err == nil:
			return stored, raw, created, known, nil
		case errors.Is(err, ErrPreconditionFailed):
			if expected.Stated() {
				// The head pointer itself, never one derived from the catalog,
				// which would list this apply's own uncommitted version.
				head, headETag, readErr := s.readHead(ctx, namespace, name)
				if readErr != nil {
					return nil, raw, created, known, readErr
				}
				if headETag != "" && head.Version == stored.GetVersion() && head.AppliedAt.Equal(applied) {
					// An identical apply moved the head onto exactly this
					// version at this stamp first: this apply's commit landed.
					return stored, raw, created, known, nil
				}
				return nil, raw, created, known, headMoved(namespace, name, expected, head.Version)
			}
			if errors.Is(err, errHeadGone) {
				// The head this apply read is gone, and only a Delete of the
				// orkflow removes it: this apply lost to it. Going round again
				// would derive the head from this apply's own uncommitted
				// version and report a commit that never moved a head, for a
				// version the Delete may then remove. Its rollback removes it.
				return nil, raw, created, known, fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted "+
					"as version %q was applied, or a Delete of it stopped part way (deleting it again finishes it): %w",
					name, ResolveNamespace(namespace), stored.GetVersion(), ErrNotFound)
			}
			looped = true
		default:
			// Not a refusal: the swap may have landed (errOutcomeUnknown, or
			// an error a lost answer left), so the version is not known to
			// be removable.
			return nil, raw, created, false, err
		}
	}
	return nil, raw, created, known, fmt.Errorf(
		"artifacts: orkflow %q in namespace %q: another apply moved the head on each of "+
			"%d attempts to publish version %q: %w",
		name, namespace, headSwapAttempts, incoming.GetVersion(), ErrPreconditionFailed)
}

// ensureHeadVersion makes sure a version the head was just moved onto is still
// stored, as the document this apply read: when it is gone and the head still
// names it, its exact bytes are put back (restoreVersion). A head that has
// moved on, or been deleted with the whole orkflow, needs nothing, and nothing
// is put back for it. Another document under this version — created afresh
// after a refused apply removed it — is never reported as this one.
func (s *S3OrkflowStore) ensureHeadVersion(
	ctx context.Context,
	namespace, name, key string,
	stored *orkstackv1.Orkflow,
	raw []byte,
) error {
	switch found, _, _, err := s.readRaw(ctx, key); {
	case err == nil:
		if !SameStoredOrkflow(found, stored) {
			return fmt.Errorf("artifacts: the head of orkflow %q names %q, and the document stored under it "+
				"now is another (%s): %w", name, stored.GetVersion(), s.uri(key), ErrVersionExists)
		}
		return nil
	case !errors.Is(err, ErrNotFound):
		return fmt.Errorf("artifacts: the head of orkflow %q names %q, which could not be read back: %w",
			name, stored.GetVersion(), err)
	}
	head, _, err := s.readHead(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("artifacts: the head of orkflow %q was moved onto %q, which is gone, "+
			"and the head could not be read again: %w", name, stored.GetVersion(), err)
	}
	if head.Version != stored.GetVersion() {
		return nil
	}
	if err := s.restoreVersion(ctx, key, raw, stored); err != nil {
		return fmt.Errorf("artifacts: the head of orkflow %q names %q, which a refused apply removed, "+
			"and putting it back failed: %w", name, stored.GetVersion(), err)
	}
	return nil
}

// confirmHeadFromNothing reads back the version an apply just made the head of:
// after a swap onto an orkflow that had no head pointer, created only if
// absent, and after a swap that followed a refused one. A Delete of the
// orkflow races either — one that took the head before this apply read it, or
// between its swaps, and this version after, leaves the head this swap wrote
// naming nothing — and for a version this apply did not create, so does the
// rollback of the refused apply that did, which this apply cannot tell from a
// Delete and is told neutrally. The apply loses to the Delete: its own head is removed,
// under that head's etag, nothing is put back, and the apply is refused as the
// orkflow deleted — or, when another apply's head is there instead, as the
// orkflow re-created since. Another document under the version is reported,
// as ensureHeadVersion reports it.
func (s *S3OrkflowStore) confirmHeadFromNothing(
	ctx context.Context,
	namespace, name, key string,
	stored *orkstackv1.Orkflow,
	applied time.Time,
	known bool,
) error {
	switch found, _, _, err := s.readRaw(ctx, key); {
	case err == nil && SameStoredOrkflow(found, stored):
		return nil
	case err == nil:
		return fmt.Errorf("artifacts: the head of orkflow %q names %q, and the document stored under it "+
			"now is another (%s): %w", name, stored.GetVersion(), s.uri(key), ErrVersionExists)
	case !errors.Is(err, ErrNotFound):
		return fmt.Errorf("artifacts: the head of orkflow %q names %q, which could not be read back: %w",
			name, stored.GetVersion(), err)
	}
	head, etag, err := s.readHead(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("artifacts: the head of orkflow %q was moved onto %q, which is gone, and the head "+
			"could not be read again: %w", name, stored.GetVersion(), err)
	}
	if !known {
		if etag != "" && head.Version == stored.GetVersion() && head.AppliedAt.Equal(applied) {
			if err := s.removeOwnHead(ctx, namespace, name, etag); err != nil {
				return err
			}
		}
		return removedSinceFound(namespace, name, stored.GetVersion())
	}
	if etag != "" && (head.Version != stored.GetVersion() || !head.AppliedAt.Equal(applied)) {
		// Another apply's head: it re-created the orkflow since, and this
		// version is gone with the orkflow it was applied to.
		return &deletedAsAppliedError{fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted as version "+
			"%q was applied, and another apply has re-created the orkflow since; this version is gone: %w",
			name, ResolveNamespace(namespace), stored.GetVersion(), ErrNotFound)}
	}
	if etag != "" {
		if err := s.removeOwnHead(ctx, namespace, name, etag); err != nil {
			return err
		}
	}
	return &deletedAsAppliedError{fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted as version %q was "+
		"applied, or a Delete of it stopped part way (deleting it again finishes it): %w",
		name, ResolveNamespace(namespace), stored.GetVersion(), ErrNotFound)}
}

// removeOwnHead removes the head this apply wrote, under that head's etag:
// one another writer has moved since is theirs, and is left alone.
func (s *S3OrkflowStore) removeOwnHead(ctx context.Context, namespace, name, etag string) error {
	headKey, err := OrkflowHeadKey(namespace, name)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket),
		Key: aws.String(headKey), IfMatch: aws.String(etag)}); err != nil && !isPreconditionFailed(err) &&
		!isNotFound(err) {
		return &deletedAsAppliedError{fmt.Errorf("artifacts: the version orkflow %q in namespace %q was applied "+
			"with is gone, and removing this apply's head failed, so it still names the removed version (apply "+
			"again or delete the orkflow to clear it): %w", name, ResolveNamespace(namespace), err)}
	}
	return nil
}

// versionStillStored reads an apply's version back before a round that
// follows a refused swap: gone, the apply is refused without touching the
// head; another document under it is reported. An apply that knows it created
// its version knows only a Delete removes it. One that does not — it found the
// identical document a refused apply created, or its own create's answer was
// lost — goes on only when the head it now reads is of the incarnation it read
// a pointer head of before its refused swap, and the version was created
// within it: that is the refused apply's rollback, and its swap puts the
// version back (ensureHeadVersion); rolledBack says so. Otherwise — a first
// read with no head pointer, another incarnation, or a head that records none
// — it cannot tell a rollback from a Delete, and is refused saying so.
func (s *S3OrkflowStore) versionStillStored(
	ctx context.Context,
	namespace, name, key string,
	stored *orkstackv1.Orkflow,
	known bool,
	head headPointer,
	sameIncarnation bool,
) (rolledBack bool, err error) {
	switch found, _, _, err := s.readRaw(ctx, key); {
	case err == nil && SameStoredOrkflow(found, stored):
		return false, nil
	case err == nil:
		return false, fmt.Errorf("artifacts: version %q of orkflow %q, which this apply was committing, is "+
			"another document now (%s): %w", stored.GetVersion(), name, s.uri(key), ErrVersionExists)
	case !errors.Is(err, ErrNotFound):
		return false, fmt.Errorf("artifacts: version %q of orkflow %q, which this apply was committing, could "+
			"not be read back: %w", stored.GetVersion(), name, err)
	case !known && sameIncarnation && !stored.GetCreatedAt().AsTime().Before(head.Since):
		return true, nil
	case !known:
		return false, removedSinceFound(namespace, name, stored.GetVersion())
	}
	return false, &deletedAsAppliedError{fmt.Errorf("artifacts: orkflow %q in namespace %q was deleted as version %q was "+
		"applied, or another apply has re-created it since; this version is gone: %w",
		name, ResolveNamespace(namespace), stored.GetVersion(), ErrNotFound)}
}

// removedSinceFound is an apply that does not know it created its version —
// it found it stored, ahead of the head, or its own create's answer was lost —
// refused because the version is gone: the rollback of a refused apply that
// created it, or a Delete, which it cannot tell apart.
func removedSinceFound(namespace, name, version string) error {
	return &deletedAsAppliedError{fmt.Errorf("artifacts: version %q of orkflow %q in namespace %q, which this "+
		"apply did not know it created, was removed since — a refused apply's rollback, or a Delete; apply "+
		"again: %w", version, name, ResolveNamespace(namespace), ErrNotFound)}
}

// deletedAsAppliedError is an apply that lost to a Delete of its orkflow: its
// version is gone, and Put has nothing to roll back.
type deletedAsAppliedError struct{ err error }

func (e *deletedAsAppliedError) Error() string { return e.err.Error() }
func (e *deletedAsAppliedError) Unwrap() error { return e.err }

// restoreVersion puts back the exact bytes of a version object a rollback took
// away, created only if absent. A version already there is read: the same
// document (an identical apply's fresh create) is as good, and another one is
// reported, never taken for this one.
func (s *S3OrkflowStore) restoreVersion(ctx context.Context, key string, raw []byte, written *orkstackv1.Orkflow) error {
	// Equal bytes after a lost answer (putConfirmed) are this restore or the
	// same version object.
	_, err := putConfirmed(ctx, s.client, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(raw),
		ContentLength: aws.Int64(int64(len(raw))),
		ContentType:   aws.String(orkflowContentType),
		IfNoneMatch:   aws.String("*"),
	}, raw)
	switch {
	case err == nil:
		return nil
	case !isPreconditionFailed(err):
		return fmt.Errorf("artifacts: restore orkflow %q: %w", s.uri(key), err)
	}
	found, readErr := s.read(ctx, key)
	if readErr != nil {
		return fmt.Errorf("artifacts: restore orkflow %q: a version is there, and reading it failed: %w",
			s.uri(key), readErr)
	}
	if !SameStoredOrkflow(found, written) {
		return fmt.Errorf("artifacts: restore orkflow %q: another document is stored under this version: %w",
			s.uri(key), ErrVersionExists)
	}
	return nil
}

// removeVersion deletes one version object. It is called on exactly one path —
// Put rolling back the object it just created and could not commit — and the
// comment on Put is where the reasoning lives. The rollback calls it only once
// the bytes it read are exactly the document its apply wrote, and the delete
// is held to the etag that read returned (If-Match): a Delete of the orkflow
// and a fresh create of this version with other content between that read and
// this removal leave the new object alone. Every Orker and every engine asks
// the store at boot whether it honours that condition, and refuses to start on
// one that does not (probeConditionalAnswers; docs/STORAGE.md).
func (s *S3OrkflowStore) removeVersion(ctx context.Context, key, etag string) error {
	input := &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if etag != "" {
		input.IfMatch = aws.String(etag)
	}
	_, err := s.client.DeleteObject(ctx, input)
	switch {
	case err == nil, isNotFound(err):
		return nil
	case isPreconditionFailed(err):
		return fmt.Errorf("artifacts: remove orkflow %q: another object is there now, and is left alone: %w",
			s.uri(key), ErrPreconditionFailed)
	}
	return fmt.Errorf("artifacts: remove orkflow %q: %w", s.uri(key), err)
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
// created it, and whether it knows it did. A version already stored is
// settled on the immutability rule.
//
// A create whose answer was lost and which a read found landed (putConfirmed)
// is reported created, so the apply goes on to move the head, but not known:
// two applies of one document behind the same head can be stamped alike —
// when neither caller's stamp is after the head's, each gets the head's plus
// 1 ns (AppliedStamp) — so the equal bytes may be the other apply's, and Put
// never removes a version it does not know it wrote.
func (s *S3OrkflowStore) writeVersion(
	ctx context.Context,
	key string,
	incoming *orkstackv1.Orkflow,
) (*orkstackv1.Orkflow, []byte, bool, bool, error) {
	document, err := marshalOrkflow(incoming)
	if err != nil {
		return nil, nil, false, false, err
	}
	switch stored, raw, _, err := s.readRaw(ctx, key); {
	case err == nil:
		stored, created, err := s.settle(stored, incoming, key)
		return stored, raw, created, false, err
	case errors.Is(err, ErrNotFound):
		// Nothing there: create it below.
	default:
		return nil, nil, false, false, err
	}
	outcome, err := putConfirmed(ctx, s.client, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(orkflowContentType),
		// "*" is the S3 spelling of "only if nothing is there".
		IfNoneMatch: aws.String("*"),
	}, document)
	if err != nil {
		if isPreconditionFailed(err) {
			// Another writer created this version between the read and the
			// write. Whatever they wrote is now the stored document, and the
			// immutability rule decides this call exactly as it would have
			// decided a second apply a minute later.
			stored, raw, _, readErr := s.readRaw(ctx, key)
			if readErr != nil {
				return nil, nil, false, false, readErr
			}
			stored, created, err := s.settle(stored, incoming, key)
			return stored, raw, created, false, err
		}
		return nil, nil, false, false, fmt.Errorf("artifacts: put orkflow %q: %w", s.uri(key), err)
	}
	return incoming, document, true, !outcome.ReadBack, nil
}

// settle answers a Put whose version is already stored: an identical document
// is the repeat it looks like, a different one is refused.
func (s *S3OrkflowStore) settle(
	stored, incoming *orkstackv1.Orkflow,
	key string,
) (*orkstackv1.Orkflow, bool, error) {
	if SameStoredOrkflow(stored, incoming) {
		return stored, false, nil
	}
	return nil, false, fmt.Errorf(
		"artifacts: orkflow %q version %q in namespace %q is already stored with different "+
			"content (%s); %s: %w",
		incoming.GetName(), incoming.GetVersion(), incoming.GetNamespace(),
		s.uri(key), "a version is immutable, so publish a new version", ErrVersionExists)
}

// Get reads one orkflow version from one namespace. An empty version resolves
// through the current pointer when one is set, and to the most recently
// applied version when none is — never into another namespace.
//
// A pointer naming a version that is not stored is an ERROR here, not a quiet
// fall back to the head. The one path that deletes a version — Put rolling
// back a version it could not commit — can race a pointer moved onto that
// version (an identical apply, a promotion of the listed version); it and the
// pointer's writer each read the other back and put the version back, or move
// the pointer back, so the state lasts only until one of them has, and outside
// that it means the bucket was edited underneath the engine. Either way,
// silently running a different version than the pointer names is the one
// answer nobody could debug.
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

// orkflowScan is what one prefix scan found: every definition under it, the two
// pointers that live beside them, each keyed by orkflow name, and the keys of the
// derived claims, which only Delete needs and nothing reads.
type orkflowScan struct {
	versions  []*orkstackv1.Orkflow
	current   map[string]string
	head      map[string]string
	claimKeys []string
}

// headOf is the version one orkflow's head names, and derivedHeadOf is the
// document it is.
//
// The pointer decides, because moving the pointer is what an apply commits.
// The documents' own stamps answer only when the pointer cannot: a catalog
// whose pointer is missing (Delete removes the pointers before the
// definitions, so a Delete that fails part way leaves the documents behind,
// and a writer killed on an orkflow's first apply does too), or — and this is
// why the check is on membership rather than on emptiness — a pointer naming
// a version this scan did not see. Put writes the document BEFORE it moves
// the pointer, so that state should be unreachable; falling back rather than
// reporting a head nothing holds is what keeps a listing answerable if it
// ever is, exactly as pinnedOrkflow does for the current pointer.
//
// The derived answer is not a listing's private convenience: S3OrkflowStore.Put
// checks an apply's expected head against the SAME rule (effectiveHead), or a
// pointer-less catalog would be readable and not editable — an editor shown
// 1.0.0 as the head, and an apply naming 1.0.0 refused against a head of "".
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
			if _, _, _, ok := ParseOrkflowDerivedKey(key); ok {
				scan.claimKeys = append(scan.claimKeys, key)
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
	wf, _, _, err := s.readRaw(ctx, key)
	return wf, err
}

// readRaw is read with the exact bytes stored and their etag: a rollback puts
// back those bytes, never a re-encoding, which a decode that discards unknown
// fields (a newer release's) would change.
func (s *S3OrkflowStore) readRaw(ctx context.Context, key string) (*orkstackv1.Orkflow, []byte, string, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, "", fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), ErrNotFound)
		}
		return nil, nil, "", fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, nil, "", fmt.Errorf("artifacts: get orkflow %q: read body: %w", s.uri(key), err)
	}
	wf, err := unmarshalOrkflow(document)
	if err != nil {
		return nil, nil, "", fmt.Errorf("artifacts: get orkflow %q: %w", s.uri(key), err)
	}
	return wf, document, aws.ToString(object.ETag), nil
}

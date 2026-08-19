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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

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
	// no-op that writes nothing and returns what is stored (created=false),
	// and a DIFFERENT document under an existing version is refused with an
	// error matching ErrVersionExists. Other versions, and every other
	// namespace, are never touched.
	Put(ctx context.Context, wf *orkstackv1.Orkflow) (*orkstackv1.Orkflow, bool, error)
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

// resolveByName reduces a flat set of versions to the one version each name
// currently resolves to, ordered by name.
//
// It is the pointer's rule applied to a whole namespace: a name somebody
// promoted resolves to the promoted version, and a name nobody promoted
// resolves to its most recently applied one. Listing had to learn this too —
// a list that showed the head while `run submit` executed the promoted
// version would report a version the deployment does not run.
func resolveByName(all []*orkstackv1.Orkflow, current map[string]string) []*orkstackv1.Orkflow {
	byName := map[string][]*orkstackv1.Orkflow{}
	names := make([]string, 0, len(all))
	for _, wf := range all {
		if _, seen := byName[wf.GetName()]; !seen {
			names = append(names, wf.GetName())
		}
		byName[wf.GetName()] = append(byName[wf.GetName()], wf)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Orkflow, 0, len(names))
	for _, name := range names {
		if pinned := pinnedOrkflow(byName[name], current[name]); pinned != nil {
			out = append(out, pinned)
			continue
		}
		if latest := latestOrkflow(byName[name]); latest != nil {
			out = append(out, latest)
		}
	}
	return out
}

// pinnedOrkflow returns the named version out of one orkflow's versions, or
// nil when the pointer is unset or names a version this scan did not see.
//
// A pointer that names nothing present cannot happen through this store —
// SetCurrentVersion validates the target and nothing deletes a version — so
// falling back to the head here is not a policy about dangling pointers; it is
// what keeps a listing answerable when a version arrives between the LIST and
// the pointer's write. Get is stricter, because Get is the read a run is
// started from.
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

// Put writes one orkflow version's document, and returns the stored document
// with whether this call created it.
//
// The version is part of the key, so publishing a new version adds an object
// and leaves every earlier version byte for byte as it was: a run pinned to a
// version always reads the definition it pinned.
//
// A version already stored is never rewritten. Re-applying the identical
// document writes NOTHING and hands back what is there — the object keeps its
// original created_at, which is what makes "compare the stored bytes before
// and after" a real check rather than a comparison of two fresh timestamps.
// A different document under the same version is ErrVersionExists: the fix is
// a new version, because rewriting this one would change what a pinned run
// reads.
//
// The create is conditional (IfAbsent), so two replicas applying the same new
// version at the same instant produce one write and one refusal, and the
// refusal is answered on the terms above rather than by overwriting the
// winner.
func (s *S3OrkflowStore) Put(
	ctx context.Context,
	wf *orkstackv1.Orkflow,
) (*orkstackv1.Orkflow, bool, error) {
	key, err := OrkflowKey(wf.GetNamespace(), wf.GetName(), wf.GetVersion())
	if err != nil {
		return nil, false, err
	}
	incoming := namespacedOrkflow(wf)
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
// (SetCurrentVersion validates its target, and no code path deletes a
// version), so reaching it means the bucket was edited underneath the engine —
// and silently running a different version than the pointer names is the one
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
		versions, err := s.versions(ctx, namespace, name)
		if err != nil {
			return nil, err
		}
		latest := latestOrkflow(versions)
		if latest == nil {
			return nil, fmt.Errorf("artifacts: get orkflow %q in namespace %q: %w",
				name, ResolveNamespace(namespace), ErrNotFound)
		}
		return latest, nil
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
	all, current, err := s.readPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return resolveByName(all, current), nil
}

// Versions returns one orkflow's whole version catalog in one prefix scan.
//
// The pointer object lives under the same prefix as the versions, so Current
// comes back from the listing this call already makes; Head is computed from
// the documents rather than from object timestamps, exactly as Get's fallback
// is, so the two can never disagree about which version is newest.
func (s *S3OrkflowStore) Versions(
	ctx context.Context,
	namespace, name string,
) (OrkflowVersions, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return OrkflowVersions{}, err
	}
	all, current, err := s.readPrefix(ctx, prefix)
	if err != nil {
		return OrkflowVersions{}, err
	}
	sortOrkflowsNewestFirst(all)
	out := OrkflowVersions{All: all, Current: current[name]}
	if head := latestOrkflow(all); head != nil {
		out.Head = head.GetVersion()
	}
	return out, nil
}

// sortOrkflowsNewestFirst orders a version catalog by application time, newest
// first, with the same tie-break latestOrkflow uses so the first element and
// "the latest" are always the same document.
func sortOrkflowsNewestFirst(versions []*orkstackv1.Orkflow) {
	sort.SliceStable(versions, func(i, j int) bool {
		return moreRecentOrkflow(versions[i], versions[j])
	})
}

// versions reads every stored version of one orkflow in one namespace.
func (s *S3OrkflowStore) versions(ctx context.Context, namespace, name string) ([]*orkstackv1.Orkflow, error) {
	prefix, err := orkflowNamePrefix(namespace, name)
	if err != nil {
		return nil, err
	}
	all, _, err := s.readPrefix(ctx, prefix)
	return all, err
}

// readPrefix lists a prefix once and reads what is under it: every definition,
// and every current pointer beside them (name -> version).
//
// One scan answers both because they share the prefix. Orkflow catalogs are
// small (one document per version), and reading the documents is what makes
// "most recently applied" exact rather than a guess from S3 timestamps.
func (s *S3OrkflowStore) readPrefix(
	ctx context.Context,
	prefix string,
) ([]*orkstackv1.Orkflow, map[string]string, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	var out []*orkstackv1.Orkflow
	current := map[string]string{}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("artifacts: list orkflows %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if namespace, name, ok := ParseOrkflowCurrentKey(key); ok {
				pointer, _, err := s.readCurrent(ctx, namespace, name)
				if err != nil {
					return nil, nil, err
				}
				if pointer.Version != "" {
					current[name] = pointer.Version
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
				return nil, nil, err
			}
			out = append(out, wf)
		}
	}
	return out, current, nil
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

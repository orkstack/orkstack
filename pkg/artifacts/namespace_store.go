package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// NamespaceStore is the registry of namespaces: one JSON document per name,
// beside the orkflows they scope.
//
//	s3://{bucket}/metadata/namespaces/{name}.json
//
// It holds no cache, on the same terms as OrkflowStore: any replica answers
// for any namespace, including one it never created.
type NamespaceStore interface {
	// Put writes one namespace document, creating or replacing it.
	Put(ctx context.Context, ns *orkstackv1.Namespace) error
	// Get reads one namespace. DefaultNamespace always resolves, whether or
	// not a document was ever written for it. Any other missing name returns
	// an error matching ErrNotFound.
	Get(ctx context.Context, name string) (*orkstackv1.Namespace, error)
	// List returns every registered namespace ordered by name, with the
	// default namespace included whether or not it was ever written.
	List(ctx context.Context) ([]*orkstackv1.Namespace, error)
	// Delete removes a namespace that holds no orkflows. A namespace that
	// still holds any is refused, and the error says how many.
	Delete(ctx context.Context, name string) error
}

const (
	// NamespacePrefix roots the namespace registry in the bucket. It sits
	// beside OrkflowPrefix rather than inside it, so listing namespaces never
	// scans definitions and a definition can never be mistaken for a registry
	// document.
	NamespacePrefix = "metadata/namespaces"

	// namespaceSuffix ends every registry object.
	namespaceSuffix = ".json"

	// namespaceContentType is what a registry object is served as.
	namespaceContentType = "application/json"
)

// ErrInvalidNamespace reports a namespace name this store cannot address.
var ErrInvalidNamespace = errors.New("invalid namespace")

// ErrNamespaceNotEmpty reports a delete refused because the namespace still
// holds orkflows.
var ErrNamespaceNotEmpty = errors.New("namespace is not empty")

// NamespaceKey renders the object key holding one namespace document:
// metadata/namespaces/{name}.json.
//
// The name is validated rather than sanitized, exactly as an orkflow's
// coordinates are: nothing can escape the prefix or address another
// namespace's document.
func NamespaceKey(name string) (string, error) {
	name = ResolveNamespace(name)
	if err := validNamespaceName(name); err != nil {
		return "", err
	}
	return NamespacePrefix + "/" + name + namespaceSuffix, nil
}

// ParseNamespaceKey reverses NamespaceKey. A key this package did not generate
// reports ok=false, so a scan skips foreign objects instead of decoding them.
func ParseNamespaceKey(key string) (name string, ok bool) {
	rest, found := strings.CutPrefix(key, NamespacePrefix+"/")
	if !found {
		return "", false
	}
	name, found = strings.CutSuffix(rest, namespaceSuffix)
	if !found || validNamespaceName(name) != nil {
		return "", false
	}
	return name, true
}

func validNamespaceName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("artifacts: namespace name is empty: %w", ErrInvalidNamespace)
	case len(name) > maxOrkflowComponent:
		return fmt.Errorf("artifacts: namespace %q exceeds %d bytes: %w",
			name, maxOrkflowComponent, ErrInvalidNamespace)
	case !cleanComponentPattern.MatchString(name):
		return fmt.Errorf("artifacts: namespace %q must match %s: %w",
			name, cleanComponentPattern, ErrInvalidNamespace)
	}
	return nil
}

// DefaultNamespaceDocument is what DefaultNamespace resolves to when nobody
// ever wrote one. It is synthesized, not stored: a fresh deployment has a
// working namespace before anyone creates anything, and no bootstrap step can
// be forgotten or half-run.
func DefaultNamespaceDocument() *orkstackv1.Namespace {
	return &orkstackv1.Namespace{
		Name:        DefaultNamespace,
		Description: "the namespace every orkflow that names none belongs to",
	}
}

// marshalNamespace renders a registry document with the same normalization the
// orkflow store uses, so byte comparison stays meaningful.
func marshalNamespace(ns *orkstackv1.Namespace) ([]byte, error) {
	if ns == nil {
		return nil, fmt.Errorf("artifacts: encode namespace: %w: nil namespace", ErrInvalidNamespace)
	}
	raw, err := protojson.Marshal(ns)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode namespace %q: %w", ns.GetName(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode namespace %q: %w", ns.GetName(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// unmarshalNamespace decodes a registry document. Unknown fields are
// discarded, so an engine can read what a newer engine wrote.
func unmarshalNamespace(document []byte) (*orkstackv1.Namespace, error) {
	ns := &orkstackv1.Namespace{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, ns); err != nil {
		return nil, fmt.Errorf("decode namespace document: %w", err)
	}
	return ns, nil
}

// S3NamespaceStore keeps the namespace registry in an S3-compatible bucket.
type S3NamespaceStore struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var _ NamespaceStore = (*S3NamespaceStore)(nil)

// NewS3NamespaceStore builds the namespace registry from the same ORK_S3_*
// config the artifact and orkflow stores use.
func NewS3NamespaceStore(ctx context.Context, cfg Config) (*S3NamespaceStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3NamespaceStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the registry.
func (s *S3NamespaceStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns.
func (s *S3NamespaceStore) Location() string { return s.uri(NamespacePrefix + "/") }

func (s *S3NamespaceStore) uri(key string) string {
	return "s3://" + s.bucket + "/" + key
}

// Put writes one namespace document.
func (s *S3NamespaceStore) Put(ctx context.Context, ns *orkstackv1.Namespace) error {
	if ns == nil {
		return fmt.Errorf("artifacts: encode namespace: %w: nil namespace", ErrInvalidNamespace)
	}
	name := ResolveNamespace(ns.GetName())
	key, err := NamespaceKey(name)
	if err != nil {
		return err
	}
	if err := validNamespacePools(ns); err != nil {
		return err
	}
	stored := proto.Clone(ns).(*orkstackv1.Namespace)
	stored.Name = name
	document, err := marshalNamespace(stored)
	if err != nil {
		return err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(namespaceContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: put namespace %q: %w", s.uri(key), err)
	}
	return nil
}

// validNamespacePools enforces the one invariant the document carries on its
// own: an allow-list that excludes the namespace's own default would refuse
// every task that names no pool.
func validNamespacePools(ns *orkstackv1.Namespace) error {
	defaultPool := ns.GetDefaultPool()
	if defaultPool == "" {
		defaultPool = "main"
	}
	if !cleanComponentPattern.MatchString(defaultPool) {
		return fmt.Errorf("artifacts: namespace %q default pool %q is invalid: %w",
			ns.GetName(), defaultPool, ErrInvalidNamespace)
	}
	allowed := ns.GetAllowedPools()
	if len(allowed) == 0 {
		return nil
	}
	foundDefault := false
	for _, pool := range allowed {
		if !cleanComponentPattern.MatchString(pool) {
			return fmt.Errorf("artifacts: namespace %q allowed pool %q is invalid: %w",
				ns.GetName(), pool, ErrInvalidNamespace)
		}
		if pool == defaultPool {
			foundDefault = true
		}
	}
	if foundDefault {
		return nil
	}
	return fmt.Errorf("artifacts: namespace %q allows pools %s but defaults to %q: %w",
		ns.GetName(), strings.Join(allowed, ", "), defaultPool, ErrInvalidNamespace)
}

// Get reads one namespace. The default namespace always resolves.
func (s *S3NamespaceStore) Get(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	name = ResolveNamespace(name)
	key, err := NamespaceKey(name)
	if err != nil {
		return nil, err
	}
	ns, err := s.read(ctx, key)
	if errors.Is(err, ErrNotFound) && name == DefaultNamespace {
		return DefaultNamespaceDocument(), nil
	}
	return ns, err
}

// List returns every registered namespace, ordered by name. The default
// namespace is always present, synthesized when no document exists for it, so
// a caller listing namespaces sees the one every unnamespaced orkflow is in.
func (s *S3NamespaceStore) List(ctx context.Context) ([]*orkstackv1.Namespace, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(NamespacePrefix + "/"),
	})
	byName := map[string]*orkstackv1.Namespace{DefaultNamespace: DefaultNamespaceDocument()}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list namespaces %q: %w",
				s.uri(NamespacePrefix+"/"), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			name, ok := ParseNamespaceKey(key)
			if !ok {
				continue
			}
			ns, err := s.read(ctx, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between the listing and the read
				}
				return nil, err
			}
			byName[name] = ns
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out, nil
}

// Delete removes a namespace document.
//
// It refuses while the namespace still holds orkflows, and says how many:
// deleting the registry entry would not delete the definitions, so the objects
// would survive under a namespace nothing lists — reachable by key, invisible
// to every reader. The default namespace is refused unconditionally, because
// every orkflow that names none is in it.
func (s *S3NamespaceStore) Delete(ctx context.Context, name string) error {
	name = ResolveNamespace(name)
	key, err := NamespaceKey(name)
	if err != nil {
		return err
	}
	if name == DefaultNamespace {
		return fmt.Errorf(
			"artifacts: namespace %q cannot be deleted: it holds every orkflow that names none: %w",
			name, ErrNamespaceNotEmpty)
	}
	held, err := s.countOrkflows(ctx, name)
	if err != nil {
		return err
	}
	if held > 0 {
		return fmt.Errorf("artifacts: namespace %q still holds %d orkflow(s); delete them first: %w",
			name, held, ErrNamespaceNotEmpty)
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("artifacts: delete namespace %q: %w", s.uri(key), err)
	}
	return nil
}

// countOrkflows counts the distinct orkflow names stored in a namespace. It is
// a keys-only scan: the definitions are never read, because the question is
// how many there are, not what they say.
func (s *S3NamespaceStore) countOrkflows(ctx context.Context, namespace string) (int, error) {
	prefix, err := OrkflowNamespacePrefix(namespace)
	if err != nil {
		return 0, err
	}
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	names := map[string]struct{}{}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("artifacts: list orkflows %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			_, name, _, ok := parseOrkflowKey(aws.ToString(object.Key))
			if !ok {
				continue
			}
			names[name] = struct{}{}
		}
	}
	return len(names), nil
}

func (s *S3NamespaceStore) read(ctx context.Context, key string) (*orkstackv1.Namespace, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get namespace %q: read body: %w", s.uri(key), err)
	}
	ns, err := unmarshalNamespace(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), err)
	}
	if name, ok := ParseNamespaceKey(key); ok {
		ns.Name = name
	}
	return ns, nil
}

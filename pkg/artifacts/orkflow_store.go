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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// OrkflowStore is the system of record for orkflow definitions. Runs live in
// Temporal; orkflows live here. Every read goes to the backing store, so a
// restarted engine sees exactly what the bucket holds and nothing is served
// from process memory.
//
// internal/engine declares the same method set as the interface the API
// depends on; Go's structural typing lets this package satisfy it without
// importing internal/.
type OrkflowStore interface {
	// Put writes one orkflow version. Writing a version replaces only that
	// version's object; other versions are never touched.
	Put(ctx context.Context, wf *orkstackv1.Orkflow) error
	// Get returns one orkflow version. An empty version means the most
	// recently applied one. A missing orkflow returns an error matching
	// ErrNotFound.
	Get(ctx context.Context, name, version string) (*orkstackv1.Orkflow, error)
	// List returns the most recently applied version of every stored orkflow,
	// ordered by name.
	List(ctx context.Context) ([]*orkstackv1.Orkflow, error)
}

const (
	// OrkflowPrefix roots orkflow definitions in the bucket:
	//
	//	s3://{bucket}/metadata/orkflows/{name}/{version}.json
	//
	// It is disjoint from the run-artifact hierarchy (`orkflows/`) and from
	// artifact name manifests (`manifests/`), so a lifecycle rule that expires
	// one orkflow's artifacts never deletes the definitions.
	OrkflowPrefix = "metadata/orkflows"

	// orkflowSuffix ends every definition object. The document is JSON.
	orkflowSuffix = ".json"

	// orkflowContentType is what a definition object is served as.
	orkflowContentType = "application/json"

	// maxOrkflowComponent bounds a name or version so a key stays far below
	// S3's 1024-byte limit and is a legal path component everywhere.
	maxOrkflowComponent = 200
)

// ErrInvalidOrkflow reports a name or version this store cannot address.
var ErrInvalidOrkflow = errors.New("invalid orkflow coordinates")

// OrkflowKey renders the object key holding one orkflow version:
// metadata/orkflows/{name}/{version}.json.
//
// Both coordinates are validated rather than sanitized: a name or version that
// is not already a safe single path component (a DSL slug and a version string
// always are) is rejected, so no input can escape the prefix or collide with
// another orkflow's key.
func OrkflowKey(name, version string) (string, error) {
	if err := validOrkflowComponent("name", name); err != nil {
		return "", err
	}
	if err := validOrkflowComponent("version", version); err != nil {
		return "", err
	}
	return OrkflowPrefix + "/" + name + "/" + version + orkflowSuffix, nil
}

// orkflowNamePrefix is the key prefix holding every version of one orkflow.
func orkflowNamePrefix(name string) (string, error) {
	if err := validOrkflowComponent("name", name); err != nil {
		return "", err
	}
	return OrkflowPrefix + "/" + name + "/", nil
}

// parseOrkflowKey reverses OrkflowKey. Keys that are not definitions (a future
// document beside them, a stray object) report false and are skipped by List.
func parseOrkflowKey(key string) (name, version string, ok bool) {
	rest, found := strings.CutPrefix(key, OrkflowPrefix+"/")
	if !found {
		return "", "", false
	}
	name, file, found := strings.Cut(rest, "/")
	if !found {
		return "", "", false
	}
	version, found = strings.CutSuffix(file, orkflowSuffix)
	if !found {
		return "", "", false
	}
	if validOrkflowComponent("name", name) != nil || validOrkflowComponent("version", version) != nil {
		return "", "", false
	}
	return name, version, true
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

// latestByName reduces a flat set of versions to the latest of each name,
// ordered by name.
func latestByName(all []*orkstackv1.Orkflow) []*orkstackv1.Orkflow {
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
		if latest := latestOrkflow(byName[name]); latest != nil {
			out = append(out, latest)
		}
	}
	return out
}

// S3OrkflowStore keeps orkflow definitions in an S3-compatible bucket, one
// immutable JSON object per version:
//
//	s3://{bucket}/metadata/orkflows/{name}/{version}.json
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
func (s *S3OrkflowStore) URI(name, version string) (string, error) {
	key, err := OrkflowKey(name, version)
	if err != nil {
		return "", err
	}
	return s.uri(key), nil
}

func (s *S3OrkflowStore) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// Put writes one orkflow version's document.
//
// The version is part of the key, so publishing a new version adds an object
// and leaves every earlier version byte for byte as it was: a run pinned to a
// version always reads the definition it pinned. Re-applying a version
// replaces that one object and nothing else, which keeps `orkctl apply`
// idempotent.
func (s *S3OrkflowStore) Put(ctx context.Context, wf *orkstackv1.Orkflow) error {
	key, err := OrkflowKey(wf.GetName(), wf.GetVersion())
	if err != nil {
		return err
	}
	document, err := marshalOrkflow(wf)
	if err != nil {
		return err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(orkflowContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: put orkflow %q: %w", s.uri(key), err)
	}
	return nil
}

// Get reads one orkflow version. An empty version resolves to the most
// recently applied one.
func (s *S3OrkflowStore) Get(ctx context.Context, name, version string) (*orkstackv1.Orkflow, error) {
	if version == "" {
		versions, err := s.versions(ctx, name)
		if err != nil {
			return nil, err
		}
		latest := latestOrkflow(versions)
		if latest == nil {
			return nil, fmt.Errorf("artifacts: get orkflow %q: %w", name, ErrNotFound)
		}
		return latest, nil
	}
	key, err := OrkflowKey(name, version)
	if err != nil {
		return nil, err
	}
	return s.read(ctx, key)
}

// List returns the most recently applied version of every orkflow in the
// bucket, ordered by name.
func (s *S3OrkflowStore) List(ctx context.Context) ([]*orkstackv1.Orkflow, error) {
	all, err := s.readPrefix(ctx, OrkflowPrefix+"/")
	if err != nil {
		return nil, err
	}
	return latestByName(all), nil
}

// versions reads every stored version of one orkflow.
func (s *S3OrkflowStore) versions(ctx context.Context, name string) ([]*orkstackv1.Orkflow, error) {
	prefix, err := orkflowNamePrefix(name)
	if err != nil {
		return nil, err
	}
	return s.readPrefix(ctx, prefix)
}

// readPrefix lists a prefix and reads every definition under it. Orkflow
// catalogs are small (one document per version), and reading them is what
// makes "most recently applied" exact rather than a guess from S3 timestamps.
func (s *S3OrkflowStore) readPrefix(ctx context.Context, prefix string) ([]*orkstackv1.Orkflow, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	var out []*orkstackv1.Orkflow
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list orkflows %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if _, _, ok := parseOrkflowKey(key); !ok {
				continue
			}
			wf, err := s.read(ctx, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between the listing and the read
				}
				return nil, err
			}
			out = append(out, wf)
		}
	}
	return out, nil
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

package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// ImagePolicyKey holds the deployment's one image policy document.
const ImagePolicyKey = SettingsPrefix + "/image-policy.json"

// ImagePolicyStore is the deployment's image supply-chain floor. It holds
// no cache, on the same terms as RetentionPolicyStore: any replica answers,
// including one that never wrote the document.
type ImagePolicyStore interface {
	// Get reads the stored policy. A deployment that never wrote one returns
	// an error matching ErrNotFound; the CALLER treats that as the empty
	// policy (api.Service.imagePolicy), because unlike retention the empty
	// policy has a safe meaning: permit everything, as before this store
	// existed. Nothing is materialized at boot.
	Get(ctx context.Context) (*orkstackv1.ImagePolicy, error)
	// Put writes the policy under cond and returns what it stored,
	// updated_at stamped by the store. It refuses (ErrInvalidNamespace, the
	// package's one invalid-document sentinel) an entry
	// task.ValidRegistryPrefixes refuses or a key task.ValidSigningKeys
	// refuses.
	Put(ctx context.Context, policy *orkstackv1.ImagePolicy, cond PutConditions) (*orkstackv1.ImagePolicy, error)
}

// ValidImagePolicy is the document rule the store holds every write to: the
// registry prefixes and the signing keys. Exported so the in-memory doubles
// in internal/api and cmd/orkctl tests refuse exactly what the store
// refuses.
func ValidImagePolicy(policy *orkstackv1.ImagePolicy) error {
	if policy == nil {
		return fmt.Errorf("artifacts: image policy: %w: nil policy", ErrInvalidNamespace)
	}
	if err := task.ValidRegistryPrefixes(policy.GetAllowedRegistries()); err != nil {
		return fmt.Errorf("artifacts: image policy %w: %w", err, ErrInvalidNamespace)
	}
	if err := task.ValidSigningKeys(policy.GetSigningKeys()); err != nil {
		return fmt.Errorf("artifacts: image policy %w: %w", err, ErrInvalidNamespace)
	}
	return nil
}

// marshalImagePolicy renders the document with the normalization every
// other registry document in this package uses. allowed_registries keep the
// order given; signing_keys is a map, which protojson emits sorted.
func marshalImagePolicy(policy *orkstackv1.ImagePolicy) ([]byte, error) {
	if policy == nil {
		return nil, fmt.Errorf("artifacts: encode image policy: %w: nil policy", ErrInvalidNamespace)
	}
	raw, err := protojson.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode image policy: %w", err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode image policy: %w", err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// unmarshalImagePolicy decodes the document. Unknown fields are discarded,
// so an engine can read what a newer engine wrote.
func unmarshalImagePolicy(document []byte) (*orkstackv1.ImagePolicy, error) {
	policy := &orkstackv1.ImagePolicy{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, policy); err != nil {
		return nil, fmt.Errorf("decode image policy document: %w", err)
	}
	return policy, nil
}

// S3ImagePolicyStore keeps the image policy document in an S3-compatible
// bucket.
type S3ImagePolicyStore struct {
	client *s3.Client
	bucket string
	// now stamps updated_at; a field only so a test can hold the clock still.
	now func() time.Time
}

// compile-time contract check.
var _ ImagePolicyStore = (*S3ImagePolicyStore)(nil)

// NewS3ImagePolicyStore builds the image policy store from the same ORK_S3_*
// config the artifact, namespace and retention stores use.
func NewS3ImagePolicyStore(ctx context.Context, cfg Config) (*S3ImagePolicyStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3ImagePolicyStore{
		client: client,
		bucket: bucket,
		now:    func() time.Time { return time.Now().UTC() },
	}, nil
}

// Location is the s3:// URI of the document this store owns.
func (s *S3ImagePolicyStore) Location() string { return s.uri(ImagePolicyKey) }

func (s *S3ImagePolicyStore) uri(key string) string {
	return "s3://" + s.bucket + "/" + key
}

func (s *S3ImagePolicyStore) clock() time.Time {
	if s.now == nil {
		return time.Now().UTC()
	}
	return s.now()
}

// Get reads the stored policy.
func (s *S3ImagePolicyStore) Get(ctx context.Context) (*orkstackv1.ImagePolicy, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ImagePolicyKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get image policy %q: %w", s.uri(ImagePolicyKey), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get image policy %q: %w", s.uri(ImagePolicyKey), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get image policy %q: read body: %w", s.uri(ImagePolicyKey), err)
	}
	policy, err := unmarshalImagePolicy(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get image policy %q: %w", s.uri(ImagePolicyKey), err)
	}
	return policy, nil
}

// Put writes the policy document under cond, whole.
func (s *S3ImagePolicyStore) Put(
	ctx context.Context,
	policy *orkstackv1.ImagePolicy,
	cond PutConditions,
) (*orkstackv1.ImagePolicy, error) {
	if err := ValidImagePolicy(policy); err != nil {
		return nil, err
	}
	stored := &orkstackv1.ImagePolicy{
		RequireDigest:     policy.GetRequireDigest(),
		AllowedRegistries: append([]string(nil), policy.GetAllowedRegistries()...),
		UpdatedAt:         timestamppb.New(s.clock()),
	}
	if len(policy.GetSigningKeys()) > 0 {
		stored.SigningKeys = make(map[string]string, len(policy.GetSigningKeys()))
		for name, keyPEM := range policy.GetSigningKeys() {
			stored.SigningKeys[name] = keyPEM
		}
	}
	document, err := marshalImagePolicy(stored)
	if err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(ImagePolicyKey),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(retentionPolicyContentType),
	}
	if cond.IfAbsent {
		input.IfNoneMatch = aws.String("*")
	}
	if cond.IfMatch != "" {
		input.IfMatch = aws.String(cond.IfMatch)
	}
	if _, err := s.client.PutObject(ctx, input); err != nil {
		if cond.conditional() && isPreconditionFailed(err) {
			return nil, fmt.Errorf("artifacts: put image policy %q: %w", s.uri(ImagePolicyKey), ErrPreconditionFailed)
		}
		return nil, fmt.Errorf("artifacts: put image policy %q: %w", s.uri(ImagePolicyKey), err)
	}
	return stored, nil
}

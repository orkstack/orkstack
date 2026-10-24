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
)

const (
	// SettingsPrefix roots deployment-wide settings documents in the bucket,
	// beside the namespace and orkflow registries.
	SettingsPrefix = "metadata/settings"

	// RetentionPolicyKey holds the deployment's one retention policy document.
	RetentionPolicyKey = SettingsPrefix + "/retention-policy.json"

	retentionPolicyContentType = "application/json"
)

// RetentionPolicyStore is the deployment's one retention setting: the most
// any namespace may keep a finished run, and what a namespace that sets none
// inherits.
//
// It holds no cache, on the same terms as NamespaceStore: any replica
// answers, including one that never wrote the document itself.
type RetentionPolicyStore interface {
	// Get reads the stored policy. A deployment that never wrote one returns
	// an error matching ErrNotFound — nothing is synthesized, unlike the
	// default namespace, because a policy this store never wrote has no safe
	// implicit value for a caller to assume.
	Get(ctx context.Context) (*orkstackv1.RetentionPolicy, error)
	// Put writes the policy under cond and returns what it stored, updated_at
	// stamped by the store. cond follows PutConditions: IfAbsent is how the
	// engine materializes the document once at first boot without two
	// replicas racing to pick different starting values.
	Put(ctx context.Context, policy *orkstackv1.RetentionPolicy, cond PutConditions) (*orkstackv1.RetentionPolicy, error)
}

// marshalRetentionPolicy renders the document with the same normalization
// every other registry document in this package uses, so byte comparison
// stays meaningful.
func marshalRetentionPolicy(policy *orkstackv1.RetentionPolicy) ([]byte, error) {
	if policy == nil {
		return nil, fmt.Errorf("artifacts: encode retention policy: %w: nil policy", ErrInvalidNamespace)
	}
	raw, err := protojson.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode retention policy: %w", err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode retention policy: %w", err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// unmarshalRetentionPolicy decodes the document. Unknown fields are
// discarded, so an engine can read what a newer engine wrote.
func unmarshalRetentionPolicy(document []byte) (*orkstackv1.RetentionPolicy, error) {
	policy := &orkstackv1.RetentionPolicy{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, policy); err != nil {
		return nil, fmt.Errorf("decode retention policy document: %w", err)
	}
	return policy, nil
}

// S3RetentionPolicyStore keeps the retention policy document in an
// S3-compatible bucket.
type S3RetentionPolicyStore struct {
	client *s3.Client
	bucket string
	// now stamps updated_at. It is a field only so a test can hold the clock
	// still; every constructor sets it to the wall clock.
	now func() time.Time
}

// compile-time contract check.
var _ RetentionPolicyStore = (*S3RetentionPolicyStore)(nil)

// NewS3RetentionPolicyStore builds the retention policy store from the same
// ORK_S3_* config the artifact and namespace stores use.
func NewS3RetentionPolicyStore(ctx context.Context, cfg Config) (*S3RetentionPolicyStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3RetentionPolicyStore{
		client: client,
		bucket: bucket,
		now:    func() time.Time { return time.Now().UTC() },
	}, nil
}

// Bucket returns the bucket holding the document.
func (s *S3RetentionPolicyStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the document this store owns.
func (s *S3RetentionPolicyStore) Location() string { return s.uri(RetentionPolicyKey) }

func (s *S3RetentionPolicyStore) uri(key string) string {
	return "s3://" + s.bucket + "/" + key
}

func (s *S3RetentionPolicyStore) clock() time.Time {
	if s.now == nil {
		return time.Now().UTC()
	}
	return s.now()
}

// Get reads the stored policy.
func (s *S3RetentionPolicyStore) Get(ctx context.Context) (*orkstackv1.RetentionPolicy, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(RetentionPolicyKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get retention policy %q: %w", s.uri(RetentionPolicyKey), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get retention policy %q: %w", s.uri(RetentionPolicyKey), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get retention policy %q: read body: %w", s.uri(RetentionPolicyKey), err)
	}
	policy, err := unmarshalRetentionPolicy(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get retention policy %q: %w", s.uri(RetentionPolicyKey), err)
	}
	return policy, nil
}

// Put writes the policy document under cond.
func (s *S3RetentionPolicyStore) Put(
	ctx context.Context,
	policy *orkstackv1.RetentionPolicy,
	cond PutConditions,
) (*orkstackv1.RetentionPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("artifacts: encode retention policy: %w: nil policy", ErrInvalidNamespace)
	}
	if d := policy.GetMaxRunRetention(); d == nil || d.CheckValid() != nil || d.AsDuration() <= 0 {
		return nil, fmt.Errorf(
			"artifacts: retention policy max_run_retention must be a positive duration: %w", ErrInvalidNamespace)
	}
	stored := &orkstackv1.RetentionPolicy{
		MaxRunRetention: policy.GetMaxRunRetention(),
		UpdatedAt:       timestamppb.New(s.clock()),
	}
	document, err := marshalRetentionPolicy(stored)
	if err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(RetentionPolicyKey),
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
			return nil, fmt.Errorf("artifacts: put retention policy %q: %w",
				s.uri(RetentionPolicyKey), ErrPreconditionFailed)
		}
		return nil, fmt.Errorf("artifacts: put retention policy %q: %w", s.uri(RetentionPolicyKey), err)
	}
	return stored, nil
}

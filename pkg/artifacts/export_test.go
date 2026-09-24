package artifacts

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// NewS3RunArchiveWithOptions builds an S3 run archive whose client carries
// extra options, so an external test can observe exactly which S3 operations
// an archive method issues. It exists in test builds only.
func NewS3RunArchiveWithOptions(ctx context.Context, cfg Config, optFns ...func(*s3.Options)) (*S3RunArchive, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3RunArchive{client: s3.New(client.Options(), optFns...), bucket: bucket}, nil
}

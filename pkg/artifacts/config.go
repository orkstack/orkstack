package artifacts

import (
	"os"
	"strconv"
	"strings"
)

// Backend identifiers understood out of the box.
const (
	BackendLocal = "local"
	BackendS3    = "s3"
)

// Localhost development defaults. They match deploy/docker-compose.yml so a
// developer needs no environment variables at all.
const (
	DefaultBackend    = BackendLocal
	DefaultLocalDir   = "./.data/artifacts"
	DefaultS3Endpoint = "http://localhost:9000"
	DefaultS3Region   = "us-east-1"
	DefaultS3Bucket   = "orkstack-artifacts"
	// Dev-only MinIO root credentials from deploy/docker-compose.yml. Real
	// deployments always supply ORK_S3_ACCESS_KEY / ORK_S3_SECRET_KEY.
	DefaultS3AccessKey = "orkstack"
	DefaultS3SecretKey = "orkstack-secret"
)

// Config selects and configures an artifact backend.
type Config struct {
	// Backend is the registered backend name ("local", "s3", ...). Empty means
	// DefaultBackend.
	Backend string

	// LocalDir is the filesystem root for the local backend.
	LocalDir string

	// S3Endpoint is the S3 base endpoint. Empty uses the AWS default endpoint
	// resolution for the region.
	S3Endpoint     string
	S3Region       string
	S3Bucket       string
	S3AccessKey    string
	S3SecretKey    string
	S3SessionToken string
	// S3PathStyle forces path-style addressing, which MinIO requires.
	S3PathStyle bool
}

// ConfigFromEnv reads ORK_* variables, falling back to localhost defaults.
//
//	ORK_ARTIFACT_BACKEND   local | s3            (default local)
//	ORK_ARTIFACT_DIR       local filesystem root (default ./.data/artifacts)
//	ORK_S3_ENDPOINT        http://localhost:9000
//	ORK_S3_REGION          us-east-1
//	ORK_S3_BUCKET          orkstack-artifacts
//	ORK_S3_ACCESS_KEY      access key id
//	ORK_S3_SECRET_KEY      secret access key
//	ORK_S3_SESSION_TOKEN   optional session token
//	ORK_S3_PATH_STYLE      true | false          (default true)
func ConfigFromEnv() Config {
	return Config{
		Backend:        envOr("ORK_ARTIFACT_BACKEND", DefaultBackend),
		LocalDir:       envOr("ORK_ARTIFACT_DIR", DefaultLocalDir),
		S3Endpoint:     envOr("ORK_S3_ENDPOINT", DefaultS3Endpoint),
		S3Region:       envOr("ORK_S3_REGION", DefaultS3Region),
		S3Bucket:       envOr("ORK_S3_BUCKET", DefaultS3Bucket),
		S3AccessKey:    envOr("ORK_S3_ACCESS_KEY", DefaultS3AccessKey),
		S3SecretKey:    envOr("ORK_S3_SECRET_KEY", DefaultS3SecretKey),
		S3SessionToken: os.Getenv("ORK_S3_SESSION_TOKEN"),
		S3PathStyle:    envBool("ORK_S3_PATH_STYLE", true),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

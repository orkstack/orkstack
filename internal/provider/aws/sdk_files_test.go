package aws

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

// The files are for AWS's own SDKs, so AWS's own reader reads them: the
// config with its credential_process, and the shared credentials file.
func TestBundle_AnAWSSDKReadsEachFile(t *testing.T) {
	for _, k := range []string{"AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	now := time.Now()
	k := keys{id: "ASIAEXAMPLE", secret: "s3cr3t/+key=", token: "tok+/=en", expiration: now.Add(time.Hour).Truncate(time.Second), life: time.Hour, asked: now}
	b, err := bundle("eu-west-1", "deploy", k, now)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, content := range b.Files {
		// The container's path, where this test keeps the files.
		content := strings.ReplaceAll(string(content), "/orkstack/credentials/deploy", dir)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	missing := filepath.Join(dir, "missing")
	for name, tc := range map[string]struct {
		config, credentials string
		expires             bool
	}{
		"AWS_CONFIG_FILE":             {filepath.Join(dir, "config"), missing, true},
		"AWS_SHARED_CREDENTIALS_FILE": {missing, filepath.Join(dir, "credentials"), false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("AWS_CONFIG_FILE", tc.config)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", tc.credentials)
			cfg, err := config.LoadDefaultConfig(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			got, err := cfg.Credentials.Retrieve(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got.AccessKeyID != k.id || got.SecretAccessKey != k.secret || got.SessionToken != k.token {
				t.Errorf("the SDK read %s/%s/%s", got.AccessKeyID, got.SecretAccessKey, got.SessionToken)
			}
			if tc.expires && (!got.CanExpire || !got.Expires.Equal(k.expiration)) {
				t.Errorf("the SDK read the expiry %v (%v), want %v", got.Expires, got.CanExpire, k.expiration)
			}
			if tc.expires && cfg.Region != "eu-west-1" {
				t.Errorf("the SDK read the region %q", cfg.Region)
			}
		})
	}
}

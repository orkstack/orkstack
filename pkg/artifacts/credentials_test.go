package artifacts_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// noChain leaves the SDK's default credential chain nothing to find on this
// machine, whatever the test host's own environment holds: no AWS_* variable,
// no shared file, no container endpoint and no instance metadata service.
func noChain(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	for _, key := range []string{
		"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "ORK_S3_SESSION_TOKEN",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", home)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(home, "no-credentials"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(home, "no-config"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// TestConfigFromEnv_KeysAreNeverDefaulted: an unset key stays empty, so the
// default credential chain runs. The development key is set only where the
// development stack sets it, and a value set is carried as it is.
func TestConfigFromEnv_KeysAreNeverDefaulted(t *testing.T) {
	cases := []struct {
		name               string
		access, secret     string
		wantAccess, wantSK string
	}{
		{name: "both unset stay empty"},
		{name: "both set", access: "a-key", secret: "a-secret", wantAccess: "a-key", wantSK: "a-secret"},
		{name: "access key alone stays alone", access: "a-key", wantAccess: "a-key"},
		{name: "secret key alone stays alone", secret: "a-secret", wantSK: "a-secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_S3_ACCESS_KEY", tc.access)
			t.Setenv("ORK_S3_SECRET_KEY", tc.secret)
			got := artifacts.ConfigFromEnv()
			if got.S3AccessKey != tc.wantAccess || got.S3SecretKey != tc.wantSK {
				t.Errorf("keys = %q, %q; want %q, %q", got.S3AccessKey, got.S3SecretKey, tc.wantAccess, tc.wantSK)
			}
		})
	}
}

// TestConfig_CheckS3Keys: a static key is whole or absent. Half of one is
// refused, naming both settings and printing neither value.
func TestConfig_CheckS3Keys(t *testing.T) {
	const access, secret, token = "VISIBLE-ACCESS", "VISIBLE-SECRET", "VISIBLE-TOKEN"
	cases := []struct {
		name      string
		cfg       artifacts.Config
		wantError []string
	}{
		{name: "neither is the default chain", cfg: artifacts.Config{}},
		{name: "both", cfg: artifacts.Config{S3AccessKey: access, S3SecretKey: secret}},
		{name: "both and a token", cfg: artifacts.Config{S3AccessKey: access, S3SecretKey: secret, S3SessionToken: token}},
		{
			name:      "access key alone",
			cfg:       artifacts.Config{S3AccessKey: access},
			wantError: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY"},
		},
		{
			name:      "secret key alone",
			cfg:       artifacts.Config{S3SecretKey: secret},
			wantError: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY"},
		},
		{
			name:      "a token with no key belongs to nothing",
			cfg:       artifacts.Config{S3SessionToken: token},
			wantError: []string{"ORK_S3_SESSION_TOKEN", "ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY"},
		},
		{
			name:      "a token with half a key",
			cfg:       artifacts.Config{S3AccessKey: access, S3SessionToken: token},
			wantError: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.CheckS3Keys()
			if len(tc.wantError) == 0 {
				if err != nil {
					t.Fatalf("CheckS3Keys() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("CheckS3Keys() = nil, want a refusal")
			}
			for _, want := range tc.wantError {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %s", err, want)
				}
			}
			for _, value := range []string{access, secret, token} {
				if strings.Contains(err.Error(), value) {
					t.Errorf("error %q prints a credential", err)
				}
			}
		})
	}
}

// TestS3Stores_RefuseHalfAKey: no store is ever built on half a key, so a path
// that skips the boot check still cannot half-use one.
func TestS3Stores_RefuseHalfAKey(t *testing.T) {
	noChain(t)
	ctx := context.Background()
	half := artifacts.Config{Backend: artifacts.BackendS3, S3AccessKey: "alone"}
	if _, err := artifacts.NewS3Store(ctx, half); err == nil || !strings.Contains(err.Error(), "ORK_S3_SECRET_KEY") {
		t.Errorf("NewS3Store(half a key) = %v, want a refusal naming ORK_S3_SECRET_KEY", err)
	}
	if _, err := artifacts.NewS3OrkflowStore(ctx, half); err == nil || !strings.Contains(err.Error(), "ORK_S3_SECRET_KEY") {
		t.Errorf("NewS3OrkflowStore(half a key) = %v, want a refusal naming ORK_S3_SECRET_KEY", err)
	}
}

// TestResolveS3Credentials: which credential a boot will sign with, or why it
// has none. The configured key wins over the chain, the chain runs only with
// no key configured, and a failure names what the operator can set. No answer
// and no error ever carries a credential.
func TestResolveS3Credentials(t *testing.T) {
	const access, secret = "VISIBLE-ACCESS", "VISIBLE-SECRET"
	writeShared := func(t *testing.T) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "credentials")
		body := "[default]\naws_access_key_id = " + access + "\naws_secret_access_key = " + secret + "\n"
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", path)
	}
	cases := []struct {
		name       string
		setup      func(t *testing.T)
		cfg        artifacts.Config
		wantSource string
		wantError  []string
	}{
		{
			name:       "the configured key",
			cfg:        artifacts.Config{S3AccessKey: access, S3SecretKey: secret},
			wantSource: "StaticCredentials",
		},
		{
			name: "the configured key wins over the chain",
			setup: func(t *testing.T) {
				t.Setenv("AWS_ACCESS_KEY_ID", "chain-access")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
			},
			cfg:        artifacts.Config{S3AccessKey: access, S3SecretKey: secret},
			wantSource: "StaticCredentials",
		},
		{
			name: "the chain's environment",
			setup: func(t *testing.T) {
				t.Setenv("AWS_ACCESS_KEY_ID", access)
				t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
			},
			wantSource: "EnvConfigCredentials",
		},
		{
			name:       "the chain's shared file",
			setup:      writeShared,
			wantSource: "SharedConfigCredentials",
		},
		{
			name:      "nothing to find",
			wantError: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "default credential chain"},
		},
		{
			name:      "half a key never reaches the chain",
			setup:     writeShared,
			cfg:       artifacts.Config{S3AccessKey: "alone"},
			wantError: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			noChain(t)
			if tc.setup != nil {
				tc.setup(t)
			}
			source, err := artifacts.ResolveS3Credentials(context.Background(), tc.cfg)
			text := source
			if err != nil {
				text += err.Error()
			}
			for _, value := range []string{access, secret} {
				if strings.Contains(text, value) {
					t.Errorf("answer %q prints a credential", text)
				}
			}
			if len(tc.wantError) > 0 {
				if err == nil {
					t.Fatalf("ResolveS3Credentials() = %q, want a refusal", source)
				}
				for _, want := range tc.wantError {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveS3Credentials() error = %v", err)
			}
			if !strings.HasPrefix(source, tc.wantSource) {
				t.Errorf("source = %q, want it to begin %q", source, tc.wantSource)
			}
		})
	}
}

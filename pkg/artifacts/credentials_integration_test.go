//go:build integration

package artifacts_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// roundTrip writes an artifact to the real store through a store built from
// cfg, and reads it back, so the store's key check saw that signature on both.
func roundTrip(t *testing.T, cfg artifacts.Config) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := artifacts.NewS3Store(ctx, cfg)
	if err != nil {
		return err
	}
	const body = "signed by the default credential chain"
	ref, err := store.Put(ctx, "chain.txt", strings.NewReader(body), artifacts.PutOptions{})
	if err != nil {
		return err
	}
	reader, err := store.Get(ctx, ref.URI)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if string(got) != body {
		t.Errorf("read back %q, want %q", got, body)
	}
	return nil
}

// chainConfig is the configuration a deployment using its default credential
// chain has: the store's address, and no key at all.
func chainConfig(t *testing.T) artifacts.Config {
	t.Helper()
	noChain(t)
	cfg := artifacts.ConfigFromEnv()
	if cfg.S3AccessKey != "" || cfg.S3SecretKey != "" {
		t.Fatalf("keys configured despite none being set: access set %v, secret set %v",
			cfg.S3AccessKey != "", cfg.S3SecretKey != "")
	}
	return cfg
}

// TestIntegration_DefaultChainReachesARealStore: with no ORK_S3_* key, the keys
// the SDK's chain finds sign the requests a real store checks. The wrong key
// through the same path is refused, so it is the chain's key that got the first
// one in, and not a store that lets anything in.
func TestIntegration_DefaultChainReachesARealStore(t *testing.T) {
	t.Run("the environment", func(t *testing.T) {
		cfg := chainConfig(t)
		t.Setenv("AWS_ACCESS_KEY_ID", s3bucket.DevAccessKey)
		t.Setenv("AWS_SECRET_ACCESS_KEY", s3bucket.DevSecretKey)
		source, err := artifacts.ResolveS3Credentials(context.Background(), cfg)
		if err != nil {
			t.Fatalf("ResolveS3Credentials: %v", err)
		}
		if !strings.HasPrefix(source, "EnvConfigCredentials") {
			t.Errorf("source = %q, want the environment's", source)
		}
		if err := roundTrip(t, cfg); err != nil {
			t.Fatalf("round trip through the chain: %v", err)
		}
	})

	t.Run("a wrong key in the environment is refused", func(t *testing.T) {
		cfg := chainConfig(t)
		t.Setenv("AWS_ACCESS_KEY_ID", s3bucket.DevAccessKey)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "not-"+s3bucket.DevSecretKey)
		if err := roundTrip(t, cfg); err == nil {
			t.Fatal("the store took a wrong key, so a pass above would prove nothing")
		}
	})

	t.Run("a shared credentials file", func(t *testing.T) {
		cfg := chainConfig(t)
		path := filepath.Join(t.TempDir(), "credentials")
		body := "[default]\naws_access_key_id = " + s3bucket.DevAccessKey +
			"\naws_secret_access_key = " + s3bucket.DevSecretKey + "\n"
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", path)
		if err := roundTrip(t, cfg); err != nil {
			t.Fatalf("round trip through the shared file: %v", err)
		}
	})

	t.Run("a configured key wins over the chain", func(t *testing.T) {
		cfg := chainConfig(t)
		t.Setenv("AWS_ACCESS_KEY_ID", s3bucket.DevAccessKey)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "not-"+s3bucket.DevSecretKey)
		cfg.S3AccessKey, cfg.S3SecretKey = s3bucket.DevAccessKey, s3bucket.DevSecretKey
		if err := roundTrip(t, cfg); err != nil {
			t.Fatalf("round trip with the configured key beside a wrong chain: %v", err)
		}
	})
}

// TestIntegration_DefaultChainAsksTheInstanceMetadataService: the path an
// instance role takes, as far as it can be taken off a cloud instance. The SDK
// finds no key anywhere else, asks a stand-in for the metadata service (token,
// role name, role credentials), and signs real requests with what it was given.
// The stand-in hands out the store's long-lived key, because the development
// store knows no role: a real instance's service, its temporary credentials'
// session token and their refresh are not exercised.
func TestIntegration_DefaultChainAsksTheInstanceMetadataService(t *testing.T) {
	const role = "orkstack-orker"
	var asked atomic.Int64
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			w.Header().Set("x-aws-ec2-metadata-token-ttl-seconds", "21600")
			_, _ = io.WriteString(w, "a-token")
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/":
			_, _ = io.WriteString(w, role)
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/"+role:
			asked.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"Code":            "Success",
				"Type":            "AWS-HMAC",
				"AccessKeyId":     s3bucket.DevAccessKey,
				"SecretAccessKey": s3bucket.DevSecretKey,
				"Expiration":      time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
				"LastUpdated":     time.Now().UTC().Format(time.RFC3339),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer metadata.Close()

	cfg := chainConfig(t)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", metadata.URL)

	source, err := artifacts.ResolveS3Credentials(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ResolveS3Credentials: %v", err)
	}
	if !strings.HasPrefix(source, "EC2RoleProvider") {
		t.Errorf("source = %q, want the instance role's", source)
	}
	if err := roundTrip(t, cfg); err != nil {
		t.Fatalf("round trip with the role's credentials: %v", err)
	}
	if asked.Load() == 0 {
		t.Error("the metadata service's credentials were never asked for")
	}
}

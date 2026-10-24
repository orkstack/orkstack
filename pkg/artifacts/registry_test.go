package artifacts_test

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

type fakeStore struct{ cfg artifacts.Config }

func (fakeStore) Put(context.Context, string, io.Reader, artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, nil
}
func (fakeStore) Get(context.Context, string) (io.ReadCloser, error) { return nil, nil }
func (fakeStore) Stat(context.Context, string) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, nil
}
func (fakeStore) List(context.Context, string) ([]artifacts.ArtifactRef, error) { return nil, nil }
func (fakeStore) Presign(context.Context, string, time.Duration) (string, error) {
	return "", nil
}
func (fakeStore) Delete(context.Context, string) error { return nil }

func TestNew_SelectsBackend(t *testing.T) {
	dir := t.TempDir()

	local, err := artifacts.New(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatalf("New(local): %v", err)
	}
	if _, ok := local.(*artifacts.LocalStore); !ok {
		t.Errorf("New(local) = %T, want *artifacts.LocalStore", local)
	}

	// An empty backend defaults to local.
	def, err := artifacts.New(artifacts.Config{LocalDir: dir})
	if err != nil {
		t.Fatalf("New(default): %v", err)
	}
	if _, ok := def.(*artifacts.LocalStore); !ok {
		t.Errorf("New(default) = %T, want *artifacts.LocalStore", def)
	}

	if _, err := artifacts.New(artifacts.Config{Backend: "gopher"}); !errors.Is(err, artifacts.ErrUnknownBackend) {
		t.Errorf("New(gopher) err = %v, want ErrUnknownBackend", err)
	}
}

func TestRegister_AddsCustomBackend(t *testing.T) {
	name := "test-registry-backend-" + t.TempDir()
	if err := artifacts.Register(name, func(cfg artifacts.Config) (artifacts.Store, error) {
		return fakeStore{cfg: cfg}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	st, err := artifacts.New(artifacts.Config{Backend: name})
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	if _, ok := st.(fakeStore); !ok {
		t.Fatalf("New(%s) = %T, want fakeStore", name, st)
	}

	if err := artifacts.Register(name, func(artifacts.Config) (artifacts.Store, error) { return nil, nil }); err == nil {
		t.Error("duplicate Register returned nil error, want error")
	}
	if !contains(artifacts.Backends(), name) {
		t.Errorf("Backends() = %v, want to contain %q", artifacts.Backends(), name)
	}
	if !contains(artifacts.Backends(), artifacts.BackendLocal) || !contains(artifacts.Backends(), artifacts.BackendS3) {
		t.Errorf("Backends() = %v, want builtin local and s3", artifacts.Backends())
	}
}

func TestConfigFromEnv_DefaultsAndOverrides(t *testing.T) {
	for _, key := range []string{
		"ORK_ARTIFACT_BACKEND", "ORK_ARTIFACT_DIR", "ORK_S3_ENDPOINT", "ORK_S3_REGION",
		"ORK_S3_BUCKET", "ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "ORK_S3_SESSION_TOKEN",
		"ORK_S3_PATH_STYLE",
	} {
		t.Setenv(key, "")
	}

	def := artifacts.ConfigFromEnv()
	if def.Backend != artifacts.BackendLocal {
		t.Errorf("default Backend = %q, want %q", def.Backend, artifacts.BackendLocal)
	}
	if def.LocalDir != artifacts.DefaultLocalDir {
		t.Errorf("default LocalDir = %q, want %q", def.LocalDir, artifacts.DefaultLocalDir)
	}
	if def.S3Endpoint != artifacts.DefaultS3Endpoint || def.S3Region != artifacts.DefaultS3Region || def.S3Bucket != artifacts.DefaultS3Bucket {
		t.Errorf("default s3 config = %+v, want localhost defaults", def)
	}
	if !def.S3PathStyle {
		t.Error("default S3PathStyle = false, want true")
	}

	t.Setenv("ORK_ARTIFACT_BACKEND", "s3")
	t.Setenv("ORK_ARTIFACT_DIR", "/tmp/ork-artifacts")
	t.Setenv("ORK_S3_ENDPOINT", "http://minio:9000")
	t.Setenv("ORK_S3_PATH_STYLE", "false")

	got := artifacts.ConfigFromEnv()
	if got.Backend != artifacts.BackendS3 || got.LocalDir != "/tmp/ork-artifacts" || got.S3Endpoint != "http://minio:9000" {
		t.Errorf("ConfigFromEnv() = %+v, want env overrides applied", got)
	}
	if got.S3PathStyle {
		t.Error("S3PathStyle = true, want false from env")
	}
}

func TestConfigFromEnv_PreservesValuesVerbatim(t *testing.T) {
	keys := []string{
		"ORK_ARTIFACT_BACKEND", "ORK_ARTIFACT_DIR", "ORK_S3_ENDPOINT", "ORK_S3_REGION",
		"ORK_S3_BUCKET", "ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "ORK_S3_SESSION_TOKEN",
		"ORK_S3_PATH_STYLE",
	}

	t.Run("unset uses defaults", func(t *testing.T) {
		for _, key := range keys {
			t.Setenv(key, "temporary")
			if err := os.Unsetenv(key); err != nil {
				t.Fatalf("Unsetenv(%s): %v", key, err)
			}
		}
		got := artifacts.ConfigFromEnv()
		if got.Backend != artifacts.DefaultBackend || got.LocalDir != artifacts.DefaultLocalDir || got.S3Endpoint != artifacts.DefaultS3Endpoint {
			t.Errorf("ConfigFromEnv(unset) = %+v, want defaults", got)
		}
	})

	t.Run("nonempty strings remain exact", func(t *testing.T) {
		values := map[string]string{
			"ORK_ARTIFACT_BACKEND": "  s3  ",
			"ORK_ARTIFACT_DIR":     " /tmp/dir ",
			"ORK_S3_ENDPOINT":      " http://localhost:9000/ ",
			"ORK_S3_REGION":        " us-east-1 ",
			"ORK_S3_BUCKET":        " bucket ",
			"ORK_S3_ACCESS_KEY":    " key ",
			"ORK_S3_SECRET_KEY":    " secret ",
			"ORK_S3_SESSION_TOKEN": " token ",
			"ORK_S3_PATH_STYLE":    " false ",
		}
		for key, value := range values {
			t.Setenv(key, value)
		}

		got := artifacts.ConfigFromEnv()
		if got.Backend != values["ORK_ARTIFACT_BACKEND"] ||
			got.LocalDir != values["ORK_ARTIFACT_DIR"] ||
			got.S3Endpoint != values["ORK_S3_ENDPOINT"] ||
			got.S3Region != values["ORK_S3_REGION"] ||
			got.S3Bucket != values["ORK_S3_BUCKET"] ||
			got.S3AccessKey != values["ORK_S3_ACCESS_KEY"] ||
			got.S3SecretKey != values["ORK_S3_SECRET_KEY"] ||
			got.S3SessionToken != values["ORK_S3_SESSION_TOKEN"] {
			t.Errorf("ConfigFromEnv(spaces) = %+v, want every nonempty string preserved verbatim", got)
		}
		if got.S3PathStyle {
			t.Error("S3PathStyle = true, want trimmed boolean false")
		}
	})

	t.Run("whitespace-only string remains exact", func(t *testing.T) {
		for _, key := range keys {
			t.Setenv(key, "")
		}
		t.Setenv("ORK_ARTIFACT_DIR", "   ")
		if got := artifacts.ConfigFromEnv().LocalDir; got != "   " {
			t.Errorf("LocalDir = %q, want three spaces", got)
		}
	})
}

func TestNewFromEnv_BuildsLocalStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORK_ARTIFACT_BACKEND", "local")
	t.Setenv("ORK_ARTIFACT_DIR", dir)

	st, err := artifacts.NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}
	ref, err := st.Put(context.Background(), "env/x.txt", nonSeekableReader{strings.NewReader("env")}, artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ref.URI == "" || ref.SHA256 == "" {
		t.Errorf("ref = %+v, want populated URI and SHA256", ref)
	}
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

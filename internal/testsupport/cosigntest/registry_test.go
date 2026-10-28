package cosigntest

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const testDigest = "sha256:d4032d4cc323c1731c707931ecada07a53076eac38e3b61ae57533f170f2c6c3"

func TestRegistryImageFrom(t *testing.T) {
	tests := []struct {
		name    string
		compose string
		want    string
		wantErr string
	}{
		{
			name: "the registry service's tag and digest, as pinned",
			compose: "services:\n" +
				"  temporal:\n    image: temporalio/temporal:1.8.2@sha256:" + strings.Repeat("b", 64) + "\n" +
				"  registry:\n    image: registry:3@" + testDigest + "\n",
			want: "registry:3@" + testDigest,
		},
		{
			name:    "no registry service",
			compose: "services:\n  temporal:\n    image: temporalio/temporal:1.8.2@sha256:" + strings.Repeat("b", 64) + "\n",
			wantErr: "no registry service",
		},
		{
			name:    "a service with no image",
			compose: "services:\n  registry:\n    ports: [\"5001:5000\"]\n",
			wantErr: "names no image",
		},
		{
			name:    "a floating tag is not a pin",
			compose: "services:\n  registry:\n    image: registry:3\n",
			wantErr: "not pinned by digest",
		},
		{
			name:    "a bare repository is not a pin",
			compose: "services:\n  registry:\n    image: registry\n",
			wantErr: "not pinned by digest",
		},
		{
			name:    "an interpolated reference is not a pin",
			compose: "services:\n  registry:\n    image: ${REGISTRY_IMAGE}@" + testDigest + "\n",
			wantErr: "not pinned by digest",
		},
		{
			name:    "a short digest is not a pin",
			compose: "services:\n  registry:\n    image: registry:3@sha256:" + strings.Repeat("a", 63) + "\n",
			wantErr: "not pinned by digest",
		},
		{
			name:    "an upper-case digest is not a pin",
			compose: "services:\n  registry:\n    image: registry:3@sha256:" + strings.Repeat("A", 64) + "\n",
			wantErr: "not pinned by digest",
		},
		{
			name:    "malformed yaml",
			compose: "services: [\n",
			wantErr: "parse",
		},
		{
			name:    "empty file",
			compose: "",
			wantErr: "no registry service",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := registryImageFrom([]byte(tt.compose))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("registryImageFrom returned %q, want an error containing %q", got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tt.wantErr)
				}
				if got != "" {
					t.Errorf("an error came with the reference %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("registryImageFrom: %v", err)
			}
			if got != tt.want {
				t.Errorf("registryImageFrom = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestComposePath_WalksUpToTheModuleRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "internal", "orker")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := composePath(nested)
	if err != nil {
		t.Fatalf("composePath: %v", err)
	}
	if want := filepath.Join(root, "deploy", "docker-compose.yml"); got != want {
		t.Errorf("composePath = %q, want %q", got, want)
	}
	// No module above it: an error that names the starting point, not a
	// guess at a relative path.
	if _, err := composePath(filepath.Join(t.TempDir(), "nowhere")); err == nil {
		t.Error("composePath found a module root above a directory that has none")
	}
}

// StartRegistry must run the registry the stack runs, to the byte: the
// reference deploy/docker-compose.yml pins, which proof-fetch pulls (a
// tag@digest pull leaves no local `registry:3`, so the floating tag the
// helper once ran would reach Docker Hub at test time).
func TestRegistryImage_IsTheStacksDigestPin(t *testing.T) {
	got, err := RegistryImage()
	if err != nil {
		t.Fatalf("RegistryImage: %v", err)
	}
	if !regexp.MustCompile(`^registry:3@sha256:[0-9a-f]{64}$`).MatchString(got) {
		t.Fatalf("RegistryImage = %q, want registry:3 pinned by a sha256 digest", got)
	}
	// Read the pin a second way, from the file itself, so a defect in the
	// function under test cannot agree with itself.
	path, err := composePath(".")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	services, _ := doc["services"].(map[string]any)
	registry, _ := services["registry"].(map[string]any)
	pinned, _ := registry["image"].(string)
	if pinned == "" {
		t.Fatalf("%s has no registry image", path)
	}
	if got != pinned {
		t.Errorf("StartRegistry would run %q, but %s pins %q", got, path, pinned)
	}
}

// The container is started from the pin and never pulls: a pull would reach
// Docker Hub, which the setup's bounded, retried fetch exists to avoid.
func TestRegistryRunArgs_RunsThePinWithoutPulling(t *testing.T) {
	const image = "registry:3@" + testDigest
	args := registryRunArgs(image, 41234)
	if len(args) == 0 || args[0] != "run" {
		t.Fatalf("args = %q, want a docker run", args)
	}
	if last := args[len(args)-1]; last != image {
		t.Errorf("the image is %q, want the pin %q last", last, image)
	}
	pull := -1
	for i, a := range args {
		if a == "--pull" {
			pull = i
		}
		if a == "registry:3" {
			t.Errorf("args %q name the floating tag registry:3", args)
		}
	}
	if pull < 0 || pull+1 >= len(args) || args[pull+1] != "never" {
		t.Errorf("args = %q, want --pull never", args)
	}
	if !slices.Contains(args, "127.0.0.1:41234:5000") {
		t.Errorf("args = %q, want the port published on loopback only", args)
	}
	if !slices.Contains(args, "REGISTRY_STORAGE_DELETE_ENABLED=true") {
		t.Errorf("args = %q, want deletion enabled", args)
	}
}

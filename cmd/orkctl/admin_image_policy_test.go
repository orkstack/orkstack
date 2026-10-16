package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// imagePolicyStore is an in-memory artifacts.ImagePolicyStore that refuses
// exactly what the store refuses and records every whole document written.
type imagePolicyStore struct {
	mu     sync.Mutex
	policy *orkstackv1.ImagePolicy
	writes []*orkstackv1.ImagePolicy
}

func (s *imagePolicyStore) Get(context.Context) (*orkstackv1.ImagePolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.policy == nil {
		return nil, artifacts.ErrNotFound
	}
	return proto.Clone(s.policy).(*orkstackv1.ImagePolicy), nil
}

func (s *imagePolicyStore) Put(_ context.Context, p *orkstackv1.ImagePolicy, _ artifacts.PutConditions) (*orkstackv1.ImagePolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := artifacts.ValidImagePolicy(p); err != nil {
		return nil, err
	}
	s.writes = append(s.writes, proto.Clone(p).(*orkstackv1.ImagePolicy))
	stored := proto.Clone(p).(*orkstackv1.ImagePolicy)
	stored.UpdatedAt = timestamppb.New(fixedNow)
	s.policy = stored
	return proto.Clone(stored).(*orkstackv1.ImagePolicy), nil
}

func (s *imagePolicyStore) lastWrite(t *testing.T) *orkstackv1.ImagePolicy {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.writes) == 0 {
		t.Fatal("no policy was written")
	}
	return s.writes[len(s.writes)-1]
}

// stampResolver pins `uses:` without a network (the catalog rule, then the
// reference parsed, with a digest derived from it), so the CLI's validate and
// submit refusals are observed on real ImageRef values.
type stampResolver struct{}

func (stampResolver) ResolveOrkflow(ctx context.Context, catalog dsl.Catalog, wf *orkstackv1.Orkflow) (dsl.Resolutions, error) {
	out := dsl.Resolutions{}
	for _, t := range wf.GetTasks() {
		reference, err := catalog.ResolveUses(t.GetId(), t.GetUses())
		if err != nil {
			return nil, err
		}
		image, err := stampResolver{}.Resolve(ctx, t.GetUses(), reference)
		if err != nil {
			return nil, err
		}
		t.Image = image
		out[t.GetUses()] = &dsl.Resolution{Image: image}
	}
	return out, nil
}

// Inspect is Resolve with no declared interface.
func (stampResolver) Inspect(ctx context.Context, uses, reference string) (*dsl.Resolution, error) {
	image, err := stampResolver{}.Resolve(ctx, uses, reference)
	if err != nil {
		return nil, err
	}
	return &dsl.Resolution{Image: image}, nil
}

func (stampResolver) Resolve(_ context.Context, uses, reference string) (*orkstackv1.ImageRef, error) {
	parsed, err := dsl.ParseReference(reference)
	if err != nil {
		return nil, err
	}
	digest := parsed.Digest
	if digest == "" {
		digest = "sha256:" + strings.Repeat("c", 64)
	}
	return &orkstackv1.ImageRef{Reference: uses, Registry: parsed.Registry, Repository: parsed.Repository,
		IndexDigest: digest, Pinned: parsed.Pinned(digest)}, nil
}

func imagePolicyFixture(t *testing.T, store *imagePolicyStore) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.ImagePolicies = store
		cfg.Resolver = stampResolver{}
		cfg.Signatures = &dsl.SignatureVerifier{Resolver: dsl.NewResolver("")}
	})
}

// writeKey writes a PEM file into a temp dir and returns its path.
func writeKey(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func storedPolicy() *orkstackv1.ImagePolicy {
	return &orkstackv1.ImagePolicy{
		RequireDigest:     true,
		AllowedRegistries: []string{"ghcr.io/", "123.dkr.ecr.eu-west-1.amazonaws.com/"},
		SigningKeys:       map[string]string{"acme-release": task.FixedSigningKeyPEM, "acme-staging": task.FixedSigningKeyPEM},
		UpdatedAt:         timestamppb.New(fixedNow),
	}
}

func TestAdminImagePolicyGet_GoldenHumanAndJSON(t *testing.T) {
	empty := imagePolicyFixture(t, &imagePolicyStore{})
	human := empty.run("admin", "image-policy", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-get-empty.golden", human.stdout)
	machine := empty.run("--json", "admin", "image-policy", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-get-empty.json.golden", machine.stdout)

	stored := imagePolicyFixture(t, &imagePolicyStore{policy: storedPolicy()})
	human = stored.run("admin", "image-policy", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-get.golden", human.stdout)
	if strings.Contains(human.stdout, "BEGIN PUBLIC KEY") || !strings.Contains(human.stdout, task.FixedSigningKeyFingerprint) {
		t.Fatalf("human output must show fingerprints, never PEM:\n%s", human.stdout)
	}
	machine = stored.run("--json", "admin", "image-policy", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-get.json.golden", machine.stdout)
}

func TestAdminImagePolicySet_ReadsEditsWrites(t *testing.T) {
	store := &imagePolicyStore{policy: storedPolicy()}
	f := imagePolicyFixture(t, store)
	set := f.run("admin", "image-policy", "set",
		"--allowed-registry", "ghcr.io/", "--allowed-registry", "localhost:5001/orkstack/",
	).requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-set.golden", set.stdout)
	written := store.lastWrite(t)
	if !written.GetRequireDigest() || len(written.GetSigningKeys()) != 2 ||
		strings.Join(written.GetAllowedRegistries(), " ") != "ghcr.io/ localhost:5001/orkstack/" {
		t.Fatalf("written = %v, want the registries replaced and require_digest and keys as stored", written)
	}

	jsonStore := &imagePolicyStore{policy: storedPolicy()}
	jf := imagePolicyFixture(t, jsonStore)
	setJSON := jf.run("--json", "admin", "image-policy", "set", "--require-digest=false",
		"--signing-key", "release="+writeKey(t, "cosign.pub", task.FixedSigningKeyPEM),
		"--remove-signing-key", "acme-staging").requireExit(t, exitOK)
	requireGolden(t, "admin-image-policy-set.json.golden", setJSON.stdout)
	written = jsonStore.lastWrite(t)
	if written.GetRequireDigest() || len(written.GetAllowedRegistries()) != 2 ||
		written.GetSigningKeys()["release"] != task.FixedSigningKeyPEM || written.GetSigningKeys()["acme-staging"] != "" {
		t.Fatalf("written = %v", written)
	}
}

func TestAdminImagePolicySet_RefusesAnUnparsableKeyFileLocally(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	path := writeKey(t, "cosign.pub", "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n")
	before := len(f.requests())
	result := f.run("admin", "image-policy", "set", "--signing-key", "acme-release="+path)
	if result.code == exitOK {
		t.Fatal("an unparsable key file was accepted")
	}
	result.requireStderrContains(t, "--signing-key acme-release: "+path+" is not a PEM ECDSA P-256 public key")
	if after := f.requests(); len(after) != before {
		t.Fatalf("a local refusal reached the wire: %v", after[before:])
	}
}

func TestAdminImagePolicySet_RefusesClearAndAllowedRegistryTogether(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	result := f.run("admin", "image-policy", "set", "--clear-allowed-registries", "--allowed-registry", "ghcr.io/")
	if result.code == exitOK {
		t.Fatal("contradictory flags were accepted")
	}
	result.requireStderrContains(t, "--allowed-registry and --clear-allowed-registries")
	if len(f.requests()) != 0 {
		t.Fatalf("a local refusal reached the wire: %v", f.requests())
	}
}

func TestAdminImagePolicySet_RefusesRemovingAKeyItIsAlsoAdding(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	result := f.run("admin", "image-policy", "set",
		"--signing-key", "release="+writeKey(t, "cosign.pub", task.FixedSigningKeyPEM),
		"--remove-signing-key", "release")
	if result.code == exitOK {
		t.Fatal("contradictory flags were accepted")
	}
	result.requireStderrContains(t, "--signing-key release and --remove-signing-key release")
	if len(f.requests()) != 0 {
		t.Fatalf("a local refusal reached the wire: %v", f.requests())
	}
}

func TestAdminImagePolicySet_SurfacesARejectedWrite(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	result := f.run("admin", "image-policy", "set", "--allowed-registry", "ghcr.io/acme/")
	if result.code == exitOK {
		t.Fatal("a floor excluding the catalog was accepted")
	}
	result.requireStderrContains(t, "failed_precondition",
		"allowed registries exclude catalog activities", "shell (ghcr.io/orkstack/activity-shell)")
}

func TestNamespaceCommands_RequireSignedByGoldenHumanAndJSON(t *testing.T) {
	store := &imagePolicyStore{policy: &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"release": task.FixedSigningKeyPEM}}}
	for _, mode := range []string{"human", "json"} {
		t.Run(mode, func(t *testing.T) {
			f := imagePolicyFixture(t, store)
			args := func(rest ...string) []string {
				if mode == "json" {
					return append([]string{"--json"}, rest...)
				}
				return rest
			}
			suffix := ".golden"
			if mode == "json" {
				suffix = ".json.golden"
			}
			f.run("namespace", "create", "prod-payments").requireExit(t, exitOK)
			set := f.run(args("namespace", "update", "prod-payments", "--require-signed-by", "cosign:release")...).requireExit(t, exitOK)
			requireGolden(t, "namespace-require-signed-by"+suffix, set.stdout)
			got := f.run(args("namespace", "get", "prod-payments")...).requireExit(t, exitOK)
			requireGolden(t, "namespace-get-require-signed-by"+suffix, got.stdout)
			list := f.run(args("namespace", "list")...).requireExit(t, exitOK)
			requireGolden(t, "namespace-list-require-signed-by"+suffix, list.stdout)
			cleared := f.run(args("namespace", "update", "prod-payments", "--clear-require-signed-by")...).requireExit(t, exitOK)
			requireGolden(t, "namespace-clear-require-signed-by"+suffix, cleared.stdout)
		})
	}
	// An unknown key is the engine's sentence, verbatim.
	f := imagePolicyFixture(t, store)
	unknown := f.run("namespace", "create", "prod-ledger", "--require-signed-by", "cosign:acme-release")
	if unknown.code == exitOK {
		t.Fatal("a signer naming an unknown key was accepted")
	}
	unknown.requireStderrContains(t, `require_signed_by "cosign:acme-release" names a signing key this deployment does not hold; known keys: release`)
}

func TestNamespaceUpdate_RefusesRequireSignedByAndClearTogether(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	result := f.run("namespace", "update", "default", "--require-signed-by", "cosign:release", "--clear-require-signed-by")
	if result.code == exitOK {
		t.Fatal("contradictory flags were accepted")
	}
	result.requireStderrContains(t, "--require-signed-by and --clear-require-signed-by")
	if len(f.requests()) != 0 {
		t.Fatalf("a local refusal reached the wire: %v", f.requests())
	}
}

func TestNamespaceUpdate_RefusesAnEmptyRequireSignedByLocally(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{})
	result := f.run("namespace", "update", "default", "--require-signed-by", "")
	if result.code == exitOK {
		t.Fatal("an empty signer was accepted")
	}
	result.requireStderrContains(t, "--require-signed-by needs cosign:<key-name>")
	if len(f.requests()) != 0 {
		t.Fatalf("a local refusal reached the wire: %v", f.requests())
	}
}

// migrateDocument writes an orkflow whose third task uses an image outside a
// ghcr.io/-only floor.
func migrateDocument(t *testing.T) string {
	t.Helper()
	return writeKey(t, "deploy.ork.yaml", `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: 0.1.0
tasks:
  - id: prepare
    uses: shell
    with: {command: "echo prepare"}
  - id: check
    uses: ghcr.io/acme/check@sha256:`+strings.Repeat("d", 64)+`
  - id: migrate
    uses: docker.io/library/alpine:3.20
`)
}

const migrateSentence = `task "migrate": image "docker.io/library/alpine:3.20" resolves to docker.io/library/alpine, which is outside the registries this deployment allows: ghcr.io/ (orkctl admin image-policy get)`

func TestOrkflowValidate_PrintsTheImageIssueOnItsFieldPath(t *testing.T) {
	f := imagePolicyFixture(t, &imagePolicyStore{policy: &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}}})
	path := migrateDocument(t)
	human := f.run("orkflow", "validate", path)
	if human.code == exitOK {
		t.Fatal("validate of a forbidden image exited 0")
	}
	human.requireContains(t, "tasks[2].uses: "+migrateSentence)

	machine := f.run("--json", "orkflow", "validate", path)
	var decoded struct {
		Issues []struct {
			Field    string `json:"field"`
			Message  string `json:"message"`
			Severity string `json:"severity"`
		} `json:"issues"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &decoded); err != nil {
		t.Fatalf("decode --json: %v\n%s", err, machine.stdout)
	}
	if len(decoded.Issues) != 1 || decoded.Issues[0].Field != "tasks[2].uses" ||
		decoded.Issues[0].Severity != "ERROR" || decoded.Issues[0].Message != migrateSentence {
		t.Fatalf("--json issues = %+v", decoded.Issues)
	}
	apply := f.run("apply", path)
	if apply.code == exitOK {
		t.Fatal("apply of a forbidden image succeeded")
	}
	apply.requireStderrContains(t, "invalid_argument", migrateSentence)
}

func TestRunSubmit_RefusedByTheImagePolicyNamesTheImageAndTheRule(t *testing.T) {
	store := &imagePolicyStore{}
	f := imagePolicyFixture(t, store)
	f.run("apply", migrateDocument(t)).requireExit(t, exitOK)
	store.mu.Lock()
	store.policy = &orkstackv1.ImagePolicy{RequireDigest: true, UpdatedAt: timestamppb.New(fixedNow)}
	store.mu.Unlock()

	submit := f.run("run", "submit", "deploy")
	if submit.code == exitOK {
		t.Fatal("a submit of a definition the policy now forbids succeeded")
	}
	submit.requireStderrContains(t, "failed_precondition", `"docker.io/library/alpine:3.20"`,
		"is a mutable tag, and this deployment requires a digest", "orkctl admin image-policy get")
	f.run("run", "list").requireExit(t, exitOK).requireContains(t, "No runs")
}

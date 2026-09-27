package api_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memImagePolicies is an in-memory ImagePolicyStore that refuses exactly what
// the store refuses (artifacts.ValidImagePolicy) and counts writes.
type memImagePolicies struct {
	mu     sync.Mutex
	policy *orkstackv1.ImagePolicy
	puts   int
	getErr error
	now    time.Time
}

func (m *memImagePolicies) Get(context.Context) (*orkstackv1.ImagePolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.policy == nil {
		return nil, fmt.Errorf("image policy: %w", artifacts.ErrNotFound)
	}
	return proto.Clone(m.policy).(*orkstackv1.ImagePolicy), nil
}

func (m *memImagePolicies) Put(_ context.Context, p *orkstackv1.ImagePolicy, _ artifacts.PutConditions) (*orkstackv1.ImagePolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := artifacts.ValidImagePolicy(p); err != nil {
		return nil, err
	}
	m.puts++
	stored := proto.Clone(p).(*orkstackv1.ImagePolicy)
	now := m.now
	if now.IsZero() {
		now = time.Date(2026, 9, 14, 11, 2, 44, 0, time.UTC)
	}
	stored.UpdatedAt = timestamppb.New(now)
	m.policy = stored
	return proto.Clone(stored).(*orkstackv1.ImagePolicy), nil
}

func (m *memImagePolicies) set(p *orkstackv1.ImagePolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policy = p
}

func (m *memImagePolicies) putCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

// stampResolver pins every `uses:` without a network: the catalog's rule
// first, then the reference parsed, with a digest derived from the reference
// (or its own pin, when it has one). It stands in for the one resolver so
// the policy can be judged on real ImageRef values.
type stampResolver struct {
	mu    sync.Mutex
	calls int
}

func (r *stampResolver) ResolveOrkflow(ctx context.Context, catalog dsl.Catalog, wf *orkstackv1.Orkflow) error {
	for _, t := range wf.GetTasks() {
		reference, err := catalog.ResolveUses(t.GetId(), t.GetUses())
		if err != nil {
			return err
		}
		image, err := r.Resolve(ctx, t.GetUses(), reference)
		if err != nil {
			return err
		}
		t.Image = image
	}
	return nil
}

func (r *stampResolver) Resolve(_ context.Context, uses, reference string) (*orkstackv1.ImageRef, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	parsed, err := dsl.ParseReference(reference)
	if err != nil {
		return nil, err
	}
	digest := parsed.Digest
	if digest == "" {
		digest = cosigntest.Digest([]byte(parsed.Registry + "/" + parsed.Repository + ":" + parsed.Tag))
	}
	return &orkstackv1.ImageRef{
		Reference: uses, Registry: parsed.Registry, Repository: parsed.Repository,
		IndexDigest: digest, Pinned: parsed.Pinned(digest),
	}, nil
}

// policyFixture is one Service over an image policy store, a namespace
// registry, a stamping resolver, and (when a signer is involved) a registry
// double serving signatures.
type policyFixture struct {
	svc      *api.Service
	orkflows engine.OrkflowStore
	policies *memImagePolicies
	reg      *fakeRegistry
	starter  *fakeStarter
	double   *cosigntest.Double
	key      *cosigntest.Key
	resolver *stampResolver
	logs     *lockedBuffer
}

type policyOptions struct {
	noVerifier bool
	noStore    bool
}

func newPolicyFixture(t *testing.T, opts policyOptions, namespaces ...*orkstackv1.Namespace) *policyFixture {
	t.Helper()
	key, err := cosigntest.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	f := &policyFixture{
		orkflows: engine.NewMemoryOrkflowStore(),
		policies: &memImagePolicies{},
		reg:      newFakeRegistry(namespaces...),
		starter:  &fakeStarter{},
		double:   cosigntest.NewDouble(t),
		key:      key,
		resolver: &stampResolver{},
		logs:     &lockedBuffer{},
	}
	f.svc = f.service(opts)
	return f
}

// service builds a Service over the fixture's shared stores, so a second
// replica (or a later configuration) sees exactly what the first wrote.
func (f *policyFixture) service(opts policyOptions) *api.Service {
	var counter int
	cfg := api.Config{
		Orkflows:   f.orkflows,
		Namespaces: f.reg,
		Starter:    f.starter,
		Resolver:   f.resolver,
		Logger:     slog.New(slog.NewTextHandler(f.logs, nil)),
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	}
	if !opts.noStore {
		cfg.ImagePolicies = f.policies
	}
	if !opts.noVerifier {
		cfg.Signatures = &dsl.SignatureVerifier{Resolver: dsl.NewResolver("")}
	}
	return api.NewService(cfg)
}

func (f *policyFixture) validate(t *testing.T, yaml, namespace string) *orkstackv1.ValidateOrkflowResponse {
	t.Helper()
	resp, err := f.svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: yaml, Namespace: namespace}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	return resp.Msg
}

func (f *policyFixture) apply(yaml, namespace string) error {
	_, err := f.svc.ApplyOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: yaml, Namespace: namespace}))
	return err
}

func (f *policyFixture) submit(name, namespace, key string, allowEmpty bool) (*orkstackv1.SubmitRunResponse, error) {
	resp, err := f.svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: name, Namespace: namespace, IdempotencyKey: key, AllowEmptyPool: allowEmpty,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (f *policyFixture) stored(t *testing.T, namespace string) int {
	t.Helper()
	list, err := f.orkflows.List(context.Background(), namespace)
	if err != nil {
		t.Fatalf("list orkflows: %v", err)
	}
	return len(list)
}

func orkflowUsing(name string, uses ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\nversion: 0.1.0\ntasks:\n", name)
	ids := []string{"migrate", "verify", "report"}
	for i, u := range uses {
		fmt.Fprintf(&b, "  - id: %s\n    uses: %q\n", ids[i], u)
		if i > 0 {
			fmt.Fprintf(&b, "    needs: [%s]\n", ids[i-1])
		}
	}
	return b.String()
}

func requirePolicyCode(t *testing.T, err error, code connect.Code, contains ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil, want %v", code)
	}
	if got := connect.CodeOf(err); got != code {
		t.Fatalf("code = %v (%v), want %v", got, err, code)
	}
	for _, want := range contains {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
}

const alpineRefusal = `task "migrate": image "docker.io/library/alpine:3.20" resolves to docker.io/library/alpine, which is outside the registries this deployment allows: ghcr.io/ (orkctl admin image-policy get)`

// --- the two RPCs ------------------------------------------------------------

func TestGetImagePolicy_AnsweresTheEmptyPolicyWhenNoneIsStored(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	resp, err := f.svc.GetImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
	if err != nil {
		t.Fatalf("GetImagePolicy: %v", err)
	}
	p := resp.Msg.GetPolicy()
	if p == nil || p.GetUpdatedAt() != nil || p.GetRequireDigest() || len(p.GetAllowedRegistries()) != 0 || len(p.GetSigningKeys()) != 0 {
		t.Fatalf("policy = %v, want the empty policy with updated_at unset", p)
	}
}

func TestGetImagePolicy_UnavailableStoreIsNotSwallowed(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.policies.getErr = errors.New("minio is down")
	_, err := f.svc.GetImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
	requirePolicyCode(t, err, connect.CodeUnavailable, "minio is down")
	// And the gates read the same store: an unreadable floor is never "no policy".
	requirePolicyCode(t, f.apply(orkflowUsing("x", "ghcr.io/acme/migrate:v3"), ""), connect.CodeUnavailable)
}

func TestImagePolicy_WithNoStoreAnswersUnimplemented(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{noStore: true})
	_, err := f.svc.GetImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
	requirePolicyCode(t, err, connect.CodeUnimplemented, "no image policy store")
	_, err = f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{}))
	requirePolicyCode(t, err, connect.CodeUnimplemented, "no image policy store")
	// The unit-test configuration treats the floor as empty.
	if err := f.apply(orkflowUsing("x", "docker.io/library/alpine:3.20"), ""); err != nil {
		t.Fatalf("apply with no store: %v", err)
	}
}

func TestUpdateImagePolicy_RefusesABadEntryBeforeAnyWrite(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	for _, p := range []*orkstackv1.ImagePolicy{
		{AllowedRegistries: []string{"acme/"}},
		{AllowedRegistries: []string{"ghcr.io/acme/*"}},
		{SigningKeys: map[string]string{"release": "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"}},
	} {
		_, err := f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{Policy: p}))
		requirePolicyCode(t, err, connect.CodeInvalidArgument)
	}
	if n := f.policies.putCount(); n != 0 {
		t.Fatalf("a refused policy was written %d time(s)", n)
	}
}

func TestUpdateImagePolicy_RefusesAListThatExcludesTheCatalog(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	_, err := f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{
		Policy: &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/acme/"}},
	}))
	requirePolicyCode(t, err, connect.CodeFailedPrecondition,
		"allowed registries exclude catalog activities", "shell (ghcr.io/orkstack/activity-shell)",
		"widen the list or change ORK_ACTIVITY_CATALOG first")
	if n := f.policies.putCount(); n != 0 {
		t.Fatalf("a refused policy was written %d time(s)", n)
	}
	// require_digest is never applied to the tag-spelled catalog.
	if _, err := f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{
		Policy: &orkstackv1.ImagePolicy{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/"}},
	})); err != nil {
		t.Fatalf("require_digest over a tag-spelled catalog: %v", err)
	}
}

func TestUpdateImagePolicy_RefusesRemovingAKeyANamespaceNames(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{},
		&orkstackv1.Namespace{Name: "prod-payments", RequireSignedBy: "cosign:acme-release"},
		&orkstackv1.Namespace{Name: "prod-ledger", RequireSignedBy: "cosign:acme-release"})
	f.policies.set(&orkstackv1.ImagePolicy{SigningKeys: map[string]string{"acme-release": task.FixedSigningKeyPEM}})
	_, err := f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{
		Policy: &orkstackv1.ImagePolicy{},
	}))
	requirePolicyCode(t, err, connect.CodeFailedPrecondition,
		`signing key "acme-release" is still required by namespace(s) prod-ledger, prod-payments; clear their require_signed_by first`)
	if n := f.policies.putCount(); n != 0 {
		t.Fatalf("a refused policy was written %d time(s)", n)
	}
}

func TestUpdateImagePolicy_WritesThroughAndStampsUpdatedAt(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	resp, err := f.svc.UpdateImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{
		Policy: &orkstackv1.ImagePolicy{
			RequireDigest:     true,
			AllowedRegistries: []string{"ghcr.io/", "localhost:5001/orkstack/"},
			SigningKeys:       map[string]string{"release": task.FixedSigningKeyPEM},
			UpdatedAt:         timestamppb.New(time.Unix(1, 0)),
		},
	}))
	if err != nil {
		t.Fatalf("UpdateImagePolicy: %v", err)
	}
	if resp.Msg.GetPolicy().GetUpdatedAt().AsTime().Equal(time.Unix(1, 0)) {
		t.Fatal("the caller's updated_at was kept; the store stamps it")
	}
	got, err := f.svc.GetImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
	if err != nil || !proto.Equal(got.Msg.GetPolicy(), resp.Msg.GetPolicy()) {
		t.Fatalf("Get after Update = %v, %v; want %v", got.Msg.GetPolicy(), err, resp.Msg.GetPolicy())
	}
}

// --- validate and apply -----------------------------------------------------

func (f *policyFixture) floor(t *testing.T, p *orkstackv1.ImagePolicy) {
	t.Helper()
	f.policies.set(p)
}

func TestValidateOrkflow_ReportsARegistryRefusalOnItsFieldPath(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.floor(t, &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}})
	resp := f.validate(t, orkflowUsing("deploy", "docker.io/library/alpine:3.20"), "")
	if resp.GetOrkflow() != nil || len(resp.GetIssues()) != 1 {
		t.Fatalf("validate = %v, want exactly one issue and no orkflow", resp)
	}
	issue := resp.GetIssues()[0]
	if issue.GetField() != "tasks[0].uses" || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
		issue.GetMessage() != alpineRefusal {
		t.Fatalf("issue = %v\nwant field tasks[0].uses, ERROR, %s", issue, alpineRefusal)
	}
	if n := f.stored(t, "default"); n != 0 {
		t.Fatalf("validate stored %d orkflow(s)", n)
	}
	// Apply agrees, and writes nothing.
	requirePolicyCode(t, f.apply(orkflowUsing("deploy", "docker.io/library/alpine:3.20"), ""), connect.CodeInvalidArgument, alpineRefusal)
	if n := f.stored(t, "default"); n != 0 {
		t.Fatalf("a refused apply stored %d orkflow(s)", n)
	}
}

func TestValidateOrkflow_ReportsATagUnderRequireDigest(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.floor(t, &orkstackv1.ImagePolicy{RequireDigest: true})
	resp := f.validate(t, orkflowUsing("deploy", "shell", "ghcr.io/acme/migrate:v3"), "")
	if len(resp.GetIssues()) != 1 {
		t.Fatalf("issues = %v", resp.GetIssues())
	}
	want := `task "verify": image "ghcr.io/acme/migrate:v3" is a mutable tag, and this deployment requires a digest — write ghcr.io/acme/migrate@sha256:<digest> (orkctl admin image-policy get)`
	if got := resp.GetIssues()[0]; got.GetField() != "tasks[1].uses" || got.GetMessage() != want {
		t.Fatalf("issue = %v, want tasks[1].uses: %s", got, want)
	}
}

func TestValidateOrkflow_ExemptsACatalogNameFromRequireDigest(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.floor(t, &orkstackv1.ImagePolicy{RequireDigest: true})
	pinned := "ghcr.io/acme/migrate@sha256:" + strings.Repeat("ab", 32)
	resp := f.validate(t, orkflowUsing("deploy", "shell", pinned), "")
	if len(resp.GetIssues()) != 0 || resp.GetOrkflow() == nil {
		t.Fatalf("validate = %v, want no issue for a catalog name and a digest", resp)
	}
}

func signedDocument(host string) string {
	return orkflowUsing("deploy", host+"/acme/migrate:v3", host+"/acme/migrate:v3", host+"/acme/migrate:v3")
}

func (f *policyFixture) signerNamespace(t *testing.T, name, key string) {
	t.Helper()
	f.floor(t, &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"release": f.key.PublicPEM}})
	if _, err := f.reg.Put(context.Background(), &orkstackv1.Namespace{Name: name, RequireSignedBy: "cosign:" + key},
		artifacts.PutConditions{}); err != nil {
		t.Fatal(err)
	}
}

// imageOf is what the stamping resolver pins for host/acme/migrate:v3.
func (f *policyFixture) imageOf(t *testing.T) *orkstackv1.ImageRef {
	t.Helper()
	image, err := (&stampResolver{}).Resolve(context.Background(), "", f.double.Host+"/acme/migrate:v3")
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func (f *policyFixture) signImage(t *testing.T) {
	t.Helper()
	image := f.imageOf(t)
	layer, err := f.key.SignatureLayer(image.GetIndexDigest(), image.GetRegistry()+"/"+image.GetRepository())
	if err != nil {
		t.Fatal(err)
	}
	f.double.Sign(image.GetRepository(), image.GetIndexDigest(), layer)
}

func TestApplyOrkflow_RefusesAnUnsignedImageOnItsFieldPath(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.signerNamespace(t, "prod", "release")
	err := f.apply(signedDocument(f.double.Host), "prod")
	image := f.imageOf(t)
	want := fmt.Sprintf(`task "migrate": image %s carries no cosign signature (%s/acme/migrate:%s is not in the registry); namespace "prod" requires cosign:release`,
		image.GetPinned(), f.double.Host, cosigntest.SignatureTag(image.GetIndexDigest()))
	requirePolicyCode(t, err, connect.CodeInvalidArgument, want)
	resp := f.validate(t, signedDocument(f.double.Host), "prod")
	if len(resp.GetIssues()) != 1 || resp.GetIssues()[0].GetField() != "tasks[0].uses" || resp.GetIssues()[0].GetMessage() != want {
		t.Fatalf("validate issues = %v, want tasks[0].uses: %s", resp.GetIssues(), want)
	}
	if n := f.stored(t, "prod"); n != 0 {
		t.Fatalf("a refused apply stored %d orkflow(s)", n)
	}
}

func TestApplyOrkflow_VerifiesEachDistinctImageOnce(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.signerNamespace(t, "prod", "release")
	f.signImage(t)
	if err := f.apply(signedDocument(f.double.Host), "prod"); err != nil {
		t.Fatalf("apply of a signed image: %v", err)
	}
	if n := f.double.CountSuffix(".sig"); n != 1 {
		t.Fatalf("three tasks of one image made %d .sig GETs, want 1 (requests %v)", n, f.double.Requests())
	}
}

func TestApplyOrkflow_RegistryOutageIsUnavailableNotInvalid(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.signerNamespace(t, "prod", "release")
	f.double.SetStatus("/manifests/", http.StatusServiceUnavailable)
	requirePolicyCode(t, f.apply(signedDocument(f.double.Host), "prod"), connect.CodeUnavailable,
		task.ReasonImageSignatureUnavailable, "this is a registry problem, not a verdict")
	// Validate agrees: an unreachable registry is an RPC failure, not an issue.
	_, err := f.svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: signedDocument(f.double.Host), Namespace: "prod"}))
	requirePolicyCode(t, err, connect.CodeUnavailable)
}

func TestApplyOrkflow_ASignerWithNoVerifierIsFailedPrecondition(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{noVerifier: true})
	f.signerNamespace(t, "prod", "release")
	requirePolicyCode(t, f.apply(signedDocument(f.double.Host), "prod"), connect.CodeFailedPrecondition,
		"this engine has no signature verifier configured")
}

// --- submit ------------------------------------------------------------------

func TestSubmitRun_RefusesAStoredDefinitionThePolicyNowForbids(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	if err := f.apply(orkflowUsing("deploy", "docker.io/library/alpine:3.20"), ""); err != nil {
		t.Fatalf("apply under the empty policy: %v", err)
	}
	// The floor tightens on another replica over the same store.
	f.floor(t, &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}})
	replicaB := f.service(policyOptions{})
	_, err := replicaB.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", IdempotencyKey: "nightly",
	}))
	requirePolicyCode(t, err, connect.CodeFailedPrecondition, alpineRefusal)
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submit started %v", started)
	}
	// No claim was taken: once the floor widens, the SAME key starts a fresh run.
	f.floor(t, &orkstackv1.ImagePolicy{})
	resp, err := replicaB.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "deploy", IdempotencyKey: "nightly",
	}))
	if err != nil {
		t.Fatalf("submit after widening: %v", err)
	}
	if started := f.starter.startedRuns(); len(started) != 1 || started[0] != resp.Msg.GetRun().GetId() {
		t.Fatalf("started %v, want exactly the fresh run %s", started, resp.Msg.GetRun().GetId())
	}
}

func TestSubmitRun_AllowEmptyPoolDoesNotBypassThePolicy(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	if err := f.apply(orkflowUsing("deploy", "docker.io/library/alpine:3.20"), ""); err != nil {
		t.Fatal(err)
	}
	f.floor(t, &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}})
	_, err := f.submit("deploy", "", "", true)
	requirePolicyCode(t, err, connect.CodeFailedPrecondition, alpineRefusal)
	if len(f.starter.startedRuns()) != 0 {
		t.Fatal("allow_empty_pool bypassed the image policy")
	}
}

func TestSubmitRun_PinsTheSelectionAndTheKeyOnThePlan(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	if _, err := f.submitNoPolicy(t); err != nil {
		t.Fatal(err)
	}
	if plan := f.lastPlan(t); plan.ImagePolicy != nil {
		t.Fatalf("under the empty policy the plan carries %v, want nil", plan.ImagePolicy)
	}

	f.signerNamespace(t, "prod", "release")
	f.signImage(t)
	policy := &orkstackv1.ImagePolicy{
		RequireDigest:     false,
		AllowedRegistries: []string{f.double.Host + "/acme/", "ghcr.io/"},
		SigningKeys:       map[string]string{"release": f.key.PublicPEM},
	}
	f.floor(t, policy)
	if err := f.apply(signedDocument(f.double.Host), "prod"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := f.submit("deploy", "prod", "", false); err != nil {
		t.Fatalf("submit: %v", err)
	}
	ns, _ := f.reg.Get(context.Background(), "prod")
	want, err := task.Selection(policy, ns)
	if err != nil {
		t.Fatal(err)
	}
	got := f.lastPlan(t).ImagePolicy.Proto()
	if !proto.Equal(got, want) || got.GetSigningKeyPem() != f.key.PublicPEM {
		t.Fatalf("plan selection = %v, want %v (PEM included)", got, want)
	}
}

func (f *policyFixture) submitNoPolicy(t *testing.T) (*orkstackv1.SubmitRunResponse, error) {
	t.Helper()
	if err := f.apply(orkflowUsing("plain", "shell"), ""); err != nil {
		return nil, err
	}
	return f.submit("plain", "", "", false)
}

func (f *policyFixture) lastPlan(t *testing.T) engine.RunPlan {
	t.Helper()
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.plans) == 0 {
		t.Fatal("nothing was started")
	}
	return f.starter.plans[len(f.starter.plans)-1]
}

func TestSubmitRun_AnAcceptedRunIsNotRevisitedWhenThePolicyTightens(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	if err := f.apply(orkflowUsing("deploy", "docker.io/library/alpine:3.20"), ""); err != nil {
		t.Fatal(err)
	}
	accepted, err := f.submit("deploy", "", "", false)
	if err != nil {
		t.Fatalf("submit under the empty policy: %v", err)
	}
	f.floor(t, &orkstackv1.ImagePolicy{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/"}})
	// The accepted run is untouched: nothing cancelled, its plan still empty.
	if canceled := f.starter.canceled; len(canceled) != 0 {
		t.Fatalf("tightening the policy cancelled %v", canceled)
	}
	if plan := f.lastPlan(t); plan.ImagePolicy != nil {
		t.Fatalf("the accepted run's plan changed: %v", plan.ImagePolicy)
	}
	got, err := f.svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: accepted.GetRun().GetId()}))
	if err != nil || got.Msg.GetRun().GetId() != accepted.GetRun().GetId() {
		t.Fatalf("the accepted run is no longer readable: %v", err)
	}
	// The next submit is refused.
	_, err = f.submit("deploy", "", "", false)
	requirePolicyCode(t, err, connect.CodeFailedPrecondition, "is a mutable tag")
}

// --- namespaces --------------------------------------------------------------

func TestCreateNamespace_RefusesASignerNamingAnUnknownKey(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.floor(t, &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"acme-staging": task.FixedSigningKeyPEM}})
	_, err := f.svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "prod-payments", RequireSignedBy: "cosign:acme-release"},
	}))
	requirePolicyCode(t, err, connect.CodeInvalidArgument,
		`namespace "prod-payments" require_signed_by "cosign:acme-release" names a signing key this deployment does not hold; known keys: acme-staging (orkctl admin image-policy set --signing-key)`)
	_, err = f.svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "prod-payments", RequireSignedBy: "notary:x"},
	}))
	requirePolicyCode(t, err, connect.CodeInvalidArgument, "cosign is the only signer this deployment understands")
	if _, err := f.svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "prod-payments", RequireSignedBy: "cosign:acme-staging"},
	})); err != nil {
		t.Fatalf("a known key: %v", err)
	}
}

func TestUpdateNamespace_ChecksTheKeyOnlyWhenTheSignerChanges(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	// A namespace naming a key the policy no longer holds (another release
	// wrote it): editing its description must still work.
	if _, err := f.reg.Put(context.Background(), &orkstackv1.Namespace{Name: "prod", RequireSignedBy: "cosign:gone"},
		artifacts.PutConditions{}); err != nil {
		t.Fatal(err)
	}
	f.floor(t, &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"release": task.FixedSigningKeyPEM}})
	stored, _ := f.reg.Get(context.Background(), "prod")
	edited := proto.Clone(stored).(*orkstackv1.Namespace)
	edited.Description = "payments"
	updated, err := f.svc.UpdateNamespace(context.Background(), connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: edited}))
	if err != nil {
		t.Fatalf("an unchanged stale signer was refused: %v", err)
	}
	if !strings.Contains(f.logs.String(), "require_signed_by key this deployment no longer holds") {
		t.Fatalf("no warning logged: %s", f.logs.String())
	}
	changed := proto.Clone(updated.Msg.GetNamespace()).(*orkstackv1.Namespace)
	changed.RequireSignedBy = "cosign:other"
	_, err = f.svc.UpdateNamespace(context.Background(), connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: changed}))
	requirePolicyCode(t, err, connect.CodeInvalidArgument, "known keys: release")
}

func TestCreateNamespace_IdempotentRepeatComparesRequireSignedBy(t *testing.T) {
	f := newPolicyFixture(t, policyOptions{})
	f.floor(t, &orkstackv1.ImagePolicy{SigningKeys: map[string]string{
		"acme-release": task.FixedSigningKeyPEM, "acme-staging": task.FixedSigningKeyPEM,
	}})
	create := func(signer string) (*orkstackv1.CreateNamespaceResponse, error) {
		resp, err := f.svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
			Namespace: &orkstackv1.Namespace{Name: "prod-payments", RequireSignedBy: signer},
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	if first, err := create("cosign:acme-release"); err != nil || !first.GetCreated() {
		t.Fatalf("first create: %v %v", first, err)
	}
	if again, err := create("cosign:acme-release"); err != nil || again.GetCreated() {
		t.Fatalf("idempotent repeat = %v, %v; want created=false", again, err)
	}
	_, err := create("cosign:acme-staging")
	requirePolicyCode(t, err, connect.CodeAlreadyExists, `require_signed_by is "cosign:acme-release", not "cosign:acme-staging"`)
	_, err = create("")
	requirePolicyCode(t, err, connect.CodeAlreadyExists, `require_signed_by is "cosign:acme-release", not "" (none)`)
}

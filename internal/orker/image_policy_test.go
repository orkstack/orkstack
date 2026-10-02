package orker

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// eventLog is the one ordered, mutex-guarded record every image-policy test
// reads: the registry double appends "GET <path>" and the runtime appends
// "pull" and "create", so "before" and "never" are assertions on one list.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *eventLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

// policyCapturingRuntime is phaseCapturingRuntime that also logs its pull and
// create into the shared event log.
type policyCapturingRuntime struct {
	phaseCapturingRuntime
	log *eventLog
}

func (r *policyCapturingRuntime) Pull(ctx context.Context, ref string) (PullInfo, error) {
	r.log.add("pull")
	return r.phaseCapturingRuntime.Pull(ctx, ref)
}

func (r *policyCapturingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.log.add("create")
	return r.phaseCapturingRuntime.Create(ctx, spec)
}

// policyHarness is one dispatcher over a policy-logging runtime, a memory
// attempt store and commit store, a phase observer, and a SignatureVerifier
// whose registry is a double logging into the same event log.
type policyHarness struct {
	d        *OCIDispatcher
	obs      *fakePhaseObserver
	attempts *artifacts.MemoryAttemptStore
	log      *eventLog
	double   *cosigntest.Double
	key      *cosigntest.Key
}

const policyRepo = "acme/migrate"

func newPolicyHarness(t *testing.T) *policyHarness {
	t.Helper()
	h := &policyHarness{
		obs:      &fakePhaseObserver{},
		attempts: artifacts.NewMemoryAttemptStore(),
		log:      &eventLog{},
		double:   cosigntest.NewDouble(t),
	}
	key, err := cosigntest.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	h.key = key
	h.double.OnRequest = h.log.add
	h.d = newAttemptDispatcher(t, &policyCapturingRuntime{log: h.log}, h.attempts)
	h.d.Phases = h.obs
	h.d.Commits = artifacts.NewMemoryCommitStore()
	h.d.Signatures = &dsl.SignatureVerifier{Resolver: dsl.NewResolver(""), AuthFileVar: RegistryAuthFileVar}
	return h
}

// input is baseExportInput pointed at the double's registry, with a commit
// URI so the commit check is real.
func (h *policyHarness) input(taskID string, sel *orkstackv1.ImagePolicySelection) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput(taskID)
	digest := cosigntest.Digest([]byte("image " + taskID))
	in.Image = &orkstackv1.ImageRef{
		Reference: h.double.Host + "/" + policyRepo + "@" + digest, Registry: h.double.Host,
		Repository: policyRepo, IndexDigest: digest, Pinned: h.double.Host + "/" + policyRepo + "@" + digest,
	}
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + taskID + "/commit.json"
	in.ImagePolicy = sel
	return in
}

func (h *policyHarness) sign(t *testing.T, in *orkstackv1.ExecuteTaskInput, key *cosigntest.Key) {
	t.Helper()
	layer, err := key.SignatureLayer(in.GetImage().GetIndexDigest(), in.GetImage().GetRegistry()+"/"+policyRepo)
	if err != nil {
		t.Fatal(err)
	}
	h.double.Sign(policyRepo, in.GetImage().GetIndexDigest(), layer)
}

func (h *policyHarness) signer() *orkstackv1.ImagePolicySelection {
	return &orkstackv1.ImagePolicySelection{RequireSignedBy: "cosign:release", SigningKeyPem: h.key.PublicPEM}
}

func requireFailure(t *testing.T, err error, reason, category string) *Failure {
	t.Helper()
	failure := failureOf(err)
	if failure == nil {
		t.Fatalf("Execute = %v, want a %s failure", err, reason)
	}
	if failure.Reason != reason || CategoryFor(failure.Reason) != category {
		t.Fatalf("failure = %s (%s), want %s (%s): %s", failure.Reason, CategoryFor(failure.Reason), reason, category, failure.Message)
	}
	return failure
}

// failureOf recovers the Failure a Temporal error carries, by its reason code
// in the error text (the ApplicationError's type is the category, its message
// "REASON: sentence").
func failureOf(err error) *Failure {
	if err == nil {
		return nil
	}
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	msg := err.Error()
	for reason := range reasonCategory {
		if strings.Contains(msg, reason+": ") {
			return &Failure{Reason: reason, Message: msg}
		}
	}
	return nil
}

func (h *policyHarness) phases() []string {
	_, phases, _, _ := h.obs.recorded()
	return phases
}

func requirePhases(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("phases = %q, want %q", got, want)
	}
}

func requireEmptyDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("%s holds %v; a refused image must leave nothing on disk", dir, names)
	}
}

func TestDispatcher_RefusesAnImageOutsideTheRegistriesBeforeAnythingRuns(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("outside", &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/"}})
	_, err := h.d.Execute(context.Background(), in)
	failure := requireFailure(t, err, ReasonImageNotPermitted, CategoryContractViolation)
	if !strings.Contains(failure.Message, "outside the registries this deployment allows: ghcr.io/acme/") ||
		!strings.Contains(failure.Message, "refused before anything is pulled") {
		t.Fatalf("message = %s", failure.Message)
	}
	if events := h.log.all(); len(events) != 0 {
		t.Fatalf("event log = %v, want nothing pulled, created or fetched", events)
	}
	requireEmptyDir(t, h.d.WorkRoot)
	requirePhases(t, h.phases(), []string{"start", "commit-check", "image-policy"})
	record := onlyAttempt(t, h.attempts, in)
	if record.GetPhase() != "image-policy" || record.GetError().GetCode() != ReasonImageNotPermitted {
		t.Fatalf("attempt record phase %q error %v, want image-policy / %s", record.GetPhase(), record.GetError(), ReasonImageNotPermitted)
	}
}

func TestDispatcher_APinnedTaskRefusedByThePolicyCreatesNoPinDirectory(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("pinned", &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/"}})
	in.Pin = &orkstackv1.PinContext{Name: "build", InstanceId: "build-1", TimeoutSeconds: 300}
	_, err := h.d.Execute(context.Background(), in)
	requireFailure(t, err, ReasonImageNotPermitted, CategoryContractViolation)
	requireEmptyDir(t, h.d.WorkRoot)
}

func TestDispatcher_RefusesATagUnderRequireDigest(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("tagged", &orkstackv1.ImagePolicySelection{RequireDigest: true})
	in.Image.Reference = h.double.Host + "/" + policyRepo + ":v3"
	_, err := h.d.Execute(context.Background(), in)
	failure := requireFailure(t, err, ReasonImageNotPermitted, CategoryContractViolation)
	if !strings.Contains(failure.Message, "is a mutable tag, and this deployment requires a digest") {
		t.Fatalf("message = %s", failure.Message)
	}
	if events := h.log.all(); len(events) != 0 {
		t.Fatalf("event log = %v", events)
	}
}

func TestDispatcher_ExemptsACatalogNameFromRequireDigest(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("catalog", &orkstackv1.ImagePolicySelection{RequireDigest: true})
	in.Image.Reference = "shell"
	if _, err := h.d.Execute(context.Background(), in); err != nil {
		t.Fatalf("a catalog name under require_digest: %v", err)
	}
}

func TestDispatcher_ValidateIsUnchangedByASelection(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("validate", &orkstackv1.ImagePolicySelection{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/acme/"}})
	in.Image.Reference = "docker.io/library/alpine:3.20"
	if failure := h.d.Validate(in); failure != nil {
		t.Fatalf("Validate = %v; the image rule must not be in Validate, which precedes adoption", failure)
	}
}

func TestDispatcher_VerifiesTheSignatureBeforePulling(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("signed", h.signer())
	h.sign(t, in, h.key)
	if _, err := h.d.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute of a signed image: %v", err)
	}
	events := h.log.all()
	sigTag := cosigntest.SignatureTag(in.GetImage().GetIndexDigest())
	if len(events) != 4 || events[0] != "GET /v2/"+policyRepo+"/manifests/"+sigTag ||
		!strings.HasPrefix(events[1], "GET /v2/"+policyRepo+"/blobs/sha256:") || events[2] != "pull" || events[3] != "create" {
		t.Fatalf("event log = %q, want the .sig GET, the blob GET, then pull, then create", events)
	}
	requirePhases(t, h.phases(), []string{"start", "commit-check", "image-policy", "workspace", "stage-inputs",
		"image", "render", "create", "run", "exit", "validate", "commit"})
}

func TestDispatcher_UnsignedIsNotSignedAndNotRetried(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("unsigned", h.signer())
	_, err := h.d.Execute(context.Background(), in)
	failure := requireFailure(t, err, ReasonImageNotSigned, CategoryContractViolation)
	if RetryableReason(failure.Reason) {
		t.Fatal("IMAGE_NOT_SIGNED is retryable")
	}
	if !strings.Contains(failure.Message, `namespace "default" requires cosign:release`) {
		t.Fatalf("message = %s", failure.Message)
	}
	for _, e := range h.log.all() {
		if e == "pull" || e == "create" {
			t.Fatalf("an unsigned image reached the runtime: %v", h.log.all())
		}
	}
}

func TestDispatcher_WrongKeyIsInvalid(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("wrong-key", h.signer())
	other, _ := cosigntest.GenerateKey()
	h.sign(t, in, other)
	_, err := h.d.Execute(context.Background(), in)
	requireFailure(t, err, ReasonImageSignatureInvalid, CategoryContractViolation)
}

func TestDispatcher_RegistryDownIsUnavailableAndRetryable(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("down", h.signer())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := listener.Addr().String()
	listener.Close()
	in.Image.Registry = host
	in.Image.Pinned = host + "/" + policyRepo + "@" + in.GetImage().GetIndexDigest()
	_, err = h.d.Execute(context.Background(), in)
	failure := requireFailure(t, err, ReasonImageSignatureUnavailable, CategoryInfrastructureError)
	if !RetryableReason(failure.Reason) {
		t.Fatal("IMAGE_SIGNATURE_UNAVAILABLE is not retryable")
	}
}

func TestDispatcher_NoVerifierIsUnavailableNotAPass(t *testing.T) {
	h := newPolicyHarness(t)
	h.d.Signatures = nil
	in := h.input("no-verifier", h.signer())
	h.sign(t, in, h.key)
	_, err := h.d.Execute(context.Background(), in)
	failure := requireFailure(t, err, ReasonImageSignatureUnavailable, CategoryInfrastructureError)
	if !strings.Contains(failure.Message, RegistryAuthFileVar) {
		t.Fatalf("message = %s", failure.Message)
	}
	for _, e := range h.log.all() {
		if e == "pull" {
			t.Fatal("an unverifiable image was pulled")
		}
	}
}

func TestDispatcher_CancelledDuringVerificationIsCancelled(t *testing.T) {
	h := newPolicyHarness(t)
	in := h.input("cancel", h.signer())
	h.sign(t, in, h.key)
	h.double.Block = make(chan struct{})
	h.double.BlockPath = ".sig"
	asked := make(chan struct{}, 1)
	h.double.OnRequest = func(e string) {
		h.log.add(e)
		if strings.HasSuffix(e, ".sig") {
			select {
			case asked <- struct{}{}:
			default:
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.d.Execute(ctx, in)
		done <- err
	}()
	select {
	case <-asked:
	case <-time.After(10 * time.Second):
		t.Fatal("the signature was never requested")
	}
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Execute did not return after cancellation")
	}
	close(h.double.Block)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want the context error", err)
	}
	_, _, _, outcome := h.obs.recorded()
	if !outcome.Canceled || outcome.Reason == ReasonImageSignatureUnavailable {
		t.Fatalf("outcome = %+v, want Canceled and no IMAGE_SIGNATURE_UNAVAILABLE", outcome)
	}
}

func TestDispatcher_NoSelectionChangesNothing(t *testing.T) {
	h := newPolicyHarness(t)
	h.d.Policy = DefaultSecurityPolicy()
	in := h.input("no-selection", nil)
	if _, err := h.d.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, e := range h.log.all() {
		if strings.HasPrefix(e, "GET ") {
			t.Fatalf("a task with no policy asked the registry: %v", h.log.all())
		}
	}
	requirePhases(t, h.phases(), wantPhaseSequence)
}

func TestDispatcher_AnAdoptedCommitIsNeverReJudged(t *testing.T) {
	for _, tc := range []struct {
		name string
		sel  func(h *policyHarness) *orkstackv1.ImagePolicySelection
	}{
		{"registries exclude the image", func(*policyHarness) *orkstackv1.ImagePolicySelection {
			return &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/"}}
		}},
		{"signer with no .sig in the registry", func(h *policyHarness) *orkstackv1.ImagePolicySelection {
			return h.signer()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPolicyHarness(t)
			// Seed the commit: the same activity committed by an earlier
			// attempt (here, one that ran before the refusal could apply).
			in := h.input("adopt", nil)
			first, err := h.d.Execute(context.Background(), in)
			if err != nil || first.GetCommitAdopted() {
				t.Fatalf("seeding attempt: %v (adopted=%v)", err, first.GetCommitAdopted())
			}
			commit, err := h.d.Commits.Get(context.Background(), in.GetCommitUri())
			if err != nil || commit.Commit.GetImage().GetIndexDigest() != in.GetImage().GetIndexDigest() {
				t.Fatalf("seeded commit: %v", err)
			}
			h.log.reset()
			h.obs = &fakePhaseObserver{}
			h.d.Phases = h.obs
			h.d.Attempts = artifacts.NewMemoryAttemptStore()

			retry := h.input("adopt", tc.sel(h))
			result, err := h.d.Execute(context.Background(), retry)
			if err != nil {
				t.Fatalf("the retry of a committed attempt was judged: %v", err)
			}
			if !result.GetCommitAdopted() || string(result.GetValuesJson()) != string(first.GetValuesJson()) {
				t.Fatalf("result = %v, want the committed result adopted", result)
			}
			if events := h.log.all(); len(events) != 0 {
				t.Fatalf("event log = %v: an adopted commit asked the registry or ran the runtime", events)
			}
			requirePhases(t, h.phases(), []string{"start", "commit-check"})
		})
	}
}

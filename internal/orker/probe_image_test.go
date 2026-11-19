package orker

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
)

const releaseCommit = "0123456789abcdef0123456789abcdef01234567"

const releaseDigest = testDigest

// releaseKeyPEM is the repository's release key, deploy/cosign.pub: the key
// a release build stamps in.
func releaseKeyPEM(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../deploy/cosign.pub")
	if err != nil {
		t.Fatalf("read deploy/cosign.pub: %v", err)
	}
	return string(raw)
}

func stampedKey(t *testing.T) string {
	return base64.StdEncoding.EncodeToString([]byte(releaseKeyPEM(t)))
}

// pullRecorder is viewProbeRuntime with every pulled reference kept.
type pullRecorder struct {
	*viewProbeRuntime
	pulled []string
}

func (r *pullRecorder) Pull(ctx context.Context, ref string) (PullInfo, error) {
	r.pulled = append(r.pulled, ref)
	return r.viewProbeRuntime.Pull(ctx, ref)
}

// registry is the release path's resolver and verifier, scripted: each
// resolve answers the next of resolveErrs (then success), and verify answers
// verifyErr. It is a hand-written fake for faults a real registry gives only
// when it is down, rate-limits this host or serves an unsigned image; the
// path against a real registry and a real cosign signature is
// TestIntegration_ProbeImageDefaultVerifiesTheSignature.
type registry struct {
	resolveErrs []error
	verifyErr   error
	resolved    []string
	verified    []string
}

func (g *registry) probe(t *testing.T, base time.Duration, clock *time.Time) *probeImageSource {
	key := releaseKeyPEM(t)
	src := newProbeImageSource("", &ReleaseProbe{
		Ref:    "ghcr.io/orkstack/orker:0.4.0",
		KeyPEM: key,
		Resolve: func(_ context.Context, ref string) (*orkstackv1.ImageRef, error) {
			g.resolved = append(g.resolved, ref)
			if len(g.resolveErrs) > 0 {
				err := g.resolveErrs[0]
				g.resolveErrs = g.resolveErrs[1:]
				if err != nil {
					return nil, err
				}
			}
			return &orkstackv1.ImageRef{Registry: "ghcr.io", Repository: "orkstack/orker", IndexDigest: releaseDigest,
				Pinned: "ghcr.io/orkstack/orker@" + releaseDigest}, nil
		},
		Verify: func(_ context.Context, image *orkstackv1.ImageRef, signer, keyPEM string) error {
			g.verified = append(g.verified, image.GetPinned()+" "+signer)
			if keyPEM != key {
				return errors.New("verified with another key")
			}
			return g.verifyErr
		},
	}, base)
	if clock != nil {
		src.now = func() time.Time { return *clock }
		src.jitter = func() float64 { return 0.5 }
	}
	return src
}

// TestDefaultProbeImage_ReleaseBuildDefaultsToItsReleaseImage: a release
// build with a stamped commit and key defaults to its own release image.
func TestDefaultProbeImage_ReleaseBuildDefaultsToItsReleaseImage(t *testing.T) {
	ref, key, err := DefaultProbeImage("v0.4.0", releaseCommit, stampedKey(t))
	if err != nil {
		t.Fatalf("DefaultProbeImage: %v", err)
	}
	if ref != "ghcr.io/orkstack/orker:0.4.0" || key != releaseKeyPEM(t) {
		t.Fatalf("DefaultProbeImage = %q, %q", ref, key)
	}
}

// TestDefaultProbeImage_RequiredOnASourceBuild: a build with no release
// version or no stamped commit has no release image, so probe_image is
// required, named in the refusal.
func TestDefaultProbeImage_RequiredOnASourceBuild(t *testing.T) {
	for name, tc := range map[string]struct{ version, commit string }{
		"a dev version":      {"dev", releaseCommit},
		"no stamped commit":  {"v0.4.0", ""},
		"a suffixed version": {"v0.4.0-rc1", releaseCommit},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DefaultProbeImage(tc.version, tc.commit, stampedKey(t))
			if err == nil || !strings.Contains(err.Error(), "profiles.probe_image is required on an Orker built from source") {
				t.Fatalf("DefaultProbeImage = %v; want probe_image required", err)
			}
		})
	}
}

// TestDefaultProbeImage_NoReleaseKeyMeansRequired: a release build that
// carries no usable signing key has no default path.
func TestDefaultProbeImage_NoReleaseKeyMeansRequired(t *testing.T) {
	for name, key := range map[string]string{
		"no key":     "",
		"not base64": "%%%",
		"not a key":  base64.StdEncoding.EncodeToString([]byte("hello")),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DefaultProbeImage("v0.4.0", releaseCommit, key)
			if err == nil || !strings.Contains(err.Error(), "release signing key") || !strings.Contains(err.Error(), "set profiles.probe_image") {
				t.Fatalf("DefaultProbeImage = %v; want the missing key named, and probe_image asked for", err)
			}
		})
	}
}

// TestParseProfiles_ProbeImageIsOptional: a profiles document without
// probe_image parses; whether the build has a default is the boot's call.
// A tag in probe_image is still refused.
func TestParseProfiles_ProbeImageIsOptional(t *testing.T) {
	doc := "profiles:\n  sensor:\n    kind: devices\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      ")
	got, err := ParseProfiles([]byte(doc))
	if err != nil || got.ProbeImage != "" || len(got.List) != 1 {
		t.Fatalf("ParseProfiles = %+v, %v; want one profile and no probe image", got, err)
	}
	tagged := "profiles:\n  probe_image: ghcr.io/orkstack/orker:0.4.0\n  sensor:\n    kind: devices\n    devices:" +
		strings.ReplaceAll(contractDevice, "          ", "      ")
	if _, err := ParseProfiles([]byte(tagged)); err == nil || !strings.Contains(err.Error(), "profiles.probe_image") {
		t.Fatalf("a tagged probe_image = %v; want it refused", err)
	}
}

// TestProbeImage_ReleaseImageIsResolvedVerifiedThenPulledByDigest: the
// default path resolves the tag, verifies the digest's signature with the
// release key, and pulls the digest, never the tag.
func TestProbeImage_ReleaseImageIsResolvedVerifiedThenPulledByDigest(t *testing.T) {
	g := &registry{}
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	image, reason, detail, err := checkProbeImage(t.Context(), rt, g.probe(t, time.Second, nil), "c0ffee")
	if err != nil || reason != 0 {
		t.Fatalf("checkProbeImage = %v, %v (%s)", reason, err, detail)
	}
	want := "ghcr.io/orkstack/orker@" + releaseDigest
	if image != want {
		t.Fatalf("image = %q, want %q", image, want)
	}
	if len(g.resolved) != 1 || g.resolved[0] != "ghcr.io/orkstack/orker:0.4.0" {
		t.Fatalf("resolved %v", g.resolved)
	}
	if len(g.verified) != 1 || g.verified[0] != want+" "+releaseSigner {
		t.Fatalf("verified %v", g.verified)
	}
	if len(rt.pulled) != 1 || rt.pulled[0] != want {
		t.Fatalf("pulled %v; want the digest alone", rt.pulled)
	}
}

// TestProbeImage_ExplicitOverrideIsUsedAsGiven: an explicit probe_image is
// pulled as pinned, with no resolve and no signature read; its build label
// is still checked.
func TestProbeImage_ExplicitOverrideIsUsedAsGiven(t *testing.T) {
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	src := newProbeImageSource(viewProbeImage, nil, time.Second)
	image, reason, _, err := checkProbeImage(t.Context(), rt, src, "c0ffee")
	if err != nil || reason != 0 || image != viewProbeImage || len(rt.pulled) != 1 || rt.pulled[0] != viewProbeImage {
		t.Fatalf("checkProbeImage = %q, %v, %v; pulled %v", image, reason, err, rt.pulled)
	}
	rt.label = "deadbeef"
	if _, reason, detail, _ := checkProbeImage(t.Context(), rt, src, "c0ffee"); reason != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH ||
		!strings.Contains(detail, "profiles.probe_image must be the orker image of this Orker's own build") {
		t.Fatalf("an override of another build = %v (%s)", reason, detail)
	}
}

// TestProbeImage_FailureDetailsAreDistinct: each default-path failure holds
// the profile with its own reason and says which it was.
func TestProbeImage_FailureDetailsAreDistinct(t *testing.T) {
	const (
		unavailable = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
		mismatch    = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	)
	for name, tc := range map[string]struct {
		resolveErr error
		verifyErr  error
		pullErr    error
		label      string
		reason     orkstackv1.ProfileCordonReason
		says       []string
	}{
		"the tag does not exist": {resolveErr: &dsl.ResolveError{Code: dsl.ReasonImageNotFound, Detail: "no manifest 0.4.0"},
			reason: unavailable, says: []string{"release image ghcr.io/orkstack/orker:0.4.0 does not exist", "set probe_image"}},
		"the registry is down": {resolveErr: &dsl.ResolveError{Code: dsl.ReasonImageResolveUnavailable, Detail: "registry answered 503", Status: 503},
			reason: unavailable, says: []string{"ghcr.io unreachable resolving ghcr.io/orkstack/orker:0.4.0", "503"}},
		"a network error": {resolveErr: errors.New("dial tcp: i/o timeout"),
			reason: unavailable, says: []string{"ghcr.io unreachable resolving", "i/o timeout"}},
		"rate-limited": {resolveErr: &dsl.ResolveError{Code: dsl.ReasonImageResolveUnsupported, Detail: "answered 429", Status: 429},
			reason: unavailable, says: []string{"ghcr.io rate-limited this host (429); retrying at"}},
		"unsigned": {verifyErr: &dsl.SignatureError{Code: task.ReasonImageNotSigned, Detail: "carries no cosign signature"},
			reason: mismatch, says: []string{"is not signed by this release's key", "set probe_image to an image you trust"}},
		"signed by another key": {verifyErr: &dsl.SignatureError{Code: task.ReasonImageSignatureInvalid, Detail: "none verifies"},
			reason: mismatch, says: []string{"is not signed by this release's key"}},
		"the signature cannot be read": {verifyErr: &dsl.SignatureError{Code: task.ReasonImageSignatureUnavailable, Detail: "registry down"},
			reason: unavailable, says: []string{"could not be checked for this release's signature", "registry down"}},
		"the check itself cannot be made": {verifyErr: &dsl.SignatureError{Code: task.ReasonImageNotPermitted, Detail: "cannot be verified: engine defect"},
			reason: unavailable, says: []string{"could not be checked for this release's signature"}},
		"credentials refused": {resolveErr: &dsl.ResolveError{Code: dsl.ReasonImageResolveUnauthorized, Detail: "answered 401", Status: 401},
			reason: unavailable, says: []string{"refused this host's credentials", RegistryAuthFileVar}},
		"the digest cannot be pulled": {pullErr: errors.New("pull failed"),
			reason: unavailable, says: []string{"could not be pulled"}},
		"the tag moved to another build": {label: "deadbeef",
			reason: mismatch, says: []string{"is not this build (commit \"deadbeef\", this Orker c0ffee)", "set probe_image to your own"}},
	} {
		t.Run(name, func(t *testing.T) {
			g := &registry{resolveErrs: []error{tc.resolveErr}, verifyErr: tc.verifyErr}
			label := tc.label
			if label == "" {
				label = "c0ffee"
			}
			rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: label, pullErr: tc.pullErr}}
			_, reason, detail, err := checkProbeImage(t.Context(), rt, g.probe(t, time.Second, nil), "c0ffee")
			if err != nil || reason != tc.reason {
				t.Fatalf("checkProbeImage = %v, %v (%s); want %v", reason, err, detail, tc.reason)
			}
			for _, s := range tc.says {
				if !strings.Contains(detail, s) {
					t.Errorf("detail %q does not say %q", detail, s)
				}
			}
			if (tc.resolveErr != nil || tc.verifyErr != nil) && len(rt.pulled) != 0 {
				t.Fatalf("pulled %v before the image was resolved and verified", rt.pulled)
			}
		})
	}
}

// TestProbeImage_ResolvedOncePerProcess: once a digest passed its signature
// and its label, it is held: no later check resolves the tag again, so a tag
// that moves cannot switch the probe.
func TestProbeImage_ResolvedOncePerProcess(t *testing.T) {
	g := &registry{}
	src := g.probe(t, time.Second, nil)
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	for range 3 {
		if _, reason, detail, err := checkProbeImage(t.Context(), rt, src, "c0ffee"); err != nil || reason != 0 {
			t.Fatalf("checkProbeImage = %v, %v (%s)", reason, err, detail)
		}
	}
	if len(g.resolved) != 1 || len(g.verified) != 1 {
		t.Fatalf("resolved %d times and verified %d; want once each", len(g.resolved), len(g.verified))
	}
	if len(rt.pulled) != 3 {
		t.Fatalf("pulled %d times; want every check to pull the held digest", len(rt.pulled))
	}
}

// TestProbeImage_UnresolvableBacksOffThenRecovers: a default-path failure is
// asked again only once its back-off has passed, and a success ends it.
func TestProbeImage_UnresolvableBacksOffThenRecovers(t *testing.T) {
	down := errors.New("dial tcp: connection refused")
	g := &registry{resolveErrs: []error{down, down}}
	clock := time.Unix(1_800_000_000, 0)
	src := g.probe(t, 5*time.Second, &clock)
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	check := func() orkstackv1.ProfileCordonReason {
		_, reason, _, err := checkProbeImage(t.Context(), rt, src, "c0ffee")
		if err != nil {
			t.Fatal(err)
		}
		return reason
	}
	if check() == 0 || len(g.resolved) != 1 {
		t.Fatalf("first check: resolved %d", len(g.resolved))
	}
	clock = clock.Add(4 * time.Second)
	if check() == 0 || len(g.resolved) != 1 {
		t.Fatalf("a check inside the back-off asked the registry: resolved %d", len(g.resolved))
	}
	clock = clock.Add(2 * time.Second) // 6 s: past the first 5 s wait
	if check() == 0 || len(g.resolved) != 2 {
		t.Fatalf("second attempt: resolved %d", len(g.resolved))
	}
	clock = clock.Add(9 * time.Second) // inside the doubled 10 s wait
	if check() == 0 || len(g.resolved) != 2 {
		t.Fatalf("a check inside the doubled back-off asked the registry: resolved %d", len(g.resolved))
	}
	clock = clock.Add(2 * time.Second)
	if reason := check(); reason != 0 || len(g.resolved) != 3 {
		t.Fatalf("after recovery: reason %v, resolved %d", reason, len(g.resolved))
	}
}

// TestProbeImage_BackoffDoublesToTheCapWithJitter: the wait doubles from
// recovery_interval within ±20 %, and never past five minutes, jitter
// included.
func TestProbeImage_BackoffDoublesToTheCapWithJitter(t *testing.T) {
	base := 5 * time.Second
	for _, tc := range []struct {
		failures int
		jitter   float64
		want     time.Duration
	}{
		{1, 0.5, 5 * time.Second},
		{2, 0.5, 10 * time.Second},
		{3, 0.5, 20 * time.Second},
		{7, 0.5, 5 * time.Minute},
		{40, 0.5, 5 * time.Minute},
		{1, 0, 4 * time.Second},
		{1, 1, 6 * time.Second},
		{40, 1, 5 * time.Minute},
		{7, 1, 5 * time.Minute},
	} {
		if got := probeBackoff(base, tc.failures, tc.jitter); got != tc.want {
			t.Errorf("probeBackoff(%s, %d, %v) = %s, want %s", base, tc.failures, tc.jitter, got, tc.want)
		}
	}
}

// TestProbeImage_OverrideDoesNotBackOff: an explicit probe_image keeps the
// fixed recovery_interval: every check asks again.
func TestProbeImage_OverrideDoesNotBackOff(t *testing.T) {
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee", pullErr: errors.New("registry down")}}
	src := newProbeImageSource(viewProbeImage, nil, time.Second)
	for range 3 {
		if _, reason, _, _ := checkProbeImage(t.Context(), rt, src, "c0ffee"); reason == 0 {
			t.Fatal("a failed pull passed")
		}
	}
	if len(rt.pulled) != 3 {
		t.Fatalf("pulled %d times; want every check to ask", len(rt.pulled))
	}
}

// TestProbeImage_AHeldDigestIsAskedAgainAtTheNextCheck: once a digest is
// held, a pull that fails holds the profile unavailable only until the next
// check, which asks again without a back-off and without resolving the tag.
func TestProbeImage_AHeldDigestIsAskedAgainAtTheNextCheck(t *testing.T) {
	g := &registry{}
	clock := time.Unix(1_800_000_000, 0)
	src := g.probe(t, 5*time.Second, &clock)
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	if _, reason, _, _ := checkProbeImage(t.Context(), rt, src, "c0ffee"); reason != 0 {
		t.Fatalf("first check = %v", reason)
	}
	rt.pullErr = errors.New("toomanyrequests")
	if _, reason, _, _ := checkProbeImage(t.Context(), rt, src, "c0ffee"); reason != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE {
		t.Fatalf("a failed pull of the held digest = %v; want PROBE_IMAGE_UNAVAILABLE", reason)
	}
	rt.pullErr = nil
	if image, reason, detail, _ := checkProbeImage(t.Context(), rt, src, "c0ffee"); reason != 0 || image != "ghcr.io/orkstack/orker@"+releaseDigest {
		t.Fatalf("the next check, at once = %q, %v (%s); want the held digest", image, reason, detail)
	}
	if len(g.resolved) != 1 || len(rt.pulled) != 3 {
		t.Fatalf("resolved %d, pulled %d; want one resolve and every check pulled", len(g.resolved), len(rt.pulled))
	}
}

// TestProbeImage_ACancelIsNoFinding: a context that ends while the registry
// is asked is an error, never a finding and never a back-off.
func TestProbeImage_ACancelIsNoFinding(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	g := &registry{}
	src := g.probe(t, time.Second, nil)
	src.release.Resolve = func(context.Context, string) (*orkstackv1.ImageRef, error) {
		cancel()
		return nil, context.Canceled
	}
	rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
	if _, reason, _, err := checkProbeImage(ctx, rt, src, "c0ffee"); err == nil || reason != 0 {
		t.Fatalf("checkProbeImage = %v, %v; want the context's end", reason, err)
	}
	if src.failures != 0 {
		t.Fatalf("a cancel counted as %d failures", src.failures)
	}
}

// TestProbeImage_ConcurrentChecksResolveOnce: the probes of one Orker share
// the source; two checks at once ask the registry once.
func TestProbeImage_ConcurrentChecksResolveOnce(t *testing.T) {
	var mu sync.Mutex
	resolves := 0
	release := make(chan struct{})
	g := &registry{}
	src := g.probe(t, time.Second, nil)
	resolve := src.release.Resolve
	src.release.Resolve = func(ctx context.Context, ref string) (*orkstackv1.ImageRef, error) {
		mu.Lock()
		resolves++
		mu.Unlock()
		<-release
		return resolve(ctx, ref)
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			rt := &pullRecorder{viewProbeRuntime: &viewProbeRuntime{label: "c0ffee"}}
			if _, reason, detail, err := checkProbeImage(t.Context(), rt, src, "c0ffee"); err != nil || reason != 0 {
				t.Errorf("checkProbeImage = %v, %v (%s)", reason, err, detail)
			}
		})
	}
	close(release)
	wg.Wait()
	if resolves != 1 {
		t.Fatalf("resolved %d times; want once", resolves)
	}
}

// TestProbeImage_ConfiguredOrNot: a source with neither probe_image nor a
// complete release probe is why Boot refuses the profiles.
func TestProbeImage_ConfiguredOrNot(t *testing.T) {
	g := &registry{}
	complete := g.probe(t, time.Second, nil)
	if err := complete.configured(); err != nil {
		t.Fatalf("a complete release probe: %v", err)
	}
	if err := newProbeImageSource(viewProbeImage, nil, time.Second).configured(); err != nil {
		t.Fatalf("an explicit probe_image: %v", err)
	}
	if err := newProbeImageSource("", nil, time.Second).configured(); err == nil ||
		!strings.Contains(err.Error(), "profiles.probe_image is required on an Orker built from source") {
		t.Fatalf("no probe image = %v", err)
	}
	if err := newProbeImageSource("", &ReleaseProbe{Ref: "x"}, time.Second).configured(); err == nil {
		t.Fatal("an incomplete release probe passed")
	}
	if NewReleaseProbe("x", "k", nil) != nil {
		t.Fatal("a release probe with no verifier was built")
	}
}

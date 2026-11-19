package orker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/buildinfo"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
)

// Where the profile probes' image comes from (docs/DEPLOY.md, "Running a
// device Orker"): what the operator pinned in profiles.probe_image, used as
// given, or, on a release build that sets none, this release's own orker
// image. The release image is resolved from its tag to a digest, its cosign
// signature verified against the release key the build carries, then pulled
// by that digest and its build label held to this Orker's commit, before it
// runs with any device attached. A digest that passed is held for the
// process's life, so a tag that moves later cannot switch the probe.

// releaseSigner is the signer the release image is verified as: the key a
// release build stamps in, under the one scheme a release signs with.
const releaseSigner = "cosign:release"

// probeBackoffCap bounds the wait between two asks of the registry for the
// release image after a failure, jitter included.
const probeBackoffCap = 5 * time.Minute

// ReleaseProbe is the default probe image of a release build: its release
// image, the key a release signs with, and how the registry is asked.
type ReleaseProbe struct {
	// Ref is the release image by tag, ghcr.io/orkstack/orker:<version>.
	Ref string
	// KeyPEM is the release's cosign public key, as deploy/cosign.pub holds it.
	KeyPEM string
	// Resolve pins Ref to the digest the registry serves for it.
	Resolve func(ctx context.Context, ref string) (*orkstackv1.ImageRef, error)
	// Verify checks image's cosign signature by signer against keyPEM.
	Verify func(ctx context.Context, image *orkstackv1.ImageRef, signer, keyPEM string) error
}

// NewReleaseProbe is the release image ref with keyPEM, asked through the
// Orker's one resolver and signature verifier. A nil verifier, or one with
// no resolver, gives none: Boot then refuses a profile with no probe_image.
func NewReleaseProbe(ref, keyPEM string, verifier *dsl.SignatureVerifier) *ReleaseProbe {
	if verifier == nil || verifier.Resolver == nil {
		return nil
	}
	return &ReleaseProbe{
		Ref:    ref,
		KeyPEM: keyPEM,
		Resolve: func(ctx context.Context, ref string) (*orkstackv1.ImageRef, error) {
			return verifier.Resolver.Resolve(ctx, "probe_image", ref)
		},
		Verify: verifier.Verify,
	}
}

// DefaultProbeImage is the release image and key a build with no
// profiles.probe_image probes with, or why it has none: a build that is not
// a release (no release version, or no stamped commit) has no release image,
// and one that carries no usable release key cannot check it. Either refusal
// asks for probe_image.
func DefaultProbeImage(version, stamped, releaseKey string) (ref, keyPEM string, err error) {
	ref, ok := buildinfo.ReleaseImage("orker", version, stamped)
	if !ok {
		return "", "", errors.New("profiles.probe_image is required on an Orker built from source (orker --version " +
			"shows no release); set it to the orker image of this build, pinned by digest")
	}
	if releaseKey == "" {
		return "", "", errors.New("this build carries no release signing key; set profiles.probe_image")
	}
	raw, err := base64.StdEncoding.DecodeString(releaseKey)
	if err != nil {
		return "", "", fmt.Errorf("this build's release signing key is not base64 (%v); set profiles.probe_image", err)
	}
	if _, err := task.ParseSigningKey(raw); err != nil {
		return "", "", fmt.Errorf("this build's release signing key is not a usable public key (%v); set profiles.probe_image", err)
	}
	return ref, string(raw), nil
}

// probeImageSource is the probe image of one Orker. It is safe for
// concurrent use; the probes of one Orker share it. mu guards its state and
// is never held across a registry or daemon call; resolving lets one caller
// at a time ask the registry, and a caller waiting for it can be cancelled.
type probeImageSource struct {
	pinned  string
	release *ReleaseProbe
	// base is recovery_interval, the release path's first wait.
	base   time.Duration
	now    func() time.Time
	jitter func() float64
	logger *slog.Logger

	resolving chan struct{}

	mu sync.Mutex
	// verified is the release image's digest once its signature passed;
	// held is it once its label passed too, kept for the process's life.
	verified string
	held     string
	// failures counts the release path's failures in a row before an image
	// was held; notBefore is when it may ask again, and last is what it
	// found then.
	failures   int
	notBefore  time.Time
	lastReason orkstackv1.ProfileCordonReason
	lastDetail string
}

// newProbeImageSource is pinned, used as given, or, when pinned is empty,
// release's image.
func newProbeImageSource(pinned string, release *ReleaseProbe, base time.Duration) *probeImageSource {
	if pinned != "" {
		release = nil
	}
	return &probeImageSource{pinned: pinned, release: release, base: base, now: time.Now, jitter: rand.Float64,
		resolving: make(chan struct{}, 1)}
}

// configured is why src cannot give the probes an image, or nil.
func (p *probeImageSource) configured() error {
	switch {
	case p == nil || (p.pinned == "" && p.release == nil):
		return errors.New("profiles.probe_image is required on an Orker built from source (orker --version " +
			"shows no release); set it to the orker image of this build, pinned by digest")
	case p.release != nil && (p.release.Ref == "" || p.release.KeyPEM == "" || p.release.Resolve == nil || p.release.Verify == nil):
		return errors.New("this Orker's release probe image is incomplete (no image, key, resolver or verifier); " +
			"set profiles.probe_image")
	}
	return nil
}

// probeBackoff is the wait after the failures-th failure in a row: base,
// doubled each time, scaled by 0.8 + 0.4 × jitter (jitter in [0, 1)) so a
// fleet does not ask in step, and never longer than probeBackoffCap.
func probeBackoff(base time.Duration, failures int, jitter float64) time.Duration {
	d := base
	for i := 1; i < failures && d < probeBackoffCap; i++ {
		d *= 2
	}
	return min(time.Duration(float64(d)*(0.8+0.4*jitter)), probeBackoffCap)
}

// checkProbeImage is the image a probe runs, judged before it runs: pulled
// by digest and its build label held to build, and, on the release path,
// first resolved and verified. A reason other than 0 holds the profile
// unavailable with its detail; only the context's own end is an error.
func checkProbeImage(ctx context.Context, rt hostViewRuntime, src *probeImageSource, build string) (string, orkstackv1.ProfileCordonReason, string, error) {
	if err := src.configured(); err != nil {
		return "", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH, err.Error(), nil
	}
	if src.release == nil {
		reason, detail, err := judgeProbeImage(ctx, rt, src.pinned, build, false)
		return src.pinned, reason, detail, err
	}
	return src.checkRelease(ctx, rt, build)
}

// checkRelease is checkProbeImage on the release path.
func (p *probeImageSource) checkRelease(ctx context.Context, rt hostViewRuntime, build string) (string, orkstackv1.ProfileCordonReason, string, error) {
	image, reason, detail, err := p.releaseImage(ctx)
	if err != nil || reason != 0 {
		return "", reason, detail, err
	}
	reason, detail, err = judgeProbeImage(ctx, rt, image, build, true)
	if err != nil {
		return "", 0, "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if reason != 0 {
		if p.held != "" {
			// A digest already held stays the probes' image: a pull or an
			// inspect that failed now is asked again at the next check, as
			// an explicit probe_image is.
			return "", reason, detail, nil
		}
		p.verified = ""
		return p.failLocked(reason, func(time.Time) string { return detail })
	}
	if p.held == "" && p.logger != nil {
		p.logger.Info("the profile probes run this release's own image, verified against its signing key",
			"image", p.release.Ref, "digest", image)
	}
	p.held, p.failures, p.notBefore = image, 0, time.Time{}
	return image, 0, "", nil
}

// releaseImage is the release image's digest to judge: the one held, the
// one verified, or one resolved from the tag and verified now. While a
// failure's wait runs, it asks nothing and answers what it found then.
func (p *probeImageSource) releaseImage(ctx context.Context) (string, orkstackv1.ProfileCordonReason, string, error) {
	if image, reason, detail, done := p.known(); done {
		return image, reason, detail, nil
	}
	select {
	case p.resolving <- struct{}{}:
	case <-ctx.Done():
		return "", 0, "", ctx.Err()
	}
	defer func() { <-p.resolving }()
	// Another caller may have resolved, or failed, while this one waited.
	if image, reason, detail, done := p.known(); done {
		return image, reason, detail, nil
	}
	const (
		unavailable = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
		mismatch    = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	)
	ref, host := p.release.Ref, registryHost(p.release.Ref)
	resolved, err := p.release.Resolve(ctx, ref)
	if err != nil {
		if ctx.Err() != nil {
			return "", 0, "", ctx.Err()
		}
		var re *dsl.ResolveError
		switch {
		case errors.As(err, &re) && re.Code == dsl.ReasonImageNotFound:
			return p.fail(unavailable, func(time.Time) string {
				return fmt.Sprintf("release image %s does not exist (not yet published?); "+
					"set probe_image to your own image to proceed", ref)
			})
		case errors.As(err, &re) && re.Status == 429:
			return p.fail(unavailable, func(at time.Time) string {
				return fmt.Sprintf("%s rate-limited this host (429); retrying at %s", host, at.UTC().Format(time.RFC3339))
			})
		case errors.As(err, &re) && re.Code == dsl.ReasonImageResolveUnauthorized:
			return p.fail(unavailable, func(time.Time) string {
				return fmt.Sprintf("%s refused this host's credentials resolving %s; check %s, or set probe_image to your own image",
					host, ref, RegistryAuthFileVar)
			})
		default:
			return p.fail(unavailable, func(time.Time) string {
				return fmt.Sprintf("%s unreachable resolving %s: %v", host, ref, err)
			})
		}
	}
	if err := p.release.Verify(ctx, resolved, releaseSigner, p.release.KeyPEM); err != nil {
		if ctx.Err() != nil {
			return "", 0, "", ctx.Err()
		}
		var se *dsl.SignatureError
		if errors.As(err, &se) && (se.Code == task.ReasonImageNotSigned || se.Code == task.ReasonImageSignatureInvalid) {
			return p.fail(mismatch, func(time.Time) string {
				return fmt.Sprintf("release image %s is not signed by this release's key (%s); "+
					"set probe_image to an image you trust", resolved.GetPinned(), se.Detail)
			})
		}
		// Not a verdict: the registry did not answer the signature read, or
		// the check itself could not be made.
		return p.fail(unavailable, func(time.Time) string {
			return fmt.Sprintf("release image %s could not be checked for this release's signature: %v", resolved.GetPinned(), err)
		})
	}
	p.mu.Lock()
	p.verified = resolved.GetPinned()
	p.mu.Unlock()
	return resolved.GetPinned(), 0, "", nil
}

// known is what the source already knows without asking: the held or
// verified digest, or the failure whose wait still runs. done is false when
// the registry must be asked.
func (p *probeImageSource) known() (string, orkstackv1.ProfileCordonReason, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.held != "":
		return p.held, 0, "", true
	case p.verified != "":
		return p.verified, 0, "", true
	case p.failures > 0 && p.now().Before(p.notBefore):
		return "", p.lastReason, p.lastDetail, true
	}
	return "", 0, "", false
}

// fail records a release-path failure and its wait, and answers it.
func (p *probeImageSource) fail(reason orkstackv1.ProfileCordonReason, detail func(retryAt time.Time) string) (string, orkstackv1.ProfileCordonReason, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failLocked(reason, detail)
}

// failLocked is fail with mu held. detail is given the instant the next ask
// may come.
func (p *probeImageSource) failLocked(reason orkstackv1.ProfileCordonReason, detail func(retryAt time.Time) string) (string, orkstackv1.ProfileCordonReason, string, error) {
	p.failures++
	p.notBefore = p.now().Add(probeBackoff(p.base, p.failures, p.jitter()))
	p.lastReason, p.lastDetail = reason, detail(p.notBefore)
	return "", p.lastReason, p.lastDetail, nil
}

// registryHost is the registry part of an image reference.
func registryHost(ref string) string {
	host, _, _ := strings.Cut(ref, "/")
	return host
}

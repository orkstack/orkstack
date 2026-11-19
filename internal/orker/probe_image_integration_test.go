//go:build integration

package orker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_ProbeImageDefaultVerifiesTheSignature is the profiles'
// default probe image path against a real registry and real cosign
// signatures: an image under this test's own registry, tagged as a release
// is, resolved to its digest, verified against a throwaway key, pulled by the
// daemon and its build label read. Unsigned, signed by another key, or of
// another build, it is refused with the reason that says so; signed by the
// key, it is the digest the probes run.
func TestIntegration_ProbeImageDefaultVerifiesTheSignature(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	registry := cosigntest.StartRegistry(t, func(format string, args ...any) { infraAbsent(t, format, args...) })
	const repo = "orkstack/orker"
	build := fmt.Sprintf("probe-default-%d", os.Getpid())
	tag := registry.Host + "/" + repo + ":0.4.0"

	dir := t.TempDir()
	must.OK(t, os.WriteFile(filepath.Join(dir, "Dockerfile"),
		[]byte("FROM "+orker.BindProbeImage+"\nLABEL "+orker.LabelBuild+"=\""+build+"\"\n"), 0o644))
	docker := func(args ...string) {
		t.Helper()
		if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
			infraAbsent(t, "docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	docker("build", "--quiet", "--provenance=false", "-t", tag, dir)
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "--force", tag).Run() })
	docker("push", "--quiet", tag)

	client := &cosigntest.Client{}
	digest, _, _, err := client.ManifestDigest(ctx, registry.Host, repo, "0.4.0")
	must.OKf(t, err, "the pushed image's digest")
	key, err := cosigntest.GenerateKey()
	must.OK(t, err)
	other, err := cosigntest.GenerateKey()
	must.OK(t, err)

	verifier := &dsl.SignatureVerifier{Resolver: dsl.NewResolver(""), AuthFileVar: orker.RegistryAuthFileVar}
	rt := orker.NewDockerRuntime()
	check := func(build string) (string, orkstackv1.ProfileCordonReason, string) {
		t.Helper()
		image, reason, detail, err := orker.CheckReleaseProbeImage(ctx, rt, orker.NewReleaseProbe(tag, key.PublicPEM, verifier), build)
		must.OKf(t, err, "CheckReleaseProbeImage")
		return image, reason, detail
	}
	mismatch := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH

	if _, reason, detail := check(build); reason != mismatch || !strings.Contains(detail, "is not signed by this release's key") {
		t.Fatalf("an unsigned image = %v (%s); want PROBE_IMAGE_MISMATCH, not signed", reason, detail)
	}
	must.OKf(t, client.Sign(ctx, other, registry.Host, repo, digest), "sign with another key")
	if _, reason, detail := check(build); reason != mismatch || !strings.Contains(detail, "is not signed by this release's key") {
		t.Fatalf("an image signed by another key = %v (%s); want PROBE_IMAGE_MISMATCH", reason, detail)
	}
	must.OKf(t, client.Unsign(ctx, registry.Host, repo, digest), "remove the other key's signature")
	must.OKf(t, client.Sign(ctx, key, registry.Host, repo, digest), "sign with the release key")
	image, reason, detail := check(build)
	if reason != 0 || image != registry.Host+"/"+repo+"@"+digest {
		t.Fatalf("a signed image of this build = %q, %v (%s); want its digest", image, reason, detail)
	}
	if _, reason, detail := check("another-build"); reason != mismatch || !strings.Contains(detail, "is not this build") {
		t.Fatalf("a signed image of another build = %v (%s); want PROBE_IMAGE_MISMATCH, not this build", reason, detail)
	}
}

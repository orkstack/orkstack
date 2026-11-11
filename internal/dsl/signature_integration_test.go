//go:build integration

package dsl_test

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// bundleRepo is the repository in the local registry this suite pushes its
// images to. Every image it pushes is its own: the config names the process
// and the instant, so the digest, and the signature tags derived from it,
// are this run's alone.
const bundleRepo = "orkstack-test/cosign-bundle"

// cosignRun is the cosign CLI of the image the release signs with, run by
// Docker on the host network (the local registry is on loopback) as this
// user, with dir at /w and the release's signing config at
// /signing-config.json.
type cosignRun struct {
	t          *testing.T
	ctx        context.Context
	image, dir string
	config     string
	transcript *strings.Builder
}

func (c *cosignRun) run(args ...string) (string, error) {
	c.t.Helper()
	argv := append([]string{"run", "--rm", "--pull", "never", "--network", "host",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-e", "HOME=/tmp", "-e", "COSIGN_PASSWORD=",
		"-v", c.dir + ":/w", "-v", c.config + ":/signing-config.json:ro", "-w", "/w", c.image}, args...)
	out, err := exec.CommandContext(c.ctx, "docker", argv...).CombinedOutput()
	fmt.Fprintf(c.transcript, "$ cosign %s\n%s\n", strings.Join(args, " "), out)
	return string(out), err
}

func (c *cosignRun) must(args ...string) string {
	c.t.Helper()
	out, err := c.run(args...)
	if err != nil {
		c.t.Fatalf("cosign %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// newCosign checks what the suite needs and returns the CLI. The registry and
// Docker are `make infra-up`'s; the image is the CI proof host's (proof-host
// fetches deploy/cosign-image.txt's pin) and is pulled here, once, on a host
// that has not pulled it yet.
func newCosign(ctx context.Context, t *testing.T, transcript *strings.Builder) *cosignRun {
	t.Helper()
	host := localcatalog.Registry()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + host + "/v2/")
	if err != nil {
		infraAbsent(t, "the local registry %s is not reachable (make infra-up): %v", host, err)
	} else {
		resp.Body.Close()
	}
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker is not on PATH: %v", err)
	}
	image, err := cosigntest.CosignImage()
	if err != nil {
		t.Fatal(err)
	}
	config, err := cosigntest.SigningConfig()
	if err != nil {
		t.Fatal(err)
	}
	if exec.CommandContext(ctx, "docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.CommandContext(ctx, "docker", "pull", "--quiet", image).CombinedOutput(); err != nil {
			infraAbsent(t, "the cosign image %s is not on the host and could not be pulled: %v: %s", image, err, out)
		}
	}
	fmt.Fprintf(transcript, "# cosign bundle interop (%s)\ncosign image  %s\nregistry      %s\n\n",
		time.Now().UTC().Format(time.RFC3339), image, host)
	return &cosignRun{t: t, ctx: ctx, image: image, dir: t.TempDir(), config: config, transcript: transcript}
}

// pushImage pushes an image of this run's own to the local registry and
// returns the ImageRef the engine would pin for it.
func pushImage(ctx context.Context, t *testing.T, label string) *orkstackv1.ImageRef {
	t.Helper()
	host := localcatalog.Registry()
	client := &cosigntest.Client{}
	token := fmt.Sprintf("%s-%d-%d", label, os.Getpid(), time.Now().UnixNano())
	layer := []byte("orkstack cosign bundle interop " + token)
	config, _ := stablejson.Marshal(map[string]any{
		"architecture": "amd64", "os": "linux",
		"config": map[string]any{"Labels": map[string]string{"org.orkstack.test": token}},
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{cosigntest.Digest(layer)}},
	})
	manifest, _ := stablejson.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     cosigntest.OCIManifestType,
		"config":        map[string]any{"mediaType": cosigntest.OCIConfigType, "size": len(config), "digest": cosigntest.Digest(config)},
		"layers": []map[string]any{{"mediaType": "application/vnd.oci.image.layer.v1.tar",
			"size": len(layer), "digest": cosigntest.Digest(layer)}},
	})
	// The registry answered newCosign's probe, so a push it refuses fails.
	for _, blob := range [][]byte{config, layer} {
		if err := client.PushBlob(ctx, host, bundleRepo, blob); err != nil {
			t.Fatalf("push a blob to %s: %v", host, err)
		}
	}
	if err := client.PutManifest(ctx, host, bundleRepo, "t-"+token, cosigntest.OCIManifestType, manifest); err != nil {
		t.Fatalf("push the image to %s: %v", host, err)
	}
	digest := cosigntest.Digest(manifest)
	return &orkstackv1.ImageRef{
		Reference: host + "/" + bundleRepo + ":t-" + token, Registry: host, Repository: bundleRepo,
		IndexDigest: digest, Pinned: host + "/" + bundleRepo + "@" + digest,
	}
}

// getManifest reads one manifest of the suite's repository: its body, or nil
// for a 404.
func getManifest(ctx context.Context, t *testing.T, ref string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+localcatalog.Registry()+"/v2/"+bundleRepo+"/manifests/"+ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", cosigntest.OCIIndexType+", "+cosigntest.OCIManifestType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return body
	case http.StatusNotFound:
		return nil
	default:
		t.Fatalf("GET manifest %s: %d %s", ref, resp.StatusCode, body)
		return nil
	}
}

func getBlob(ctx context.Context, t *testing.T, digest string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+localcatalog.Registry()+"/v2/"+bundleRepo+"/blobs/"+digest, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET blob %s: %d %v", digest, resp.StatusCode, err)
	}
	return body
}

// redescribe rewrites the first descriptor of doc's list field to name
// digest and size: the referrer manifest's bundle layer, or the referrers
// index's one manifest.
func redescribe(t *testing.T, doc []byte, field, digest string, size int) []byte {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal(doc, &parsed); err != nil {
		t.Fatal(err)
	}
	list, _ := parsed[field].([]any)
	if len(list) == 0 {
		t.Fatalf("%s has no %s", doc, field)
	}
	first, _ := list[0].(map[string]any)
	first["digest"], first["size"] = digest, size
	out, err := stablejson.Marshal(parsed)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func verify(t *testing.T, image *orkstackv1.ImageRef, keyPath string) error {
	t.Helper()
	pub, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	v := &dsl.SignatureVerifier{Resolver: dsl.NewResolver("")}
	return v.Verify(context.Background(), image, "cosign:"+strings.TrimSuffix(filepath.Base(keyPath), ".pub"), string(pub))
}

func requireCode(t *testing.T, err error, code string) *dsl.SignatureError {
	t.Helper()
	var sig *dsl.SignatureError
	if !errors.As(err, &sig) || sig.Code != code {
		t.Fatalf("Verify = %v, want %s", err, code)
	}
	return sig
}

// TestIntegration_ACosignBundleFromThePinnedImageVerifies: cosign 3, from the
// image the release signs with (deploy/cosign-image.txt), signs an image in
// the local registry by key under the release's signing config
// (deploy/cosign-signing-config.json), with no deprecated flag. It writes
// the bundle layout and no legacy .sig, and records nothing in a
// transparency log. OrkStack's verifier accepts that bundle with the key
// and refuses it with another; a bundle whose statement was changed under
// its signature is refused by OrkStack and by cosign alike. An image signed
// as v0.1.0's were (cosign 3 with the legacy-layout flags) still verifies,
// and an unsigned one is IMAGE_NOT_SIGNED.
func TestIntegration_ACosignBundleFromThePinnedImageVerifies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var transcript strings.Builder
	cos := newCosign(ctx, t, &transcript)
	defer func() { proof.Write(t, "dsl", "cosign-bundle-interop.txt", []byte(transcript.String())) }()
	note := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		t.Log(line)
		fmt.Fprintf(&transcript, "# %s\n", line)
	}
	cos.must("generate-key-pair", "--output-key-prefix", "release")
	cos.must("generate-key-pair", "--output-key-prefix", "other")
	release, other := filepath.Join(cos.dir, "release.pub"), filepath.Join(cos.dir, "other.pub")

	image := pushImage(ctx, t, "bundle")
	out := cos.must("sign", "--key", "release.key", "--signing-config", "/signing-config.json",
		"--allow-insecure-registry", "--yes", image.GetPinned())
	if strings.Contains(out, "deprecated") || strings.Contains(strings.ToLower(out), "tlog entry created") {
		t.Fatalf("cosign sign used a deprecated flag or wrote to a transparency log:\n%s", out)
	}

	// cosign wrote the bundle layout, as cosigntest lays it out, and no .sig.
	if sig := getManifest(ctx, t, cosigntest.SignatureTag(image.GetIndexDigest())); sig != nil {
		t.Fatalf("cosign 3 wrote a legacy %s: %s", cosigntest.SignatureTag(image.GetIndexDigest()), sig)
	}
	indexBody := getManifest(ctx, t, cosigntest.ReferrersTag(image.GetIndexDigest()))
	var index struct {
		Manifests []struct {
			Digest       string            `json:"digest"`
			ArtifactType string            `json:"artifactType"`
			Annotations  map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(indexBody, &index); err != nil || len(index.Manifests) != 1 ||
		index.Manifests[0].ArtifactType != cosigntest.BundleType ||
		index.Manifests[0].Annotations[cosigntest.BundlePredicateAnnotation] != cosigntest.SignPredicateType {
		t.Fatalf("the referrers tag holds %s (%v), want one bundle referrer", indexBody, err)
	}
	var referrer struct {
		ArtifactType string `json:"artifactType"`
		Layers       []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
		} `json:"layers"`
		Subject struct {
			Digest string `json:"digest"`
		} `json:"subject"`
	}
	referrerBody := getManifest(ctx, t, index.Manifests[0].Digest)
	if err := json.Unmarshal(referrerBody, &referrer); err != nil || referrer.ArtifactType != cosigntest.BundleType ||
		len(referrer.Layers) != 1 || referrer.Layers[0].MediaType != cosigntest.BundleType ||
		referrer.Subject.Digest != image.GetIndexDigest() {
		t.Fatalf("the referrer manifest is %s (%v), not the layout cosigntest writes", referrerBody, err)
	}
	bundle := getBlob(ctx, t, referrer.Layers[0].Digest)
	var parsed map[string]any
	if err := json.Unmarshal(bundle, &parsed); err != nil || parsed["mediaType"] != cosigntest.BundleType {
		t.Fatalf("the bundle is %s (%v)", bundle, err)
	}
	// The check release.yml, the smoke and the setup script make with jq:
	// (.verificationMaterial.tlogEntries // []) | length == 0.
	material, _ := parsed["verificationMaterial"].(map[string]any)
	if entries, isList := material["tlogEntries"].([]any); material["tlogEntries"] != nil && (!isList || len(entries) > 0) {
		t.Fatalf("the bundle carries transparency-log entries: %s", bundle)
	}
	note("cosign signed %s: bundle %s, no .sig, no transparency-log entry", image.GetPinned(), referrer.Layers[0].Digest)

	if err := verify(t, image, release); err != nil {
		t.Fatalf("OrkStack's verifier refuses cosign's bundle by the signing key: %v", err)
	}
	note("OrkStack's verifier accepts it with the signing key")
	sig := requireCode(t, verify(t, image, other), task.ReasonImageSignatureInvalid)
	if !strings.Contains(sig.Detail, "has 1 cosign signature(s)") {
		t.Fatalf("detail = %s", sig.Detail)
	}
	note("and refuses it with another key: %s", sig.Error())
	cos.must("verify", "--key", "release.pub", "--insecure-ignore-tlog=true", "--allow-insecure-registry", image.GetPinned())
	if out, err := cos.run("verify", "--key", "other.pub", "--insecure-ignore-tlog=true", "--allow-insecure-registry", image.GetPinned()); err == nil {
		t.Fatalf("cosign verify accepted the bundle with another key:\n%s", out)
	}

	// Tamper: the statement cosign signed, one byte longer, under the
	// signature cosign made over the original. It still names this image;
	// only the signature no longer holds. The referrers tag lists it alone.
	envelope, _ := parsed["dsseEnvelope"].(map[string]any)
	payload, _ := envelope["payload"].(string)
	signed, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	envelope["payload"] = base64.StdEncoding.EncodeToString(append(signed, ' '))
	tampered, _ := stablejson.Marshal(parsed)
	client := &cosigntest.Client{}
	host := localcatalog.Registry()
	if err := client.PushBlob(ctx, host, bundleRepo, tampered); err != nil {
		t.Fatal(err)
	}
	tamperedManifest := redescribe(t, referrerBody, "layers", cosigntest.Digest(tampered), len(tampered))
	if err := client.PutManifest(ctx, host, bundleRepo, cosigntest.Digest(tamperedManifest), cosigntest.OCIManifestType,
		tamperedManifest); err != nil {
		t.Fatal(err)
	}
	tamperedIndex := redescribe(t, indexBody, "manifests", cosigntest.Digest(tamperedManifest), len(tamperedManifest))
	if err := client.PutManifest(ctx, host, bundleRepo, cosigntest.ReferrersTag(image.GetIndexDigest()), cosigntest.OCIIndexType,
		tamperedIndex); err != nil {
		t.Fatal(err)
	}
	sig = requireCode(t, verify(t, image, release), task.ReasonImageSignatureInvalid)
	note("a statement changed under cosign's signature is refused: %s", sig.Error())
	if out, err := cos.run("verify", "--key", "release.pub", "--insecure-ignore-tlog=true", "--allow-insecure-registry", image.GetPinned()); err == nil {
		t.Fatalf("cosign verify accepted the tampered bundle:\n%s", out)
	}
	note("and cosign verify refuses it too")

	// v0.1.0's images: cosign 3 with the flags that write the legacy layout.
	legacy := pushImage(ctx, t, "legacy")
	requireCode(t, verify(t, legacy, release), task.ReasonImageNotSigned)
	note("an unsigned image is %s", task.ReasonImageNotSigned)
	cos.must("sign", "--key", "release.key", "--new-bundle-format=false", "--use-signing-config=false", "--tlog-upload=false",
		"--allow-insecure-registry", "--yes", legacy.GetPinned())
	if getManifest(ctx, t, cosigntest.SignatureTag(legacy.GetIndexDigest())) == nil {
		t.Fatalf("cosign's legacy flags wrote no %s", cosigntest.SignatureTag(legacy.GetIndexDigest()))
	}
	if err := verify(t, legacy, release); err != nil {
		t.Fatalf("an image signed as v0.1.0's were no longer verifies: %v", err)
	}
	note("an image signed in the legacy layout, as v0.1.0's were, still verifies")
}

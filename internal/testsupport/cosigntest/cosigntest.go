// Package cosigntest writes and serves cosign key-based signatures the way
// `cosign sign --key` lays them out, for the suites that prove
// internal/dsl.SignatureVerifier and for test/tools/cosign-sign.
//
// It is TEST TOOLING. No binary under cmd/ links it: OrkStack never signs
// anything and never holds a private key (image-supply-chain-policy §
// Non-goals). It exists as a package rather than a test helper because four
// suites (internal/dsl, internal/api, internal/orker, cmd/orkctl) and one
// tool need the same layout, and one copy of a layout is the point — the
// integration suite proves this copy is what the real `cosign` CLI writes and
// reads.
package cosigntest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// The layout's constants, restated from internal/dsl/signature.go on purpose:
// a test tool that imported the verifier's own constants could never catch
// the verifier drifting from cosign.
const (
	SignatureAnnotation   = "dev.cosignproject.cosign/signature"
	SimpleSigningType     = "application/vnd.dev.cosign.simplesigning.v1+json"
	OCIManifestType       = "application/vnd.oci.image.manifest.v1+json"
	OCIConfigType         = "application/vnd.oci.image.config.v1+json"
	CosignSignatureType   = "cosign container image signature"
	signatureTagSuffix    = ".sig"
	manifestAcceptHeaders = "application/vnd.oci.image.index.v1+json, " +
		"application/vnd.docker.distribution.manifest.list.v2+json, " +
		"application/vnd.oci.image.manifest.v1+json, " +
		"application/vnd.docker.distribution.manifest.v2+json"
)

// Key is one cosign key pair: the private key signs, PublicPEM is what
// `cosign generate-key-pair` writes to cosign.pub.
type Key struct {
	Private    *ecdsa.PrivateKey
	PublicPEM  string
	PrivatePEM string
}

// GenerateKey makes a P-256 key pair.
func GenerateKey() (*Key, error) {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return keyOf(private)
}

func keyOf(private *ecdsa.PrivateKey) (*Key, error) {
	der, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
	if err != nil {
		return nil, err
	}
	privDER, err := x509.MarshalECPrivateKey(private)
	if err != nil {
		return nil, err
	}
	return &Key{
		Private:    private,
		PublicPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
		PrivatePEM: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})),
	}, nil
}

// ParsePrivateKey reads an unencrypted PEM "EC PRIVATE KEY" (what GenerateKey
// writes; cosign's own encrypted key format is not read here).
func ParsePrivateKey(data []byte) (*Key, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "EC PRIVATE KEY" {
		return nil, errors.New("not a PEM \"EC PRIVATE KEY\" block")
	}
	private, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return keyOf(private)
}

// Payload is the simple-signing payload cosign signs for one digest.
func Payload(digest, dockerReference string) []byte {
	body, _ := json.Marshal(map[string]any{
		"critical": map[string]any{
			"identity": map[string]string{"docker-reference": dockerReference},
			"image":    map[string]string{"docker-manifest-digest": digest},
			"type":     CosignSignatureType,
		},
		"optional": nil,
	})
	return body
}

// Sign returns base64(DER ECDSA over sha256(payload)), the annotation value.
func (k *Key) Sign(payload []byte) (string, error) {
	sum := sha256.Sum256(payload)
	sig, err := ecdsa.SignASN1(rand.Reader, k.Private, sum[:])
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// Layer is one signature: its payload blob and its annotation.
type Layer struct {
	Payload   []byte
	Signature string
}

// SignatureLayer builds the layer key signs over digest.
func (k *Key) SignatureLayer(digest, dockerReference string) (Layer, error) {
	payload := Payload(digest, dockerReference)
	sig, err := k.Sign(payload)
	if err != nil {
		return Layer{}, err
	}
	return Layer{Payload: payload, Signature: sig}, nil
}

// Digest is "sha256:<hex>" of b.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// SignatureTag is the tag cosign stores an image digest's signatures under.
func SignatureTag(digest string) string {
	return "sha256-" + strings.TrimPrefix(digest, "sha256:") + signatureTagSuffix
}

// configBlob is the (empty-ish) OCI config the signature manifest names.
var configBlob = []byte(`{"architecture":"","config":{},"created":"0001-01-01T00:00:00Z","history":[{"created":"0001-01-01T00:00:00Z"}],"os":"","rootfs":{"type":"layers","diff_ids":[]}}`)

// Manifest builds the signature object's OCI image manifest.
func Manifest(layers []Layer) []byte {
	type descriptor struct {
		MediaType   string            `json:"mediaType"`
		Size        int               `json:"size"`
		Digest      string            `json:"digest"`
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	m := struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Config        descriptor   `json:"config"`
		Layers        []descriptor `json:"layers"`
	}{
		SchemaVersion: 2,
		MediaType:     OCIManifestType,
		Config:        descriptor{MediaType: OCIConfigType, Size: len(configBlob), Digest: Digest(configBlob)},
	}
	for _, l := range layers {
		m.Layers = append(m.Layers, descriptor{
			MediaType: SimpleSigningType, Size: len(l.Payload), Digest: Digest(l.Payload),
			Annotations: map[string]string{SignatureAnnotation: l.Signature},
		})
	}
	body, _ := json.Marshal(m)
	return body
}

// Client speaks the distribution API to a real, anonymous, plain-HTTP
// registry — the local one `make infra-up` starts, or a private one a test
// starts. It writes exactly the layout the verifier reads.
type Client struct {
	HTTP *http.Client
}

func (c *Client) http() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// ManifestDigest resolves host/repo:ref to the digest of the object the
// registry serves for it (index-first, as the engine's resolver asks).
func (c *Client) ManifestDigest(ctx context.Context, host, repo, ref string) (string, []byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s/v2/%s/manifests/%s", host, repo, ref), nil)
	if err != nil {
		return "", nil, "", err
	}
	req.Header.Set("Accept", manifestAcceptHeaders)
	resp, err := c.http().Do(req)
	if err != nil {
		return "", nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", nil, "", fmt.Errorf("GET manifest %s/%s:%s: %d %s", host, repo, ref, resp.StatusCode, body)
	}
	return Digest(body), body, resp.Header.Get("Content-Type"), nil
}

// PushBlob uploads one blob (monolithic POST-then-PUT upload).
func (c *Client) PushBlob(ctx context.Context, host, repo string, blob []byte) error {
	digest := Digest(blob)
	head, err := http.NewRequestWithContext(ctx, http.MethodHead,
		fmt.Sprintf("http://%s/v2/%s/blobs/%s", host, repo, digest), nil)
	if err != nil {
		return err
	}
	if resp, err := c.http().Do(head); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
	}
	start, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/v2/%s/blobs/uploads/", host, repo), nil)
	if err != nil {
		return err
	}
	resp, err := c.http().Do(start)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("start blob upload to %s/%s: %d", host, repo, resp.StatusCode)
	}
	location, err := resp.Location()
	if err != nil {
		return fmt.Errorf("start blob upload: %w", err)
	}
	query := location.Query()
	query.Set("digest", digest)
	location.RawQuery = query.Encode()
	put, err := http.NewRequestWithContext(ctx, http.MethodPut, location.String(), bytes.NewReader(blob))
	if err != nil {
		return err
	}
	put.Header.Set("Content-Type", "application/octet-stream")
	put.ContentLength = int64(len(blob))
	resp, err = c.http().Do(put)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("finish blob upload %s to %s/%s: %d %s", digest, host, repo, resp.StatusCode, body)
	}
	return nil
}

// PutManifest stores a manifest under ref (a tag or a digest).
func (c *Client) PutManifest(ctx context.Context, host, repo, ref, mediaType string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("http://%s/v2/%s/manifests/%s", host, repo, ref), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mediaType)
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("PUT manifest %s/%s:%s: %d %s", host, repo, ref, resp.StatusCode, out)
	}
	return nil
}

// PushSignature writes layers as the signature object of host/repo@digest,
// replacing any signature object already at that tag.
func (c *Client) PushSignature(ctx context.Context, host, repo, digest string, layers []Layer) error {
	if err := c.PushBlob(ctx, host, repo, configBlob); err != nil {
		return err
	}
	for _, l := range layers {
		if err := c.PushBlob(ctx, host, repo, l.Payload); err != nil {
			return err
		}
	}
	return c.PutManifest(ctx, host, repo, SignatureTag(digest), OCIManifestType, Manifest(layers))
}

// Sign signs host/repo@digest with key, as `cosign sign --key` does.
func (c *Client) Sign(ctx context.Context, key *Key, host, repo, digest string) error {
	layer, err := key.SignatureLayer(digest, host+"/"+repo)
	if err != nil {
		return err
	}
	return c.PushSignature(ctx, host, repo, digest, []Layer{layer})
}

// Unsign removes host/repo@digest's signature object, so the .sig tag answers
// 404. The registry must have deletion enabled
// (REGISTRY_STORAGE_DELETE_ENABLED=true); a registry without it answers 405
// and this returns that as an error rather than pretending.
func (c *Client) Unsign(ctx context.Context, host, repo, digest string) error {
	sigDigest, _, _, err := c.ManifestDigest(ctx, host, repo, SignatureTag(digest))
	if err != nil {
		return fmt.Errorf("unsign: read the signature object: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("http://%s/v2/%s/manifests/%s", host, repo, sigDigest), nil)
	if err != nil {
		return err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unsign %s/%s@%s: DELETE answered %d %s", host, repo, sigDigest, resp.StatusCode, body)
	}
	return nil
}

// CopyImage copies srcHost/repo:ref (an index or a single manifest, with every
// child manifest and blob) to dstHost/repo:ref, preserving its digest.
func (c *Client) CopyImage(ctx context.Context, srcHost, dstHost, repo, ref string) (string, error) {
	digest, body, mediaType, err := c.ManifestDigest(ctx, srcHost, repo, ref)
	if err != nil {
		return "", err
	}
	if err := c.copyManifest(ctx, srcHost, dstHost, repo, body, mediaType); err != nil {
		return "", err
	}
	if err := c.PutManifest(ctx, dstHost, repo, ref, mediaType, body); err != nil {
		return "", err
	}
	return digest, nil
}

func (c *Client) copyManifest(ctx context.Context, srcHost, dstHost, repo string, body []byte, mediaType string) error {
	var doc struct {
		Manifests []struct {
			Digest    string `json:"digest"`
			MediaType string `json:"mediaType"`
		} `json:"manifests"`
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return err
	}
	for _, child := range doc.Manifests {
		_, childBody, childType, err := c.ManifestDigest(ctx, srcHost, repo, child.Digest)
		if err != nil {
			return err
		}
		if err := c.copyManifest(ctx, srcHost, dstHost, repo, childBody, childType); err != nil {
			return err
		}
		if err := c.PutManifest(ctx, dstHost, repo, child.Digest, childType, childBody); err != nil {
			return err
		}
	}
	blobs := []string{}
	if doc.Config.Digest != "" {
		blobs = append(blobs, doc.Config.Digest)
	}
	for _, l := range doc.Layers {
		blobs = append(blobs, l.Digest)
	}
	for _, digest := range blobs {
		blob, err := c.getBlob(ctx, srcHost, repo, digest)
		if err != nil {
			return err
		}
		if err := c.PushBlob(ctx, dstHost, repo, blob); err != nil {
			return err
		}
	}
	_ = mediaType
	return nil
}

func (c *Client) getBlob(ctx context.Context, host, repo, digest string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s/v2/%s/blobs/%s", host, repo, digest), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET blob %s/%s@%s: %d", host, repo, digest, resp.StatusCode)
	}
	return body, nil
}

// SplitReference splits "host/repo:tag" or "host/repo@digest" into its parts.
// The host is the first segment; this is test tooling for loopback
// registries, not a general parser.
func SplitReference(ref string) (host, repo, tagOrDigest string, err error) {
	host, rest, ok := strings.Cut(ref, "/")
	if !ok || rest == "" {
		return "", "", "", fmt.Errorf("reference %q has no host/repository", ref)
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		return host, rest[:at], rest[at+1:], nil
	}
	if colon := strings.LastIndex(rest, ":"); colon >= 0 {
		return host, rest[:colon], rest[colon+1:], nil
	}
	return host, rest, "latest", nil
}

// hostOf strips a scheme from an httptest URL.
func hostOf(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Host
	}
	return raw
}

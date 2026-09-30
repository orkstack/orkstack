package dsl

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// Cosign signature verification (image-supply-chain-policy §4).
//
// THERE IS EXACTLY ONE RESOLVER, and that extends to signatures: the verifier
// is built over Resolver, reuses its bearer/basic authentication and its
// loopback-only plain-HTTP rule, and is the one implementation the engine
// (apply, validate, submit) and the Orker (dispatch) share. It is one fixed
// scheme — key-based cosign over the simple-signing payload, ECDSA P-256 /
// SHA-256 — with no interface behind it, no keyless mode and no sigstore
// dependency (AGENTS.md §13: not a plugin framework).

// Cosign's key-based signature layout, as `cosign sign --key` writes it with
// default settings: an OCI image manifest at the tag "sha256-<hex>.sig" in
// the SAME repository, one layer per signature, each layer's blob the
// simple-signing payload and its annotation the signature.
const (
	CosignSignatureTagSuffix     = ".sig"
	CosignSignatureAnnotation    = "dev.cosignproject.cosign/signature"
	MediaTypeCosignSimpleSigning = "application/vnd.dev.cosign.simplesigning.v1+json"
	// CosignSignatureType is critical.type in the simple-signing payload.
	CosignSignatureType         = "cosign container image signature"
	maxSignatureLayers          = 16
	maxSignaturePayloadBytes    = 1 << 20
	defaultSignatureAuthFileVar = "ORK_REGISTRY_AUTH_FILE"
)

// SignatureError is a verification that did not pass, carrying the
// task.ReasonImage* code that says why.
type SignatureError struct {
	Image  string // ImageRef.pinned
	Signer string // "cosign:<name>"
	Code   string
	Detail string
	Err    error
}

// Error renders "image <pinned> <detail>": the sentence up to the point where
// a caller adds the namespace clause it alone knows.
func (e *SignatureError) Error() string {
	return fmt.Sprintf("image %s %s", e.Image, e.Detail)
}

// Unwrap exposes the transport error, when there was one.
func (e *SignatureError) Unwrap() error { return e.Err }

// SignatureVerifier verifies a pinned image's cosign signature against one
// key. It is safe for concurrent use.
type SignatureVerifier struct {
	Resolver *Resolver
	// AuthFileVar names the setting an unauthorized refusal points at: the
	// engine's ORK_REGISTRY_AUTH_FILE (the default) or the Orker's
	// ORK_ORKER_REGISTRY_AUTH_FILE. It changes the sentence and nothing else.
	AuthFileVar string
}

func (v *SignatureVerifier) authFileVar() string {
	if v.AuthFileVar != "" {
		return v.AuthFileVar
	}
	return defaultSignatureAuthFileVar
}

// cosignManifest is the part of the signature object's manifest verification
// reads.
type cosignManifest struct {
	MediaType string `json:"mediaType"`
	Layers    []struct {
		MediaType   string            `json:"mediaType"`
		Digest      string            `json:"digest"`
		Size        int64             `json:"size"`
		Annotations map[string]string `json:"annotations"`
	} `json:"layers"`
}

// simpleSigning is the part of the simple-signing payload verification reads.
type simpleSigning struct {
	Critical struct {
		Identity struct {
			DockerReference string `json:"docker-reference"`
		} `json:"identity"`
		Image struct {
			DockerManifestDigest string `json:"docker-manifest-digest"`
		} `json:"image"`
		Type string `json:"type"`
	} `json:"critical"`
}

// Verify fetches the signature manifest for image.index_digest from
// image.registry/image.repository and accepts the image when AT LEAST ONE
// layer verifies:
//
//  1. GET /v2/<repository>/manifests/sha256-<hex>.sig through
//     Resolver.getManifest. 404 → IMAGE_NOT_SIGNED. 401/403 after the
//     token dance, 5xx, or no answer → IMAGE_SIGNATURE_UNAVAILABLE. The
//     body must be an OCI image manifest (or Docker v2 manifest); anything
//     else → IMAGE_SIGNATURE_INVALID.
//  2. For each of at most maxSignatureLayers layers, in manifest order:
//     GET /v2/<repository>/blobs/<layer.digest> (Resolver.getSignatureBlob, the same
//     auth flow, bounded at maxSignaturePayloadBytes); sha256(body) must
//     equal layer.digest; the annotation dev.cosignproject.cosign/signature
//     is base64 of a DER ECDSA signature; ecdsa.VerifyASN1(key,
//     sha256(body), sig) must hold; the body must be JSON whose
//     critical.image.docker-manifest-digest equals image.index_digest and
//     whose critical.type is "cosign container image signature". A layer
//     failing any step is skipped and counted.
//  3. No layer verified → IMAGE_SIGNATURE_INVALID naming the count. A blob
//     that could not be fetched (not a mismatch — a transport failure) →
//     IMAGE_SIGNATURE_UNAVAILABLE, never a skip: an unreachable registry is
//     never read as "no valid signature" and never as "signed".
//
// The payload's critical.identity.docker-reference is NOT required to
// match: the signature binds the KEY HOLDER to the DIGEST, which is the claim
// the policy makes, and cosign's own key-based verify checks the digest
// claim only.
func (v *SignatureVerifier) Verify(ctx context.Context, image *orkstackv1.ImageRef, signer string, keyPEM string) error {
	pinned := image.GetPinned()
	defect := func(err error) error {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageNotPermitted,
			Detail: fmt.Sprintf("cannot be verified: the run's image policy names signer %q with no usable key; "+
				"this is an engine defect, not a workload error", signer),
			Err: err}
	}
	_, keyName, err := task.ParseSigner(signer)
	if err != nil || keyName == "" {
		if err == nil {
			err = errors.New("empty signer")
		}
		return defect(err)
	}
	key, err := task.ParseSigningKey([]byte(keyPEM))
	if err != nil {
		return defect(err)
	}
	digest := image.GetIndexDigest()
	if digest == "" {
		if _, after, ok := strings.Cut(pinned, "@"); ok {
			digest = after
		}
	}
	if !ValidDigest(digest) || image.GetRegistry() == "" || image.GetRepository() == "" {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageNotPermitted,
			Detail: fmt.Sprintf("cannot be verified: it carries no registry, repository and sha256 digest "+
				"(digest %q); this is an engine defect, not a workload error", digest)}
	}
	if v == nil || v.Resolver == nil {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageSignatureUnavailable,
			Detail: "could not be checked for a cosign signature: no registry client is configured; " +
				"this is a registry problem, not a verdict"}
	}

	sigTag := "sha256-" + strings.TrimPrefix(digest, "sha256:") + CosignSignatureTagSuffix
	ref := Reference{
		Original:   image.GetRegistry() + "/" + image.GetRepository() + ":" + sigTag,
		Registry:   image.GetRegistry(),
		Repository: image.GetRepository(),
		Tag:        sigTag,
	}
	unavailable := func(what string, err error) error {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageSignatureUnavailable,
			Detail: fmt.Sprintf("could not be checked for a cosign signature: %s; "+
				"this is a registry problem, not a verdict", what), Err: err}
	}

	var authorization string
	body, contentType, _, err := v.Resolver.getManifest(ctx, ref, ref.Target(), &authorization)
	if err != nil {
		var resolve *ResolveError
		if errors.As(err, &resolve) && resolve.Code == ReasonImageNotFound {
			return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageNotSigned,
				Detail: fmt.Sprintf("carries no cosign signature (%s is not in the registry)", ref.Original),
				Err:    err}
		}
		return unavailable(v.transportSentence(ref, err), err)
	}
	var manifest cosignManifest
	mediaType := mediaTypeOf(contentType)
	if jsonErr := json.Unmarshal(body, &manifest); jsonErr != nil ||
		(mediaType != MediaTypeOCIManifest && mediaType != MediaTypeDockerManifest &&
			manifest.MediaType != MediaTypeOCIManifest && manifest.MediaType != MediaTypeDockerManifest) {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageSignatureInvalid,
			Detail: fmt.Sprintf("has a signature object %s that is not an image manifest (%s)",
				ref.Original, contentType), Err: jsonErr}
	}

	count := len(manifest.Layers)
	for i, layer := range manifest.Layers {
		if i >= maxSignatureLayers {
			break
		}
		if !ValidDigest(layer.Digest) || layer.Size > maxSignaturePayloadBytes {
			continue
		}
		payload, err := v.Resolver.getSignatureBlob(ctx, ref, layer.Digest)
		if err != nil {
			var resolve *ResolveError
			switch {
			case errors.Is(err, errBlobTooLarge):
				continue
			case errors.As(err, &resolve) && resolve.Code == ReasonImageNotFound:
				continue
			default:
				return unavailable(v.transportSentence(ref, err), err)
			}
		}
		if verifiesLayer(key, digest, layer.Digest, layer.Annotations[CosignSignatureAnnotation], payload) {
			return nil
		}
	}
	return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageSignatureInvalid,
		Detail: fmt.Sprintf("has %d cosign signature(s) and none verifies with key %q (%s) over this digest",
			count, keyName, task.SigningKeyFingerprint(key))}
}

// verifiesLayer is step 2 for one layer: content address, signature, claim.
func verifiesLayer(key *ecdsa.PublicKey, imageDigest, layerDigest, annotation string, payload []byte) bool {
	sum := sha256.Sum256(payload)
	if "sha256:"+hex.EncodeToString(sum[:]) != layerDigest {
		return false
	}
	signature, err := base64.StdEncoding.DecodeString(annotation)
	if err != nil || len(signature) == 0 {
		return false
	}
	if !ecdsa.VerifyASN1(key, sum[:], signature) {
		return false
	}
	var claim simpleSigning
	if err := json.Unmarshal(payload, &claim); err != nil {
		return false
	}
	return claim.Critical.Type == CosignSignatureType &&
		claim.Critical.Image.DockerManifestDigest == imageDigest
}

// transportSentence says why the registry could not be asked, in the words
// an operator acts on: which host, which status, which setting.
func (v *SignatureVerifier) transportSentence(ref Reference, err error) string {
	var resolve *ResolveError
	if errors.As(err, &resolve) {
		switch resolve.Code {
		case ReasonImageResolveUnauthorized:
			return fmt.Sprintf("registry %s refused the signature read (%s); set %s to a docker "+
				"config.json with credentials for %s", ref.APIHost(), resolve.Detail, v.authFileVar(), ref.Registry)
		case ReasonImageResolveUnavailable:
			if resolve.Err != nil {
				return fmt.Sprintf("registry %s could not be reached: %v", ref.APIHost(), resolve.Err)
			}
			return resolve.Detail
		default:
			return resolve.Detail
		}
	}
	return fmt.Sprintf("registry %s: %v", ref.APIHost(), err)
}

// errBlobTooLarge is a signature payload over maxSignaturePayloadBytes: the
// layer is refused (skipped), never read whole.
var errBlobTooLarge = errors.New("signature payload exceeds the size bound")

// getSignatureBlob fetches one signature blob of ref's repository: GET
// {scheme}://{apiHost}/v2/{repository}/blobs/{digest}, with the status
// handling getManifest has and one bearer-token round trip, following ONE
// redirect (registries serve blobs from a CDN with a 307) without forwarding
// credentials to the redirect's host, with the body bounded at
// maxSignaturePayloadBytes.
func (r *Resolver) getSignatureBlob(ctx context.Context, ref Reference, digest string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s://%s/v2/%s/blobs/%s", ref.Scheme(), ref.APIHost(), ref.Repository, digest)
	noFollow := *r.client()
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	limit := int64(maxSignaturePayloadBytes) + 1

	resp, body, err := r.doWith(ctx, &noFollow, endpoint, "", "", limit)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		if challenge := parseBearerChallenge(resp.Header.Get("WWW-Authenticate")); challenge != nil {
			token, tokenErr := r.fetchToken(ctx, ref, challenge)
			if tokenErr != nil {
				return nil, tokenErr
			}
			if token != "" {
				resp, body, err = r.doWith(ctx, &noFollow, endpoint, "Bearer "+token, "", limit)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location, locErr := resp.Location()
		if locErr != nil {
			return nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnsupported,
				Detail: fmt.Sprintf("registry %s redirected the blob read without a usable Location", ref.APIHost()),
				Err:    locErr}
		}
		resp, body, err = r.getRedirected(ctx, &noFollow, location, limit)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			return nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
				Detail: fmt.Sprintf("registry %s redirected the blob read more than once", ref.APIHost())}
		}
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		if int64(len(body)) > maxSignaturePayloadBytes {
			return nil, errBlobTooLarge
		}
		return body, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, &ResolveError{Reference: endpoint, Code: ReasonImageNotFound,
			Detail: fmt.Sprintf("registry %s has no blob %s in %s", ref.APIHost(), digest, ref.Repository)}
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnauthorized,
			Detail: fmt.Sprintf("registry %s refused the blob read with %d", ref.APIHost(), resp.StatusCode)}
	case resp.StatusCode >= 500:
		return nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
			Detail: fmt.Sprintf("registry %s answered %d", ref.APIHost(), resp.StatusCode)}
	default:
		return nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
			Detail: fmt.Sprintf("registry %s answered %d to a blob read", ref.APIHost(), resp.StatusCode)}
	}
}

// getRedirected follows the one redirect a blob read may take. No
// Authorization is sent: a redirect target is a CDN holding a pre-signed URL,
// and a registry credential must never leave for another host.
func (r *Resolver) getRedirected(ctx context.Context, client *http.Client, location *url.URL, limit int64) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, location.String(), nil)
	if err != nil {
		return nil, nil, &ResolveError{Reference: location.String(), Code: ReasonImageResolveUnsupported,
			Detail: "cannot build the redirected blob request", Err: err}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, &ResolveError{Reference: location.String(), Code: ReasonImageResolveUnavailable,
			Detail: "the blob's redirect target could not be reached", Err: err}
	}
	defer resp.Body.Close()
	body, err := readLimited(resp, limit)
	if err != nil {
		return nil, nil, &ResolveError{Reference: location.String(), Code: ReasonImageResolveUnavailable,
			Detail: "the redirected blob response could not be read", Err: err}
	}
	return resp, body, nil
}

// readLimited reads at most limit bytes of a response body.
func readLimited(resp *http.Response, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}

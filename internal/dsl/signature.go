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

// Cosign signature verification (docs/DEPLOY.md "Image supply-chain policy").
//
// THERE IS EXACTLY ONE RESOLVER, and that extends to signatures: the verifier
// is built over Resolver, reuses its bearer/basic authentication and its
// loopback-only plain-HTTP rule, and is the one implementation the engine
// (apply, validate, submit) and the Orker (dispatch) share. It is one fixed
// scheme — key-based cosign signatures, ECDSA P-256 / SHA-256, in either of
// the two layouts cosign stores them in — with no interface behind it, no
// keyless mode, no transparency log and no sigstore dependency (AGENTS.md
// §13: not a plugin framework). Both layouts are a signature by the key over
// a document naming the image's digest; the standard library verifies either.

// Cosign's legacy key-based signature layout, as `cosign sign --key` writes
// it before cosign 3 (and cosign 3 with --new-bundle-format=false): an OCI
// image manifest at the tag "sha256-<hex>.sig" in the SAME repository, one
// layer per signature, each layer's blob the simple-signing payload and its
// annotation the signature. OrkStack v0.1.0's images are signed this way.
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

// Cosign's bundle layout, what `cosign sign --key` writes by default since
// cosign 3: a Sigstore bundle (v0.3) stored as an OCI referrer of the image.
// The referrer is an OCI image manifest whose one layer is the bundle; the
// bundle's DSSE envelope carries an in-toto statement whose subject is the
// image's digest and whose predicate type says it is a cosign signature, and
// the envelope's signature is over DSSE's pre-authentication encoding of that
// statement. A registry with the OCI referrers API lists the referrer there;
// one without lists it in the image index tagged "sha256-<hex>" (the
// referrers tag schema), exactly as cosign's own registry client looks.
const (
	MediaTypeSigstoreBundle = "application/vnd.dev.sigstore.bundle.v0.3+json"
	// sigstoreBundlePrefix starts every Sigstore bundle media type, of any
	// version: a bundle of another version is a signature this verifier
	// counts and cannot read.
	sigstoreBundlePrefix = "application/vnd.dev.sigstore.bundle."
	// CosignSignPredicateType is the predicate type of the statement cosign
	// signs for an image; another one is an attestation, not a signature.
	CosignSignPredicateType   = "https://sigstore.dev/cosign/sign/v1"
	dsseInTotoPayloadType     = "application/vnd.in-toto+json"
	inTotoStatementType       = "https://in-toto.io/Statement/v1"
	bundlePredicateAnnotation = "dev.sigstore.bundle.predicateType"
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

// cosignManifest is the part of a signature object's manifest verification
// reads: the legacy .sig manifest and a bundle's referrer manifest alike.
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

// referrersIndex is the part of a referrers index (the referrers API's answer
// or the referrers tag's image index) verification reads.
type referrersIndex struct {
	MediaType string `json:"mediaType"`
	Manifests []struct {
		MediaType    string            `json:"mediaType"`
		Digest       string            `json:"digest"`
		Size         int64             `json:"size"`
		ArtifactType string            `json:"artifactType"`
		Annotations  map[string]string `json:"annotations"`
	} `json:"manifests"`
}

// sigstoreBundle is the part of a Sigstore bundle verification reads. The
// verification material (a key hint, transparency-log entries) is not read:
// the key is the policy's, and OrkStack reads no transparency log.
type sigstoreBundle struct {
	MediaType    string `json:"mediaType"`
	DSSEEnvelope *struct {
		Payload     string `json:"payload"`
		PayloadType string `json:"payloadType"`
		Signatures  []struct {
			Sig string `json:"sig"`
		} `json:"signatures"`
	} `json:"dsseEnvelope"`
}

// inTotoStatement is the part of the signed statement verification reads.
type inTotoStatement struct {
	Type    string `json:"_type"`
	Subject []struct {
		Digest map[string]string `json:"digest"`
	} `json:"subject"`
	PredicateType string `json:"predicateType"`
}

// signatureRead is what one layout held for the digest: whether it stored any
// signature object, how many signatures it listed (those of a bundle version
// it cannot read included), and whether one verified. invalid, when set, is
// the layout's own refusal (a .sig object that is not an image manifest, a
// referrers answer that is not an image index), returned only when nothing
// else verifies.
type signatureRead struct {
	found    bool
	count    int
	verified bool
	invalid  *SignatureError
}

// signatureCheck is one Verify call's fixed inputs.
type signatureCheck struct {
	v       *SignatureVerifier
	pinned  string
	signer  string
	key     *ecdsa.PublicKey
	digest  string
	repo    Reference // the image's repository; Tag and Original vary per GET
	authHdr string    // the bearer token the first GET earned, reused by the rest
}

// Verify accepts the image when AT LEAST ONE signature by the key over
// image.index_digest verifies, in either layout, read in this order:
//
//  1. The legacy layout. GET /v2/<repository>/manifests/sha256-<hex>.sig
//     through Resolver.getManifest; 404 is "none here". The body must be an
//     OCI image manifest (or Docker v2 manifest). For each of at most
//     maxSignatureLayers layers, in manifest order: GET the layer's blob
//     (Resolver.getSignatureBlob, the same auth flow, bounded at
//     maxSignaturePayloadBytes); sha256(body) must equal layer.digest; the
//     annotation dev.cosignproject.cosign/signature is base64 of a DER ECDSA
//     signature; ecdsa.VerifyASN1(key, sha256(body), sig) must hold; the body
//     must be JSON whose critical.image.docker-manifest-digest equals
//     image.index_digest and whose critical.type is "cosign container image
//     signature".
//  2. The bundle layout, only when the legacy layout verified nothing. GET
//     /v2/<repository>/referrers/<digest>; an OCI image index answered with
//     200 lists the referrers, and any other 200, a 404 or a 400 means the
//     registry has no referrers API, so the index is GET
//     /v2/<repository>/manifests/sha256-<hex> instead (404: no referrers).
//     A referrer is a signature when its descriptor's artifactType is the
//     bundle v0.3 media type and its predicate-type annotation, when it has
//     one, is cosign's signature predicate; one of another bundle version is
//     counted and never fetched, and an index that is not one is invalid,
//     never "none". For each of at most
//     maxSignatureLayers of them, in index order: GET the referrer manifest
//     by digest, which must hash to it; its first bundle layer's blob, bounded
//     and content-addressed as above, is the bundle; the bundle's DSSE
//     envelope must carry an in-toto payload with a signature for which
//     ecdsa.VerifyASN1(key, sha256(PAE(payloadType, payload)), sig) holds; the
//     payload must be an in-toto v1 statement with cosign's signature
//     predicate type whose subject names image.index_digest.
//  3. Nothing verified: no signature in either layout → IMAGE_NOT_SIGNED;
//     otherwise IMAGE_SIGNATURE_INVALID naming the count of both. A GET the
//     registry did not answer (a transport failure, 401/403 after the token
//     dance, 5xx) → IMAGE_SIGNATURE_UNAVAILABLE, never a skip: an
//     unreachable registry is never read as "no valid signature" and never
//     as "signed". A layer, referrer or blob that is absent (404), over its
//     bound or not what its digest names is skipped and counted.
//
// Neither layout's identity claim (the payload's docker-reference, the
// manifest's subject) is required to match: the signature binds the KEY
// HOLDER to the DIGEST, which is the claim the policy makes, and cosign's own
// key-based verify checks the digest claim only.
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

	c := &signatureCheck{v: v, pinned: pinned, signer: signer, key: key, digest: digest,
		repo: Reference{Registry: image.GetRegistry(), Repository: image.GetRepository()}}
	legacy, err := c.readLegacy(ctx)
	if err != nil {
		return err
	}
	if legacy.verified {
		return nil
	}
	bundles, err := c.readBundles(ctx)
	if err != nil {
		return err
	}
	if bundles.verified {
		return nil
	}
	if legacy.invalid != nil {
		return legacy.invalid
	}
	if bundles.invalid != nil {
		return bundles.invalid
	}
	if !legacy.found && !bundles.found {
		return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageNotSigned,
			Detail: fmt.Sprintf("carries no cosign signature (%s is not in the registry, and no cosign bundle refers to %s)",
				c.at(c.legacyTag()).Original, digest)}
	}
	return &SignatureError{Image: pinned, Signer: signer, Code: task.ReasonImageSignatureInvalid,
		Detail: fmt.Sprintf("has %d cosign signature(s) and none verifies with key %q (%s) over this digest",
			legacy.count+bundles.count, keyName, task.SigningKeyFingerprint(key))}
}

// at is the image's repository at one tag or digest.
func (c *signatureCheck) at(target string) Reference {
	ref := c.repo
	sep := ":"
	if strings.HasPrefix(target, "sha256:") {
		sep = "@"
		ref.Digest = target
	} else {
		ref.Tag = target
	}
	ref.Original = ref.Registry + "/" + ref.Repository + sep + target
	return ref
}

func (c *signatureCheck) legacyTag() string {
	return "sha256-" + strings.TrimPrefix(c.digest, "sha256:") + CosignSignatureTagSuffix
}

// unavailable is the verdict for a GET the registry did not answer.
func (c *signatureCheck) unavailable(ref Reference, err error) error {
	return &SignatureError{Image: c.pinned, Signer: c.signer, Code: task.ReasonImageSignatureUnavailable,
		Detail: fmt.Sprintf("could not be checked for a cosign signature: %s; "+
			"this is a registry problem, not a verdict", c.v.transportSentence(ref, err)), Err: err}
}

// getManifest is Resolver.getManifest with this check's token; a 404 is
// (nil, nil) and any other failure is the unavailable verdict.
func (c *signatureCheck) getManifest(ctx context.Context, ref Reference) ([]byte, string, error) {
	body, contentType, _, err := c.v.Resolver.getManifest(ctx, ref, ref.Target(), &c.authHdr)
	if err != nil {
		var resolve *ResolveError
		if errors.As(err, &resolve) && resolve.Code == ReasonImageNotFound {
			return nil, "", nil
		}
		return nil, "", c.unavailable(ref, err)
	}
	return body, contentType, nil
}

// getBlob is Resolver.getSignatureBlob; skip reports a blob that is absent or
// over the payload bound, and err the unavailable verdict.
func (c *signatureCheck) getBlob(ctx context.Context, ref Reference, digest string) (body []byte, skip bool, err error) {
	payload, err := c.v.Resolver.getSignatureBlob(ctx, ref, digest, &c.authHdr)
	if err != nil {
		var resolve *ResolveError
		switch {
		case errors.Is(err, errBlobTooLarge):
			return nil, true, nil
		case errors.As(err, &resolve) && resolve.Code == ReasonImageNotFound:
			return nil, true, nil
		default:
			return nil, false, c.unavailable(ref, err)
		}
	}
	return payload, false, nil
}

// readLegacy is Verify's step 1.
func (c *signatureCheck) readLegacy(ctx context.Context) (signatureRead, error) {
	ref := c.at(c.legacyTag())
	body, contentType, err := c.getManifest(ctx, ref)
	if err != nil || body == nil {
		return signatureRead{}, err
	}
	read := signatureRead{found: true}
	var manifest cosignManifest
	mediaType := mediaTypeOf(contentType)
	if jsonErr := json.Unmarshal(body, &manifest); jsonErr != nil ||
		(mediaType != MediaTypeOCIManifest && mediaType != MediaTypeDockerManifest &&
			manifest.MediaType != MediaTypeOCIManifest && manifest.MediaType != MediaTypeDockerManifest) {
		read.invalid = &SignatureError{Image: c.pinned, Signer: c.signer, Code: task.ReasonImageSignatureInvalid,
			Detail: fmt.Sprintf("has a signature object %s that is not an image manifest (%s)",
				ref.Original, SafeReason(contentType)), Err: jsonErr}
		return read, nil
	}
	read.count = len(manifest.Layers)
	for i, layer := range manifest.Layers {
		if i >= maxSignatureLayers {
			break
		}
		if !ValidDigest(layer.Digest) || layer.Size > maxSignaturePayloadBytes {
			continue
		}
		payload, skip, err := c.getBlob(ctx, ref, layer.Digest)
		if err != nil {
			return read, err
		}
		if !skip && verifiesLayer(c.key, c.digest, layer.Digest, layer.Annotations[CosignSignatureAnnotation], payload) {
			read.verified = true
			return read, nil
		}
	}
	return read, nil
}

// readBundles is Verify's step 2.
func (c *signatureCheck) readBundles(ctx context.Context) (signatureRead, error) {
	index, invalid, err := c.referrers(ctx)
	if err != nil || invalid != nil || index == nil {
		return signatureRead{found: invalid != nil, invalid: invalid}, err
	}
	candidates, unread := bundleReferrers(index)
	read := signatureRead{found: len(candidates)+unread > 0, count: len(candidates) + unread}
	for i, referrer := range candidates {
		if i >= maxSignatureLayers {
			break
		}
		ref := c.at(referrer)
		body, _, err := c.getManifest(ctx, ref)
		if err != nil {
			return read, err
		}
		if body == nil || digestOf(body) != referrer {
			continue
		}
		layer, ok := bundleLayer(body)
		if !ok {
			continue
		}
		bundle, skip, err := c.getBlob(ctx, ref, layer)
		if err != nil {
			return read, err
		}
		if !skip && digestOf(bundle) == layer && bundleVerifies(c.key, c.digest, bundle) {
			read.verified = true
			return read, nil
		}
	}
	return read, nil
}

// referrers returns the image index that lists the digest's referrers, or nil
// when there is none: the referrers API's answer when the registry has the
// API, the referrers tag's index when it does not (the fallback cosign's
// registry client takes on exactly the same answers). An answer that is not
// an image index within the manifest bound is invalid, the layout's own
// refusal: never read as "no signature".
func (c *signatureCheck) referrers(ctx context.Context) ([]byte, *SignatureError, error) {
	r := c.v.Resolver
	ref := c.at(c.digest)
	endpoint := fmt.Sprintf("%s://%s/v2/%s/referrers/%s?artifactType=%s",
		ref.Scheme(), ref.APIHost(), ref.Repository, c.digest, url.QueryEscape(MediaTypeSigstoreBundle))
	resp, body, err := r.authorizedGet(ctx, ref, endpoint, &c.authHdr)
	if err != nil {
		return nil, nil, c.unavailable(ref, err)
	}
	contentType := resp.Header.Get("Content-Type")
	switch {
	case resp.StatusCode == http.StatusOK && mediaTypeOf(contentType) == MediaTypeOCIIndex:
		return c.readIndex(body, contentType, "the referrers API's answer for "+ref.Original)
	case resp.StatusCode == http.StatusOK, resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusBadRequest:
		// No referrers API: the referrers tag schema.
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, nil, c.unavailable(ref, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnauthorized,
			Detail: fmt.Sprintf("registry %s refused the referrers read with %d", ref.APIHost(), resp.StatusCode)})
	default:
		return nil, nil, c.unavailable(ref, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
			Detail: fmt.Sprintf("registry %s answered %d to a referrers read", ref.APIHost(), resp.StatusCode)})
	}
	tagged := c.at("sha256-" + strings.TrimPrefix(c.digest, "sha256:"))
	body, contentType, err = c.getManifest(ctx, tagged)
	if err != nil || body == nil {
		return nil, nil, err
	}
	return c.readIndex(body, contentType, "the referrers tag "+tagged.Original)
}

// readIndex accepts body as a referrers index: JSON within the manifest
// bound whose mediaType, or failing that its Content-Type, is an OCI image
// index. Anything else is the layout's invalid verdict, naming what was read.
func (c *signatureCheck) readIndex(body []byte, contentType, what string) ([]byte, *SignatureError, error) {
	var index referrersIndex
	jsonErr := json.Unmarshal(body, &index)
	mediaType := mediaTypeOf(index.MediaType)
	if mediaType == "" {
		mediaType = mediaTypeOf(contentType)
	}
	if int64(len(body)) > maxManifestBytes || jsonErr != nil || mediaType != MediaTypeOCIIndex {
		return nil, &SignatureError{Image: c.pinned, Signer: c.signer, Code: task.ReasonImageSignatureInvalid,
			Detail: fmt.Sprintf("has %s that is not an image index within %d bytes (%s)", what, maxManifestBytes, SafeReason(contentType)),
			Err:    jsonErr}, nil
	}
	return body, nil, nil
}

// bundleReferrers lists, in index order, the digests of the referrers in a
// referrers index that are cosign signature bundles: an OCI image manifest
// whose artifactType is the bundle v0.3 media type, of a size a manifest may
// have, and whose predicate-type annotation, when it carries one, is cosign's
// signature predicate (another is an attestation). unread counts the
// signatures in a bundle of another version, which are never fetched.
// Anything unparseable lists nothing.
func bundleReferrers(index []byte) (digests []string, unread int) {
	var parsed referrersIndex
	if json.Unmarshal(index, &parsed) != nil {
		return nil, 0
	}
	for _, m := range parsed.Manifests {
		predicate, annotated := m.Annotations[bundlePredicateAnnotation]
		if annotated && predicate != CosignSignPredicateType {
			continue
		}
		switch {
		case m.ArtifactType == MediaTypeSigstoreBundle && mediaTypeOf(m.MediaType) == MediaTypeOCIManifest &&
			ValidDigest(m.Digest) && m.Size >= 0 && m.Size <= maxManifestBytes:
			digests = append(digests, m.Digest)
		case m.ArtifactType != MediaTypeSigstoreBundle && strings.HasPrefix(m.ArtifactType, sigstoreBundlePrefix):
			unread++
		}
	}
	return digests, unread
}

// bundleLayer returns the digest of the first layer of a referrer manifest
// that is a bundle within the payload bound.
func bundleLayer(manifest []byte) (string, bool) {
	var parsed cosignManifest
	if json.Unmarshal(manifest, &parsed) != nil || mediaTypeOf(parsed.MediaType) != MediaTypeOCIManifest {
		return "", false
	}
	for _, layer := range parsed.Layers {
		if mediaTypeOf(layer.MediaType) == MediaTypeSigstoreBundle && ValidDigest(layer.Digest) &&
			layer.Size >= 0 && layer.Size <= maxSignaturePayloadBytes {
			return layer.Digest, true
		}
	}
	return "", false
}

// bundleVerifies is step 2's check of one bundle's bytes: a v0.3 bundle whose
// DSSE envelope carries an in-toto payload, a signature by key over its PAE,
// and a statement with cosign's signature predicate whose subject names
// imageDigest. At most maxSignatureLayers of the envelope's signatures are
// tried.
func bundleVerifies(key *ecdsa.PublicKey, imageDigest string, bundle []byte) bool {
	var b sigstoreBundle
	if json.Unmarshal(bundle, &b) != nil || b.MediaType != MediaTypeSigstoreBundle || b.DSSEEnvelope == nil {
		return false
	}
	envelope := b.DSSEEnvelope
	if envelope.PayloadType != dsseInTotoPayloadType {
		return false
	}
	payload, ok := decodeBase64(envelope.Payload)
	if !ok {
		return false
	}
	sum := sha256.Sum256(dssePAE(envelope.PayloadType, payload))
	signed := false
	for i, s := range envelope.Signatures {
		if i >= maxSignatureLayers {
			break
		}
		if sig, ok := decodeBase64(s.Sig); ok && len(sig) > 0 && ecdsa.VerifyASN1(key, sum[:], sig) {
			signed = true
			break
		}
	}
	if !signed {
		return false
	}
	var statement inTotoStatement
	if json.Unmarshal(payload, &statement) != nil ||
		statement.Type != inTotoStatementType || statement.PredicateType != CosignSignPredicateType {
		return false
	}
	want := strings.TrimPrefix(imageDigest, "sha256:")
	for _, subject := range statement.Subject {
		if subject.Digest["sha256"] == want {
			return true
		}
	}
	return false
}

// dssePAE is DSSE's pre-authentication encoding, the bytes a DSSE signature
// is over: "DSSEv1 <len(type)> <type> <len(body)> <body>".
func dssePAE(payloadType string, payload []byte) []byte {
	return fmt.Appendf(nil, "DSSEv1 %d %s %d %s", len(payloadType), payloadType, len(payload), payload)
}

// decodeBase64 reads a bundle's bytes field as the protobuf JSON mapping
// writes and reads it: standard or URL-safe base64, padded or not.
func decodeBase64(s string) ([]byte, bool) {
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding,
	} {
		if b, err := encoding.DecodeString(s); err == nil {
			return b, true
		}
	}
	return nil, false
}

// verifiesLayer is step 1 for one layer: content address, signature, claim.
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
				// A network error can quote a host the registry named (a
				// token realm): escaped and cut, as ResolveError.Error does.
				return fmt.Sprintf("registry %s could not be reached: %s", ref.APIHost(), SafeReason(resolve.Err.Error()))
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
// {scheme}://{apiHost}/v2/{repository}/blobs/{digest}, with *authorization
// (the token an earlier GET of the same check earned, or none), the status
// handling getManifest has and one bearer-token round trip whose token is
// written back through authorization, following ONE redirect (registries
// serve blobs from a CDN with a 307) without forwarding credentials to the
// redirect's host, with the body bounded at maxSignaturePayloadBytes.
func (r *Resolver) getSignatureBlob(ctx context.Context, ref Reference, digest string, authorization *string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s://%s/v2/%s/blobs/%s", ref.Scheme(), ref.APIHost(), ref.Repository, digest)
	noFollow := *r.client()
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	limit := int64(maxSignaturePayloadBytes) + 1

	resp, body, err := r.doWith(ctx, &noFollow, endpoint, *authorization, "", limit)
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
				*authorization = "Bearer " + token
				resp, body, err = r.doWith(ctx, &noFollow, endpoint, *authorization, "", limit)
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

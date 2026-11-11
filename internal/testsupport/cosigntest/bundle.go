package cosigntest

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// cosign 3's default layout, restated from internal/dsl/signature.go for the
// same reason as the legacy constants: `cosign sign --key` with the new
// bundle format stores a Sigstore bundle (v0.3) as an OCI referrer of the
// image. The referrer is an OCI image manifest whose subject is the image,
// whose artifactType and one layer are the bundle media type, and whose
// layer's blob is the bundle: a DSSE envelope over an in-toto statement that
// names the image's digest. A registry with the referrers API lists it there;
// one without lists it in the image index tagged "sha256-<hex>".
const (
	BundleType        = "application/vnd.dev.sigstore.bundle.v0.3+json"
	InTotoPayloadType = "application/vnd.in-toto+json"
	StatementType     = "https://in-toto.io/Statement/v1"
	SignPredicateType = "https://sigstore.dev/cosign/sign/v1"
	OCIIndexType      = "application/vnd.oci.image.index.v1+json"
	OCIEmptyType      = "application/vnd.oci.empty.v1+json"
	// The annotations cosign writes on the referrer manifest and on its
	// descriptor in the index.
	BundleContentAnnotation   = "dev.sigstore.bundle.content"
	BundlePredicateAnnotation = "dev.sigstore.bundle.predicateType"
)

// emptyConfig is the OCI empty descriptor's blob, the referrer's config.
var emptyConfig = []byte("{}")

// Statement is the in-toto statement cosign 3 signs for one image digest.
func Statement(digest string) []byte {
	return StatementWith(digest, SignPredicateType)
}

// StatementWith is Statement with another predicate type: an attestation
// rather than a signature.
func StatementWith(digest, predicateType string) []byte {
	body, _ := stablejson.Marshal(map[string]any{
		"_type": StatementType,
		"subject": []map[string]any{{
			"digest":      map[string]string{"sha256": strings.TrimPrefix(digest, "sha256:")},
			"annotations": map[string]string{},
		}},
		"predicateType": predicateType,
		"predicate":     map[string]any{},
	})
	return body
}

// PAE is DSSE's pre-authentication encoding of one payload, the bytes a DSSE
// signature is over.
func PAE(payloadType string, payload []byte) []byte {
	return fmt.Appendf(nil, "DSSEv1 %d %s %d %s", len(payloadType), payloadType, len(payload), payload)
}

// Bundle is the bundle k writes for digest, as `cosign sign --key` does with
// no transparency log: a public-key hint and a DSSE envelope over Statement.
func (k *Key) Bundle(digest string) ([]byte, error) {
	return k.BundleOver(InTotoPayloadType, Statement(digest))
}

// BundleOver is a bundle whose envelope carries payload as payloadType,
// signed by k over its PAE.
func (k *Key) BundleOver(payloadType string, payload []byte) ([]byte, error) {
	sum := sha256.Sum256(PAE(payloadType, payload))
	sig, err := ecdsa.SignASN1(rand.Reader, k.Private, sum[:])
	if err != nil {
		return nil, err
	}
	return EnvelopeBundle(k.Hint(), payloadType, payload, base64.StdEncoding.EncodeToString(sig)), nil
}

// EnvelopeBundle assembles a bundle from its parts, signed or not: the
// tamper cases build one by hand.
func EnvelopeBundle(hint, payloadType string, payload []byte, signatures ...string) []byte {
	sigs := make([]map[string]string, 0, len(signatures))
	for _, s := range signatures {
		sigs = append(sigs, map[string]string{"sig": s})
	}
	body, _ := stablejson.Marshal(map[string]any{
		"mediaType": BundleType,
		"verificationMaterial": map[string]any{
			"publicKey": map[string]string{"hint": hint},
		},
		"dsseEnvelope": map[string]any{
			"payload":     base64.StdEncoding.EncodeToString(payload),
			"payloadType": payloadType,
			"signatures":  sigs,
		},
	})
	return body
}

// Hint is the key hint cosign writes: base64 of the SHA-256 of the public
// key's DER.
func (k *Key) Hint() string {
	der, _ := x509.MarshalPKIXPublicKey(&k.Private.PublicKey)
	sum := sha256.Sum256(der)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// ReferrersTag is the tag a registry without the referrers API keeps an
// image digest's referrers index under (the OCI referrers tag schema).
func ReferrersTag(digest string) string {
	return "sha256-" + strings.TrimPrefix(digest, "sha256:")
}

// BundleManifest is the referrer manifest cosign writes for one bundle blob
// about the image subject (an image index of subjectSize bytes).
func BundleManifest(bundle []byte, subject string, subjectSize int) []byte {
	type descriptor struct {
		MediaType    string            `json:"mediaType"`
		Size         int               `json:"size"`
		Digest       string            `json:"digest"`
		ArtifactType string            `json:"artifactType,omitempty"`
		Annotations  map[string]string `json:"annotations,omitempty"`
	}
	m := struct {
		SchemaVersion int               `json:"schemaVersion"`
		MediaType     string            `json:"mediaType"`
		Config        descriptor        `json:"config"`
		Layers        []descriptor      `json:"layers"`
		Annotations   map[string]string `json:"annotations"`
		Subject       descriptor        `json:"subject"`
		ArtifactType  string            `json:"artifactType"`
	}{
		SchemaVersion: 2,
		MediaType:     OCIManifestType,
		Config: descriptor{MediaType: OCIEmptyType, Size: len(emptyConfig), Digest: Digest(emptyConfig),
			ArtifactType: BundleType},
		Layers: []descriptor{{MediaType: BundleType, Size: len(bundle), Digest: Digest(bundle),
			Annotations: map[string]string{"org.opencontainers.image.title": ReferrersTag(Digest(bundle)) + ".sigstore.json"}}},
		Annotations: map[string]string{
			BundleContentAnnotation:   "dsse-envelope",
			BundlePredicateAnnotation: SignPredicateType,
		},
		Subject:      descriptor{MediaType: OCIIndexType, Size: subjectSize, Digest: subject},
		ArtifactType: BundleType,
	}
	body, _ := stablejson.Marshal(m)
	return body
}

// Referrer is one entry of a referrers index: a referrer manifest and what
// its descriptor says about it.
type Referrer struct {
	Manifest      []byte
	ArtifactType  string
	PredicateType string
}

// BundleReferrer is the index entry cosign writes for a bundle's manifest.
func BundleReferrer(manifest []byte) Referrer {
	return Referrer{Manifest: manifest, ArtifactType: BundleType, PredicateType: SignPredicateType}
}

// ReferrersIndex is the image index listing referrers, as the referrers API
// answers and as the referrers tag holds.
func ReferrersIndex(referrers ...Referrer) []byte {
	type descriptor struct {
		MediaType    string            `json:"mediaType"`
		Size         int               `json:"size"`
		Digest       string            `json:"digest"`
		Annotations  map[string]string `json:"annotations,omitempty"`
		ArtifactType string            `json:"artifactType,omitempty"`
	}
	index := struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Manifests     []descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: OCIIndexType, Manifests: []descriptor{}}
	for _, r := range referrers {
		annotations := map[string]string{}
		if r.PredicateType != "" {
			annotations[BundleContentAnnotation] = "dsse-envelope"
			annotations[BundlePredicateAnnotation] = r.PredicateType
		}
		index.Manifests = append(index.Manifests, descriptor{
			MediaType: OCIManifestType, Size: len(r.Manifest), Digest: Digest(r.Manifest),
			Annotations: annotations, ArtifactType: r.ArtifactType,
		})
	}
	body, _ := stablejson.Marshal(index)
	return body
}

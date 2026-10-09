package task

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestImagePolicy_RegistryVectors(t *testing.T) {
	for _, v := range ImagePolicyVectors().Registries {
		name := fmt.Sprintf("%v→%s/%s", v.Allowed, v.Registry, v.Repository)
		t.Run(name, func(t *testing.T) {
			if v.Invalid != "" {
				err := ValidRegistryPrefixes(v.Allowed)
				if err == nil || !strings.Contains(err.Error(), v.Invalid) {
					t.Fatalf("ValidRegistryPrefixes(%v) = %v, want an error containing %q", v.Allowed, err, v.Invalid)
				}
				return
			}
			if err := ValidRegistryPrefixes(v.Allowed); err != nil {
				t.Fatalf("ValidRegistryPrefixes(%v) = %v, want nil", v.Allowed, err)
			}
			if got := RegistryAllowed(v.Allowed, v.Registry, v.Repository); got != v.Permitted {
				t.Fatalf("RegistryAllowed(%v, %q, %q) = %v, want %v", v.Allowed, v.Registry, v.Repository, got, v.Permitted)
			}
		})
	}
}

func TestImagePolicy_RequireDigestVectors(t *testing.T) {
	sel := &orkstackv1.ImagePolicySelection{RequireDigest: true}
	for _, v := range ImagePolicyVectors().Digests {
		t.Run(v.Reference, func(t *testing.T) {
			image := &orkstackv1.ImageRef{Reference: v.Reference, Registry: "ghcr.io", Repository: "acme/migrate"}
			refusal := CheckImage(sel, 0, "migrate", image)
			if (refusal == nil) != v.Permitted {
				t.Fatalf("CheckImage(require_digest, %q) = %v, want permitted=%v", v.Reference, refusal, v.Permitted)
			}
			if refusal != nil && refusal.Code != ReasonImageNotPermitted {
				t.Fatalf("code = %q, want %q", refusal.Code, ReasonImageNotPermitted)
			}
		})
	}
}

func TestValidRegistryPrefix_RefusesEveryMalformedShape(t *testing.T) {
	cases := []string{
		"GHCR.io/acme/",
		"acme/",
		"ghcr.io/acme/*",
		"ghcr.io/acme@sha256:" + sixtyFourHex,
		"ghcr.io/acme/x:v1",
		"ghcr io/",
		"ghcr.io/" + strings.Repeat("a", 256-len("ghcr.io/")),
		"",
	}
	for _, entry := range cases {
		t.Run(entry, func(t *testing.T) {
			err := ValidRegistryPrefix(entry)
			if err == nil {
				t.Fatalf("ValidRegistryPrefix(%q) = nil, want a refusal", entry)
			}
			if entry != "" && !strings.Contains(err.Error(), fmt.Sprintf("%q", entry)) {
				t.Fatalf("refusal %q does not name the entry %q", err, entry)
			}
		})
	}
	if err := ValidRegistryPrefix("acme/"); err == nil || !strings.Contains(err.Error(), "write docker.io/acme/") {
		t.Fatalf("acme/ refusal = %v, want the sentence naming docker.io/acme/", err)
	}
	for _, ok := range []string{"ghcr.io/acme/", "ghcr.io/acme", "localhost:5001/", "localhost/x", "123.dkr.ecr.eu-west-1.amazonaws.com/"} {
		if err := ValidRegistryPrefix(ok); err != nil {
			t.Fatalf("ValidRegistryPrefix(%q) = %v, want nil", ok, err)
		}
	}
}

func TestValidRegistryPrefixes_BoundsAndDuplicates(t *testing.T) {
	entries := make([]string, 0, MaxAllowedRegistries+1)
	for i := 0; i < MaxAllowedRegistries; i++ {
		entries = append(entries, fmt.Sprintf("ghcr.io/team%d/", i))
	}
	if err := ValidRegistryPrefixes(entries); err != nil {
		t.Fatalf("64 entries: %v", err)
	}
	entries = append(entries, "ghcr.io/one-more/")
	if err := ValidRegistryPrefixes(entries); err == nil || !strings.Contains(err.Error(), "65 entries") {
		t.Fatalf("65 entries: %v, want a refusal naming the count", err)
	}
	err := ValidRegistryPrefixes([]string{"ghcr.io/acme", "ghcr.io/acme/"})
	if err == nil || !strings.Contains(err.Error(), "allowed_registries[1]") || !strings.Contains(err.Error(), "allowed_registries[0]") {
		t.Fatalf("duplicate: %v, want a refusal naming both indexes", err)
	}
}

func pemOf(t *testing.T, blockType string, der []byte) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}

func TestParseSigningKey_AcceptsCosignPubAndRefusesTheRest(t *testing.T) {
	if _, err := ParseSigningKey([]byte(FixedSigningKeyPEM)); err != nil {
		t.Fatalf("P-256 PKIX PEM: %v", err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	rsaDER, _ := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	p384DER, _ := x509.MarshalPKIXPublicKey(&p384.PublicKey)
	p256, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	privDER, _ := x509.MarshalECPrivateKey(p256)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "x"},
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(1<<31, 0)}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &p256.PublicKey, p256)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, want string
		pem        []byte
	}{
		{"rsa", "*rsa.PublicKey", pemOf(t, "PUBLIC KEY", rsaDER)},
		{"p384", "P-384", pemOf(t, "PUBLIC KEY", p384DER)},
		{"private", `"EC PRIVATE KEY"`, pemOf(t, "EC PRIVATE KEY", privDER)},
		{"certificate", `"CERTIFICATE"`, pemOf(t, "CERTIFICATE", certDER)},
		{"not-pem", "not a PEM block", []byte("hello")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseSigningKey(c.pem)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("ParseSigningKey(%s) = %v, want an error naming %s", c.name, err, c.want)
			}
		})
	}
}

func TestSigningKeyFingerprint_IsSha256OverDER(t *testing.T) {
	key, err := ParseSigningKey([]byte(FixedSigningKeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	if got := SigningKeyFingerprint(key); got != FixedSigningKeyFingerprint {
		t.Fatalf("fingerprint = %s, want %s", got, FixedSigningKeyFingerprint)
	}
	if got := SigningKeyPEMFingerprint(FixedSigningKeyPEM); got != FixedSigningKeyFingerprint {
		t.Fatalf("PEM fingerprint = %s", got)
	}
}

func TestValidSigningKeys_BoundsNamesAndPEM(t *testing.T) {
	keys := map[string]string{}
	for i := 0; i < MaxSigningKeys; i++ {
		keys[fmt.Sprintf("key-%d", i)] = FixedSigningKeyPEM
	}
	if err := ValidSigningKeys(keys); err != nil {
		t.Fatalf("16 keys: %v", err)
	}
	keys["key-16"] = FixedSigningKeyPEM
	if err := ValidSigningKeys(keys); err == nil || !strings.Contains(err.Error(), "17 entries") {
		t.Fatalf("17 keys: %v", err)
	}
	if err := ValidSigningKeys(map[string]string{"Acme": FixedSigningKeyPEM}); err == nil || !strings.Contains(err.Error(), `"Acme"`) {
		t.Fatalf("name Acme: %v", err)
	}
	big := FixedSigningKeyPEM + strings.Repeat(" ", MaxSigningKeyPEMBytes+1-len(FixedSigningKeyPEM))
	if err := ValidSigningKeys(map[string]string{"acme": big}); err == nil || !strings.Contains(err.Error(), "4097 bytes") {
		t.Fatalf("4097-byte PEM: %v", err)
	}
}

func TestImagePolicy_OnlyCosignIsASigner(t *testing.T) {
	scheme, name, err := ParseSigner("cosign:acme-release")
	if err != nil || scheme != SignerSchemeCosign || name != "acme-release" {
		t.Fatalf("cosign:acme-release = %q %q %v", scheme, name, err)
	}
	for _, bad := range []string{"notary:x", "cosign:", "cosign:Acme", "acme-release"} {
		_, _, err := ParseSigner(bad)
		want := fmt.Sprintf("require_signed_by %q is not cosign:<key-name>; cosign is the only signer this deployment understands", bad)
		if err == nil || err.Error() != want {
			t.Fatalf("ParseSigner(%q) = %v, want %q", bad, err, want)
		}
	}
	if s, n, err := ParseSigner(""); s != "" || n != "" || err != nil {
		t.Fatalf("empty = %q %q %v", s, n, err)
	}
}

func TestSelection_IsNilForTheEmptyPolicyAndUnsetNamespace(t *testing.T) {
	for _, policy := range []*orkstackv1.ImagePolicy{nil, {}, {SigningKeys: map[string]string{"release": FixedSigningKeyPEM}}} {
		sel, err := Selection(policy, &orkstackv1.Namespace{Name: "prod"})
		if err != nil || sel != nil {
			t.Fatalf("Selection(%v) = %v, %v; want nil, nil", policy, sel, err)
		}
	}
	sel, err := Selection(&orkstackv1.ImagePolicy{RequireDigest: true}, nil)
	if err != nil || sel == nil || !sel.GetRequireDigest() {
		t.Fatalf("require_digest alone = %v, %v", sel, err)
	}
}

func TestSelection_CarriesTheNamedKeyPEM(t *testing.T) {
	policy := &orkstackv1.ImagePolicy{
		AllowedRegistries: []string{"ghcr.io/acme/"},
		SigningKeys:       map[string]string{"release": FixedSigningKeyPEM},
	}
	sel, err := Selection(policy, &orkstackv1.Namespace{Name: "prod", RequireSignedBy: "cosign:release"})
	if err != nil {
		t.Fatal(err)
	}
	if sel.GetRequireSignedBy() != "cosign:release" || sel.GetSigningKeyPem() != FixedSigningKeyPEM ||
		len(sel.GetAllowedRegistries()) != 1 {
		t.Fatalf("selection = %v", sel)
	}
	_, err = Selection(policy, &orkstackv1.Namespace{Name: "prod", RequireSignedBy: "cosign:absent"})
	if err == nil || !strings.Contains(err.Error(), "known keys: release") {
		t.Fatalf("absent key: %v", err)
	}
}

func orkflowOf(images ...*orkstackv1.ImageRef) *orkstackv1.Orkflow {
	wf := &orkstackv1.Orkflow{}
	for i, image := range images {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: []string{"prepare", "migrate", "verify"}[i], Image: image})
	}
	return wf
}

func TestCheckOrkflow_ReturnsTheFirstRefusalInDocumentOrder(t *testing.T) {
	sel := &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/", "123.dkr.ecr.eu-west-1.amazonaws.com/"}}
	wf := orkflowOf(
		&orkstackv1.ImageRef{Reference: "ghcr.io/acme/prep:v1", Registry: "ghcr.io", Repository: "acme/prep"},
		&orkstackv1.ImageRef{Reference: "docker.io/library/alpine:3.20", Registry: "docker.io", Repository: "library/alpine"},
		&orkstackv1.ImageRef{Reference: "quay.io/x/y:1", Registry: "quay.io", Repository: "x/y"},
	)
	err := CheckOrkflow(sel, wf)
	refusal, ok := err.(*ImagePolicyRefusal)
	if !ok {
		t.Fatalf("CheckOrkflow = %v, want *ImagePolicyRefusal", err)
	}
	if refusal.TaskIndex != 1 || refusal.Field() != "tasks[1].uses" {
		t.Fatalf("refusal index %d field %q", refusal.TaskIndex, refusal.Field())
	}
	want := `task "migrate": image "docker.io/library/alpine:3.20" resolves to docker.io/library/alpine, which is outside the registries this deployment allows: ghcr.io/acme/, 123.dkr.ecr.eu-west-1.amazonaws.com/ (orkctl admin image-policy get)`
	if refusal.Error() != want {
		t.Fatalf("Error() =\n%s\nwant\n%s", refusal.Error(), want)
	}
}

func TestCheckOrkflow_RequireDigestSentence(t *testing.T) {
	sel := &orkstackv1.ImagePolicySelection{RequireDigest: true}
	wf := orkflowOf(
		&orkstackv1.ImageRef{Reference: "shell", Registry: "ghcr.io", Repository: "orkstack/activity-shell"},
		&orkstackv1.ImageRef{Reference: "ghcr.io/acme/migrate:v3", Registry: "ghcr.io", Repository: "acme/migrate"},
	)
	err := CheckOrkflow(sel, wf)
	want := `task "migrate": image "ghcr.io/acme/migrate:v3" is a mutable tag, and this deployment requires a digest — write ghcr.io/acme/migrate@sha256:<digest> (orkctl admin image-policy get)`
	if err == nil || err.Error() != want {
		t.Fatalf("Error() =\n%v\nwant\n%s", err, want)
	}
}

func TestCheckOrkflow_NilSelectionWalksNoTask(t *testing.T) {
	// A nil orkflow would panic nowhere, but a nil selection must not even
	// look: the orkflow here carries an image every selection would refuse.
	wf := orkflowOf(&orkstackv1.ImageRef{Reference: "docker.io/x/y:latest", Registry: "docker.io", Repository: "x/y"})
	if err := CheckOrkflow(nil, wf); err != nil {
		t.Fatalf("nil selection: %v", err)
	}
	if refusal := CheckImage(nil, 0, "x", wf.GetTasks()[0].GetImage()); refusal != nil {
		t.Fatalf("nil selection CheckImage: %v", refusal)
	}
}

func TestCheckOrkflow_SkipsAnUnresolvedImage(t *testing.T) {
	sel := &orkstackv1.ImagePolicySelection{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/acme/"}}
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a", Uses: "docker.io/x/y:1"}}}
	if err := CheckOrkflow(sel, wf); err != nil {
		t.Fatalf("unresolved image: %v", err)
	}
}

func TestImagePolicyVectors_AreExportedForTheBoundaries(t *testing.T) {
	v := ImagePolicyVectors()
	if len(v.Registries) != 13 || len(v.Digests) != 5 {
		t.Fatalf("vectors: %d registry rows, %d digest rows; want 13 and 5", len(v.Registries), len(v.Digests))
	}
	if v.FixedSigningKeyFingerprint != SigningKeyPEMFingerprint(v.FixedSigningKeyPEM) {
		t.Fatalf("fixed key fingerprint does not match its PEM")
	}
}

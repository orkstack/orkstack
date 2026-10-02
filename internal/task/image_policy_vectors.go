package task

// Shared vectors for the image supply-chain rule: "one rule, restated at each
// boundary over shared vectors" (AGENTS.md §7 asks it of the slug rule). The
// task package's own tests walk them, and pkg/artifacts, internal/api and
// internal/orker reuse them, so no boundary can drift from the rule.
//
// They live in a non-test file because a Go _test.go file cannot be
// imported by another package's tests. They are data and nothing else: no
// binary calls ImagePolicyVectors.

// RegistryVector is one row of the allowed_registries table: an entry list, a
// resolved "{registry}/{repository}", and whether the image is permitted.
// Invalid is set for an entry the store refuses at validation, in which case
// Registry/Repository are empty and Permitted is meaningless.
type RegistryVector struct {
	Allowed    []string
	Registry   string
	Repository string
	Permitted  bool
	// Invalid names why the (single) entry is refused at validation; ""
	// for an entry that validates.
	Invalid string
}

// DigestVector is one row of the require_digest table: a `uses:` as the
// author wrote it, and whether require_digest permits it.
type DigestVector struct {
	Reference string
	Permitted bool
}

// ImagePolicyVectorSet is every shared vector.
type ImagePolicyVectorSet struct {
	Registries []RegistryVector
	Digests    []DigestVector
	// FixedSigningKeyPEM is a P-256 cosign.pub whose fingerprint is
	// FixedSigningKeyFingerprint. The UI's fingerprint(pem) is held to the
	// same literal (ui/test/imagePolicyForm.test.ts).
	FixedSigningKeyPEM         string
	FixedSigningKeyFingerprint string
}

// FixedSigningKeyPEM is public material generated once for the vectors.
const FixedSigningKeyPEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEu53TEXjEgvYqzDzOZvb8ZJcT+95O
VM++cNZJJfWL6BwbqKs4tIX+kOtqfQqOF0yrl/Wh6uZ/Za70t1qHI/xFfQ==
-----END PUBLIC KEY-----
`

// FixedSigningKeyFingerprint is SigningKeyFingerprint of FixedSigningKeyPEM.
const FixedSigningKeyFingerprint = "sha256:f5a5f53127e68edd8110a0d95cb84f3a2832c15af3833f64da493fcf23306c41"

// ImagePolicyVectors returns the shared vectors (spec §1's tables).
func ImagePolicyVectors() ImagePolicyVectorSet {
	return ImagePolicyVectorSet{
		Registries: []RegistryVector{
			{Allowed: []string{"ghcr.io/acme/"}, Registry: "ghcr.io", Repository: "acme/migrate", Permitted: true},
			{Allowed: []string{"ghcr.io/acme"}, Registry: "ghcr.io", Repository: "acme/migrate", Permitted: true},
			{Allowed: []string{"ghcr.io/acme/"}, Registry: "ghcr.io", Repository: "acmeevil/migrate", Permitted: false},
			{Allowed: []string{"ghcr.io/acme/migrate"}, Registry: "ghcr.io", Repository: "acme/migrate", Permitted: true},
			{Allowed: []string{"ghcr.io/acme/migrate"}, Registry: "ghcr.io", Repository: "acme/migrate-evil", Permitted: false},
			{Allowed: []string{"ghcr.io/"}, Registry: "ghcr.io", Repository: "anyone/anything", Permitted: true},
			{Allowed: []string{"docker.io/library/"}, Registry: "docker.io", Repository: "library/alpine", Permitted: true},
			{Allowed: []string{"docker.io/library/"}, Registry: "docker.io", Repository: "acme/alpine", Permitted: false},
			{Allowed: []string{"localhost:5001/"}, Registry: "localhost:5001", Repository: "orkstack/activity-shell", Permitted: true},
			{Allowed: []string{"GHCR.io/acme/"}, Invalid: "not lower-case"},
			{Allowed: []string{"acme/"}, Invalid: "write docker.io/acme/"},
			{Allowed: []string{"ghcr.io/acme/*"}, Invalid: `"*"`},
			{Allowed: nil, Registry: "docker.io", Repository: "anyone/anything", Permitted: true},
		},
		Digests: []DigestVector{
			{Reference: "shell", Permitted: true},
			{Reference: "ghcr.io/acme/migrate@sha256:" + sixtyFourHex, Permitted: true},
			{Reference: "ghcr.io/acme/migrate:v3", Permitted: false},
			{Reference: "ghcr.io/acme/migrate", Permitted: false},
			{Reference: "alpine:3.20", Permitted: false},
		},
		FixedSigningKeyPEM:         FixedSigningKeyPEM,
		FixedSigningKeyFingerprint: FixedSigningKeyFingerprint,
	}
}

const sixtyFourHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

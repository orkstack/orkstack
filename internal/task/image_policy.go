package task

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The image supply-chain rule (AGENTS.md §5: "Leave room for later policies
// such as requireDigest, signed-image checks, and allowed registries").
//
// This file is the ONE place the rule is stated. It is pure — no registry, no
// clock, no store — so every gate reaches it: the engine at validate, apply
// and submit (internal/api), the store before it writes a document
// (pkg/artifacts), and the Orker before it pulls (internal/orker). Signature
// verification needs a registry and lives in internal/dsl/signature.go; the
// parts of it that do not (the key and the signer spelling) are here.

// Reason codes for a policy refusal. They are Error.code strings, reach the
// author and the operator verbatim, and are re-exported by internal/orker
// (failure.go) so the engine and the Orker spell one refusal one way.
const (
	// ReasonImageNotPermitted: the reference is a mutable tag under
	// require_digest, or the resolved registry/repository is outside
	// allowed_registries, or the selection names a signer with no usable key.
	ReasonImageNotPermitted = "IMAGE_NOT_PERMITTED"
	// ReasonImageNotSigned: the registry has no cosign signature object for
	// the digest (the .sig tag is 404).
	ReasonImageNotSigned = "IMAGE_NOT_SIGNED"
	// ReasonImageSignatureInvalid: signatures exist and none verifies with
	// the named key over this digest.
	ReasonImageSignatureInvalid = "IMAGE_SIGNATURE_INVALID"
	// ReasonImageSignatureUnavailable: the registry could not be asked
	// (unreachable, 5xx, unauthorized). Never a pass; retryable.
	ReasonImageSignatureUnavailable = "IMAGE_SIGNATURE_UNAVAILABLE"
)

// Bounds on the policy document, and the one signer scheme.
const (
	MaxAllowedRegistries    = 64
	MaxAllowedRegistryBytes = 255
	MaxSigningKeys          = 16
	MaxSigningKeyPEMBytes   = 4096
	MaxSigningKeyNameBytes  = 64
	SignerSchemeCosign      = "cosign"
)

// imagePolicyCommand is the command every refusal points at: the one read
// that shows the policy in force.
const imagePolicyCommand = "orkctl admin image-policy get"

// signingKeyNamePattern is the slug rule every other OrkStack name follows.
var signingKeyNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ValidRegistryPrefix reports why one allowed_registries entry cannot be
// stored, or nil. The rule, stated once:
//   - 1..MaxAllowedRegistryBytes bytes, no whitespace, no control character,
//     no "*", no "@", no ",";
//   - a trailing "/" is permitted and ignored for matching;
//   - the first "/"-segment is a host: it contains "." or ":" or equals
//     "localhost" — the same test dsl.ParseReference uses to tell a host
//     from a Docker Hub namespace — so "acme/" is refused with the sentence
//     `write docker.io/acme/`;
//   - ":" may appear only in that first segment (a port);
//   - it is lower-case (a registry host and an OCI repository path are).
func ValidRegistryPrefix(entry string) error {
	if entry == "" {
		return fmt.Errorf("allowed registry entry is empty; write a {registry}/{repository} prefix such as ghcr.io/acme/")
	}
	if len(entry) > MaxAllowedRegistryBytes {
		return fmt.Errorf("allowed registry %q is %d bytes; the limit is %d",
			entry, len(entry), MaxAllowedRegistryBytes)
	}
	for _, r := range entry {
		switch {
		case unicode.IsSpace(r):
			return fmt.Errorf("allowed registry %q contains whitespace", entry)
		case unicode.IsControl(r):
			return fmt.Errorf("allowed registry %q contains a control character", entry)
		case r == '*':
			return fmt.Errorf("allowed registry %q contains \"*\"; entries are literal prefixes "+
				"matched on \"/\" boundaries, with no wildcard (ghcr.io/acme/ already covers every "+
				"repository under it)", entry)
		case r == '@':
			return fmt.Errorf("allowed registry %q contains \"@\"; an entry is a registry/repository "+
				"prefix, never a digest", entry)
		case r == ',':
			return fmt.Errorf("allowed registry %q contains \",\"; give one prefix per entry", entry)
		}
	}
	if strings.ToLower(entry) != entry {
		return fmt.Errorf("allowed registry %q is not lower-case; registry hosts and OCI "+
			"repository paths are, so write %q", entry, strings.ToLower(entry))
	}
	trimmed := strings.TrimSuffix(entry, "/")
	if trimmed == "" {
		return fmt.Errorf("allowed registry %q names no registry", entry)
	}
	host, rest, _ := strings.Cut(trimmed, "/")
	if host == "" {
		return fmt.Errorf("allowed registry %q begins with \"/\"; it must begin with a registry host", entry)
	}
	if !(host == "localhost" || strings.ContainsAny(host, ".:")) {
		return fmt.Errorf("allowed registry %q does not begin with a registry host; "+
			"a first segment with no \".\" or \":\" is a Docker Hub namespace, so write docker.io/%s",
			entry, entry)
	}
	if strings.Contains(rest, ":") {
		return fmt.Errorf("allowed registry %q contains \":\" after the host; an entry is a "+
			"registry/repository prefix, never a tag", entry)
	}
	if strings.Contains(trimmed, "//") {
		return fmt.Errorf("allowed registry %q contains an empty path segment", entry)
	}
	return nil
}

// ValidRegistryPrefixes applies ValidRegistryPrefix to every entry, refuses
// more than MaxAllowedRegistries and any duplicate after trailing-"/"
// normalization, naming the entry and its index.
func ValidRegistryPrefixes(entries []string) error {
	if len(entries) > MaxAllowedRegistries {
		return fmt.Errorf("allowed_registries has %d entries; the limit is %d",
			len(entries), MaxAllowedRegistries)
	}
	seen := make(map[string]int, len(entries))
	for i, entry := range entries {
		if err := ValidRegistryPrefix(entry); err != nil {
			return fmt.Errorf("allowed_registries[%d]: %w", i, err)
		}
		key := strings.TrimSuffix(entry, "/")
		if first, dup := seen[key]; dup {
			return fmt.Errorf("allowed_registries[%d] %q duplicates allowed_registries[%d] %q",
				i, entry, first, entries[first])
		}
		seen[key] = i
	}
	return nil
}

// RegistryAllowed reports whether an image's "{registry}/{repository}" —
// ImageRef.registry + "/" + ImageRef.repository, already normalized by the
// resolver (docker.io/library/alpine, never alpine) — is covered by the
// list. An empty list permits everything: "no restriction", never
// "restriction to nothing", the reading Namespace.allowed_pools has. An
// entry matches when, with any trailing "/" removed, it equals the name or
// is a prefix of it followed by "/". ghcr.io/acme therefore matches
// ghcr.io/acme/migrate and never ghcr.io/acmeevil/migrate.
func RegistryAllowed(allowed []string, registry, repository string) bool {
	if len(allowed) == 0 {
		return true
	}
	name := registry + "/" + repository
	for _, entry := range allowed {
		prefix := strings.TrimSuffix(entry, "/")
		if prefix == "" {
			continue
		}
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			return true
		}
	}
	return false
}

// ValidSigningKeyName is the slug rule (^[a-z][a-z0-9-]*$) bounded at
// MaxSigningKeyNameBytes.
func ValidSigningKeyName(name string) error {
	if name == "" {
		return fmt.Errorf("signing key name is empty")
	}
	if len(name) > MaxSigningKeyNameBytes {
		return fmt.Errorf("signing key name %q is %d bytes; the limit is %d",
			name, len(name), MaxSigningKeyNameBytes)
	}
	if !signingKeyNamePattern.MatchString(name) {
		return fmt.Errorf("signing key name %q must match %s", name, signingKeyNamePattern)
	}
	return nil
}

// ParseSigningKey decodes one PEM "PUBLIC KEY" block into an ECDSA P-256
// public key — what `cosign generate-key-pair` writes to cosign.pub. Any
// other block type, algorithm or curve is refused naming what it found.
func ParseSigningKey(data []byte) (*ecdsa.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("not a PEM block; expected a PEM \"PUBLIC KEY\" (cosign.pub)")
	}
	if strings.TrimSpace(string(rest)) != "" {
		return nil, fmt.Errorf("carries data after its PEM %q block; expected exactly one \"PUBLIC KEY\"",
			block.Type)
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("is a PEM %q block; expected a \"PUBLIC KEY\" (cosign.pub) — "+
			"a signing key is public material and OrkStack never holds a private key", block.Type)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("PEM \"PUBLIC KEY\" block is not a PKIX public key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("is a %T public key; cosign key-based signing uses ECDSA P-256", parsed)
	}
	if key.Curve != elliptic.P256() {
		return nil, fmt.Errorf("is an ECDSA %s public key; cosign key-based signing uses P-256",
			key.Curve.Params().Name)
	}
	return key, nil
}

// SigningKeyFingerprint is "sha256:<64 hex>" over the DER-encoded
// SubjectPublicKeyInfo. It is what a listing, an audit record and a refusal
// print in place of the PEM.
func SigningKeyFingerprint(key *ecdsa.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// SigningKeyPEMFingerprint is SigningKeyFingerprint over a PEM, or "" when the
// PEM does not parse. For renderers, which must print something for a stored
// key and never the block itself.
func SigningKeyPEMFingerprint(keyPEM string) string {
	key, err := ParseSigningKey([]byte(keyPEM))
	if err != nil {
		return ""
	}
	return SigningKeyFingerprint(key)
}

// ValidSigningKeys validates a name→PEM map: every name and every key, at
// most MaxSigningKeys entries, each PEM at most MaxSigningKeyPEMBytes.
func ValidSigningKeys(keys map[string]string) error {
	if len(keys) > MaxSigningKeys {
		return fmt.Errorf("signing_keys has %d entries; the limit is %d", len(keys), MaxSigningKeys)
	}
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ValidSigningKeyName(name); err != nil {
			return fmt.Errorf("signing_keys: %w", err)
		}
		keyPEM := keys[name]
		if len(keyPEM) > MaxSigningKeyPEMBytes {
			return fmt.Errorf("signing key %q is %d bytes of PEM; the limit is %d",
				name, len(keyPEM), MaxSigningKeyPEMBytes)
		}
		if _, err := ParseSigningKey([]byte(keyPEM)); err != nil {
			return fmt.Errorf("signing key %q %w", name, err)
		}
	}
	return nil
}

// ParseSigner splits Namespace.require_signed_by. Only "cosign:<name>" is
// accepted, with <name> a valid signing key name; anything else is refused
// with `require_signed_by "…" is not cosign:<key-name>; cosign is the only
// signer this deployment understands`. Empty is ("", "", nil): inherit.
func ParseSigner(requireSignedBy string) (scheme, name string, err error) {
	if requireSignedBy == "" {
		return "", "", nil
	}
	scheme, name, found := strings.Cut(requireSignedBy, ":")
	if !found || scheme != SignerSchemeCosign || ValidSigningKeyName(name) != nil {
		return "", "", fmt.Errorf("require_signed_by %q is not cosign:<key-name>; "+
			"cosign is the only signer this deployment understands", requireSignedBy)
	}
	return scheme, name, nil
}

// Selection composes the policy in force for a namespace: the deployment
// floor plus the namespace's tightening. It returns nil — meaning "no
// policy, unset on the wire" — when require_digest is false, the registry
// list is empty and the namespace names no signer, which is what keeps
// recorded payloads byte-identical for a deployment that opted into
// nothing. A namespace naming a key the policy does not hold returns an
// error rather than a selection with an empty key: the engine refuses such
// a write, so this is a consistency check, not a path.
func Selection(policy *orkstackv1.ImagePolicy, ns *orkstackv1.Namespace) (*orkstackv1.ImagePolicySelection, error) {
	signer := ns.GetRequireSignedBy()
	_, keyName, err := ParseSigner(signer)
	if err != nil {
		return nil, fmt.Errorf("namespace %q %w", ns.GetName(), err)
	}
	if !policy.GetRequireDigest() && len(policy.GetAllowedRegistries()) == 0 && signer == "" {
		return nil, nil
	}
	sel := &orkstackv1.ImagePolicySelection{
		RequireDigest:     policy.GetRequireDigest(),
		AllowedRegistries: append([]string(nil), policy.GetAllowedRegistries()...),
	}
	if signer != "" {
		keyPEM, ok := policy.GetSigningKeys()[keyName]
		if !ok || keyPEM == "" {
			return nil, fmt.Errorf("namespace %q require_signed_by %q names a signing key this "+
				"deployment does not hold; known keys: %s (orkctl admin image-policy set --signing-key)",
				ns.GetName(), signer, KnownSigningKeys(policy))
		}
		sel.RequireSignedBy = signer
		sel.SigningKeyPem = keyPEM
	}
	return sel, nil
}

// KnownSigningKeys renders the policy's key names, sorted, or "(none)".
func KnownSigningKeys(policy *orkstackv1.ImagePolicy) string {
	names := make([]string, 0, len(policy.GetSigningKeys()))
	for name := range policy.GetSigningKeys() {
		names = append(names, name)
	}
	if len(names) == 0 {
		return "(none)"
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// ImagePolicyRefusal is one task's failure against the pure half of the
// rule. Callers prefix nothing and rephrase nothing; Field() anchors it.
type ImagePolicyRefusal struct {
	TaskIndex int    // position in Orkflow.tasks
	Task      string // task id
	Reference string // ImageRef.reference, as the author wrote it
	Resolved  string // "{registry}/{repository}"
	Code      string // ReasonImageNotPermitted
	Allowed   []string
	// Digest is true when the refusal is require_digest's (a mutable tag),
	// false when it is allowed_registries'.
	Digest bool
}

// Error renders the refusal's one sentence.
func (e *ImagePolicyRefusal) Error() string {
	if e.Digest {
		return fmt.Sprintf("task %q: image %q is a mutable tag, and this deployment requires a digest "+
			"— write %s@sha256:<digest> (%s)", e.Task, e.Reference, e.Resolved, imagePolicyCommand)
	}
	return fmt.Sprintf("task %q: image %q resolves to %s, which is outside the registries this "+
		"deployment allows: %s (%s)", e.Task, e.Reference, e.Resolved,
		strings.Join(e.Allowed, ", "), imagePolicyCommand)
}

// Field anchors the refusal to the task's `uses:`, e.g. "tasks[2].uses".
func (e *ImagePolicyRefusal) Field() string { return fmt.Sprintf("tasks[%d].uses", e.TaskIndex) }

// IsRegistryReference reports whether a `uses:` value is a registry
// reference rather than a catalog name: it contains any of "/", ":", "@" —
// the three runes dsl.looksLikeReference uses, and
// TestImagePolicy_RequireDigestAgreesWithLooksLikeReference pins the two
// together.
func IsRegistryReference(uses string) bool {
	return strings.ContainsAny(uses, "/:@")
}

// satisfiesRequireDigest judges the reference AS THE AUTHOR WROTE IT: a
// catalog name is exempt, and a registry reference passes only when it pins
// a sha256 digest.
func satisfiesRequireDigest(reference string) bool {
	if !IsRegistryReference(reference) {
		return true
	}
	return strings.Contains(reference, "@sha256:")
}

// CheckImage is the I/O-free check of one resolved image against a
// selection: require_digest, then allowed_registries. It is what the Orker
// runs at execute's STEP 3b — after its retry-start commit check, never in
// Validate, which precedes that check — and what CheckOrkflow runs per task.
// A nil selection permits everything without looking at the image.
func CheckImage(sel *orkstackv1.ImagePolicySelection, taskIndex int, taskID string, image *orkstackv1.ImageRef) *ImagePolicyRefusal {
	if sel == nil || image == nil {
		return nil
	}
	resolved := image.GetRegistry() + "/" + image.GetRepository()
	if sel.GetRequireDigest() && !satisfiesRequireDigest(image.GetReference()) {
		return &ImagePolicyRefusal{
			TaskIndex: taskIndex, Task: taskID, Reference: image.GetReference(),
			Resolved: resolved, Code: ReasonImageNotPermitted, Digest: true,
		}
	}
	if !RegistryAllowed(sel.GetAllowedRegistries(), image.GetRegistry(), image.GetRepository()) {
		return &ImagePolicyRefusal{
			TaskIndex: taskIndex, Task: taskID, Reference: image.GetReference(),
			Resolved: resolved, Code: ReasonImageNotPermitted,
			Allowed: append([]string(nil), sel.GetAllowedRegistries()...),
		}
	}
	return nil
}

// CheckOrkflow walks Orkflow.tasks in document order and returns the FIRST
// refusal: the same "one actionable error, then fix and re-run" shape every
// other preparation refusal has. It requires Task.image to be resolved
// (it runs after resolveImages); a task with a nil image is skipped, which
// is the air-gapped, resolver-less unit-test configuration.
func CheckOrkflow(sel *orkstackv1.ImagePolicySelection, wf *orkstackv1.Orkflow) error {
	if sel == nil {
		return nil
	}
	for i, t := range wf.GetTasks() {
		if t.GetImage() == nil {
			continue
		}
		if refusal := CheckImage(sel, i, t.GetId(), t.GetImage()); refusal != nil {
			return refusal
		}
	}
	return nil
}

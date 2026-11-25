package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// A namespace's identity policy (docs/TASK_IDENTITY.md): the audiences its
// tasks may ask for, its grants, whether only a current version may hold an
// identity, and how many attempts past a task's own retries may hold one.
// It shares the namespace's id record under the prefix only engines write,
// so it belongs to the incarnation the id is bound to: a namespace made
// again starts with none. Each change is written against the generation the
// writer read, with an immutable revision written before the swap:
//
//	metadata/identity/namespaces/{namespace}.json                      (its "policy" member)
//	metadata/identity/namespaces/{namespace}/revisions/{gen:012}-{sha256}.json
const (
	// MaxGrants is the most grants one namespace holds.
	MaxGrants = 64
	// MaxGrantOrkflows is the most Orkflows one grant lists.
	MaxGrantOrkflows = 64
	// MaxAudiences is the most audiences one namespace allows.
	MaxAudiences = 32

	policyFormat = 1
	// MaxPolicyBytes bounds a stored policy, so the id record that holds it
	// stays under its own bound and the id is always readable.
	MaxPolicyBytes = 32 << 10
)

// OriginWord is a run origin's kind as a grant's origins and an admission
// name it.
func OriginWord(kind orkstackv1.RunOriginKind) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(kind.String(), "RUN_ORIGIN_KIND_")), "_", "-")
}

// grantOrigin reports whether word is a kind of start a grant may serve: one
// a run can have. A word no run can have would serve no run, and one named
// for starts that are recorded as another kind would not bound them.
func grantOrigin(word string) bool {
	for n := range orkstackv1.RunOriginKind_name {
		kind := orkstackv1.RunOriginKind(n)
		if kind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && OriginWord(kind) == word {
			return true
		}
	}
	return false
}

var (
	// ErrPolicyChanged is a write from a generation that is no longer the
	// policy's: another write came first.
	ErrPolicyChanged = errors.New("the namespace's identity policy was changed by another write; read it again")
	// ErrInvalidPolicy is a policy that cannot be stored.
	ErrInvalidPolicy = errors.New("invalid identity policy")
	// ErrPolicyUnreadable is a stored policy that does not decode or is of
	// a format this release does not read. It is never read as "no
	// policy", and never written over.
	ErrPolicyUnreadable = errors.New("the namespace's identity policy is unreadable")

	grantNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
)

// NamespacePolicy is one namespace's identity policy. Generation, ChangedBy
// and ChangedAt are the store's.
type NamespacePolicy struct {
	Audiences             []string         `json:"audiences,omitempty"`
	Grants                map[string]Grant `json:"grants,omitempty"`
	IdentitiesCurrentOnly bool             `json:"identities_current_only,omitzero"`
	// AttemptsExtra is unset to take the deployment's.
	AttemptsExtra *int32 `json:"attempts_extra,omitzero"`
	Generation    int64  `json:"generation"`
	// ChangedBy is who wrote this generation, as the request recorded it:
	// verified when ChangedByVerified, else asserted by a gateway, and
	// empty when nothing named the caller.
	ChangedBy         string    `json:"changed_by,omitzero"`
	ChangedByVerified bool      `json:"changed_by_verified,omitzero"`
	ChangedAt         time.Time `json:"changed_at"`
}

// Grant is a named principal a namespace's tasks may act as, the
// Orkflows that may use it, and the kinds of start it serves (all, when
// Origins is unset).
type Grant struct {
	Provider    string   `json:"provider"`
	Principal   string   `json:"principal"`
	Orkflows    []string `json:"orkflows"`
	CurrentOnly bool     `json:"current_only,omitzero"`
	Origins     []string `json:"origins,omitempty"`
}

type policyWire struct {
	V int `json:"v"`
	NamespacePolicy
}

// PolicyWriter is who writes a policy, as the request recorded the caller.
type PolicyWriter struct {
	Principal string
	Verified  bool
}

// NamespacePolicy reads the policy of the namespace incarnation created at
// createdAt: ok false when there is no record, it is another incarnation's,
// or it holds no policy.
func (s *IncarnationStore) NamespacePolicy(ctx context.Context, namespace string, createdAt time.Time) (NamespacePolicy, bool, error) {
	ref, err := namespaceRef(namespace)
	if err != nil {
		return NamespacePolicy{}, false, err
	}
	rec, _, found, err := s.get(ctx, ref)
	if err != nil || !found || !rec.BoundTo.Equal(createdAt) || len(rec.Policy) == 0 {
		return NamespacePolicy{}, false, err
	}
	p, err := decodePolicy(rec.Policy)
	if err != nil {
		return NamespacePolicy{}, false, fmt.Errorf("artifacts: %s: %w", s.b.uri(ref.key), err)
	}
	return p, !policyIsEmpty(p), nil
}

// SetNamespacePolicy writes p as the policy of the namespace incarnation
// created at createdAt, against generation ifGeneration (0 for none yet),
// by principal by. It writes the change's revision first, then swaps the
// record; a writer that read another generation is ErrPolicyChanged. The
// record's id is kept, and minted when the incarnation has none.
func (s *IncarnationStore) SetNamespacePolicy(ctx context.Context, namespace string, createdAt time.Time, p NamespacePolicy, ifGeneration int64, by PolicyWriter) (NamespacePolicy, error) {
	ref, err := namespaceRef(namespace)
	if err != nil {
		return NamespacePolicy{}, err
	}
	for range incarnationTries {
		rec, version, found, err := s.get(ctx, ref)
		if err != nil {
			return NamespacePolicy{}, err
		}
		if !found || !rec.BoundTo.Equal(createdAt) {
			if err := s.ensurePolicyRecord(ctx, ref, createdAt); err != nil {
				return NamespacePolicy{}, err
			}
			continue
		}
		var generation int64
		if len(rec.Policy) > 0 {
			current, err := decodePolicy(rec.Policy)
			if err != nil {
				return NamespacePolicy{}, fmt.Errorf("artifacts: %s: %w", s.b.uri(ref.key), err)
			}
			generation = current.Generation
		}
		if generation != ifGeneration {
			return NamespacePolicy{}, fmt.Errorf("artifacts: namespace %s's identity policy is at generation %d, not %d: %w", namespace, generation, ifGeneration, ErrPolicyChanged)
		}
		next := p
		next.Generation, next.ChangedBy, next.ChangedByVerified, next.ChangedAt = generation+1, by.Principal, by.Verified, s.now().UTC()
		raw, err := encodePolicy(next)
		if err != nil {
			return NamespacePolicy{}, err
		}
		revision := fmt.Sprintf("%s/namespaces/%s/revisions/%012d-%s.json", IncarnationPrefix, namespace, next.Generation, hexDigest(raw))
		if err := s.b.create(ctx, revision, "application/json", raw); err != nil && !errors.Is(err, errBlobExists) {
			return NamespacePolicy{}, fmt.Errorf("artifacts: write %s: %w", s.b.uri(revision), err)
		}
		rec.Policy = raw
		body, err := stablejson.Marshal(rec)
		if err != nil {
			return NamespacePolicy{}, fmt.Errorf("artifacts: encode %s: %w", s.b.uri(ref.key), err)
		}
		err = s.b.replace(ctx, ref.key, "application/json", body, version)
		if errors.Is(err, errBlobChanged) || errors.Is(err, errBlobExists) {
			continue
		}
		if err != nil {
			return NamespacePolicy{}, fmt.Errorf("artifacts: write %s: %w", s.b.uri(ref.key), err)
		}
		decoded, err := decodePolicy(raw)
		if err != nil {
			return NamespacePolicy{}, err
		}
		return decoded, nil
	}
	return NamespacePolicy{}, fmt.Errorf("artifacts: %s: %w", ref, ErrIncarnationContended)
}

// ensurePolicyRecord makes the record a policy is written into: the
// incarnation's id record, or for `default`, whose id is a constant and is
// never minted, a record holding that constant.
func (s *IncarnationStore) ensurePolicyRecord(ctx context.Context, ref incarnationRef, createdAt time.Time) error {
	if ref.name != DefaultNamespace {
		_, err := s.ensure(ctx, ref, createdAt)
		return err
	}
	body, err := stablejson.Marshal(incarnationRecord{V: incarnationFormat, Kind: ref.kind, Name: ref.name,
		ID: DefaultNamespaceID, BoundTo: createdAt.UTC(), MintedAt: s.now().UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: encode %s: %w", s.b.uri(ref.key), err)
	}
	err = s.b.create(ctx, ref.key, "application/json", body)
	if err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write %s: %w", s.b.uri(ref.key), err)
	}
	return nil
}

// ClearNamespacePolicy removes the policy of the namespace incarnation
// created at createdAt and keeps its id: the first step of a delete, so a
// delete that stops half way leaves a namespace with no grants, never
// grants with no namespace. No record, or another incarnation's, is not an
// error.
func (s *IncarnationStore) ClearNamespacePolicy(ctx context.Context, namespace string, createdAt time.Time) error {
	ref, err := namespaceRef(namespace)
	if err != nil {
		return err
	}
	for range incarnationTries {
		rec, version, found, err := s.get(ctx, ref)
		if err != nil {
			return err
		}
		if !found || !rec.BoundTo.Equal(createdAt) || len(rec.Policy) == 0 {
			return nil
		}
		// The cleared policy keeps its generation, one higher, so a writer
		// that read an earlier one is refused rather than taken for current.
		// A policy this release cannot read is removed as it is.
		cleared := []byte(nil)
		if current, derr := decodePolicy(rec.Policy); derr == nil {
			if policyIsEmpty(current) {
				return nil
			}
			empty := NamespacePolicy{Generation: current.Generation + 1, ChangedAt: s.now().UTC()}
			if cleared, err = encodePolicy(empty); err != nil {
				return err
			}
		}
		rec.Policy = cleared
		body, err := stablejson.Marshal(rec)
		if err != nil {
			return fmt.Errorf("artifacts: encode %s: %w", s.b.uri(ref.key), err)
		}
		err = s.b.replace(ctx, ref.key, "application/json", body, version)
		if errors.Is(err, errBlobChanged) || errors.Is(err, errBlobExists) {
			continue
		}
		if err != nil {
			return fmt.Errorf("artifacts: write %s: %w", s.b.uri(ref.key), err)
		}
		return nil
	}
	return fmt.Errorf("artifacts: %s: %w", ref, ErrIncarnationContended)
}

// CheckNamespacePolicy says why the store would refuse p's audiences,
// grants and attempts_extra, or nil; who writes it and when are the store's.
func CheckNamespacePolicy(p NamespacePolicy) error {
	p.Generation, p.ChangedAt = 1, time.Unix(1, 0)
	p.ChangedBy, p.ChangedByVerified = "", false
	if reason := checkPolicy(p); reason != "" {
		return fmt.Errorf("identity policy: %s: %w", reason, ErrInvalidPolicy)
	}
	return nil
}

// ClearNamespacePolicy does not decode what it removes, so a delete works
// even over a policy this release cannot read.

// encodePolicy is a policy's stored bytes; a policy that cannot be stored
// is ErrInvalidPolicy.
func encodePolicy(p NamespacePolicy) ([]byte, error) {
	p.ChangedAt = p.ChangedAt.UTC()
	if reason := checkPolicy(p); reason != "" {
		return nil, fmt.Errorf("artifacts: identity policy: %s: %w", reason, ErrInvalidPolicy)
	}
	raw, err := stablejson.Marshal(policyWire{V: policyFormat, NamespacePolicy: p})
	if err != nil {
		return nil, fmt.Errorf("artifacts: identity policy: %w", err)
	}
	if len(raw) > MaxPolicyBytes {
		return nil, fmt.Errorf("artifacts: identity policy of %d bytes is over %d: %w", len(raw), MaxPolicyBytes, ErrInvalidPolicy)
	}
	return raw, nil
}

// decodePolicy reads a stored policy. Only the exact bytes encodePolicy
// writes are a policy.
func decodePolicy(raw []byte) (NamespacePolicy, error) {
	version, bad := peekVersion(raw)
	if bad != "" {
		return NamespacePolicy{}, fmt.Errorf("%s: %w", bad, ErrPolicyUnreadable)
	}
	if version != policyFormat {
		return NamespacePolicy{}, fmt.Errorf("format %d: %w", version, ErrPolicyUnreadable)
	}
	var w policyWire
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return NamespacePolicy{}, fmt.Errorf("does not decode: %v: %w", err, ErrPolicyUnreadable)
	}
	again, err := encodePolicy(w.NamespacePolicy)
	if err != nil {
		return NamespacePolicy{}, fmt.Errorf("%v: %w", err, ErrPolicyUnreadable)
	}
	if string(again) != string(raw) {
		return NamespacePolicy{}, fmt.Errorf("not as a policy is written: %w", ErrPolicyUnreadable)
	}
	return w.NamespacePolicy, nil
}

// checkPolicy says why p cannot be stored, or "".
func checkPolicy(p NamespacePolicy) string {
	if len(p.Audiences) > MaxAudiences {
		return fmt.Sprintf("%d audiences is more than %d", len(p.Audiences), MaxAudiences)
	}
	if reason := distinct("audience", p.Audiences, func(a string) bool { return text(a) && plainText(a, false) }); reason != "" {
		return reason
	}
	if p.Audiences != nil && len(p.Audiences) == 0 {
		return "no audiences is written as none"
	}
	if p.AttemptsExtra != nil && (*p.AttemptsExtra < 0 || *p.AttemptsExtra > MaxAttemptsExtra) {
		return fmt.Sprintf("attempts_extra %d is not 0 to %d", *p.AttemptsExtra, MaxAttemptsExtra)
	}
	if len(p.Grants) > MaxGrants {
		return fmt.Sprintf("%d grants is more than %d", len(p.Grants), MaxGrants)
	}
	if p.Grants != nil && len(p.Grants) == 0 {
		return "no grants is written as none"
	}
	for name, g := range p.Grants {
		if !grantNamePattern.MatchString(name) {
			return fmt.Sprintf("grant name %q must match %s", name, grantNamePattern)
		}
		if !slug(g.Provider) || !text(g.Principal) || !plainText(g.Principal, false) {
			return fmt.Sprintf("grant %q needs a provider and a principal", name)
		}
		if len(g.Orkflows) < 1 || len(g.Orkflows) > MaxGrantOrkflows {
			return fmt.Sprintf("grant %q lists %d Orkflows, not 1 to %d", name, len(g.Orkflows), MaxGrantOrkflows)
		}
		if reason := distinct("grant "+name+"'s Orkflow", g.Orkflows, grantOrkflow); reason != "" {
			return reason
		}
		if g.Origins != nil && len(g.Origins) == 0 {
			return fmt.Sprintf("grant %q's empty origins are written as none", name)
		}
		if reason := distinct("grant "+name+"'s origin", g.Origins, grantOrigin); reason != "" {
			return reason
		}
	}
	if p.Generation < 1 || len(p.ChangedBy) > maxAdmittedText || !plainText(p.ChangedBy, true) || (p.ChangedByVerified && p.ChangedBy == "") || p.ChangedAt.IsZero() {
		return "a policy needs a generation and when it was changed, and a verified writer is named"
	}
	return ""
}

// grantOrkflow says whether s is an Orkflow a grant may list: "*", a name
// of the namespace, or <owner>/<name> of a shared one.
func grantOrkflow(s string) bool {
	if s == "*" {
		return true
	}
	owner, name, shared := strings.Cut(s, "/")
	if !shared {
		return slug(s)
	}
	return slug(owner) && slug(name)
}

// slug is the rule every OrkStack name is held to: ^[a-z][a-z0-9-]*$, at
// most 200 bytes.
func slug(s string) bool {
	return len(s) <= maxPoolName && namespaceNamePattern.MatchString(s)
}

// distinct says why values are not each ok and named once, or "".
func distinct(what string, values []string, ok func(string) bool) string {
	seen := map[string]bool{}
	for _, v := range values {
		if !ok(v) {
			return fmt.Sprintf("%s %q is not one", what, v)
		}
		if seen[v] {
			return fmt.Sprintf("%s %q is named twice", what, v)
		}
		seen[v] = true
	}
	return ""
}

// policyIsEmpty says p allows nothing: no audience, grant or allowance.
func policyIsEmpty(p NamespacePolicy) bool {
	return len(p.Audiences) == 0 && len(p.Grants) == 0 && !p.IdentitiesCurrentOnly && p.AttemptsExtra == nil
}

// plainText says s is valid UTF-8 with no control, format or (unless
// spaces) whitespace character: what a policy stores becomes a token's
// claims and lines of logs and tables, so nothing in it may forge a line or
// hide a character.
func plainText(s string, spaces bool) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || (!spaces && unicode.IsSpace(r)) {
			return false
		}
	}
	return true
}

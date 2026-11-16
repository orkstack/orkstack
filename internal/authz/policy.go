package authz

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/jwt"
)

// The access document's bounds (docs/ACCESS.md, "The access document").
const (
	MaxCustomRoles         = 256
	MaxBindings            = 4096
	MaxSubjectsPerBinding  = 64
	MaxServiceAccounts     = 256
	MaxServiceAccountKeys  = 4
	MaxAccessDocumentBytes = 4 << 20
	// MaxSubjectBytes bounds one subject string, and MaxPrincipalIDBytes a
	// disabled principal's id, as authn bounds an id.
	MaxSubjectBytes     = 1024
	MaxPrincipalIDBytes = 256
	// maxIssuerNameBytes is a trusted issuer's name's bound (auth.issuers).
	maxIssuerNameBytes = 32
)

// AccessSchemaVersion is the only access document format this build reads or
// writes.
const AccessSchemaVersion = 1

// BreakGlassAccount is the service account name a break-glass assertion
// authenticates as; it is reserved, so no stored account takes it.
const BreakGlassAccount = "break-glass"

// ErrInvalidDocument wraps every refusal of an access document.
var ErrInvalidDocument = errors.New("invalid access document")

// Subject is the verified caller a decision is about, as authentication
// found it.
type Subject struct {
	ID     string
	Kind   string // "user" or "service"
	Issuer string // the trusted issuer's name, "serviceaccount", "cert" or "config"
	Groups []string
	Claims map[string]string
}

// Resource is what a request acts on, as narrow as the request names it. An
// empty Namespace and Pool is the deployment. NamespaceID and OrkflowID are
// the immutable ids of the incarnation the request reads, when it knows
// them; a binding pinned to another incarnation does not cover it.
type Resource struct {
	Namespace   string
	Orkflow     string
	Pool        string
	NamespaceID string
	OrkflowID   string
}

// Kind is the resource's scope kind.
func (r Resource) Kind() ScopeKind {
	switch {
	case r.Pool != "":
		return ScopePool
	case r.Namespace == "":
		return ScopeDeployment
	case r.Orkflow == "":
		return ScopeNamespace
	}
	return ScopeOrkflow
}

// String renders the resource as a scope: deployment, namespace:<ns>,
// orkflow:<ns>/<name> or pool:<pool>.
func (r Resource) String() string {
	switch r.Kind() {
	case ScopePool:
		return "pool:" + r.Pool
	case ScopeDeployment:
		return "deployment"
	case ScopeNamespace:
		return "namespace:" + r.Namespace
	}
	return "orkflow:" + r.Namespace + "/" + r.Orkflow
}

// Scope is a binding's scope.
type Scope struct {
	Kind      ScopeKind
	Namespace string
	Orkflow   string
	Pool      string
}

// ParseScope reads `deployment`, `namespace:<ns>`, `orkflow:<ns>/<name>` or
// `pool:<pool>`. Names are validated, never sanitized.
func ParseScope(s string) (Scope, error) {
	switch {
	case s == "deployment":
		return Scope{Kind: ScopeDeployment}, nil
	case strings.HasPrefix(s, "namespace:"):
		ns := strings.TrimPrefix(s, "namespace:")
		if err := dsl.ValidSlug("namespace", ns); err != nil {
			return Scope{}, fmt.Errorf("scope %q: %w", s, err)
		}
		return Scope{Kind: ScopeNamespace, Namespace: ns}, nil
	case strings.HasPrefix(s, "orkflow:"):
		ns, name, ok := strings.Cut(strings.TrimPrefix(s, "orkflow:"), "/")
		if !ok {
			return Scope{}, fmt.Errorf("scope %q: an Orkflow scope is orkflow:<namespace>/<name>", s)
		}
		if err := dsl.ValidSlug("namespace", ns); err != nil {
			return Scope{}, fmt.Errorf("scope %q: %w", s, err)
		}
		if err := dsl.ValidSlug("orkflow name", name); err != nil {
			return Scope{}, fmt.Errorf("scope %q: %w", s, err)
		}
		return Scope{Kind: ScopeOrkflow, Namespace: ns, Orkflow: name}, nil
	case strings.HasPrefix(s, "pool:"):
		pool := strings.TrimPrefix(s, "pool:")
		if err := dsl.ValidSlug("pool", pool); err != nil {
			return Scope{}, fmt.Errorf("scope %q: %w", s, err)
		}
		return Scope{Kind: ScopePool, Pool: pool}, nil
	}
	return Scope{}, fmt.Errorf("scope %q is not deployment, namespace:<ns>, orkflow:<ns>/<name> or pool:<pool>", s)
}

func (s Scope) String() string {
	switch s.Kind {
	case ScopeDeployment:
		return "deployment"
	case ScopeNamespace:
		return "namespace:" + s.Namespace
	case ScopeOrkflow:
		return "orkflow:" + s.Namespace + "/" + s.Orkflow
	case ScopePool:
		return "pool:" + s.Pool
	}
	return ""
}

// covers reports whether the scope covers the resource by name.
func (s Scope) covers(r Resource) bool {
	switch s.Kind {
	case ScopeDeployment:
		return true
	case ScopeNamespace:
		return r.Pool == "" && r.Namespace == s.Namespace
	case ScopeOrkflow:
		return r.Pool == "" && r.Namespace == s.Namespace && r.Orkflow == s.Orkflow
	case ScopePool:
		return r.Pool == s.Pool
	}
	return false
}

// The subject kinds, a closed set.
const (
	subjectUser           = "user"
	subjectGroup          = "group"
	subjectClaims         = "claims"
	subjectServiceAccount = "serviceaccount"
	subjectCert           = "cert"
)

// pattern is a literal, or a prefix when it ended in one `*`.
type pattern struct {
	value  string
	prefix bool
}

func parsePattern(what, v string) (pattern, error) {
	if v == "" {
		return pattern{}, fmt.Errorf("%s is empty", what)
	}
	if v == "*" {
		return pattern{}, fmt.Errorf("%s is only `*`, which would match anything", what)
	}
	if i := strings.IndexByte(v, '*'); i >= 0 && i != len(v)-1 {
		return pattern{}, fmt.Errorf("%s %q holds a `*` that is not its last character", what, v)
	}
	if strings.HasSuffix(v, "*") {
		return pattern{value: strings.TrimSuffix(v, "*"), prefix: true}, nil
	}
	return pattern{value: v}, nil
}

func (p pattern) matches(s string) bool {
	if p.prefix {
		return strings.HasPrefix(s, p.value)
	}
	return s == p.value
}

// Selector is one parsed subject of a binding.
type Selector struct {
	raw    string
	kind   string
	issuer string
	value  pattern
	claims []claimSelector
}

type claimSelector struct {
	name  string
	value pattern
}

// String is the subject as written.
func (s Selector) String() string { return s.raw }

// ParseSubject reads one subject: `user:<issuer>:<name>`,
// `group:<issuer>:<group>`, `claims:<issuer>:<k>=<v>,…`,
// `serviceaccount:<name>` or `cert:<name>`. A value is literal, or ends in
// one `*` that matches any remainder; a value of only `*`, or a `*` anywhere
// but the end, is refused, and so is a `claims:` subject with no selector.
func ParseSubject(s string) (Selector, error) {
	if len(s) > MaxSubjectBytes {
		return Selector{}, fmt.Errorf("subject is %d bytes, more than %d", len(s), MaxSubjectBytes)
	}
	if !printable(s) {
		return Selector{}, fmt.Errorf("subject %q holds a control character or invalid UTF-8", s)
	}
	kind, rest, ok := strings.Cut(s, ":")
	if !ok {
		return Selector{}, fmt.Errorf("subject %q has no kind (user, group, claims, serviceaccount or cert)", s)
	}
	sel := Selector{raw: s, kind: kind}
	switch kind {
	case subjectUser, subjectGroup, subjectClaims:
		issuer, value, ok := strings.Cut(rest, ":")
		if !ok {
			return Selector{}, fmt.Errorf("subject %q: a %s subject is %s:<issuer>:<value>", s, kind, kind)
		}
		if err := validIssuerName(issuer); err != nil {
			return Selector{}, fmt.Errorf("subject %q: %w", s, err)
		}
		sel.issuer = issuer
		if kind != subjectClaims {
			p, err := parsePattern("its value", value)
			if err != nil {
				return Selector{}, fmt.Errorf("subject %q: %w", s, err)
			}
			sel.value = p
			return sel, nil
		}
		if value == "" {
			return Selector{}, fmt.Errorf("subject %q selects on no claim: it would match every token of %s", s, issuer)
		}
		seen := map[string]bool{}
		for part := range strings.SplitSeq(value, ",") {
			name, v, ok := strings.Cut(part, "=")
			if !ok || name == "" {
				return Selector{}, fmt.Errorf("subject %q: each selector is <claim>=<value>", s)
			}
			if seen[name] {
				return Selector{}, fmt.Errorf("subject %q selects on claim %q twice", s, name)
			}
			seen[name] = true
			p, err := parsePattern("claim "+name, v)
			if err != nil {
				return Selector{}, fmt.Errorf("subject %q: %w", s, err)
			}
			sel.claims = append(sel.claims, claimSelector{name: name, value: p})
		}
		if !slices.ContainsFunc(sel.claims, func(c claimSelector) bool { return !c.value.prefix }) {
			return Selector{}, fmt.Errorf("subject %q: every selector is a wildcard; at least one must be literal", s)
		}
		return sel, nil
	case subjectServiceAccount, subjectCert:
		p, err := parsePattern("its name", rest)
		if err != nil {
			return Selector{}, fmt.Errorf("subject %q: %w", s, err)
		}
		sel.value = p
		return sel, nil
	}
	return Selector{}, fmt.Errorf("subject %q: kind %q is not user, group, claims, serviceaccount or cert", s, kind)
}

// Matches reports whether the subject selects the caller.
func (s Selector) Matches(sub *Subject) bool {
	if sub == nil {
		return false
	}
	switch s.kind {
	case subjectUser:
		prefix := "user:" + s.issuer + ":"
		return sub.Kind == "user" && sub.Issuer == s.issuer && strings.HasPrefix(sub.ID, prefix) &&
			s.value.matches(strings.TrimPrefix(sub.ID, prefix))
	case subjectGroup:
		return sub.Kind == "user" && sub.Issuer == s.issuer && slices.ContainsFunc(sub.Groups, s.value.matches)
	case subjectClaims:
		if sub.Kind != "service" || sub.Issuer != s.issuer {
			return false
		}
		for _, c := range s.claims {
			v, ok := claimValue(sub, c.name)
			if !ok || !c.value.matches(v) {
				return false
			}
		}
		return true
	case subjectServiceAccount:
		return strings.HasPrefix(sub.ID, "serviceaccount:") && s.value.matches(strings.TrimPrefix(sub.ID, "serviceaccount:"))
	case subjectCert:
		return strings.HasPrefix(sub.ID, "cert:") && s.value.matches(strings.TrimPrefix(sub.ID, "cert:"))
	}
	return false
}

// claimValue is one of a service principal's kept claims; `sub` is the one
// its id is made of (service:<issuer>:<sub>).
func claimValue(sub *Subject, name string) (string, bool) {
	if name == "sub" {
		prefix := "service:" + sub.Issuer + ":"
		if strings.HasPrefix(sub.ID, prefix) {
			return strings.TrimPrefix(sub.ID, prefix), true
		}
		return "", false
	}
	v, ok := sub.Claims[name]
	return v, ok
}

// namesOnly reports a literal selector that names exactly the principal id:
// a binding of the principal itself, not of a group, a claim or a prefix.
func (s Selector) namesOnly(id string) bool {
	if s.value.prefix {
		return false
	}
	switch s.kind {
	case subjectUser:
		return id == "user:"+s.issuer+":"+s.value.value
	case subjectServiceAccount:
		return id == "serviceaccount:"+s.value.value
	case subjectCert:
		return id == "cert:"+s.value.value
	}
	return false
}

// ClaimsReliedOn are the claims a `claims:` subject selects on, for an
// explanation; empty for another kind.
func (s Selector) ClaimsReliedOn() []string {
	out := make([]string, 0, len(s.claims))
	for _, c := range s.claims {
		out = append(out, c.name)
	}
	return out
}

func validIssuerName(name string) error {
	if len(name) > maxIssuerNameBytes {
		return fmt.Errorf("issuer name %q is longer than %d bytes", name, maxIssuerNameBytes)
	}
	return dsl.ValidSlug("issuer", name)
}

func printable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Binding is a compiled binding: the document's, or one configuration
// makes (auth.admins).
type Binding struct {
	Name        string
	Role        string
	Scope       Scope
	ScopeID     string
	Subjects    []Selector
	OrkerIDsAny bool
	// Rule is what a decision this binding admits records:
	// "binding:<name>", or "config:admins" for configuration's.
	Rule string
	// FromConfiguration is true for a binding configuration makes.
	FromConfiguration bool
}

// ConfigAdminsRule is the rule a decision admitted by auth.admins records.
const ConfigAdminsRule = "config:admins"

// BreakGlassRule is the rule a decision admitted by break-glass records.
const BreakGlassRule = "config:break-glass"

// Policy is the compiled access document a decision is made against, with
// the bindings configuration adds. It is immutable once compiled.
type Policy struct {
	// Revision is the document's revision; 0 for a deployment that never
	// wrote one.
	Revision int64
	// ReadAt is when the read of the document this was compiled from was
	// sent: how old a decision's document was.
	ReadAt time.Time

	roles      map[string]map[Permission]bool
	bindings   []Binding
	configured []Binding
	disabled   map[string]bool
	accounts   map[string]ServiceAccount
	// configOnly is a policy past its document's stale limit: only the
	// principals configuration names are served, catalog.read included.
	configOnly bool
}

// allBindings yields the document's bindings and then configuration's,
// without copying them: a decision walks them on every request.
func (p *Policy) allBindings(yield func(Binding) bool) {
	for _, b := range p.bindings {
		if !yield(b) {
			return
		}
	}
	for _, b := range p.configured {
		if !yield(b) {
			return
		}
	}
}

// Bindings lists the document's bindings and then configuration's.
func (p *Policy) Bindings() []Binding {
	out := slices.Clone(p.bindings)
	return append(out, p.configured...)
}

// ConfigurationOnly is the policy with the document's grants removed: what
// still authorizes once the document is past its stale limit. The
// document's `disabled` list is kept, so a principal disabled there stays
// disabled whatever configuration grants it.
func (p *Policy) ConfigurationOnly() *Policy {
	return &Policy{Revision: p.Revision, ReadAt: p.ReadAt, roles: map[string]map[Permission]bool{},
		configured: slices.Clone(p.configured), disabled: p.disabled, configOnly: true}
}

// Disabled reports whether id is under `disabled`.
func (p *Policy) Disabled(id string) bool { return p.disabled[id] }

// RolePermissions is a role's permissions, built-in or custom. The map is
// the policy's own: callers only read it.
func (p *Policy) RolePermissions(name string) (map[Permission]bool, bool) {
	if perms, ok := builtInRoles[name]; ok {
		return perms, true
	}
	perms, ok := p.roles[name]
	return perms, ok
}

// Configuration is what configuration contributes to every policy.
type Configuration struct {
	// Admins are auth.admins: subjects that hold admin at the deployment.
	Admins []string
	// BreakGlass is true when auth.break_glass_key_file is set: the
	// break-glass principal then holds admin at the deployment.
	BreakGlass bool
	// Orkers are auth.orkers: subjects that hold the orker role at a pool.
	Orkers []OrkerEntry
}

// OrkerEntry is one auth.orkers entry: a subject bound to the orker role at
// a pool by configuration, optionally for any Orker id the request's attempt
// names (orker_ids: any).
type OrkerEntry struct {
	Subject     string
	Pool        string
	OrkerIDsAny bool
}

// ConfigOrkersRule prefixes the rule a decision admitted by an auth.orkers
// entry records: config:orkers/<index>.
const ConfigOrkersRule = "config:orkers"

// CompileConfiguration validates configuration's subjects, once, at boot.
func CompileConfiguration(c Configuration) ([]Binding, error) {
	var out []Binding
	if c.BreakGlass {
		sel, err := ParseSubject("serviceaccount:" + BreakGlassAccount)
		if err != nil {
			return nil, err
		}
		out = append(out, Binding{Name: BreakGlassRule, Role: RoleAdmin, Scope: Scope{Kind: ScopeDeployment},
			Subjects: []Selector{sel}, Rule: BreakGlassRule, FromConfiguration: true})
	}
	seenOrker := map[OrkerEntry]bool{}
	for i, o := range c.Orkers {
		sel, err := ParseSubject(o.Subject)
		if err != nil {
			return nil, fmt.Errorf("auth.orkers[%d]: %w", i, err)
		}
		if err := dsl.ValidSlug("pool", o.Pool); err != nil {
			return nil, fmt.Errorf("auth.orkers[%d]: %w", i, err)
		}
		if seenOrker[o] {
			return nil, fmt.Errorf("auth.orkers lists subject %q at pool %q twice", o.Subject, o.Pool)
		}
		seenOrker[o] = true
		name := fmt.Sprintf("%s/%d", ConfigOrkersRule, i)
		out = append(out, Binding{Name: name, Role: RoleOrker, Scope: Scope{Kind: ScopePool, Pool: o.Pool},
			Subjects: []Selector{sel}, OrkerIDsAny: o.OrkerIDsAny, Rule: name, FromConfiguration: true})
	}
	if len(c.Admins) == 0 {
		return out, nil
	}
	b := Binding{
		Name: ConfigAdminsRule, Role: RoleAdmin, Scope: Scope{Kind: ScopeDeployment},
		Rule: ConfigAdminsRule, FromConfiguration: true,
	}
	seen := map[string]bool{}
	for _, s := range c.Admins {
		sel, err := ParseSubject(s)
		if err != nil {
			return nil, fmt.Errorf("auth.admins: %w", err)
		}
		if seen[s] {
			return nil, fmt.Errorf("auth.admins lists %q twice", s)
		}
		seen[s] = true
		b.Subjects = append(b.Subjects, sel)
	}
	return append(out, b), nil
}

// Compile turns a stored document into a policy, with configuration's
// bindings beside its own. It refuses what no decision could be made
// against (an unknown permission or role, a subject or scope that does not
// parse, a bound exceeded); the checks a write adds are Validate's. A nil
// document is a deployment that never wrote one: only configuration grants.
func Compile(doc *orkstackv1.AccessDocument, configured []Binding, readAt time.Time) (*Policy, error) {
	p := &Policy{ReadAt: readAt, roles: map[string]map[Permission]bool{}, disabled: map[string]bool{},
		configured: slices.Clone(configured)}
	if doc == nil {
		return p, nil
	}
	if v := doc.GetSchemaVersion(); v != AccessSchemaVersion {
		return nil, fmt.Errorf("%w: schema_version %d is not one this release reads (%d)", ErrInvalidDocument, v, AccessSchemaVersion)
	}
	p.Revision = doc.GetRevision()
	if n := len(doc.GetRoles()); n > MaxCustomRoles {
		return nil, fmt.Errorf("%w: %d custom roles, more than %d", ErrInvalidDocument, n, MaxCustomRoles)
	}
	for _, r := range doc.GetRoles() {
		if err := dsl.ValidSlug("role name", r.GetName()); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidDocument, err)
		}
		if IsBuiltInRole(r.GetName()) {
			return nil, fmt.Errorf("%w: role %q is a built-in role's name", ErrInvalidDocument, r.GetName())
		}
		if r.GetBuiltIn() {
			return nil, fmt.Errorf("%w: role %q: a stored role is never built in", ErrInvalidDocument, r.GetName())
		}
		if _, dup := p.roles[r.GetName()]; dup {
			return nil, fmt.Errorf("%w: role %q is declared twice", ErrInvalidDocument, r.GetName())
		}
		perms := make(map[Permission]bool, len(r.GetPermissions()))
		for _, name := range r.GetPermissions() {
			if !Known(Permission(name)) {
				return nil, fmt.Errorf("%w: role %q lists %q, which is not in the catalogue", ErrInvalidDocument, r.GetName(), name)
			}
			if perms[Permission(name)] {
				return nil, fmt.Errorf("%w: role %q lists %q twice", ErrInvalidDocument, r.GetName(), name)
			}
			perms[Permission(name)] = true
		}
		p.roles[r.GetName()] = perms
	}
	if n := len(doc.GetBindings()); n > MaxBindings {
		return nil, fmt.Errorf("%w: %d bindings, more than %d", ErrInvalidDocument, n, MaxBindings)
	}
	names := map[string]bool{}
	for _, b := range doc.GetBindings() {
		cb, err := compileBinding(b, p)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidDocument, err)
		}
		if names[cb.Name] {
			return nil, fmt.Errorf("%w: binding %q is declared twice", ErrInvalidDocument, cb.Name)
		}
		names[cb.Name] = true
		p.bindings = append(p.bindings, cb)
	}
	if n := len(doc.GetServiceAccounts()); n > MaxServiceAccounts {
		return nil, fmt.Errorf("%w: %d service accounts, more than %d", ErrInvalidDocument, n, MaxServiceAccounts)
	}
	p.accounts = make(map[string]ServiceAccount, len(doc.GetServiceAccounts()))
	for _, sa := range doc.GetServiceAccounts() {
		account, err := compileServiceAccount(sa)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidDocument, err)
		}
		if _, dup := p.accounts[sa.GetName()]; dup {
			return nil, fmt.Errorf("%w: service account %q is declared twice", ErrInvalidDocument, sa.GetName())
		}
		p.accounts[sa.GetName()] = account
	}
	for _, id := range doc.GetDisabled() {
		if id == "" || len(id) > MaxPrincipalIDBytes || !printable(id) {
			return nil, fmt.Errorf("%w: disabled principal %q is not a principal id of 1 to %d printable bytes", ErrInvalidDocument, id, MaxPrincipalIDBytes)
		}
		if p.disabled[id] {
			return nil, fmt.Errorf("%w: principal %q is disabled twice", ErrInvalidDocument, id)
		}
		p.disabled[id] = true
	}
	return p, nil
}

func compileBinding(b *orkstackv1.Binding, p *Policy) (Binding, error) {
	if err := dsl.ValidSlug("binding name", b.GetName()); err != nil {
		return Binding{}, err
	}
	if _, ok := p.RolePermissions(b.GetRole()); !ok {
		return Binding{}, fmt.Errorf("binding %q names role %q, which is neither built in nor declared", b.GetName(), b.GetRole())
	}
	scope, err := ParseScope(b.GetScope())
	if err != nil {
		return Binding{}, fmt.Errorf("binding %q: %w", b.GetName(), err)
	}
	if b.GetScopeId() != "" && scope.Kind != ScopeNamespace && scope.Kind != ScopeOrkflow {
		return Binding{}, fmt.Errorf("binding %q: only a namespace or Orkflow scope is pinned", b.GetName())
	}
	if b.GetOrkerIdsAny() && b.GetRole() != RoleOrker {
		return Binding{}, fmt.Errorf("binding %q: orker_ids_any is only for the %s role", b.GetName(), RoleOrker)
	}
	if b.GetFromConfiguration() {
		return Binding{}, fmt.Errorf("binding %q: a stored binding is never from configuration", b.GetName())
	}
	if b.GetPin() != orkstackv1.BindingPin_BINDING_PIN_UNSPECIFIED || b.GetPinnedAt() != nil {
		return Binding{}, fmt.Errorf("binding %q: pin and pinned_at are output only, never stored", b.GetName())
	}
	n := len(b.GetSubjects())
	if n == 0 || n > MaxSubjectsPerBinding {
		return Binding{}, fmt.Errorf("binding %q has %d subjects; a binding has 1 to %d", b.GetName(), n, MaxSubjectsPerBinding)
	}
	cb := Binding{Name: b.GetName(), Role: b.GetRole(), Scope: scope, ScopeID: b.GetScopeId(),
		OrkerIDsAny: b.GetOrkerIdsAny(), Rule: "binding:" + b.GetName()}
	seen := map[string]bool{}
	for _, s := range b.GetSubjects() {
		sel, err := ParseSubject(s)
		if err != nil {
			return Binding{}, fmt.Errorf("binding %q: %w", b.GetName(), err)
		}
		if seen[s] {
			return Binding{}, fmt.Errorf("binding %q lists subject %q twice", b.GetName(), s)
		}
		seen[s] = true
		cb.Subjects = append(cb.Subjects, sel)
	}
	return cb, nil
}

// ServiceAccount is one service account of the document, compiled: the keys
// its assertions verify under, by RFC 7638 thumbprint, and the Orker it
// names, if any.
type ServiceAccount struct {
	Name    string
	Keys    jwt.Static
	OrkerID string
}

// ServiceAccount returns the document's service account name. A policy past
// its document's stale limit serves configuration's principals only, so it
// knows no service account.
func (p *Policy) ServiceAccount(name string) (ServiceAccount, bool) {
	if p == nil || p.configOnly {
		return ServiceAccount{}, false
	}
	sa, ok := p.accounts[name]
	return sa, ok
}

func compileServiceAccount(sa *orkstackv1.ServiceAccount) (ServiceAccount, error) {
	out := ServiceAccount{Name: sa.GetName(), OrkerID: sa.GetOrkerId(), Keys: jwt.Static{}}
	if err := dsl.ValidSlug("service account name", sa.GetName()); err != nil {
		return out, err
	}
	if sa.GetName() == BreakGlassAccount {
		return out, fmt.Errorf("service account name %q is reserved for break-glass", BreakGlassAccount)
	}
	if n := len(sa.GetKeys()); n > MaxServiceAccountKeys {
		return out, fmt.Errorf("service account %q has %d keys, more than %d", sa.GetName(), n, MaxServiceAccountKeys)
	}
	if id := sa.GetOrkerId(); id != "" && (len(id) > MaxPrincipalIDBytes || !printable(id)) {
		return out, fmt.Errorf("service account %q: orker_id is not 1 to %d printable bytes", sa.GetName(), MaxPrincipalIDBytes)
	}
	kids := map[string]bool{}
	for i, k := range sa.GetKeys() {
		key, err := jwt.ParsePublicKeyPEM([]byte(k.GetPublicKeyPem()))
		if err != nil {
			return out, fmt.Errorf("service account %q key %d: %w", sa.GetName(), i, err)
		}
		thumb, err := key.Thumbprint()
		if err != nil {
			return out, fmt.Errorf("service account %q key %d: %w", sa.GetName(), i, err)
		}
		if kid := k.GetKid(); kid != "" && kid != thumb {
			return out, fmt.Errorf("service account %q key %d: kid is not the key's RFC 7638 thumbprint", sa.GetName(), i)
		}
		if kids[thumb] {
			return out, fmt.Errorf("service account %q lists one key twice", sa.GetName())
		}
		kids[thumb] = true
		out.Keys[thumb] = key
	}
	return out, nil
}

// IssuerFacts is what validation needs to know of one trusted issuer.
type IssuerFacts struct {
	// Claims are the claims a binding may select on, besides `sub`.
	Claims []string
}

// ValidateOptions are the facts a write is validated against.
type ValidateOptions struct {
	// Issuer looks a trusted issuer up by name.
	Issuer func(name string) (IssuerFacts, bool)
	// NamespaceExists reports whether a namespace exists.
	NamespaceExists func(name string) bool
	// AdminsConfigured is true when auth.admins names at least one subject.
	AdminsConfigured bool
}

// Validate is Compile with the rules a write adds (authn-authz.md §6.2): a
// namespace scope naming no namespace, a selector on a claim its issuer does
// not keep, and a document that leaves nobody but configuration able to
// hold access.write while auth.admins is empty are refused. It returns the
// warnings a write shows: a scope naming an Orkflow's namespace that does
// not exist yet, and a subject naming an issuer this engine does not trust.
func Validate(doc *orkstackv1.AccessDocument, opts ValidateOptions) (warnings []string, err error) {
	p, err := Compile(doc, nil, time.Time{})
	if err != nil {
		return nil, err
	}
	writers := false
	for _, b := range p.bindings {
		switch b.Scope.Kind {
		case ScopeNamespace:
			if opts.NamespaceExists != nil && !opts.NamespaceExists(b.Scope.Namespace) {
				return nil, fmt.Errorf("%w: binding %q names namespace %q, which does not exist", ErrInvalidDocument, b.Name, b.Scope.Namespace)
			}
		case ScopeOrkflow:
			if opts.NamespaceExists != nil && !opts.NamespaceExists(b.Scope.Namespace) {
				warnings = append(warnings, fmt.Sprintf("binding %q names namespace %q, which does not exist yet", b.Name, b.Scope.Namespace))
			}
		}
		for _, sel := range b.Subjects {
			if sel.issuer == "" || opts.Issuer == nil {
				continue
			}
			facts, ok := opts.Issuer(sel.issuer)
			if !ok {
				warnings = append(warnings, fmt.Sprintf("binding %q: subject %q names issuer %q, which this engine does not trust", b.Name, sel.raw, sel.issuer))
				continue
			}
			for _, c := range sel.claims {
				if c.name != "sub" && !slices.Contains(facts.Claims, c.name) {
					return nil, fmt.Errorf("%w: binding %q: subject %q selects on claim %q, which issuer %q does not keep (its claims list)",
						ErrInvalidDocument, b.Name, sel.raw, c.name, sel.issuer)
				}
			}
		}
		if perms, _ := p.RolePermissions(b.Role); b.Scope.Kind == ScopeDeployment && perms[PermAccessWrite] {
			writers = true
		}
	}
	if !writers && !opts.AdminsConfigured {
		return nil, fmt.Errorf("%w: no binding at the deployment grants %s and auth.admins is empty: nobody could change access again",
			ErrInvalidDocument, PermAccessWrite)
	}
	return warnings, nil
}

// PinsWithin reports whether any binding is pinned to an incarnation inside
// namespace: the namespace itself or one of its Orkflows. Only then can a
// decision there depend on which incarnation a request reads, so only then
// need its ids be read (authn-authz.md §6.2).
func (p *Policy) PinsWithin(namespace string) bool {
	if p == nil {
		return false
	}
	for b := range p.allBindings {
		if b.ScopeID != "" && b.Scope.Namespace == namespace {
			return true
		}
	}
	return false
}

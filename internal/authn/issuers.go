package authn

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/jwt"
	"go.yaml.in/yaml/v4"
)

// IssuersSetting names the trusted-issuers document in errors.
const IssuersSetting = "auth.issuers"

// DefaultMaxStale is how long an issuer's key set is served past its age
// when it cannot be refreshed (authn-authz.md §3.1, §6.2).
const DefaultMaxStale = time.Hour

// issuerNamePattern: a name appears in principal ids, so it is a short slug.
var issuerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// publicMultiTenantHosts are issuers that mint for every tenant they have.
// dedicated: true is refused for them (authn-authz.md §3.1). The list guards
// against copying an example onto a public issuer; it is not the control
// and is not complete. A Microsoft host is listed only for its common
// endpoints, by path.
var publicMultiTenantHosts = map[string][]string{
	"token.actions.githubusercontent.com": nil,
	"gitlab.com":                          nil,
	"agent.buildkite.com":                 nil,
	"app.terraform.io":                    nil,
	"oidc.circleci.com":                   nil,
	"accounts.google.com":                 nil,
	"login.microsoftonline.com":           {"/common", "/organizations", "/consumers"},
}

// Issuer is one trusted issuer, as the document declares it and validated.
type Issuer struct {
	Name                 string
	Issuer               string
	Kind                 Kind
	Audiences            []string
	Algorithms           []jwt.Algorithm
	UsernameClaim        string
	RequireVerifiedEmail bool
	GroupsClaim          string
	MaxLifetime          time.Duration
	MaxStale             time.Duration
	RequiredClaims       map[string]string
	Claims               []string
	Dedicated            bool
	// OrkerIDClaim is the path of object keys to the claim holding an
	// Orker's id, from the document's /-separated form; nil when unset.
	OrkerIDClaim []string
	CAFile       string
	JWKSURI      string
	JWKSFile     string
	Login        *Login
}

// Login is an issuer's sign-in settings.
type Login struct {
	ClientID     string
	ClientSecret string // a source, env:NAME or file:/path; never a value
	CLIClientID  string
	Scopes       []string
	DeviceFlow   bool
}

// Policy is the internal/jwt policy this issuer's tokens are held to.
func (i *Issuer) Policy(leeway time.Duration) jwt.Policy {
	return jwt.Policy{
		Issuer:      i.Issuer,
		Audiences:   i.Audiences,
		Algorithms:  i.Algorithms,
		MaxLifetime: i.MaxLifetime,
		Leeway:      leeway,
	}
}

// Issuers is a validated trusted-issuers document. The zero value trusts
// no issuer.
type Issuers struct {
	byName map[string]*Issuer
	byURL  map[string]*Issuer
}

// Names lists every issuer, sorted.
func (s Issuers) Names() []string {
	names := make([]string, 0, len(s.byName))
	for n := range s.byName {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// Len is the number of trusted issuers.
func (s Issuers) Len() int { return len(s.byName) }

// ByName returns the issuer declared under name.
func (s Issuers) ByName(name string) (*Issuer, bool) {
	i, ok := s.byName[name]
	return i, ok
}

// ByIssuer returns the issuer whose iss is exactly iss: the issuer a token
// is judged by.
func (s Issuers) ByIssuer(iss string) (*Issuer, bool) {
	i, ok := s.byURL[iss]
	return i, ok
}

type issuersDocument struct {
	Issuers map[string]issuerDocument `yaml:"issuers"`
}

type issuerDocument struct {
	Issuer               string               `yaml:"issuer"`
	Kind                 string               `yaml:"kind"`
	Audiences            []string             `yaml:"audiences"`
	Algorithms           []string             `yaml:"algorithms"`
	UsernameClaim        string               `yaml:"username_claim"`
	RequireVerifiedEmail bool                 `yaml:"require_verified_email"`
	GroupsClaim          string               `yaml:"groups_claim"`
	MaxLifetime          time.Duration        `yaml:"max_lifetime"`
	MaxStale             *time.Duration       `yaml:"max_stale"`
	RequiredClaims       map[string]yaml.Node `yaml:"required_claims"`
	Claims               []string             `yaml:"claims"`
	Dedicated            bool                 `yaml:"dedicated"`
	OrkerIDClaim         string               `yaml:"orker_id_claim"`
	CAFile               string               `yaml:"ca_file"`
	JWKSURI              string               `yaml:"jwks_uri"`
	JWKSFile             string               `yaml:"jwks_file"`
	Login                *loginDocument       `yaml:"login"`
}

type loginDocument struct {
	ClientID     string        `yaml:"client_id"`
	ClientSecret *secretSource `yaml:"client_secret"`
	CLIClientID  string        `yaml:"cli_client_id"`
	Scopes       []string      `yaml:"scopes"`
	DeviceFlow   bool          `yaml:"device_flow"`
}

type secretSource struct {
	Source string `yaml:"source"`
}

// ParseIssuers decodes and validates one trusted-issuers document. Empty
// input trusts no issuer. Every refusal names the issuer and the key.
func ParseIssuers(raw []byte) (Issuers, error) {
	if strings.TrimSpace(string(raw)) == "" {
		return Issuers{}, nil
	}
	if line, found := dsl.SecondDocumentLine(raw); found {
		return Issuers{}, fmt.Errorf("%s: line %d: a second YAML document begins here; the setting holds one", IssuersSetting, line)
	}
	var doc issuersDocument
	if err := dsl.RefuseMergeTags(raw, doc); err != nil {
		return Issuers{}, fmt.Errorf("%s: %w", IssuersSetting, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return Issuers{}, fmt.Errorf("%s: parse yaml: %w", IssuersSetting, err)
	}
	out := Issuers{byName: map[string]*Issuer{}, byURL: map[string]*Issuer{}}
	names := make([]string, 0, len(doc.Issuers))
	for n := range doc.Issuers {
		names = append(names, n)
	}
	slices.Sort(names)
	var errs []error
	for _, name := range names {
		iss, err := validateIssuer(name, doc.Issuers[name])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if other, dup := out.byURL[iss.Issuer]; dup {
			errs = append(errs, fmt.Errorf("%s: issuers %q and %q both declare issuer %q; a token's iss must choose one", IssuersSetting, other.Name, name, iss.Issuer))
			continue
		}
		out.byName[name] = iss
		out.byURL[iss.Issuer] = iss
	}
	if err := errors.Join(errs...); err != nil {
		return Issuers{}, err
	}
	return out, nil
}

func validateIssuer(name string, d issuerDocument) (*Issuer, error) {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: issuer %q: %s", IssuersSetting, name, fmt.Sprintf(format, args...)))
	}
	if !issuerNamePattern.MatchString(name) {
		fail("the name is not a slug of at most 32 bytes (%s)", issuerNamePattern)
	}
	i := &Issuer{
		Name:                 name,
		Issuer:               d.Issuer,
		Kind:                 Kind(d.Kind),
		Audiences:            d.Audiences,
		UsernameClaim:        d.UsernameClaim,
		RequireVerifiedEmail: d.RequireVerifiedEmail,
		GroupsClaim:          d.GroupsClaim,
		MaxLifetime:          d.MaxLifetime,
		MaxStale:             DefaultMaxStale,
		Claims:               d.Claims,
		Dedicated:            d.Dedicated,
		CAFile:               d.CAFile,
		JWKSURI:              d.JWKSURI,
		JWKSFile:             d.JWKSFile,
	}
	if i.UsernameClaim == "" {
		i.UsernameClaim = "sub"
	}
	if d.MaxStale != nil {
		i.MaxStale = *d.MaxStale
	}

	issURL, urlErr := url.Parse(d.Issuer)
	switch {
	case d.Issuer == "":
		fail("issuer is required")
	case strings.HasPrefix(d.Issuer, "serviceaccount:"):
		fail("issuer %q is the form of a service account's assertion", d.Issuer)
	case d.JWKSFile == "" && (urlErr != nil || issURL.Scheme != "https" || issURL.Host == "" || issURL.User != nil || issURL.RawQuery != "" || issURL.Fragment != ""):
		fail("issuer must be an https URL with no query or fragment, unless jwks_file supplies its keys")
	}

	switch i.Kind {
	case KindUser, KindService:
	case "":
		fail("kind is required (user or service)")
	default:
		fail("kind %q is not user or service", d.Kind)
	}
	if len(d.Audiences) == 0 || slices.Contains(d.Audiences, "") {
		fail("at least one non-empty audience is required")
	}
	if len(d.Algorithms) == 0 {
		fail("algorithms is required (RS256, ES256)")
	}
	for _, a := range d.Algorithms {
		alg, err := jwt.ParseAlgorithm(a)
		if err != nil {
			fail("%v", err)
			continue
		}
		if slices.Contains(i.Algorithms, alg) {
			fail("algorithm %s is listed twice", alg)
			continue
		}
		i.Algorithms = append(i.Algorithms, alg)
	}
	if i.UsernameClaim == "email" && !i.RequireVerifiedEmail {
		fail("username_claim email needs require_verified_email: true (a mailbox renamed or handed on is that person's access)")
	}
	if d.MaxLifetime <= 0 {
		fail("max_lifetime is required and must be positive")
	}
	if i.MaxStale < 0 {
		fail("max_stale must not be negative")
	}

	if len(d.RequiredClaims) > 0 {
		i.RequiredClaims = make(map[string]string, len(d.RequiredClaims))
		for claim, node := range d.RequiredClaims {
			value, ok := requiredClaimValue(node)
			if claim == "" || !ok {
				fail("required_claims.%s must be a string or an integer of at most 15 digits", claim)
				continue
			}
			i.RequiredClaims[claim] = value
		}
	}
	if i.Kind == KindService && len(d.RequiredClaims) == 0 && !d.Dedicated {
		fail("a service issuer needs required_claims (an owner id), or dedicated: true when it issues only to this organisation")
	}
	if d.Dedicated && urlErr == nil {
		host := strings.TrimSuffix(strings.ToLower(issURL.Hostname()), ".")
		cleaned := path.Clean("/" + issURL.Path)
		if paths, listed := publicMultiTenantHosts[host]; listed {
			if paths == nil || slices.ContainsFunc(paths, func(p string) bool { return cleaned == p || strings.HasPrefix(cleaned, p+"/") }) {
				fail("dedicated: true is refused for %s, which issues tokens to every tenant it serves; use required_claims", issURL.Hostname())
			}
		}
	}
	for _, c := range d.Claims {
		if c == "" {
			fail("claims lists an empty name")
		}
	}
	if d.OrkerIDClaim != "" {
		path := strings.Split(d.OrkerIDClaim, "/")
		if slices.Contains(path, "") {
			fail("orker_id_claim %q is a /-separated path of non-empty object keys", d.OrkerIDClaim)
		}
		i.OrkerIDClaim = path
	}

	if d.JWKSURI != "" && d.JWKSFile != "" {
		fail("give jwks_uri or jwks_file, not both")
	}
	if d.JWKSURI != "" {
		u, err := url.Parse(d.JWKSURI)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			fail("jwks_uri must be an https URL")
		}
	}

	if d.Login != nil {
		login, err := validateLogin(d.Login)
		switch {
		case i.Kind != KindUser:
			fail("login is for a user issuer")
		case err != nil:
			fail("login: %v", err)
		default:
			i.Login = login
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return i, nil
}

func validateLogin(d *loginDocument) (*Login, error) {
	var errs []error
	if d.ClientID == "" {
		errs = append(errs, errors.New("client_id is required"))
	}
	l := &Login{ClientID: d.ClientID, CLIClientID: d.CLIClientID, Scopes: d.Scopes, DeviceFlow: d.DeviceFlow}
	if d.ClientSecret != nil {
		if !validSecretSource(d.ClientSecret.Source) {
			errs = append(errs, fmt.Errorf("client_secret.source %q must be env:NAME or file:/path; a secret is never written in the document", d.ClientSecret.Source))
		}
		l.ClientSecret = d.ClientSecret.Source
	}
	if !slices.Contains(d.Scopes, "openid") {
		errs = append(errs, errors.New("scopes must include openid"))
	}
	return l, errors.Join(errs...)
}

// envNamePattern is the name an env: source may give.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validSecretSource admits env:NAME, NAME a variable's name, and
// file:/path, a plain absolute path of at most 4096 bytes (AGENTS.md §6).
func validSecretSource(source string) bool {
	scheme, locator, _ := strings.Cut(source, ":")
	switch scheme {
	case "env":
		return envNamePattern.MatchString(locator)
	case "file":
		return len(locator) <= 4096 && filepath.IsAbs(locator) && filepath.Clean(locator) == locator
	}
	return false
}

// requiredClaimValue reads one required_claims value: a string as written,
// or an integer of at most 15 digits as its decimal digits, so 1234567 and
// "1234567" are one requirement. Anything else is refused.
func requiredClaimValue(node yaml.Node) (string, bool) {
	if node.Kind != yaml.ScalarNode {
		return "", false
	}
	switch node.ShortTag() {
	case "!!str":
		return node.Value, node.Value != ""
	case "!!int":
		v := node.Value
		if v == "" || len(v) > 15 || strings.Trim(v, "0123456789") != "" || (len(v) > 1 && v[0] == '0') {
			return "", false
		}
		return v, true
	}
	return "", false
}

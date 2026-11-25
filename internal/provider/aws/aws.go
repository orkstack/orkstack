// Package aws is the "aws" provider type (E81, E83): it exchanges the
// attempt's identity token for a role's session at STS, hands the session out
// as files, and reads Parameter Store, Secrets Manager and KMS with it.
//
// It never consults a credential chain. Its clients are built from their own
// Options, never from config.LoadDefaultConfig: the exchange is anonymous,
// and every other call is signed with the session the exchange returned. Of
// the Orker's environment it follows the proxy settings (HTTPS_PROXY,
// HTTP_PROXY, NO_PROXY) and nothing else: no AWS_* variable, no profile or
// shared file, no instance metadata, no container endpoint (providers.md §7;
// TestEnvironment_ProxyYesEveryAWSSettingNo).
//
// A session keeps its attempt's token source for as long as it is open. AWS
// extends no session, so a refreshed credential is a new exchange with a
// fresh token, which Session.Credential makes itself: the credential is the
// login.
package aws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TypeName is the value of a declaration's `type:`.
const TypeName = "aws"

// LoginScheme is the word of a login in a policy string,
// "<name>:sts:<role ARN>".
const LoginScheme = "sts"

// DefaultAudience is the audience AWS's own SDKs use for a web identity.
const DefaultAudience = "sts.amazonaws.com"

// The bounds STS puts on a role's session; the role's own maximum may be
// lower, and STS says so.
const (
	minSessionSeconds     = 900
	defaultSessionSeconds = 3600
	maxSessionSeconds     = 43200
)

var (
	regionShape   = regexp.MustCompile(`^[a-z]{2,}(-[a-z0-9]+)+$`)
	audienceShape = regexp.MustCompile(`^[A-Za-z0-9._:/-]{1,255}$`)
)

// Type is the "aws" provider type.
type Type struct{}

// Name is TypeName.
func (Type) Name() string { return TypeName }

// New reads one declaration. It does no I/O.
func (Type) New(name string, c *provider.Config) (provider.Provider, error) {
	p := &Provider{name: name, audience: DefaultAudience, session: defaultSessionSeconds * time.Second, tags: true}
	if region, ok := c.String("region"); !ok {
		if !c.Has("region") {
			c.Errorf("region", "is required")
		}
	} else if len(region) > 32 || !regionShape.MatchString(region) {
		c.Errorf("region", "%q is not an AWS region's name", region)
	} else {
		p.region = region
	}
	if audience, ok := c.String("audience"); ok {
		if !audienceShape.MatchString(audience) {
			c.Errorf("audience", "%q must match %s", audience, audienceShape)
		}
		p.audience = audience
	}
	if seconds, ok := c.Int("session_seconds", minSessionSeconds, maxSessionSeconds); ok {
		p.session = time.Duration(seconds) * time.Second
	}
	p.stsEndpoint = endpoint(c, "sts_endpoint")
	p.endpoint = endpoint(c, "endpoint")
	if tags, ok := c.Bool("session_tags"); ok {
		p.tags = tags
	}
	if keys, ok := c.Strings("transitive_tags"); ok {
		for i, key := range keys {
			switch {
			case !slices.Contains(artifacts.SessionTagKeys, key):
				c.Errorf("transitive_tags", "%q is not a session tag OrkStack sends (%s)", key, strings.Join(artifacts.SessionTagKeys, ", "))
			case slices.Contains(keys[:i], key):
				c.Errorf("transitive_tags", "%q is listed twice", key)
			}
		}
		if !p.tags && len(keys) > 0 {
			c.Errorf("transitive_tags", "needs session_tags: true")
		}
		p.transitive = slices.Sorted(slices.Values(keys))
	}
	// One transport for every attempt's clients, so connections are reused. A
	// redirect is an answer, never followed: it would carry the identity
	// token, or a signed request, to a host nobody declared. The proxy
	// settings are followed, as every outbound call of an Orker follows them:
	// a proxy carries an https connection whose ends are still these two.
	p.http = &http.Client{
		Transport:     bounded{awshttp.NewBuildableClient().GetTransport()},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return p, nil
}

// maxAnswerBytes bounds one answer: a value is at most
// provider.MaxValueBytes, and base64 in a JSON document makes it a third
// longer.
const maxAnswerBytes = 4 << 20

// bounded refuses to read an answer past maxAnswerBytes.
type bounded struct{ next http.RoundTripper }

func (b bounded) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := b.next.RoundTrip(r)
	if err == nil {
		resp.Body = http.MaxBytesReader(nil, resp.Body, maxAnswerBytes)
	}
	return resp, err
}

// endpoint reads an override of an endpoint: an https URL, or an http one of
// a loopback address, for a stand-in. It carries the identity token or the
// values read with it, so it is never sent in the clear to another host.
func endpoint(c *provider.Config, key string) string {
	raw, ok := c.String(key)
	if !ok {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		c.Errorf(key, "must be a URL with a host and no user, query or fragment")
		return ""
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !loopback(u.Hostname())) {
		c.Errorf(key, "must be https (http is taken for a loopback address alone)")
		return ""
	}
	return raw
}

func loopback(host string) bool {
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}

// Provider is one declared aws provider.
type Provider struct {
	name, region, audience string
	session                time.Duration
	stsEndpoint, endpoint  string
	tags                   bool
	transitive             []string
	http                   *http.Client
}

var (
	_ provider.Provider      = (*Provider)(nil)
	_ provider.SessionTagger = (*Provider)(nil)
	_ provider.Session       = (*session)(nil)
)

func (p *Provider) Name() string        { return p.name }
func (p *Provider) Type() string        { return TypeName }
func (p *Provider) Audience() string    { return p.audience }
func (p *Provider) LoginScheme() string { return LoginScheme }

// SessionTags are the declaration's own: every token for this provider's
// audience carries the tags, or none does.
func (p *Provider) SessionTags() (on bool, transitive []string) {
	return p.tags, slices.Clone(p.transitive)
}

// Describe names the region, the session's length and the tag setting; an
// endpoint override is an operator's own address.
func (p *Provider) Describe() provider.Description {
	fields := map[string]string{
		"region":          p.region,
		"session_seconds": strconv.Itoa(int(p.session / time.Second)),
		"session_tags":    strconv.FormatBool(p.tags),
	}
	for key, v := range map[string]string{"sts_endpoint": p.stsEndpoint, "endpoint": p.endpoint, "transitive_tags": strings.Join(p.transitive, ",")} {
		if v != "" {
			fields[key] = v
		}
	}
	return provider.Description{Type: TypeName, Audience: p.audience, Fields: fields}
}

// Parse checks one reference.
func (p *Provider) Parse(ref provider.Ref) (provider.Parsed, error) {
	detail, err := parse(ref, p.region)
	if err != nil {
		return provider.Parsed{}, err
	}
	return provider.Parsed{Ref: ref, Detail: detail}, nil
}

// Login exchanges the attempt's identity token for a session of the role
// principal names.
func (p *Provider) Login(ctx context.Context, principal string, who provider.Attempt, token provider.TokenSource) (provider.Session, error) {
	s := &session{p: p, principal: principal, who: who, token: token, now: time.Now}
	if err := s.login(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// duration is the session length STS is asked for: session_seconds, and for
// an event source's instance no longer than the attempt's MaxLogin
// (identity.token.instance_max_ttl), but never under STS's own least.
func (p *Provider) duration(who provider.Attempt) time.Duration {
	if who.Instance && who.MaxLogin > 0 {
		return max(min(p.session, who.MaxLogin.Truncate(time.Second)), minSessionSeconds*time.Second)
	}
	return p.session
}

// sessionNameShape is STS's grammar for a session's name.
var sessionNameShape = regexp.MustCompile(`^[A-Za-z0-9_+=,.@-]{2,64}$`)

// sessionName is the session's name, which CloudTrail shows:
// "<run id>-<attempt>" where STS takes it, else a digest of it.
func sessionName(who provider.Attempt) string {
	name := fmt.Sprintf("%s-%d", who.RunID, who.Attempt)
	if who.RunID != "" && sessionNameShape.MatchString(name) {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return "ork-" + hex.EncodeToString(sum[:20])
}

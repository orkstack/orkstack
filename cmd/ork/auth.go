package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// authConfig is the auth.* settings (docs/AUTHENTICATION.md).
type authConfig struct {
	mode              string
	publicURL         string
	issuers           authn.Issuers
	admins            []string
	orkers            []authz.OrkerEntry
	breakGlassKeyFile string
	clockSkew         time.Duration
	policyRefresh     time.Duration
	policyStaleLimit  time.Duration
	// signinKeyFiles and signin are auth.signin.*: the key set that seals
	// the login cookie and the sign-in records, and a sign-in's limits.
	signinKeyFiles []string
	signin         authn.SigninSettings
}

// signinOffered reports an issuer with a login: the web UI's sign-in is on.
func (c authConfig) signinOffered() bool {
	for _, name := range c.issuers.Names() {
		if i, _ := c.issuers.ByName(name); i.Login != nil {
			return true
		}
	}
	return false
}

// The auth.* defaults.
const (
	// maxBreakGlassKeyBytes bounds the break-glass key file: one PEM key.
	maxBreakGlassKeyBytes = 64 << 10
)

// orkersDocument is ORK_AUTH_ORKERS: auth.orkers, as its own document.
type orkersDocument struct {
	Orkers []struct {
		Subject  string `yaml:"subject"`
		Pool     string `yaml:"pool"`
		OrkerIDs string `yaml:"orker_ids"`
	} `yaml:"orkers"`
}

// parseOrkers decodes auth.orkers strictly: one document, no unknown key,
// no merge key, and orker_ids either absent or `any`. Whether each subject
// and pool is one is CompileConfiguration's to say.
func parseOrkers(raw []byte) ([]authz.OrkerEntry, error) {
	if line, found := dsl.SecondDocumentLine(raw); found {
		return nil, fmt.Errorf("line %d: a second YAML document begins here; the setting holds one", line)
	}
	var doc orkersDocument
	if err := dsl.RefuseMergeTags(raw, doc); err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	out := make([]authz.OrkerEntry, 0, len(doc.Orkers))
	for i, o := range doc.Orkers {
		if o.Subject == "" || o.Pool == "" {
			return nil, fmt.Errorf("orkers[%d]: subject and pool are required", i)
		}
		if o.OrkerIDs != "" && o.OrkerIDs != "any" {
			return nil, fmt.Errorf("orkers[%d]: orker_ids is `any` or absent, not %q", i, o.OrkerIDs)
		}
		out = append(out, authz.OrkerEntry{Subject: o.Subject, Pool: o.Pool, OrkerIDsAny: o.OrkerIDs == "any"})
	}
	return out, nil
}

// authFromEnv reads the auth.* settings. Every error refuses the boot,
// naming its setting.
func authFromEnv(lookup func(string) (string, bool)) (authConfig, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}
	c := authConfig{
		mode:              get("ORK_AUTH_MODE"),
		publicURL:         get("ORK_AUTH_PUBLIC_URL"),
		breakGlassKeyFile: get("ORK_AUTH_BREAK_GLASS_KEY_FILE"),
	}
	if c.mode == "" {
		c.mode = authn.ModeAsserted
	}
	if artifacts.AuthModeRank(c.mode) == 0 {
		return c, fmt.Errorf("auth.mode (ORK_AUTH_MODE) %q is not asserted, observe or enforce", c.mode)
	}
	for _, s := range strings.Split(get("ORK_AUTH_ADMINS"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.admins = append(c.admins, s)
		}
	}
	var err error
	if c.clockSkew, err = authDuration(get, "ORK_AUTH_CLOCK_SKEW", "auth.clock_skew", authn.DefaultClockSkew, false); err != nil {
		return c, err
	}
	if c.policyRefresh, err = authDuration(get, "ORK_AUTH_POLICY_REFRESH", "auth.policy_refresh", api.DefaultPolicyRefresh, false); err != nil {
		return c, err
	}
	if c.policyStaleLimit, err = authDuration(get, "ORK_AUTH_POLICY_STALE_LIMIT", "auth.policy_stale_limit", api.DefaultPolicyStaleLimit, true); err != nil {
		return c, err
	}
	for _, s := range strings.Split(get("ORK_AUTH_SIGNIN_KEY_FILES"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.signinKeyFiles = append(c.signinKeyFiles, s)
		}
	}
	if c.signin.MaxAge, err = authDuration(get, "ORK_AUTH_SIGNIN_MAX_AGE", "auth.signin.max_age", authn.DefaultSigninMaxAge, false); err != nil {
		return c, err
	}
	if c.signin.IdleTimeout, err = authDuration(get, "ORK_AUTH_SIGNIN_IDLE_TIMEOUT", "auth.signin.idle_timeout", authn.DefaultSigninIdleTimeout, false); err != nil {
		return c, err
	}
	if c.signin.RevalidateInterval, err = authDuration(get, "ORK_AUTH_SIGNIN_REVALIDATE_INTERVAL", "auth.signin.revalidate_interval", authn.DefaultSigninRevalidateInterval, false); err != nil {
		return c, err
	}
	if doc := get("ORK_AUTH_ISSUERS"); doc != "" {
		if c.issuers, err = authn.ParseIssuers([]byte(doc)); err != nil {
			return c, fmt.Errorf("auth.issuers (ORK_AUTH_ISSUERS): %w", err)
		}
	}
	if _, err := authz.CompileConfiguration(authz.Configuration{Admins: c.admins}); err != nil {
		return c, fmt.Errorf("auth.admins (ORK_AUTH_ADMINS): %w", err)
	}
	if doc := get("ORK_AUTH_ORKERS"); doc != "" {
		if c.orkers, err = parseOrkers([]byte(doc)); err != nil {
			return c, fmt.Errorf("auth.orkers (ORK_AUTH_ORKERS): %w", err)
		}
		if _, err := authz.CompileConfiguration(authz.Configuration{Orkers: c.orkers}); err != nil {
			return c, fmt.Errorf("auth.orkers (ORK_AUTH_ORKERS): %w", err)
		}
	}
	if c.publicURL != "" {
		u, err := url.Parse(c.publicURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil ||
			(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("auth.public_url (ORK_AUTH_PUBLIC_URL) %q is not an http(s) URL of a host alone", c.publicURL)
		}
	}
	return c, nil
}

func authDuration(get func(string) string, env, setting string, fallback time.Duration, zeroOK bool) (time.Duration, error) {
	raw := get(env)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 || (d == 0 && !zeroOK) {
		return 0, fmt.Errorf("%s (%s) %q is not a positive duration", setting, env, raw)
	}
	return d, nil
}

// enabled reports a mode that authenticates.
func (c authConfig) enabled() bool { return c.mode != authn.ModeAsserted }

// capabilities are what this engine's poller identity advertises of its
// mode: an enforcing engine says so, for the fleet check.
func (c authConfig) capabilities() []string {
	if c.mode == authn.ModeEnforce {
		return []string{engine.CapabilityAuthnEnforce}
	}
	return nil
}

// engineDescription is what this engine says of itself on the engines'
// presence channel (engine.EngineDescription): its mode, its idempotency key's
// life, its deployment and build, and, when it authenticates, the digests of
// auth.admins and auth.orkers, which every replica must hold alike.
func engineDescription(cfg config) engine.EngineDescription {
	ttl := cfg.idempotencyTTL
	if ttl <= 0 {
		ttl = engine.DefaultClaimTTL // what engine.ClaimLife holds a claim for
	}
	d := engine.EngineDescription{Mode: cfg.auth.mode, ClaimTTL: ttl,
		Deployment: cfg.deployment.Name, BuildID: cfg.deployment.BuildID}
	if cfg.auth.enabled() {
		d.AdminsDigest = engine.ConfigDigest(cfg.auth.admins)
		orkers := make([]string, 0, len(cfg.auth.orkers))
		for _, o := range cfg.auth.orkers {
			orkers = append(orkers, fmt.Sprintf("%s\x00%s\x00%t", o.Subject, o.Pool, o.OrkerIDsAny))
		}
		d.OrkersDigest = engine.ConfigDigest(orkers)
	}
	return d
}

// checkBoot refuses what could never work: enforce with no way to
// authenticate, break-glass with no public URL to bind its assertions to,
// and a heap profile served off loopback by a process that may hold keys.
func (c authConfig) checkBoot(adminAddr string, pprofEnabled bool) error {
	if !c.enabled() {
		return nil
	}
	if c.mode == authn.ModeEnforce && c.issuers.Len() == 0 && c.breakGlassKeyFile == "" {
		return errors.New("auth.mode enforce with no auth.issuers and no auth.break_glass_key_file: nobody could ever authenticate")
	}
	if c.breakGlassKeyFile != "" && c.publicURL == "" {
		return errors.New("auth.break_glass_key_file needs auth.public_url: a break-glass assertion is bound to the deployment's public URL")
	}
	if c.signinOffered() {
		if err := c.checkSignin(); err != nil {
			return err
		}
	}
	if pprofEnabled && adminAddr != "" && !loopbackAddr(adminAddr) {
		return fmt.Errorf("telemetry.pprof_enabled with auth.mode %s needs a loopback telemetry.admin_addr (%q is not): a heap profile of a process that holds keys is those keys", c.mode, adminAddr)
	}
	return nil
}

// checkSignin refuses a sign-in that could not be safe: no public URL to
// redirect to, one that is not https off loopback (the cookies are Secure),
// no key to seal with, or limits that contradict each other.
func (c authConfig) checkSignin() error {
	if c.publicURL == "" {
		return errors.New("an issuer offers sign-in (login) and auth.public_url is not set: it is the redirect URI's origin")
	}
	u, err := url.Parse(c.publicURL)
	if err != nil {
		return fmt.Errorf("auth.public_url %q: %w", c.publicURL, err)
	}
	if u.Scheme != "https" && !authn.LoopbackHost(u.Hostname()) {
		return fmt.Errorf("an issuer offers sign-in and auth.public_url %q is not https: the sign-in cookie is Secure", c.publicURL)
	}
	if len(c.signinKeyFiles) == 0 {
		return errors.New("an issuer offers sign-in (login) and auth.signin.key_files (ORK_AUTH_SIGNIN_KEY_FILES) names no key")
	}
	if c.signin.IdleTimeout > c.signin.MaxAge || c.signin.RevalidateInterval > c.signin.MaxAge {
		return errors.New("auth.signin.idle_timeout and revalidate_interval are at most auth.signin.max_age")
	}
	return nil
}

// signinSecret reads a login's client secret from its source: env:NAME, or
// file:/path read at each use, at most 64 KiB, trimmed.
func signinSecret(source string) (string, error) {
	scheme, locator, _ := strings.Cut(source, ":")
	var v string
	switch scheme {
	case "env":
		v = os.Getenv(locator)
	case "file":
		f, err := os.Open(locator)
		if err != nil {
			return "", err
		}
		defer f.Close()
		raw, err := io.ReadAll(io.LimitReader(f, 64<<10+1))
		if err != nil {
			return "", err
		}
		if len(raw) > 64<<10 {
			return "", fmt.Errorf("%s is larger than a secret", locator)
		}
		v = string(raw)
	default:
		return "", fmt.Errorf("%q is not env:NAME or file:/path", source)
	}
	if v = strings.TrimSpace(v); v == "" {
		return "", fmt.Errorf("%s is empty", source)
	}
	return v, nil
}

func loopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// breakGlassFrom reads the one public key of auth.break_glass_key_file.
func breakGlassFrom(c authConfig) (*authn.BreakGlass, error) {
	if c.breakGlassKeyFile == "" {
		return nil, nil
	}
	f, err := os.Open(c.breakGlassKeyFile)
	if err != nil {
		return nil, fmt.Errorf("auth.break_glass_key_file: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxBreakGlassKeyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("auth.break_glass_key_file: %w", err)
	}
	if len(raw) > maxBreakGlassKeyBytes {
		return nil, fmt.Errorf("auth.break_glass_key_file: more than %d bytes", maxBreakGlassKeyBytes)
	}
	key, err := jwt.ParsePublicKeyPEM(raw)
	if err != nil {
		return nil, fmt.Errorf("auth.break_glass_key_file: %w", err)
	}
	keys, err := jwt.StaticByThumbprint(key)
	if err != nil {
		return nil, fmt.Errorf("auth.break_glass_key_file: %w", err)
	}
	public, _ := url.Parse(c.publicURL)
	return &authn.BreakGlass{Keys: keys, Audience: strings.TrimSuffix(c.publicURL, "/"), PublicURL: public, Replay: jwt.NewReplayCache()}, nil
}

// accessSetup is what run needs of authentication.
type accessSetup struct {
	access     *api.AccessControl
	breakGlass func(r *http.Request, p *authn.Principal, reason string)
}

// setupAccess reads the mode record, refusing to start beneath it, and, for
// an engine whose mode is observe or enforce, builds authentication and
// authorization: the trusted issuers' key caches, the access document's
// cache and, in enforce mode, the fleet check. In asserted mode it builds
// nothing else.
func setupAccess(ctx context.Context, c authConfig, logger *slog.Logger, storeCfg artifacts.Config,
	hostAllowed func(string) bool, claimTTL time.Duration, checker engine.CapabilityChecker,
	presence engine.EnginePresenceReader, self engine.EngineDescription,
	audit *api.AuditRecorder, metrics *telemetry.AuthInstruments,
) (accessSetup, error) {
	modes, err := artifacts.NewS3ModeStore(ctx, storeCfg)
	if err != nil {
		return accessSetup{}, fmt.Errorf("open the mode record: %w", err)
	}
	host, _ := os.Hostname()
	guard := &api.ModeGuard{Store: modes, Mode: c.mode, ClaimTTL: claimTTL, Logger: logger, Host: host,
		Missing: metrics.CountModeRecordMissing}
	if presence != nil {
		// Lowering to asserted waits the longest claim life any engine
		// advertises on the presence channel, not this engine's alone.
		guard.PeerClaimTTL = func(ctx context.Context) (time.Duration, error) {
			engines, err := presence.ObserveEngines(ctx)
			if err != nil {
				return 0, err
			}
			ttl, by := engine.LargestClaimTTL(engines)
			if ttl > claimTTL {
				// The engine a lowering waits for is named here, on the
				// operator's surface, never in the API's refusal: an identity
				// carries a host name. A poller of the namespace may say
				// anything, so an unexpected one here is worth a look.
				logger.Warn("mode record: an engine advertises a longer idempotency key life than this one; lowering to asserted waits for it",
					"engine", by, "claim_ttl", ttl, "own_claim_ttl", claimTTL)
			}
			return ttl, nil
		}
	}
	if err := bootModeRecord(ctx, guard, logger); err != nil {
		return accessSetup{}, err
	}
	go guard.Watch(ctx)
	if !c.enabled() {
		return accessSetup{}, nil
	}
	glass, err := breakGlassFrom(c)
	if err != nil {
		return accessSetup{}, err
	}
	configured, err := authz.CompileConfiguration(authz.Configuration{Admins: c.admins, BreakGlass: glass != nil, Orkers: c.orkers})
	if err != nil {
		return accessSetup{}, err
	}
	keys := map[string]jwt.KeySource{}
	for _, name := range c.issuers.Names() {
		issuer, _ := c.issuers.ByName(name)
		cache, err := authn.NewKeyCache(issuer, nil, nil)
		if err != nil {
			return accessSetup{}, fmt.Errorf("auth.issuers: %w", err)
		}
		keys[name] = cache
	}
	documents, err := artifacts.NewS3AccessStore(ctx, storeCfg)
	if err != nil {
		return accessSetup{}, fmt.Errorf("open the access document: %w", err)
	}
	policies := api.NewPolicyCache(documents, api.PolicyCacheOptions{Configured: configured, Refresh: c.policyRefresh,
		StaleLimit: staleLimitOption(c.policyStaleLimit), Logger: logger,
		OnRead:       func(outcome string) { metrics.CountPolicyRefresh(context.Background(), outcome) },
		OnUnexpected: func() { metrics.CountDocumentUnexpected(context.Background()) }})
	if err := metrics.WatchPolicy(func() (int64, float64) {
		st := policies.State()
		return st.Revision, st.Age.Seconds()
	}); err != nil {
		return accessSetup{}, fmt.Errorf("auth metrics: %w", err)
	}
	go policies.Run(ctx)
	access := &api.AccessControl{
		Mode: c.mode, Policies: policies, Configured: configured, ModeRecord: guard, Logger: logger, Counter: metrics,
		Authenticator: &authn.Authenticator{Issuers: c.issuers, Keys: keys, Leeway: c.clockSkew, BreakGlass: glass,
			HostAllowed: hostAllowed, ServiceAccounts: serviceAccountsFrom(c, policies)},
	}
	if c.mode == authn.ModeEnforce {
		access.Fleet = &api.FleetReport{Presence: presence, Checker: checker, Self: self, Logger: logger,
			Unenforced: metrics.SetFleetUnenforced}
		go access.Fleet.Run(ctx)
	}
	if c.signinOffered() {
		signins, err := signinsFrom(ctx, c, keys, policies, storeCfg, audit, logger)
		if err != nil {
			return accessSetup{}, err
		}
		access.Signins = signins
	}
	logger.Info("authentication",
		"mode", c.mode, "issuers", c.issuers.Names(), "admins", len(c.admins), "break_glass", glass != nil, "service_accounts", c.publicURL != "",
		"policy_store", documents.Location(), "policy_refresh", c.policyRefresh, "policy_stale_limit", c.policyStaleLimit)
	return accessSetup{access: access, breakGlass: breakGlassRecorder(audit, logger, metrics)}, nil
}

// serviceAccountsFrom accepts the access document's service accounts'
// assertions, whose audience is the deployment's public URL: an engine with
// no auth.public_url accepts none (authn-authz.md §3.2).
func serviceAccountsFrom(c authConfig, policies *api.PolicyCache) *authn.ServiceAccounts {
	if c.publicURL == "" {
		return nil
	}
	public, _ := url.Parse(c.publicURL)
	return &authn.ServiceAccounts{
		Lookup: func(ctx context.Context, name string) (authz.ServiceAccount, bool, error) {
			p, err := policies.Policy(ctx)
			if err != nil {
				return authz.ServiceAccount{}, false, err
			}
			sa, ok := p.ServiceAccount(name)
			return sa, ok, nil
		},
		Audience: strings.TrimSuffix(c.publicURL, "/"), PublicURL: public, Replay: jwt.NewReplayCache(),
	}
}

// signinsFrom builds the web UI's sign-in: the key set, the record store,
// and its audit records (auth.signin.*).
func signinsFrom(ctx context.Context, c authConfig, keys map[string]jwt.KeySource, policies *api.PolicyCache,
	storeCfg artifacts.Config, audit *api.AuditRecorder, logger *slog.Logger,
) (*authn.Signins, error) {
	sealer, err := authn.LoadSigninKeys(c.signinKeyFiles)
	if err != nil {
		return nil, err
	}
	for _, f := range c.signinKeyFiles {
		if info, err := os.Stat(f); err == nil && info.Mode().Perm()&0o077 != 0 {
			logger.Warn("auth.signin.key_files: a sign-in key is readable by other users; chmod 600 it",
				"file", f, "mode", info.Mode().Perm())
		}
	}
	records, err := artifacts.NewS3SigninStore(ctx, storeCfg)
	if err != nil {
		return nil, fmt.Errorf("open the sign-in records: %w", err)
	}
	public, _ := url.Parse(strings.TrimSuffix(c.publicURL, "/"))
	s := &authn.Signins{Issuers: c.issuers, Keys: keys, Sealer: sealer, Records: records, PublicURL: public,
		Settings: c.signin, Leeway: c.clockSkew, Secret: signinSecret, Logger: logger,
		Disabled: func(ctx context.Context, id string) (bool, error) {
			p, err := policies.Policy(ctx)
			if err != nil {
				return false, err // a sign-in is then refused for now, and nothing ends
			}
			return p.Disabled(id), nil
		},
		OnEvent: func(ctx context.Context, e authn.SigninEvent) {
			at := time.Now().UTC()
			rec := &orkstackv1.AuditRecord{SchemaVersion: artifacts.AuditSchemaVersionVerified, Id: artifacts.NewAuditID(at),
				At: timestamppb.New(at), Action: e.Action, Target: e.PublicID, Detail: e.Reason, PrincipalIssuer: e.Issuer,
				Succeeded: e.Action != "auth.signin.refused", Signin: e.PublicID, AuthMethod: authn.MethodOIDC,
				Procedure: "sign-in"}
			if e.Principal != nil {
				rec.Principal, rec.PrincipalVerified = e.Principal.ID, true
			}
			if rec.Target == "" {
				rec.Target = "-"
			}
			if err := audit.RecordErr(ctx, rec); err != nil {
				logger.Error("a sign-in's audit record could not be written; this log line is its trail", "error", err,
					"action", e.Action, "principal", rec.GetPrincipal(), "signin", e.PublicID, "reason", e.Reason)
			}
		}}
	logger.Info("sign-in", "issuers", loginIssuers(c.issuers), "keys", sealer.KeyIDs(), "records", records.Location(),
		"max_age", c.signin.MaxAge, "idle_timeout", c.signin.IdleTimeout, "revalidate_interval", c.signin.RevalidateInterval)
	return s, nil
}

func loginIssuers(issuers authn.Issuers) []string {
	var out []string
	for _, name := range issuers.Names() {
		if i, _ := issuers.ByName(name); i.Login != nil {
			out = append(out, name)
		}
	}
	return out
}

// staleLimitOption turns auth.policy_stale_limit into the cache's option:
// 0 closes at once, which the cache spells as negative.
func staleLimitOption(d time.Duration) time.Duration {
	if d == 0 {
		return -1
	}
	return d
}

// bootModeRecord applies the record's boot rules. A store that cannot be
// read is waited for: the engine already cannot serve without the store. A
// record that refuses (higher, or unreadable) stops the boot.
func bootModeRecord(ctx context.Context, guard *api.ModeGuard, logger *slog.Logger) error {
	backoff := time.Second
	for {
		err := guard.Boot(ctx)
		if err == nil || errors.Is(err, api.ErrModeRecordHigher) || errors.Is(err, artifacts.ErrModeRecordUnreadable) {
			return err
		}
		logger.Warn("mode record: the store did not answer; this engine does not serve until it does", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

// breakGlassRecorder writes auth.break-glass.used before the request is
// served. If the record cannot be written, the whole record goes to the
// engine's log at error level and the request is served: break-glass
// exists for the day things are broken.
func breakGlassRecorder(audit *api.AuditRecorder, logger *slog.Logger, metrics *telemetry.AuthInstruments) func(r *http.Request, p *authn.Principal, reason string) {
	return func(r *http.Request, p *authn.Principal, reason string) {
		metrics.CountBreakGlass(r.Context())
		at := time.Now().UTC()
		rec := &orkstackv1.AuditRecord{
			SchemaVersion: artifacts.AuditSchemaVersionVerified, Id: artifacts.NewAuditID(at), At: timestamppb.New(at),
			Principal: p.ID, PrincipalVerified: true, AuthMethod: p.Method, PrincipalIssuer: p.Issuer,
			SourceAddress: r.RemoteAddr, Action: "auth.break-glass.used", Procedure: r.Method + " " + r.URL.Path,
			Target: "-", Detail: "reason=" + fmt.Sprintf("%q", reason), Succeeded: true, Rule: authz.BreakGlassRule,
		}
		logger.Error("break-glass used", "path", r.URL.Path, "method", r.Method, "reason", reason, "source", r.RemoteAddr)
		if err := audit.RecordErr(r.Context(), rec); err != nil {
			logger.Error("break-glass audit record could not be written; this log line is its trail",
				"error", err, "id", rec.GetId(), "at", at, "principal", rec.GetPrincipal(), "procedure", rec.GetProcedure(),
				"source", rec.GetSourceAddress(), "detail", rec.GetDetail())
		}
	}
}

// setModeRecordArgs is `ork --set-mode-record MODE --reason TEXT [--force]`,
// taken out of the arguments before the configuration flags are parsed.
type setModeRecordArgs struct {
	requested bool
	mode      string
	reason    string
	force     bool
}

// splitSetModeRecord separates the --set-mode-record command's own
// arguments from the configuration's.
func splitSetModeRecord(args []string) (setModeRecordArgs, []string, error) {
	var out setModeRecordArgs
	var rest []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		value := func(name string) (string, error) {
			if v, ok := strings.CutPrefix(arg, name+"="); ok {
				return v, nil
			}
			i++
			if i >= len(args) {
				return "", fmt.Errorf("%s requires a value", name)
			}
			return args[i], nil
		}
		var err error
		switch {
		case arg == "--set-mode-record" || strings.HasPrefix(arg, "--set-mode-record="):
			out.requested = true
			out.mode, err = value("--set-mode-record")
		case arg == "--reason" || strings.HasPrefix(arg, "--reason="):
			out.reason, err = value("--reason")
		case arg == "--force":
			out.force = true
		default:
			rest = append(rest, arg)
		}
		if err != nil {
			return out, nil, err
		}
	}
	if !out.requested && (out.reason != "" || out.force) {
		return out, nil, errors.New("--reason and --force belong to --set-mode-record")
	}
	if out.requested {
		if artifacts.AuthModeRank(out.mode) == 0 {
			return out, nil, fmt.Errorf("--set-mode-record %q is not asserted, observe or enforce", out.mode)
		}
		if strings.TrimSpace(out.reason) == "" || len(out.reason) > 256 || strings.ContainsAny(out.reason, "\r\n") {
			return out, nil, errors.New("--set-mode-record needs --reason, 1 to 256 bytes on one line")
		}
	}
	return out, rest, nil
}

// setModeRecord is the way back from a mode record no engine can read: it
// prints the store it is about to change, refuses a record that decodes
// unless --force, writes auth.mode.record.set (and stops, changing nothing,
// if that cannot be written), then overwrites the record and exits without
// serving.
func setModeRecord(ctx context.Context, a setModeRecordArgs, storeCfg artifacts.Config, audit *api.AuditRecorder, out io.Writer, logger *slog.Logger) error {
	modes, err := artifacts.NewS3ModeStore(ctx, storeCfg)
	if err != nil {
		return err
	}
	return setModeRecordIn(ctx, a, modes, storeCfg.S3Endpoint, audit, out, logger)
}

func setModeRecordIn(ctx context.Context, a setModeRecordArgs, modes *artifacts.ModeStore, endpoint string, audit *api.AuditRecorder, out io.Writer, logger *slog.Logger) error {
	fmt.Fprintf(out, "mode record: %s (endpoint %s)\n", modes.Location(), endpoint)
	rec, etag, err := modes.Get(ctx)
	replacing := "none"
	switch {
	case err == nil:
		replacing = rec.Mode
		if !a.force {
			return fmt.Errorf("the mode record decodes and names %q: lower a readable record with `orkctl admin auth lower`, or pass --force when no engine can start", rec.Mode)
		}
	case errors.Is(err, artifacts.ErrModeRecordUnreadable):
		replacing = "unreadable"
	case errors.Is(err, artifacts.ErrNotFound):
	default:
		return fmt.Errorf("read the mode record: %w", err)
	}
	host, _ := os.Hostname()
	at := time.Now().UTC()
	entry := &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion, Id: artifacts.NewAuditID(at), At: timestamppb.New(at),
		Action: "auth.mode.record.set", Procedure: "ork --set-mode-record", Target: a.mode,
		Detail: fmt.Sprintf("host=%s replacing=%s reason=%q force=%t", host, replacing, a.reason, a.force), Succeeded: true,
	}
	if err := audit.RecordErr(ctx, entry); err != nil {
		return fmt.Errorf("the audit record auth.mode.record.set could not be written, so nothing was changed: %w", err)
	}
	cond := artifacts.PutConditions{IfMatch: etag}
	if etag == "" {
		cond = artifacts.PutConditions{IfAbsent: true}
	}
	next := artifacts.ModeRecord{Mode: a.mode, RaisedAt: at, UpdatedAt: at, UpdatedBy: "ork --set-mode-record on " + host, Reason: a.reason}
	if _, err := modes.Put(ctx, next, cond); err != nil {
		return fmt.Errorf("write the mode record: %w", err)
	}
	logger.Error("mode record overwritten by ork --set-mode-record", "record", modes.Location(), "mode", a.mode,
		"replacing", replacing, "reason", a.reason, "host", host)
	fmt.Fprintf(out, "mode record set to %s (was %s)\n", a.mode, replacing)
	return nil
}

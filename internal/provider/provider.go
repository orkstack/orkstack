// Package provider is the one mechanism behind credential and secret
// providers (E81, E83): a provider logs in as the attempt with the attempt's
// own identity token, reads a secret or issues a bundle of files, and the
// Orker writes what it got as files. The dispatcher depends on the types of
// this package and on nothing provider-specific.
//
// The rule that makes this safe is that a provider is never called with the
// Orker's own credentials: its only way to authenticate is the TokenSource it
// is handed for one attempt. No type reads the Orker's environment, an instance
// role or a credential chain, but for the proxy settings a type's page says it
// follows (docs/PROVIDERS.md).
//
// "Pluggable" means a compiled-in Type selected by configuration from the
// closed set internal/provider/types holds, and nothing else: no Go plugin, no command run on a
// host, no network protocol, no discovery (AGENTS.md §13).
package provider

import (
	"context"
	"errors"
	"time"
)

// Type is one compiled-in kind of provider ("aws", "vault").
type Type interface {
	// Name is the value of a declaration's `type:` key.
	Name() string
	// New builds one declared provider from its configuration. It does no
	// I/O. It reads every key it accepts from config; a key it did not read
	// is refused by the document's parser, so a type never sees, and never
	// has to refuse, a key it does not know.
	New(name string, config *Config) (Provider, error)
}

// Provider is one declaration of the providers document (ORK_PROVIDERS).
type Provider interface {
	// Name is the declared name, which is the scheme of its sources.
	Name() string
	// Type is the name of the Type that built it.
	Type() string
	// Audience is the aud an identity token must carry to log in here.
	Audience() string
	// Describe is what may be shown to an operator: an address or a region,
	// never a secret.
	Describe() Description
	// LoginScheme is the one word of this type's login credentials in a
	// policy string, "<name>:<scheme>:<principal>" ("sts", "auth"): a
	// constant of the type, of ^[a-z][a-z0-9]{0,15}$.
	LoginScheme() string
	// Parse checks one reference. It does no I/O: the engine calls it at
	// validate, create and submit, the Orker at dispatch, and both must
	// agree. It decides nothing of the reference's policy string, which is
	// Policy's.
	Parse(ref Ref) (Parsed, error)
	// Login authenticates as principal with the attempt's identity. token is
	// the only credential it is ever given.
	Login(ctx context.Context, principal string, who Attempt, token TokenSource) (Session, error)
}

// Session is one attempt's login to one provider as one principal. One
// session serves every secret and credential of an attempt that names the
// same grant.
type Session interface {
	// Secret reads one value. It is called once and never refreshed.
	Secret(ctx context.Context, ref Parsed) (Value, error)
	// Credential issues one bundle of files.
	Credential(ctx context.Context, ref Parsed) (Bundle, error)
	// Renew extends a bundle where the provider can (a Vault lease).
	// ErrReissue tells the refresh engine to call Credential again instead.
	Renew(ctx context.Context, b Bundle) (Bundle, error)
	// Lifetime is how long the login itself lasts from now; zero means it
	// does not end.
	Lifetime() time.Duration
	// Close ends the login and revokes what it issued, where the provider
	// can. It is bounded by ctx; its error is counted and logged, never
	// ignored.
	Close(ctx context.Context) error
}

// LoginRenewer is a Session whose login can be extended (a Vault token). A
// session that has no such method is replaced by a new login.
type LoginRenewer interface {
	// RenewLogin extends the login in place and answers how long it now
	// lasts, from now. ErrReissue, or a life that is not longer than what was
	// left, has the refresh engine log in again. What the login issued stays
	// as it is: a bundle issued from it is not replaced.
	//
	// A renewal mints nothing, so the engine is not asked whether the grant
	// still stands: a login is replaced at MaxTaskLogin since it was made, an
	// instance's at Attempt.MaxLogin, however often it was renewed. A call
	// given up on at its timeout may still be running at the next one.
	RenewLogin(ctx context.Context) (time.Duration, error)
}

// SessionTagger is a Provider whose identity tokens may carry session tags
// (AWS's, read from the token itself). The engine asks at boot and pins what
// it was told into each run's admission; nothing here is asked of a session.
type SessionTagger interface {
	// SessionTags answers whether tokens for Audience carry tags, and which of
	// them follow the session through role chaining.
	SessionTags() (on bool, transitive []string)
}

// ErrReissue is what Session.Renew returns when the provider will not extend
// a bundle any further: the refresh engine then calls Credential for a new
// one. It is not a failure.
var ErrReissue = errors.New("provider: the bundle cannot be renewed; issue a new one")

// TokenSource mints the attempt's identity token for the provider's
// audience. It is the only credential a provider is ever given. lifetime is
// counted from the moment the token was received, on the caller's monotonic
// clock.
type TokenSource func(ctx context.Context) (token string, lifetime time.Duration, err error)

// Attempt names the attempt a login is for. A provider may use it to name
// the session where the provider shows one (an STS session name, which is
// what CloudTrail shows); it never authenticates anything.
type Attempt struct {
	Namespace string
	RunID     string
	TaskID    string
	// Attempt is the attempt's ordinal, from 1.
	Attempt int
	// Instance marks an event source's instance rather than a task, for the
	// stricter rules a provider applies to one (providers.md §6, §8).
	Instance bool
	// MaxLogin is, for an instance, identity.token.instance_max_ttl: the
	// longest login to ask for. The refresh engine fills it in.
	MaxLogin time.Duration
}

// Kind says what a reference asks for.
type Kind int

const (
	// KindSecret is one value, read once before the container starts.
	KindSecret Kind = iota + 1
	// KindCredential is a bundle of files with a lifetime, kept fresh while
	// the container runs.
	KindCredential
)

func (k Kind) String() string {
	switch k {
	case KindSecret:
		return "secret"
	case KindCredential:
		return "credential"
	default:
		return "unknown"
	}
}

// Ref is one reference a task makes to a provider, as the definition states
// it. Parse checks it; nothing else reads it.
type Ref struct {
	Kind Kind
	// Locator is a secret's source after "<name>:", or a credential's path,
	// or "" for a credential that is the login itself.
	Locator string
	// Ciphertext is a secret's ciphertext, for a provider that decrypts.
	Ciphertext []byte
	// Name is the credential's name in its task, for a file that names its
	// own path in the container. Only Session.Credential is given it: the
	// refresh engine fills it in, and no policy string holds it.
	Name string
	// Params are a credential's literal parameters, for a path that is
	// written rather than read (providers.md §8.3). Bounds: ValidParams.
	Params map[string]string
}

// Parsed is a reference a provider accepted. The provider keeps whatever it
// needs in Detail, which only it reads.
type Parsed struct {
	Ref    Ref
	Detail any
}

// Value is one secret.
type Value struct {
	Bytes []byte
	// Also are further values to mask beside Bytes. A provider can only add
	// to what the Orker masks: the Orker masks Bytes itself.
	Also [][]byte
}

// Bundle is one credential: a set of files with a lifetime.
type Bundle struct {
	// Files maps a file name to its content. Names: ValidFileName.
	Files map[string][]byte
	// Plain names the files that hold nothing sensitive (a config, an
	// address). Every other file is masked.
	Plain map[string]bool
	// Secrets are the values in it that are certainly secret (a secret access
	// key, a password, a token, a private key) and no other: a task that
	// returns one in values.json fails. A user name, a host or a certificate
	// is masked in logs and may be returned. At most MaxBundleSecrets.
	Secrets [][]byte
	// Lifetime is counted from the moment the answer was received, on the
	// Orker's monotonic clock. Zero means the bundle does not expire, so it
	// is never refreshed.
	Lifetime time.Duration
	// Handle is the provider's own state for Renew (a lease id). The Orker
	// never reads it, writes it or logs it.
	Handle any
}

// Description is what an operator may be shown of one declared provider.
// It never holds a secret: the document holds none.
type Description struct {
	Type     string
	Audience string
	// Fields are type-specific, such as "region" or "address".
	Fields map[string]string
}

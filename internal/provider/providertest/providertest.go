// Package providertest is the shared suite every provider type passes against
// its stand-in (providers.md §3, step 4): login as the attempt, a refusal, an
// outage, throttling, expiry, renewal, close, and "no error text holds a
// value". A provider type's own test builds a Harness around the real
// component it is tested against (LocalStack or Moto for aws, a Vault dev
// server for vault) and calls Conformance.
package providertest

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// Harness is one provider under test and the levers of its stand-in. A lever
// left nil skips the cases that need it, and the skip says why: a provider
// whose stand-in cannot throttle shows it in its own test's output.
type Harness struct {
	// Provider is declared against the stand-in.
	Provider provider.Provider
	// Principal is one the stand-in admits; Refused is one it refuses.
	Principal string
	Refused   string
	// Token mints a valid identity token for Provider.Audience().
	Token provider.TokenSource
	// Attempt is the attempt the logins are for.
	Attempt provider.Attempt

	// Secret, when set, is a reference the stand-in answers with
	// SecretValue.
	Secret      *provider.Ref
	SecretValue []byte
	// Missing, when set, is a secret reference the stand-in does not hold.
	Missing *provider.Ref
	// Credential, when set, is a reference that issues a bundle.
	Credential *provider.Ref
	// Expires says the credential's bundle has a lifetime.
	Expires bool

	// Outage makes the stand-in unreachable until restore is called.
	Outage func(t *testing.T) (restore func())
	// Throttle makes the stand-in throttle until restore is called.
	Throttle func(t *testing.T) (restore func())
	// Revoked reports whether what a session issued was revoked once the
	// session closed. Nil skips the check, for a provider whose credentials
	// end only at their own expiry.
	Revoked func(t *testing.T, b provider.Bundle) bool
	// Tokens are the identity tokens the stand-in was presented, in order: how
	// the suite sees that a login used the attempt's token and nothing else.
	Tokens func() []string
	// Values are every value the stand-in holds or issued that must never
	// appear in an error's text.
	Values func() [][]byte
}

// Conformance runs the suite.
func Conformance(t *testing.T, h Harness) {
	t.Helper()
	if h.Provider == nil || h.Token == nil || h.Principal == "" {
		t.Fatal("providertest: a Harness needs Provider, Token and Principal")
	}
	var minted []string
	token := func(ctx context.Context) (string, time.Duration, error) {
		tok, life, err := h.Token(ctx)
		if err == nil {
			minted = append(minted, tok)
		}
		return tok, life, err
	}
	values := func() [][]byte {
		var out [][]byte
		if h.Values != nil {
			out = append(out, h.Values()...)
		}
		for _, tok := range minted {
			out = append(out, []byte(tok))
		}
		if h.SecretValue != nil {
			out = append(out, h.SecretValue)
		}
		return out
	}
	ctx := context.Background()

	t.Run("Identity", func(t *testing.T) {
		p := h.Provider
		if err := provider.ValidName(p.Name()); err != nil {
			t.Errorf("Name() = %q: %v", p.Name(), err)
		}
		if a, b := p.LoginScheme(), p.LoginScheme(); a == "" || a != b {
			t.Errorf("LoginScheme() = %q then %q; it is one constant of the type", a, b)
		}
		if p.Type() == "" || p.Audience() == "" {
			t.Errorf("Type() = %q, Audience() = %q; both are required", p.Type(), p.Audience())
		}
		for key, v := range p.Describe().Fields {
			for _, value := range values() {
				if len(value) >= 8 && strings.Contains(v, string(value)) {
					t.Errorf("Describe().Fields[%q] holds a value the stand-in treats as secret", key)
				}
			}
		}
	})

	t.Run("Parse", func(t *testing.T) {
		for _, ref := range []*provider.Ref{h.Secret, h.Missing, h.Credential} {
			if ref == nil {
				continue
			}
			parsed, err := h.Provider.Parse(*ref)
			if err != nil {
				t.Errorf("Parse(%+v): %v", *ref, err)
				continue
			}
			if !reflect.DeepEqual(parsed.Ref, *ref) {
				t.Errorf("Parse(%+v).Ref = %+v; a reference is checked, never changed", *ref, parsed.Ref)
			}
			again, err := h.Provider.Parse(*ref)
			if err != nil || !reflect.DeepEqual(again, parsed) {
				t.Errorf("Parse is not a function of its input: %+v then %+v (%v)", parsed, again, err)
			}
			if policy, err := provider.Policy(h.Provider, *ref, h.Principal); err != nil {
				t.Errorf("a reference Parse accepts has no policy string: %+v as %q: %q, %v", *ref, h.Principal, policy, err)
			}
		}
		for _, bad := range []string{"../escape", "a/./b", "a//b", "a%2fb", "a?b", "a b"} {
			if _, err := h.Provider.Parse(provider.Ref{Kind: provider.KindSecret, Locator: bad}); err == nil {
				t.Errorf("Parse accepted the locator %q; a locator is validated, never normalized", bad)
			}
		}
	})

	t.Run("LoginAsTheAttempt", func(t *testing.T) {
		before := 0
		if h.Tokens != nil {
			before = len(h.Tokens())
		}
		mintedBefore := len(minted)
		s, err := h.Provider.Login(ctx, h.Principal, h.Attempt, token)
		if err != nil {
			t.Fatalf("Login(%q): %v", h.Principal, err)
		}
		defer closeOK(t, s)
		if len(minted) == mintedBefore {
			t.Fatal("Login did not ask the token source: a provider's only credential is the attempt's token")
		}
		if h.Tokens != nil {
			seen := h.Tokens()[before:]
			if len(seen) == 0 {
				t.Fatal("the stand-in saw no identity token")
			}
			for _, tok := range seen {
				if !slices.Contains(minted, tok) {
					t.Error("the stand-in was presented a token the attempt's token source did not mint")
				}
			}
		}
		if s.Lifetime() < 0 {
			t.Errorf("Lifetime() = %s; it is from now, never negative", s.Lifetime())
		}
		if renewer, ok := s.(provider.LoginRenewer); ok {
			switch life, err := renewer.RenewLogin(ctx); {
			case err != nil && !errors.Is(err, provider.ErrReissue):
				t.Errorf("RenewLogin = %v; a fresh login is extended, or ErrReissue", err)
			case err == nil && life <= 0:
				t.Errorf("RenewLogin = %s; how long the login now lasts, from now", life)
			}
		}
	})

	t.Run("ATokenSourceRefusalPassesThrough", func(t *testing.T) {
		refusal := &provider.TokenRefused{Reason: "IDENTITY_REVOKED"}
		_, err := h.Provider.Login(ctx, h.Principal, h.Attempt, func(context.Context) (string, time.Duration, error) {
			return "", 0, refusal
		})
		var got *provider.TokenRefused
		if !errors.As(err, &got) {
			t.Fatalf("Login with a refusing token source = %v; the refusal must pass through", err)
		}
		_, err = h.Provider.Login(ctx, h.Principal, h.Attempt, func(context.Context) (string, time.Duration, error) {
			return "", 0, errors.New("the engine did not answer")
		})
		if err == nil || provider.ClassOf(err) != provider.Unavailable {
			t.Errorf("Login with an unanswering token source = %v (class %s); want unavailable", err, provider.ClassOf(err))
		}
	})

	t.Run("Refusal", func(t *testing.T) {
		if h.Refused == "" {
			t.Skip("the stand-in has no principal it refuses")
		}
		s, err := h.Provider.Login(ctx, h.Refused, h.Attempt, token)
		if err == nil {
			// A provider may admit the login and refuse the read (Vault's
			// role exists, its policy does not cover the path).
			defer closeOK(t, s)
			if h.Secret == nil {
				t.Fatalf("Login(%q) succeeded; the stand-in refuses it", h.Refused)
			}
			_, err = s.Secret(ctx, mustParse(t, h.Provider, *h.Secret))
			if err == nil {
				t.Fatalf("Login(%q) and its read both succeeded; the stand-in refuses it", h.Refused)
			}
		}
		if c := provider.ClassOf(err); c != provider.Refused {
			t.Errorf("a refusal classified %s; want refused (%v)", c, err)
		}
		checkError(t, err, values())
	})

	t.Run("Outage", func(t *testing.T) {
		if h.Outage == nil {
			t.Skip("the stand-in cannot be made unreachable")
		}
		restore := h.Outage(t)
		callCtx, cancel := context.WithTimeout(ctx, provider.DefaultCallTimeout)
		_, err := h.Provider.Login(callCtx, h.Principal, h.Attempt, token)
		cancel()
		restore()
		if err == nil {
			t.Fatal("Login succeeded during an outage")
		}
		if c := provider.ClassOf(err); c != provider.Unavailable {
			t.Errorf("an outage classified %s; want unavailable (%v)", c, err)
		}
		checkError(t, err, values())
	})

	t.Run("Throttling", func(t *testing.T) {
		if h.Throttle == nil {
			t.Skip("the stand-in cannot throttle")
		}
		restore := h.Throttle(t)
		_, err := h.Provider.Login(ctx, h.Principal, h.Attempt, token)
		restore()
		if err == nil {
			t.Fatal("Login succeeded while throttled")
		}
		if c := provider.ClassOf(err); c != provider.Unavailable {
			t.Errorf("throttling classified %s; want unavailable (%v)", c, err)
		}
		checkError(t, err, values())
	})

	t.Run("Secret", func(t *testing.T) {
		if h.Secret == nil {
			t.Skip("the provider reads no secret")
		}
		s := mustLogin(t, h, token)
		defer closeOK(t, s)
		v, err := s.Secret(ctx, mustParse(t, h.Provider, *h.Secret))
		if err != nil {
			t.Fatalf("Secret: %v", err)
		}
		if !bytes.Equal(v.Bytes, h.SecretValue) {
			t.Errorf("Secret = %d bytes; want the stand-in's value", len(v.Bytes))
		}
		if h.Missing != nil {
			_, err := s.Secret(ctx, mustParse(t, h.Provider, *h.Missing))
			if c := provider.ClassOf(err); err == nil || c != provider.NotFound {
				t.Errorf("a missing secret = %v (class %s); want not_found", err, c)
			}
			checkError(t, err, values())
		}
	})

	t.Run("CredentialExpiryAndRenewal", func(t *testing.T) {
		if h.Credential == nil {
			t.Skip("the provider issues no credential")
		}
		s := mustLogin(t, h, token)
		defer closeOK(t, s)
		parsed := mustParse(t, h.Provider, *h.Credential)
		b, err := s.Credential(ctx, parsed)
		if err != nil {
			t.Fatalf("Credential: %v", err)
		}
		if err := provider.ValidBundle(b); err != nil {
			t.Fatalf("Credential's bundle cannot be put in place: %v", err)
		}
		if h.Expires && b.Lifetime <= 0 {
			t.Errorf("the bundle's lifetime is %s; the stand-in's credential expires", b.Lifetime)
		}
		renewed, err := s.Renew(ctx, b)
		switch {
		case errors.Is(err, provider.ErrReissue):
			again, err := s.Credential(ctx, parsed)
			if err != nil {
				t.Fatalf("Credential after ErrReissue: %v", err)
			}
			if err := provider.ValidBundle(again); err != nil {
				t.Errorf("a re-issued bundle cannot be put in place: %v", err)
			}
		case err != nil:
			t.Fatalf("Renew: %v", err)
		default:
			if err := provider.ValidBundle(renewed); err != nil {
				t.Errorf("a renewed bundle cannot be put in place: %v", err)
			}
		}
	})

	t.Run("Close", func(t *testing.T) {
		s := mustLogin(t, h, token)
		var b provider.Bundle
		if h.Credential != nil {
			var err error
			b, err = s.Credential(ctx, mustParse(t, h.Provider, *h.Credential))
			if err != nil {
				t.Fatalf("Credential: %v", err)
			}
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), provider.CloseTimeout)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if h.Revoked != nil && h.Credential != nil && !h.Revoked(t, b) {
			t.Error("the stand-in still honours what the closed session issued")
		}
	})
}

func mustLogin(t *testing.T, h Harness, token provider.TokenSource) provider.Session {
	t.Helper()
	s, err := h.Provider.Login(context.Background(), h.Principal, h.Attempt, token)
	if err != nil {
		t.Fatalf("Login(%q): %v", h.Principal, err)
	}
	return s
}

func mustParse(t *testing.T, p provider.Provider, ref provider.Ref) provider.Parsed {
	t.Helper()
	parsed, err := p.Parse(ref)
	if err != nil {
		t.Fatalf("Parse(%+v): %v", ref, err)
	}
	return parsed
}

func closeOK(t *testing.T, s provider.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), provider.CloseTimeout)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// checkError holds a provider's error to its contract: it is a
// *provider.Error, and its text holds none of the values.
func checkError(t *testing.T, err error, values [][]byte) {
	t.Helper()
	if err == nil {
		return
	}
	var pe *provider.Error
	if !errors.As(err, &pe) {
		t.Errorf("error %T is not a *provider.Error; it is the only error a provider returns", err)
	}
	text := err.Error()
	for _, value := range values {
		if len(value) >= 8 && strings.Contains(text, string(value)) {
			t.Errorf("an error's text holds a value: %q", strings.ReplaceAll(text, string(value), "<value>"))
		}
	}
}

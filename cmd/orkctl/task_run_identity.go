package main

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// A local run is no attempt Temporal knows: no engine issues it an identity
// and no provider a credential. Each one its task declares is supplied by the
// caller or left out by name, the rule secrets follow
// (docs/OCI_ACTIVITY_CONTRACT.md §10.4).

// localTokenLife is how long a supplied token that names no expiry of its own
// is said to live before its file is read again.
const localTokenLife = time.Hour

// maxLocalToken bounds one supplied token.
const maxLocalToken = 64 << 10

// suppliedIdentities is what --identity, --omit-identity, --credential and
// --omit-credential said.
type suppliedIdentities struct {
	tokens         map[string]string // name -> the token file's absolute path
	credentials    map[string]string // name -> the directory's absolute path
	omitTokens     map[string]bool
	omitCredential map[string]bool
	policy         orker.SecurityPolicy
}

// parseIdentityFlags parses the four flags and reads what they name once, so
// a file that could not be used is refused before anything is fetched.
func parseIdentityFlags(opts *taskRunOptions, policy orker.SecurityPolicy, now time.Time) (suppliedIdentities, error) {
	out := suppliedIdentities{tokens: map[string]string{}, credentials: map[string]string{},
		omitTokens: map[string]bool{}, omitCredential: map[string]bool{}, policy: policy}
	for _, kind := range []struct {
		flag, scheme string
		values       []string
		omitted      []string
		into         map[string]string
		omit         map[string]bool
		check        func(string) error
	}{
		{"identity", "file", opts.identities, opts.omitIdentities, out.tokens, out.omitTokens,
			func(path string) error { _, err := readLocalToken(path, policy, now); return err }},
		{"credential", "dir", opts.credentials, opts.omitCredentials, out.credentials, out.omitCredential,
			func(path string) error { _, err := orker.ReadLocalCredential(path, policy); return pathCause(err) }},
	} {
		for _, value := range kind.values {
			name, source, found := strings.Cut(value, "=")
			locator, schemed := strings.CutPrefix(source, kind.scheme+":")
			if !found || name == "" || !schemed || locator == "" {
				return out, fmt.Errorf("invalid --%s %q: want NAME=%s:PATH", kind.flag, value, kind.scheme)
			}
			if _, twice := kind.into[name]; twice {
				return out, fmt.Errorf("--%s %s is given twice", kind.flag, name)
			}
			abs, err := filepath.Abs(locator)
			if err != nil {
				return out, fmt.Errorf("--%s %s: %s: %w", kind.flag, name, locator, err)
			}
			if err := ownSourceFlag("--"+kind.flag, name, "file:"+abs, policy); err != nil {
				return out, err
			}
			if err := kind.check(abs); err != nil {
				return out, fmt.Errorf("--%s %s: %s: %w", kind.flag, name, locator, err)
			}
			kind.into[name] = abs
		}
		for _, name := range kind.omitted {
			if _, both := kind.into[name]; both {
				return out, fmt.Errorf("%s %q is both supplied and omitted", kind.flag, name)
			}
			kind.omit[name] = true
		}
	}
	return out, nil
}

// problems is every flag that names what the task does not declare and, unless
// nothing starts, every one it declares that was neither supplied nor omitted.
func (s suppliedIdentities) problems(spec *orkstackv1.Task, renderOnly bool) []string {
	var out []string
	var tokens, credentials []string
	for _, id := range spec.GetIdentities() {
		tokens = append(tokens, id.GetName())
	}
	for _, c := range spec.GetCredentials() {
		credentials = append(credentials, c.GetName())
	}
	for _, kind := range []struct {
		word, example string
		declared      []string
		supplied      map[string]string
		omitted       map[string]bool
	}{
		{"identity", "file:PATH", tokens, s.tokens, s.omitTokens},
		{"credential", "dir:PATH", credentials, s.credentials, s.omitCredential},
	} {
		list := "none"
		if len(kind.declared) > 0 {
			list = strings.Join(kind.declared, ", ")
		}
		for _, flag := range []struct {
			name  string
			names []string
		}{{"--" + kind.word, slices.Sorted(maps.Keys(kind.supplied))}, {"--omit-" + kind.word, slices.Sorted(maps.Keys(kind.omitted))}} {
			for _, name := range flag.names {
				if !slices.Contains(kind.declared, name) {
					out = append(out, fmt.Sprintf("%s %s: task %q declares no %s named %q (declared: %s)",
						flag.name, name, spec.GetId(), kind.word, name, list))
				}
			}
		}
		if renderOnly {
			continue
		}
		for _, name := range kind.declared {
			if _, ok := kind.supplied[name]; ok || kind.omitted[name] {
				continue
			}
			out = append(out, fmt.Sprintf("task %q declares %s %q, which a local run is not issued; supply it with --%s %s=%s, "+
				"or run without it with --omit-%s %s", spec.GetId(), kind.word, name, kind.word, name, kind.example, kind.word, name))
		}
	}
	return out
}

// local is what the dispatcher is given, false with nothing supplied: each
// declared identity that was supplied, whose file is read again at every
// refresh, and each supplied credential, in the order declared.
func (s suppliedIdentities) local(spec *orkstackv1.Task) (orker.LocalIdentity, bool) {
	var local orker.LocalIdentity
	for _, id := range spec.GetIdentities() {
		if _, ok := s.tokens[id.GetName()]; ok {
			local.Identities = append(local.Identities, id)
		}
	}
	for _, c := range spec.GetCredentials() {
		if dir, ok := s.credentials[c.GetName()]; ok {
			local.Credentials = append(local.Credentials, orker.LocalCredential{Name: c.GetName(), Dir: dir})
		}
	}
	local.Mint = func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		now := time.Now()
		resp := &orkstackv1.MintAttemptTokensResponse{}
		for _, want := range req.GetTokens() {
			supplied, err := readLocalToken(s.tokens[want.GetName()], s.policy, now)
			if err != nil {
				return nil, fmt.Errorf("--identity %s: %w", want.GetName(), err)
			}
			resp.Tokens = append(resp.Tokens, &orkstackv1.IssuedToken{Name: want.GetName(), Audience: want.GetAudience(), Token: supplied.token,
				ExpiresInSeconds: int32(supplied.life / time.Second), IssuedAt: timestamppb.New(now)})
		}
		return resp, nil
	}
	return local, len(local.Identities) > 0 || len(local.Credentials) > 0
}

// notes is what a supplied token's file says that its container will read
// otherwise: the expiration of a token that names none is this run's own.
func (s suppliedIdentities) notes(now time.Time) []string {
	var out []string
	for _, name := range slices.Sorted(maps.Keys(s.tokens)) {
		if token, err := readLocalToken(s.tokens[name], s.policy, now); err == nil && !token.expires {
			out = append(out, fmt.Sprintf("note: --identity %s: its token names no expiry; its expiration file says %s from each read of it", name, localTokenLife))
		}
	}
	return out
}

// localToken is a supplied token and the life it has left, and whether the
// token says so itself.
type localToken struct {
	token   string
	life    time.Duration
	expires bool
}

// readLocalToken is the token path holds, one line, and the life it has left:
// to its own expiry where it is a JWT that names one, localTokenLife otherwise.
// Nothing of it is verified: what it is worth is for whoever it is shown to.
func readLocalToken(path string, policy orker.SecurityPolicy, now time.Time) (localToken, error) {
	raw, err := orker.ReadLocalToken(path, maxLocalToken+1, policy)
	if err != nil {
		return localToken{}, pathCause(err)
	}
	token := strings.TrimRight(string(raw), "\r\n")
	switch {
	case len(raw) > maxLocalToken:
		return localToken{}, fmt.Errorf("it is over %d bytes", maxLocalToken)
	case token == "" || strings.ContainsAny(token, "\r\n"):
		return localToken{}, errors.New("it does not hold one token on one line")
	}
	at, named, err := unverifiedExpiry(token)
	switch {
	case err != nil:
		return localToken{}, err
	case !named:
		return localToken{token: token, life: localTokenLife}, nil
	}
	seconds := int64(at.Sub(now) / time.Second)
	if seconds <= 0 {
		return localToken{}, fmt.Errorf("its token expired at %s", at.UTC().Format(time.RFC3339))
	}
	return localToken{token: token, life: time.Duration(min(seconds, math.MaxInt32)) * time.Second, expires: true}, nil
}

// unverifiedExpiry is the exp a JWT's payload names, and whether it names one:
// a number of seconds that may have a fraction (RFC 7519, NumericDate). A token
// that is no JWT names none.
func unverifiedExpiry(token string) (time.Time, bool, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false, nil
	}
	var claims map[string]jsontext.Value
	if err := json.Unmarshal(payload, &claims); err != nil || claims["exp"] == nil {
		return time.Time{}, false, nil
	}
	var exp float64
	if err := json.Unmarshal(claims["exp"], &exp); err != nil || exp < 0 || exp > 1<<40 {
		return time.Time{}, false, errors.New("its token's exp is no time")
	}
	return time.Unix(int64(exp), 0), true, nil
}

// pathCause is what the file system said, without the path the sentence
// already names.
func pathCause(err error) error {
	if pathErr := (*os.PathError)(nil); errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}

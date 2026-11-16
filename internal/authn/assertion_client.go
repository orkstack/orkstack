package authn

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// MaxPrivateKeyFileBytes bounds a service account's private key file.
const MaxPrivateKeyFileBytes = 64 << 10

// ReadSignerFile reads a service account's private key (jwt.ParsePrivateKeyPEM) from path, at most
// MaxPrivateKeyFileBytes. label names the setting in every error.
func ReadSignerFile(path, label string) (*jwt.Signer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxPrivateKeyFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", label, path, err)
	}
	if len(raw) > MaxPrivateKeyFileBytes {
		return nil, fmt.Errorf("%s %s is larger than a private key (%d bytes)", label, path, MaxPrivateKeyFileBytes)
	}
	s, err := jwt.ParsePrivateKeyPEM(raw)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", label, path, err)
	}
	return s, nil
}

// AssertionTransport authenticates every request it sends as a service
// account: it signs one assertion bound to the request's method, URL and
// body (authn-authz.md §3.2) and sends it as a bearer. A request's body is
// read whole to hash it, at most AssertionMaxBody, the bound the engine
// reads it under. orkctl and an Orker sign with it, so both bind a request
// the same way.
type AssertionTransport struct {
	// Base sends the signed request; nil is http.DefaultTransport.
	Base    http.RoundTripper
	Signer  *jwt.Signer
	Account string
	// Audience is the deployment's public URL.
	Audience string
	// Reason is sent as Ork-Reason; break-glass requires one.
	Reason string
	Now    func() time.Time
}

// RoundTrip signs a copy of req and sends it. The caller's request is never
// changed.
func (t *AssertionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	closeBody := func() {
		if req.Body != nil {
			_ = req.Body.Close()
		}
	}
	if t.Signer == nil || t.Account == "" || t.Audience == "" {
		closeBody()
		return nil, errors.New("service-account signing is not configured")
	}
	var body []byte
	if req.Body != nil && req.Body != http.NoBody {
		var err error
		body, err = io.ReadAll(io.LimitReader(req.Body, AssertionMaxBody+1))
		closeBody()
		if err != nil {
			return nil, fmt.Errorf("read the request body to sign it: %w", err)
		}
		if len(body) > AssertionMaxBody {
			return nil, fmt.Errorf("the request body is over the %d bytes an assertion is signed for", AssertionMaxBody)
		}
	}
	now := time.Now
	if t.Now != nil {
		now = t.Now
	}
	token, err := t.Signer.SignRequest(jwt.AssertionRequest{Account: t.Account, Audience: t.Audience,
		Method: req.Method, URL: req.URL.String(), Body: body}, now(), nil)
	if err != nil {
		return nil, fmt.Errorf("sign the request as service account %q: %w", t.Account, err)
	}
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+token)
	if t.Reason != "" {
		out.Header.Set(ReasonHeader, t.Reason)
	}
	if body != nil {
		out.Body = io.NopCloser(bytes.NewReader(body))
		out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
		out.ContentLength = int64(len(body))
	}
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(out)
}

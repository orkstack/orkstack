package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// maxAnswerBytes bounds an answer: a value's bound with room for the JSON
// around it and its escapes.
const maxAnswerBytes = 8 * provider.MaxValueBytes

// httpClient is the provider's HTTP client, made at its first call, when the
// CA file is read. It takes no proxy from the environment, and follows no
// redirect, which would hand a login token to whatever host the answer named.
// With no CA file the roots are the host's, as Go finds them.
type httpClient struct {
	caFile string

	mu     sync.Mutex
	client *http.Client
}

func (h *httpClient) get() (*http.Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.client != nil {
		return h.client, nil
	}
	conf := &tls.Config{MinVersion: tls.VersionTLS12}
	if h.caFile != "" {
		pem, err := os.ReadFile(h.caFile)
		if err != nil {
			return nil, err
		}
		conf.RootCAs = x509.NewCertPool()
		if !conf.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("the CA file holds no certificate")
		}
	}
	h.client = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: provider.DefaultCallTimeout}).DialContext,
			TLSClientConfig:     conf,
			TLSHandshakeTimeout: provider.DefaultCallTimeout,
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     time.Minute,
		},
	}
	return h.client, nil
}

// call makes one request to an API path with token, empty for a login, and
// returns the answer's body when its status is a success.
func (p *vaultProvider) call(ctx context.Context, op, method, path, token string, in any) ([]byte, error) {
	client, err := p.http.get()
	if err != nil {
		return nil, failed(provider.Unavailable, op, "ca-file", err)
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return nil, failed(provider.Invalid, op, "request", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiURL(p.address, path), body)
	if err != nil {
		return nil, failed(provider.Invalid, op, "request", err)
	}
	req.Header.Set("X-Vault-Request", "true")
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if p.namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.namespace)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, failed(provider.Unavailable, op, "no-answer", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Its body is Vault's own words, never read further than lets the
		// connection be used again.
		_, _ = io.CopyN(io.Discard, resp.Body, 4<<10)
		class, code := classify(op, resp.StatusCode)
		if code == "redirect" {
			slog.Warn("vault answered with a redirect, which is not followed: a login's token would go with it; point the provider's address at the active node or a load balancer", "provider", p.name)
		}
		return nil, failed(class, op, code, nil)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes+1))
	if err != nil {
		return nil, failed(provider.Unavailable, op, "cut-answer", err)
	}
	if len(raw) > maxAnswerBytes {
		return nil, failed(provider.Invalid, op, "answer-too-large", nil)
	}
	return raw, nil
}

// classify is the provider's row of the classification table (§11). Anything
// not in it is Unavailable.
func classify(op string, status int) (provider.Class, string) {
	code := strconv.Itoa(status)
	switch {
	case status >= 300 && status < 400:
		return provider.Unavailable, "redirect"
	case status == http.StatusForbidden:
		return provider.Refused, code
	case status == http.StatusBadRequest && (op == opLogin || op == opIssue):
		return provider.Refused, code
	case status == http.StatusNotFound && (op == opRead || op == opIssue):
		return provider.NotFound, code
	}
	return provider.Unavailable, code
}

// is reports whether err is this package's answer with code.
func is(err error, code string) bool {
	var pe *provider.Error
	return errors.As(err, &pe) && pe.Code == code
}

package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
)

// postDeclared POSTs body to path, declaring contentType, or no Content-Type
// at all when contentType is empty, and returns the status and response body.
func (f *auditFixture) postDeclared(t *testing.T, path, contentType, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, payload
}

// A browser sends a cross-site POST without asking first only when its body is
// declared text/plain, application/x-www-form-urlencoded or
// multipart/form-data, or not declared at all. The adapter acts on none of
// them: it refuses with 415 before it reads the body, so nothing is
// registered, and, like a Connect call of an unsupported codec, the refusal is
// never a write the audit trail records.
func TestWebhookAdapter_RegisterRefusesABodyNotDeclaredJSON(t *testing.T) {
	const body = `{"url":"https://hooks.example.test/cross-site","events":["RUN_SUCCEEDED"]}`
	for _, contentType := range []string{
		"", // no Content-Type: what a Blob body without a type sends
		"text/plain",
		"text/plain;charset=UTF-8",          // what fetch sends for a string body
		"application/x-www-form-urlencoded", // what curl -d sends
		"multipart/form-data; boundary=x",
		"application/merge-patch+json",
		"application/jsonp",
		"application/json; charset", // a malformed parameter
		"application/json, text/plain",
	} {
		t.Run(contentType, func(t *testing.T) {
			f := newAuditFixture(t, auditOptions{})
			status, payload := f.postDeclared(t, api.WebhookCollectionPath, contentType, body)
			if status != http.StatusUnsupportedMediaType {
				t.Fatalf("status = %d, want 415: %s", status, payload)
			}
			var envelope struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(payload, &envelope); err != nil {
				t.Fatalf("decode error envelope: %v (%s)", err, payload)
			}
			if envelope.Error.Code != "invalid_argument" || !strings.Contains(envelope.Error.Message, "application/json") {
				t.Errorf("error = %+v, want invalid_argument naming application/json", envelope.Error)
			}
			hooks, err := f.svc.Webhooks().ListContext(context.Background())
			if err != nil || len(hooks) != 0 {
				t.Errorf("registry = %v, %v; want nothing registered", hooks, err)
			}
			if records := f.records(t); len(records) != 0 {
				t.Errorf("audit records = %v, want none", records)
			}
		})
	}

	// The positive control: the same body declared JSON, with or without
	// parameters and in any case, is registered and audited, so the refusals
	// above are the declaration's doing and not a broken fixture's.
	for _, contentType := range []string{
		"application/json",
		"application/json; charset=utf-8",
		"Application/JSON;Charset=UTF-8",
	} {
		t.Run(contentType, func(t *testing.T) {
			f := newAuditFixture(t, auditOptions{})
			status, payload := f.postDeclared(t, api.WebhookCollectionPath, contentType, body)
			if status != http.StatusCreated {
				t.Fatalf("status = %d, want 201: %s", status, payload)
			}
			hooks, err := f.svc.Webhooks().ListContext(context.Background())
			if err != nil || len(hooks) != 1 || hooks[0].URL != "https://hooks.example.test/cross-site" {
				t.Errorf("registry = %v, %v; want the one subscription", hooks, err)
			}
			records := f.records(t)
			if len(records) != 1 || records[0].GetAction() != "webhook.register" || !records[0].GetSucceeded() {
				t.Errorf("audit records = %v, want one successful webhook.register", records)
			}
		})
	}
}

// The Connect services already need a body declared as one of their codecs:
// connect-go answers 415 before any handler or interceptor runs. Pinned here
// because docs/SECURITY_MODEL.md relies on it for every Connect write.
func TestConnectWrite_RefusesABodyNotDeclaredJSONOrProto(t *testing.T) {
	const body = `{"url":"https://hooks.example.test/cross-site"}`
	for _, contentType := range []string{"", "text/plain", "text/plain;charset=UTF-8", "application/x-www-form-urlencoded"} {
		t.Run(contentType, func(t *testing.T) {
			f := newAuditFixture(t, auditOptions{})
			status, payload := f.postDeclared(t, orkstackv1connect.WebhookServiceRegisterWebhookProcedure, contentType, body)
			if status != http.StatusUnsupportedMediaType {
				t.Fatalf("status = %d, want 415: %s", status, payload)
			}
			hooks, err := f.svc.Webhooks().ListContext(context.Background())
			if err != nil || len(hooks) != 0 {
				t.Errorf("registry = %v, %v; want nothing registered", hooks, err)
			}
			if records := f.records(t); len(records) != 0 {
				t.Errorf("audit records = %v, want none", records)
			}
		})
	}
	f := newAuditFixture(t, auditOptions{})
	if status, payload := f.postDeclared(t, orkstackv1connect.WebhookServiceRegisterWebhookProcedure, "application/json", body); status != http.StatusOK {
		t.Fatalf("declared application/json: status = %d, want 200: %s", status, payload)
	}
}

// A browser asks before any cross-site request that is not a simple one: a
// DELETE, a body declared application/json, or an Authorization header the
// page set. The
// engine grants no such request: it answers a preflight without an
// Access-Control-Allow-Origin, so the browser never sends the request.
func TestAPIRoutes_GrantNoCrossOriginPreflight(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	for _, c := range []struct{ path, method, headers string }{
		{api.WebhookCollectionPath, http.MethodPost, "content-type"},
		{"/v1/webhooks/whk-1", http.MethodDelete, ""},
		{"/v1/triggers/trg-1", http.MethodPost, "authorization,content-type"},
		{"/v1/runs/ork-1/awaits/gate", http.MethodPost, "authorization"},
		{"/v1/runs/ork-1/awaits/gate/reject", http.MethodPost, "authorization"},
		{orkstackv1connect.WebhookServiceRegisterWebhookProcedure, http.MethodPost, "content-type"},
	} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, f.server.URL+c.path, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Origin", "https://attacker.example")
		req.Header.Set("Access-Control-Request-Method", c.method)
		if c.headers != "" {
			req.Header.Set("Access-Control-Request-Headers", c.headers)
		}
		resp, err := f.server.Client().Do(req)
		if err != nil {
			t.Fatalf("OPTIONS %s: %v", c.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Errorf("OPTIONS %s (%s) = %d, want a refusal", c.path, c.method, resp.StatusCode)
		}
		for name := range resp.Header {
			if strings.HasPrefix(strings.ToLower(name), "access-control-") {
				t.Errorf("OPTIONS %s (%s) answered %s: %q", c.path, c.method, name, resp.Header.Values(name))
			}
		}
	}
	if records := f.records(t); len(records) != 0 {
		t.Errorf("a preflight was recorded: %v", records)
	}
}

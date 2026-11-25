package providertest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/internal/scripted"
	"github.com/orkstack/orkstack/internal/provider/providertest"
)

// TestConformance_TheSuiteAgainstTheScriptedProvider runs the suite against
// the scripted provider, whose outage, throttling and refusal are levers a
// test pulls: it proves the suite exercises each case. Every built-in type
// runs the same suite against a real component (P16, P17a).
func TestConformance_TheSuiteAgainstTheScriptedProvider(t *testing.T) {
	p := scripted.New("kv", "vault.internal")
	p.Admit("payments-app")
	p.SetLifetime(30*time.Minute, 1)
	p.SetSecret("secret/data/db", []byte("s3cr3t-value"))
	n := 0
	providertest.Conformance(t, providertest.Harness{
		Provider:  p,
		Principal: "payments-app",
		Refused:   "intruder",
		Token: func(context.Context) (string, time.Duration, error) {
			n++
			return fmt.Sprintf("identity-token-%04d", n), time.Hour, nil
		},
		Attempt:     provider.Attempt{Namespace: "payments", RunID: "ork-1", TaskID: "migrate", Attempt: 1},
		Secret:      &provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/db#url"},
		SecretValue: []byte("s3cr3t-value"),
		Missing:     &provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/none"},
		Credential:  &provider.Ref{Kind: provider.KindCredential, Locator: "creds/payments-rw"},
		Expires:     true,
		Outage: func(*testing.T) func() {
			p.Down()
			return p.Up
		},
		Throttle: func(*testing.T) func() {
			p.Script("login", &provider.Error{Class: provider.Unavailable, Op: "login", Code: "Throttling"})
			return func() {}
		},
		Revoked: func(_ *testing.T, b provider.Bundle) bool { return p.Revoked(b) },
		Tokens:  p.Tokens,
	})
}

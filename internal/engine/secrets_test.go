package engine_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

var sandboxSecrets = []string{"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"}

func secretTask(id string, secrets ...*orkstackv1.SecretRef) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: "shell", Secrets: secrets}
}

// TestCheckAllowedSecrets_EmptyListVisitsNoTask: a namespace with no policy
// pays nothing and refuses nothing — not even a source no list could ever
// match.
func TestCheckAllowedSecrets_EmptyListVisitsNoTask(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "exfil", Tasks: []*orkstackv1.Task{
		secretTask("grab",
			&orkstackv1.SecretRef{Name: "db", Source: "NO_SCHEME"},
			&orkstackv1.SecretRef{Name: "up", Source: "file:/a/../b"}),
	}}
	for _, ns := range []*orkstackv1.Namespace{nil, {Name: "sandbox"}, {Name: "sandbox", AllowedSecrets: []string{}}} {
		if err := engine.CheckAllowedSecrets(ns, wf); err != nil {
			t.Fatalf("CheckAllowedSecrets(%v) = %v, want nil", ns, err)
		}
	}
}

func TestCheckAllowedSecrets_ReturnsTheFirstRefusalInDocumentOrder(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: sandboxSecrets}
	wf := &orkstackv1.Orkflow{Name: "exfil", Tasks: []*orkstackv1.Task{
		secretTask("fine", &orkstackv1.SecretRef{Name: "token", Source: "env:SANDBOX_TOKEN"}),
		secretTask("grab",
			&orkstackv1.SecretRef{Name: "db", Source: "env:PROD_DB_PASSWORD"},
			&orkstackv1.SecretRef{Name: "key", Source: "env:PROD_KEY"}),
	}}
	err := engine.CheckAllowedSecrets(ns, wf)
	var refused *engine.SecretNotAllowedError
	if !errors.As(err, &refused) {
		t.Fatalf("CheckAllowedSecrets = %v, want a *SecretNotAllowedError", err)
	}
	if refused.TaskIndex != 1 || refused.SecretIndex != 0 || refused.Task != "grab" || refused.Secret != "db" {
		t.Fatalf("refusal coordinates = task %d %q, secret %d %q; want task 1 grab, secret 0 db",
			refused.TaskIndex, refused.Task, refused.SecretIndex, refused.Secret)
	}
	if got := refused.Field(); got != "tasks[1].secrets[0].source" {
		t.Fatalf("Field() = %q", got)
	}
	want := `namespace "sandbox" may not resolve "env:PROD_DB_PASSWORD" (task "grab", secret "db"); allowed: env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*`
	if err.Error() != want {
		t.Fatalf("Error() =\n%s\nwant\n%s", err, want)
	}
	var policy *dsl.SecretSourceRefusal
	if !errors.As(err, &policy) || policy.Why != "" {
		t.Fatalf("Unwrap() does not reach the dsl refusal: %v", err)
	}
}

// TestCheckAllowedSecrets_QuotesALongSourceByItsHead: a source is the task
// author's to write, of any length, so the refusal quotes a long one as every
// secret refusal does (dsl.QuoteSecretSource), by its head and its length.
func TestCheckAllowedSecrets_QuotesALongSourceByItsHead(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: sandboxSecrets}
	source := "env:PROD_" + strings.Repeat("A", 200<<10)
	wf := &orkstackv1.Orkflow{Name: "exfil", Tasks: []*orkstackv1.Task{
		secretTask("grab", &orkstackv1.SecretRef{Name: "db", Source: source}),
	}}
	err := engine.CheckAllowedSecrets(ns, wf)
	want := fmt.Sprintf(`namespace "sandbox" may not resolve %q… (%d bytes) (task "grab", secret "db"); allowed: `+
		`env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*`, source[:64], len(source))
	if err == nil || err.Error() != want {
		t.Fatalf("Error() = %d bytes, want\n%s", len(fmt.Sprint(err)), want)
	}
}

func TestCheckAllowedSecrets_TraversalSentence(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: sandboxSecrets}
	wf := &orkstackv1.Orkflow{Name: "exfil", Tasks: []*orkstackv1.Task{
		secretTask("grab", &orkstackv1.SecretRef{Name: "db", Source: "file:/etc/orkstack/secrets/sandbox/../payments/db"}),
	}}
	err := engine.CheckAllowedSecrets(ns, wf)
	var refused *engine.SecretNotAllowedError
	if !errors.As(err, &refused) || refused.Refusal == nil || refused.Refusal.Why != ".." {
		t.Fatalf("CheckAllowedSecrets = %#v, want a refusal with Why \"..\"", err)
	}
	want := `namespace "sandbox" may not resolve "file:/etc/orkstack/secrets/sandbox/../payments/db" (task "grab", secret "db"): a "file:" locator with a ".." segment is refused, never normalized; allowed: env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*`
	if err.Error() != want {
		t.Fatalf("Error() =\n%s\nwant\n%s", err, want)
	}
	for source, phrase := range map[string]string{
		"file:/etc/orkstack/secrets/sandbox/./db": `a "file:" locator with a "." segment is refused, never normalized`,
		"file:/etc/orkstack/secrets/sandbox//db":  `a "file:" locator with an empty segment is refused, never normalized`,
		"file:etc/orkstack/secrets/sandbox/db":    `a "file:" locator with a relative path is refused, never normalized`,
	} {
		wf.Tasks[0].Secrets[0].Source = source
		err := engine.CheckAllowedSecrets(ns, wf)
		want := `namespace "sandbox" may not resolve "` + source + `" (task "grab", secret "db"): ` + phrase +
			`; allowed: env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*`
		if err == nil || err.Error() != want {
			t.Errorf("Error() =\n%v\nwant\n%s", err, want)
		}
	}
}

// TestSecretsReadByAGrant_AreTheGrantsToJudge: a secret read from a provider
// through a grant is skipped here, where its whole source would be judged;
// one that names the Orker's own source is judged whatever grant it names,
// and one with no grant as ever. The same holds of a shared Orkflow's.
func TestSecretsReadByAGrant_AreTheGrantsToJudge(t *testing.T) {
	for _, tc := range []struct {
		secret  *orkstackv1.SecretRef
		skipped bool
	}{
		{&orkstackv1.SecretRef{Name: "s", Source: "vault:secret/db#url", Grant: "reader"}, true},
		{&orkstackv1.SecretRef{Name: "s", Source: "vault:secret/db#url"}, false},
		{&orkstackv1.SecretRef{Name: "s", Source: "env:DB", Grant: "reader"}, false},
		{&orkstackv1.SecretRef{Name: "s", Source: "file:/run/db", Grant: "reader"}, false},
	} {
		wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{{Id: "t", Secrets: []*orkstackv1.SecretRef{tc.secret}}}}
		bounded := engine.CheckAllowedSecrets(&orkstackv1.Namespace{Name: "payments", AllowedSecrets: []string{"env:OTHER"}}, wf)
		foreign := engine.CheckForeignSecrets(&orkstackv1.Namespace{Name: "payments"}, "platform", wf)
		if (bounded == nil) != tc.skipped || (foreign == nil) != tc.skipped {
			t.Errorf("%s with grant %q: allowed_secrets %v, shared %v; skipped should be %v", tc.secret.GetSource(), tc.secret.GetGrant(), bounded, foreign, tc.skipped)
		}
	}
}

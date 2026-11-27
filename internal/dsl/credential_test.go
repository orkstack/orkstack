package dsl_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

const twoCredentials = `    credentials:
      - name: aws
        grant: deployer
      - name: tls
        grant: vault-app
        path: pki-int/issue/payments-server
        params:
          common_name: payments.internal
          ttl: 12h
        refresh: best_effort
`

// TestCompile_CredentialsCompile: with task identity available, each
// credential compiles with its name, grant, path, params and refresh policy.
func TestCompile_CredentialsCompile(t *testing.T) {
	wf, err := dsl.Compile([]byte(identitySource(twoCredentials)))
	if err != nil {
		t.Fatal(err)
	}
	creds := wf.GetTasks()[0].GetCredentials()
	if len(creds) != 2 || creds[0].GetName() != "aws" || creds[0].GetGrant() != "deployer" || creds[0].GetPath() != "" ||
		creds[0].GetRefresh() != orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED ||
		creds[1].GetPath() != "pki-int/issue/payments-server" || creds[1].GetParams()["ttl"] != "12h" ||
		creds[1].GetRefresh() != orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT {
		t.Fatalf("credentials = %v", creds)
	}
}

// TestValidate_CredentialRules: one row a rule: 56 (name, grant, path,
// params' shape, count), 57 (params are literals), 58 (a fan-out's
// credentials are not templated) and 54 (none on an awaited, sleeping or
// composed task).
func TestValidate_CredentialRules(t *testing.T) {
	cred := func(fields string) string { return "    credentials:\n      - " + fields + "\n" }
	nine := "    credentials:\n"
	for i := range 9 {
		nine += "      - {name: c" + string(rune('a'+i)) + ", grant: g}\n"
	}
	cases := []struct {
		name, source, want string
	}{
		{"a name with a capital", identitySource(cred("{name: Aws, grant: g}")), "credentials[0].name"},
		{"a name twice", identitySource("    credentials:\n      - {name: a, grant: g}\n      - {name: a, grant: h}\n"), "credentials[1].name"},
		{"no grant", identitySource(cred("{name: a}")), "credentials[0].grant"},
		{"a grant of the wrong shape", identitySource(cred("{name: a, grant: Deployer}")), "credentials[0].grant"},
		{"a path that climbs", identitySource(cred("{name: a, grant: g, path: 'database/../sys/raw'}")), "credentials[0].path"},
		{"a path that is percent-encoded", identitySource(cred("{name: a, grant: g, path: 'database%2fcreds'}")), "credentials[0].path"},
		{"a path with a field", identitySource(cred("{name: a, grant: g, path: 'database/creds/x#password'}")), "credentials[0].path"},
		{"params with no path", identitySource(cred("{name: a, grant: g, params: {ttl: 1h}}")), "credentials[0].params"},
		{"a params key of the wrong shape", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: {TTL: 1h}}")), "credentials[0].params"},
		{"a templated param", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: {common_name: '{{ .inputs.host }}'}}")), "credentials[0].params"},
		{"a refresh policy", identitySource(cred("{name: a, grant: g, refresh: sometimes}")), "credentials[0].refresh"},
		{"nine credentials", identitySource(nine), "tasks[0].credentials"},
		{"an unknown key", identitySource(cred("{name: a, grant: g, audience: x}")), "audience"},
		{"a fan-out's templated path", identitySource(fanOut + cred("{name: a, grant: g, path: '{{ .item }}'}")), "same for every item"},
		{"a fan-out's templated grant", identitySource(fanOut + cred("{name: a, grant: '{{ .item }}'}")), "same for every item"},
		{"a fan-out's templated name", identitySource(fanOut + cred("{name: '{{ .item }}', grant: g}")), "same for every item"},
		{"seventeen params", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: " + params(17, 1) + "}")), "credentials[0].params"},
		{"a param of 1025 bytes", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: {ttl: " + strings.Repeat("v", 1025) + "}}")), "credentials[0].params"},
		{"params over 4 KiB", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: " + params(5, 1000) + "}")), "credentials[0].params"},
		{"a param with a control character", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: {ttl: \"1h\\u0001\"}}")), "credentials[0].params"},
		{"a path over 1024 bytes", identitySource(cred("{name: a, grant: g, path: " + strings.Repeat("p", 1025) + "}")), "credentials[0].path"},
	}
	// Rule 60: on a task that runs no container, refused on the key itself.
	noContainer := []struct{ name, source string }{
		{"an awaited task", "apiVersion: orkstack/v1\nkind: Orkflow\nname: w\ntasks:\n  - id: approve\n    await: {timeout_seconds: 60}\n" + cred("{name: a, grant: g}")},
		{"a sleeping task", "apiVersion: orkstack/v1\nkind: Orkflow\nname: w\ntasks:\n  - id: wait\n    sleep: {seconds: 60}\n" + cred("{name: a, grant: g}")},
		{"a composed task", "apiVersion: orkstack/v1\nkind: Orkflow\nname: w\ntasks:\n  - id: child\n    uses: orkflow:child@1.0.0\n" + cred("{name: a, grant: g}")},
	}
	// Controls: at each bound, and a fan-out with literal credentials,
	// everything holds until the build's own gate.
	atTheBounds := []struct{ name, source string }{
		{"eight credentials", identitySource(eight())},
		{"sixteen params of 255 bytes", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: " + params(16, 240) + "}"))},
		{"a param of 1024 bytes", identitySource(cred("{name: a, grant: g, path: pki/issue/r, params: {ttl: " + strings.Repeat("v", 1024) + "}}"))},
		{"a fan-out's literal credentials", identitySource(fanOut + cred("{name: a, grant: g, path: pki/issue/r, params: {ttl: 1h}}"))},
	}
	for _, c := range cases {
		_, err := dsl.Compile([]byte(c.source))
		if !names(err, c.want) {
			t.Errorf("%s: %v, want it to name %s", c.name, err, c.want)
		}
	}
	for _, c := range noContainer {
		_, err := dsl.Compile([]byte(c.source))
		var ve *dsl.ValidationError
		if !errors.As(err, &ve) || ve.Field != "tasks[0].credentials" {
			t.Errorf("credentials on %s = %v; want a refusal on tasks[0].credentials", c.name, err)
		}
	}
	for _, c := range atTheBounds {
		if _, err := dsl.Compile([]byte(c.source)); err != nil {
			t.Errorf("%s = %v; want it to compile", c.name, err)
		}
	}
}

const fanOut = "    each: {over: '{{ .inputs.files }}', max_items: 5}\n"

// params is a flow mapping of n keys, each value width bytes.
func params(n, width int) string {
	var b strings.Builder
	b.WriteString("{")
	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("k" + strings.Repeat("a", i) + ": " + strings.Repeat("v", width))
	}
	b.WriteString("}")
	return b.String()
}

func eight() string {
	s := "    credentials:\n"
	for i := range 8 {
		s += "      - {name: c" + string(rune('a'+i)) + ", grant: g}\n"
	}
	return s
}

// TestCanonical_DigestUnchangedWithoutCredentials: a task's canonical form
// holds credentials only when it declares some, so every definition stored
// before keeps its digest.
func TestCanonical_DigestUnchangedWithoutCredentials(t *testing.T) {
	plain, err := dsl.Compile([]byte(identitySource("")))
	if err != nil {
		t.Fatal(err)
	}
	before, err := dsl.CanonicalOrkflow(plain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(before), "credentials") {
		t.Fatalf("a task with no credential has them in its canonical form: %s", before)
	}
	with, err := dsl.Compile([]byte(identitySource(twoCredentials)))
	if err != nil {
		t.Fatal(err)
	}
	after, err := dsl.CanonicalOrkflow(with)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), `"credentials"`) || !strings.Contains(string(after), "payments.internal") {
		t.Fatalf("a task's credentials are not in its canonical form: %s", after)
	}
}

// TestValidate_ProviderSecretRules: rule 65, one row each, with controls at
// the bounds that compile until the build's own gate.
func TestValidate_ProviderSecretRules(t *testing.T) {
	secret := func(fields string) string { return identitySource("    secrets:\n      - " + fields + "\n") }
	b64 := func(n int) string { return base64.StdEncoding.EncodeToString(make([]byte, n)) }
	cases := []struct{ name, source, want string }{
		{"a grant with an env source", secret("{name: s, source: 'env:S', grant: g}"), "secrets[0].grant"},
		{"a grant with a file source", secret("{name: s, source: 'file:/run/s', grant: g}"), "secrets[0].grant"},
		{"a grant of the wrong shape", secret("{name: s, source: 'kv:a/b', grant: G}"), "secrets[0].grant"},
		{"a provider name of the wrong shape", secret("{name: s, source: 'my-vault:a/b', grant: g}"), "secrets[0].source"},
		{"a locator that climbs", secret("{name: s, source: 'kv:a/../b', grant: g}"), "secrets[0].source"},
		{"a locator that is percent-encoded", secret("{name: s, source: 'kv:a%2fb', grant: g}"), "secrets[0].source"},
		{"a ciphertext with no grant", secret("{name: s, source: 'kv:a', ciphertext: QUJD}"), "secrets[0].ciphertext"},
		{"a ciphertext that is not base64", secret("{name: s, source: 'kv:a', grant: g, ciphertext: 'not base64!'}"), "secrets[0].ciphertext"},
		{"a ciphertext with a trailing line break", secret("{name: s, source: 'kv:a', grant: g, ciphertext: \"QUJD\\n\"}"), "secrets[0].ciphertext"},
		{"a ciphertext broken over lines", secret("{name: s, source: 'kv:a', grant: g, ciphertext: \"QU\\nJD\"}"), "secrets[0].ciphertext"},
		{"a ciphertext without its padding", secret("{name: s, source: 'kv:a', grant: g, ciphertext: QUI}"), "secrets[0].ciphertext"},
		{"a URL-safe ciphertext", secret("{name: s, source: 'kv:a', grant: g, ciphertext: '-_-_'}"), "secrets[0].ciphertext"},
		{"a provider source with no grant", secret("{name: s, source: 'vault:secret/data/db'}"), "secrets[0].grant"},
		{"a ciphertext of 8193 characters", secret("{name: s, source: 'kv:a', grant: g, ciphertext: " + strings.Repeat("A", 8193) + "}"), "secrets[0].ciphertext"},
		{"ciphertexts over 16 KiB a task", identitySource("    secrets:\n      - {name: a, source: 'kv:a', grant: g, ciphertext: " + b64(6144) + "}\n      - {name: b, source: 'kv:b', grant: g, ciphertext: " + b64(6144) + "}\n      - {name: c, source: 'kv:c', grant: g, ciphertext: " + b64(4097) + "}\n"), "tasks[0].secrets"},
	}
	atTheBounds := []struct{ name, source string }{
		{"a provider secret with a field", secret("{name: s, source: 'vault:secret/data/payments/db#url', grant: vault-app}")},
		{"a ciphertext of 8192 characters", secret("{name: s, source: 'aws:kms:alias/k', grant: reader, ciphertext: " + b64(6144) + "}")},
		{"ciphertexts of exactly 16 KiB a task", identitySource("    secrets:\n      - {name: a, source: 'kv:a', grant: g, ciphertext: " + b64(6144) + "}\n      - {name: b, source: 'kv:b', grant: g, ciphertext: " + b64(6144) + "}\n      - {name: c, source: 'kv:c', grant: g, ciphertext: " + b64(4096) + "}\n")},
	}
	for _, c := range cases {
		_, err := dsl.Compile([]byte(c.source))
		var ve *dsl.ValidationError
		if !errors.As(err, &ve) || ve.Field != "tasks[0]."+strings.TrimPrefix(c.want, "tasks[0].") {
			t.Errorf("%s: %v; want a refusal on %s", c.name, err, c.want)
		}
	}
	for _, c := range atTheBounds {
		if _, err := dsl.Compile([]byte(c.source)); err != nil {
			t.Errorf("%s = %v; want it to compile", c.name, err)
		}
	}
	// A secret from the Orker's own sources is untouched by all of it.
	for _, source := range []string{"'env:S'", "'file:/run/secrets/s'"} {
		if _, err := dsl.Compile([]byte(secret("{name: s, source: " + source + "}"))); err != nil {
			t.Errorf("an Orker's own secret %s = %v", source, err)
		}
	}
}

// TestCanonical_DigestUnchangedWithoutProviderSecrets: grant and ciphertext
// are in a secret's canonical form only when set.
func TestCanonical_DigestUnchangedWithoutProviderSecrets(t *testing.T) {
	plain, err := dsl.Compile([]byte(identitySource("    secrets:\n      - {name: s, source: 'env:S'}\n")))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := dsl.CanonicalOrkflow(plain)
	if strings.Contains(string(before), "grant") || strings.Contains(string(before), "ciphertext") {
		t.Fatalf("an env secret's canonical form names a grant or a ciphertext: %s", before)
	}
	with, err := dsl.Compile([]byte(identitySource("    secrets:\n      - {name: s, source: 'aws:kms:alias/k', grant: reader, ciphertext: QUJD}\n")))
	if err != nil {
		t.Fatal(err)
	}
	after, _ := dsl.CanonicalOrkflow(with)
	if !strings.Contains(string(after), `"grant":"reader"`) || !strings.Contains(string(after), `"ciphertext":"QUJD"`) {
		t.Fatalf("a provider secret's canonical form lacks its grant or ciphertext: %s", after)
	}
}

// TestCanonical_AnEnvSecretsBytesAreUnchanged pins the canonical form of a
// task with an env secret, as it was before grant and ciphertext existed.
func TestCanonical_AnEnvSecretsBytesAreUnchanged(t *testing.T) {
	wf, err := dsl.Compile([]byte(identitySource("    secrets:\n      - {name: s, source: 'env:S'}\n")))
	if err != nil {
		t.Fatal(err)
	}
	got, err := dsl.CanonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	if want := envSecretCanonical; string(got) != want {
		t.Errorf("the canonical form of a task with an env secret changed:\n got %s\nwant %s", got, want)
	}
}

// envSecretCanonical is the canonical form of that task, captured from the
// tree before grant and ciphertext existed.
const envSecretCanonical = `{"description":"","inputs":{"files":{"type":"array"}},"labels":{},"name":"uploader","namespace":"","tasks":[{"cancellationGracePeriodSeconds":0,"command":[],"condition":"","env":{},"id":"upload","image":null,"needs":[],"network":0,"pool":"","resources":null,"retry":null,"runAsOrkerUser":false,"secrets":[{"name":"s","source":"env:S"}],"timeoutSeconds":300,"uses":"shell","with":null}],"version":"d3a3204d3818047ea3a74d723ef7f54768b4b82c53e83234b2e7ece2348548fa"}`

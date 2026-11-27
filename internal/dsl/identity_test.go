package dsl_test

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// identitySource is a one-task document whose task carries extra.
func identitySource(extra string) string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: uploader\n" +
		"inputs: {files: {type: array}}\ntasks:\n  - id: upload\n    uses: shell\n" + extra
}

const twoIdentities = `    identities:
      - name: gcp
        audience: //iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/ork/providers/ork
        ttl_seconds: 1800
      - name: vault
        audience: https://vault.example
        refresh: best_effort
`

// TestCompile_ATaskWithNoIdentity: a document without one compiles as it did.
func TestCompile_ATaskWithNoIdentity(t *testing.T) {
	if _, err := dsl.Compile([]byte(identitySource(""))); err != nil {
		t.Fatalf("a task with no identity = %v", err)
	}
}

// TestCompile_IdentitiesCompile: with task identity available, each
// identity compiles with its name, audience, life and refresh policy.
func TestCompile_IdentitiesCompile(t *testing.T) {
	wf, err := dsl.Compile([]byte(identitySource(twoIdentities)))
	if err != nil {
		t.Fatal(err)
	}
	ids := wf.GetTasks()[0].GetIdentities()
	if len(ids) != 2 || ids[0].GetName() != "gcp" || ids[0].GetTtlSeconds() != 1800 ||
		ids[0].GetRefresh() != orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED ||
		ids[1].GetAudience() != "https://vault.example" || ids[1].GetTtlSeconds() != 0 ||
		ids[1].GetRefresh() != orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT {
		t.Fatalf("identities = %v", ids)
	}
}

// TestValidate_IdentityRules: one row a rule: 53 (name, audience, life,
// count), 54 (no identity on an awaited, sleeping or composed task) and 55
// (a fan-out's identities are not templated).
func TestValidate_IdentityRules(t *testing.T) {
	id := func(fields string) string { return "    identities:\n      - " + fields + "\n" }
	nine := "    identities:\n"
	for i := range 9 {
		nine += "      - {name: i" + string(rune('a'+i)) + ", audience: a}\n"
	}
	cases := []struct {
		name, source, want string
	}{
		{"a name with a capital", identitySource(id("{name: Gcp, audience: a}")), "identities[0].name"},
		{"a name of 64 bytes", identitySource(id("{name: a" + strings.Repeat("b", 63) + ", audience: a}")), "identities[0].name"},
		{"a name twice", identitySource("    identities:\n      - {name: a, audience: x}\n      - {name: a, audience: y}\n"), "identities[1].name"},
		{"no audience", identitySource(id("{name: a}")), "identities[0].audience"},
		{"an audience with a space", identitySource(id("{name: a, audience: 'a b'}")), "identities[0].audience"},
		{"an audience of 256 bytes", identitySource(id("{name: a, audience: " + strings.Repeat("a", 256) + "}")), "identities[0].audience"},
		{"a life under ten minutes", identitySource(id("{name: a, audience: a, ttl_seconds: 599}")), "identities[0].ttl_seconds"},
		{"a refresh policy", identitySource(id("{name: a, audience: a, refresh: sometimes}")), "identities[0].refresh"},
		{"nine identities", identitySource(nine), "tasks[0].identities"},
		{"an unknown key", identitySource(id("{name: a, audience: a, path: x}")), "path"},
		{"on an awaited task", "apiVersion: orkstack/v1\nkind: Orkflow\nname: w\ntasks:\n  - id: approve\n    await: {timeout_seconds: 60}\n" + id("{name: a, audience: a}"), "identities"},
		{"on a sleeping task", "apiVersion: orkstack/v1\nkind: Orkflow\nname: w\ntasks:\n  - id: wait\n    sleep: {seconds: 60}\n" + id("{name: a, audience: a}"), "identities"},
		{"a fan-out's templated audience", identitySource("    each: {over: '{{ .inputs.files }}', max_items: 5}\n" + id("{name: a, audience: '{{ .item }}'}")), "identities[0].audience"},
	}
	for _, c := range cases {
		_, err := dsl.Compile([]byte(c.source))
		if !names(err, c.want) {
			t.Errorf("%s: %v, want it to name %s", c.name, err, c.want)
		}
	}
	ok := identitySource(id("{name: a, audience: a, ttl_seconds: 600}"))
	if _, err := dsl.Compile([]byte(ok)); err != nil {
		t.Errorf("a life of exactly ten minutes = %v", err)
	}
}

// TestCanonical_DigestUnchangedWithoutIdentities: a task's canonical form
// holds identities only when it declares some, so every definition stored
// before keeps its digest, and one that declares them has another.
func TestCanonical_DigestUnchangedWithoutIdentities(t *testing.T) {
	plain, err := dsl.Compile([]byte(identitySource("")))
	if err != nil {
		t.Fatal(err)
	}
	before, err := dsl.CanonicalOrkflow(plain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(before), "identities") {
		t.Fatalf("a task with no identity has them in its canonical form: %s", before)
	}
	with, err := dsl.Compile([]byte(identitySource(twoIdentities)))
	if err != nil {
		t.Fatal(err)
	}
	after, err := dsl.CanonicalOrkflow(with)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), `"identities"`) || !strings.Contains(string(after), "https://vault.example") {
		t.Fatalf("a task's identities are not in its canonical form: %s", after)
	}
}

// names says whether err names want: as the field it is about, or in its
// message.
func names(err error, want string) bool {
	if err == nil {
		return false
	}
	var ve *dsl.ValidationError
	if errors.As(err, &ve) && strings.Contains(ve.Field, want) {
		return true
	}
	return strings.Contains(err.Error(), want)
}

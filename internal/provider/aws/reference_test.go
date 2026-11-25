package aws

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
)

func TestParse(t *testing.T) {
	blob := []byte("ciphertext")
	for name, tc := range map[string]struct {
		ref  provider.Ref
		want reference
		err  string
	}{
		"a parameter":                  {ref: secret("ssm:db-url"), want: reference{service: serviceSSM, id: "db-url"}},
		"a parameter by its path":      {ref: secret("ssm:/payments/db/url"), want: reference{service: serviceSSM, id: "/payments/db/url"}},
		"a parameter's version":        {ref: secret("ssm:/payments/db:3"), want: reference{service: serviceSSM, id: "/payments/db:3"}},
		"a parameter's ARN":            {ref: secret("ssm:arn:aws:ssm:eu-west-1:123456789012:parameter/payments/db"), want: reference{service: serviceSSM, id: "arn:aws:ssm:eu-west-1:123456789012:parameter/payments/db"}},
		"a secret":                     {ref: secret("secretsmanager:payments/db"), want: reference{service: serviceSecrets, id: "payments/db"}},
		"a secret's ARN and key":       {ref: secret("secretsmanager:arn:aws:secretsmanager:eu-west-1:123456789012:secret:payments/db-AbCdEf#url"), want: reference{service: serviceSecrets, id: "arn:aws:secretsmanager:eu-west-1:123456789012:secret:payments/db-AbCdEf", field: "url"}},
		"a key's alias":                {ref: kms("kms:alias/payments", blob), want: reference{service: serviceKMS, id: "alias/payments"}},
		"a key's id":                   {ref: kms("kms:1234abcd-12ab-34cd-56ef-1234567890ab", blob), want: reference{service: serviceKMS, id: "1234abcd-12ab-34cd-56ef-1234567890ab"}},
		"a multi-region key's ARN":     {ref: kms("kms:arn:aws:kms:eu-west-1:123456789012:key/mrk-1234abcd12ab34cd56ef1234567890ab", blob), want: reference{service: serviceKMS, id: "arn:aws:kms:eu-west-1:123456789012:key/mrk-1234abcd12ab34cd56ef1234567890ab"}},
		"the largest ciphertext":       {ref: kms("kms:alias/a", bytes.Repeat([]byte("x"), maxCiphertext)), want: reference{service: serviceKMS, id: "alias/a"}},
		"the login":                    {ref: provider.Ref{Kind: provider.KindCredential}, want: reference{}},
		"a credential with a path":     {ref: provider.Ref{Kind: provider.KindCredential, Locator: "creds/a"}, err: "takes no path"},
		"a credential with params":     {ref: provider.Ref{Kind: provider.KindCredential, Params: map[string]string{"ttl": "1h"}}, err: "takes no params"},
		"a credential's ciphertext":    {ref: provider.Ref{Kind: provider.KindCredential, Ciphertext: blob}, err: "takes no ciphertext"},
		"no service":                   {ref: secret("db-url"), err: "ssm:<name>, secretsmanager:<id> or kms:<key>"},
		"another service":              {ref: secret("s3:bucket"), err: "ssm:<name>, secretsmanager:<id> or kms:<key>"},
		"the login's scheme":           {ref: secret("sts:role"), err: "ssm:<name>, secretsmanager:<id> or kms:<key>"},
		"no name":                      {ref: secret("ssm:"), err: "not a name ssm takes"},
		"a name SSM does not take":     {ref: secret("ssm:db@prod"), err: "not a name ssm takes"},
		"a parameter's key":            {ref: secret("ssm:db#url"), err: "only a secretsmanager source"},
		"a key's key":                  {ref: kms("kms:alias/a#x", blob), err: "only a secretsmanager source"},
		"a key with no ciphertext":     {ref: secret("kms:alias/a"), err: "it has none"},
		"a ciphertext over KMS's":      {ref: kms("kms:alias/a", bytes.Repeat([]byte("x"), maxCiphertext+1)), err: "at most 6144"},
		"a parameter's ciphertext":     {ref: kms("ssm:db", blob), err: "only a kms source"},
		"an ARN of another region":     {ref: secret("ssm:arn:aws:ssm:us-east-1:123456789012:parameter/x"), err: "another region"},
		"a secret of another region":   {ref: secret("secretsmanager:arn:aws:secretsmanager:us-east-1:123456789012:secret:x"), err: "another region"},
		"a key of another region":      {ref: kms("kms:arn:aws:kms:us-east-1:123456789012:alias/x", blob), err: "another region"},
		"a climb in the first segment": {ref: secret("ssm:../team-b/x"), err: "never normalized"},
		"a climb after a colon":        {ref: secret("ssm:a:.."), err: "never normalized"},
		"a climb in a secret's id":     {ref: secret("secretsmanager:../x"), err: "never normalized"},
		"a parameter ARN with no /":    {ref: secret("ssm:arn:aws:ssm:eu-west-1:123456789012:parameterx"), err: "not a name ssm takes"},
		"a key's id that is none":      {ref: kms("kms:abc", blob), err: "not a name kms takes"},
		"key/ outside an ARN":          {ref: kms("kms:key/1234abcd-12ab-34cd-56ef-1234567890ab", blob), err: "not a name kms takes"},
		"an ARN of another service":    {ref: secret("secretsmanager:arn:aws:iam::123456789012:role/x"), err: "not a name secretsmanager takes"},
		"a KMS ARN that is no key":     {ref: kms("kms:arn:aws:kms:eu-west-1:123456789012:grant/x", blob), err: "not a name kms takes"},
		"a path that climbs":           {ref: secret("ssm:/a/../b"), err: "never normalized"},
		"an unknown kind":              {ref: provider.Ref{Locator: "ssm:a"}, err: "kind unknown"},
		"a param on a secret":          {ref: provider.Ref{Kind: provider.KindSecret, Locator: "ssm:a", Params: map[string]string{"x": "y"}}, err: "takes no params"},
		"an empty key after the sha":   {ref: secret("secretsmanager:a#"), err: "empty field"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parse(tc.ref, "eu-west-1")
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("parse(%+v) = %+v, %v; want an error holding %q", tc.ref, got, err, tc.err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("parse(%+v) = %+v, %v; want %+v", tc.ref, got, err, tc.want)
			}
		})
	}
}

// FuzzParse: whatever the reference, a refusal holds no control character; an
// accepted one is the locator's own bytes split, never changed; and it has a
// policy string that is the locator without its key.
func FuzzParse(f *testing.F) {
	for _, l := range []string{"ssm:/payments/db:3", "secretsmanager:payments/db#url", "kms:alias/a", "", "sts:role", "ssm:/a/../b",
		"secretsmanager:arn:aws:secretsmanager:eu-west-1:123456789012:secret:x-AbCdEf#k", "ssm:a\x00b", "kms:arn:aws:kms:r:123456789012:key/mrk-a", "a#b#c"} {
		f.Add(l, []byte("blob"), true)
		f.Add(l, []byte(nil), true)
		f.Add(l, []byte(nil), false)
	}
	p := mustNew(f, "region: eu-west-1")
	f.Fuzz(func(t *testing.T, locator string, ciphertext []byte, isSecret bool) {
		ref := provider.Ref{Kind: provider.KindCredential, Locator: locator, Ciphertext: ciphertext}
		if isSecret {
			ref.Kind = provider.KindSecret
		}
		parsed, err := p.Parse(ref)
		if err != nil {
			if strings.ContainsFunc(err.Error(), func(r rune) bool { return r < 0x20 || r == 0x7f }) {
				t.Fatalf("a refusal holds a control character: %q", err)
			}
			return
		}
		if !reflect.DeepEqual(parsed.Ref, ref) {
			t.Fatalf("Parse changed the reference: %+v to %+v", ref, parsed.Ref)
		}
		detail := parsed.Detail.(reference)
		if !isSecret {
			if detail != (reference{}) || locator != "" {
				t.Fatalf("a credential parsed to %+v from %q", detail, locator)
			}
			return
		}
		spelled := string(detail.service) + ":" + detail.id
		policy, err := provider.Policy(p, ref, "arn:aws:iam::123456789012:role/r")
		if err != nil || policy != p.Name()+":"+spelled {
			t.Fatalf("the policy string of %q is %q, %v; want %q", locator, policy, err, p.Name()+":"+spelled)
		}
		if detail.field != "" {
			spelled += "#" + detail.field
		}
		if spelled != locator {
			t.Fatalf("%q was read as %q", locator, spelled)
		}
	})
}

func secret(locator string) provider.Ref {
	return provider.Ref{Kind: provider.KindSecret, Locator: locator}
}

func kms(locator string, ciphertext []byte) provider.Ref {
	return provider.Ref{Kind: provider.KindSecret, Locator: locator, Ciphertext: ciphertext}
}

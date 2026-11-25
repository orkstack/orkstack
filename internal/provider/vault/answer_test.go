package vault

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

func TestSecretValue(t *testing.T) {
	const v1 = `{"data":{"password":"s3crét","port":5432,"on":true,"none":null,"nested":{"a":"b"},"data":{"password":"inner"}}}`
	const v2 = `{"data":{"data":{"password":"two"},"metadata":{"version":3}}}`
	for _, c := range []struct {
		name, body string
		d          detail
		want       string
		class      provider.Class
		ok         bool
	}{
		{"a field", v1, detail{field: "password"}, "s3crét", 0, true},
		{"a number's own spelling", v1, detail{field: "port"}, "5432", 0, true},
		{"a boolean", v1, detail{field: "on"}, "true", 0, true},
		{"an object", v1, detail{field: "nested"}, `{"a":"b"}`, 0, true},
		{"v1 never looks inside data", v1, detail{field: "data"}, `{"password":"inner"}`, 0, true},
		{"v2 looks inside data", v2, detail{field: "password", kv2: true}, "two", 0, true},
		{"v2 never reads the metadata", v2, detail{field: "metadata", kv2: true}, "", provider.NotFound, false},
		{"v2's whole value", v2, detail{kv2: true}, `{"password":"two"}`, 0, true},
		{"a field that is not there", v1, detail{field: "user"}, "", provider.NotFound, false},
		{"a null field", v1, detail{field: "none"}, "", provider.NotFound, false},
		{"a deleted v2 version", `{"data":{"data":null,"metadata":{}}}`, detail{kv2: true}, "", provider.NotFound, false},
		{"no data", `{"auth":null}`, detail{}, "", provider.Unavailable, false},
		{"a v1 answer read as v2", `{"data":{"password":"x"}}`, detail{kv2: true}, "", provider.Unavailable, false},
		{"not JSON", `<html>`, detail{}, "", provider.Unavailable, false},
		{"a key twice", `{"data":{"a":"1","a":"2"}}`, detail{field: "a"}, "", provider.Unavailable, false},
		{"more parts than are masked", `{"data":{"a":[` + strings.Repeat("1,", maxScalars) + `1]}}`, detail{}, "", provider.Invalid, false},
		{"a second value", `{"data":{"a":"1"}}{}`, detail{field: "a"}, "", provider.Unavailable, false},
	} {
		v, err := secretValue([]byte(c.body), c.d)
		if (err == nil) != c.ok || string(v.Bytes) != c.want || (err != nil && provider.ClassOf(err) != c.class) {
			t.Errorf("%s: %q, %v (class %s); want %q", c.name, v.Bytes, err, provider.ClassOf(err), c.want)
		}
	}
	whole, err := secretValue([]byte(`{"data":{"user":"u-longer","pass":"p-longer","n":1,"deep":{"x":["y-longer"]}}}`), detail{})
	if err != nil {
		t.Fatal(err)
	}
	deep := `{"data":{"a":` + strings.Repeat("[", 4000) + `"x-longer"` + strings.Repeat("]", 4000) + `}}`
	if v, err := secretValue([]byte(deep), detail{}); err != nil || len(v.Also) != 1 {
		t.Fatalf("a value 4000 deep = %d parts, %v", len(v.Also), err)
	}
	var also []string
	for _, a := range whole.Also {
		also = append(also, string(a))
	}
	slices.Sort(also)
	if strings.Join(also, ",") != "1,p-longer,u-longer,y-longer" {
		t.Fatalf("a whole value also masks %v, want each scalar of it", also)
	}
}

func TestLeaseBundle(t *testing.T) {
	b, err := leaseBundle([]byte(`{"lease_id":"database/creds/app/x","lease_duration":3600,"renewable":true,"data":{"username":"u","password":"p","opts":{"a":1},"a/b":"z"}}`), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.ValidBundle(b); err != nil {
		t.Fatal(err)
	}
	if string(b.Files["username"]) != "u" || string(b.Files["password"]) != "p" || b.Files["opts"] != nil || b.Files["a/b"] != nil ||
		!strings.Contains(string(b.Files["secret.json"]), `"a/b":"z"`) || len(b.Plain) != 0 || b.Lifetime.Seconds() != 3600 {
		t.Fatalf("bundle = %+v", b)
	}
	for field, secret := range map[string]bool{"password": true, "token": true, "secret_key": true, "security_token": true,
		"private_key": true, "secret_id": true, "client_secret": true, "username": false, "access_key": false, "client_id": false, "lease": false} {
		b, err := leaseBundle([]byte(`{"lease_id":"x","lease_duration":60,"data":{"`+field+`":"a-value","password_empty":"","password":""}}`), time.Now())
		if field == "password" {
			b, err = leaseBundle([]byte(`{"lease_id":"x","lease_duration":60,"data":{"password":"a-value"}}`), time.Now())
		}
		if err != nil || provider.ValidBundle(b) != nil || (len(b.Secrets) == 1 && string(b.Secrets[0]) == "a-value") != secret || len(b.Secrets) > 1 {
			t.Errorf("%s: secrets %q, %v; want it named secret %v", field, b.Secrets, err, secret)
		}
	}
	many := make([]string, 0, 20)
	for _, c := range "abcdefghijklmnopqrst" {
		many = append(many, `"`+string(c)+`":"1"`)
	}
	for name, body := range map[string]string{
		"no lease":        `{"lease_id":"","lease_duration":0,"data":{"a":"b"}}`,
		"a lease forever": `{"lease_id":"x","lease_duration":0,"data":{"a":"b"}}`,
		"a negative life": `{"lease_id":"x","lease_duration":-1,"data":{"a":"b"}}`,
		"no data":         `{"lease_id":"x","lease_duration":60}`,
		"too many files":  `{"lease_id":"x","lease_duration":60,"data":{` + strings.Join(many, ",") + `}}`,
	} {
		if _, err := leaseBundle([]byte(body), time.Now()); err == nil {
			t.Errorf("%s: a bundle was made of it", name)
		}
	}
}

// FuzzVaultResponse: no answer, however malformed, panics or yields a value
// from anywhere but where the configuration says it is; a bundle that is made
// can be put in place or is refused by the framework's own check.
func FuzzVaultResponse(f *testing.F) {
	for _, s := range []string{`{"data":{"data":{"a":"b"}}}`, `{"data":{"a":"b"},"lease_id":"x","lease_duration":60}`, `{}`, `[]`, `null`, `{"data":[1]}`, `{"lease_duration":1e400}`} {
		f.Add([]byte(s), "a", true)
		f.Add([]byte(s), "", false)
	}
	f.Fuzz(func(t *testing.T, body []byte, field string, kv2 bool) {
		v, err := secretValue(body, detail{field: field, kv2: kv2})
		if err != nil && v.Bytes != nil {
			t.Fatalf("an error with a value: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), "\n") {
			t.Fatalf("an error of two lines: %q", err)
		}
		if b, err := leaseBundle(body, time.Now()); err == nil && (len(b.Files) == 0 || b.Files["secret.json"] == nil || b.Lifetime <= 0) {
			t.Fatalf("a bundle of nothing: %+v", b)
		}
	})
}

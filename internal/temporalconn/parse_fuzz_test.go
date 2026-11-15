package temporalconn

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/testpki"
)

// certErrors are every refusal parseCertificates may give: words of its own,
// never the file's.
var certErrors = regexp.MustCompile(`^(holds no PEM certificate|PEM block [0-9]+ is not a certificate( that parses)?)$`)

// FuzzParseCertificates holds what a CA or certificate file's parser claims:
// it never panics; it accepts only input whose every PEM block is a
// certificate that parses, at least one, and returns each of them in order;
// and a refusal is one of its own sentences, never a piece of the input.
func FuzzParseCertificates(f *testing.F) {
	ca := testpki.NewCA(f, "seed")
	leaf := ca.Issue(f, testpki.LeafOptions{Name: "seed", Hosts: []string{"127.0.0.1"}})
	f.Add(ca.PEM)
	f.Add(append(append([]byte("comment\n"), leaf.CertPEM...), ca.PEM...))
	f.Add(leaf.KeyPEM)
	f.Add([]byte("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"))
	f.Add([]byte(""))
	f.Add([]byte("not pem at all"))
	f.Fuzz(func(t *testing.T, data []byte) {
		certs, err := parseCertificates(data)
		if err != nil {
			if !certErrors.MatchString(err.Error()) {
				t.Fatalf("refusal %q is not one of parseCertificates' own", err)
			}
			if certs != nil {
				t.Fatal("a refusal returned certificates")
			}
			return
		}
		if len(certs) == 0 {
			t.Fatal("accepted input with no certificate")
		}
		rest := data
		for i, c := range certs {
			idx := bytes.Index(rest, []byte("-----BEGIN"))
			if idx < 0 {
				t.Fatalf("certificate %d has no PEM block in the input", i)
			}
			if c.Raw == nil {
				t.Fatalf("certificate %d has no DER", i)
			}
			rest = rest[idx+1:]
		}
	})
}

// FuzzLoadAPIKey: the key is the file's content without surrounding white
// space; a file of white space alone, or a key with a byte a header cannot
// carry, is refused with words of its own.
func FuzzLoadAPIKey(f *testing.F) {
	f.Add([]byte("key\n"))
	f.Add([]byte(" \r\n"))
	f.Add([]byte("a b\tc\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := loadAPIKey([][]byte{data})
		want := string(bytes.TrimSpace(data))
		if want == "" {
			if err == nil || err.Error() != refusal(SettingAPIKeyFile, "the file is empty").Error() {
				t.Fatalf("white space alone: err = %v, want the empty-file refusal", err)
			}
			return
		}
		printable := true
		for i := 0; i < len(want); i++ {
			if want[i] < 0x21 || want[i] > 0x7e {
				printable = false
			}
		}
		if !printable {
			if err == nil || !strings.HasPrefix(err.Error(), SettingAPIKeyFile+" (ORK_TEMPORAL_API_KEY_FILE): holds a character a request header cannot carry") {
				t.Fatalf("loadAPIKey(%q) = %q, %v; want the fixed refusal", data, got.key, err)
			}
			return
		}
		if err != nil || got.key != want {
			t.Fatalf("loadAPIKey(%q) = %q, %v; want %q", data, got.key, err, want)
		}
	})
}

// FuzzSettingsFromEnv: a path or server name of white space alone is
// refused, never read as unset; every other comes back trimmed and otherwise
// as written; enabled is read only as true, false, 1 or 0, any
// other value refused naming the setting and never echoing more than the
// value itself.
func FuzzSettingsFromEnv(f *testing.F) {
	f.Add("true", " /ca.pem ", "temporal")
	f.Add("", "", "")
	f.Add("yes", "/x", "\t")
	f.Add("FALSE", "a", "b")
	f.Fuzz(func(t *testing.T, enabled, path, name string) {
		env := map[string]string{
			"ORK_TEMPORAL_TLS_ENABLED":     enabled,
			"ORK_TEMPORAL_TLS_CA_FILE":     path,
			"ORK_TEMPORAL_TLS_SERVER_NAME": name,
		}
		s, err := SettingsFromEnv(func(k string) string { return env[k] })
		for _, v := range []struct{ setting, raw string }{{SettingCAFile, path}, {SettingServerName, name}} {
			if v.raw != "" && strings.TrimSpace(v.raw) == "" {
				if err == nil || !strings.Contains(err.Error(), "white space alone") {
					t.Fatalf("%s of white space alone: err = %v, want a refusal", v.setting, err)
				}
				return
			}
		}
		switch strings.ToLower(strings.TrimSpace(enabled)) {
		case "", "true", "1", "false", "0":
			if err != nil {
				t.Fatalf("enabled %q refused: %v", enabled, err)
			}
		default:
			if err == nil || !strings.HasPrefix(err.Error(), SettingEnabled+" (ORK_TEMPORAL_TLS_ENABLED): ") {
				t.Fatalf("enabled %q: err = %v, want a refusal naming the setting", enabled, err)
			}
			return
		}
		if s.CAFile != strings.TrimSpace(path) || s.ServerName != strings.TrimSpace(name) {
			t.Fatalf("SettingsFromEnv = %+v from path %q, name %q", s, path, name)
		}
		switch strings.ToLower(strings.TrimSpace(enabled)) {
		case "":
			if s.Enabled != nil {
				t.Fatal("an unset enabled came back set")
			}
		case "true", "1":
			if s.Enabled == nil || !*s.Enabled {
				t.Fatal("enabled true came back otherwise")
			}
		default:
			if s.Enabled == nil || *s.Enabled {
				t.Fatal("enabled false came back otherwise")
			}
		}
	})
}

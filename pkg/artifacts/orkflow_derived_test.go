package artifacts

import (
	"errors"
	"strings"
	"testing"
)

const derivedTestHash = "cf134121b6e90cc5e646b415e804152ff8d32dae245f6b924d0e35d351f3dfbc"

func TestOrkflowDerivedKey_RoundTripsAndStaysOutOfTheVersionNamespace(t *testing.T) {
	key, err := OrkflowDerivedKey("payments", "nightly", derivedTestHash)
	if err != nil {
		t.Fatalf("OrkflowDerivedKey: %v", err)
	}
	if want := "metadata/orkflows/payments/nightly/@derived-" + derivedTestHash + ".json"; key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	namespace, name, hash, ok := ParseOrkflowDerivedKey(key)
	if !ok || namespace != "payments" || name != "nightly" || hash != derivedTestHash {
		t.Fatalf("ParseOrkflowDerivedKey = %q, %q, %q, %v", namespace, name, hash, ok)
	}
	// A listing must never read a claim as a version, on this release or one
	// that predates claims: both skip a basename that begins "@".
	if _, _, _, ok := parseOrkflowKey(key); ok {
		t.Fatalf("parseOrkflowKey accepted the claim %q as a version", key)
	}
}

func TestParseOrkflowDerivedKey_RefusesWhatIsNotAClaim(t *testing.T) {
	for _, key := range []string{
		"metadata/orkflows/payments/nightly/1.0.0.json",
		"metadata/orkflows/payments/nightly/@head.json",
		"metadata/orkflows/payments/nightly/@current.json",
		"metadata/orkflows/payments/nightly/@derived-.json",
		"metadata/orkflows/payments/nightly/@derived-abc",
		"metadata/orkflows/payments/nightly/@derived-a/b.json",
		"metadata/orkflows/Pay/nightly/@derived-abc.json",
		"metadata/orkflows/nightly/@derived-abc.json",
		"objects/payments/nightly/@derived-abc.json",
	} {
		if _, _, _, ok := ParseOrkflowDerivedKey(key); ok {
			t.Errorf("ParseOrkflowDerivedKey(%q) accepted it", key)
		}
	}
}

func TestOrkflowDerivedKey_RefusesAHashThatCouldEscape(t *testing.T) {
	for _, hash := range []string{"", "../x", "a/b", "@head", strings.Repeat("a", maxOrkflowComponent+1)} {
		if _, err := OrkflowDerivedKey("payments", "nightly", hash); !errors.Is(err, ErrInvalidOrkflow) {
			t.Errorf("OrkflowDerivedKey(%q) = %v, want ErrInvalidOrkflow", hash, err)
		}
	}
}

func TestUnmarshalDerivedClaim_RefusesWhatNamesNoVersion(t *testing.T) {
	for name, document := range map[string]string{
		"not json":     "nope",
		"no version":   `{}`,
		"empty":        `{"version":""}`,
		"a path":       `{"version":"../../x"}`,
		"a meta key":   `{"version":"@head"}`,
		"wrong type":   `{"version":7}`,
		"a whole list": `[]`,
	} {
		if claim, err := unmarshalDerivedClaim([]byte(document)); err == nil {
			t.Errorf("%s: unmarshalDerivedClaim(%q) = %+v, want an error", name, document, claim)
		}
	}
	claim, err := unmarshalDerivedClaim([]byte(`{"version":"01h455vb4pex5vsknk084sn02q"}`))
	if err != nil || claim.Version != "01h455vb4pex5vsknk084sn02q" {
		t.Fatalf("a good claim = %+v, %v", claim, err)
	}
}

// FuzzDerivedClaim_ADocumentThatDecodesNamesAnAddressableVersion: a claim is read
// back from object storage and its version becomes a key, so whatever bytes
// decode must name a version that key can hold, and write back to what decoded.
func FuzzDerivedClaim_ADocumentThatDecodesNamesAnAddressableVersion(f *testing.F) {
	for _, seed := range []string{
		`{"version":"01h455vb4pex5vsknk084sn02q"}`,
		`{"version":"` + derivedTestHash + `"}`,
		`{"version":"1.2.3"}`,
		`{"version":"../../x"}`,
		`{"version":""}`,
		`{"version":7}`,
		`{}`,
		`[]`,
		`nope`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, document []byte) {
		claim, err := unmarshalDerivedClaim(document)
		if err != nil {
			return
		}
		key, err := OrkflowKey("payments", "nightly", claim.Version)
		if err != nil {
			t.Fatalf("version %q decoded but is not addressable: %v", claim.Version, err)
		}
		if _, _, version, ok := parseOrkflowKey(key); !ok || version != claim.Version {
			t.Fatalf("key %q does not parse back to version %q", key, claim.Version)
		}
		written, err := marshalDerivedClaim(claim)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		again, err := unmarshalDerivedClaim(written)
		if err != nil || again != claim {
			t.Fatalf("round trip of %+v = %+v, %v", claim, again, err)
		}
	})
}

// FuzzOrkflowDerivedKey_AParsedKeyIsTheKeyTheBuilderWrites: two keys never name
// one claim, and nothing the parser accepts can leave its prefix.
func FuzzOrkflowDerivedKey_AParsedKeyIsTheKeyTheBuilderWrites(f *testing.F) {
	for _, seed := range []string{
		"metadata/orkflows/payments/nightly/@derived-" + derivedTestHash + ".json",
		"metadata/orkflows/default/a/@derived-b.json",
		"metadata/orkflows/payments/nightly/@derived-../x.json",
		"metadata/orkflows/payments/nightly/@head.json",
		"metadata/orkflows/payments/nightly/1.0.0.json",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		namespace, name, hash, ok := ParseOrkflowDerivedKey(key)
		if !ok {
			return
		}
		built, err := OrkflowDerivedKey(namespace, name, hash)
		if err != nil {
			t.Fatalf("parsed %q to (%q, %q, %q), which the builder refuses: %v", key, namespace, name, hash, err)
		}
		if built != key {
			t.Fatalf("parsed %q, which the builder renders as %q", key, built)
		}
	})
}

package authn

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func keyFile(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSigninKeys_SealAndOpen(t *testing.T) {
	a, b := keyFile(t), keyFile(t)
	old, err := LoadSigninKeys([]string{a})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := old.Seal(PurposeRecord, "metadata/identity/signins/x.json", []byte("refresh"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := old.Open(PurposeRecord, "metadata/identity/signins/x.json", sealed)
	if err != nil || string(got) != "refresh" {
		t.Fatalf("Open = %q %v", got, err)
	}
	if bytes.Contains(sealed, []byte("refresh")) {
		t.Fatal("the plaintext is visible in the sealed bytes")
	}
	// A record copied to another key, or passed off as a cookie, does not open.
	if _, err := old.Open(PurposeRecord, "metadata/identity/signins/y.json", sealed); err == nil {
		t.Error("a record opened under another storage key")
	}
	if _, err := old.Open(PurposeLogin, "metadata/identity/signins/x.json", sealed); err == nil {
		t.Error("a record opened as a login cookie")
	}
	// Rotation: the new key added last still opens what the old one sealed,
	// then first, seals with itself and still opens the old.
	both, err := LoadSigninKeys([]string{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := both.Open(PurposeRecord, "metadata/identity/signins/x.json", sealed); err != nil {
		t.Fatalf("the old key added first no longer opens: %v", err)
	}
	moved, err := LoadSigninKeys([]string{b, a})
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := moved.Seal(PurposeLogin, "", []byte("x"))
	if _, err := both.Open(PurposeLogin, "", fresh); err != nil {
		t.Fatalf("an engine with both keys cannot open the new key's: %v", err)
	}
	if _, err := old.Open(PurposeLogin, "", fresh); !errors.Is(err, ErrSigninKeyUnknown) {
		t.Fatalf("an engine without the new key = %v; want ErrSigninKeyUnknown", err)
	}
	// Tampering is refused.
	tampered := bytes.Clone(sealed)
	tampered[len(tampered)-2] ^= 'A' ^ 'B'
	if _, err := old.Open(PurposeRecord, "metadata/identity/signins/x.json", tampered); err == nil {
		t.Error("a tampered record opened")
	}
	for _, junk := range []string{"", "v1.", "v2.0123456789abcdef.AAAA", "v1.0123456789abcdef.!!"} {
		if _, err := old.Open(PurposeLogin, "", []byte(junk)); err == nil {
			t.Errorf("Open(%q) succeeded", junk)
		}
	}
}

func TestLoadSigninKeys_Refuses(t *testing.T) {
	short := filepath.Join(t.TempDir(), "short")
	if err := os.WriteFile(short, []byte("tooshort"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := keyFile(t)
	for name, files := range map[string][]string{
		"none":         nil,
		"missing":      {filepath.Join(t.TempDir(), "nope")},
		"not 32 bytes": {short},
		"twice":        {a, a},
	} {
		if _, err := LoadSigninKeys(files); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func FuzzSigninKeysOpen(f *testing.F) {
	raw := make([]byte, 32)
	k, err := newSigninKeys([][]byte{raw})
	if err != nil {
		f.Fatal(err)
	}
	sealed, _ := k.Seal(PurposeLogin, "", []byte("seed"))
	f.Add(sealed)
	f.Add([]byte("v1.0123456789abcdef.AAAA"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, in []byte) {
		// Property: Open never panics, and opens only what this key sealed.
		out, err := k.Open(PurposeLogin, "", in)
		if err == nil && !bytes.Equal(in, sealed) {
			if _, err2 := k.Open(PurposeRecord, "", in); err2 == nil {
				t.Fatalf("one sealed value opened for two purposes: %q", out)
			}
		}
	})
}

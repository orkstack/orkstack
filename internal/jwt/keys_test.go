package jwt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
)

// RFC 7638 §3.1's example key and its thumbprint.
const (
	rfc7638Key        = `{"keys":[{"kty":"RSA","kid":"2011-04-29","alg":"RS256","n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw","e":"AQAB"}]}`
	rfc7638Thumbprint = "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"
)

func TestJWT_ThumbprintMatchesRFC7638(t *testing.T) {
	set, err := ParseKeySet([]byte(rfc7638Key))
	if err != nil {
		t.Fatal(err)
	}
	key, ok := set.Lookup("2011-04-29")
	if !ok {
		t.Fatal("key not kept")
	}
	tp, err := key.Thumbprint()
	if err != nil || tp != rfc7638Thumbprint {
		t.Fatalf("thumbprint %q, %v; want %q", tp, err, rfc7638Thumbprint)
	}
}

func jwkEntry(t testing.TB, kid, extra string) string {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := NewPublicKey(&k.PublicKey)
	raw, err := pk.MarshalJWK(kid)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if extra != "" {
		s = strings.TrimSuffix(s, "}") + "," + extra + "}"
	}
	return s
}

func TestParseKeySet(t *testing.T) {
	_, e := testKeys(t)
	ep, _ := NewPublicKey(&e.PublicKey)
	good, _ := ep.MarshalJWK("good")

	t.Run("keeps a usable key", func(t *testing.T) {
		set, err := ParseKeySet([]byte(`{"keys":[` + string(good) + `]}`))
		if err != nil || set.Len() != 1 {
			t.Fatalf("len %d, %v", set.Len(), err)
		}
	})
	t.Run("refuses", func(t *testing.T) {
		many := make([]string, MaxKeySetKeys+1)
		for i := range many {
			many[i] = `{"kty":"oct"}`
		}
		for name, doc := range map[string]string{
			"too many keys":   `{"keys":[` + strings.Join(many, ",") + `]}`,
			"too many bytes":  `{"keys":[],"pad":"` + strings.Repeat("x", MaxKeySetBytes) + `"}`,
			"not an object":   `[]`,
			"no keys":         `{}`,
			"keys not array":  `{"keys":{}}`,
			"private EC key":  `{"keys":[{"kty":"EC","kid":"p","crv":"P-256","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0","d":"jpsQnnGQmL-YBIffH1136cspYG6-0iY7X1fCE9-E9LI"}]}`,
			"symmetric key":   `{"keys":[{"kty":"oct","kid":"h","k":"AyM1SysPpbyDfgZld3umj1qzKObwVMkoqQ-EstJQLr8"}]}`,
			"duplicate names": `{"keys":[],"keys":[]}`,
		} {
			if _, err := ParseKeySet([]byte(doc)); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
	})
	t.Run("skips keys it would not use", func(t *testing.T) {
		doc := `{"keys":[` + strings.Join([]string{
			jwkEntry(t, "", ""), // no kid
			strings.Replace(jwkEntry(t, "enc", ""), `"use":"sig"`, `"use":"enc"`, 1),
			strings.Replace(jwkEntry(t, "alg", ""), `"alg":"ES256"`, `"alg":"ES384"`, 1),
			`{"kty":"OKP","kid":"ed","crv":"Ed25519","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}`,
			smallRSA(t),

			string(good),
		}, ",") + `]}`
		set, err := ParseKeySet([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		if set.Len() != 1 {
			t.Fatalf("kept %d keys; want only the usable one", set.Len())
		}
	})
	t.Run("a kid published twice is never used", func(t *testing.T) {
		doc := `{"keys":[` + jwkEntry(t, "same", "") + "," + jwkEntry(t, "same", "") + "," + jwkEntry(t, "same", "") + `]}`
		set, err := ParseKeySet([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := set.Lookup("same"); ok {
			t.Fatal("an ambiguous kid was kept")
		}
	})
}

func TestParsePublicKeyPEM(t *testing.T) {
	_, e := testKeys(t)
	der, err := x509.MarshalPKIXPublicKey(&e.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	block := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	if k, err := ParsePublicKeyPEM(block); err != nil || k.Algorithm() != ES256 {
		t.Fatalf("got %v, %v", k.Algorithm(), err)
	}
	for name, data := range map[string][]byte{
		"empty":       nil,
		"wrong type":  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}),
		"two blocks":  append(append([]byte{}, block...), block...),
		"garbage DER": pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte{1, 2}}),
	} {
		if _, err := ParsePublicKeyPEM(data); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// smallRSA is a published 1024-bit RSA key: below MinRSABits.
func smallRSA(t testing.TB) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := (&jose.JSONWebKey{Key: &k.PublicKey, KeyID: "small", Use: "sig"}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestJWT_OnlyThisPackageImportsTheLibrary holds the rule that every JOSE
// decision is made here: no other package in the module imports go-jose.
func TestJWT_OnlyThisPackageImportsTheLibrary(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	self := filepath.Join(root, "internal", "jwt")
	fset := token.NewFileSet()
	var offenders []string
	checked := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "proof", ".data", "testdata":
				return filepath.SkipDir
			}
			if path == self || strings.HasPrefix(d.Name(), ".") && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		checked++
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(p, "github.com/go-jose/") || strings.HasPrefix(p, "gopkg.in/square/go-jose") || strings.HasPrefix(p, "gopkg.in/go-jose/") {
				rel, _ := filepath.Rel(root, path)
				offenders = append(offenders, rel+" imports "+p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 100 {
		t.Fatalf("only %d Go files were checked; the walk did not see the module", checked)
	}
	if len(offenders) > 0 {
		t.Fatalf("only internal/jwt may import the JOSE library:\n%s", strings.Join(offenders, "\n"))
	}
}

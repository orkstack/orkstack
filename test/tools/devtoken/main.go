// Command devtoken is the development stack's token issuer, for
// `make up IDENTITY=1` (CONTRIBUTING.md, "The development stack"): a key pair
// made once under a directory, its public half published as a JWK set the
// engine trusts as the issuer `dev` (auth.issuers with jwks_file), and tokens
// signed with it.
//
//	devtoken init -dir .data/identity
//	devtoken sign -dir .data/identity -iss orkstack-dev -aud orkstack \
//		-sub dev-admin -ttl 720h -out .data/identity/admin.token
//	devtoken sign ... -sub orker-1 -claim orker_id=orker-1 -out .data/identity/orker.token
//
// It is a development tool: its key lives on the developer's disk and
// signs whatever it is asked to.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

const (
	keyFile  = "dev-issuer.key"
	jwksFile = "dev-issuer.jwks.json"
	// engineKeyFile is the engine's own signing key
	// (identity.signing.key_files), which enforce requires.
	engineKeyFile = "engine-signing.key"
)

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: devtoken init|sign [flags]"))
	}
	switch os.Args[1] {
	case "init":
		fs := flag.NewFlagSet("init", flag.ExitOnError)
		dir := fs.String("dir", "", "the directory that holds the key pair")
		_ = fs.Parse(os.Args[2:])
		fail(initKeys(*dir))
	case "sign":
		fs := flag.NewFlagSet("sign", flag.ExitOnError)
		var o signOptions
		dir := fs.String("dir", "", "the directory that holds the key pair")
		fs.StringVar(&o.issuer, "iss", "", "the token's iss")
		fs.StringVar(&o.audience, "aud", "", "the token's aud")
		fs.StringVar(&o.subject, "sub", "", "the token's sub")
		fs.DurationVar(&o.ttl, "ttl", 30*24*time.Hour, "how long the token lives")
		fs.StringVar(&o.out, "out", "", "the file the token is written to, mode 0600")
		var extra multi
		fs.Var(&extra, "claim", "a further claim as name=value (repeatable)")
		_ = fs.Parse(os.Args[2:])
		o.claims = map[string]string{}
		for _, kv := range extra {
			name, value, ok := strings.Cut(kv, "=")
			if !ok || name == "" {
				fail(fmt.Errorf("-claim %q is not name=value", kv))
			}
			o.claims[name] = value
		}
		fail(signToken(*dir, o))
	default:
		fail(fmt.Errorf("unknown command %q: init or sign", os.Args[1]))
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "devtoken:", err)
		os.Exit(1)
	}
}

// initKeys makes the issuer's key pair and the engine's signing key under
// dir, each unless it is there already: a key replaced would invalidate
// every token, or decision, signed before it.
func initKeys(dir string) error {
	if dir == "" {
		return errors.New("-dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, name := range []string{keyFile, engineKeyFile} {
		if err := makeKey(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return writeJWKS(dir)
}

// makeKey writes a P-256 private key to keyPath, 0600, unless a file is
// there already.
func makeKey(keyPath string) error {
	if _, err := os.Stat(keyPath); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func loadSigner(dir string) (*jwt.Signer, error) {
	raw, err := os.ReadFile(filepath.Join(dir, keyFile))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s holds no PEM key", keyFile)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not a P-256 key", keyFile)
	}
	return jwt.NewSigner(ec)
}

// writeJWKS publishes the key pair's public half as a one-key JWK set.
func writeJWKS(dir string) error {
	signer, err := loadSigner(dir)
	if err != nil {
		return err
	}
	jwk, err := signer.Public().MarshalJWK(signer.KeyID())
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, jwksFile), []byte(`{"keys":[`+string(jwk)+`]}`+"\n"), 0o644)
}

type signOptions struct {
	issuer, audience, subject, out string
	ttl                            time.Duration
	claims                         map[string]string
}

// signToken writes one token signed with the key pair under dir.
func signToken(dir string, o signOptions) error {
	if o.issuer == "" || o.audience == "" || o.subject == "" || o.out == "" || o.ttl <= 0 {
		return errors.New("sign needs -iss, -aud, -sub, -out and a positive -ttl")
	}
	signer, err := loadSigner(dir)
	if err != nil {
		return err
	}
	now := time.Now().Truncate(time.Second)
	claims := map[string]any{"iss": o.issuer, "aud": o.audience, "sub": o.subject,
		"iat": now.Unix(), "exp": now.Add(o.ttl).Unix()}
	for name, value := range o.claims {
		if _, reserved := claims[name]; reserved {
			return fmt.Errorf("-claim %s would replace a registered claim", name)
		}
		claims[name] = value
	}
	token, err := signer.Sign("JWT", claims)
	if err != nil {
		return err
	}
	return os.WriteFile(o.out, []byte(token+"\n"), 0o600)
}

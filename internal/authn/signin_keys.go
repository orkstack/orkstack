package authn

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// SigninKeySize is the size of one sign-in key: 32 random bytes, AES-256.
const SigninKeySize = 32

// The purposes a sign-in key seals for. Each is associated data, so a value
// sealed for one never opens as another (authn-authz.md §6.4).
const (
	// PurposeLogin is the login cookie: state, nonce, verifier, return path.
	PurposeLogin = "ork-login"
	// PurposeRecord is a sign-in record, with its storage key.
	PurposeRecord = "ork-signin-record"
	// PurposeCookie is the sign-in cookie: its 32 random bytes, sealed, so
	// a cookie no engine made is refused before the store is asked.
	PurposeCookie = "ork-signin-cookie"
)

// sealedPrefix begins every sealed value: its format version.
const sealedPrefix = "v1."

// ErrSigninKeyUnknown is a sealed value naming a key this engine lacks: a
// sign-in it cannot open, answered as a stale sign-in.
var ErrSigninKeyUnknown = errors.New("sealed under a sign-in key this engine does not hold")

// ErrSealed is a sealed value that does not open: malformed, tampered with,
// or sealed for another purpose or key.
var ErrSealed = errors.New("not a value this engine sealed")

type signinKey struct {
	kid  string
	aead cipher.AEAD
}

// SigninKeys is auth.signin.key_files: the first key seals; every key
// opens. It covers the login cookie and the sign-in records, and nothing
// else.
type SigninKeys struct {
	keys []signinKey
}

// LoadSigninKeys reads auth.signin.key_files, each exactly SigninKeySize
// bytes, in order.
func LoadSigninKeys(files []string) (*SigninKeys, error) {
	if len(files) == 0 {
		return nil, errors.New("auth.signin.key_files names no key")
	}
	raws := make([][]byte, 0, len(files))
	for _, f := range files {
		raw, err := readBounded(f, SigninKeySize)
		if err != nil {
			return nil, fmt.Errorf("auth.signin.key_files: %w", err)
		}
		if len(raw) != SigninKeySize {
			return nil, fmt.Errorf("auth.signin.key_files: %s holds %d bytes; a key is %d random bytes", f, len(raw), SigninKeySize)
		}
		raws = append(raws, raw)
	}
	return newSigninKeys(raws)
}

func newSigninKeys(raws [][]byte) (*SigninKeys, error) {
	out := &SigninKeys{}
	seen := map[string]bool{}
	for _, raw := range raws {
		block, err := aes.NewCipher(raw)
		if err != nil {
			return nil, fmt.Errorf("sign-in key: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("sign-in key: %w", err)
		}
		sum := sha256.Sum256(raw)
		kid := hex.EncodeToString(sum[:])[:16]
		if seen[kid] {
			return nil, fmt.Errorf("auth.signin.key_files lists key %s twice", kid)
		}
		seen[kid] = true
		out.keys = append(out.keys, signinKey{kid: kid, aead: aead})
	}
	return out, nil
}

// KeyIDs lists the keys' ids, the sealing key first.
func (k *SigninKeys) KeyIDs() []string {
	out := make([]string, len(k.keys))
	for i, key := range k.keys {
		out[i] = key.kid
	}
	return out
}

func associated(purpose, binding string) []byte {
	return []byte(purpose + "\x00" + binding)
}

// Seal encrypts plaintext for purpose under the first key, bound to
// binding (a record's storage key; empty for a cookie). The result is
// v1.<kid>.<base64url(nonce || ciphertext)>.
func (k *SigninKeys) Seal(purpose, binding string, plaintext []byte) ([]byte, error) {
	if k == nil || len(k.keys) == 0 {
		return nil, errors.New("no sign-in key")
	}
	key := k.keys[0]
	nonce := make([]byte, key.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	ct := key.aead.Seal(nonce, nonce, plaintext, associated(purpose, binding))
	var b bytes.Buffer
	b.WriteString(sealedPrefix)
	b.WriteString(key.kid)
	b.WriteByte('.')
	b.WriteString(base64.RawURLEncoding.EncodeToString(ct))
	return b.Bytes(), nil
}

// Open decrypts what Seal wrote for purpose and binding. A value naming a
// key this engine lacks is ErrSigninKeyUnknown; any other failure is
// ErrSealed.
func (k *SigninKeys) Open(purpose, binding string, sealed []byte) ([]byte, error) {
	if k == nil {
		return nil, ErrSealed
	}
	rest, ok := strings.CutPrefix(string(sealed), sealedPrefix)
	if !ok {
		return nil, ErrSealed
	}
	kid, body, ok := strings.Cut(rest, ".")
	if !ok || len(kid) != 16 {
		return nil, ErrSealed
	}
	var key *signinKey
	for i := range k.keys {
		if k.keys[i].kid == kid {
			key = &k.keys[i]
			break
		}
	}
	if key == nil {
		return nil, ErrSigninKeyUnknown
	}
	ct, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil || len(ct) < key.aead.NonceSize()+key.aead.Overhead() {
		return nil, ErrSealed
	}
	n := key.aead.NonceSize()
	out, err := key.aead.Open(nil, ct[:n], ct[n:], associated(purpose, binding))
	if err != nil {
		return nil, ErrSealed
	}
	return out, nil
}

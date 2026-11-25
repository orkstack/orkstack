package vault

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// What Vault answered, read strictly: one JSON value, no key twice, valid
// UTF-8 (encoding/json/v2's defaults). Vault's own words are never repeated:
// an error is a class and a code of this package's.

// The operations and codes of this package's errors.
const (
	opLogin = "login"
	opRead  = "read"
	opIssue = "issue"
	opRenew = "renew"
	opClose = "close"
)

func failed(class provider.Class, op, code string, err error) error {
	return &provider.Error{Class: class, Op: op, Code: code, Err: err}
}

// answer is the members of Vault's answer this package reads.
type answer struct {
	LeaseID       string         `json:"lease_id"`
	LeaseDuration int64          `json:"lease_duration"`
	Renewable     bool           `json:"renewable"`
	Data          jsontext.Value `json:"data"`
	Auth          *auth          `json:"auth"`
}

type auth struct {
	ClientToken   string `json:"client_token"`
	Accessor      string `json:"accessor"`
	LeaseDuration int64  `json:"lease_duration"`
	Renewable     bool   `json:"renewable"`
	TokenType     string `json:"token_type"`
	NumUses       int64  `json:"num_uses"`
}

func readAnswer(op string, body []byte) (answer, error) {
	var a answer
	if err := json.Unmarshal(body, &a); err != nil {
		// The decoder's error can quote what it read, so it is not kept.
		return answer{}, failed(provider.Unavailable, op, "malformed-answer", nil)
	}
	return a, nil
}

// object is v as a JSON object; ok is false when it is not one.
func object(v jsontext.Value) (map[string]jsontext.Value, bool) {
	var m map[string]jsontext.Value
	if v.Kind() != '{' || json.Unmarshal(v, &m) != nil {
		return nil, false
	}
	return m, true
}

// secretValue is the value d names in a read's answer (§8.1).
func secretValue(body []byte, d detail) (provider.Value, error) {
	a, err := readAnswer(opRead, body)
	if err != nil {
		return provider.Value{}, err
	}
	data := a.Data
	if d.kv2 {
		outer, ok := object(data)
		if !ok {
			return provider.Value{}, failed(provider.Unavailable, opRead, "malformed-answer", nil)
		}
		inner, ok := outer["data"]
		if !ok {
			// The mount is not KV version 2, as it is declared: the Orker's
			// configuration, never a missing secret.
			return provider.Value{}, failed(provider.Unavailable, opRead, "not-kv2", nil)
		}
		if inner.Kind() == 'n' {
			// Vault answers a deleted version 404; this is the same thing said
			// in a body.
			return provider.Value{}, failed(provider.NotFound, opRead, "deleted", nil)
		}
		data = inner
	}
	fields, ok := object(data)
	if !ok {
		return provider.Value{}, failed(provider.Unavailable, opRead, "malformed-answer", nil)
	}
	if d.field == "" {
		return wholeValue(data)
	}
	v, ok := fields[d.field]
	if !ok || v.Kind() == 'n' {
		return provider.Value{}, failed(provider.NotFound, opRead, "no-field", nil)
	}
	if b, ok := scalar(v); ok {
		return provider.Value{Bytes: b}, nil
	}
	return wholeValue(v)
}

// wholeValue is JSON handed over as it is, masked whole and by its parts.
func wholeValue(v jsontext.Value) (provider.Value, error) {
	also, err := scalars(v)
	if err != nil {
		return provider.Value{}, err
	}
	return provider.Value{Bytes: slices.Clone(v), Also: also}, nil
}

// scalar is a string's text, or a number's or boolean's own spelling.
func scalar(v jsontext.Value) ([]byte, bool) {
	switch v.Kind() {
	case '"':
		var s string
		if json.Unmarshal(v, &s) != nil {
			return nil, false
		}
		return []byte(s), true
	case '0', 't', 'f':
		return slices.Clone(v), true
	}
	return nil, false
}

// maxScalars bounds the parts of one value that are masked: a value of more
// is not one a person wrote, and is refused rather than masked in part.
const maxScalars = 4096

// scalars are every scalar inside v, at any depth: a value handed over as
// JSON is also masked by its parts. It reads v once, however deep it is.
func scalars(v jsontext.Value) ([][]byte, error) {
	var out [][]byte
	dec := jsontext.NewDecoder(bytes.NewReader(v))
	for {
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, failed(provider.Unavailable, opRead, "malformed-answer", nil)
		}
		kind, n := dec.StackIndex(dec.StackDepth())
		if kind == '{' && n%2 == 1 {
			continue // a member's name
		}
		switch tok.Kind() {
		case '"', '0', 't', 'f':
			out = append(out, []byte(tok.String()))
		}
		if len(out) > maxScalars {
			return nil, failed(provider.Invalid, opRead, "too-many-values", nil)
		}
	}
}

// secretFields are the fields of Vault's engines that are certainly secret,
// whichever engine it is: a task may not return one in its results. A user
// name, an access key's id or a certificate is not among them.
var secretFields = []string{"password", "token", "secret_key", "security_token", "private_key", "secret_id", "client_secret"}

// wholeFile is the file that holds the whole of a credential's data.
const wholeFile = "secret.json"

// lease is a bundle's handle: what Renew extends.
type lease struct {
	id        string
	renewable bool
	first     time.Duration
	// got is when the lease was last asked for or extended, and life how long
	// from then.
	got  time.Time
	life time.Duration
}

// leaseBundle is a credential's bundle from a leased answer (§8.2): the
// whole of its data, and one file per scalar field whose name can be a
// file's. Any other field is in the whole. An answer with no lease, or more
// fields than a bundle holds, is refused: it is not a credential, or not one
// that fits. sent is when it was asked for.
func leaseBundle(body []byte, sent time.Time) (provider.Bundle, error) {
	a, err := readAnswer(opIssue, body)
	if err != nil {
		return provider.Bundle{}, err
	}
	fields, ok := object(a.Data)
	if !ok {
		return provider.Bundle{}, failed(provider.Unavailable, opIssue, "malformed-answer", nil)
	}
	life, ok := bundleLife(a.LeaseDuration)
	if a.LeaseID == "" || !ok {
		return provider.Bundle{}, failed(provider.Invalid, opIssue, "no-lease", nil)
	}
	files := map[string][]byte{wholeFile: slices.Clone(a.Data)}
	for name, v := range fields {
		b, ok := scalar(v)
		if !ok || name == wholeFile || provider.ValidFileName(name) != nil {
			continue
		}
		files[name] = b
	}
	if len(files) > provider.MaxBundleFiles {
		return provider.Bundle{}, failed(provider.Invalid, opIssue, "too-many-fields", nil)
	}
	var secrets [][]byte
	for _, name := range secretFields {
		if b := files[name]; len(b) > 0 {
			secrets = append(secrets, b)
		}
	}
	return provider.Bundle{Files: files, Secrets: secrets, Lifetime: life,
		Handle: lease{id: a.LeaseID, renewable: a.Renewable, first: life, got: sent, life: life}}, nil
}

// seconds is a life Vault states, ok when it is one that ends. It is bounded
// only so that it is a duration; a bundle's is bounded again (bundleLife).
func seconds(n int64) (time.Duration, bool) {
	if n < 1 {
		return 0, false
	}
	return time.Duration(min(n, 1<<31)) * time.Second, true
}

// bundleLife is seconds, at most what the framework lets a bundle live: a
// longer one is refreshed as if it ended there.
func bundleLife(n int64) (time.Duration, bool) {
	d, ok := seconds(n)
	return min(d, provider.MaxLifetime), ok
}

package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RecordedInTime says a record the store wrote at storedAt (its object's
// own Last-Modified, never an instant the record states) settles what was
// due by deadline: written at or before it, counted in whole seconds, and,
// when the run has ended (runEnd set), at or before its end. It is the one
// decision that adopts a record: a record not in time is adopted by
// nothing. A zero storedAt or deadline is never in time.
func RecordedInTime(storedAt, deadline, runEnd time.Time) bool {
	return artifacts.RecordedInTime(storedAt, deadline, runEnd)
}

// ErrInvalidResult reports a result whose digest cannot be taken.
var ErrInvalidResult = errors.New("invalid result")

// ResultArtifact is one artifact of a result, by name and the SHA-256 of
// its bytes. System artifacts (a task's logs) are not a result's.
type ResultArtifact struct {
	Name   string
	SHA256 string
}

// resultForm is the canonical form ResultSHA256 hashes; its field order is
// its bytes' order.
type resultForm struct {
	V         int         `json:"v"`
	Values    string      `json:"values"`
	Artifacts [][2]string `json:"artifacts"`
}

// ResultSHA256 is the digest a commit and a record of the same result
// share: the SHA-256, in lowercase hex, of
// {"v":1,"values":<values' SHA-256>,"artifacts":[[name,SHA-256],…]} with
// the artifacts sorted by name. Nothing of when, where or by which attempt
// the result was made is in it. It is the only digest two records of one
// result are compared by; a record's own digest never is.
func ResultSHA256(valuesSHA256 string, artifacts []ResultArtifact) (string, error) {
	if !lowerHex64(valuesSHA256) {
		return "", fmt.Errorf("%w: the values' digest %q is not a SHA-256 in lowercase hex", ErrInvalidResult, valuesSHA256)
	}
	form := resultForm{V: 1, Values: valuesSHA256, Artifacts: make([][2]string, 0, len(artifacts))}
	seen := make(map[string]bool, len(artifacts))
	for _, a := range artifacts {
		if a.Name == "" || seen[a.Name] {
			return "", fmt.Errorf("%w: artifact name %q is empty or given twice", ErrInvalidResult, a.Name)
		}
		if !lowerHex64(a.SHA256) {
			return "", fmt.Errorf("%w: artifact %q's digest is not a SHA-256 in lowercase hex", ErrInvalidResult, a.Name)
		}
		seen[a.Name] = true
		form.Artifacts = append(form.Artifacts, [2]string{a.Name, a.SHA256})
	}
	sort.Slice(form.Artifacts, func(i, j int) bool { return form.Artifacts[i][0] < form.Artifacts[j][0] })
	raw, err := stablejson.Marshal(form)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// minNonceBytes is the shortest admission nonce a binding is keyed by.
const minNonceBytes = 16

// InputBinding binds a record to the start input its run was admitted
// with: an HMAC-SHA256 of the input's bytes, as the start carried them,
// keyed by the run's admission nonce. Only InputBindingMatches compares it.
// A nonce of fewer than 16 bytes binds nothing: nil.
func InputBinding(nonce, input []byte) []byte {
	if len(nonce) < minNonceBytes {
		return nil
	}
	mac := hmac.New(sha256.New, nonce)
	mac.Write(input)
	return mac.Sum(nil)
}

// InputBindingMatches says binding is the one InputBinding gives for input
// under nonce, compared in constant time. An empty nonce or binding
// matches nothing: a record without a binding settles nothing.
func InputBindingMatches(nonce, input, binding []byte) bool {
	if len(nonce) < minNonceBytes || len(binding) != sha256.Size {
		return false
	}
	return hmac.Equal(InputBinding(nonce, input), binding)
}

func lowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

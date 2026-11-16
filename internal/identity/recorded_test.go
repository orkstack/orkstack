package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

var (
	hexA = strings.Repeat("a", 64)
	hexB = strings.Repeat("b", 64)
	hexC = strings.Repeat("c", 64)
)

// TestRecordedInTime: a record is in time when the store wrote it at or
// before the deadline, counted in whole seconds, and at or before the run's
// end when the run has ended; a record with no instant, or held to no
// deadline, never is.
func TestRecordedInTime(t *testing.T) {
	deadline := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	end := deadline.Add(-time.Minute)
	tests := []struct {
		name            string
		stored, d, rEnd time.Time
		want            bool
	}{
		{"before the deadline", deadline.Add(-time.Hour), deadline, time.Time{}, true},
		{"at the deadline", deadline, deadline, time.Time{}, true},
		{"within the deadline's second", deadline.Add(900 * time.Millisecond), deadline, time.Time{}, true},
		{"the second after", deadline.Add(time.Second), deadline, time.Time{}, false},
		{"after the run ended, before the deadline", end.Add(time.Second), deadline, end, false},
		{"at the run's end", end, deadline, end, true},
		{"no instant", time.Time{}, deadline, time.Time{}, false},
		{"no deadline", deadline, time.Time{}, time.Time{}, false},
	}
	for _, tt := range tests {
		if got := RecordedInTime(tt.stored, tt.d, tt.rEnd); got != tt.want {
			t.Errorf("%s: %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestResultSHA256: the digest of a result is the SHA-256 of one canonical
// form, the values' digest and the artifacts' names and digests sorted by
// name, so nothing of when, where or by which attempt it was made changes
// it, and any name or digest does.
func TestResultSHA256(t *testing.T) {
	sum := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
	two := []ResultArtifact{{Name: "b.tar", SHA256: hexC}, {Name: "a.txt", SHA256: hexB}}
	tests := []struct {
		name      string
		values    string
		artifacts []ResultArtifact
		want      string
	}{
		{"values only", hexA, nil, `{"v":1,"values":"` + hexA + `","artifacts":[]}`},
		{"artifacts sorted by name", hexA, two,
			`{"v":1,"values":"` + hexA + `","artifacts":[["a.txt","` + hexB + `"],["b.tar","` + hexC + `"]]}`},
	}
	for _, tt := range tests {
		got, err := ResultSHA256(tt.values, tt.artifacts)
		if err != nil || got != sum(tt.want) {
			t.Errorf("%s = %s, %v; want the digest of %s", tt.name, got, err, tt.want)
		}
	}
	reordered, _ := ResultSHA256(hexA, []ResultArtifact{two[1], two[0]})
	base, _ := ResultSHA256(hexA, two)
	renamed, _ := ResultSHA256(hexA, []ResultArtifact{{Name: "c.tar", SHA256: hexC}, two[1]})
	changed, _ := ResultSHA256(hexA, []ResultArtifact{{Name: "b.tar", SHA256: hexA}, two[1]})
	if reordered != base || renamed == base || changed == base {
		t.Fatalf("order %s, renamed %s, changed %s against %s", reordered, renamed, changed, base)
	}
	// The golden vector: a release that writes other bytes moves every
	// digest two designs compare.
	if golden, _ := ResultSHA256(hexA, nil); golden != "0fb6891ee60622e4ac76780118cdbaca6115bef085ef540c46bbb30c230fe189" {
		t.Fatalf("golden = %s", golden)
	}
	for name, call := range map[string]func() (string, error){
		"values not a digest":  func() (string, error) { return ResultSHA256("abc", nil) },
		"values in upper case": func() (string, error) { return ResultSHA256(strings.ToUpper(hexA), nil) },
		"an artifact's digest": func() (string, error) { return ResultSHA256(hexA, []ResultArtifact{{Name: "a", SHA256: "x"}}) },
		"an artifact unnamed":  func() (string, error) { return ResultSHA256(hexA, []ResultArtifact{{SHA256: hexB}}) },
		"an artifact twice": func() (string, error) {
			return ResultSHA256(hexA, []ResultArtifact{{Name: "a", SHA256: hexB}, {Name: "a", SHA256: hexC}})
		},
	} {
		if _, err := call(); !errors.Is(err, ErrInvalidResult) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestInputBinding: the binding is an HMAC of the start input under the
// run's admission nonce, and it matches only that input under that nonce;
// an empty binding or nonce matches nothing.
func TestInputBinding(t *testing.T) {
	nonce, input := []byte("0123456789abcdef"), []byte(`{"n":1}`)
	if InputBinding([]byte("short"), input) != nil {
		t.Fatal("a binding under a nonce of fewer than 16 bytes")
	}
	if InputBinding(nil, input) != nil {
		t.Fatal("a binding under no nonce")
	}
	b := InputBinding(nonce, input)
	if len(b) != 32 || !InputBindingMatches(nonce, input, b) {
		t.Fatalf("binding %x does not match its own input", b)
	}
	for name, ok := range map[string]bool{
		"another input":  InputBindingMatches(nonce, []byte(`{"n":2}`), b),
		"another nonce":  InputBindingMatches([]byte("fedcba9876543210"), input, b),
		"no binding":     InputBindingMatches(nonce, input, nil),
		"no nonce":       InputBindingMatches(nil, input, b),
		"a short prefix": InputBindingMatches(nonce, input, b[:16]),
	} {
		if ok {
			t.Errorf("%s matched", name)
		}
	}
}

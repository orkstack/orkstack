package triggers_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/triggers"
)

// slugRule is AGENTS.md §7's name rule, ^[a-z][a-z0-9-]*$ in at most 200
// bytes, written as a byte loop so it is independent of the regular
// expression the package compiles.
func slugRule(s string) bool {
	if s == "" || len(s) > 200 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// versionRule is the stored-version rule: ^[A-Za-z0-9][A-Za-z0-9._-]*$ in at
// most 200 bytes.
func versionRule(s string) bool {
	if s == "" || len(s) > 200 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && (i == 0 || c != '.' && c != '_' && c != '-') {
			return false
		}
	}
	return true
}

// FuzzRegistration_ValidateIsExactlyTheNameRules: a registration validates
// exactly when its coordinates satisfy the rules the package states (a slug
// for the namespace, the Orkflow and the optional name, a version for the
// optional version), and a registration that validates is prepared into a
// record that carries its coordinates, a well-formed id and a token that
// verifies. Names are validated, never sanitized.
func FuzzRegistration_ValidateIsExactlyTheNameRules(f *testing.F) {
	f.Add("payments", "hello-ork", "0.2.0", "nightly", "ci")
	f.Add("default", "a", "2026-09-08.1", "", "")
	f.Add("Payments", "hello-ork", "", "", "")
	f.Add("payments", "hello-ork\n", "", "", "")
	f.Add("payments", "hello-ork", "-1", "", "")
	f.Add("payments", "hello-ork", "1", "", "A")
	f.Add("p", strings.Repeat("a", 200), strings.Repeat("1", 200), "", strings.Repeat("b", 200))
	f.Add("p", strings.Repeat("a", 201), "", "", "")
	f.Add("p", "a\xff", "\u00e9", "", "\u0131")
	f.Fuzz(func(t *testing.T, namespace, orkflow, version, description, name string) {
		reg := triggers.Registration{
			Namespace: namespace, OrkflowName: orkflow, OrkflowVersion: version, Description: description, Name: name,
		}
		want := slugRule(namespace) && slugRule(orkflow) && (name == "" || slugRule(name)) && (version == "" || versionRule(version))
		err := reg.Validate()
		if (err == nil) != want {
			t.Fatalf("Validate(%+v) = %v; the name rules say accepted=%v", reg, err, want)
		}
		if nameErr := triggers.ValidateName(name); (nameErr == nil) != (name == "" || slugRule(name)) {
			t.Fatalf("ValidateName(%q) = %v", name, nameErr)
		}
		trigger, secret, prepareErr := triggers.Prepare(reg)
		if !want {
			if prepareErr == nil {
				t.Fatalf("Prepare accepted %+v, which does not validate", reg)
			}
			return
		}
		if prepareErr != nil {
			t.Fatalf("Prepare(%+v): %v", reg, prepareErr)
		}
		if trigger.Namespace != namespace || trigger.OrkflowName != orkflow || trigger.OrkflowVersion != version ||
			trigger.Description != description || trigger.Name != name {
			t.Fatalf("Prepare(%+v) stored %+v: a coordinate was changed", reg, trigger)
		}
		if !triggers.ValidID(trigger.ID) {
			t.Fatalf("Prepare minted id %q, which ValidID refuses", trigger.ID)
		}
		if raw, err := hex.DecodeString(secret); err != nil || len(raw) != 32 {
			t.Fatalf("secret %q is not 32 bytes of hex", secret)
		}
		if !triggers.VerifySecret(trigger, secret) || triggers.VerifySecret(trigger.Public(), secret) {
			t.Fatalf("the issued secret must verify against the record, and never against its public form")
		}
		if bytes.Contains(trigger.SecretHash, []byte(secret)) || bytes.Contains(trigger.SecretSalt, []byte(secret)) {
			t.Fatalf("the record holds the plaintext secret")
		}
	})
}

// FuzzVerifySecret: a presented token verifies exactly when it is the secret
// the record's digest was made from, and never when it is empty or the record
// lacks its salt or digest. The comparison is on the digest alone.
func FuzzVerifySecret(f *testing.F) {
	f.Add("s3cr3t", "s3cr3t", []byte("0123456789abcdef"), uint8(0))
	f.Add("s3cr3t", "S3CR3T", []byte("0123456789abcdef"), uint8(0))
	f.Add("s3cr3t", "", []byte("0123456789abcdef"), uint8(0))
	f.Add("s3cr3t", "s3cr3t", []byte(nil), uint8(0))
	f.Add("c", "c", []byte("ab"), uint8(3))
	f.Add("bc", "c", []byte("a"), uint8(0))
	f.Fuzz(func(t *testing.T, secret, presented string, salt []byte, flip uint8) {
		record := triggers.Trigger{SecretSalt: salt, SecretHash: triggers.HashSecret(salt, secret)}
		want := presented == secret && presented != "" && len(salt) > 0
		if got := triggers.VerifySecret(record, presented); got != want {
			t.Fatalf("VerifySecret(salt %x, secret %q, presented %q) = %v, want %v", salt, secret, presented, got, want)
		}
		if len(record.SecretHash) == 0 {
			return
		}
		tampered := record
		tampered.SecretHash = append([]byte(nil), record.SecretHash...)
		tampered.SecretHash[int(flip)%len(tampered.SecretHash)] ^= 1 << (flip % 8)
		if triggers.VerifySecret(tampered, secret) {
			t.Fatalf("a digest with one flipped bit still verified")
		}
	})
}

// FuzzApplyChange: whatever an update's change function does to the copy it
// is handed, the result either is refused, or keeps the id, namespace,
// Orkflow, creation time and token exactly and passes the registration rules
// the create path applies; and the record it started from is never altered.
func FuzzApplyChange(f *testing.F) {
	f.Add("renamed", "1.0", "d", uint8(0), "other")
	f.Add("", "", "", uint8(1), "trg-0000000000000000")
	f.Add("Bad Name", "1.0", "", uint8(0), "")
	f.Add("ok", "-bad", "", uint8(0), "")
	f.Add("ok", "1", "", uint8(2), "other")
	f.Add("ok", "1", "", uint8(3), "other")
	f.Add("ok", "1", "", uint8(4), "")
	f.Add("ok", "1", "", uint8(5), "")
	f.Add("ok", "1", "", uint8(6), "")
	f.Fuzz(func(t *testing.T, name, version, description string, touch uint8, other string) {
		current, _, err := triggers.Prepare(triggers.Registration{Namespace: "payments", OrkflowName: "hello-ork", Name: "ci"})
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		snapshot := current
		snapshot.SecretSalt = append([]byte(nil), current.SecretSalt...)
		snapshot.SecretHash = append([]byte(nil), current.SecretHash...)

		next, err := triggers.ApplyChange(current, func(next *triggers.Trigger) error {
			next.Name, next.OrkflowVersion, next.Description = name, version, description
			switch touch % 7 {
			case 1:
				next.ID = other
			case 2:
				next.Namespace = other
			case 3:
				next.OrkflowName = other
			case 4:
				next.CreatedAt = next.CreatedAt.Add(1)
			case 5:
				next.SecretSalt[0] ^= 1
			case 6:
				next.SecretHash = nil
			}
			return nil
		})
		if current.ID != snapshot.ID || current.Name != snapshot.Name || current.OrkflowVersion != snapshot.OrkflowVersion ||
			!bytes.Equal(current.SecretSalt, snapshot.SecretSalt) || !bytes.Equal(current.SecretHash, snapshot.SecretHash) ||
			!current.CreatedAt.Equal(snapshot.CreatedAt) {
			t.Fatalf("ApplyChange altered the record it started from: %+v became %+v", snapshot, current)
		}
		identityTouched := touch%7 != 0 && !(touch%7 == 1 && other == current.ID) &&
			!(touch%7 == 2 && other == current.Namespace) && !(touch%7 == 3 && other == current.OrkflowName)
		if identityTouched {
			if err == nil {
				t.Fatalf("an update that changed an immutable field (case %d, %q) was accepted: %+v", touch%7, other, next)
			}
			return
		}
		valid := (name == "" || slugRule(name)) && (version == "" || versionRule(version))
		if (err == nil) != valid {
			t.Fatalf("ApplyChange(name %q, version %q) = %v; the registration rules say accepted=%v", name, version, err, valid)
		}
		if err != nil {
			if next.ID != "" || next.Name != "" {
				t.Fatalf("a refused update returned a record: %+v", next)
			}
			return
		}
		if next.ID != current.ID || next.Namespace != current.Namespace || next.OrkflowName != current.OrkflowName ||
			!next.CreatedAt.Equal(current.CreatedAt) || !bytes.Equal(next.SecretSalt, current.SecretSalt) ||
			!bytes.Equal(next.SecretHash, current.SecretHash) {
			t.Fatalf("an accepted update moved an immutable field: %+v from %+v", next, current)
		}
		if next.Name != name || next.OrkflowVersion != version || next.Description != description {
			t.Fatalf("an accepted update did not apply its change: %+v", next)
		}
	})
}

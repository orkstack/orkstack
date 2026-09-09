package triggers_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/triggers"
)

func TestPrepare_MintsAnOpaqueIDAndAToken(t *testing.T) {
	reg := triggers.Registration{
		Namespace:      "payments",
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.2.0",
		Description:    "nightly",
	}
	trigger, secret, err := triggers.Prepare(reg)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if !strings.HasPrefix(trigger.ID, triggers.IDPrefix) {
		t.Errorf("id = %q, want the %q prefix", trigger.ID, triggers.IDPrefix)
	}
	if hexPart := strings.TrimPrefix(trigger.ID, triggers.IDPrefix); len(hexPart) != 16 {
		t.Errorf("id %q has %d hex characters after the prefix, want 16", trigger.ID, len(hexPart))
	}
	if !triggers.ValidID(trigger.ID) {
		t.Errorf("ValidID(%q) = false for a generated id", trigger.ID)
	}
	if len(secret) != 64 {
		t.Errorf("secret has %d hex characters, want 64 (32 random bytes)", len(secret))
	}
	if _, err := hex.DecodeString(secret); err != nil {
		t.Errorf("secret %q is not hex: %v", secret, err)
	}
	if trigger.CreatedAt.IsZero() || trigger.CreatedAt.Location().String() != "UTC" {
		t.Errorf("createdAt = %v, want a UTC instant", trigger.CreatedAt)
	}
	if trigger.Namespace != reg.Namespace || trigger.OrkflowName != reg.OrkflowName ||
		trigger.OrkflowVersion != reg.OrkflowVersion || trigger.Description != reg.Description {
		t.Errorf("prepared %+v, want the registration's coordinates", trigger)
	}

	// The digest is exactly sha256(salt || secret): the salt is mixed in, and
	// the plaintext is nowhere on the record.
	want := sha256.Sum256(append(append([]byte(nil), trigger.SecretSalt...), secret...))
	if hex.EncodeToString(trigger.SecretHash) != hex.EncodeToString(want[:]) {
		t.Errorf("digest = %x, want sha256(salt || secret) = %x", trigger.SecretHash, want)
	}
	if len(trigger.SecretSalt) != 16 {
		t.Errorf("salt is %d bytes, want 16", len(trigger.SecretSalt))
	}

	second, otherSecret, err := triggers.Prepare(reg)
	if err != nil {
		t.Fatalf("Prepare (second): %v", err)
	}
	if second.ID == trigger.ID || otherSecret == secret {
		t.Error("two preparations drew the same id or the same secret")
	}
	if hex.EncodeToString(second.SecretSalt) == hex.EncodeToString(trigger.SecretSalt) {
		t.Error("two preparations drew the same salt")
	}
}

func TestVerifySecret_AcceptsOnlyTheIssuedToken(t *testing.T) {
	record, secret, err := triggers.Prepare(triggers.Registration{
		Namespace: "default", OrkflowName: "hello-ork",
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	noHash := record
	noHash.SecretHash = nil
	noSalt := record
	noSalt.SecretSalt = nil

	cases := []struct {
		label     string
		record    triggers.Trigger
		presented string
		want      bool
	}{
		{"the issued token", record, secret, true},
		{"an empty token", record, "", false},
		{"a truncated token", record, secret[:len(secret)-1], false},
		{"an extended token", record, secret + "0", false},
		{"the token upper-cased", record, strings.ToUpper(secret), false},
		{"the digest presented as the token", record, hex.EncodeToString(record.SecretHash), false},
		{"a record stripped for a listing", record.Public(), secret, false},
		{"a record with no digest", noHash, secret, false},
		{"a record with no salt", noSalt, secret, false},
	}
	for _, tc := range cases {
		if got := triggers.VerifySecret(tc.record, tc.presented); got != tc.want {
			t.Errorf("VerifySecret(%s) = %v, want %v", tc.label, got, tc.want)
		}
	}
}

func TestValidID_AcceptsOnlyGeneratedShapes(t *testing.T) {
	cases := map[string]bool{
		"trg-0123456789abcdef":                 true,
		"trg-0123456789abcdef0123456789abcdef": true,
		"trg-0123456789ABCDEF":                 false,
		"trg-0123456789abcde":                  false,
		"trg-":                                 false,
		"trg":                                  false,
		"wh-0123456789abcdef":                  false,
		"":                                     false,
		"../../etc/passwd":                     false,
		"trg-0123456789abcdef/../x":            false,
		"trg-0123456789abcdef.json":            false,
	}
	for id, want := range cases {
		if got := triggers.ValidID(id); got != want {
			t.Errorf("ValidID(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestRegistrationValidate_RefusesCoordinatesNoSubmissionCouldUse(t *testing.T) {
	cases := []struct {
		label string
		reg   triggers.Registration
		ok    bool
	}{
		{"a namespace and an orkflow", triggers.Registration{
			Namespace: "default", OrkflowName: "hello-ork"}, true},
		{"a pinned version", triggers.Registration{
			Namespace: "team-a", OrkflowName: "hello-ork", OrkflowVersion: "2026-09-08.1"}, true},
		{"no namespace", triggers.Registration{OrkflowName: "hello-ork"}, false},
		{"no orkflow", triggers.Registration{Namespace: "default"}, false},
		{"an upper-case namespace", triggers.Registration{
			Namespace: "Default", OrkflowName: "hello-ork"}, false},
		{"a namespace with a separator", triggers.Registration{
			Namespace: "team/a", OrkflowName: "hello-ork"}, false},
		{"a namespace that climbs", triggers.Registration{
			Namespace: "..", OrkflowName: "hello-ork"}, false},
		{"an orkflow starting with a digit", triggers.Registration{
			Namespace: "default", OrkflowName: "1hello"}, false},
		{"a version with a separator", triggers.Registration{
			Namespace: "default", OrkflowName: "hello-ork", OrkflowVersion: "1/0"}, false},
		{"an over-long namespace", triggers.Registration{
			Namespace:   "a" + strings.Repeat("b", triggers.MaxComponent),
			OrkflowName: "hello-ork"}, false},
	}
	for _, tc := range cases {
		err := tc.reg.Validate()
		if tc.ok && err != nil {
			t.Errorf("%s: Validate = %v, want accepted", tc.label, err)
		}
		if !tc.ok {
			if err == nil {
				t.Errorf("%s: Validate accepted %+v", tc.label, tc.reg)
				continue
			}
			if _, _, prepareErr := triggers.Prepare(tc.reg); prepareErr == nil {
				t.Errorf("%s: Prepare minted a record for a refused registration", tc.label)
			}
		}
	}
}

func TestTriggerPublicAndMatches(t *testing.T) {
	record, _, err := triggers.Prepare(triggers.Registration{
		Namespace: "payments", OrkflowName: "hello-ork",
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	public := record.Public()
	if len(public.SecretHash) != 0 || len(public.SecretSalt) != 0 {
		t.Error("Public kept the verification material")
	}
	if public.ID != record.ID || public.Namespace != record.Namespace {
		t.Errorf("Public = %+v, want the record's coordinates", public)
	}
	if len(record.SecretHash) == 0 {
		t.Error("Public mutated the record it copied from")
	}

	cases := []struct {
		namespace string
		orkflow   string
		want      bool
	}{
		{"payments", "", true},
		{"payments", "hello-ork", true},
		{"payments", "other-ork", false},
		{"default", "", false},
		{"default", "hello-ork", false},
		{"", "", false},
	}
	for _, tc := range cases {
		if got := record.Matches(tc.namespace, tc.orkflow); got != tc.want {
			t.Errorf("Matches(%q, %q) = %v, want %v", tc.namespace, tc.orkflow, got, tc.want)
		}
	}
}

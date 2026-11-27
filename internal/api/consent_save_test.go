package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// identityDef is a definition whose task holds an identity, written to the
// store directly.
func identityDef(ns, name, version, audience string) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Namespace: ns, Name: name, Version: version, Tasks: []*orkstackv1.Task{{
		Id: "apply", Image: &orkstackv1.ImageRef{Reference: "ghcr.io/acme/apply:1"},
		Identities: []*orkstackv1.IdentityRef{{Name: "tok", Audience: audience}},
	}}}
}

func asksOf(t *testing.T, def *orkstackv1.Orkflow) consent.Asks {
	t.Helper()
	a, err := consent.Compute(context.Background(), "team", def, func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
		return nil, errors.New("no child")
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestDecideConsent is the save table of workload-identity §3.2: a save
// that binds records a consent only to the digest the Orkflow asks for now,
// an Orkflow that asks for nothing needs none, a save that does not bind
// keeps the consent unless it gives one, and nothing is recorded on a
// refusal.
func TestDecideConsent(t *testing.T) {
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	nothing := asksOf(t, &orkstackv1.Orkflow{})
	role := asksOf(t, identityDef("team", "deploy", "2", "sts"))
	prior := consentRecord{Digest: consent.Empty(), At: now.Add(-time.Hour), By: "alice"}
	for _, tc := range []struct {
		name     string
		binds    bool
		unknown  bool
		given    string
		asks     consent.Asks
		want     consentRecord
		refusal  string
		mentions []string
	}{
		{name: "a bind of an Orkflow that asks for nothing", binds: true, asks: nothing,
			want: consentRecord{Digest: consent.Empty(), At: now, By: "bob"}},
		{name: "a bind with the digest shown", binds: true, given: role.Digest, asks: role,
			want: consentRecord{Digest: role.Digest, At: now, By: "bob", Audiences: []string{"sts"}}},
		{name: "a bind with no consent", binds: true, asks: role, refusal: "IDENTITY_CONSENT_MISMATCH",
			mentions: []string{"no consent was given", role.Digest, "orkctl schedule update nightly --consent=" + role.Digest}},
		{name: "a bind with a digest no longer asked for", binds: true, given: consent.Empty(), asks: role,
			refusal: "IDENTITY_CONSENT_MISMATCH", mentions: []string{"has changed since you were shown it", role.Digest}},
		{name: "a malformed digest", binds: true, given: "c1:nope", asks: role, refusal: "IDENTITY_CONSENT_MISMATCH"},
		{name: "a save that binds nothing keeps the consent", asks: role, want: prior},
		{name: "a save that binds nothing gives a consent", given: role.Digest, asks: role,
			want: consentRecord{Digest: role.Digest, At: now, By: "bob", Audiences: []string{"sts"}}},
		{name: "a save that binds nothing gives a stale one", given: consent.Empty(), asks: role, refusal: "IDENTITY_CONSENT_MISMATCH"},
		{name: "a bind of an Orkflow whose child is not there records no consent", binds: true, unknown: true, want: consentRecord{}},
		{name: "a save of one that binds nothing keeps the consent", unknown: true, want: prior},
		{name: "a consent to one whose child is not there", binds: true, unknown: true, given: role.Digest,
			refusal: "IDENTITY_CONSENT_MISMATCH", mentions: []string{"cannot be known"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decideConsent(consentSave{
				Kind: "schedule", Name: "nightly", Orkflow: "deploy", Binds: tc.binds, Unknown: tc.unknown, Given: tc.given,
				Asks: tc.asks, Prior: prior, By: "bob", Now: now,
			})
			if tc.refusal != "" {
				if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("err = %v, want failed_precondition %s", err, tc.refusal)
				}
				for _, m := range tc.mentions {
					if !strings.Contains(err.Error(), m) {
						t.Errorf("refusal %q does not mention %q", err.Error(), m)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("decideConsent: %v", err)
			}
			if got.Digest != tc.want.Digest || !got.At.Equal(tc.want.At) || got.By != tc.want.By ||
				strings.Join(got.Audiences, ",") != strings.Join(tc.want.Audiences, ",") ||
				strings.Join(got.Grants, ",") != strings.Join(tc.want.Grants, ",") {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestAsksFor_ResolvesTheVersionTheObjectFollows: an object that pins no
// version is judged on the current pointer, and an absent Orkflow is
// not_found, never "asks for nothing".
func TestAsksFor_ResolvesTheVersionTheObjectFollows(t *testing.T) {
	ctx := context.Background()
	store := engine.NewMemoryOrkflowStore()
	for _, def := range []*orkstackv1.Orkflow{identityDef("team", "deploy", "1", "first"), identityDef("team", "deploy", "2", "second")} {
		if _, _, err := store.Put(ctx, def, artifacts.HeadExpectation{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.SetCurrentVersion(ctx, "team", "deploy", "2", artifacts.CurrentExpectation{}); err != nil {
		t.Fatal(err)
	}
	s := &Service{orkflows: store}
	for _, tc := range []struct{ version, audience string }{{"1", "first"}, {"2", "second"}, {"", "second"}} {
		asks, err := s.asksFor(ctx, "team", "deploy", tc.version)
		if err != nil {
			t.Fatalf("asksFor(%q): %v", tc.version, err)
		}
		if strings.Join(asks.Audiences, ",") != tc.audience {
			t.Errorf("asksFor(%q) audiences = %v, want %s", tc.version, asks.Audiences, tc.audience)
		}
	}
	if _, err := s.asksFor(ctx, "team", "absent", ""); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("an absent Orkflow: err = %v, want not_found", err)
	}
}

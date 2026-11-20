package identity

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestAudienceAllowed: an audience is allowed only by an exact pattern or a
// pattern whose trailing `*` matches any rest; no pattern allows nothing.
func TestAudienceAllowed(t *testing.T) {
	for _, tc := range []struct {
		patterns []string
		audience string
		want     bool
	}{
		{nil, "sts.amazonaws.com", false},
		{[]string{"sts.amazonaws.com"}, "sts.amazonaws.com", true},
		{[]string{"sts.amazonaws.com"}, "sts.amazonaws.com.evil", false},
		{[]string{"https://vault.example/*"}, "https://vault.example/v1/auth", true},
		{[]string{"https://vault.example/*"}, "https://vault.example", false},
		{[]string{"*"}, "anything", true},
		{[]string{"a*b"}, "a*b", true},
		{[]string{"a*b"}, "axb", false},
	} {
		if got := AudienceAllowed(tc.patterns, tc.audience); got != tc.want {
			t.Errorf("AudienceAllowed(%v, %q) = %v, want %v", tc.patterns, tc.audience, got, tc.want)
		}
	}
}

// TestCheckAudiences: the first identity whose audience the namespace does
// not allow is refused IDENTITY_NOT_PERMITTED, naming the field, the task
// and the audience; a definition that asks for none passes any policy.
func TestCheckAudiences(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		{Id: "plain"},
		{Id: "apply", Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com"}, {Name: "vault", Audience: "vault"}}},
	}}
	if err := CheckAudiences("payments", []string{"sts.amazonaws.com", "vault"}, wf); err != nil {
		t.Fatalf("allowed: %v", err)
	}
	err := CheckAudiences("payments", []string{"sts.amazonaws.com"}, wf)
	var refused *AudienceNotPermittedError
	if !errors.As(err, &refused) || refused.Field() != "tasks[1].identities[1].audience" ||
		!strings.Contains(err.Error(), "IDENTITY_NOT_PERMITTED") || !strings.Contains(err.Error(), `"vault"`) {
		t.Fatalf("err = %v, want IDENTITY_NOT_PERMITTED at tasks[1].identities[1].audience", err)
	}
	if err := CheckAudiences("payments", nil, &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "plain"}}}); err != nil {
		t.Fatalf("a definition asking for nothing: %v", err)
	}
	if err := CheckAudiences("payments", nil, wf); !errors.As(err, &refused) || !strings.Contains(err.Error(), "allows no audience") {
		t.Fatalf("no policy: err = %v", err)
	}
}

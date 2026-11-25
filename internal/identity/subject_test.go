package identity

import (
	"strings"
	"testing"
)

// TestSubject_BothForms: each kind of task gives the colon form and the
// SPIFFE ID of workload-identity.md §4 and §4.1, and the SPIFFE ID parses
// back to the names it was built from.
func TestSubject_BothForms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		s     Subject
		ork   string
		spiff string
	}{
		{"task", Subject{Namespace: "payments", Orkflow: "deploy", Task: "apply"},
			"ork:namespace:payments:orkflow:deploy:task:apply",
			"spiffe://ork.acme.dev/ork/ns/payments/orkflow/deploy/task/apply"},
		{"grant", Subject{Namespace: "payments", Orkflow: "deploy", Task: "apply", Grant: "deployer"},
			"ork:namespace:payments:orkflow:deploy:task:apply:grant:deployer",
			"spiffe://ork.acme.dev/ork/ns/payments/orkflow/deploy/task/apply/grant/deployer"},
		{"shared", Subject{Namespace: "payments", Owner: "platform", Orkflow: "deploy-lib", Task: "apply"},
			"ork:namespace:payments:orkflow:platform/deploy-lib:task:apply",
			"spiffe://ork.acme.dev/ork/ns/payments/shared/platform/deploy-lib/task/apply"},
		{"shared grant", Subject{Namespace: "payments", Owner: "platform", Orkflow: "deploy-lib", Task: "apply", Grant: "g"},
			"ork:namespace:payments:orkflow:platform/deploy-lib:task:apply:grant:g",
			"spiffe://ork.acme.dev/ork/ns/payments/shared/platform/deploy-lib/task/apply/grant/g"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ork, err := tc.s.Ork()
			if err != nil || ork != tc.ork {
				t.Fatalf("Ork() = %q, %v; want %q", ork, err, tc.ork)
			}
			id, err := tc.s.SPIFFEID("ork.acme.dev")
			if err != nil || id != tc.spiff {
				t.Fatalf("SPIFFEID() = %q, %v; want %q", id, err, tc.spiff)
			}
			back, domain, err := ParseSPIFFEID(id)
			if err != nil || back != tc.s || domain != "ork.acme.dev" {
				t.Fatalf("ParseSPIFFEID(%q) = %+v, %q, %v", id, back, domain, err)
			}
		})
	}
}

// TestSubject_Refuses: a name that is not a slug, a missing part, and a
// malformed trust domain build no subject.
func TestSubject_Refuses(t *testing.T) {
	good := Subject{Namespace: "payments", Orkflow: "deploy", Task: "apply"}
	for name, s := range map[string]Subject{
		"no namespace":   {Orkflow: "deploy", Task: "apply"},
		"no orkflow":     {Namespace: "payments", Task: "apply"},
		"no task":        {Namespace: "payments", Orkflow: "deploy"},
		"a colon":        {Namespace: "pay:ments", Orkflow: "deploy", Task: "apply"},
		"a slash":        {Namespace: "payments", Orkflow: "de/ploy", Task: "apply"},
		"an upper case":  {Namespace: "payments", Orkflow: "deploy", Task: "Apply"},
		"a bad grant":    {Namespace: "payments", Orkflow: "deploy", Task: "apply", Grant: "a b"},
		"a bad owner":    {Namespace: "payments", Owner: "-x", Orkflow: "deploy", Task: "apply"},
		"a long segment": {Namespace: strings.Repeat("a", 201), Orkflow: "deploy", Task: "apply"},
	} {
		if v, err := s.Ork(); err == nil {
			t.Errorf("%s: Ork() = %q", name, v)
		}
		if v, err := s.SPIFFEID("ork.acme.dev"); err == nil {
			t.Errorf("%s: SPIFFEID() = %q", name, v)
		}
	}
	for _, domain := range []string{"", "Ork.acme.dev", "ork.acme.dev:443", "https://ork.acme.dev", "ork.acme.dev/x", "ork acme", strings.Repeat("a", 221)} {
		if err := ValidTrustDomain(domain); err == nil {
			t.Errorf("trust domain %q was accepted", domain)
		}
		if v, err := good.SPIFFEID(domain); err == nil {
			t.Errorf("SPIFFEID(%q) = %q", domain, v)
		}
	}
	for _, domain := range []string{"ork.acme.dev", "a", "ork_x-1.example", strings.Repeat("a", 220)} {
		if err := ValidTrustDomain(domain); err != nil {
			t.Errorf("trust domain %q: %v", domain, err)
		}
	}
	for _, id := range []string{
		"", "spiffe://ork.acme.dev", "spiffe://ork.acme.dev/other/ns/a/orkflow/b/task/c",
		"spiffe://ork.acme.dev/ork/ns/a/orkflow/b/task/c/grant", "spiffe://ork.acme.dev/ork/ns/a/orkflow/b/task/c/extra/x",
		"spiffe://ork.acme.dev/ork/ns/a/orkflow/b/task/c/", "https://ork.acme.dev/ork/ns/a/orkflow/b/task/c",
		"spiffe://ORK/ork/ns/a/orkflow/b/task/c", "spiffe://ork.acme.dev/ork/ns/A/orkflow/b/task/c",
		"spiffe://ork.acme.dev/ork/ns/a/shared/b/task/c",
	} {
		if s, _, err := ParseSPIFFEID(id); err == nil {
			t.Errorf("ParseSPIFFEID(%q) = %+v", id, s)
		}
	}
}

// TestValidTrustDomain_LeavesRoomForASubject: the longest trust domain is the
// one with which the shortest subject still fits, so a longer one is refused
// where it is set and not at every create.
func TestValidTrustDomain_LeavesRoomForASubject(t *testing.T) {
	shortest := Subject{Namespace: "a", Orkflow: "a", Task: "a"}
	longest := strings.Repeat("d", 220)
	if err := ValidTrustDomain(longest); err != nil {
		t.Fatalf("a 220-byte trust domain: %v", err)
	}
	id, err := shortest.SPIFFEID(longest)
	if err != nil || len(id) != MaxSubjectBytes {
		t.Fatalf("SPIFFEID is %d bytes, %v; want exactly %d", len(id), err, MaxSubjectBytes)
	}
	if err := shortest.Fits(FormatSPIFFE, longest); err != nil {
		t.Fatalf("the shortest subject under the longest trust domain: %v", err)
	}
	if err := ValidTrustDomain(longest + "d"); err == nil {
		t.Fatal("a trust domain with which no subject fits was accepted")
	}
}

// TestSubject_Fits: a subject of at most 255 bytes fits; with a trust
// domain set both forms are held to it whichever is chosen, so a later
// switch refuses no Orkflow that exists.
func TestSubject_Fits(t *testing.T) {
	// 29 bytes of "ork:namespace::orkflow::task:" frame, so these three
	// names make an ork form of exactly 255 bytes.
	s := Subject{Namespace: strings.Repeat("n", 76), Orkflow: strings.Repeat("o", 76), Task: strings.Repeat("t", 74)}
	ork, err := s.Ork()
	if err != nil || len(ork) != 255 {
		t.Fatalf("len(Ork()) = %d, %v", len(ork), err)
	}
	if err := s.Fits(FormatOrk, ""); err != nil {
		t.Fatalf("a 255-byte subject: %v", err)
	}
	if err := s.Fits(FormatOrk, "ork.acme.dev"); err == nil || !strings.Contains(err.Error(), "SPIFFE") {
		t.Fatalf("with a trust domain the SPIFFE form, longer, fit: %v", err)
	}
	s.Task += "t"
	if err := s.Fits(FormatOrk, ""); err == nil || !strings.Contains(err.Error(), "256") {
		t.Fatalf("a 256-byte subject: %v", err)
	}
	short := Subject{Namespace: "a", Orkflow: "b", Task: "c", Grant: "d"}
	for _, f := range []Format{FormatOrk, FormatSPIFFE} {
		if err := short.Fits(f, "ork.acme.dev"); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	if err := short.Fits(FormatSPIFFE, ""); err == nil {
		t.Fatal("the SPIFFE format with no trust domain fit")
	}
	if _, err := ParseFormat("other"); err == nil {
		t.Fatal("an unknown format was accepted")
	}
	if f, err := ParseFormat(""); err != nil || f != FormatOrk {
		t.Fatalf("the default format = %q, %v", f, err)
	}
}

// TestSubject_Claims: the chosen form is sub, and with a trust domain set
// the other form is its own claim.
func TestSubject_Claims(t *testing.T) {
	s := Subject{Namespace: "payments", Orkflow: "deploy", Task: "apply"}
	for _, tc := range []struct {
		format     Format
		domain     string
		sub, other string
		otherClaim string
	}{
		{FormatOrk, "", "ork:namespace:payments:orkflow:deploy:task:apply", "", ""},
		{FormatOrk, "ork.acme.dev", "ork:namespace:payments:orkflow:deploy:task:apply", "spiffe://ork.acme.dev/ork/ns/payments/orkflow/deploy/task/apply", ClaimSPIFFEID},
		{FormatSPIFFE, "ork.acme.dev", "spiffe://ork.acme.dev/ork/ns/payments/orkflow/deploy/task/apply", "ork:namespace:payments:orkflow:deploy:task:apply", ClaimSubject},
	} {
		sub, claim, value, err := s.Claims(tc.format, tc.domain)
		if err != nil || sub != tc.sub || claim != tc.otherClaim || value != tc.other {
			t.Fatalf("%s %q: Claims() = %q, %q=%q, %v", tc.format, tc.domain, sub, claim, value, err)
		}
	}
}

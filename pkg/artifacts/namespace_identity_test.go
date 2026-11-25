package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var policyEpoch = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func testPolicy() NamespacePolicy {
	extra := int32(1)
	return NamespacePolicy{
		Audiences:     []string{"https://vault.example", "//iam.googleapis.com/projects/1/*"},
		AttemptsExtra: &extra,
		Grants: map[string]Grant{
			"deployer": {Provider: "aws", Principal: "arn:aws:iam::1:role/deploy", Orkflows: []string{"deploy"}, CurrentOnly: true, Origins: []string{"submit", "run-again"}},
			"reader":   {Provider: "aws", Principal: "arn:aws:iam::1:role/read", Orkflows: []string{"*", "platform/deploy-lib"}},
		},
	}
}

// failingReplace refuses every replace, as a store that went down between
// a policy's revision and its swap.
type failingReplace struct{ blobs }

func (failingReplace) replace(context.Context, string, string, []byte, string) error {
	return errors.New("connection refused")
}

func policyStore(t *testing.T) (*IncarnationStore, blobs) {
	t.Helper()
	s, err := NewIncarnationStore(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return policyEpoch }
	return s, s.b
}

// TestNamespacePolicy_WrittenByGenerationRevisionFirst: a policy is written
// against the generation the writer read, with an immutable revision
// written before the swap; a writer that read an older generation is
// refused and changes nothing; the namespace's id is kept.
func TestNamespacePolicy_WrittenByGenerationRevisionFirst(t *testing.T) {
	ctx := context.Background()
	for name, s := range incarnationStores(t) {
		t.Run(name, func(t *testing.T) {
			s.now = func() time.Time { return policyEpoch }
			if _, ok, err := s.NamespacePolicy(ctx, "payments", policyEpoch); err != nil || ok {
				t.Fatalf("before any write = %v, %v", ok, err)
			}
			id, err := s.EnsureNamespaceID(ctx, "payments", policyEpoch)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{Principal: "alice"})
			if err != nil || got.Generation != 1 || got.ChangedBy != "alice" || !got.ChangedAt.Equal(policyEpoch) {
				t.Fatalf("first write = %+v, %v", got, err)
			}
			read, ok, err := s.NamespacePolicy(ctx, "payments", policyEpoch)
			if err != nil || !ok || read.Generation != 1 || read.Grants["deployer"].Principal != "arn:aws:iam::1:role/deploy" || *read.AttemptsExtra != 1 {
				t.Fatalf("read = %+v, %v, %v", read, ok, err)
			}
			if again, err := s.EnsureNamespaceID(ctx, "payments", policyEpoch); err != nil || again != id {
				t.Fatalf("the id after a policy write = %q, %v; was %q", again, err, id)
			}
			next := testPolicy()
			next.Audiences = []string{"https://vault.example"}
			if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, next, 0, PolicyWriter{Principal: "bob"}); !errors.Is(err, ErrPolicyChanged) {
				t.Fatalf("a write from generation 0 after 1 = %v", err)
			}
			if got, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, next, 1, PolicyWriter{Principal: "bob"}); err != nil || got.Generation != 2 {
				t.Fatalf("a write from generation 1 = %+v, %v", got, err)
			}
			revisions, err := s.b.list(ctx, blobListInput{Prefix: IncarnationPrefix + "/namespaces/payments/revisions/"})
			if err != nil || len(revisions.Objects) != 2 || !strings.Contains(revisions.Objects[0].Key, "/000000000001-") || !strings.Contains(revisions.Objects[1].Key, "/000000000002-") {
				t.Fatalf("revisions = %+v, %v", revisions.Objects, err)
			}
		})
	}
}

// TestNamespacePolicy_ARevisionWithNoSwapChangesNothing: a write whose
// swap fails leaves its revision and the policy as it was.
func TestNamespacePolicy_ARevisionWithNoSwapChangesNothing(t *testing.T) {
	ctx := context.Background()
	s, b := policyStore(t)
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{Principal: "alice"}); err != nil {
		t.Fatal(err)
	}
	s.b = failingReplace{b}
	next := testPolicy()
	next.Grants = nil
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, next, 1, PolicyWriter{Principal: "bob"}); err == nil {
		t.Fatal("a write whose swap failed succeeded")
	}
	s.b = b
	read, _, err := s.NamespacePolicy(ctx, "payments", policyEpoch)
	if err != nil || read.Generation != 1 || len(read.Grants) != 2 {
		t.Fatalf("after a failed swap = %+v, %v", read, err)
	}
	revisions, _ := b.list(ctx, blobListInput{Prefix: IncarnationPrefix + "/namespaces/payments/revisions/"})
	if len(revisions.Objects) != 2 {
		t.Fatalf("revisions = %+v", revisions.Objects)
	}
}

// TestNamespacePolicy_BelongsToOneIncarnation: a policy is read only for
// the incarnation it was written for; a namespace made again starts with
// none, and a policy cleared for a delete keeps the id.
func TestNamespacePolicy_BelongsToOneIncarnation(t *testing.T) {
	ctx := context.Background()
	s, _ := policyStore(t)
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{Principal: "alice"}); err != nil {
		t.Fatal(err)
	}
	again := policyEpoch.Add(time.Hour)
	if _, ok, err := s.NamespacePolicy(ctx, "payments", again); err != nil || ok {
		t.Fatalf("another incarnation's policy = %v, %v", ok, err)
	}
	id, _, _ := s.NamespaceID(ctx, "payments", policyEpoch)
	if err := s.ClearNamespacePolicy(ctx, "payments", policyEpoch); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.NamespacePolicy(ctx, "payments", policyEpoch); err != nil || ok {
		t.Fatalf("a cleared policy = %v, %v", ok, err)
	}
	if kept, ok, err := s.NamespaceID(ctx, "payments", policyEpoch); err != nil || !ok || kept != id {
		t.Fatalf("the id after a clear = %q, %v, %v; was %q", kept, ok, err, id)
	}
	if err := s.ClearNamespacePolicy(ctx, "payments", again); err != nil {
		t.Fatalf("clearing another incarnation's policy = %v", err)
	}
	if _, err := s.EnsureNamespaceID(ctx, "payments", again); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.NamespacePolicy(ctx, "payments", again); err != nil || ok {
		t.Fatalf("a namespace made again inherits a policy: %v, %v", ok, err)
	}
	// A new incarnation's first policy starts its generations again.
	if got, err := s.SetNamespacePolicy(ctx, "payments", again, testPolicy(), 0, PolicyWriter{Principal: "carol"}); err != nil || got.Generation != 1 {
		t.Fatalf("the new incarnation's first policy = %+v, %v", got, err)
	}
}

// TestNamespacePolicy_DefaultHoldsOne: `default`, whose id is a constant,
// may still hold a policy, and its id is still never stored as minted.
func TestNamespacePolicy_DefaultHoldsOne(t *testing.T) {
	ctx := context.Background()
	s, _ := policyStore(t)
	if _, err := s.SetNamespacePolicy(ctx, DefaultNamespace, time.Time{}, testPolicy(), 0, PolicyWriter{Principal: "alice"}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.NamespacePolicy(ctx, DefaultNamespace, time.Time{}); err != nil || !ok || got.Generation != 1 {
		t.Fatalf("default's policy = %+v, %v, %v", got, ok, err)
	}
	if id, ok, err := s.NamespaceID(ctx, DefaultNamespace, time.Time{}); err != nil || !ok || id != DefaultNamespaceID {
		t.Fatalf("default's id = %q, %v, %v", id, ok, err)
	}
}

// TestNamespacePolicy_RefusesWhatCannotBeStored: a policy the mint could
// misread is refused before anything is written.
func TestNamespacePolicy_RefusesWhatCannotBeStored(t *testing.T) {
	ctx := context.Background()
	over := int32(MaxAttemptsExtra + 1)
	for name, mutate := range map[string]func(*NamespacePolicy){
		"an empty audience":        func(p *NamespacePolicy) { p.Audiences = []string{""} },
		"an audience twice":        func(p *NamespacePolicy) { p.Audiences = []string{"a", "a"} },
		"a grant name":             func(p *NamespacePolicy) { p.Grants["Deployer"] = p.Grants["deployer"] },
		"a grant with no provider": func(p *NamespacePolicy) { g := p.Grants["reader"]; g.Provider = ""; p.Grants["reader"] = g },
		"a grant with no orkflows": func(p *NamespacePolicy) { g := p.Grants["reader"]; g.Orkflows = nil; p.Grants["reader"] = g },
		"an orkflow name": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Orkflows = []string{"Deploy"}
			p.Grants["reader"] = g
		},
		"a shared orkflow's owner": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Orkflows = []string{"Bad/x"}
			p.Grants["reader"] = g
		},
		"an origin": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Origins = []string{"cron"}
			p.Grants["reader"] = g
		},
		"an origin twice": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Origins = []string{"submit", "submit"}
			p.Grants["reader"] = g
		},
		"extra over the most":      func(p *NamespacePolicy) { p.AttemptsExtra = &over },
		"an audience with a space": func(p *NamespacePolicy) { p.Audiences = []string{"a b"} },
		"a principal with a newline": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Principal = "a\nlevel=ERROR forged"
			p.Grants["reader"] = g
		},
		"a principal with a bidi mark": func(p *NamespacePolicy) {
			g := p.Grants["reader"]
			g.Principal = "arn\u202e"
			p.Grants["reader"] = g
		},
		"an audience not UTF-8": func(p *NamespacePolicy) { p.Audiences = []string{"\xff"} },
		"65 grants": func(p *NamespacePolicy) {
			for i := range 65 {
				p.Grants["g"+strings.Repeat("a", i)] = p.Grants["reader"]
			}
		},
	} {
		s, _ := policyStore(t)
		p := testPolicy()
		mutate(&p)
		if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, p, 0, PolicyWriter{Principal: "alice"}); !errors.Is(err, ErrInvalidPolicy) {
			t.Errorf("%s = %v; want ErrInvalidPolicy", name, err)
		}
		if _, ok, _ := s.NamespacePolicy(ctx, "payments", policyEpoch); ok {
			t.Errorf("%s: a refused policy was written", name)
		}
	}
	s, _ := policyStore(t)
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{Verified: true}); !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("a verified write by nobody = %v", err)
	}
	if got, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{}); err != nil || got.ChangedBy != "" || got.ChangedByVerified {
		t.Errorf("a write that names no caller = %+v, %v", got, err)
	}
}

// TestNamespacePolicy_AnUnreadablePolicyLeavesTheIDReadable: a policy that
// does not decode is ErrPolicyUnreadable to a policy read, never "none",
// and the namespace's id is still read.
func TestNamespacePolicy_AnUnreadablePolicyLeavesTheIDReadable(t *testing.T) {
	ctx := context.Background()
	s, b := policyStore(t)
	id, err := s.EnsureNamespaceID(ctx, "payments", policyEpoch)
	if err != nil {
		t.Fatal(err)
	}
	key := IncarnationPrefix + "/namespaces/payments.json"
	raw, version, err := b.get(ctx, key, incarnationMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Replace(raw, []byte(`"id":`), []byte(`"policy":{"v":1,"grants":7},"id":`), 1)
	if err := b.replace(ctx, key, "application/json", bad, version); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.NamespacePolicy(ctx, "payments", policyEpoch); !errors.Is(err, ErrPolicyUnreadable) {
		t.Fatalf("an undecodable policy = %v", err)
	}
	if got, ok, err := s.NamespaceID(ctx, "payments", policyEpoch); err != nil || !ok || got != id {
		t.Fatalf("the id beside it = %q, %v, %v", got, ok, err)
	}
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, testPolicy(), 0, PolicyWriter{Principal: "alice"}); !errors.Is(err, ErrPolicyUnreadable) {
		t.Fatalf("a write over an unreadable policy = %v", err)
	}
}

// FuzzNamespacePolicy: a policy read back either decodes, and then encodes
// to exactly its bytes, or is refused; it never panics.
func FuzzNamespacePolicy(f *testing.F) {
	p := testPolicy()
	p.Generation, p.ChangedBy, p.ChangedAt = 3, "alice", policyEpoch
	raw, err := encodePolicy(p)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"v":1}`))
	f.Add([]byte(`{"v":2,"grants":{}}`))
	f.Add(bytes.Replace(raw, []byte(`"aws"`), []byte(`null`), 1))
	f.Fuzz(func(t *testing.T, raw []byte) {
		p, err := decodePolicy(raw)
		if err != nil {
			return
		}
		again, err := encodePolicy(p)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%q re-encodes as %q, %v", raw, again, err)
		}
	})
}

// TestNamespacePolicy_APolicyTooLargeIsRefusedAndTheIDStaysReadable: a
// policy over MaxPolicyBytes is refused before anything is written, so the
// id record that would hold it is never made unreadable.
func TestNamespacePolicy_APolicyTooLargeIsRefusedAndTheIDStaysReadable(t *testing.T) {
	ctx := context.Background()
	s, _ := policyStore(t)
	id, err := s.EnsureNamespaceID(ctx, "payments", policyEpoch)
	if err != nil {
		t.Fatal(err)
	}
	big := NamespacePolicy{Grants: map[string]Grant{}}
	for i := range MaxGrants {
		var orkflows []string
		for j := range MaxGrantOrkflows {
			orkflows = append(orkflows, fmt.Sprintf("o%02d%s", j, strings.Repeat("x", 20)))
		}
		big.Grants[fmt.Sprintf("g%02d", i)] = Grant{Provider: "aws", Principal: "p", Orkflows: orkflows}
	}
	if _, err := s.SetNamespacePolicy(ctx, "payments", policyEpoch, big, 0, PolicyWriter{}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("a policy over the bound = %v", err)
	}
	if got, ok, err := s.NamespaceID(ctx, "payments", policyEpoch); err != nil || !ok || got != id {
		t.Fatalf("the id after = %q, %v, %v", got, ok, err)
	}
}

// TestCheckNamespacePolicy_AGrantServesOnlyStartsARunCanHave: a grant's
// origins are the words runs are recorded with. event-source is one only
// once a run an event source starts is recorded as that: until then a grant
// that names it is refused, not kept to serve nothing.
func TestCheckNamespacePolicy_AGrantServesOnlyStartsARunCanHave(t *testing.T) {
	_, eventSource := orkstackv1.RunOriginKind_value["RUN_ORIGIN_KIND_EVENT_SOURCE"]
	for word, want := range map[string]bool{
		"submit": true, "schedule": true, "trigger": true, "parent-run": true, "run-again": true,
		"event-source": eventSource,
		"unspecified":  false, "": false, "SUBMIT": false, "run_again": false, "RUN_ORIGIN_KIND_SUBMIT": false,
	} {
		err := CheckNamespacePolicy(NamespacePolicy{Grants: map[string]Grant{
			"g": {Provider: "aws", Principal: "p", Orkflows: []string{"*"}, Origins: []string{word}}}})
		if (err == nil) != want || (err != nil && !errors.Is(err, ErrInvalidPolicy)) {
			t.Errorf("a grant for %q: %v; accepted should be %v", word, err, want)
		}
	}
}

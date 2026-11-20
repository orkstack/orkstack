package identity

import (
	"testing"
	"time"
)

// TestMintLimits: a principal's bucket admits its burst and then refuses;
// an attempt's budget admits 4 granted mints a minute; a refunded mint
// spends nothing; time refills both.
func TestMintLimits(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	m := NewMintLimits(func() time.Time { return now })
	for i := range principalBurst {
		if _, ok := m.TakePrincipal("p"); !ok {
			t.Fatalf("principal refused at %d of its burst", i)
		}
	}
	if _, ok := m.TakePrincipal("p"); ok {
		t.Fatal("past its burst a principal was admitted")
	}
	if _, ok := m.TakePrincipal("q"); !ok {
		t.Fatal("one principal's flood refused another")
	}
	for range attemptBudget {
		s, ok := m.TakeAttempt("a")
		if !ok {
			t.Fatal("attempt refused within its budget")
		}
		_ = s
	}
	if _, ok := m.TakeAttempt("a"); ok {
		t.Fatal("a fifth granted mint in a minute was admitted")
	}
	now = now.Add(attemptWindow / attemptBudget)
	s, ok := m.TakeAttempt("a")
	if !ok {
		t.Fatal("the budget did not refill")
	}
	s.Refund()
	if _, ok := m.TakeAttempt("a"); !ok {
		t.Fatal("a refunded mint spent its token")
	}
}

// TestCommitLimits: the commit's principal bucket is 512, refilled 128 a
// second, and its attempt budget is the mint's.
func TestCommitLimits(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	m := NewCommitLimits(func() time.Time { return now })
	for i := range commitPrincipalBurst {
		if _, ok := m.TakePrincipal("p"); !ok {
			t.Fatalf("principal refused at %d of its burst", i)
		}
	}
	if _, ok := m.TakePrincipal("p"); ok {
		t.Fatal("past its burst a principal was admitted")
	}
	now = now.Add(time.Second)
	for i := range commitPrincipalRate {
		if _, ok := m.TakePrincipal("p"); !ok {
			t.Fatalf("a second refilled %d, not %d", i, commitPrincipalRate)
		}
	}
	for range attemptBudget {
		if _, ok := m.TakeAttempt("a"); !ok {
			t.Fatal("attempt refused within its budget")
		}
	}
	if _, ok := m.TakeAttempt("a"); ok {
		t.Fatal("a fifth granted call in a minute was admitted")
	}
}

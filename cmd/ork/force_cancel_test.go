package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// publishedNone is a deployment that never published a key.
type publishedNone struct{}

func (publishedNone) Read(context.Context) (artifacts.PublishedKeys, error) {
	return artifacts.PublishedKeys{}, nil
}

// TestForceCancelKeys: an engine with signing keys signs and verifies with
// them; one without, whose issuer is a nil *identity.Issuer, signs nothing and
// reads a signed record as nobody's only in asserted mode where nobody ever
// published a key.
func TestForceCancelKeys(t *testing.T) {
	issuer := &identity.Issuer{}
	if got := forceCancelKeys(issuer, publishedNone{}, func() string { return "enforce" }); got != any(issuer) {
		t.Fatalf("with keys: %T", got)
	}
	mode := "asserted"
	keyless := forceCancelKeys(nil, publishedNone{}, func() string { return mode })
	if _, err := keyless.SignDecision(jwt.KindForceCancelRecord, []byte("x")); !errors.Is(err, identity.ErrCannotSign) {
		t.Errorf("without keys it signed, or failed otherwise: %v", err)
	}
	if _, _, v := keyless.VerifyDecision(context.Background(), "a.b.c", jwt.KindForceCancelRecord, time.Now(), identity.NoBinding); v != identity.VerdictBad {
		t.Errorf("asserted, no key ever published: %v", v)
	}
	mode = "enforce"
	if _, _, v := keyless.VerifyDecision(context.Background(), "a.b.c", jwt.KindForceCancelRecord, time.Now(), identity.NoBinding); v != identity.VerdictUnavailable {
		t.Errorf("enforce: %v", v)
	}
}

// TestDeployedMode: until the mode record is read no mode is known, so nothing
// is judged by one; then it is the guard's. What must not act before waits.
func TestDeployedMode(t *testing.T) {
	m := newDeployedMode()
	if got := m.Mode(); got != "" {
		t.Errorf("before the record: %q", got)
	}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if m.wait(ended) {
		t.Error("known before the record was read")
	}
	m.follow(func() string { return "enforce" })
	if got := m.Mode(); got != "enforce" || !m.wait(context.Background()) {
		t.Errorf("after: %q", got)
	}
}

// TestForceCancelQueue: the engines run workflow code on the one channel a
// force cancel's ending is taken from, whatever the Orkers' queue is.
func TestForceCancelQueue(t *testing.T) {
	if got := queuesFor(config{defaultRouteOverride: "ork-elsewhere"}).run; got != task.DefaultQueue {
		t.Fatalf("workflow code runs on %q, and Publish takes work only from %q", got, task.DefaultQueue)
	}
}

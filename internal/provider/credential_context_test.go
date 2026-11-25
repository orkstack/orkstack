package provider_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// witness is a provider that keeps what it was called with and issues one
// JSON file: what the engine tells a provider, and what it masks, are the
// engine's own doing and need an answer no real provider would shape.
type witness struct {
	provider.Provider
	mu       sync.Mutex
	attempts []provider.Attempt
	refs     []provider.Ref
}

type witnessSession struct {
	provider.Session
	w *witness
}

func (w *witness) Login(ctx context.Context, principal string, who provider.Attempt, token provider.TokenSource) (provider.Session, error) {
	s, err := w.Provider.Login(ctx, principal, who, token)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts = append(w.attempts, who)
	return &witnessSession{Session: s, w: w}, nil
}

func (s *witnessSession) Credential(_ context.Context, ref provider.Parsed) (provider.Bundle, error) {
	s.w.mu.Lock()
	defer s.w.mu.Unlock()
	s.w.refs = append(s.w.refs, ref.Ref)
	return provider.Bundle{Files: map[string][]byte{
		"process.json": []byte(`{"Version":1,"Pin":"1234","SecretAccessKey":"a-long-secret-key"}`),
		"plain.json":   []byte(`{"region":"a-plain-region"}`),
	}, Plain: map[string]bool{"plain.json": true}, Secrets: [][]byte{[]byte("a-long-secret-key")}, Lifetime: time.Hour}, nil
}

// guardingMasker is a masker that keeps what it was told to keep out of a
// result, as the Orker's does.
type guardingMasker struct {
	provider.Masker
	mu      sync.Mutex
	guarded map[string][]string
}

func (m *guardingMasker) Guard(holder string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.guarded == nil {
		m.guarded = map[string][]string{}
	}
	m.guarded[holder] = append(m.guarded[holder], string(value))
}

// Only what a provider names certainly secret, and a secret's own value, is
// kept out of a task's result; what is masked besides is not.
func TestEngine_GuardsOnlyWhatIsCertainlySecret(t *testing.T) {
	m := &guardingMasker{}
	r := newRig(t, time.Hour, 0, withWitness(&witness{}), func(c *provider.EngineConfig) { m.Masker, c.Masker = c.Masker, m })
	r.setup()
	if len(m.guarded) != 1 {
		t.Fatalf("guarded %v; want the bundle's secret alone", m.guarded)
	}
	for holder, values := range m.guarded {
		if !strings.HasPrefix(holder, "credential/db/") || len(values) != 1 || values[0] != "a-long-secret-key" {
			t.Errorf("guarded %v under %q", values, holder)
		}
	}
}

func withWitness(w *witness) rigOption {
	return func(c *provider.EngineConfig) {
		g := c.Grants["vault-app"]
		w.Provider = g.Provider
		g.Provider = w
		c.Grants["vault-app"] = g
	}
}

// A provider that writes a file naming its own path is told the credential's
// name; an instance's login is told its longest life, a task's is not.
func TestEngine_TellsAProviderTheCredentialsNameAndAnInstancesMaxLogin(t *testing.T) {
	for name, tc := range map[string]struct {
		opt  rigOption
		want time.Duration
	}{
		"a task":      {func(c *provider.EngineConfig) { c.InstanceMaxTTL = time.Hour }, 0},
		"an instance": {withInstanceMaxTTL(time.Hour), time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			w := &witness{}
			newRig(t, time.Hour, 0, withWitness(w), tc.opt).setup()
			if len(w.refs) != 1 || w.refs[0].Name != "db" || w.refs[0].Locator != "creds/payments-rw" {
				t.Errorf("Credential was asked with %+v, want the name db", w.refs)
			}
			if len(w.attempts) != 1 || w.attempts[0].MaxLogin != tc.want {
				t.Errorf("Login was asked with %+v, want MaxLogin %s", w.attempts, tc.want)
			}
		})
	}
}

// A value inside a sensitive JSON file is masked on its own, since a process
// prints a key more often than its file; one of a plain file is not.
func TestEngine_MasksTheValuesInsideAFile(t *testing.T) {
	r := newRig(t, time.Hour, 0, withWitness(&witness{}))
	r.setup()
	if !r.masked("key=a-long-secret-key") {
		t.Error("the secret key inside process.json is not masked on its own")
	}
	if r.masked("a-plain-region") {
		t.Error("a value of a plain file is masked")
	}
	r.tickFor(50 * time.Minute) // past two refreshes
	if said := r.linesWith("process.json holds a value shorter than"); said != 1 {
		t.Errorf("a value too short to mask was said %d times; want once", said)
	}
}

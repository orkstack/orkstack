package vault

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// steppedClock is the engine's clocks and the provider's, under the test's
// hand, so that hours of renewals run in no wall time.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Wall() time.Time     { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *steppedClock) Boot() time.Duration { return c.Wall().Sub(time.Unix(0, 0)) }
func (c *steppedClock) step(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// tokenVault answers logins and renewals as Vault's token store does, on the
// stepped clock: a life in whole seconds, at most what is left of the token's
// maximum. A real Vault is asked the same in real time by
// TestIntegration_VaultRenewsALoginToItsMaximum; hours of it cannot be.
type tokenVault struct {
	clock    *steppedClock
	ttl, max time.Duration // a max of 0 is a token renewed for as long as it is asked

	mu              sync.Mutex
	made            map[string]time.Time
	ends            map[string]time.Time
	renewals, stray int
	// close counts renewals asked less than Vault's rounding after another,
	// and not with it.
	close     int
	renewedAt time.Time
}

func (v *tokenVault) grant(token string) time.Duration {
	life, now := v.ttl, v.clock.Wall()
	if v.max > 0 {
		life = min(life, v.made[token].Add(v.max).Sub(now))
	}
	life = life.Truncate(time.Second)
	v.ends[token] = now.Add(life)
	return life
}

func (v *tokenVault) live(token string) bool {
	return token != "" && v.clock.Wall().Before(v.ends[token])
}

func (v *tokenVault) serve(f *fault) {
	answer := func(w http.ResponseWriter, token string, life time.Duration) {
		_, _ = fmt.Fprint(w, loginBody(token, int(life/time.Second)))
	}
	f.on(loginPath, func(w http.ResponseWriter, _ *http.Request) {
		v.mu.Lock()
		defer v.mu.Unlock()
		token := fmt.Sprintf("hvs.%d", len(v.made)+1)
		v.made[token] = v.clock.Wall()
		answer(w, token, v.grant(token))
	})
	f.on(renewPath, func(w http.ResponseWriter, r *http.Request) {
		v.mu.Lock()
		defer v.mu.Unlock()
		token := r.Header.Get("X-Vault-Token")
		if !v.live(token) {
			v.stray++
			w.WriteHeader(http.StatusForbidden)
			return
		}
		v.renewals++
		if gap := v.clock.Wall().Sub(v.renewedAt); gap > 0 && gap < renewalRounding {
			v.close++
		}
		v.renewedAt = v.clock.Wall()
		answer(w, token, v.grant(token))
	})
	f.on(revokePath, status(204))
}

// TestEngine_RenewsALoginToItsMaximumAndReplacesIt: with the real refresh
// engine, a login is renewed while that gives it more life and replaced when
// it does not, whichever of the two rules sees it: the provider's, which
// takes the whole first life again for a renewal, or the engine's, which
// wants more than was left. The login in use never runs out, no renewal is
// asked of one that has, and neither rule makes a loop of logins or renewals.
func TestEngine_RenewsALoginToItsMaximumAndReplacesIt(t *testing.T) {
	const run = 12 * time.Hour
	// The login and the credential that is it are each renewed at about a
	// third of their life, so a login renewed without end is asked for about
	// twice in each ten and a half minutes.
	for name, c := range map[string]struct {
		ttl, max              time.Duration
		minLogins, maxLogins  int
		minRenews, maxRenewal int
		apart                 bool
	}{
		// About every two hours, less what the last renewal could not give.
		"a maximum of four lives": {30 * time.Minute, 2 * time.Hour, 6, 9, 80, 150, false},
		// No renewal adds anything: a login each third of a life, and at most
		// a renewal asked before each.
		"a maximum of one life":   {30 * time.Minute, 30 * time.Minute, 24, 80, 0, 160, false},
		"a maximum a second over": {30 * time.Minute, 30*time.Minute + time.Second, 24, 80, 0, 160, false},
		// The credential is renewed a second and a half before its login, so
		// the login's renewal adds less than Vault's rounding.
		"renewed twice at once":    {30 * time.Minute, 0, 1, 1, 110, 150, true},
		"as long as it is renewed": {30 * time.Minute, 0, 1, 1, 110, 150, false},
	} {
		t.Run(name, func(t *testing.T) {
			clock := &steppedClock{now: time.Unix(1_000_000, 0)}
			f := newFault(t)
			v := &tokenVault{clock: clock, ttl: c.ttl, max: c.max, made: map[string]time.Time{}, ends: map[string]time.Time{}}
			v.serve(f)
			p := f.provider().(*vaultProvider)
			p.now = clock.Wall
			root := t.TempDir()
			t.Cleanup(func() { // a credential's directories are read-only, which keeps them from TempDir
				_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, _ error) error {
					if d != nil && d.IsDir() {
						_ = os.Chmod(path, 0o700)
					}
					return nil
				})
			})
			draws := 0
			unit := func() float64 {
				// The login's is drawn first, at its making, and sets it that
				// far behind the credential from then on.
				if draws++; c.apart && draws == 1 {
					return 0.51
				}
				return 0.5
			}
			engine, err := provider.NewEngine(provider.EngineConfig{
				Attempt:     provider.Attempt{Namespace: "payments", RunID: "ork-1", TaskID: "migrate", Attempt: 1},
				Grants:      map[string]provider.Grant{"app": {Provider: p, Principal: "app", Token: identity}},
				Credentials: []provider.CredentialSpec{{Name: "login", Grant: "app", Ref: parsed(t, p, provider.KindCredential, "")}},
				Root:        filepath.Join(root, "credentials"), Masker: logstream.NewMasker(), Clock: clock,
				Unit:  unit,
				Hooks: provider.Hooks{Stop: func(f provider.Failure) { t.Errorf("the task was stopped: %v", f) }},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(engine.Close)
			if err := engine.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := engine.Issue(context.Background()); err != nil {
				t.Fatal(err)
			}
			for end := clock.Boot() + run; clock.Boot() < end; {
				wait := engine.Tick(context.Background())
				engine.Check()
				v.mu.Lock()
				if latest := fmt.Sprintf("hvs.%d", len(v.made)); !v.live(latest) {
					t.Errorf("the login in use ran out at %s", clock.Wall())
				}
				v.mu.Unlock()
				clock.step(min(max(wait, 100*time.Millisecond), 10*time.Second))
			}
			v.mu.Lock()
			defer v.mu.Unlock()
			if (v.close >= 10) != c.apart {
				t.Errorf("%d renewals came close after another; the case is of them: %v", v.close, c.apart)
			}
			if n := len(v.made); n < c.minLogins || n > c.maxLogins {
				t.Errorf("%d logins in %s, want %d to %d", n, run, c.minLogins, c.maxLogins)
			}
			if v.renewals < c.minRenews || v.renewals > c.maxRenewal || v.stray != 0 {
				t.Errorf("%d renewals and %d of a login that had run out, want %d to %d and none", v.renewals, v.stray, c.minRenews, c.maxRenewal)
			}
		})
	}
}

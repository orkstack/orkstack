package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// mintWorld is a mint over the real local record store, with the run's
// describe and start reads the library's own start fixture answers.
type mintWorld struct {
	t       *testing.T
	dir     string
	store   *artifacts.RecordStore
	starts  *startWorld
	minter  *Minter
	adm     artifacts.Admission
	sha     string
	facts   ExecutionFacts
	allowed []string
	grants  map[string]artifacts.Grant
	now     time.Time
}

func newMintWorld(t *testing.T) *mintWorld {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	w := &mintWorld{t: t, dir: dir, store: store, allowed: []string{"sts"}, now: started.Add(time.Minute)}
	adm, _ := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	adm.NamespaceID, adm.OrkflowID, adm.Issuer, adm.TokenTTL = "ns_1", "of_1", "https://ork.example", 3600
	adm.Sets = []artifacts.IdentitySet{{Identities: []artifacts.AdmittedIdentity{{Name: "aws", Audience: "sts", TTLSeconds: 900, Refresh: "required"}}}}
	adm.Tasks["build"] = artifacts.AdmittedTask{Set: 1, Pool: "main", SpecSHA256: strings.Repeat("a", 64), IdentitySlots: 2, AttemptDeadline: 3600}
	w.adm, w.sha = adm, w.write(adm)
	w.starts = &startWorld{chains: map[string]artifacts.RecordRead[artifacts.Chain]{}, firstStart: map[string]string{}}
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
	w.facts = ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "e1", FirstExecutionID: "e1",
		Pending: []PendingAttempt{{ActivityID: "ork/build", Started: true, Attempt: 1, LastWorker: "orker-1", StartedAt: started}}}
	jti := 0
	w.minter = &Minter{Stores: store, Starts: w.starts,
		Describe: func(context.Context, string) (ExecutionFacts, error) { return w.facts, nil },
		Policy: func(context.Context, string, string) (artifacts.NamespacePolicy, error) {
			return artifacts.NamespacePolicy{Audiences: w.allowed, Grants: w.grants}, nil
		},
		Signer: func() (*jwt.Signer, error) { return signer, nil },
		Issuer: "https://ork.example", Deployment: "prod",
		TTL:            TTLBounds{Default: time.Hour, Min: 10 * time.Minute, Max: 12 * time.Hour},
		Skew:           time.Minute,
		RecordIssuance: true,
		Now:            func() time.Time { return w.now },
		NewJTI:         func() string { jti++; return strings.Repeat("0", 15) + string(rune('0'+jti)) },
	}
	return w
}

// write stores adm as the engine writes it, named by its bytes.
func (w *mintWorld) write(adm artifacts.Admission) string {
	raw, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		w.t.Fatal(err)
	}
	key, err := artifacts.AdmissionKey(adm.Namespace, adm.RunID, sha)
	if err != nil {
		w.t.Fatal(err)
	}
	path := filepath.Join(w.dir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		w.t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		w.t.Fatal(err)
	}
	return sha
}

func (w *mintWorld) request() MintRequest {
	return MintRequest{Namespace: "ns", RunID: "ork-1", TaskID: "build", Attempt: 1, TaskSpecSHA256: strings.Repeat("a", 64),
		AdmissionSHA256: w.sha, Tokens: []TokenAsk{{Name: "aws", Audience: "sts"}}}
}

func (w *mintWorld) mint(req MintRequest) ([]IssuedToken, error) {
	a, err := w.minter.Admit(context.Background(), req)
	if err != nil {
		return nil, err
	}
	issued, err := w.minter.Issue(context.Background(), a, req, Caller{Principal: "orker:orker-1", OrkerID: "orker-1"})
	return issued.Tokens, err
}

func reasonOf(err error) string {
	var m *MintError
	if errors.As(err, &m) {
		return m.Reason
	}
	return "not a MintError: " + errMessage(err)
}

func errMessage(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestMint_Grants: a request every step admits is answered one signed
// token per identity, its life the identity's own held to the bounds, and
// the record of what was issued names it.
func TestMint_Grants(t *testing.T) {
	w := newMintWorld(t)
	tokens, err := w.mint(w.request())
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if len(tokens) != 1 || tokens[0].Name != "aws" || tokens[0].ExpiresIn != 15*time.Minute || strings.Count(tokens[0].Token, ".") != 2 {
		t.Fatalf("tokens = %+v", tokens)
	}
	recs, _, err := w.store.ListIssued(context.Background(), artifacts.IssuedQuery{Namespace: "ns", RunID: "ork-1"})
	if err != nil || len(recs) != 1 || len(recs[0].Value.Tokens) != 1 || recs[0].Value.Tokens[0].JTI != tokens[0].JTI ||
		recs[0].Value.Tokens[0].Subject != "ork:namespace:ns:orkflow:deploy:task:build" {
		t.Fatalf("issued records = %+v, %v", recs, err)
	}
}

// TestMint_TagsWhatTheAdmissionPinned: the signed token carries the tags the
// run was admitted with, and a run admitted with none carries none.
func TestMint_TagsWhatTheAdmissionPinned(t *testing.T) {
	for name, tags := range map[string]map[string]artifacts.SessionTagging{
		"none":                      nil,
		"tags":                      {"sts": {}},
		"tags, one of them follows": {"sts": {Transitive: []string{"ork_task"}}},
	} {
		t.Run(name, func(t *testing.T) {
			w := newMintWorld(t)
			adm := w.adm
			adm.SessionTags = tags
			w.sha = w.write(adm)
			w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
			tokens, err := w.mint(w.request())
			if err != nil {
				t.Fatal(err)
			}
			payload, err := base64.RawURLEncoding.DecodeString(strings.Split(tokens[0].Token, ".")[1])
			if err != nil {
				t.Fatal(err)
			}
			var claims struct {
				Tags *struct {
					Principal  map[string][]string `json:"principal_tags"`
					Transitive []string            `json:"transitive_tag_keys"`
				} `json:"https://aws.amazon.com/tags"`
			}
			if err := json.Unmarshal(payload, &claims); err != nil {
				t.Fatal(err)
			}
			if tags == nil {
				if claims.Tags != nil {
					t.Fatalf("an untagged run's token carries %+v", claims.Tags)
				}
				return
			}
			if claims.Tags == nil || claims.Tags.Principal["ork_task"][0] != "build" || claims.Tags.Principal["ork_namespace"][0] != "ns" ||
				claims.Tags.Principal["ork_namespace_id"][0] != "ns_1" || claims.Tags.Transitive == nil ||
				!slices.Equal(claims.Tags.Transitive, tags["sts"].Transitive) {
				t.Fatalf("the token carries %s", payload)
			}
		})
	}
}

// TestMint_Refuses: each step's refusal, and each step that could not ask
// answering unavailable, issue nothing.
func TestMint_Refuses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*mintWorld, *MintRequest)
		want  string
		audit bool
	}{
		{"no admission record", func(w *mintWorld, r *MintRequest) { r.AdmissionSHA256 = otherSHA }, ReasonNotAdmitted, true},
		{"another namespace", func(w *mintWorld, r *MintRequest) { r.Namespace = "other" }, ReasonNotAdmitted, true},
		{"a task not admitted", func(w *mintWorld, r *MintRequest) { r.TaskID = "deploy" }, ReasonNotAdmitted, true},
		{"an identity not declared", func(w *mintWorld, r *MintRequest) { r.Tokens[0].Audience = "vault" }, ReasonNotAdmitted, true},
		{"a grant not admitted", func(w *mintWorld, r *MintRequest) { r.Tokens[0].Name = "grant:deployer" }, ReasonNotAdmitted, true},
		{"an item of a task that is no fan-out", func(w *mintWorld, r *MintRequest) { r.Item = artifacts.Item{Index: 1, Set: true} }, ReasonNotAdmitted, true},
		{"one identity twice", func(w *mintWorld, r *MintRequest) { r.Tokens = append(r.Tokens, r.Tokens[0]) }, ReasonNotAdmitted, true},
		{"too many tokens", func(w *mintWorld, r *MintRequest) { r.Tokens = make([]TokenAsk, MaxMintTokens+1) }, ReasonNotAdmitted, false},
		{"another Orker", func(w *mintWorld, r *MintRequest) { w.facts.Pending[0].LastWorker = "orker-2" }, ReasonAttemptNotHeld, false},
		{"another attempt", func(w *mintWorld, r *MintRequest) { r.Attempt = 2 }, ReasonAttemptNotHeld, false},
		{"a closed run", func(w *mintWorld, r *MintRequest) { w.facts.Open = false }, ReasonAttemptNotHeld, false},
		{"a describe that fails", func(w *mintWorld, r *MintRequest) {
			w.minter.Describe = func(context.Context, string) (ExecutionFacts, error) {
				return ExecutionFacts{}, errors.New("frontend down")
			}
		}, ReasonUnavailable, false},
		{"another start", func(w *mintWorld, r *MintRequest) {
			w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e0", AdmissionSHA256: w.sha})
		}, ReasonNotAdmitted, true},
		{"another task spec", func(w *mintWorld, r *MintRequest) { r.TaskSpecSHA256 = otherSHA }, ReasonTaskNotAsAdmitted, true},
		{"follows with no checker", func(w *mintWorld, r *MintRequest) {
			adm := w.adm
			adm.Follows = map[string][]artifacts.FollowRef{"build": {{TaskID: "approve", Kind: "awaited"}}}
			r.AdmissionSHA256 = w.write(adm)
			w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: r.AdmissionSHA256})
		}, ReasonEngineRefused, false},
		{"an audience no longer allowed", func(w *mintWorld, r *MintRequest) { w.allowed = []string{"other"} }, ReasonRevoked, true},
		{"a policy that cannot be read", func(w *mintWorld, r *MintRequest) {
			w.minter.Policy = func(context.Context, string, string) (artifacts.NamespacePolicy, error) {
				return artifacts.NamespacePolicy{}, errors.New("bucket unreachable")
			}
		}, ReasonUnavailable, false},
		{"another issuer", func(w *mintWorld, r *MintRequest) { w.minter.Issuer = "https://other.example" }, ReasonEngineRefused, false},
		{"a namespace that is gone", func(w *mintWorld, r *MintRequest) {
			w.minter.Policy = func(context.Context, string, string) (artifacts.NamespacePolicy, error) {
				return artifacts.NamespacePolicy{}, ErrNamespaceGone
			}
		}, ReasonRevoked, true},
		{"a run no longer executing", func(w *mintWorld, r *MintRequest) {
			w.minter.Describe = func(context.Context, string) (ExecutionFacts, error) { return ExecutionFacts{}, ErrNoSuchExecution }
		}, ReasonAttemptNotHeld, false},
		{"no signing key", func(w *mintWorld, r *MintRequest) {
			w.minter.Signer = func() (*jwt.Signer, error) { return nil, errors.New("no eligible key") }
		}, ReasonEngineRefused, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newMintWorld(t)
			req := w.request()
			tc.edit(w, &req)
			tokens, err := w.mint(req)
			var m *MintError
			if len(tokens) != 0 || !errors.As(err, &m) || m.Reason != tc.want || m.Audit != tc.audit {
				t.Fatalf("tokens = %d, err = %v (audit %v); want %s, audit %v", len(tokens), err, m != nil && m.Audit, tc.want, tc.audit)
			}
			if recs, _, _ := w.store.ListIssued(context.Background(), artifacts.IssuedQuery{Namespace: "ns", RunID: "ork-1"}); len(recs) != 0 {
				t.Fatalf("a refused mint left %d records of issue", len(recs))
			}
		})
	}
}

// grantWorld is a mint of a run admitted with grant deployer, which its task
// uses, and with grant other, which it does not.
func grantWorld(t *testing.T, edit func(*artifacts.Admission)) (*mintWorld, MintRequest) {
	t.Helper()
	w := newMintWorld(t)
	adm := w.adm
	adm.Grants = map[string]artifacts.GrantSelection{
		"deployer": {Provider: "aws", Principal: "role/deploy", Audience: "sts"},
		"other":    {Provider: "aws", Principal: "role/other", Audience: "sts"}}
	adm.Tasks = map[string]artifacts.AdmittedTask{
		"build": {Set: 1, Pool: "main", SpecSHA256: strings.Repeat("a", 64), IdentitySlots: 2, AttemptDeadline: 3600, Grants: []string{"deployer"}},
		"lint":  {Pool: "main", SpecSHA256: strings.Repeat("b", 64), IdentitySlots: 1, AttemptDeadline: 3600, Grants: []string{"other"}}}
	if edit != nil {
		edit(&adm)
	}
	w.adm, w.sha = adm, w.write(adm)
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
	w.allowed = nil // a grant's audience is its own, not one a task may ask for
	w.grants = map[string]artifacts.Grant{"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{"deploy", "platform/deploy"}, Origins: []string{"submit"}}}
	req := w.request()
	req.AdmissionSHA256 = w.sha
	req.Tokens = []TokenAsk{{Name: "grant:deployer", Audience: "sts"}}
	return w, req
}

// TestMint_AGrantsLogin: a task is issued a token for a login through a grant
// it uses, whose subject ends in the grant, with the run's own life, for the
// grant's audience though its namespace lists none.
func TestMint_AGrantsLogin(t *testing.T) {
	w, req := grantWorld(t, nil)
	tokens, err := w.mint(req)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if len(tokens) != 1 || tokens[0].Name != "grant:deployer" || tokens[0].Audience != "sts" || tokens[0].ExpiresIn != time.Hour {
		t.Fatalf("tokens = %+v", tokens)
	}
	recs, _, err := w.store.ListIssued(context.Background(), artifacts.IssuedQuery{Namespace: "ns", RunID: "ork-1"})
	if err != nil || len(recs) != 1 || len(recs[0].Value.Tokens) != 1 || recs[0].Value.Tokens[0].Name != "grant:deployer" ||
		recs[0].Value.Tokens[0].Subject != "ork:namespace:ns:orkflow:deploy:task:build:grant:deployer" {
		t.Fatalf("issued records = %+v, %v", recs, err)
	}

	// Beside an identity, each is its own token.
	w, req = grantWorld(t, nil)
	w.allowed = []string{"sts"}
	req.Tokens = append(req.Tokens, TokenAsk{Name: "aws", Audience: "sts"})
	if tokens, err = w.mint(req); err != nil || len(tokens) != 2 || tokens[0].Name != "aws" || tokens[1].Name != "grant:deployer" || tokens[0].Token == tokens[1].Token {
		t.Fatalf("an identity and a grant: %+v, %v", tokens, err)
	}
}

// TestMint_AGrantsLoginNamesItsGrantInTheTags: the one tag that is no claim's
// is the grant's own name, and a declared identity beside it has none.
func TestMint_AGrantsLoginNamesItsGrantInTheTags(t *testing.T) {
	w, req := grantWorld(t, func(a *artifacts.Admission) { a.SessionTags = map[string]artifacts.SessionTagging{"sts": {}} })
	w.allowed = []string{"sts"}
	req.Tokens = append(req.Tokens, TokenAsk{Name: "aws", Audience: "sts"})
	tokens, err := w.mint(req)
	if err != nil || len(tokens) != 2 {
		t.Fatalf("tokens = %+v, %v", tokens, err)
	}
	for _, token := range tokens {
		payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token.Token, ".")[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims struct {
			Tags struct {
				Principal map[string][]string `json:"principal_tags"`
			} `json:"https://aws.amazon.com/tags"`
		}
		if err := json.Unmarshal(payload, &claims); err != nil {
			t.Fatal(err)
		}
		want := map[string][]string{"grant:deployer": {"deployer"}}[token.Name]
		if got := claims.Tags.Principal["ork_grant"]; !slices.Equal(got, want) || claims.Tags.Principal["ork_task"] == nil {
			t.Errorf("%s: ork_grant = %v of %v, want %v", token.Name, got, claims.Tags.Principal, want)
		}
	}
}

// TestMint_RefusesAGrant: a grant the task does not use, or the run was not
// admitted with, or that was admitted for another audience is not admitted;
// one the namespace's policy no longer gives as it did is revoked. Nothing is
// issued, whatever else the request asks.
func TestMint_RefusesAGrant(t *testing.T) {
	shared := func(a *artifacts.Admission) { a.OrkflowNamespace = "platform" }
	grant := func(edit func(*artifacts.Grant)) func(*mintWorld, *MintRequest) {
		return func(w *mintWorld, _ *MintRequest) {
			g := w.grants["deployer"]
			edit(&g)
			w.grants = map[string]artifacts.Grant{"deployer": g}
		}
	}
	for _, tc := range []struct {
		name      string
		admission func(*artifacts.Admission)
		edit      func(*mintWorld, *MintRequest)
		want      string
	}{
		{"one the run has not", nil, func(_ *mintWorld, r *MintRequest) { r.Tokens[0].Name = "grant:admin" }, ReasonNotAdmitted},
		{"one another task uses", nil, func(_ *mintWorld, r *MintRequest) { r.Tokens[0].Name = "grant:other" }, ReasonNotAdmitted},
		{"another audience", nil, func(_ *mintWorld, r *MintRequest) { r.Tokens[0].Audience = "vault" }, ReasonNotAdmitted},
		{"twice", nil, func(_ *mintWorld, r *MintRequest) { r.Tokens = append(r.Tokens, r.Tokens[0]) }, ReasonNotAdmitted},
		{"no name", nil, func(_ *mintWorld, r *MintRequest) { r.Tokens[0].Name = "grant:" }, ReasonNotAdmitted},
		{"beside one that is admitted", nil, func(_ *mintWorld, r *MintRequest) {
			r.Tokens = append(r.Tokens, TokenAsk{Name: "grant:other", Audience: "sts"})
		}, ReasonNotAdmitted},
		{"gone", nil, func(w *mintWorld, _ *MintRequest) { w.grants = nil }, ReasonRevoked},
		{"another principal", nil, grant(func(g *artifacts.Grant) { g.Principal = "role/admin" }), ReasonRevoked},
		{"another provider", nil, grant(func(g *artifacts.Grant) { g.Provider = "gcp" }), ReasonRevoked},
		{"no longer listed", nil, grant(func(g *artifacts.Grant) { g.Orkflows = []string{"report"} }), ReasonRevoked},
		{"another start", nil, grant(func(g *artifacts.Grant) { g.Origins = []string{"trigger"} }), ReasonRevoked},
		{"a shared Orkflow under *", shared, grant(func(g *artifacts.Grant) { g.Orkflows = []string{"*", "deploy", "ns/deploy"} }), ReasonRevoked},
		{"a policy that cannot be read", nil, func(w *mintWorld, _ *MintRequest) {
			w.minter.Policy = func(context.Context, string, string) (artifacts.NamespacePolicy, error) {
				return artifacts.NamespacePolicy{}, errors.New("bucket unreachable")
			}
		}, ReasonUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, req := grantWorld(t, tc.admission)
			tc.edit(w, &req)
			tokens, err := w.mint(req)
			if len(tokens) != 0 || reasonOf(err) != tc.want {
				t.Fatalf("tokens = %d, err = %v; want %s", len(tokens), err, tc.want)
			}
			if recs, _, _ := w.store.ListIssued(context.Background(), artifacts.IssuedQuery{Namespace: "ns", RunID: "ork-1"}); len(recs) != 0 {
				t.Fatalf("a refused mint left %d records of issue", len(recs))
			}
		})
	}
	for name, edit := range map[string]func(*mintWorld, *MintRequest){
		"every Orkflow of its own": grant(func(g *artifacts.Grant) { g.Orkflows = []string{"*"} }),
		"every start":              grant(func(g *artifacts.Grant) { g.Origins = nil }),
		"not the current version":  grant(func(g *artifacts.Grant) { g.CurrentOnly = true }),
	} {
		w, req := grantWorld(t, nil)
		edit(w, &req)
		if tokens, err := w.mint(req); err != nil || len(tokens) != 1 {
			t.Errorf("%s: %d tokens, %v", name, len(tokens), err)
		}
	}
	w, req := grantWorld(t, shared)
	if tokens, err := w.mint(req); err != nil || len(tokens) != 1 {
		t.Errorf("a shared Orkflow listed by its owner: %d tokens, %v", len(tokens), err)
	}
}

// TestMint_AttemptCap: an attempt beyond the task's identity slots is
// refused once the others hold them, and one that outlived its deadline is
// refused; the same attempt asking again holds its slot.
func TestMint_AttemptCap(t *testing.T) {
	w := newMintWorld(t)
	for attempt := int32(1); attempt <= 3; attempt++ {
		w.facts.Pending[0].Attempt = attempt
		w.facts.Pending[0].StartedAt = started.Add(time.Duration(attempt) * time.Second)
		req := w.request()
		req.Attempt = attempt
		_, err := w.mint(req)
		switch {
		case attempt <= 2 && err != nil:
			t.Fatalf("attempt %d: %v", attempt, err)
		case attempt == 3 && reasonOf(err) != ReasonAttemptsExhausted:
			t.Fatalf("attempt 3 past two slots: %v", err)
		}
	}
	w.facts.Pending[0].Attempt, w.facts.Pending[0].StartedAt = 2, started.Add(2*time.Second)
	req := w.request()
	req.Attempt = 2
	if _, err := w.mint(req); err != nil {
		t.Fatalf("the same attempt again: %v", err)
	}
	w.now = started.Add(3 * time.Hour)
	if _, err := w.mint(req); reasonOf(err) != ReasonAttemptExpired {
		t.Fatalf("past the deadline: %v", err)
	}
}

// TestMint_IssuanceOff: with identity.issuance.records off a mint writes
// no record; required, a record that cannot be written issues nothing.
func TestMint_IssuanceOff(t *testing.T) {
	w := newMintWorld(t)
	w.minter.RecordIssuance = false
	if _, err := w.mint(w.request()); err != nil {
		t.Fatal(err)
	}
	if recs, _, _ := w.store.ListIssued(context.Background(), artifacts.IssuedQuery{Namespace: "ns", RunID: "ork-1"}); len(recs) != 0 {
		t.Fatalf("records off wrote %d", len(recs))
	}
}

// TestMint_APolicyOutageSpendsNoSlot: the policy is read before the attempt
// cap's slot is taken, so an outage refuses unavailable and the next ask of
// a task with one slot is granted.
func TestMint_APolicyOutageSpendsNoSlot(t *testing.T) {
	w := newMintWorld(t)
	adm := w.adm
	row := adm.Tasks["build"]
	row.IdentitySlots = 1
	adm.Tasks["build"] = row
	w.sha = w.write(adm)
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
	good := w.minter.Policy
	w.minter.Policy = func(context.Context, string, string) (artifacts.NamespacePolicy, error) {
		return artifacts.NamespacePolicy{}, errors.New("bucket unreachable")
	}
	if _, err := w.mint(w.request()); reasonOf(err) != ReasonUnavailable {
		t.Fatalf("outage: %v", err)
	}
	w.minter.Policy = good
	w.facts.Pending[0].Attempt, w.facts.Pending[0].StartedAt = 2, started.Add(time.Second)
	req := w.request()
	req.Attempt = 2
	if _, err := w.mint(req); err != nil {
		t.Fatalf("the next attempt after an outage: %v", err)
	}
}

// TestMintError_Public: what a caller is told is the reason and a fixed
// sentence, never a store's key, a cluster's error or the run's shape;
// TASK_NOT_AS_ADMITTED alone keeps its detail of members and builds.
func TestMintError_Public(t *testing.T) {
	secret := "s3://bucket/metadata/snapshots/ns/ork-1/identity/admission-x.json"
	for _, reason := range []string{ReasonUnavailable, ReasonEngineRefused, ReasonNotAdmitted, ReasonAttemptNotHeld,
		ReasonAttemptsExhausted, ReasonAttemptExpired, ReasonRevoked} {
		got := (&MintError{Reason: reason, Message: secret}).Public()
		if !strings.HasPrefix(got, reason+": ") || strings.Contains(got, secret) {
			t.Errorf("%s: Public() = %q", reason, got)
		}
	}
	if got := (&MintError{Reason: ReasonTaskNotAsAdmitted, Message: "differs in: env"}).Public(); !strings.Contains(got, "differs in: env") {
		t.Errorf("TASK_NOT_AS_ADMITTED lost its detail: %q", got)
	}
}

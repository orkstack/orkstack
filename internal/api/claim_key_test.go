package api_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Whose an idempotency key is: docs/AUTHENTICATION.md, "Idempotency keys".

const (
	keyAlice = "user:corp:alice"
	keyNina  = "user:corp:nina"
)

func asKeyCaller(mode, id string) context.Context {
	ctx := authn.WithMode(context.Background(), mode)
	if id == "" {
		return ctx
	}
	return authn.WithPrincipal(ctx, &authn.Principal{Kind: authn.KindUser, ID: id, Issuer: "corp", Method: authn.MethodJWT})
}

// keyDeployment is one deployment's mode record, whose mode and clock a test
// moves: alice reads and submits, nina submits and reads nothing.
type keyDeployment struct {
	t     *testing.T
	h     *keyedHarness
	modes *artifacts.ModeStore
	now   time.Time
}

func newKeyDeployment(t *testing.T) *keyDeployment {
	t.Helper()
	return &keyDeployment{t: t, h: newKeyedHarness(t), modes: artifacts.NewModeStoreOn(artifacts.NewMemoryCAS()), now: time.Now()}
}

// in restarts the engine in mode.
func (d *keyDeployment) in(mode string) {
	d.t.Helper()
	if mode == authn.ModeAsserted {
		d.h.svc.SetAccessForTest(nil)
		return
	}
	store := artifacts.NewAccessStoreOn(artifacts.NewMemoryCAS())
	if _, err := store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1,
		Roles: []*orkstackv1.Role{{Name: "submits", Permissions: []string{string(authz.PermRunSubmit)}}},
		Bindings: []*orkstackv1.Binding{
			{Name: "alice", Role: authz.RoleOperator, Scope: "deployment", Subjects: []string{keyAlice}},
			{Name: "nina", Role: "submits", Scope: "deployment", Subjects: []string{keyNina}},
		}}, nil, artifacts.AccessWrite{At: time.Now()}); err != nil {
		d.t.Fatal(err)
	}
	guard := &api.ModeGuard{Store: d.modes, Mode: mode, ClaimTTL: time.Hour, Now: func() time.Time { return d.now }}
	if err := guard.Boot(context.Background()); err != nil {
		d.t.Fatalf("boot in %s: %v", mode, err)
	}
	d.h.svc.SetAccessForTest(&api.AccessControl{Mode: mode, ModeRecord: guard, Policies: api.NewPolicyCache(store, api.PolicyCacheOptions{})})
}

// lower writes the record as `orkctl admin auth lower` does.
func (d *keyDeployment) lower(mode string) {
	d.t.Helper()
	rec, etag, err := d.modes.Get(context.Background())
	if err != nil {
		d.t.Fatal(err)
	}
	if rec.Mode == artifacts.AuthModeEnforce {
		rec.LeftEnforceAt = d.now
	}
	rec.Mode = mode
	if _, err := d.modes.Put(context.Background(), rec, artifacts.PutConditions{IfMatch: etag}); err != nil {
		d.t.Fatal(err)
	}
}

// pass moves the clock past one claim's life and its grace.
func (d *keyDeployment) pass() { d.now = d.now.Add(time.Hour + 6*time.Minute) }

func (d *keyDeployment) run(ctx context.Context, key, url string) string {
	d.t.Helper()
	resp, err := d.h.submitIn(ctx, key, url)
	if err != nil {
		d.t.Fatalf("submit under %q: %v", key, err)
	}
	return resp.GetRun().GetId()
}

// TestOwnKeys_AnotherCallersKeyNeitherReturnsNorBlocksARun: in enforce mode,
// two callers' same key is two keys, whether the submission is the same one
// or another; each caller's retry is answered with its own run.
func TestOwnKeys_AnotherCallersKeyNeitherReturnsNorBlocksARun(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	d.pass()
	alice, nina := asKeyCaller(authn.ModeEnforce, keyAlice), asKeyCaller(authn.ModeEnforce, keyNina)
	first := d.run(alice, "nightly", "https://a")
	same := d.run(nina, "nightly", "https://a")
	if same == first {
		t.Fatalf("nina's submission under alice's key was answered with alice's run %s", first)
	}
	if got := d.run(alice, "nightly", "https://a"); got != first {
		t.Errorf("alice's retry = %s, want her run %s", got, first)
	}
	if got := d.run(nina, "nightly", "https://a"); got != same {
		t.Errorf("nina's retry = %s, want her run %s", got, same)
	}
	// A key somebody took first for another submission blocks nobody else's.
	taken := d.run(nina, "deploy", "https://taken")
	if got := d.run(alice, "deploy", "https://wanted"); got == taken {
		t.Errorf("alice's deploy is nina's run")
	}
	if _, err := d.h.submitIn(nina, "deploy", "https://other"); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Errorf("nina's own key for another submission = %v, want already_exists", err)
	}
}

// TestOwnKeys_ASharedClaimIsReadForOneClaimsLifeAfterARaise: a key claimed
// before the deployment enforced is answered, to a caller who may read its
// run and to nobody else, and only while such a claim can still be alive.
func TestOwnKeys_ASharedClaimIsReadForOneClaimsLifeAfterARaise(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeAsserted)
	before := d.run(context.Background(), "nightly", "https://a")
	d.in(authn.ModeEnforce)
	alice, nina := asKeyCaller(authn.ModeEnforce, keyAlice), asKeyCaller(authn.ModeEnforce, keyNina)
	if got := d.run(alice, "nightly", "https://a"); got != before {
		t.Errorf("alice's retry across the raise = %s, want %s", got, before)
	}
	started := len(d.h.starter.startedRuns())
	if _, err := d.h.submitIn(nina, "nightly", "https://a"); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("nina, who may not read the run = %v, want permission_denied", err)
	}
	if _, err := d.h.submitIn(alice, "nightly", "https://b"); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Errorf("another submission under the shared claim = %v, want already_exists", err)
	}
	if got := len(d.h.starter.startedRuns()); got != started {
		t.Errorf("%d runs started by refused submissions", got-started)
	}
	d.pass()
	if got := d.run(nina, "nightly", "https://a"); got == before {
		t.Error("past one claim's life the shared claim was still read")
	}
}

// TestOwnKeys_ASharedClaimThatNeverStartedIsStartedForItsReader: the id a
// key names is started by the caller who may read it, as that caller, and
// not for one who may not.
func TestOwnKeys_ASharedClaimThatNeverStartedIsStartedForItsReader(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeAsserted)
	id := d.h.strand(t, "nightly", "https://a")
	d.in(authn.ModeEnforce)
	if _, err := d.h.submitIn(asKeyCaller(authn.ModeEnforce, keyNina), "nightly", "https://a"); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("nina = %v, want permission_denied", err)
	}
	if n := len(d.h.starter.startedRuns()); n != 0 {
		t.Fatalf("a caller who may not read the run started %d", n)
	}
	resp, err := d.h.submitIn(asKeyCaller(authn.ModeEnforce, keyAlice), "nightly", "https://a")
	if err != nil || resp.GetRun().GetId() != id {
		t.Fatalf("alice = %v, %v; want %s started", resp.GetRun().GetId(), err, id)
	}
	if got := resp.GetRun().GetOrigin(); got.GetPrincipal() != keyAlice || !got.GetPrincipalVerified() {
		t.Errorf("the run's origin is %v, want alice, verified", got)
	}
}

// TestOwnKeys_AnObservingEngineReadsACallersClaim: after the deployment
// leaves enforce, a caller whose credential verifies is answered its own
// claim for one claim's life; a request with no verified caller is not, and
// new claims are the namespace's.
func TestOwnKeys_AnObservingEngineReadsACallersClaim(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	mine := d.run(asKeyCaller(authn.ModeEnforce, keyAlice), "nightly", "https://a")
	d.lower(artifacts.AuthModeObserve)
	d.in(authn.ModeObserve)
	if got := d.run(asKeyCaller(authn.ModeObserve, keyAlice), "nightly", "https://a"); got != mine {
		t.Errorf("alice's retry in observe = %s, want %s", got, mine)
	}
	shared := d.run(asKeyCaller(authn.ModeObserve, ""), "nightly", "https://a")
	if shared == mine {
		t.Error("a request nobody verified was answered a principal's claim")
	}
	d.pass()
	if got := d.run(asKeyCaller(authn.ModeObserve, keyNina), "nightly", "https://a"); got != shared {
		t.Errorf("in observe a key is the namespace's: got %s, want %s", got, shared)
	}
}

// TestOwnKeys_AnUnreadOtherClaimStartsNothing: whether the key was used
// before the mode changed is not known, so nothing is started.
func TestOwnKeys_AnUnreadOtherClaimStartsNothing(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	d.h.claimer.lookupErrAt = map[string]error{claimKeyIn(artifacts.DefaultNamespace, "nightly"): errors.New("the frontend did not answer")}
	_, err := d.h.submitIn(asKeyCaller(authn.ModeEnforce, keyAlice), "nightly", "https://a")
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("err = %v, want unavailable", err)
	}
	if n, p := len(d.h.starter.startedRuns()), d.h.claimer.proposed; n != 0 || p != 0 {
		t.Errorf("%d runs started and %d claims made", n, p)
	}
	// Past the window the other coordinate is not asked.
	d.pass()
	d.run(asKeyCaller(authn.ModeEnforce, keyAlice), "nightly", "https://a")
}

// TestOwnKeys_NoCallerNoKey: an enforcing engine binds a key to no shared
// coordinate when a request reaches it with nobody verified.
func TestOwnKeys_NoCallerNoKey(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	if _, err := d.h.submitIn(asKeyCaller(authn.ModeEnforce, ""), "nightly", ""); connect.CodeOf(err) != connect.CodeInternal {
		t.Errorf("err = %v, want internal", err)
	}
	if d.h.claimer.proposed != 0 {
		t.Error("a claim was made")
	}
}

// aliceKey and ninaKey are the owned coordinates of a key.
func aliceKey(key string) string {
	return `{"v":1,"namespace":"` + artifacts.DefaultNamespace + `","principal":"` + keyAlice + `","key":"` + key + `"}`
}

// TestOwnKeys_TheCallersOwnClaimWins: where a caller's own claim and the
// namespace's shared one both stand, the caller is answered its own, in every
// mode that reads both; and where its own cannot be read nothing is answered.
func TestOwnKeys_TheCallersOwnClaimWins(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	alice := asKeyCaller(authn.ModeEnforce, keyAlice)
	mine := d.run(alice, "nightly", "https://a")
	d.lower(artifacts.AuthModeObserve)
	d.in(authn.ModeObserve)
	shared := d.run(asKeyCaller(authn.ModeObserve, ""), "nightly", "https://a")
	if shared == mine {
		t.Fatal("both claims are one")
	}
	if got := d.run(asKeyCaller(authn.ModeObserve, keyAlice), "nightly", "https://a"); got != mine {
		t.Errorf("in observe alice was answered %s, want her own %s", got, mine)
	}
	d.in(authn.ModeEnforce)
	if got := d.run(alice, "nightly", "https://a"); got != mine {
		t.Errorf("in enforce alice was answered %s, want her own %s", got, mine)
	}
	d.h.claimer.lookupErrAt = map[string]error{aliceKey("nightly"): errors.New("no answer")}
	if resp, err := d.h.submitIn(alice, "nightly", "https://a"); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("with her own claim unread alice was answered %v, %v; want unavailable", resp.GetRun().GetId(), err)
	}
}

// TestOwnKeys_ARecordThatForgotEnforceIsNotBelieved: a mode record that was
// deleted or forced, and so holds no time the deployment left enforce, still
// has a caller's own claim read for one claim's life after it was written.
func TestOwnKeys_ARecordThatForgotEnforceIsNotBelieved(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	mine := d.run(asKeyCaller(authn.ModeEnforce, keyAlice), "nightly", "https://a")
	d.modes = artifacts.NewModeStoreOn(artifacts.NewMemoryCAS()) // the record is gone
	d.in(authn.ModeObserve)
	if got := d.run(asKeyCaller(authn.ModeObserve, keyAlice), "nightly", "https://a"); got != mine {
		t.Errorf("alice's retry = %s, want %s", got, mine)
	}
}

// TestOwnKeys_AGuessAboutAnothersClaimSaysNothing: a caller who may not read
// a shared claim's run is refused alike whether its submission is the claim's
// or another, so the answer tells it nothing of what was submitted.
func TestOwnKeys_AGuessAboutAnothersClaimSaysNothing(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeAsserted)
	d.run(context.Background(), "nightly", "https://a")
	d.in(authn.ModeEnforce)
	nina := asKeyCaller(authn.ModeEnforce, keyNina)
	_, right := d.h.submitIn(nina, "nightly", "https://a")
	_, wrong := d.h.submitIn(nina, "nightly", "https://b")
	if connect.CodeOf(right) != connect.CodePermissionDenied || right == nil || wrong == nil || right.Error() != wrong.Error() {
		t.Errorf("the same submission = %v; another = %v; want one answer", right, wrong)
	}
}

// TestOwnKeys_AClaimWithNoFingerprintIsItsReaders: a claim v0.3.0 made is
// answered to a caller who may read its run, never started, and refused to
// one who may not.
func TestOwnKeys_AClaimWithNoFingerprintIsItsReaders(t *testing.T) {
	d := newKeyDeployment(t)
	d.in(authn.ModeEnforce)
	d.h.claimer.bind(d.h.key("k"), &orkstackv1.Run{Id: "ork-legacy", Namespace: artifacts.DefaultNamespace, OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_PENDING})
	if got := d.run(asKeyCaller(authn.ModeEnforce, keyAlice), "k", "https://a"); got != "ork-legacy" {
		t.Errorf("alice was answered %s", got)
	}
	if _, err := d.h.submitIn(asKeyCaller(authn.ModeEnforce, keyNina), "k", "https://a"); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("nina = %v, want permission_denied", err)
	}
	if n := len(d.h.starter.startedRuns()); n != 0 {
		t.Errorf("%d runs started", n)
	}
}

// TestOwnKeys_AKeyIsValidUTF8: two keys of different bytes are never one
// coordinate, so a key that is not valid UTF-8 is refused, in every mode.
func TestOwnKeys_AKeyIsValidUTF8(t *testing.T) {
	for _, mode := range []string{authn.ModeAsserted, authn.ModeObserve, authn.ModeEnforce} {
		d := newKeyDeployment(t)
		d.in(mode)
		if _, err := d.h.submitIn(asKeyCaller(mode, keyAlice), "caf\xe9", ""); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v, want invalid_argument", mode, err)
		}
		if d.h.claimer.proposed != 0 {
			t.Errorf("%s: a claim was made", mode)
		}
	}
}

// TestTriggerInvoke_AKeyIsTheTriggers: through the door, in enforce: two
// triggers' same key is two keys; a claim made before the raise is answered
// to the trigger that made it and is 403 to another, starting nothing; and a
// key that is not valid UTF-8 is 400.
func TestTriggerInvoke_AKeyIsTheTriggers(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	one, oneSecret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	two, twoSecret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	before := map[string]string{"Idempotency-Key": "before"}
	if status, body := f.invoke(t, one.GetId(), bearer(oneSecret), `{}`, before); status != http.StatusCreated {
		t.Fatalf("before the raise = %d: %s", status, body)
	}
	d := &keyDeployment{t: t, h: &keyedHarness{svc: f.svc}, modes: artifacts.NewModeStoreOn(artifacts.NewMemoryCAS()), now: time.Now()}
	d.in(authn.ModeEnforce)
	if status, body := f.invoke(t, one.GetId(), bearer(oneSecret), `{}`, before); status != http.StatusCreated {
		t.Errorf("the trigger's own retry = %d: %s", status, body)
	}
	if status, body := f.invoke(t, two.GetId(), bearer(twoSecret), `{}`, before); status != http.StatusForbidden || triggerErrorCode(t, body) != "permission_denied" {
		t.Errorf("another trigger's = %d: %s; want 403 permission_denied", status, body)
	}
	if n := len(f.starter.startedRuns()); n != 1 {
		t.Errorf("%d runs started, want one", n)
	}
	after := map[string]string{"Idempotency-Key": "after"}
	for _, tr := range []struct{ id, secret string }{{one.GetId(), oneSecret}, {two.GetId(), twoSecret}} {
		if status, body := f.invoke(t, tr.id, bearer(tr.secret), `{}`, after); status != http.StatusCreated {
			t.Errorf("%s = %d: %s", tr.id, status, body)
		}
	}
	if n := len(f.starter.startedRuns()); n != 3 {
		t.Errorf("%d runs started, want three: two triggers' same key is two keys", n)
	}
	if status, body := f.invoke(t, one.GetId(), bearer(oneSecret), `{}`, map[string]string{"Idempotency-Key": "caf\xe9"}); status != http.StatusBadRequest {
		t.Errorf("a key that is not UTF-8 = %d: %s; want 400", status, body)
	}
}

package api

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// toldStarter counts what a run is told.
type toldStarter struct {
	engine.RunStarter
	told []*orkstackv1.AwaitSignal
}

func (s *toldStarter) SignalAwait(_ context.Context, _ string, sig *orkstackv1.AwaitSignal) error {
	s.told = append(s.told, sig)
	return nil
}

// TestAwaitDoor_WhoMayLearnAResolution: where callers are authenticated, a
// caller who may resolve and not read the run learns neither who resolved an
// await nor, by guessing, how: only the principal on record, the same
// callback and a reader of the run are told that their answer repeats it, and
// a redeliver tells what was decided to a reader alone.
func TestAwaitDoor_WhoMayLearnAResolution(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	const api, callback = orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK
	sha := strings.Repeat("a", 64)
	// put records a rejection of await task, given by via and by principal.
	put := func(task, principal string, via orkstackv1.AwaitResolvedVia) *artifacts.RecordedFound {
		t.Helper()
		detail := &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: "ork-1", TaskId: task, Via: via, Reason: "too risky"}
		if principal != "" {
			detail.Principal, detail.PrincipalVerified, detail.PrincipalIssuer = principal, true, "corp"
		}
		d, err := store.WriteResolutionDetail(ctx, detail)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.WriteRecordedValues(ctx, &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
			Namespace: "default", RunId: "ork-1", TaskId: task, AdmissionSha256: sha, InputBinding: make([]byte, 32), Rejected: true, DetailSha256: d}); err != nil {
			t.Fatal(err)
		}
		key, _ := artifacts.RecordKey(artifacts.RecordResolution, "default", "ork-1", task, 0)
		read, err := store.ReadRecordedValues(ctx, artifacts.SlotDoor, nil, key, artifacts.RecordWant{
			Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: "default", RunID: "ork-1", TaskID: task, AdmissionSHA256: sha})
		if err != nil || read.Answer != artifacts.RecordFound {
			t.Fatalf("read %+v, %v", read, err)
		}
		return read.Value
	}
	byPerson, byCallback := put("gate", "user:corp:nina", api), put("hook", "", callback)

	s := &Service{logger: slog.Default(), now: time.Now, clockSkew: time.Minute, awaitChecks: &identity.FollowsCheck{Store: store}}
	s.access, _ = accessFor(t, authn.ModeEnforce,
		[]*orkstackv1.Role{{Name: "resolver", Permissions: []string{"run.await.resolve"}}},
		&orkstackv1.Binding{Name: "resolvers", Role: "resolver", Scope: "namespace:default", Subjects: []string{"user:corp:nina", "user:corp:omar"}},
		&orkstackv1.Binding{Name: "viewers", Role: "viewer", Scope: "namespace:default", Subjects: []string{"user:corp:vera"}})
	as := func(id string) (context.Context, authn.RecordedPrincipal) {
		return authn.WithPrincipal(authn.WithMode(ctx, authn.ModeEnforce), &authn.Principal{Kind: authn.KindUser, ID: id, Issuer: "corp", Method: authn.MethodJWT}),
			authn.RecordedPrincipal{ID: id, Verified: true, Issuer: "corp", Method: authn.MethodJWT}
	}
	for _, tc := range []struct {
		name      string
		found     *artifacts.RecordedFound
		task, who string
		via       orkstackv1.AwaitResolvedVia
		reason    string
		redeliver bool
		repeat    bool
		names     bool
		decision  bool
	}{
		{name: "the principal on record repeats it", found: byPerson, task: "gate", who: "user:corp:nina", via: api, reason: "too risky", repeat: true, decision: true},
		{name: "another guesses it", found: byPerson, task: "gate", who: "user:corp:omar", via: api, reason: "too risky"},
		{name: "another answers otherwise", found: byPerson, task: "gate", who: "user:corp:omar", via: api, reason: "no"},
		{name: "a reader repeats it", found: byPerson, task: "gate", who: "user:corp:vera", via: api, reason: "too risky", repeat: true, decision: true},
		{name: "a reader answers otherwise", found: byPerson, task: "gate", who: "user:corp:vera", via: api, reason: "no", names: true},
		{name: "a callback repeats a person's", found: byPerson, task: "gate", via: callback, reason: "too risky"},
		{name: "the callback repeats its own", found: byCallback, task: "hook", via: callback, reason: "too risky", repeat: true, decision: true},
		{name: "a person repeats a callback's", found: byCallback, task: "hook", who: "user:corp:omar", via: api, reason: "too risky"},
		{name: "a resolver delivers it again", found: byPerson, task: "gate", who: "user:corp:omar", via: api, redeliver: true},
		{name: "a reader delivers it again", found: byPerson, task: "gate", who: "user:corp:vera", via: api, redeliver: true, decision: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &toldStarter{}
			reqCtx, who := authn.WithMode(ctx, authn.ModeEnforce), authn.RecordedPrincipal{}
			if tc.who != "" {
				reqCtx, who = as(tc.who)
			}
			d := &awaitDoor{s: s, sha: sha, key: t.Name(), at: time.Now(), instant: time.Now(), dated: true, deadline: time.Now().Add(time.Hour),
				clock: &artifacts.AnswerClock{}, adm: artifacts.Admission{Namespace: "default", RunID: "ork-1"}, resp: &orkstackv1.ResolveAwaitResponse{},
				r: awaitResolution{binding: engine.Binding{Starter: starter}, taskID: tc.task, reject: !tc.redeliver, reason: tc.reason, via: tc.via,
					redeliver: tc.redeliver, principal: who.ID, who: who,
					run: &orkstackv1.Run{Id: "ork-1", Namespace: "default", OrkflowName: "deploy"}}}
			resp, err := d.standing(reqCtx, tc.found)
			if len(starter.told) != 1 {
				t.Errorf("the run was told %d times; every request that meets a record tells it", len(starter.told))
			}
			if tc.repeat || tc.redeliver {
				if err != nil || resp.GetRepeat() != tc.repeat || resp.GetReject() != tc.decision {
					t.Fatalf("%v, %v; want repeat %v, its decision told %v", resp, err, tc.repeat, tc.decision)
				}
				return
			}
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), reasonAwaitAlreadyResolved) {
				t.Fatalf("%v, %v; want it refused", resp, err)
			}
			if strings.Contains(err.Error(), "nina") != tc.names || strings.Contains(err.Error(), "risky") {
				t.Fatalf("%v names who resolved it: want %v, and never why", err, tc.names)
			}
		})
	}
}

package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/schedules"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// TestJudgeResumedStarter: a resume of a run a schedule or a trigger started,
// that holds an identity, is judged by the binder the run's admission
// recorded, never by a schedule entry found from what the run submitted
// (which a floating version does not name): it resumes while that binder
// may still submit and its starter's consent for it now is to what the run's
// version asks for, and is refused when the binding no longer grants it,
// when the consent changed or was revoked, or when no binder was recorded.
func TestJudgeResumedStarter(t *testing.T) {
	ctx := context.Background()
	ns := &orkstackv1.Namespace{Name: "payments"}
	alice := &artifacts.AdmittedBinder{Principal: "user:corp:alice", Verified: true, Binding: artifacts.BinderBinding{
		Name: "deployers", Selector: "group:corp:deployers", Role: "submitter", Scope: "namespace:payments"}}
	probe, _ := starterWorld(t)
	asks, err := probe.asksFor(ctx, "payments", "deploy", "1")
	if err != nil {
		t.Fatal(err)
	}
	// The schedule floats: its entry names no version.
	entry := func(binder, digest string) artifacts.ScheduleBinding {
		return artifacts.ScheduleBinding{OrkflowName: "deploy", Binder: binder, BinderVerified: true, Consent: digest, ConsentAt: time.Now()}
	}
	other := "c1:" + strings.Repeat("0", 64)
	id, err := schedules.NewID()
	if err != nil {
		t.Fatal(err)
	}
	schedule := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, Id: id, Description: "nightly"}
	for _, tc := range []struct {
		name     string
		entries  []artifacts.ScheduleBinding
		admitted *artifacts.AdmittedBinder
		bindings []*orkstackv1.Binding
		reason   string
		code     connect.Code
	}{
		{"its binder may still submit, with its consent", []artifacts.ScheduleBinding{entry("user:corp:alice", asks.Digest)}, alice, []*orkstackv1.Binding{deployers}, "", 0},
		{"the newest entry by its binder is judged", []artifacts.ScheduleBinding{entry("user:corp:alice", other), entry("user:corp:alice", asks.Digest)}, alice, []*orkstackv1.Binding{deployers}, "", 0},
		{"its binding no longer grants it", []artifacts.ScheduleBinding{entry("user:corp:alice", asks.Digest)}, alice, nil, reasonStarterNotPermitted, connect.CodePermissionDenied},
		{"its consent changed", []artifacts.ScheduleBinding{entry("user:corp:alice", other)}, alice, []*orkstackv1.Binding{deployers}, reasonStarterNotConsented, connect.CodeFailedPrecondition},
		// A consent saved since by the same name, unverified, is no binder's.
		{"its consent is an unverified binder's", []artifacts.ScheduleBinding{func() artifacts.ScheduleBinding {
			e := entry("user:corp:alice", asks.Digest)
			e.BinderVerified = false
			return e
		}()}, alice, []*orkstackv1.Binding{deployers}, reasonStarterNotConsented, connect.CodeFailedPrecondition},
		{"its consent was revoked", []artifacts.ScheduleBinding{entry("user:corp:bob", asks.Digest)}, alice, []*orkstackv1.Binding{deployers}, reasonStarterNotConsented, connect.CodeFailedPrecondition},
		{"no binder was recorded", []artifacts.ScheduleBinding{entry("user:corp:alice", asks.Digest)}, nil, []*orkstackv1.Binding{deployers}, reasonStarterNotRecorded, connect.CodeFailedPrecondition},
	} {
		t.Run("schedule: "+tc.name, func(t *testing.T) {
			s, _ := starterWorld(t, tc.bindings...)
			store, err := artifacts.NewScheduleBindingStore(ctx, artifacts.Config{Backend: "local", LocalDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Update(ctx, "payments", id, func(b *artifacts.ScheduleBindings) error {
				b.Entries = tc.entries
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			s.scheduleBindings = store
			judged(t, s.judgeResumedStarter(ctx, ns, schedule, tc.admitted, "deploy", "1"), tc.reason, tc.code)
		})
	}
	bound := triggers.Binding{Binder: "user:corp:alice", Verified: true, BoundAt: time.Now(), Consent: asks.Digest, ConsentAt: time.Now()}
	for _, tc := range []struct {
		name       string
		binder     string
		unverified bool
		reason     string
	}{
		{"its binder may still submit, with its consent", "user:corp:alice", false, ""},
		// Bound since by another: alice's consent is no longer the trigger's.
		{"it was bound by another since", "user:corp:bob", false, reasonStarterNotConsented},
		{"it was bound since by an unverified alice", "user:corp:alice", true, reasonStarterNotConsented},
	} {
		t.Run("trigger: "+tc.name, func(t *testing.T) {
			s, store := starterWorld(t, deployers)
			b := bound
			b.Binder, b.Verified = tc.binder, !tc.unverified
			tr, _, err := store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "deploy", Binding: b})
			if err != nil {
				t.Fatal(err)
			}
			origin := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: tr.ID}
			judged(t, s.judgeResumedStarter(ctx, ns, origin, alice, "deploy", "1"), tc.reason, connect.CodeFailedPrecondition)
		})
	}
}

// judged asserts err is nil when reason is "", and otherwise a refusal of
// code that leads with reason.
func judged(t *testing.T, err error, reason string, code connect.Code) {
	t.Helper()
	if reason == "" {
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		return
	}
	if connect.CodeOf(err) != code || !strings.Contains(err.Error(), reason) {
		t.Fatalf("err = %v, want %s %s", err, code, reason)
	}
}

// TestAdmittedBinder: the binder judged is the one the run's admission
// recorded at submit; a run with no admission, or one not on record, has none.
func TestAdmittedBinder(t *testing.T) {
	ctx := context.Background()
	records, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: "local", LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	binder := &artifacts.AdmittedBinder{Principal: "user:corp:alice", Verified: true, Binding: artifacts.BinderBinding{Name: "deployers", Role: "submitter"}}
	sha, _, err := records.WriteAdmission(ctx, artifacts.Admission{RunID: "ork-r", Namespace: "default", Orkflow: "wf", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "schedule", ID: "s", Binder: binder}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(),
		EngineBuild: "b", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, Tasks: map[string]artifacts.AdmittedTask{}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{awaitRecords: records}
	base := func(sha string) *resumeBase {
		b := &resumeBase{namespace: "default", run: &orkstackv1.Run{Id: "ork-r"}, start: &engine.RunStartRecord{}}
		if sha != "" {
			b.start.Plan.Identity = &engine.IdentityPlan{AdmissionSHA256: sha}
		}
		return b
	}
	if got, err := s.admittedBinder(ctx, base(sha)); err != nil || got == nil || *got != *binder {
		t.Fatalf("recorded = %+v, %v", got, err)
	}
	if got, err := s.admittedBinder(ctx, base("")); err != nil || got != nil {
		t.Fatalf("no admission = %+v, %v", got, err)
	}
	if got, err := s.admittedBinder(ctx, base(strings.Repeat("e", 64))); err != nil || got != nil {
		t.Fatalf("an admission not on record = %+v, %v", got, err)
	}
}

// TestStarter_NoTriggerStore: an engine that keeps no triggers judges a
// trigger's run as one whose binder is not recorded, at a fire and at a
// resume alike, and never crashes.
func TestStarter_NoTriggerStore(t *testing.T) {
	ctx := context.Background()
	s, _ := starterWorld(t, deployers)
	s.triggers = nil
	ns := &orkstackv1.Namespace{Name: "payments"}
	origin := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg"}
	alice := &artifacts.AdmittedBinder{Principal: "user:corp:alice", Verified: true, Binding: artifacts.BinderBinding{
		Name: "deployers", Selector: "group:corp:deployers", Role: "submitter", Scope: "namespace:payments"}}
	msg := &orkstackv1.SubmitRunRequest{OrkflowName: "deploy", Namespace: "payments"}
	judged(t, s.checkStarter(ctx, ns, msg, origin), reasonStarterNotRecorded, connect.CodeFailedPrecondition)
	judged(t, s.judgeResumedStarter(ctx, ns, origin, alice, "deploy", "1"), reasonStarterNotConsented, connect.CodeFailedPrecondition)
}

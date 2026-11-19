package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// roleOrkflow is a version of "deploy" whose task holds an identity for
// audience. The DSL refuses `identities:` until task identity lands, so the
// definition is stored directly.
func roleOrkflow(version, audience string) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Namespace: "default", Name: "deploy", Version: version, Tasks: []*orkstackv1.Task{{
		Id: "apply", Image: &orkstackv1.ImageRef{Reference: "ghcr.io/acme/apply:1"},
		Identities: []*orkstackv1.IdentityRef{{Name: "tok", Audience: audience}},
	}}}
}

type consentHarness struct {
	*scheduleHarness
	orkflows *engine.MemoryOrkflowStore
	bindings *artifacts.ScheduleBindingStore
}

func newConsentHarness(t *testing.T) *consentHarness {
	t.Helper()
	orkflows := engine.NewMemoryOrkflowStore()
	bindings, err := artifacts.NewScheduleBindingStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	h := newScheduleHarnessWith(t, func(cfg *api.Config) { cfg.Orkflows, cfg.ScheduleBindings = orkflows, bindings })
	c := &consentHarness{scheduleHarness: h, orkflows: orkflows, bindings: bindings}
	c.put(t, roleOrkflow("1", "sts"))
	return c
}

func (h *consentHarness) put(t *testing.T, def *orkstackv1.Orkflow) {
	t.Helper()
	ctx := context.Background()
	_, _, err := h.orkflows.Put(ctx, def, artifacts.HeadExpectation{})
	must.OK(t, err)
	_, err = h.orkflows.SetCurrentVersion(ctx, "default", def.GetName(), def.GetVersion(), artifacts.CurrentExpectation{})
	must.OK(t, err)
}

func (h *consentHarness) get(t *testing.T, name string) *orkstackv1.Schedule {
	t.Helper()
	resp, err := h.svc.GetSchedule(context.Background(), connect.NewRequest(&orkstackv1.GetScheduleRequest{Name: name}))
	must.OK(t, err)
	return resp.Msg.GetSchedule()
}

func refusedForConsent(t *testing.T, err error, digest string) {
	t.Helper()
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "IDENTITY_CONSENT_MISMATCH") ||
		!strings.Contains(err.Error(), digest) {
		t.Fatalf("err = %v, want IDENTITY_CONSENT_MISMATCH handing out %s", err, digest)
	}
}

// TestScheduleConsent_ToTheDigestShown: a create of a schedule whose
// Orkflow asks for an identity is refused without a consent, and the
// refusal hands out the digest; a consent to that digest is recorded with
// its short list, and the schedule shows what it asks for, not stale.
// Nothing is created by the refusal.
func TestScheduleConsent_ToTheDigestShown(t *testing.T) {
	h := newConsentHarness(t)
	req := &orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "deploy", Spec: dailySpec()}
	_, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("create without a consent: err = %v", err)
	}
	digest := err.Error()[strings.Index(err.Error(), "--consent=")+len("--consent="):]
	digest = strings.TrimSuffix(digest, "`")
	if !consent.Valid(digest) || digest == consent.Empty() {
		t.Fatalf("the refusal hands out %q", digest)
	}
	if len(h.schedules.created) != 0 {
		t.Fatal("a refused create created a schedule")
	}
	req.Consent = consent.Empty()
	_, err = h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(req))
	refusedForConsent(t, err, digest)

	req.Consent = digest
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(req))
	must.OK(t, err)
	s := created.Msg.GetSchedule()
	if s.GetConsented().GetDigest() != digest || strings.Join(s.GetConsented().GetAudiences(), ",") != "sts" {
		t.Fatalf("consented = %v, want %s for audience sts", s.GetConsented(), digest)
	}
	if s.GetAsksFor().GetDigest() != digest || s.GetConsentStale() {
		t.Fatalf("asks_for = %v stale = %v", s.GetAsksFor(), s.GetConsentStale())
	}
}

// TestScheduleConsent_StaleWhenThePointerMoves: a schedule that follows
// the current pointer is stale once a version asking for more is current;
// a re-save that changes nothing it submits keeps the stale consent; a
// consent to the old digest is refused; one to the new digest is recorded
// and leaves the binder as it was; a rebind keeps the consent.
func TestScheduleConsent_StaleWhenThePointerMoves(t *testing.T) {
	h := newConsentHarness(t)
	first := mustConsentDigest(t, h)
	_, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "deploy", Spec: dailySpec(), Consent: first}))
	must.OK(t, err)

	h.put(t, roleOrkflow("2", "vault"))
	s := h.get(t, "nightly")
	if !s.GetConsentStale() || s.GetAsksFor().GetDigest() == first {
		t.Fatalf("after the pointer moved: stale = %v asks_for = %v", s.GetConsentStale(), s.GetAsksFor())
	}
	second := s.GetAsksFor().GetDigest()

	update := &orkstackv1.UpdateScheduleRequest{Name: "nightly", Spec: dailySpec(), Description: "re-saved"}
	_, err = h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(update))
	must.OK(t, err)
	if got := h.get(t, "nightly"); got.GetConsented().GetDigest() != first || !got.GetConsentStale() {
		t.Fatalf("a save that changes nothing changed the consent: %v", got.GetConsented())
	}

	update.Consent = first
	_, err = h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(update))
	refusedForConsent(t, err, second)
	if err != nil && !strings.Contains(err.Error(), "now also asks for audience vault") {
		t.Errorf("the refusal does not say what changed: %v", err)
	}

	update.Consent = second
	_, err = h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(update))
	must.OK(t, err)
	got := h.get(t, "nightly")
	if got.GetConsented().GetDigest() != second || got.GetConsentStale() || h.binder(t, got) != "user:corp:alice" {
		t.Fatalf("after consenting: consented = %v stale = %v binder = %q", got.GetConsented(), got.GetConsentStale(), h.binder(t, got))
	}

	_, err = h.svc.UpdateSchedule(as("user:corp:carol"), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", Spec: dailySpec(), Description: "re-saved", Rebind: true}))
	must.OK(t, err)
	got = h.get(t, "nightly")
	if h.binder(t, got) != "user:corp:carol" || got.GetConsented().GetDigest() != second {
		t.Fatalf("after a rebind: binder = %q consented = %v", h.binder(t, got), got.GetConsented())
	}
}

func mustConsentDigest(t *testing.T, h *consentHarness) string {
	t.Helper()
	_, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "probe", OrkflowName: "deploy", Spec: dailySpec()}))
	if err == nil {
		t.Fatal("a create with no consent was accepted")
	}
	return strings.TrimSuffix(err.Error()[strings.Index(err.Error(), "--consent=")+len("--consent="):], "`")
}

// binder is who the schedule's record names as the binder of what it
// submits now.
func (h *consentHarness) binder(t *testing.T, s *orkstackv1.Schedule) string {
	t.Helper()
	doc, err := h.bindings.Get(context.Background(), "default", s.GetId())
	must.OK(t, err)
	for _, e := range doc.Entries {
		if e.OrkflowName == s.GetOrkflowName() {
			return e.Binder
		}
	}
	return ""
}

// TestTriggerConsent: a trigger is bound by the same rule as a schedule: a
// create needs a consent to the digest shown, a save that changes nothing
// keeps it, a stale one is shown as stale, and a consent to the new digest
// is recorded.
func TestTriggerConsent(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	h := &consentHarness{orkflows: orkflows}
	h.put(t, roleOrkflow("1", "sts"))
	svc := api.NewService(api.Config{Orkflows: orkflows, Namespaces: newFakeRegistry(), Triggers: triggers.NewMemoryStore()})
	_, err := svc.CreateTrigger(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "deploy", Name: "hook"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("create without a consent: err = %v", err)
	}
	first := strings.TrimSuffix(err.Error()[strings.Index(err.Error(), "--consent=")+len("--consent="):], "`")
	created, err := svc.CreateTrigger(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "deploy", Name: "hook", Consent: first}))
	must.OK(t, err)
	tr := created.Msg.GetTrigger()
	if tr.GetConsented().GetDigest() != first || tr.GetConsentStale() || strings.Join(tr.GetConsented().GetAudiences(), ",") != "sts" {
		t.Fatalf("created: consented = %v stale = %v", tr.GetConsented(), tr.GetConsentStale())
	}

	h.put(t, roleOrkflow("2", "vault"))
	update := &orkstackv1.UpdateTriggerRequest{Id: tr.GetId(), Name: "hook", Description: "re-saved"}
	updated, err := svc.UpdateTrigger(as("user:corp:bob"), connect.NewRequest(update))
	must.OK(t, err)
	if got := updated.Msg.GetTrigger(); got.GetConsented().GetDigest() != first || !got.GetConsentStale() {
		t.Fatalf("after the pointer moved: consented = %v stale = %v", got.GetConsented(), got.GetConsentStale())
	}
	second := updated.Msg.GetTrigger().GetAsksFor().GetDigest()
	update.Consent = first
	_, err = svc.UpdateTrigger(as("user:corp:bob"), connect.NewRequest(update))
	refusedForConsent(t, err, second)
	update.Consent = second
	updated, err = svc.UpdateTrigger(as("user:corp:bob"), connect.NewRequest(update))
	must.OK(t, err)
	if got := updated.Msg.GetTrigger(); got.GetConsented().GetDigest() != second || got.GetConsentStale() {
		t.Fatalf("after consenting: consented = %v stale = %v", got.GetConsented(), got.GetConsentStale())
	}
}

// TestPromotion_SaysWhatItWillBreak: promoting a version that asks for
// other identities lists each schedule following the pointer whose consent
// it makes stale, with the digest to consent to; a pinned schedule is not
// listed, and a version asking for nothing breaks nothing.
func TestPromotion_SaysWhatItWillBreak(t *testing.T) {
	h := newConsentHarness(t)
	ctx := context.Background()
	first := mustConsentDigest(t, h)
	for _, req := range []*orkstackv1.CreateScheduleRequest{
		{Name: "nightly", OrkflowName: "deploy", Spec: dailySpec(), Consent: first},
		{Name: "pinned", OrkflowName: "deploy", OrkflowVersion: "1", Spec: dailySpec(), Consent: first},
	} {
		_, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(req))
		must.OK(t, err)
	}
	for _, def := range []*orkstackv1.Orkflow{roleOrkflow("2", "vault"), {Namespace: "default", Name: "deploy", Version: "3"}} {
		_, _, err := h.orkflows.Put(ctx, def, artifacts.HeadExpectation{})
		must.OK(t, err)
	}
	resp, err := h.svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "2"}))
	must.OK(t, err)
	stale := resp.Msg.GetStaleConsent()
	if len(stale) != 1 || stale[0].GetKind() != "schedule" || stale[0].GetName() != "nightly" ||
		!consent.Valid(stale[0].GetDigest()) || stale[0].GetDigest() == first {
		t.Fatalf("stale_consent = %v, want schedule nightly with version 2's digest", stale)
	}
	if got := h.get(t, "nightly").GetAsksFor().GetDigest(); got != stale[0].GetDigest() {
		t.Fatalf("the promotion hands out %s, the schedule %s", stale[0].GetDigest(), got)
	}
	resp, err = h.svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "3"}))
	must.OK(t, err)
	if len(resp.Msg.GetStaleConsent()) != 0 {
		t.Fatalf("a version asking for nothing lists %v", resp.Msg.GetStaleConsent())
	}
}

// flakyBindings answers reads with an error while down.
type flakyBindings struct {
	api.ScheduleBindingStore
	down bool
}

func (f *flakyBindings) Get(ctx context.Context, ns, id string) (artifacts.ScheduleBindings, error) {
	if f.down {
		return artifacts.ScheduleBindings{}, errors.New("bucket unreachable")
	}
	return f.ScheduleBindingStore.Get(ctx, ns, id)
}

// TestScheduleConsent_AnUnreadableRecordIsNeitherNoneNorStale: a schedule
// whose binding record cannot be read shows neither what it asks for nor a
// consent, never "none recorded"; a promotion does not list it as stale,
// and says that it could not check it.
func TestScheduleConsent_AnUnreadableRecordIsNeitherNoneNorStale(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	store, err := artifacts.NewScheduleBindingStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	flaky := &flakyBindings{ScheduleBindingStore: store}
	h := &consentHarness{orkflows: orkflows}
	h.scheduleHarness = newScheduleHarnessWith(t, func(cfg *api.Config) { cfg.Orkflows, cfg.ScheduleBindings = orkflows, flaky })
	h.put(t, roleOrkflow("1", "sts"))
	first := mustConsentDigest(t, h)
	_, err = h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "deploy", Spec: dailySpec(), Consent: first}))
	must.OK(t, err)
	_, _, err = orkflows.Put(context.Background(), roleOrkflow("2", "vault"), artifacts.HeadExpectation{})
	must.OK(t, err)

	flaky.down = true
	if s := h.get(t, "nightly"); s.GetAsksFor() != nil || s.GetConsented() != nil || s.GetConsentStale() {
		t.Fatalf("an unreadable record shows asks_for = %v consented = %v stale = %v", s.GetAsksFor(), s.GetConsented(), s.GetConsentStale())
	}
	resp, err := h.svc.SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "2"}))
	must.OK(t, err)
	if len(resp.Msg.GetStaleConsent()) != 0 || !strings.Contains(resp.Msg.GetStaleConsentUnchecked(), "schedule") {
		t.Fatalf("stale = %v unchecked = %q, want none listed and the schedule named as unchecked",
			resp.Msg.GetStaleConsent(), resp.Msg.GetStaleConsentUnchecked())
	}
}

// TestTriggerConsent_ARenameReadsNoOrkflow: a save that binds nothing and
// gives no consent does not read the Orkflow, so renaming a trigger whose
// Orkflow is gone still works.
func TestTriggerConsent_ARenameReadsNoOrkflow(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	h := &consentHarness{orkflows: orkflows}
	h.put(t, &orkstackv1.Orkflow{Namespace: "default", Name: "deploy", Version: "1"})
	svc := api.NewService(api.Config{Orkflows: orkflows, Namespaces: newFakeRegistry(), Triggers: triggers.NewMemoryStore()})
	created, err := svc.CreateTrigger(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "deploy", Name: "hook"}))
	must.OK(t, err)
	must.OK(t, orkflows.Delete(context.Background(), "default", "deploy"))
	_, err = svc.UpdateTrigger(as("user:corp:bob"), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.Msg.GetTrigger().GetId(), Name: "renamed"}))
	must.OK(t, err)
}

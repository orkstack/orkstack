package api_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// bindingHarness is the schedule harness with the API's schedule records.
type bindingHarness struct {
	*scheduleHarness
	bindings *artifacts.ScheduleBindingStore
}

func newBindingHarness(t *testing.T) *bindingHarness {
	t.Helper()
	h := newScheduleHarness(t)
	store, err := artifacts.NewScheduleBindingStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	var counter int
	h.svc = api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), Runs: h.runs,
		Bus: engine.NewEventBus(), Starter: h.starter, Schedules: h.schedules, ScheduleBindings: store,
		Now: func() time.Time { return h.now },
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("ork-bind-%d", counter)
		},
	})
	h.apply(t, helloYAML(t))
	return &bindingHarness{scheduleHarness: h, bindings: store}
}

// as is a context of a request OrkStack verified as id.
func as(id string) context.Context {
	ctx := authn.WithMode(context.Background(), authn.ModeEnforce)
	return authn.WithPrincipal(ctx, &authn.Principal{Kind: authn.KindUser, ID: id, Method: authn.MethodOIDC})
}

func (h *bindingHarness) record(t *testing.T, id string) artifacts.ScheduleBindings {
	t.Helper()
	doc, err := h.bindings.Get(context.Background(), "default", id)
	must.OK(t, err)
	return doc
}

func input(t *testing.T, v string) *orkstackv1.Payload {
	t.Helper()
	data, err := structpb.NewStruct(map[string]any{"who": v})
	must.OK(t, err)
	return &orkstackv1.Payload{Data: data}
}

// TestSchedule_BinderFollowsWhatIsSubmitted: a create binds its caller with
// the empty table's consent; an update that changes what the schedule
// submits binds its caller; one that changes only the description or the
// times, and a pause, keep the binder; --rebind binds the caller without a
// change. The object holds the entry of what the cluster holds, trimmed.
func TestSchedule_BinderFollowsWhatIsSubmitted(t *testing.T) {
	h := newBindingHarness(t)
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec(), Input: input(t, "a")}))
	must.OK(t, err)
	id := created.Msg.GetSchedule().GetId()
	if c := created.Msg.GetSchedule().GetConsented(); c.GetDigest() != consent.Empty() || c.GetAt() == nil {
		t.Fatalf("a created schedule's consent = %v, want the empty table's", c)
	}
	if got := created.Msg.GetSchedule().GetAsksFor().GetDigest(); got != consent.Empty() {
		t.Fatalf("asks_for = %q", got)
	}
	binder := func() artifacts.ScheduleBinding {
		t.Helper()
		doc := h.record(t, id)
		if len(doc.Entries) != 1 {
			t.Fatalf("the record holds %d entries after a save, want the one the cluster holds: %+v", len(doc.Entries), doc.Entries)
		}
		return doc.Entries[0]
	}
	if b := binder(); b.Binder != "user:corp:alice" || !b.BinderVerified || b.BinderMethod != authn.MethodOIDC || b.Consent != consent.Empty() {
		t.Fatalf("after create: %+v", b)
	}

	update := func(ctx context.Context, req *orkstackv1.UpdateScheduleRequest) {
		t.Helper()
		req.Name = "nightly"
		if req.Spec == nil {
			req.Spec = dailySpec()
		}
		_, err := h.svc.UpdateSchedule(ctx, connect.NewRequest(req))
		must.OK(t, err)
	}
	// Bob changes only the description and the times: Alice stays the binder.
	h.now = h.now.Add(time.Hour)
	update(as("user:corp:bob"), &orkstackv1.UpdateScheduleRequest{Input: input(t, "a"), Description: "nightly run",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 3 * * *"}, Timezone: "Europe/Lisbon"}})
	if b := binder(); b.Binder != "user:corp:alice" {
		t.Fatalf("a save that submits the same rebound to %q", b.Binder)
	}
	// Bob pauses it: nothing is written.
	_, err = h.svc.PauseSchedule(as("user:corp:bob"), connect.NewRequest(&orkstackv1.PauseScheduleRequest{Name: "nightly"}))
	must.OK(t, err)
	if b := binder(); b.Binder != "user:corp:alice" {
		t.Fatalf("a pause rebound to %q", b.Binder)
	}
	// Bob changes the input: Bob binds.
	update(as("user:corp:bob"), &orkstackv1.UpdateScheduleRequest{Input: input(t, "b")})
	if b := binder(); b.Binder != "user:corp:bob" {
		t.Fatalf("a save that changes the input left the binder %q", b.Binder)
	}
	// Carol rebinds without a change: Carol binds.
	update(as("user:corp:carol"), &orkstackv1.UpdateScheduleRequest{Input: input(t, "b"), Rebind: true})
	if b := binder(); b.Binder != "user:corp:carol" || !b.BinderVerified {
		t.Fatalf("--rebind left %+v", b)
	}
	got, err := h.svc.GetSchedule(context.Background(), connect.NewRequest(&orkstackv1.GetScheduleRequest{Name: "nightly"}))
	must.OK(t, err)
	if got.Msg.GetSchedule().GetConsented().GetDigest() != consent.Empty() {
		t.Fatalf("GetSchedule consent = %v", got.Msg.GetSchedule().GetConsented())
	}

	// Delete removes the object with the schedule.
	_, err = h.svc.DeleteSchedule(context.Background(), connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "nightly"}))
	must.OK(t, err)
	if doc := h.record(t, id); len(doc.Entries) != 0 {
		t.Fatalf("after delete the record holds %+v", doc.Entries)
	}
}

// TestSchedule_AnAssertedCallerIsRecordedAsAsserted: under asserted mode
// the binder is the header's principal, never marked verified.
func TestSchedule_AnAssertedCallerIsRecordedAsAsserted(t *testing.T) {
	h := newBindingHarness(t)
	req := connect.NewRequest(&orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()})
	req.Header().Set(authn.PrincipalHeader, "gateway-says-alice")
	created, err := h.svc.CreateSchedule(context.Background(), req)
	must.OK(t, err)
	b := h.record(t, created.Msg.GetSchedule().GetId()).Entries[0]
	if b.Binder != "gateway-says-alice" || b.BinderVerified || b.BinderMethod != "" {
		t.Fatalf("an asserted create recorded %+v", b)
	}
}

// failingBindings refuses every write.
type failingBindings struct{ api.ScheduleBindingStore }

func (failingBindings) Update(context.Context, string, string, func(*artifacts.ScheduleBindings) error) (artifacts.ScheduleBindings, error) {
	return artifacts.ScheduleBindings{}, errors.New("bucket unreachable")
}

// TestSchedule_ARecordThatCannotBeWrittenRefusesTheSave: the record comes
// first, so a store that cannot take it refuses the save before the cluster
// is asked, and no schedule exists unrecorded.
func TestSchedule_ARecordThatCannotBeWrittenRefusesTheSave(t *testing.T) {
	h := newScheduleHarness(t)
	h.svc = api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), Runs: h.runs,
		Bus: engine.NewEventBus(), Starter: h.starter, Schedules: h.schedules, ScheduleBindings: failingBindings{},
		Now: func() time.Time { return h.now },
	})
	h.apply(t, helloYAML(t))
	_, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("create with an unwritable record = %v, want unavailable", err)
	}
	if len(h.schedules.created) != 0 {
		t.Fatalf("the cluster was asked anyway: %v", h.schedules.created)
	}
}

// TestTrigger_BinderFollowsTheVersionItStarts: a create binds its caller;
// a new description keeps the binder; a new version, or --rebind, binds the
// caller. The response carries the consent.
func TestTrigger_BinderFollowsTheVersionItStarts(t *testing.T) {
	store := triggers.NewMemoryStore()
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), Triggers: store})
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)}))
	must.OK(t, err)
	created, err := svc.CreateTrigger(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"}))
	must.OK(t, err)
	id := created.Msg.GetTrigger().GetId()
	if created.Msg.GetTrigger().GetConsented().GetDigest() != consent.Empty() || created.Msg.GetTrigger().GetAsksFor().GetDigest() != consent.Empty() {
		t.Fatalf("a created trigger's consent = %v, asks for %v", created.Msg.GetTrigger().GetConsented(), created.Msg.GetTrigger().GetAsksFor())
	}
	binder := func() triggers.Binding {
		t.Helper()
		got, found, err := store.Get(context.Background(), id)
		if err != nil || !found {
			t.Fatalf("get %s: %v %v", id, found, err)
		}
		return got.Binding
	}
	if b := binder(); b.Binder != "user:corp:alice" || !b.Verified || b.Method != authn.MethodOIDC {
		t.Fatalf("after create: %+v", b)
	}
	update := func(who string, req *orkstackv1.UpdateTriggerRequest) {
		t.Helper()
		req.Id = id
		_, err := svc.UpdateTrigger(as(who), connect.NewRequest(req))
		must.OK(t, err)
	}
	update("user:corp:bob", &orkstackv1.UpdateTriggerRequest{Description: "hook"})
	if b := binder(); b.Binder != "user:corp:alice" {
		t.Fatalf("a new description rebound to %q", b.Binder)
	}
	update("user:corp:bob", &orkstackv1.UpdateTriggerRequest{OrkflowVersion: "0.2.0", Description: "hook"})
	if b := binder(); b.Binder != "user:corp:bob" {
		t.Fatalf("a new version left the binder %q", b.Binder)
	}
	update("user:corp:carol", &orkstackv1.UpdateTriggerRequest{OrkflowVersion: "0.2.0", Description: "hook", Rebind: true})
	if b := binder(); b.Binder != "user:corp:carol" {
		t.Fatalf("--rebind left %q", b.Binder)
	}
}

// TestSchedule_AFailedDeleteKeepsTheRecord: a cluster delete that fails
// after the record was emptied puts the entries back, so a schedule that
// still fires keeps its binder.
func TestSchedule_AFailedDeleteKeepsTheRecord(t *testing.T) {
	h := newBindingHarness(t)
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	must.OK(t, err)
	h.schedules.deleteErr = errors.New("cluster unreachable")
	if _, err := h.svc.DeleteSchedule(context.Background(), connect.NewRequest(&orkstackv1.DeleteScheduleRequest{Name: "nightly"})); err == nil {
		t.Fatal("a failed cluster delete answered success")
	}
	doc := h.record(t, created.Msg.GetSchedule().GetId())
	if len(doc.Entries) != 1 || doc.Entries[0].Binder != "user:corp:alice" {
		t.Fatalf("after a failed delete the record holds %+v", doc.Entries)
	}
}

// TestSchedule_ARefusedUpdateLeavesNoEntry: an update the cluster refuses
// after its entry was added takes the entry out again, so refused saves
// cannot push out the entry of what the cluster holds.
func TestSchedule_ARefusedUpdateLeavesNoEntry(t *testing.T) {
	h := newBindingHarness(t)
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	must.OK(t, err)
	h.schedules.updateErr = engine.ErrScheduleSpecRefused
	for _, who := range []string{"a", "b", "c", "d", "e"} {
		_, err := h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
			Name: "nightly", Spec: dailySpec(), Input: input(t, who)}))
		if err == nil {
			t.Fatal("a refused update answered success")
		}
	}
	doc := h.record(t, created.Msg.GetSchedule().GetId())
	if len(doc.Entries) != 1 || doc.Entries[0].Binder != "user:corp:alice" {
		t.Fatalf("after five refused updates the record holds %+v, want only the created entry", doc.Entries)
	}
}

// racingTrim empties the record just before the trim's write, as another
// save's trim that ran in between would.
type racingTrim struct {
	*artifacts.ScheduleBindingStore
	calls int
}

func (r *racingTrim) Update(ctx context.Context, ns, id string, change func(*artifacts.ScheduleBindings) error) (artifacts.ScheduleBindings, error) {
	r.calls++
	if r.calls == 2 {
		return r.ScheduleBindingStore.Update(ctx, ns, id, func(doc *artifacts.ScheduleBindings) error {
			doc.Entries = nil
			return change(doc)
		})
	}
	return r.ScheduleBindingStore.Update(ctx, ns, id, change)
}

// TestSchedule_ATrimPutsBackTheEntryOfWhatTheClusterHolds: when another
// save's trim removed this save's entry and the cluster holds this save's
// submission, the trim puts the entry back.
func TestSchedule_ATrimPutsBackTheEntryOfWhatTheClusterHolds(t *testing.T) {
	h := newScheduleHarness(t)
	store, err := artifacts.NewScheduleBindingStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	h.svc = api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), Runs: h.runs,
		Bus: engine.NewEventBus(), Starter: h.starter, Schedules: h.schedules, ScheduleBindings: &racingTrim{ScheduleBindingStore: store},
		Now: func() time.Time { return h.now },
	})
	h.apply(t, helloYAML(t))
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	must.OK(t, err)
	doc, err := store.Get(context.Background(), "default", created.Msg.GetSchedule().GetId())
	must.OK(t, err)
	if len(doc.Entries) != 1 || doc.Entries[0].Binder != "user:corp:alice" {
		t.Fatalf("after a raced trim the record holds %+v", doc.Entries)
	}
}

// TestSchedule_ARefusedSaveOfTheSameSubmissionKeepsTheBinder: a save that
// changes nothing it submits replaces the entry the cluster holds with one
// of the same binder; when the cluster refuses it, that entry stays, so the
// schedule never loses its record.
func TestSchedule_ARefusedSaveOfTheSameSubmissionKeepsTheBinder(t *testing.T) {
	h := newBindingHarness(t)
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	must.OK(t, err)
	h.schedules.updateErr = engine.ErrScheduleSpecRefused
	if _, err := h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", Spec: dailySpec(), Description: "new words"})); err == nil {
		t.Fatal("a refused update answered success")
	}
	doc := h.record(t, created.Msg.GetSchedule().GetId())
	if len(doc.Entries) != 1 || doc.Entries[0].Binder != "user:corp:alice" {
		t.Fatalf("after a refused same-submission save the record holds %+v", doc.Entries)
	}
}

// TestSchedule_ARefusedRebindNamesNobodyNew: a --rebind the cluster refuses
// records nobody: the binder stays who it was.
func TestSchedule_ARefusedRebindNamesNobodyNew(t *testing.T) {
	h := newBindingHarness(t)
	created, err := h.svc.CreateSchedule(as("user:corp:alice"), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()}))
	must.OK(t, err)
	h.schedules.updateErr = engine.ErrScheduleSpecRefused
	if _, err := h.svc.UpdateSchedule(as("user:corp:bob"), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", Spec: dailySpec(), Rebind: true})); err == nil {
		t.Fatal("a refused rebind answered success")
	}
	doc := h.record(t, created.Msg.GetSchedule().GetId())
	if len(doc.Entries) != 1 || doc.Entries[0].Binder != "user:corp:alice" {
		t.Fatalf("after a refused rebind the record holds %+v, want Alice still", doc.Entries)
	}
}

// TestSchedule_ABinderIsRecordedClean: an asserted header's control and
// format characters never reach the record, and a rebind that names nobody
// is refused.
func TestSchedule_ABinderIsRecordedClean(t *testing.T) {
	h := newBindingHarness(t)
	req := connect.NewRequest(&orkstackv1.CreateScheduleRequest{Name: "nightly", OrkflowName: "hello-ork", Spec: dailySpec()})
	req.Header().Set(authn.PrincipalHeader, "eve\x1b[31m\u202eadmin")
	created, err := h.svc.CreateSchedule(context.Background(), req)
	must.OK(t, err)
	b := h.record(t, created.Msg.GetSchedule().GetId()).Entries[0]
	if b.Binder != "eve [31m admin" || b.BinderVerified {
		t.Fatalf("the asserted binder was recorded as %q (verified %v)", b.Binder, b.BinderVerified)
	}
	_, err = h.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Name: "nightly", Spec: dailySpec(), Rebind: true}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a rebind naming nobody = %v, want failed_precondition", err)
	}
}

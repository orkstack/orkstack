package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/triggers"
)

func binding(version, binder string) ScheduleBinding {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	return ScheduleBinding{OrkflowName: "nightly", OrkflowVersion: version, InputSHA256: strings.Repeat("a", 64),
		Binder: binder, BinderVerified: true, BinderMethod: "token", BoundAt: at,
		Consent: "c1:" + strings.Repeat("0", 64), ConsentAt: at, SavedAt: at}
}

const testScheduleID = "sch-0123456789abcdef"

// TestScheduleBindings_AddAndKeep: an entry that submits what an older one
// does replaces it, the set holds at most four, and Keep trims to what the
// cluster holds plus the save in progress.
func TestScheduleBindings_AddAndKeep(t *testing.T) {
	var s ScheduleBindings
	for _, v := range []string{"1", "2", "3", "4", "5"} {
		s.Add(binding(v, "user:corp:a"))
	}
	if len(s.Entries) != MaxScheduleBindings || s.Entries[0].OrkflowVersion != "2" {
		t.Fatalf("entries = %+v, want the newest %d", s.Entries, MaxScheduleBindings)
	}
	s.Add(binding("3", "user:corp:b"))
	if len(s.Entries) != MaxScheduleBindings || s.Entries[len(s.Entries)-1].Binder != "user:corp:b" {
		t.Fatalf("re-adding version 3 = %+v", s.Entries)
	}
	if got, ok := s.Find(binding("3", "")); !ok || got.Binder != "user:corp:b" {
		t.Fatalf("Find(3) = %+v, %v", got, ok)
	}
	s.Keep(binding("5", ""), binding("3", ""), binding("9", ""))
	if len(s.Entries) != 2 || s.Entries[0].OrkflowVersion != "5" || s.Entries[1].OrkflowVersion != "3" {
		t.Fatalf("Keep = %+v", s.Entries)
	}
}

func scheduleBindingStores(t *testing.T) map[string]*ScheduleBindingStore {
	t.Helper()
	local, err := NewScheduleBindingStore(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s3, err := NewScheduleBindingStore(context.Background(), newS3Double(t).config())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*ScheduleBindingStore{"local": local, "s3": s3}
}

// TestScheduleBindingStore_RoundTrip: an object is created by the first
// update, replaced by later ones, read back as written, removed by Delete,
// and every object of a namespace by DeleteNamespace; a missing object reads
// as an empty set of that schedule.
func TestScheduleBindingStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, store := range scheduleBindingStores(t) {
		t.Run(name, func(t *testing.T) {
			empty, err := store.Get(ctx, "payments", testScheduleID)
			if err != nil || len(empty.Entries) != 0 || empty.ScheduleID != testScheduleID || empty.V != 1 {
				t.Fatalf("missing object = %+v, %v", empty, err)
			}
			for _, v := range []string{"1", "2"} {
				if _, err := store.Update(ctx, "payments", testScheduleID, func(s *ScheduleBindings) error {
					s.Add(binding(v, "user:corp:a"))
					return nil
				}); err != nil {
					t.Fatalf("update %s: %v", v, err)
				}
			}
			got, err := store.Get(ctx, "payments", testScheduleID)
			if err != nil || len(got.Entries) != 2 || got.Entries[1] != binding("2", "user:corp:a") {
				t.Fatalf("read back %+v, %v", got, err)
			}
			refused := errors.New("refused")
			if _, err := store.Update(ctx, "payments", testScheduleID, func(*ScheduleBindings) error { return refused }); !errors.Is(err, refused) {
				t.Fatalf("a refused change answered %v", err)
			}
			other := "sch-fedcba9876543210"
			if _, err := store.Update(ctx, "payments", other, func(s *ScheduleBindings) error {
				s.Add(binding("1", "user:corp:a"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, "payments", testScheduleID); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, "payments", testScheduleID); err != nil {
				t.Fatalf("deleting a missing object: %v", err)
			}
			if got, err := store.Get(ctx, "payments", testScheduleID); err != nil || len(got.Entries) != 0 {
				t.Fatalf("after delete %+v, %v", got, err)
			}
			if err := store.DeleteNamespace(ctx, "payments"); err != nil {
				t.Fatal(err)
			}
			if got, err := store.Get(ctx, "payments", other); err != nil || len(got.Entries) != 0 {
				t.Fatalf("after DeleteNamespace %+v, %v", got, err)
			}
		})
	}
}

// TestScheduleBindingStore_RefusesBadKeysAndForeignObjects: an invalid
// namespace or id builds no key, and an object that names another schedule
// is refused rather than adopted.
func TestScheduleBindingStore_RefusesBadKeysAndForeignObjects(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	store, err := NewScheduleBindingStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ ns, id string }{{"Bad", testScheduleID}, {"payments", "../x"}, {"payments", "sch-short"}} {
		if _, err := store.Get(ctx, tc.ns, tc.id); err == nil {
			t.Errorf("Get(%q, %q) built a key", tc.ns, tc.id)
		}
	}
	d.objects[ScheduleBindingPrefix+"/payments/"+testScheduleID+".json"] = []byte(`{"v":1,"scheduleId":"sch-fedcba9876543210","namespace":"payments","entries":[]}`)
	if _, err := store.Get(ctx, "payments", testScheduleID); err == nil || !strings.Contains(err.Error(), "names schedule") {
		t.Fatalf("a foreign object read as %v", err)
	}
}

// TestScheduleBindingStore_GivesUpWhenContended: a write that loses its
// condition every time ends ErrScheduleBindingContended after five tries.
func TestScheduleBindingStore_GivesUpWhenContended(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	store, err := NewScheduleBindingStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	key := ScheduleBindingPrefix + "/payments/" + testScheduleID + ".json"
	tries := 0
	_, err = store.Update(ctx, "payments", testScheduleID, func(s *ScheduleBindings) error {
		tries++
		// Another writer lands between every read and its write.
		d.mu.Lock()
		d.objects[key] = []byte(`{"v":1,"scheduleId":"` + testScheduleID + `","namespace":"payments","entries":[]}` + strings.Repeat(" ", tries))
		d.mu.Unlock()
		s.Add(binding("1", "user:corp:a"))
		return nil
	})
	if !errors.Is(err, ErrScheduleBindingContended) || tries != scheduleBindingTries {
		t.Fatalf("err = %v after %d tries, want contended after %d", err, tries, scheduleBindingTries)
	}
}

// TestTriggerRecord_BeforeBindersDecodesUnbound: a trigger record written
// before triggers recorded a binder decodes with none, and round-trips one.
func TestTriggerRecord_BeforeBindersDecodesUnbound(t *testing.T) {
	old := `{"id":"trg-0123456789abcdef","namespace":"default","orkflowName":"hello-ork","createdAt":"2026-10-01T00:00:00Z","secretSalt":"00","secretHash":"11"}`
	trigger, err := unmarshalTriggerRecord([]byte(old))
	if err != nil {
		t.Fatal(err)
	}
	if trigger.Binding != (triggers.Binding{}) {
		t.Fatalf("an older record decoded bound: %+v", trigger.Binding)
	}
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	trigger.Binding = triggers.Binding{Binder: "user:corp:a", Verified: true, Method: "oidc", BoundAt: at, Consent: "c1:" + strings.Repeat("0", 64), ConsentAt: at}
	raw, err := marshalTriggerRecord(trigger)
	if err != nil {
		t.Fatal(err)
	}
	back, err := unmarshalTriggerRecord(raw)
	if err != nil || back.Binding != trigger.Binding {
		t.Fatalf("round trip = %+v, %v", back.Binding, err)
	}
}

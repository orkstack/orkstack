package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func incarnationStores(t *testing.T) map[string]*IncarnationStore {
	t.Helper()
	local, err := NewIncarnationStore(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s3, err := NewIncarnationStore(context.Background(), newS3Double(t).config())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*IncarnationStore{"local": local, "s3": s3}
}

// TestIncarnation_NamespaceIDIsMintedOnceForItsIncarnation: the first Ensure
// mints an `ns_` id, later ones and Get answer it, a read never writes one,
// and a namespace made again (another created_at) gets a new id while the
// old record is ignored, not adopted.
func TestIncarnation_NamespaceIDIsMintedOnceForItsIncarnation(t *testing.T) {
	ctx := context.Background()
	first := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	again := first.Add(time.Hour)
	for name, s := range incarnationStores(t) {
		t.Run(name, func(t *testing.T) {
			if id, ok, err := s.NamespaceID(ctx, "payments", first); err != nil || ok || id != "" {
				t.Fatalf("before any write Get = %q, %v, %v", id, ok, err)
			}
			if id, ok, _ := s.NamespaceID(ctx, "payments", first); ok || id != "" {
				t.Fatal("a read wrote an id")
			}
			id, err := s.EnsureNamespaceID(ctx, "payments", first)
			if err != nil || !strings.HasPrefix(id, "ns_") || len(id) != len("ns_")+26 {
				t.Fatalf("Ensure = %q, %v", id, err)
			}
			if again, err := s.EnsureNamespaceID(ctx, "payments", first); err != nil || again != id {
				t.Fatalf("a second Ensure = %q, %v, want %q", again, err, id)
			}
			if got, ok, err := s.NamespaceID(ctx, "payments", first); err != nil || !ok || got != id {
				t.Fatalf("Get = %q, %v, %v, want %q", got, ok, err, id)
			}
			if got, ok, err := s.NamespaceID(ctx, "payments", again); err != nil || ok || got != "" {
				t.Fatalf("another incarnation read %q, %v, %v", got, ok, err)
			}
			next, err := s.EnsureNamespaceID(ctx, "payments", again)
			if err != nil || next == id || !strings.HasPrefix(next, "ns_") {
				t.Fatalf("the new incarnation's Ensure = %q, %v (old %q)", next, err, id)
			}
			if got, ok, _ := s.NamespaceID(ctx, "payments", first); ok || got != "" {
				t.Fatalf("the old incarnation still reads %q", got)
			}
			// A delete of the incarnation before keeps the record a newer one
			// wrote.
			if err := s.DeleteNamespaceID(ctx, "payments", first); err != nil {
				t.Fatal(err)
			}
			if got, ok, err := s.NamespaceID(ctx, "payments", again); err != nil || !ok || got != next {
				t.Fatalf("a delete of the old incarnation removed the new one's record: %q, %v, %v", got, ok, err)
			}
			if err := s.DeleteNamespaceID(ctx, "payments", again); err != nil {
				t.Fatal(err)
			}
			if err := s.DeleteNamespaceID(ctx, "payments", again); err != nil {
				t.Fatalf("deleting a missing record: %v", err)
			}
			if _, ok, err := s.NamespaceID(ctx, "payments", again); err != nil || ok {
				t.Fatalf("after delete ok=%v err=%v", ok, err)
			}
		})
	}
}

// TestIncarnation_DefaultIsAConstantAndNeverStored: `default` answers
// ns_default from Get and Ensure, writes nothing and cannot be deleted.
func TestIncarnation_DefaultIsAConstantAndNeverStored(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	s, err := NewIncarnationStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []func() (string, error){
		func() (string, error) { return s.EnsureNamespaceID(ctx, "default", time.Time{}) },
		func() (string, error) { id, _, err := s.NamespaceID(ctx, "default", time.Time{}); return id, err },
	} {
		if id, err := f(); err != nil || id != DefaultNamespaceID {
			t.Fatalf("default = %q, %v", id, err)
		}
	}
	if len(d.objects) != 0 {
		t.Fatalf("default wrote %v", d.objects)
	}
	if err := s.DeleteNamespaceID(ctx, "default", time.Time{}); err == nil {
		t.Fatal("deleting default's id was allowed")
	}
}

// TestIncarnation_OrkflowIDFollowsTheHeadsSince: an Orkflow's id is bound to
// its head's since, a legacy head's zero since included, and a re-created
// Orkflow gets a new one.
func TestIncarnation_OrkflowIDFollowsTheHeadsSince(t *testing.T) {
	ctx := context.Background()
	since := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for name, s := range incarnationStores(t) {
		t.Run(name, func(t *testing.T) {
			legacy, err := s.EnsureOrkflowID(ctx, "payments", "report", time.Time{})
			if err != nil || !strings.HasPrefix(legacy, "of_") {
				t.Fatalf("legacy Ensure = %q, %v", legacy, err)
			}
			if got, ok, err := s.OrkflowID(ctx, "payments", "report", time.Time{}); err != nil || !ok || got != legacy {
				t.Fatalf("legacy Get = %q, %v, %v", got, ok, err)
			}
			id, err := s.EnsureOrkflowID(ctx, "payments", "report", since)
			if err != nil || id == legacy {
				t.Fatalf("re-created Ensure = %q, %v (legacy %q)", id, err, legacy)
			}
			if other, err := s.EnsureOrkflowID(ctx, "payments", "deploy", since); err != nil || other == id {
				t.Fatalf("another Orkflow = %q, %v", other, err)
			}
			if err := s.DeleteOrkflowID(ctx, "payments", "report", time.Time{}); err != nil {
				t.Fatal(err)
			}
			if _, ok, _ := s.OrkflowID(ctx, "payments", "report", since); !ok {
				t.Fatal("a delete of the legacy incarnation removed the re-created one's record")
			}
			if err := s.DeleteOrkflowID(ctx, "payments", "report", since); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := s.OrkflowID(ctx, "payments", "report", since); err != nil || ok {
				t.Fatalf("after delete ok=%v err=%v", ok, err)
			}
			if got, ok, err := s.OrkflowID(ctx, "payments", "deploy", since); err != nil || !ok || got == "" {
				t.Fatalf("delete touched another Orkflow: %q, %v, %v", got, ok, err)
			}
		})
	}
}

// TestIncarnation_RefusesBadNamesAndForeignRecords: an invalid name builds
// no key, and a record that names another thing is an error, never adopted
// and never overwritten.
func TestIncarnation_RefusesBadNamesAndForeignRecords(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	s, err := NewIncarnationStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureNamespaceID(ctx, "Bad", time.Time{}); err == nil {
		t.Error("an invalid namespace built a key")
	}
	if _, err := s.EnsureOrkflowID(ctx, "payments", "../x", time.Time{}); err == nil {
		t.Error("an invalid Orkflow name built a key")
	}
	d.objects[IncarnationPrefix+"/namespaces/payments.json"] = []byte(`{"v":1,"kind":"namespace","name":"orders","id":"ns_x","boundTo":"2026-11-15T12:00:00Z"}`)
	if _, _, err := s.NamespaceID(ctx, "payments", time.Time{}); err == nil || !strings.Contains(err.Error(), "names") {
		t.Fatalf("a foreign record read as %v", err)
	}
	if _, err := s.EnsureNamespaceID(ctx, "payments", time.Time{}); err == nil {
		t.Fatal("Ensure replaced a foreign record")
	}
	d.objects[IncarnationPrefix+"/namespaces/payments.json"] = []byte(`not json`)
	if _, _, err := s.NamespaceID(ctx, "payments", time.Time{}); err == nil {
		t.Fatal("an undecodable record read as no record")
	}
}

// TestIncarnation_RacingEnsuresAgree: two engines that mint at once end with
// one id, the one stored.
func TestIncarnation_RacingEnsuresAgree(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	a, err := NewIncarnationStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewIncarnationStore(ctx, d.config())
	if err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	ids := make(chan string, 8)
	errs := make(chan error, 8)
	for i := range 8 {
		s := a
		if i%2 == 1 {
			s = b
		}
		go func() {
			id, err := s.EnsureOrkflowID(ctx, "payments", "report", since)
			ids <- id
			errs <- err
		}()
	}
	var first string
	for range 8 {
		if err := <-errs; err != nil && !errors.Is(err, ErrIncarnationContended) {
			t.Fatal(err)
		}
		id := <-ids
		if id == "" {
			continue
		}
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("racing Ensures answered %q and %q", first, id)
		}
	}
	if stored, ok, err := a.OrkflowID(ctx, "payments", "report", since); err != nil || !ok || stored != first {
		t.Fatalf("stored %q, %v, %v; answered %q", stored, ok, err, first)
	}
}

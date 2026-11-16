package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSigninStore_CreateGetReplaceDelete(t *testing.T) {
	ctx := context.Background()
	cas := NewMemoryCAS()
	s := NewMemorySigninStore(cas)
	id := strings.Repeat("ab", 32)
	if _, _, err := s.Get(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get of nothing = %v; want ErrNotFound", err)
	}
	etag, err := s.Create(ctx, id, []byte("sealed-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, id, []byte("again")); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("a second create = %v; want ErrPreconditionFailed", err)
	}
	body, got, err := s.Get(ctx, id)
	if err != nil || string(body) != "sealed-1" || got != etag {
		t.Fatalf("Get = %q %q %v", body, got, err)
	}
	next, err := s.Replace(ctx, id, []byte("sealed-2"), etag)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Replace(ctx, id, []byte("stale"), etag); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("a replace under a stale etag = %v; want ErrPreconditionFailed", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("deleting a record already gone = %v", err)
	}
	if _, err := s.Replace(ctx, id, []byte("after"), next); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("a replace after delete = %v; want ErrPreconditionFailed", err)
	}
	if _, ok := cas.Raw(SigninKey(id)); ok {
		t.Fatal("a replace recreated a deleted record")
	}
}

func TestSigninStore_RefusesWhatIsNotARecordID(t *testing.T) {
	s := NewMemorySigninStore(nil)
	for _, id := range []string{"", "../x", strings.Repeat("A", 64), strings.Repeat("a", 63)} {
		if _, err := s.Create(context.Background(), id, nil); err == nil {
			t.Errorf("Create(%q) accepted", id)
		}
	}
}

func TestSigninStore_List(t *testing.T) {
	ctx := context.Background()
	cas := NewMemoryCAS()
	s := NewMemorySigninStore(cas)
	ids := []string{strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)}
	for _, id := range ids {
		if _, err := s.Create(ctx, id, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	cas.PutRaw(SigninsPrefix+"not-a-record.json", []byte("x"))
	got, more, err := s.List(ctx, "")
	if err != nil || more || strings.Join(got, ",") != strings.Join(ids, ",") {
		t.Fatalf("List = %v %v %v", got, more, err)
	}
	got, _, err = s.List(ctx, ids[0])
	if err != nil || len(got) != 2 || got[0] != ids[1] {
		t.Fatalf("List after the first = %v %v", got, err)
	}
}

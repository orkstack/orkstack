package artifacts

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

var recordNames = []RecordName{
	{TaskID: "build"}, {TaskID: "build", Scope: 7},
	{TaskID: "fan", Item: ItemAt(0)}, {TaskID: "fan", Item: ItemAt(12345), Scope: 3},
	{TaskID: "gate", Kind: RecordResolution}, {TaskID: "gate", Kind: RecordResolution, Scope: 3},
	{TaskID: "sub", Kind: RecordComposed}, {Kind: RecordOutputsName},
}

// TestRecordName_Keys: a name's key is the key its writer uses, and reads
// back as the name.
func TestRecordName_Keys(t *testing.T) {
	for _, n := range recordNames {
		got, ok := ParseRecordName(n.Rel())
		if !ok || got != n {
			t.Errorf("%+v reads back %+v, %v", n, got, ok)
		}
		var want string
		var err error
		switch {
		case n.Kind == RecordOutputsName:
			want, err = OutputsRecordKey("ns", "ork-1")
		case n.Kind != RecordCommitName:
			want, err = RecordKey(n.Kind, "ns", "ork-1", n.TaskID, n.Scope)
		case n.Item.Set:
			want, err = ScopedItemCommitKey("ns", "ork-1", n.TaskID, n.Item.Index, n.Scope)
		default:
			want, err = ScopedCommitKey("ns", "ork-1", n.TaskID, n.Scope)
		}
		if err == nil && n.Kind == RecordCommitName {
			want, err = AttestedCommitKey(want)
		}
		if key, kerr := RecordKeyOf("ns", "ork-1", n); err != nil || kerr != nil || key != want {
			t.Errorf("%+v: %q, want %q (%v, %v)", n, key, want, err, kerr)
		}
	}
	for _, rel := range []string{"", "build", "build/", "/commit.json", "build/other.json", "build/r7/commit.json", "build/1/commit.json",
		"build/00001/resolution.json", "build/x/commit.json", "Build/commit.json", "build/r00000000000000000000/commit.json",
		"build/00001/r00000000000000000003/commit.json", "a/b/c/d/commit.json", "outputs.json/"} {
		if n, ok := ParseRecordName(rel); ok {
			t.Errorf("%q reads as %+v", rel, n)
		}
	}
}

func FuzzParseRecordName(f *testing.F) {
	for _, n := range recordNames {
		f.Add(n.Rel())
	}
	f.Add("build/r7/commit.json")
	f.Fuzz(func(t *testing.T, rel string) {
		if n, ok := ParseRecordName(rel); ok && n.Rel() != rel {
			t.Fatalf("%q reads as %+v, whose key is %q", rel, n, n.Rel())
		}
	})
}

// TestListRecords: in the order of their keys, a page at a time, of a run or
// of one of its tasks, and nothing that is no record or another run's.
func TestListRecords(t *testing.T) {
	ctx := context.Background()
	s, b := newTestRecordStore(t)
	var want []string
	for _, n := range recordNames {
		key, err := RecordKeyOf("ns", "ork-1", n)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.put(ctx, key, "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		want = append(want, n.Rel())
	}
	slices.Sort(want)
	for _, key := range []string{"metadata/attested/ns/ork-1/build/other.json", "metadata/attested/ns/ork-10/build/commit.json"} {
		if err := b.put(ctx, key, "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	after := ""
	for {
		names, reached, more, err := s.ListRecords(ctx, SlotList, "ns", "ork-1", "", after, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			got = append(got, n.Rel())
		}
		if !more {
			break
		}
		after = reached
	}
	if !slices.Equal(got, want) {
		t.Fatalf("listed\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	names, _, _, err := s.ListRecords(ctx, SlotList, "ns", "ork-1", "gate", "", 10)
	if err != nil || len(names) != 2 {
		t.Fatalf("%v, %v", names, err)
	}
}

// emptyPages is a store that says more follow a page with none.
type emptyPages struct{ blobs }

func (emptyPages) list(context.Context, blobListInput) (blobListPage, error) {
	return blobListPage{Truncated: true}, nil
}

// TestListRecords_APageOfNoneThatIsNotTheLast is not an answer: there is no key
// to go on from, and the listing is not whole.
func TestListRecords_APageOfNoneThatIsNotTheLast(t *testing.T) {
	s, b := newTestRecordStore(t)
	s.b = emptyPages{b}
	var un *RecordStoreUnanswered
	if names, _, more, err := s.ListRecords(context.Background(), SlotRun, "ns", "ork-1", "", "", 5); !errors.As(err, &un) || more || names != nil {
		t.Fatalf("%v, %v, %v", names, more, err)
	}
	if sha, err := s.AdmissionOf(context.Background(), "ns", "ork-1"); sha != "" || !errors.As(err, &un) {
		t.Fatalf("%q, %v", sha, err)
	}
}

func TestAdmissionOf(t *testing.T) {
	ctx := context.Background()
	s, b := newTestRecordStore(t)
	at := func(sha string) {
		key, err := AdmissionKey("ns", "ork-1", sha)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.put(ctx, key, "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	if sha, err := s.AdmissionOf(ctx, "ns", "ork-1"); sha != "" || err != nil {
		t.Fatalf("%q, %v", sha, err)
	}
	at(strings.Repeat("a", 64))
	if sha, err := s.AdmissionOf(ctx, "ns", "ork-1"); sha != strings.Repeat("a", 64) || err != nil {
		t.Fatalf("%q, %v", sha, err)
	}
	at(strings.Repeat("b", 64))
	if sha, err := s.AdmissionOf(ctx, "ns", "ork-1"); sha != "" || !errors.Is(err, ErrManyAdmissions) {
		t.Fatalf("%q, %v", sha, err)
	}
}

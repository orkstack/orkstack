package orker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestParseUnitsLabel(t *testing.T) {
	for _, tc := range []struct {
		label string
		want  []int
		ok    bool
	}{
		{"amd:0", []int{0}, true},
		{"amd:0,2", []int{0, 2}, true},
		{"amd:2,0", []int{0, 2}, true},
		{"intel:0", nil, false},  // another profile
		{"amd:", nil, false},     // no index
		{"amd:3", nil, false},    // out of range for 3 units
		{"amd:-1", nil, false},   // negative
		{"amd:0,0", nil, false},  // twice
		{"amd:01", nil, false},   // not canonical
		{"amd 0", nil, false},    // no colon
		{"", nil, false},         // none
		{"amd:0,,1", nil, false}, // empty index
	} {
		got, ok := parseUnitsLabel(tc.label, "amd", 3)
		if ok != tc.ok || !slices.Equal(got, tc.want) {
			t.Errorf("parseUnitsLabel(%q) = %v, %v; want %v, %v", tc.label, got, ok, tc.want, tc.ok)
		}
	}
}

// FuzzParseUnitsLabel: an accepted label names distinct in-range indices,
// ascending, and renders back to an equivalent label.
func FuzzParseUnitsLabel(f *testing.F) {
	for _, seed := range []string{"amd:0", "amd:0,2", "amd:", "intel:0", "amd:01", "amd:0,0", ""} {
		f.Add(seed, 3)
	}
	f.Fuzz(func(t *testing.T, label string, n int) {
		n = min(max(n, 0), 64)
		got, ok := parseUnitsLabel(label, "amd", n)
		if !ok {
			if got != nil {
				t.Fatalf("a refused label returned %v", got)
			}
			return
		}
		if len(got) == 0 || !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(got) {
			t.Fatalf("indices %v are not distinct and ascending", got)
		}
		for _, i := range got {
			if i < 0 || i >= n {
				t.Fatalf("index %d out of range %d", i, n)
			}
		}
		again, ok := parseUnitsLabel(unitsLabel("amd", got), "amd", n)
		if !ok || !slices.Equal(again, got) {
			t.Fatalf("rendered %q parsed to %v, %v", unitsLabel("amd", got), again, ok)
		}
	})
}

var unitContainerLines = "c1\texited\tB:1:2\tamd:0\torker-a\tork-r\tbuild\t\t2\n" +
	"c2\trunning\tB:9:9\tamd:1\torker-b\tork-s\ttrain\t3\t1\n" +
	"short\tline\n" +
	"\trunning\tB:1:2\tamd:0\torker-a\tr\tt\t\t1\n"

func TestParseUnitContainers(t *testing.T) {
	got := parseUnitContainers(unitContainerLines)
	if len(got) != 2 {
		t.Fatalf("parsed %d containers, want 2: %+v", len(got), got)
	}
	want := UnitContainer{Lock: "B:1:2", Units: "amd:0", Orker: "orker-a", State: "exited",
		OwnContainer: OwnContainer{ID: "c1", RunID: "ork-r", TaskID: "build", Attempt: "2"}}
	if got[0] != want {
		t.Fatalf("first = %+v, want %+v", got[0], want)
	}
	if got[1].Item != "3" || got[1].Lock != "B:9:9" || got[1].State != "running" {
		t.Fatalf("second = %+v", got[1])
	}
}

// FuzzParseUnitContainers: every parsed container has an id and came from one
// line of nine tab-separated fields.
func FuzzParseUnitContainers(f *testing.F) {
	f.Add(unitContainerLines)
	f.Add("")
	f.Fuzz(func(t *testing.T, answer string) {
		for _, c := range parseUnitContainers(answer) {
			if c.ID == "" || strings.ContainsAny(c.ID+c.Lock+c.Units, "\t\n") {
				t.Fatalf("parsed %+v from %q", c, answer)
			}
		}
	})
}

type fakeUnitReap struct {
	listed   []UnitContainer
	listErr  error
	failRm   map[string]bool
	removed  []string
	released []artifacts.AttemptRef
	logged   []string
}

func (f *fakeUnitReap) deps(c *Capacity) unitReapDeps {
	return unitReapDeps{
		list: func(context.Context) ([]UnitContainer, error) { return f.listed, f.listErr },
		remove: func(_ context.Context, id ContainerID) error {
			if f.failRm[string(id)] {
				return errors.New("daemon busy")
			}
			f.removed = append(f.removed, string(id))
			return nil
		},
		release: func(_ context.Context, ref artifacts.AttemptRef) error {
			f.released = append(f.released, ref)
			return nil
		},
		log:      func(msg string, _ ...any) { f.logged = append(f.logged, msg) },
		capacity: c,
	}
}

// TestReapUnits_ActsOnlyUnderItsLock: a container under the held lock is
// removed, whichever Orker labelled it, and its attempt released; one under
// another lock is logged and never touched.
func TestReapUnits_ActsOnlyUnderItsLock(t *testing.T) {
	c, _ := gpuLedger(t, 2, 2, 1)
	f := &fakeUnitReap{listed: []UnitContainer{
		{Lock: "B:1:2", Units: "amd:0", Orker: "orker-old", OwnContainer: OwnContainer{ID: "mine", RunID: "ork-r", TaskID: "t", Attempt: "1"}},
		{Lock: "B:9:9", Units: "amd:1", Orker: "orker-x", State: "exited", OwnContainer: OwnContainer{ID: "theirs", RunID: "ork-s", TaskID: "t", Attempt: "1"}},
	}}
	foreign, err := reapUnitsUnderLock(context.Background(), "B:1:2", "amd", 2, f.deps(c))
	if err != nil {
		t.Fatal(err)
	}
	if len(foreign) != 0 {
		t.Fatalf("an exited container under another lock is foreign: %v", foreign)
	}
	if !slices.Equal(f.removed, []string{"mine"}) {
		t.Fatalf("removed %v, want only [mine]", f.removed)
	}
	if len(f.released) != 1 || f.released[0].RunID != "ork-r" {
		t.Fatalf("released %+v, want the removed container's attempt", f.released)
	}
	if len(f.logged) == 0 {
		t.Fatal("the container under another lock was not logged")
	}
	wantGPUReserved(t, c, "nothing seeded", 0)
}

// TestReapUnits_SeedsExactIndicesOnFailedRemove: a removal that fails seeds
// the container's exact units, and releases nothing for it.
func TestReapUnits_SeedsExactIndicesOnFailedRemove(t *testing.T) {
	c, _ := gpuLedger(t, 2, 3, 1)
	f := &fakeUnitReap{failRm: map[string]bool{"stuck": true}, listed: []UnitContainer{
		{Lock: "L", Units: "amd:1,2", OwnContainer: OwnContainer{ID: "stuck", RunID: "r", TaskID: "t", Attempt: "1"}},
	}}
	if _, err := reapUnitsUnderLock(context.Background(), "L", "amd", 3, f.deps(c)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	hold := c.unremoved["stuck"]
	c.mu.Unlock()
	if !slices.Equal(hold.units, []int{1, 2}) || len(f.released) != 0 {
		t.Fatalf("seeded %v, released %v; want [1 2] and nothing", hold.units, f.released)
	}
	wantGPUReserved(t, c, "seeded", 2)
}

// TestReapUnits_UnreadableLabelSeedsEveryUnit fails closed.
func TestReapUnits_UnreadableLabelSeedsEveryUnit(t *testing.T) {
	c, _ := gpuLedger(t, 2, 3, 1)
	f := &fakeUnitReap{failRm: map[string]bool{"odd": true}, listed: []UnitContainer{
		{Lock: "L", Units: "amd:7", OwnContainer: OwnContainer{ID: "odd"}},
	}}
	if _, err := reapUnitsUnderLock(context.Background(), "L", "amd", 3, f.deps(c)); err != nil {
		t.Fatal(err)
	}
	wantGPUReserved(t, c, "every unit seeded", 3)
	if len(f.logged) == 0 {
		t.Fatal("the unreadable label was not logged")
	}
}

// TestReapUnits_ListFailureIsReturned: a reap that cannot list says so; boot
// then holds the profile out of service rather than bind beside a container
// it could not see.
func TestReapUnits_ListFailureIsReturned(t *testing.T) {
	c, _ := gpuLedger(t, 2, 1, 1)
	f := &fakeUnitReap{listErr: errors.New("daemon down")}
	if _, err := reapUnitsUnderLock(context.Background(), "L", "amd", 1, f.deps(c)); err == nil {
		t.Fatal("a list failure was not returned")
	}
}

// TestReapUnits_MergesWithTheByIDSeed: the by-id pass seeded cpu and memory
// for a container it could not remove; the lock pass adds its units to the
// same entry, and one later removal frees both.
func TestReapUnits_MergesWithTheByIDSeed(t *testing.T) {
	c, _ := gpuLedger(t, 2, 2, 1)
	c.seed("both")
	f := &fakeUnitReap{failRm: map[string]bool{"both": true}, listed: []UnitContainer{
		{Lock: "L", Units: "amd:1", OwnContainer: OwnContainer{ID: "both"}},
	}}
	if _, err := reapUnitsUnderLock(context.Background(), "L", "amd", 2, f.deps(c)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	hold := c.unremoved["both"]
	c.mu.Unlock()
	if hold.held.milli != 100 || hold.held.gpu != 1 {
		t.Fatalf("merged hold = %+v, want the cpu/memory seed and one unit", hold)
	}
	c.reap(context.Background(), func(context.Context, ContainerID) error { return nil })
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reserved != (amount{}) {
		t.Fatalf("reserved after removal = %v", c.reserved)
	}
}

// TestReapUnits_RunningUnderAnotherLockIsForeign: a running container bound
// under another lock (the lock directory recreated while it ran) is left
// alone and returned, so the profile holds FOREIGN_UNITS_HELD until it is
// gone; one of another profile, or exited, is not.
func TestReapUnits_RunningUnderAnotherLockIsForeign(t *testing.T) {
	c, _ := gpuLedger(t, 2, 2, 1)
	f := &fakeUnitReap{listed: []UnitContainer{
		{Lock: "OLD", Units: "amd:0", State: "running", OwnContainer: OwnContainer{ID: "still"}},
		{Lock: "OLD", Units: "intel:0", State: "running", OwnContainer: OwnContainer{ID: "other-profile"}},
		{Lock: "OLD", Units: "amd:1", State: "exited", OwnContainer: OwnContainer{ID: "done"}},
	}}
	foreign, err := reapUnitsUnderLock(context.Background(), "NEW", "amd", 2, f.deps(c))
	if err != nil {
		t.Fatal(err)
	}
	if len(foreign) != 1 || foreign[0].ID != "still" || len(f.removed) != 0 {
		t.Fatalf("foreign %v, removed %v; want [still] and nothing removed", foreign, f.removed)
	}
}

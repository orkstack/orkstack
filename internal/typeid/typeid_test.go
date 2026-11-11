package typeid

import (
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// shape is the whole contract a reader may hold an id to: 26 characters of the
// TypeID alphabet, the first of which carries only three bits.
var shape = regexp.MustCompile(`^[0-7][0-9a-hjkmnp-tv-z]{25}$`)

// TestEncode_MatchesTheTypeIDSpecification holds encode to the specification's
// own vectors (github.com/jetify-com/typeid, spec/valid.yml), so an id here is
// a TypeID suffix any other implementation reads the same way.
func TestEncode_MatchesTheTypeIDSpecification(t *testing.T) {
	cases := []struct{ uuid, want string }{
		{"00000000-0000-0000-0000-000000000000", "00000000000000000000000000"},
		{"00000000-0000-0000-0000-000000000001", "00000000000000000000000001"},
		{"00000000-0000-0000-0000-00000000000a", "0000000000000000000000000a"},
		{"00000000-0000-0000-0000-000000000010", "0000000000000000000000000g"},
		{"00000000-0000-0000-0000-000000000020", "00000000000000000000000010"},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", "7zzzzzzzzzzzzzzzzzzzzzzzzz"},
		{"01890a5d-ac96-774b-bcce-b302099a8057", "01h455vb4pex5vsknk084sn02q"},
	}
	for _, tc := range cases {
		id, err := uuid.Parse(tc.uuid)
		must.OKf(t, err, "parse %q", tc.uuid)
		if got := encode(id); got != tc.want {
			t.Errorf("encode(%s) = %q, want %q", tc.uuid, got, tc.want)
		}
	}
}

// TestEncode_PreservesOrder: comparing two encodings answers what comparing the
// two UUIDs does, which is what makes the time order of a UUIDv7 survive.
func TestEncode_PreservesOrder(t *testing.T) {
	values := []string{
		"00000000-0000-0000-0000-000000000000",
		"00000000-0000-0000-0000-00000000001f",
		"00000000-0000-0000-0000-000000000020",
		"00000000-0000-0000-0001-000000000000",
		"00000001-0000-0000-0000-000000000000",
		"7fffffff-ffff-ffff-ffff-ffffffffffff",
		"80000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}
	for i := 1; i < len(values); i++ {
		a, b := encode(uuid.MustParse(values[i-1])), encode(uuid.MustParse(values[i]))
		if a >= b {
			t.Errorf("encode(%s) = %q does not sort before encode(%s) = %q", values[i-1], a, values[i], b)
		}
	}
}

func TestNew_HasTheDocumentedShape(t *testing.T) {
	id := New()
	if len(id) != Length || !shape.MatchString(id) {
		t.Fatalf("New() = %q, want %d characters matching %s", id, Length, shape)
	}
}

// TestNew_IsStrictlyIncreasingWithinAProcess measures the property the run id
// generator used to get from its own counter: in one goroutine, far faster than
// the clock's resolution, every id sorts after the one before it.
func TestNew_IsStrictlyIncreasingWithinAProcess(t *testing.T) {
	const ids = 200_000
	previous := New()
	for i := range ids {
		next := New()
		if next <= previous {
			t.Fatalf("id %d = %q does not sort after %q", i, next, previous)
		}
		previous = next
	}
}

// TestNew_ConcurrentCallersNeverShareOrReverseAnID: goroutines racing for the
// generator each still see their own ids strictly increase, and no two callers
// are handed one id.
func TestNew_ConcurrentCallersNeverShareOrReverseAnID(t *testing.T) {
	const (
		goroutines = 8
		perCaller  = 25_000
	)
	issued := make([][]string, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			ids := make([]string, perCaller)
			for i := range ids {
				ids[i] = New()
			}
			issued[g] = ids
		})
	}
	wg.Wait()

	seen := make(map[string]struct{}, goroutines*perCaller)
	for g, ids := range issued {
		if !slices.IsSorted(ids) {
			t.Errorf("goroutine %d's ids are not in order", g)
		}
		for _, id := range ids {
			if _, clash := seen[id]; clash {
				t.Fatalf("id %q was issued twice", id)
			}
			seen[id] = struct{}{}
		}
	}
}

// TestFallbackV7_IsAWellFormedUUIDv7 covers the branch crypto/rand no longer
// reaches: it stays a version 7, RFC variant UUID carrying the millisecond
// clock, and two calls of one process or of two processes differ.
func TestFallbackV7_IsAWellFormedUUIDv7(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 123456789, time.UTC)
	id := fallbackV7(now, 4242, 7)
	if id.Version() != 7 {
		t.Errorf("version = %d, want 7", id.Version())
	}
	if id.Variant() != uuid.RFC4122 {
		t.Errorf("variant = %v, want RFC4122", id.Variant())
	}
	sec, nsec := id.Time().UnixTime()
	if got := time.Unix(sec, nsec).UnixMilli(); got != now.UnixMilli() {
		t.Errorf("embedded millisecond = %d, want %d", got, now.UnixMilli())
	}
	if fallbackV7(now, 4242, 8) == id {
		t.Error("two ids of one process share a value")
	}
	if fallbackV7(now, 4243, 7) == id {
		t.Error("two processes share a value")
	}
	if encoded := encode(id); !shape.MatchString(encoded) {
		t.Errorf("encode = %q, want a match for %s", encoded, shape)
	}
}

// BenchmarkNew reports what one id costs, the number quoted beside the
// monotonicity measurement.
func BenchmarkNew(b *testing.B) {
	for b.Loop() {
		_ = New()
	}
}

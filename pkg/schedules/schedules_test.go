package schedules

import (
	"strings"
	"testing"
)

func TestNewID_ShapeAndValidID(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	if !strings.HasPrefix(id, "sch-") || len(id) != len("sch-")+16 {
		t.Fatalf("NewID() = %q, want sch-{16 hex}", id)
	}
	if !ValidID(id) {
		t.Fatalf("ValidID(%q) = false for a generated id", id)
	}
	other, err := NewID()
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	if other == id {
		t.Fatalf("two NewID calls returned the same id %q", id)
	}
	for _, bad := range []string{
		"",
		"sch-",
		"trg-4a1c0e2b7d3f8156",
		"sch-4A1C0E2B7D3F8156",
		"sch-4a1c0e2b7d3f815",
		"sch-4a1c0e2b7d3f81567",
		"sch-4a1c0e2b7d3f815g",
		"xsch-4a1c0e2b7d3f8156",
	} {
		if ValidID(bad) {
			t.Errorf("ValidID(%q) = true, want false", bad)
		}
	}
	if !ValidID("sch-4a1c0e2b7d3f8156") {
		t.Errorf("ValidID refused a well-formed id")
	}
}

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

// A schedule's zone is an IANA name. Go also reads "Local" as the zone of the
// machine it runs on, which is a different instant on every host.
func TestValidTimezone_IsAnIANANameAndNotTheHostsOwn(t *testing.T) {
	for _, name := range []string{"UTC", "Europe/Lisbon", "America/Argentina/Buenos_Aires", "Etc/GMT+5"} {
		if err := ValidTimezone(name); err != nil {
			t.Errorf("ValidTimezone(%q) = %v, want an accepted zone", name, err)
		}
	}
	for _, name := range []string{"", "Local", "Mars/Olympus", "../zoneinfo/UTC", "/etc/localtime", "Europe/../UTC", "UTC\x00", "Europe/Lisbon "} {
		if err := ValidTimezone(name); err == nil {
			t.Errorf("ValidTimezone(%q) accepted a name that is not an IANA zone", name)
		}
	}
	if err := ValidTimezone("Local"); err == nil || !strings.Contains(err.Error(), "machine") {
		t.Errorf("ValidTimezone(Local) = %v, want it to say the zone is the machine's own", err)
	}
}

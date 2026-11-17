package orker

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestMemoKey_CoversTheProfiles: a cached task's memo key covers each
// profile it names, by name, identity and revision; a task naming none keeps
// the key it always had.
func TestMemoKey_CoversTheProfiles(t *testing.T) {
	in, _ := pinnedKeyInput()
	config, run := []byte("{}\n"), []byte("{}\n")
	key := func(entries []ProfileMemoEntry) string {
		t.Helper()
		k, ok := computeMemoKey(in, config, run, nil, nil, entries)
		if !ok {
			t.Fatal("not keyable")
		}
		return k.Key
	}
	plain := key(nil)
	if empty := key([]ProfileMemoEntry{}); empty != plain {
		t.Fatal("an empty profile list changed the key of a task naming none")
	}
	base := []ProfileMemoEntry{{Name: "sensor", Identity: gateIdentity, Revision: "a"}}
	withSensor := key(base)
	if withSensor == plain {
		t.Fatal("a profile left the key unchanged")
	}
	for name, entries := range map[string][]ProfileMemoEntry{
		"another identity": {{Name: "sensor", Identity: strings.Repeat("f", 64), Revision: "a"}},
		"another revision": {{Name: "sensor", Identity: gateIdentity, Revision: "b"}},
		"another profile":  {{Name: "modem", Identity: gateIdentity, Revision: "a"}},
	} {
		if key(entries) == withSensor {
			t.Errorf("%s left the key unchanged", name)
		}
	}
}

// TestProfileMemoEntries_FromTheTaskAndThisOrker: the entries are the
// task's refs with this Orker's revision of each profile.
func TestProfileMemoEntries_FromTheTaskAndThisOrker(t *testing.T) {
	declared := sensorProfiles(t, "")
	declared.List[0].Revision = "r7"
	d := &OCIDispatcher{profiles: newProfileState(declared, "")}
	in := &orkstackv1.ExecuteTaskInput{Devices: []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: gateIdentity}}}
	got := d.profileMemoEntries(in)
	if len(got) != 1 || got[0] != (ProfileMemoEntry{Name: "sensor", Identity: gateIdentity, Revision: "r7"}) {
		t.Fatalf("entries = %+v", got)
	}
	if d.profileMemoEntries(&orkstackv1.ExecuteTaskInput{}) != nil {
		t.Fatal("a task naming no profile has entries")
	}
}

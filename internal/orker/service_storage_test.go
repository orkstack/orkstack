package orker

import "testing"

// TestBootServiceStorage: a source host keeps its services' files where its
// identity files are, when that is memory; on disk, or with nowhere at all, it
// boots and is unfit, saying so. An Orker that hosts none chooses nothing.
func TestBootServiceStorage(t *testing.T) {
	for name, tc := range map[string]struct {
		hosts  bool
		choice identityStorageChoice
		want   string
	}{
		"memory":          {true, identityStorageChoice{Dir: "/run/user/1/orkstack", Kind: identityStorageMemory}, "/run/user/1/orkstack"},
		"disk":            {true, identityStorageChoice{Dir: "/var/lib/orkstack", Kind: identityStorageDisk}, ""},
		"the work root":   {true, identityStorageChoice{Kind: identityStorageDisk}, ""},
		"hosts no source": {false, identityStorageChoice{Dir: "/run/user/1/orkstack", Kind: identityStorageMemory}, ""},
	} {
		h := newPolicyHarness(t)
		a := &Activities{dispatcher: h.d}
		if tc.hosts {
			a.sourceHost = SourceHostSettings{MaxInstances: 1}
		}
		h.d.identityStorage.Store(&tc.choice)
		a.bootServiceStorage(t.Context(), t.TempDir())
		if got := a.services.storageDir(); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}

// TestSwapsOn: a host swaps where its table has a device below its heading.
func TestSwapsOn(t *testing.T) {
	for table, want := range map[string]bool{
		"": false, "Filename\tType\tSize\tUsed\tPriority\n": false,
		"Filename\tType\tSize\tUsed\tPriority\n/dev/dm-1 partition 1024 0 -2\n": true,
	} {
		if got := swapsOn(table); got != want {
			t.Errorf("%q: %v", table, got)
		}
	}
}

package artifacts

import (
	"path"
	"testing"
)

// A run-index key whose status segment NormalizeRunStatus cannot produce was
// not written by this package, so each family's parser refuses it: the nightly
// fuzz lane found "A", which the writer spells "a", read back as a run whose
// cursor is another key (testdata/fuzz/FuzzKeys_AKeyTheParserAcceptsIsTheKeyTheBuilderWrites).
// A status the writer can produce, one that itself begins "status_" included,
// is read and its newest-first cursor is exactly the stored key.
func TestRunIndexParsers_ReadOnlyAStatusTheWriterSpells(t *testing.T) {
	const stamp = "20260102T030405.000000006Z"
	seg := func(raw string) string {
		t.Helper()
		s, err := encodeArchiveSegment(raw)
		if err != nil {
			t.Fatalf("encodeArchiveSegment(%q): %v", raw, err)
		}
		return s
	}
	origin, err := encodeOriginIDSegment("trg-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		status string
		read   bool
	}{
		{"succeeded", true},
		{"status_succeeded", true}, // NormalizeRunStatus("STATUS_STATUS_SUCCEEDED")
		{"a", true},
		{"A", false},
		{"Succeeded", false},
		{"STATUS_SUCCEEDED", false},
		{"succeeded ", false},
	} {
		status := seg(c.status)
		keys := map[string]string{
			"run index":    path.Join(RunIndexNamespace, "payments", "nightly", status, stamp, "ork-1-1"),
			"recent index": path.Join(RunRecentIndexNamespace, "payments", invertStamp(stamp), "nightly", status, "ork-1-1"),
			"origin index": path.Join(RunOriginIndexNamespace, "payments", "trigger", origin, "nightly", status, stamp, "ork-1-1"),
		}
		for family, key := range keys {
			var entry RunIndexEntry
			var ok bool
			switch family {
			case "run index":
				entry, ok = ParseRunIndexKey(key)
			case "recent index":
				entry, ok = ParseRunRecentIndexKey(key)
			default:
				entry, ok = ParseRunOriginIndexKey(key)
			}
			if ok != c.read {
				t.Errorf("%s %q: read = %v, want %v", family, key, ok, c.read)
				continue
			}
			if !ok {
				continue
			}
			var rebuilt string
			switch family {
			case "run index":
				rebuilt, err = RunIndexKeyForEntry(entry)
			case "recent index":
				rebuilt, err = RunRecentIndexKeyForEntry(entry)
			default:
				continue // no cursor is built from an origin key
			}
			if err != nil || rebuilt != key {
				t.Errorf("%s %q: the cursor for %+v is %q (%v), want the stored key", family, key, entry, rebuilt, err)
			}
		}
	}
}

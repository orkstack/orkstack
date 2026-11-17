package runstatus

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Every Status value is either live or terminal, never both and never
// neither, so a value added to the proto cannot be missed by a reader that
// asks this package. UNSPECIFIED is the absence of a status and is neither.
func TestRunStatus_EveryValueClassified(t *testing.T) {
	values := orkstackv1.Status(0).Descriptor().Values()
	for i := range values.Len() {
		status := orkstackv1.Status(values.Get(i).Number())
		live, terminal := IsLive(status), IsTerminal(status)
		if status == orkstackv1.Status_STATUS_UNSPECIFIED {
			if live || terminal {
				t.Errorf("%s: live %v, terminal %v; UNSPECIFIED is neither", status, live, terminal)
			}
			continue
		}
		if live == terminal {
			t.Errorf("%s: live %v, terminal %v; want exactly one", status, live, terminal)
		}
	}
}

// A paused run is live: readers keep waiting on it and count it open.
func TestRunStatus_PausedIsLive(t *testing.T) {
	if !IsLive(orkstackv1.Status_STATUS_PAUSED) || IsTerminal(orkstackv1.Status_STATUS_PAUSED) {
		t.Fatal("PAUSED must be live and not terminal")
	}
}

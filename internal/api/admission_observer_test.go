package api_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// recordingAdmissionObserver stands in for telemetry.EngineObserver on the
// refusal seam: every ObserveRefusal it is handed, in order.
type recordingAdmissionObserver struct {
	mu       sync.Mutex
	refusals []observedRefusal
}

type observedRefusal struct {
	namespace string
	telemetry *orkstackv1.TelemetrySelection
	orkflow   string
	reason    string
}

func (o *recordingAdmissionObserver) ObserveRefusal(
	_ context.Context,
	namespace string,
	telemetry *orkstackv1.TelemetrySelection,
	orkflow string,
	reason string,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.refusals = append(o.refusals, observedRefusal{namespace, telemetry, orkflow, reason})
}

func (o *recordingAdmissionObserver) seen() []observedRefusal {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observedRefusal(nil), o.refusals...)
}

var _ engine.AdmissionObserver = (*recordingAdmissionObserver)(nil)

// newFixtureWithAdmissions is newAdmissionFixture with refusals announced to
// admissions, in the shape of newFixtureWithObserver. A nil admissions leaves
// Config.Admissions unset.
func newFixtureWithAdmissions(t *testing.T, admissions *recordingAdmissionObserver) *admissionFixture {
	t.Helper()
	return newAdmissionFixture(t, func(cfg *api.Config) {
		if admissions != nil {
			cfg.Admissions = admissions
		}
	})
}

func TestSubmitRun_RefusalIsLoggedAndCounted(t *testing.T) {
	for _, wired := range []bool{true, false} {
		t.Run(fmt.Sprintf("admissions wired=%v", wired), func(t *testing.T) {
			recorder := &recordingAdmissionObserver{}
			var wiredRecorder *recordingAdmissionObserver
			if wired {
				wiredRecorder = recorder
			}
			f := newFixtureWithAdmissions(t, wiredRecorder)

			// Admitted: no call, no line.
			if _, err := f.submit("", false); err != nil {
				t.Fatalf("admitted submit: %v", err)
			}
			if got := recorder.seen(); len(got) != 0 {
				t.Fatalf("an admitted submit was announced: %v", got)
			}
			if strings.Contains(f.logs.String(), "run refused") {
				t.Fatalf("an admitted submit logged a refusal: %s", f.logs.String())
			}

			held := f.hold(t, heldReason)
			_, heldErr := f.submit("", false)
			requireRefusal(t, heldErr, connect.CodeFailedPrecondition, heldReason)
			f.release(t)
			f.setCap(t, 1)
			_, capErr := f.submit("", false)
			requireRefusal(t, capErr, connect.CodeFailedPrecondition, "already has 1 runs in flight")

			lines := refusalLines(f.logs.String())
			if len(lines) != 2 {
				t.Fatalf("run refused lines = %d, want 2:\n%s", len(lines), f.logs.String())
			}
			since := held.GetHold().GetSince().AsTime().UTC().Format(time.RFC3339)
			for _, want := range []string{"level=INFO", "namespace=payments", "orkflow=reconcile",
				"reason=NAMESPACE_HELD", "hold_since=" + since} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("held line %q lacks %q", lines[0], want)
				}
			}
			for _, want := range []string{"level=INFO", "namespace=payments", "orkflow=reconcile",
				"reason=MAX_RUNNING_REACHED", "running=1", "max_running=1"} {
				if !strings.Contains(lines[1], want) {
					t.Errorf("cap line %q lacks %q", lines[1], want)
				}
			}

			seen := recorder.seen()
			if !wired {
				if len(seen) != 0 {
					t.Fatalf("an unwired seam recorded %v", seen)
				}
				return
			}
			if len(seen) != 2 {
				t.Fatalf("ObserveRefusal calls = %d, want 2: %+v", len(seen), seen)
			}
			for i, want := range []string{"NAMESPACE_HELD", "MAX_RUNNING_REACHED"} {
				got := seen[i]
				if got.namespace != "payments" || got.orkflow != "reconcile" || got.reason != want {
					t.Errorf("call %d = {%s %s %s}, want {payments reconcile %s}",
						i, got.namespace, got.orkflow, got.reason, want)
				}
				if got.telemetry.GetSink() != "b" {
					t.Errorf("call %d sink = %q, want b", i, got.telemetry.GetSink())
				}
				attrs := got.telemetry.GetAttributes()
				wantAttrs := map[string]string{"ork.ns.team": "payments", "ork.label.tier": "gold"}
				if len(attrs) != len(wantAttrs) {
					t.Errorf("call %d attributes = %v, want exactly %v", i, attrs, wantAttrs)
				}
				for key, value := range wantAttrs {
					if attrs[key] != value {
						t.Errorf("call %d attributes[%q] = %q, want %q", i, key, attrs[key], value)
					}
				}
				for key := range attrs {
					if !strings.HasPrefix(key, "ork.ns.") && !strings.HasPrefix(key, "ork.label.") {
						t.Errorf("call %d carries the unprefixed key %q: the raw policy leaked", i, key)
					}
				}
			}
		})
	}
}

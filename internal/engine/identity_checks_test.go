package engine

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestNewIdentityChecks_WiresWhatTheEngineChecks: the one wiring the engine
// and its tests share reads a first start's plan, reads admissions through
// its cache for the follows check, and gives the recorder every part it
// asks of the cluster and the records.
func TestNewIdentityChecks_WiresWhatTheEngineChecks(t *testing.T) {
	records, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	// A client that dials nothing until it is asked: the wiring alone is held.
	temporal, err := client.NewLazyClient(ClientOptions(client.Options{HostPort: "127.0.0.1:1"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(temporal.Close)
	checks := NewIdentityChecks(records, temporal, 1<<20)
	if checks.Starts.Plans == nil || checks.Starts.RecordStore != records {
		t.Fatalf("starts = %+v; want the records and a first start's plan read", checks.Starts)
	}
	if store, ok := checks.Follows.Store.(cachedAdmissions); !ok || store.admissions != checks.Admissions || store.RecordStore != records {
		t.Fatalf("the follows check reads %T; want admissions through the checks' cache", checks.Follows.Store)
	}
	rec := checks.Recorder(records, func(context.Context, artifacts.Admission, string) (string, error) { return "", nil }, "b", time.Now)
	if st, ok := rec.Starts.(identity.ClusterStarts); !ok || st.Plans == nil || rec.Describe == nil || rec.Follows != checks.Follows || rec.PinnedImage == nil || rec.Now == nil || rec.EngineBuild != "b" {
		t.Fatalf("recorder = %+v", rec)
	}
	if s, ok := rec.Stores.(recorderStores); !ok || s.admissions != checks.Admissions || s.records != records {
		t.Fatalf("the recorder's stores are %T", rec.Stores)
	}
}

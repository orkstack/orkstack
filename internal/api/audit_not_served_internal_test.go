package api

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestAudit_NotYetServedWritesAreNotMounted holds the one reason a write may
// stay unrecorded for now: no engine serves it. Every write whose reason is
// notYetServed must answer 404 from the real mux, so the change that mounts
// its service fails here until it records the write (auditedProcedures) or
// states another reason.
func TestAudit_NotYetServedWritesAreNotMounted(t *testing.T) {
	svc := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus()})
	mux := NewMux(svc, MuxOptions{})
	var procedures []string
	for procedure, reason := range unauditedWrites {
		if reason == notYetServed {
			procedures = append(procedures, procedure)
		}
	}
	sort.Strings(procedures)
	if len(procedures) == 0 {
		t.Fatal("no write is marked notYetServed; delete this test with the constant")
	}
	call := func(procedure string) int {
		req := httptest.NewRequest(http.MethodPost, procedure, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}
	// Control: the same request to a served procedure is not a 404, so a 404
	// below means "not mounted", not "malformed".
	if code := call(orkstackv1connect.SystemServiceGetSystemInfoProcedure); code == http.StatusNotFound {
		t.Fatalf("a served procedure answered 404 (%d): the probe does not reach the mux's routes", code)
	}
	for _, procedure := range procedures {
		if code := call(procedure); code != http.StatusNotFound {
			t.Errorf("%s is marked notYetServed but the mux answers %d: record it in auditedProcedures (internal/api/audit.go) before serving it", procedure, code)
		}
	}
}

package api

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestReadStarter_AnEventSource: a run an event source's message starts is
// judged by the binder and the consent that source records, read under the
// name and the id its origin carries, and by no other object's. What cannot
// be read is unavailable; what is not this source's is a starter with no
// binder, which is refused.
func TestReadStarter_AnEventSource(t *testing.T) {
	const id = "evs-0123456789abcdef"
	ctx := context.Background()
	origin := func(sourceID string) *orkstackv1.RunOrigin {
		return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE, Id: sourceID, Description: "orders"}
	}
	for _, c := range []struct {
		name        string
		origin      *orkstackv1.RunOrigin
		msg         *orkstackv1.SubmitRunRequest
		version     string // the version the source pins
		absent      bool
		damaged     bool
		namespace   string // the run's; "" is the source's, default
		binder      string
		unavailable bool
	}{
		{name: "its own", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, binder: "alice"},
		{name: "the current version, resolved", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy", OrkflowVersion: "2"}, binder: "alice"},
		{name: "the version it pins", origin: origin(id), version: "1", msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy", OrkflowVersion: "1"}, binder: "alice"},
		{name: "another version than it pins", origin: origin(id), version: "1", msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy", OrkflowVersion: "2"}},
		{name: "the current version where it pins one", origin: origin(id), version: "1", msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}},
		{name: "another source under the name", origin: origin("evs-fedcba9876543210"), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}},
		{name: "an origin with no id", origin: origin(""), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}},
		{name: "another namespace's run", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, namespace: "payments"},
		{name: "another Orkflow", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "other"}},
		{name: "no such source", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, absent: true},
		{name: "a damaged document", origin: origin(id), msg: &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, damaged: true, unavailable: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := artifacts.NewLocalEventSourceStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !c.absent {
				if _, err := store.Create(ctx, &orkstackv1.EventSource{Namespace: "default", Name: "orders", Id: id, OrkflowName: "deploy",
					OrkflowVersion: c.version, BoundBy: "alice", BoundByVerified: true, Consented: &orkstackv1.ConsentView{Digest: "d1"}}); err != nil {
					t.Fatal(err)
				}
			}
			if c.damaged {
				damageDocument(t, dir)
			}
			s := &Service{eventSources: store}
			st, err := s.readStarter(ctx, cmp.Or(c.namespace, "default"), c.msg, c.origin)
			if c.unavailable {
				if connect.CodeOf(err) != connect.CodeUnavailable {
					t.Fatalf("%+v, %v; want unavailable", st, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if st.binding != nil {
				t.Fatalf("a binding, %+v, that no source records", st.binding)
			}
			if st.kind != "event-source" || st.name != "orders" || st.binder != c.binder || st.verified != (c.binder != "") {
				t.Fatalf("%+v; want event-source orders with binder %q", st, c.binder)
			}
			if want := map[bool]string{true: "d1"}[c.binder != ""]; st.consent.Digest != want {
				t.Fatalf("consent %q; want %q", st.consent.Digest, want)
			}
		})
	}
	// No store, as with the feature off: a starter with no binder.
	st, err := (&Service{}).readStarter(ctx, "default", &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, origin(id))
	if err != nil || st.binder != "" || st.kind != "event-source" {
		t.Fatalf("no store: %+v, %v", st, err)
	}
}

// TestStartedByAnObject: a message's run is an object's start, so its binder
// is judged; it is never taken for a person's submit.
func TestStartedByAnObject(t *testing.T) {
	for kind, want := range map[orkstackv1.RunOriginKind]bool{
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE: true,
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE:     true,
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER:      true,
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT:       false,
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:    false,
	} {
		if got, err := startedByAnObject(&orkstackv1.RunOrigin{Kind: kind}); err != nil || got != want {
			t.Errorf("%v: %v, %v; want %v", kind, got, err, want)
		}
	}
}

// damageDocument overwrites the one event source document under dir.
func damageDocument(t *testing.T, dir string) {
	t.Helper()
	damaged := 0
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "orders.json") {
			return err
		}
		damaged++
		return os.WriteFile(path, []byte("{"), 0o600)
	}); err != nil || damaged != 1 {
		t.Fatalf("damaged %d documents: %v", damaged, err)
	}
}

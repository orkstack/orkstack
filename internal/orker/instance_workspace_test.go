package orker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

func workspaceRig(t *testing.T) (*Activities, *eventsources.ServiceSpec, string) {
	t.Helper()
	h := newPolicyHarness(t)
	h.d.OrkerID = "orker-1"
	a := &Activities{dispatcher: h.d}
	dir := t.TempDir()
	a.services.setStorage(dir)
	spec := instanceSpec(h)
	spec.With = []byte(`{"stream":"ORDERS"}`)
	return a, spec, filepath.Join(dir, "orker-1")
}

func contextOf(t *testing.T, spec *eventsources.ServiceSpec) servicehost.Context {
	t.Helper()
	spec.Limits.StatusSeconds = 30
	c, err := servicehost.BuildContext(*spec, "prod", 1, 2, instanceRun(7, 2))
	must.OK(t, err)
	return c
}

// TestServiceWorkspace: a start's files are under the Orker's own directory of
// memory, sealed, with each secret in a file of its own; removed, nothing is left.
func TestServiceWorkspace(t *testing.T) {
	t.Setenv("PAY_KEY", "s3cret")
	a, spec, own := workspaceRig(t)
	spec.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "env:PAY_KEY"}}
	name := instanceName("evs-0123456789abcdef", 1, 7, 2)
	if name != "evs-0123456789abcdef-1-7-2" {
		t.Errorf("a start's name is %q", name)
	}
	w, f := a.newServiceWorkspace(name, contextOf(t, spec), spec)
	if f != nil {
		t.Fatal(f)
	}
	for file, want := range map[string]string{"input/config.json": `{"stream":"ORDERS"}`, "input/secrets/token": "s3cret"} {
		raw, err := os.ReadFile(filepath.Join(own, name, file))
		if err != nil || string(raw) != want {
			t.Errorf("%s: %q, %v", file, raw, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(own, name, "input/service.json"))
	if err != nil || !strings.Contains(string(raw), `"run":"7-2"`) || strings.Contains(string(raw), "s3cret") {
		t.Errorf("service.json: %s, %v", raw, err)
	}
	for file, want := range map[string]os.FileMode{"": 0o700, "input": 0o555, "input/secrets": 0o555, "input/service.json": 0o444, "input/secrets/token": 0o444, "ask": 0o755} {
		info, err := os.Stat(filepath.Join(own, name, file))
		if err != nil || info.Mode().Perm() != want {
			t.Errorf("%s: %v, %v; want %v", file, info.Mode(), err, want)
		}
	}
	for _, m := range w.mounts() {
		if !m.ReadOnly || !strings.HasPrefix(m.Source, own+"/") {
			t.Errorf("mount %+v", m)
		}
	}
	must.OK(t, w.remove())
	if _, err := os.Stat(filepath.Join(own, name)); !os.IsNotExist(err) {
		t.Errorf("left behind: %v", err)
	}
	must.OK(t, (*serviceWorkspace)(nil).remove())

	spec.With = nil
	w, f = a.newServiceWorkspace(name, contextOf(t, spec), spec)
	if f != nil {
		t.Fatal(f)
	}
	if raw, _ := os.ReadFile(filepath.Join(own, name, "input/config.json")); string(raw) != "{}" {
		t.Errorf("no configuration is %q", raw)
	}
	// Another start of the same name is never made over this one, nor removes it.
	if _, f := a.newServiceWorkspace(name, contextOf(t, spec), spec); f == nil || f.Reason != "SERVICE_HOST_STALLED" {
		t.Errorf("a second start of one name: %v", f)
	}
	if _, err := os.Stat(filepath.Join(own, name, "input/config.json")); err != nil {
		t.Errorf("the first start's files: %v", err)
	}
	must.OK(t, w.remove())
}

// TestServiceWorkspace_Fails: what cannot be made leaves nothing, and says whose
// it is.
func TestServiceWorkspace_Fails(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*testing.T, *Activities, *eventsources.ServiceSpec)
		reason string
		cause  string
	}{
		"no memory-backed directory": {func(_ *testing.T, a *Activities, _ *eventsources.ServiceSpec) { a.services.setStorage("") }, "SERVICE_HOST_UNFIT", ""},
		"a secret this host lacks": {func(_ *testing.T, _ *Activities, s *eventsources.ServiceSpec) {
			s.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "env:NOT_SET_HERE"}}
		}, "SECRET_UNRESOLVED", ""},
		"a secret below the floor": {func(_ *testing.T, a *Activities, s *eventsources.ServiceSpec) {
			a.dispatcher.Policy.SecretAllow = []string{"env:PAY_*"}
			s.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "env:OTHER"}}
		}, "SECRET_NOT_PERMITTED", ""},
		"a scheme nobody resolves": {func(_ *testing.T, _ *Activities, s *eventsources.ServiceSpec) {
			s.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "vault:kv"}}
		}, "SECRET_NOT_PERMITTED", "SECRET_SCHEME_FORBIDDEN"},
		"a start named by a path": {func(*testing.T, *Activities, *eventsources.ServiceSpec) {}, "SERVICE_HOST_STALLED", ""},
		"its directory is someone else's": {func(t *testing.T, a *Activities, _ *eventsources.ServiceSpec) {
			dir := t.TempDir()
			must.OK(t, os.MkdirAll(filepath.Join(dir, "orker-1", "theirs"), 0o755))
			a.services.setStorage(dir)
		}, "SERVICE_HOST_STALLED", ""},
	} {
		t.Run(name, func(t *testing.T) {
			a, spec, own := workspaceRig(t)
			tc.change(t, a, spec)
			start := "one"
			if name == "a start named by a path" {
				start = "../one"
			}
			w, f := a.newServiceWorkspace(start, contextOf(t, spec), spec)
			if w != nil || f == nil || f.Reason != tc.reason || f.Cause != tc.cause {
				t.Fatalf("%+v, %+v; want %s", w, f, tc.reason)
			}
			if _, ok := eventsources.LookupInstanceEnd(f.Reason); !ok {
				t.Errorf("%s is in no table", f.Reason)
			}
			if _, err := os.Stat(filepath.Join(own, "one")); !os.IsNotExist(err) {
				t.Errorf("left behind: %v", err)
			}
		})
	}
}

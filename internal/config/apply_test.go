package config

import (
	"fmt"
	"testing"
)

// fakeEnv is a minimal in-memory stand-in for the process environment, so
// Apply's tests never touch os.Setenv (which would leak between parallel
// tests and other packages sharing this process).
type fakeEnv struct {
	m map[string]string
}

func newFakeEnv(preset map[string]string) *fakeEnv {
	m := make(map[string]string, len(preset))
	for k, v := range preset {
		m[k] = v
	}
	return &fakeEnv{m: m}
}

func (f *fakeEnv) lookup(key string) (string, bool) {
	v, ok := f.m[key]
	return v, ok
}

func (f *fakeEnv) setenv(key, value string) error {
	f.m[key] = value
	return nil
}

func TestApply_SetsUnsetVariables(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_level: debug\n  log_format: json\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	env := newFakeEnv(nil)
	set, err := doc.Apply(env.lookup, env.setenv)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got, ok := env.lookup("ORK_LOG_LEVEL"); !ok || got != "debug" {
		t.Errorf("ORK_LOG_LEVEL = %q, %v; want debug, true", got, ok)
	}
	if got, ok := env.lookup("ORK_LOG_FORMAT"); !ok || got != "json" {
		t.Errorf("ORK_LOG_FORMAT = %q, %v; want json, true", got, ok)
	}

	wantSet := map[string]bool{"ORK_LOG_LEVEL": true, "ORK_LOG_FORMAT": true}
	if len(set) != len(wantSet) {
		t.Fatalf("Apply returned %v, want exactly %v", set, wantSet)
	}
	for _, env := range set {
		if !wantSet[env] {
			t.Errorf("Apply returned unexpected variable %q", env)
		}
	}
}

func TestApply_EnvironmentWins_EvenWhenEmpty(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_level: debug\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// ORK_LOG_LEVEL is present in the environment, set to "" — Apply must
	// leave it alone rather than treating an empty string as absent.
	env := newFakeEnv(map[string]string{"ORK_LOG_LEVEL": ""})
	set, err := doc.Apply(env.lookup, env.setenv)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got, _ := env.lookup("ORK_LOG_LEVEL"); got != "" {
		t.Errorf("ORK_LOG_LEVEL = %q, want untouched empty string", got)
	}
	for _, e := range set {
		if e == "ORK_LOG_LEVEL" {
			t.Error("Apply reported ORK_LOG_LEVEL as set, but the environment already had it")
		}
	}
}

func TestApply_SetenvError_Propagates(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_level: debug\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	env := newFakeEnv(nil)
	boom := fmt.Errorf("boom")
	_, err = doc.Apply(env.lookup, func(string, string) error { return boom })
	if err == nil {
		t.Fatal("Apply: want an error when setenv fails")
	}
}

func TestApply_SchemaOrder(t *testing.T) {
	// Apply's returned list follows Schema order, so a boot log reads the
	// same way every time regardless of the file's own key order.
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_format: json\n  log_level: debug\ntemporal:\n  address: x:1\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	env := newFakeEnv(nil)
	set, err := doc.Apply(env.lookup, env.setenv)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// temporal.address precedes telemetry.log_format/log_level in Schema.
	idxAddr, idxFormat := -1, -1
	for i, e := range set {
		if e == "ORK_TEMPORAL_ADDRESS" {
			idxAddr = i
		}
		if e == "ORK_LOG_FORMAT" {
			idxFormat = i
		}
	}
	if idxAddr < 0 || idxFormat < 0 || idxAddr > idxFormat {
		t.Errorf("Apply order = %v, want ORK_TEMPORAL_ADDRESS before ORK_LOG_FORMAT", set)
	}
}

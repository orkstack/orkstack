package config

import (
	"testing"
)

// TestRenderDefaults_IsNoOp proves loading RenderDefaults()'s output and
// applying it changes nothing observable: every active (uncommented) leaf
// resolves with origin=file and the exact value RenderDefaults wrote for
// it (canonicalDefaultValue), and every leaf the renderer left commented
// out still resolves with origin=default and Var.Default unchanged — for
// BOTH binaries, since one shared file must behave identically read by
// either.
func TestRenderDefaults_IsNoOp(t *testing.T) {
	path := writeTemp(t, "rendered-defaults.yaml", RenderDefaults())
	noEnv := func(string) (string, bool) { return "", false }

	for _, binary := range []Binary{BinaryOrk, BinaryOrker} {
		doc, err := Load(path, binary)
		if err != nil {
			t.Fatalf("Load(%s): %v", binary, err)
		}
		for _, v := range Schema {
			if !v.usesBinary(binary) {
				continue
			}
			value, org := doc.resolve(v, noEnv)
			if isActiveDefault(v) {
				if org != originFile {
					t.Errorf("%s (%s): origin = %s, want file (rendered active)", v.Path, binary, org)
				}
				if want := canonicalDefaultValue(v, binary); value != want {
					t.Errorf("%s (%s): value = %q, want %q", v.Path, binary, value, want)
				}
			} else {
				if org != originDefault {
					t.Errorf("%s (%s): origin = %s, want default (rendered commented out)", v.Path, binary, org)
				}
				if value != v.Default {
					t.Errorf("%s (%s): value = %q, want Var.Default %q", v.Path, binary, value, v.Default)
				}
			}
		}
	}
}

// TestRenderDefaults_ApplySetsNothingObservablyDifferent proves the same
// no-op property through Document.Apply itself (not just resolve): with an
// empty starting environment, applying the rendered file sets exactly the
// active leaves' variables, each to canonicalDefaultValue — never to a
// leaf the renderer left commented out.
func TestRenderDefaults_ApplySetsNothingObservablyDifferent(t *testing.T) {
	path := writeTemp(t, "rendered-defaults-apply.yaml", RenderDefaults())

	for _, binary := range []Binary{BinaryOrk, BinaryOrker} {
		doc, err := Load(path, binary)
		if err != nil {
			t.Fatalf("Load(%s): %v", binary, err)
		}
		env := map[string]string{}
		lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
		setenv := func(k, v string) error { env[k] = v; return nil }

		if _, err := doc.Apply(lookup, setenv); err != nil {
			t.Fatalf("Apply(%s): %v", binary, err)
		}

		for _, v := range Schema {
			if !v.usesBinary(binary) {
				continue
			}
			got, set := env[v.Env]
			if isActiveDefault(v) {
				if !set {
					t.Errorf("%s (%s): Apply left %s unset, want %q", v.Path, binary, v.Env, canonicalDefaultValue(v, binary))
					continue
				}
				if want := canonicalDefaultValue(v, binary); got != want {
					t.Errorf("%s (%s): Apply set %s = %q, want %q", v.Path, binary, v.Env, got, want)
				}
			} else if set {
				t.Errorf("%s (%s): Apply set %s = %q, want it left unset (rendered commented out)", v.Path, binary, v.Env, got)
			}
		}
	}
}

// TestRenderDefaults_ExplicitEmptyNeverAgainstNonEmptyDefault proves the
// renderer never falls into Load's own explicit-empty refusal
// (load.go's checkExplicitEmpty): every leaf RenderDefaults writes with an
// empty value ("", [], {}) is one whose OWN Default is the placeholder
// EmptyDefault, never a leaf with a real non-empty default.
func TestRenderDefaults_ExplicitEmptyNeverAgainstNonEmptyDefault(t *testing.T) {
	for _, v := range Schema {
		if !isActiveDefault(v) {
			continue
		}
		if canonicalDefaultValue(v, BinaryOrk) == "" && v.Default != EmptyDefault {
			t.Errorf("%s (%s): renders an empty value but Var.Default is %q, not EmptyDefault — Load's checkExplicitEmpty would refuse this", v.Path, v.Env, v.Default)
		}
	}
}

// TestDefaultsRenderMaps_ReferenceRealSchemaEntries proves every Env named
// in render.go's small lookup tables (rawMachineOverride, alwaysCommentedEnv,
// perBinaryDefault) actually names a Schema entry — a stale entry left
// behind after a Schema key is renamed or removed would otherwise be
// silently ignored rather than failing loudly.
func TestDefaultsRenderMaps_ReferenceRealSchemaEntries(t *testing.T) {
	envs := map[string]bool{}
	for _, v := range Schema {
		envs[v.Env] = true
	}
	for _, env := range sortedEnvNames(alwaysCommentedEnv) {
		if !envs[env] {
			t.Errorf("alwaysCommentedEnv names %q, which is not in Schema", env)
		}
	}
	for env := range rawMachineOverride {
		if !envs[env] {
			t.Errorf("rawMachineOverride names %q, which is not in Schema", env)
		}
	}
	for env := range perBinaryDefault {
		if !envs[env] {
			t.Errorf("perBinaryDefault names %q, which is not in Schema", env)
		}
	}
}

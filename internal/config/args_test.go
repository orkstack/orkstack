package config

import (
	"strings"
	"testing"
)

func TestParseArgs(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	withConfigFileVar := func(v string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			if k == ConfigFileVar {
				return v, true
			}
			return "", false
		}
	}

	cases := []struct {
		name    string
		args    []string
		lookup  func(string) (string, bool)
		want    Args
		wantErr bool
	}{
		{"no args, no env", nil, noEnv, Args{}, false},
		{"--config PATH", []string{"--config", "/etc/orkstack.yaml"}, noEnv, Args{Path: "/etc/orkstack.yaml"}, false},
		{"--config=PATH", []string{"--config=/etc/orkstack.yaml"}, noEnv, Args{Path: "/etc/orkstack.yaml"}, false},
		{"--check alone", []string{"--check"}, noEnv, Args{Check: true}, false},
		{"--config PATH --check", []string{"--config", "/a.yaml", "--check"}, noEnv, Args{Path: "/a.yaml", Check: true}, false},
		{"--check --config=PATH", []string{"--check", "--config=/a.yaml"}, noEnv, Args{Path: "/a.yaml", Check: true}, false},
		{"env fallback", nil, withConfigFileVar("/env/path.yaml"), Args{Path: "/env/path.yaml"}, false},
		{"flag wins over env", []string{"--config", "/flag/path.yaml"}, withConfigFileVar("/env/path.yaml"), Args{Path: "/flag/path.yaml"}, false},
		{"--config with no value", []string{"--config"}, noEnv, Args{}, true},
		{"--config= empty", []string{"--config="}, noEnv, Args{}, true},
		{"unknown flag", []string{"--bogus"}, noEnv, Args{}, true},
		{"bare positional arg", []string{"/some/path.yaml"}, noEnv, Args{}, true},
		{"-h", []string{"-h"}, noEnv, Args{}, true},
		{"--help", []string{"--help"}, noEnv, Args{}, true},
		{"--no-config alone", []string{"--no-config"}, noEnv, Args{}, false},
		{"--no-config ignores ORK_CONFIG_FILE", []string{"--no-config"}, withConfigFileVar("/env/path.yaml"), Args{}, false},
		{"--no-config --check is an error", []string{"--no-config", "--check"}, noEnv, Args{}, true},
		{"--defaults alone", []string{"--defaults"}, noEnv, Args{Defaults: true}, false},
		{"--defaults ignores ORK_CONFIG_FILE presence but still reports it", []string{"--defaults"}, withConfigFileVar("/env/path.yaml"), Args{Path: "/env/path.yaml", Defaults: true}, false},
		{"--defaults with --no-config", []string{"--no-config", "--defaults"}, noEnv, Args{Defaults: true}, false},
		{"--defaults with --check and --no-config is still an error", []string{"--no-config", "--check", "--defaults"}, noEnv, Args{Defaults: true}, true},

		{"--healthcheck alone", []string{"--healthcheck"}, noEnv, Args{Healthcheck: true}, false},
		{"--healthcheck --config PATH", []string{"--healthcheck", "--config", "/a.yaml"}, noEnv, Args{Path: "/a.yaml", Healthcheck: true}, false},
		{"--config=PATH --healthcheck", []string{"--config=/a.yaml", "--healthcheck"}, noEnv, Args{Path: "/a.yaml", Healthcheck: true}, false},
		{"--healthcheck falls back to ORK_CONFIG_FILE", []string{"--healthcheck"}, withConfigFileVar("/env/path.yaml"), Args{Path: "/env/path.yaml", Healthcheck: true}, false},
		{"--healthcheck with --no-config reads the environment only", []string{"--healthcheck", "--no-config"}, withConfigFileVar("/env/path.yaml"), Args{Healthcheck: true}, false},
		{"--healthcheck with --defaults is parsed, main answers --defaults first", []string{"--healthcheck", "--defaults"}, noEnv, Args{Defaults: true, Healthcheck: true}, false},
		{"--healthcheck --check is an error", []string{"--healthcheck", "--check"}, noEnv, Args{}, true},
		{"--check --healthcheck is an error", []string{"--check", "--healthcheck"}, noEnv, Args{}, true},
		{"--healthcheck --check with a file is still an error", []string{"--config", "/a.yaml", "--healthcheck", "--check"}, noEnv, Args{}, true},
		{"--healthcheck --check --no-config is an error", []string{"--no-config", "--check", "--healthcheck"}, noEnv, Args{}, true},
		{"--healthcheck takes no value", []string{"--healthcheck=true"}, noEnv, Args{}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseArgs(tc.args, tc.lookup)
			if tc.wantErr {
				if err == nil {
					t.Fatal("ParseArgs: want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseArgs: %v", err)
			}
			if got != tc.want {
				t.Errorf("ParseArgs = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestParseArgs_HealthcheckWithCheckIsRefusedNamingBoth: --check validates a
// file and starts nothing, --healthcheck asks a running process; combining
// them has no meaning, and the refusal says which two flags clash.
func TestParseArgs_HealthcheckWithCheckIsRefusedNamingBoth(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	for _, args := range [][]string{{"--check", "--healthcheck"}, {"--healthcheck", "--check"}} {
		_, err := ParseArgs(args, noEnv)
		if err == nil {
			t.Fatalf("ParseArgs(%v): want an error", args)
		}
		for _, want := range []string{"--healthcheck", "--check"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("ParseArgs(%v) error = %q, want it to name %s", args, err.Error(), want)
			}
		}
	}
}

func TestParseArgs_HelpText_ListsBothFlags(t *testing.T) {
	_, err := ParseArgs([]string{"--help"}, func(string) (string, bool) { return "", false })
	if err == nil {
		t.Fatal("ParseArgs(--help): want an error carrying usage text")
	}
	msg := err.Error()
	if !strings.Contains(msg, "--config") || !strings.Contains(msg, "--check") {
		t.Errorf("usage error = %q, want it to mention --config and --check", msg)
	}
	if !strings.Contains(msg, "--no-config") {
		t.Errorf("usage error = %q, want it to mention --no-config", msg)
	}
	if !strings.Contains(msg, "--defaults") {
		t.Errorf("usage error = %q, want it to mention --defaults", msg)
	}
	if !strings.Contains(msg, "--healthcheck") {
		t.Errorf("usage error = %q, want it to mention --healthcheck", msg)
	}
}

func TestParseArgs_ConfigRequiresPath_ErrorMessage(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	for _, args := range [][]string{{"--config"}, {"--config="}} {
		_, err := ParseArgs(args, noEnv)
		if err == nil {
			t.Fatalf("ParseArgs(%v): want an error", args)
		}
		if !strings.Contains(err.Error(), "--config requires a path") {
			t.Errorf("ParseArgs(%v) error = %q, want it to say \"--config requires a path\"", args, err.Error())
		}
	}
}

func TestParseArgs_NoConfig_CheckRequiresFile(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	_, err := ParseArgs([]string{"--no-config", "--check"}, noEnv)
	if err == nil {
		t.Fatal("ParseArgs(--no-config --check): want an error")
	}
	if !strings.Contains(err.Error(), "--check requires a config file") {
		t.Errorf("error = %q, want it to say \"--check requires a config file\"", err.Error())
	}
}

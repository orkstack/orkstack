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
		name         string
		args         []string
		lookup       func(string) (string, bool)
		wantPath     string
		wantCheck    bool
		wantDefaults bool
		wantErr      bool
	}{
		{"no args, no env", nil, noEnv, "", false, false, false},
		{"--config PATH", []string{"--config", "/etc/orkstack.yaml"}, noEnv, "/etc/orkstack.yaml", false, false, false},
		{"--config=PATH", []string{"--config=/etc/orkstack.yaml"}, noEnv, "/etc/orkstack.yaml", false, false, false},
		{"--check alone", []string{"--check"}, noEnv, "", true, false, false},
		{"--config PATH --check", []string{"--config", "/a.yaml", "--check"}, noEnv, "/a.yaml", true, false, false},
		{"--check --config=PATH", []string{"--check", "--config=/a.yaml"}, noEnv, "/a.yaml", true, false, false},
		{"env fallback", nil, withConfigFileVar("/env/path.yaml"), "/env/path.yaml", false, false, false},
		{"flag wins over env", []string{"--config", "/flag/path.yaml"}, withConfigFileVar("/env/path.yaml"), "/flag/path.yaml", false, false, false},
		{"--config with no value", []string{"--config"}, noEnv, "", false, false, true},
		{"--config= empty", []string{"--config="}, noEnv, "", false, false, true},
		{"unknown flag", []string{"--bogus"}, noEnv, "", false, false, true},
		{"bare positional arg", []string{"/some/path.yaml"}, noEnv, "", false, false, true},
		{"-h", []string{"-h"}, noEnv, "", false, false, true},
		{"--help", []string{"--help"}, noEnv, "", false, false, true},
		{"--no-config alone", []string{"--no-config"}, noEnv, "", false, false, false},
		{"--no-config ignores ORK_CONFIG_FILE", []string{"--no-config"}, withConfigFileVar("/env/path.yaml"), "", false, false, false},
		{"--no-config --check is an error", []string{"--no-config", "--check"}, noEnv, "", false, false, true},
		{"--defaults alone", []string{"--defaults"}, noEnv, "", false, true, false},
		{"--defaults ignores ORK_CONFIG_FILE presence but still reports it", []string{"--defaults"}, withConfigFileVar("/env/path.yaml"), "/env/path.yaml", false, true, false},
		{"--defaults with --no-config", []string{"--no-config", "--defaults"}, noEnv, "", false, true, false},
		{"--defaults with --check and --no-config is still an error", []string{"--no-config", "--check", "--defaults"}, noEnv, "", false, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path, check, defaults, err := ParseArgs(tc.args, tc.lookup)
			if tc.wantErr {
				if err == nil {
					t.Fatal("ParseArgs: want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseArgs: %v", err)
			}
			if path != tc.wantPath {
				t.Errorf("path = %q, want %q", path, tc.wantPath)
			}
			if check != tc.wantCheck {
				t.Errorf("check = %v, want %v", check, tc.wantCheck)
			}
			if defaults != tc.wantDefaults {
				t.Errorf("defaults = %v, want %v", defaults, tc.wantDefaults)
			}
		})
	}
}

func TestParseArgs_HelpText_ListsBothFlags(t *testing.T) {
	_, _, _, err := ParseArgs([]string{"--help"}, func(string) (string, bool) { return "", false })
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
}

func TestParseArgs_ConfigRequiresPath_ErrorMessage(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	for _, args := range [][]string{{"--config"}, {"--config="}} {
		_, _, _, err := ParseArgs(args, noEnv)
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
	_, _, _, err := ParseArgs([]string{"--no-config", "--check"}, noEnv)
	if err == nil {
		t.Fatal("ParseArgs(--no-config --check): want an error")
	}
	if !strings.Contains(err.Error(), "--check requires a config file") {
		t.Errorf("error = %q, want it to say \"--check requires a config file\"", err.Error())
	}
}

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// FuzzParseArgs: the two daemons read the same argv. The flags are read in any
// order and combination, --config consumes the word after it whatever that
// word is, --no-config ignores ORK_CONFIG_FILE, and the one environment
// fallback applies only when no path was given. Every other word is refused
// by name.
func FuzzParseArgs(f *testing.F) {
	for _, seed := range [][3]string{
		{"--check", "--config", "/etc/ork.yaml"}, {"--config=/a", "--defaults", ""}, {"--no-config", "--check", ""}, {"--healthcheck", "--check", ""},
		{"--config", "--check", ""}, {"--config=", "", ""}, {"-h", "", ""}, {"--bogus", "", ""}, {"--no-config", "--healthcheck", "--config=x"},
		{"--defaults", "--defaults", "--defaults"}, {"--CHECK", "", ""}, {"--config=a=b", "--config", "c"},
	} {
		f.Add(seed[0], seed[1], seed[2], "/env.yaml", true)
		f.Add(seed[0], seed[1], seed[2], "", false)
	}
	f.Fuzz(func(t *testing.T, a, b, c, fromEnv string, envSet bool) {
		var args []string
		for _, word := range []string{a, b, c} {
			if word != "" {
				args = append(args, word)
			}
		}
		lookups := 0
		lookup := func(name string) (string, bool) {
			lookups++
			if name != ConfigFileVar {
				t.Fatalf("ParseArgs looked up %q", name)
			}
			return fromEnv, envSet
		}
		got, err := ParseArgs(args, lookup)

		// An independent reading of the same grammar.
		var want Args
		var noConfig bool
		var wantErr string
		for i := 0; i < len(args) && wantErr == ""; i++ {
			switch word := args[i]; {
			case word == "-h" || word == "--help":
				wantErr = Usage
			case word == "--check":
				want.Check = true
			case word == "--healthcheck":
				want.Healthcheck = true
			case word == "--defaults":
				want.Defaults = true
			case word == "--no-config":
				noConfig = true
			case word == "--config":
				if i++; i >= len(args) {
					wantErr = "--config requires a path"
				} else {
					want.Path = args[i]
				}
			case strings.HasPrefix(word, "--config="):
				if want.Path = strings.TrimPrefix(word, "--config="); want.Path == "" {
					wantErr = "--config requires a path"
				}
			default:
				wantErr = "unrecognized argument"
			}
		}
		if wantErr == "" && want.Check && want.Healthcheck {
			wantErr = "--healthcheck cannot be combined with --check"
		}
		if wantErr == "" && noConfig {
			if want.Check {
				wantErr = "--check requires a config file"
			}
			want = Args{Defaults: want.Defaults, Healthcheck: want.Healthcheck}
		} else if wantErr == "" && want.Path == "" && envSet {
			want.Path = fromEnv
		}
		if wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("ParseArgs(%q) = %+v, %v; want a refusal containing %q", args, got, err, wantErr)
			}
			return
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("ParseArgs(%q, env %q/%v) = %+v, %v; want %+v", args, fromEnv, envSet, got, err, want)
		}
		if noConfig && lookups != 0 {
			t.Fatalf("ParseArgs(%q) read ORK_CONFIG_FILE under --no-config", args)
		}
	})
}

// FuzzLoad_AValueTheFileSetsIsOneItsKindReads: a configuration file is the
// operator's, but it is read once and then reaches the daemons as environment
// variables their readers parse. An accepted value must be one the strictest
// of those readers accepts as it stands (an integer, a boolean or a duration
// with nothing around it, text the environment can hold), every variable it
// sets is declared in the schema, and a refusal names the file. A leaf that
// reads a file by name is not part of the corpus: the text may not contain
// "file".
func FuzzLoad_AValueTheFileSetsIsOneItsKindReads(f *testing.F) {
	for _, seed := range []string{
		"", "# nothing\n", "temporal:\n  address: temporal.internal:7233\n  namespace: ork-test\n", "engine:\n  ws_allowed_origins: [a, b]\n",
		"temporal:\n  address: {ork: a:1, orker: b:2}\n", "temporal:\n  nope: 1\n", "- 1\n", "[", "temporal: 1\n", "temporal:\n  address: null\n",
		"temporal:\n  address: ' padded '\n", "temporal:\n  address: \"a\\0b\"\n", "orker:\n  max_concurrent: ' 5 '\n", "orker:\n  max_concurrent: 5\n",
		"orker:\n  max_concurrent: '5.0'\n", "orker:\n  max_concurrent: abc\n", "temporal:\n  address: a\n  address: b\n",
		"telemetry:\n  otlp:\n    headers:\n      A: b\n",
	} {
		f.Add([]byte(seed))
	}
	dir := f.TempDir()
	path := filepath.Join(dir, "ork.yaml")
	byEnv := map[string]Var{}
	for _, v := range Schema {
		byEnv[v.Env] = v
	}
	f.Fuzz(func(t *testing.T, content []byte) {
		if strings.Contains(strings.ToLower(string(content)), "file") {
			return
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, binary := range []Binary{BinaryOrk, BinaryOrker} {
			doc, err := Load(path, binary)
			if err != nil {
				if !strings.Contains(err.Error(), path) || doc != nil {
					t.Fatalf("Load(%q, %s) refused as %q (document %v)", content, binary, err, doc)
				}
				continue
			}
			set := map[string]string{}
			names, err := doc.Apply(func(string) (string, bool) { return "", false }, func(name, value string) error {
				set[name] = value
				return nil
			})
			if err != nil || len(names) != len(set) {
				t.Fatalf("Apply of %q: %v, %v", content, names, err)
			}
			for name, value := range set {
				v, declared := byEnv[name]
				if !declared {
					t.Fatalf("Load(%q) set %s, which the schema does not declare", content, name)
				}
				if strings.IndexByte(value, 0) >= 0 || !utf8.ValidString(value) {
					t.Fatalf("Load(%q) set %s to %q, which the environment cannot hold as text", content, name, value)
				}
				switch v.Kind {
				case KindInt:
					if _, err := strconv.Atoi(value); err != nil {
						t.Fatalf("Load(%q) set the integer %s to %q: %v", content, name, value, err)
					}
				case KindBool:
					if _, err := strconv.ParseBool(value); err != nil {
						t.Fatalf("Load(%q) set the boolean %s to %q: %v", content, name, value, err)
					}
				case KindDuration:
					if _, err := time.ParseDuration(value); err != nil {
						t.Fatalf("Load(%q) set the duration %s to %q: %v", content, name, value, err)
					}
				}
			}
		}
	})
}

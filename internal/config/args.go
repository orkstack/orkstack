package config

import (
	"fmt"
	"strings"
)

// ConfigFileVar is the environment fallback ParseArgs consults when no
// --config flag is given, matching the specification's "Mechanism" section
// ("--config PATH (flag) or ORK_CONFIG_FILE=PATH (env; flag wins)").
const ConfigFileVar = "ORK_CONFIG_FILE"

// Usage is the string a -h/--help invocation returns as an error, so a
// caller's main() can print it and exit non-zero the same way any other
// argument error does.
const Usage = "usage: [--config PATH | --config=PATH | --no-config] [--check | --healthcheck] [--defaults]"

// Args is what the two daemons' argv asked for.
type Args struct {
	// Path is the configuration file, from --config or ORK_CONFIG_FILE, or
	// "" for none.
	Path string
	// Check is --check: validate the file and print the effective
	// configuration, starting nothing.
	Check bool
	// Defaults is --defaults: print the built-in defaults and stop.
	Defaults bool
	// Healthcheck is --healthcheck: ask the already running process's own
	// admin listener whether it is ready, and exit 0 or 1.
	Healthcheck bool
}

// ParseArgs handles the two daemons' argv (excluding argv[0]): --config
// PATH, --config=PATH, --no-config, --check, --healthcheck, and --defaults, in
// any order and combination. With no --config flag it falls back to
// ORK_CONFIG_FILE via lookup. --no-config means "no file, and ignore
// ORK_CONFIG_FILE too" — it returns path "" without consulting lookup at all,
// the deliberate escape hatch for a caller that must not read that variable
// even when the environment happens to set it. --check with --no-config is
// refused, naming the requirement, since there is then no file to validate.
// --healthcheck with --check is refused too, naming both flags: --check
// starts nothing and reads no process, --healthcheck is only a question to
// one. --healthcheck reads its configuration the way a daemon boot does, so
// it takes --config, ORK_CONFIG_FILE and --no-config like any other run.
// --defaults needs no config file at all — a caller that sees defaults
// true should print RenderDefaults() and exit before doing anything else
// with path/check (cmd/ork's and cmd/orker's main do exactly that), so it
// is not subject to that same refusal and every other flag here still
// parses normally alongside it; beside --healthcheck, --defaults is
// answered first and nothing is probed. -h/--help and any other argument are
// errors — -h/--help returns Usage as the error text, so main can print it
// verbatim.
func ParseArgs(args []string, lookup func(string) (string, bool)) (Args, error) {
	var parsed Args
	var noConfig bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			return Args{}, fmt.Errorf("%s", Usage)

		case arg == "--check":
			parsed.Check = true

		case arg == "--healthcheck":
			parsed.Healthcheck = true

		case arg == "--defaults":
			parsed.Defaults = true

		case arg == "--no-config":
			noConfig = true

		case arg == "--config":
			i++
			if i >= len(args) {
				return Args{}, fmt.Errorf("--config requires a path")
			}
			parsed.Path = args[i]

		case strings.HasPrefix(arg, "--config="):
			parsed.Path = strings.TrimPrefix(arg, "--config=")
			if parsed.Path == "" {
				return Args{}, fmt.Errorf("--config requires a path")
			}

		default:
			return Args{}, fmt.Errorf("unrecognized argument %q (%s)", arg, Usage)
		}
	}

	if parsed.Check && parsed.Healthcheck {
		return Args{}, fmt.Errorf("--healthcheck cannot be combined with --check: --check validates a file and " +
			"starts nothing, --healthcheck asks a running process whether it is ready")
	}

	if noConfig {
		if parsed.Check {
			return Args{Defaults: parsed.Defaults}, fmt.Errorf("--check requires a config file")
		}
		return Args{Defaults: parsed.Defaults, Healthcheck: parsed.Healthcheck}, nil
	}

	if parsed.Path == "" {
		if v, ok := lookup(ConfigFileVar); ok {
			parsed.Path = v
		}
	}
	return parsed, nil
}

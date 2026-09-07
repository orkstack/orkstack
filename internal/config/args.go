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
const Usage = "usage: [--config PATH | --config=PATH | --no-config] [--check] [--defaults]"

// ParseArgs handles the two daemons' argv (excluding argv[0]): --config
// PATH, --config=PATH, --no-config, --check, and --defaults, in any order
// and combination. With no --config flag it falls back to ORK_CONFIG_FILE
// via lookup. --no-config means "no file, and ignore ORK_CONFIG_FILE too"
// — it returns path "" without consulting lookup at all, the deliberate
// escape hatch for a caller that must not read that variable even when the
// environment happens to set it. --check with --no-config is refused,
// naming the requirement, since there is then no file to validate.
// --defaults needs no config file at all — a caller that sees defaults
// true should print RenderDefaults() and exit before doing anything else
// with path/check (cmd/ork's and cmd/orker's main do exactly that), so it
// is not subject to that same refusal and every other flag here still
// parses normally alongside it. -h/--help and any other argument are
// errors — -h/--help returns Usage as the error text, so main can print it
// verbatim.
func ParseArgs(args []string, lookup func(string) (string, bool)) (path string, check bool, defaults bool, err error) {
	var noConfig bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			return "", false, false, fmt.Errorf("%s", Usage)

		case arg == "--check":
			check = true

		case arg == "--defaults":
			defaults = true

		case arg == "--no-config":
			noConfig = true

		case arg == "--config":
			i++
			if i >= len(args) {
				return "", false, false, fmt.Errorf("--config requires a path")
			}
			path = args[i]

		case strings.HasPrefix(arg, "--config="):
			path = strings.TrimPrefix(arg, "--config=")
			if path == "" {
				return "", false, false, fmt.Errorf("--config requires a path")
			}

		default:
			return "", false, false, fmt.Errorf("unrecognized argument %q (%s)", arg, Usage)
		}
	}

	if noConfig {
		if check {
			return "", false, defaults, fmt.Errorf("--check requires a config file")
		}
		return "", false, defaults, nil
	}

	if path == "" {
		if v, ok := lookup(ConfigFileVar); ok {
			path = v
		}
	}
	return path, check, defaults, nil
}

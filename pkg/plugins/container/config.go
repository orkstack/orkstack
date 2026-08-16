package containerplugin

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// expandEnv resolves `$VAR` and `${VAR}` references between the task's own
// environment variables. Values reach this point already rendered by the
// engine's template layer, so `{{ .inputs.name }}` is substituted first and
// `$VAR` second.
//
// Every reference reads the *declared* value, never an already-expanded one, so
// the result does not depend on map order and a cycle is impossible. An
// undeclared name expands to the empty string, as a shell would. `$$` is a
// literal `$`, which is how a value that really wants a dollar sign keeps one.
func expandEnv(declared map[string]string) map[string]string {
	if len(declared) == 0 {
		return declared
	}
	expanded := make(map[string]string, len(declared))
	for key, value := range declared {
		expanded[key] = os.Expand(value, func(name string) string {
			if name == "$" {
				return "$"
			}
			return declared[name]
		})
	}
	return expanded
}

// validateEnv rejects names Docker cannot carry. A name holding '=' would
// silently redefine a different variable.
func validateEnv(env map[string]string) error {
	for _, key := range sortedKeys(env) {
		switch {
		case strings.TrimSpace(key) == "":
			return fmt.Errorf("`env` has an empty variable name")
		case strings.ContainsAny(key, "= \t\n"):
			return fmt.Errorf("`env` name %q may not contain '=' or whitespace", key)
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func configString(config map[string]any, key string) string {
	value, ok := config[key]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func configStringMap(config map[string]any, key string) (map[string]string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return nil, nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("`%s` must be a map, got %T", key, value)
	}
	out := make(map[string]string, len(raw))
	for name, item := range raw {
		if item == nil {
			out[name] = ""
			continue
		}
		if s, ok := item.(string); ok {
			out[name] = s
			continue
		}
		out[name] = fmt.Sprintf("%v", item)
	}
	return out, nil
}

// configCommand reads `command`. A list is the container's argv, used verbatim.
// A string is a shell one-liner: the plugin never splits it itself, because
// splitting is where quoting bugs live; it hands the exact string to the
// image's own `sh -c`.
func configCommand(config map[string]any) ([]string, error) {
	value, ok := config["command"]
	if !ok || value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil, nil
		}
		return []string{"sh", "-c", typed}, nil
	case []any:
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("`command`[%d] must be a string, got %T", i, item)
			}
			out = append(out, s)
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	case []string:
		return typed, nil
	default:
		return nil, fmt.Errorf("`command` must be a string or a list of strings, got %T", value)
	}
}

// configSeconds reads a duration expressed in seconds, accepting the number a
// YAML author writes and the string a template renders.
func configSeconds(config map[string]any, key string) (float64, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return 0, nil
	}
	seconds, err := asFloat(value)
	if err != nil {
		return 0, fmt.Errorf("`%s` %w", key, err)
	}
	if seconds < 0 {
		return 0, fmt.Errorf("`%s` must not be negative, got %v", key, seconds)
	}
	return seconds, nil
}

func asFloat(value any) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case int:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, fmt.Errorf("must be a number, got %q", typed)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("must be a number, got %T", value)
	}
}

func asBool(value any) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		if err != nil {
			return false, fmt.Errorf("must be true or false, got %q", typed)
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("must be true or false, got %T", value)
	}
}

// headBuffer keeps the first limit bytes written to it and discards the rest,
// so a chatty container cannot put megabytes on the task's output payload.
type headBuffer struct {
	limit int
	buf   strings.Builder
}

func (h *headBuffer) Write(p []byte) (int, error) {
	if room := h.limit - h.buf.Len(); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		h.buf.Write(p[:room])
	}
	return len(p), nil
}

func (h *headBuffer) String() string { return h.buf.String() }

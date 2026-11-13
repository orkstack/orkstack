package dsl

import (
	"fmt"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

// The run-level `timeout:` bounds (docs/DSL.md "Run timeout"): a Go duration
// of whole seconds, 10s through 8760h.
const (
	MinRunTimeoutSeconds = 10
	MaxRunTimeoutSeconds = 31_536_000
)

// ParseRunTimeout reads the text of a `timeout:` value: a Go duration
// (time.ParseDuration) of whole seconds inside the bounds. It returns the
// seconds, or 0 and an error whose text is the DSL's refusal.
func ParseRunTimeout(text string) (int64, error) {
	d, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("timeout: %q is not a duration such as 30m or 6h", text)
	}
	if d%time.Second != 0 {
		return 0, fmt.Errorf("timeout: %s must be a whole number of seconds", text)
	}
	seconds := int64(d / time.Second)
	if err := checkRunTimeoutBounds(text, seconds); err != nil {
		return 0, err
	}
	return seconds, nil
}

func checkRunTimeoutBounds(written string, seconds int64) error {
	if seconds < MinRunTimeoutSeconds || seconds > MaxRunTimeoutSeconds {
		return fmt.Errorf("timeout: %s is outside 10s … 8760h; omit timeout: for a run with no time limit", written)
	}
	return nil
}

// ValidateRunTimeout checks a compiled definition's run timeout in seconds:
// 0 is no timeout, anything else is inside the bounds. It is what create and
// plan run again over a stored definition, as validate does at compile.
func ValidateRunTimeout(seconds int64) error {
	if seconds == 0 {
		return nil
	}
	written := shortDuration(time.Duration(seconds) * time.Second)
	if seconds < 0 {
		written = fmt.Sprintf("%ds", seconds)
	}
	if err := checkRunTimeoutBounds(written, seconds); err != nil {
		return validationError("timeout", err)
	}
	return nil
}

// compileRunTimeout reads the document's `timeout:` node: absent is 0, and
// the strict mapping accepts only a string scalar (docs/JSON.md).
func compileRunTimeout(node *yaml.Node) (int64, error) {
	if node.Kind == 0 {
		return 0, nil
	}
	if err := structural(node, "timeout"); err != nil {
		return 0, authoredError(err)
	}
	fail := func(err error) (int64, error) {
		return 0, validationError("timeout", fmt.Errorf("line %d: %w", node.Line, err))
	}
	if node.Kind != yaml.ScalarNode {
		return fail(fmt.Errorf("timeout: %s is not a duration such as 30m or 6h", DescribeNode(node)))
	}
	switch node.Tag {
	case "!!str":
		seconds, err := ParseRunTimeout(node.Value)
		if err != nil {
			return fail(err)
		}
		return seconds, nil
	case "!!int", "!!float":
		return fail(fmt.Errorf("timeout: %s is not a duration: the run's timeout is a duration such as 3600s or 1h; tasks use timeout_seconds", node.Value))
	case "!!null":
		return fail(fmt.Errorf("timeout: null is not a duration such as 30m or 6h; omit timeout: for a run with no time limit"))
	}
	return fail(fmt.Errorf("timeout: %s is not a duration such as 30m or 6h", DescribeNode(node)))
}

// shortDuration is Go's duration text without its zero tails: 6h, not 6h0m0s.
// The canonical form uses it, so 6h and 360m are one definition.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

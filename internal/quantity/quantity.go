// Package quantity is the Kubernetes spelling of a resource quantity: the one
// grammar a task's `resources:` is written in, an Orker's caps and defaults
// are configured in, and the two are compared with. The DSL refuses a request
// it cannot read at validate and apply (docs/DSL.md rule 6d), and the Orker
// reads the same request again at dispatch, so a spelling one accepts the
// other never refuses.
package quantity

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	// ErrNotPositiveCPU reports a cpu quantity that reads but is no limit.
	ErrNotPositiveCPU = errors.New("not a positive, finite number of cpus")
	// ErrNotByteCount reports a memory quantity that reads but is no limit.
	ErrNotByteCount = errors.New("not between one byte and what an int64 holds")
)

// ParseCPU reads the Kubernetes CPU spelling: "2", "0.5", "500m". It reads the
// spelling only; CPU also requires a limit.
func ParseCPU(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty cpu quantity")
	}
	if milli, ok := strings.CutSuffix(raw, "m"); ok {
		value, err := strconv.ParseFloat(milli, 64)
		if err != nil {
			return 0, err
		}
		return value / 1000, nil
	}
	return strconv.ParseFloat(raw, 64)
}

// memorySuffixes is the Kubernetes memory spelling, binary and decimal.
var memorySuffixes = []struct {
	suffix string
	factor float64
}{
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"K", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
	{"k", 1e3},
}

// ParseMemory reads the Kubernetes memory spelling: "4Gi", "512Mi", "1000000",
// in bytes. It reads the spelling only; Memory also requires a limit.
func ParseMemory(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty memory quantity")
	}
	for _, unit := range memorySuffixes {
		if trimmed, ok := strings.CutSuffix(raw, unit.suffix); ok {
			value, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
			if err != nil {
				return 0, err
			}
			return value * unit.factor, nil
		}
	}
	return strconv.ParseFloat(raw, 64)
}

// PositiveFinite reports whether a cpu quantity is safe to pass to a runtime
// that interprets zero as unlimited.
func PositiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// ByteCount reports whether a memory quantity, in bytes, is a limit: at least
// one whole byte, because the runtime is handed the byte count as an integer
// and reads 0 as unlimited, and below 2^63, because the attempt record carries
// it as an int64. NaN and both infinities fail it.
func ByteCount(bytes float64) bool {
	return bytes >= 1 && bytes < 1<<63
}

// CPU reads a cpu quantity that is a limit: positive and finite.
func CPU(raw string) (float64, error) {
	cores, err := ParseCPU(raw)
	if err != nil {
		return 0, err
	}
	if !PositiveFinite(cores) {
		return 0, ErrNotPositiveCPU
	}
	return cores, nil
}

// Memory reads a memory quantity that is a limit, in bytes.
func Memory(raw string) (float64, error) {
	bytes, err := ParseMemory(raw)
	if err != nil {
		return 0, err
	}
	if !ByteCount(bytes) {
		return 0, ErrNotByteCount
	}
	return bytes, nil
}

package orker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// A stop's grace is only as long as the process manager lets the Orker live
// after its first signal: killed sooner, its running tasks are lost rather than
// handed back. Where the manager's stop timeout is readable without new
// credentials, the Orker reads it at boot and warns when it is too short; it
// never changes what the Orker does (docs/DEPLOY.md, "Stop timeouts").

// StopTimeoutFloor is what a stop with no grace usually takes past it: the
// default cancellation grace and the longest poll the cluster may hold open.
// It is the figure a platform whose stop timeout is capped (ECS Fargate's
// 120 s) is held to; StopTimeoutTail is the worst case.
const StopTimeoutFloor = DefaultCancellationGrace + 60*time.Second

// fargateStopCap is ECS Fargate's largest stopTimeout.
const fargateStopCap = 120 * time.Second

// StopTimeoutWarnings are the warnings about this process's stop timeout, read
// once at boot: empty where it is long enough or cannot be read.
func StopTimeoutWarnings(ctx context.Context, grace time.Duration) []string {
	var warnings []string
	warnings = append(warnings, ecsStopWarnings(os.Getenv("ECS_CONTAINER_METADATA_URI_V4"), grace)...)
	cgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return warnings
	}
	unit := systemdUnitFromCgroup(string(cgroup))
	if unit == "" {
		return warnings
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "systemctl", "show", unit, "-p", "TimeoutStopUSec", "-p", "KillMode").Output()
	if err != nil {
		// A missing systemctl or a unit it cannot show is unreadable, never
		// an error.
		return warnings
	}
	return append(warnings, systemdStopWarnings(unit, string(out), grace)...)
}

// systemdUnitFromCgroup is the systemd service this process runs in, read from
// /proc/self/cgroup, or "" where it runs in none (a container, a user session).
func systemdUnitFromCgroup(cgroup string) string {
	for line := range strings.SplitSeq(cgroup, "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || (parts[0] != "0" && parts[1] != "name=systemd") {
			continue
		}
		// The innermost service is this process's: a user service sits under
		// its user manager's user@<uid>.service.
		unit := ""
		for elem := range strings.SplitSeq(parts[2], "/") {
			if strings.HasSuffix(elem, ".service") {
				unit = elem
			}
		}
		if unit != "" {
			return unit
		}
	}
	return ""
}

// systemdStopWarnings reads `systemctl show -p TimeoutStopUSec -p KillMode` for
// unit and warns when the stop timeout is below the grace plus the stop's tail
// or KillMode is not mixed. An unreadable figure warns nothing.
func systemdStopWarnings(unit, show string, grace time.Duration) []string {
	var timeout, killMode string
	for line := range strings.SplitSeq(show, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch key {
		case "TimeoutStopUSec":
			timeout = value
		case "KillMode":
			killMode = value
		}
	}
	need := grace + StopTimeoutTail
	var warnings []string
	if span, infinite, err := parseSystemdTimespan(timeout); err == nil && !infinite && span < need {
		warnings = append(warnings, fmt.Sprintf(
			"systemd unit %s stops this Orker after %s (TimeoutStopSec), shorter than its stop grace %s plus the "+
				"stop's tail: set TimeoutStopSec to at least %s, or running tasks are lost rather than handed back",
			unit, span, grace, need))
	}
	if killMode != "" && killMode != "mixed" {
		warnings = append(warnings, fmt.Sprintf(
			"systemd unit %s has KillMode=%s: with anything but KillMode=mixed, systemd signals the runtime processes "+
				"this Orker started when it stops it, so set KillMode=mixed", unit, killMode))
	}
	return warnings
}

// systemdUnits are the time units systemd writes a timespan in.
var systemdUnits = []struct {
	suffix string
	unit   time.Duration
}{
	{"usec", time.Microsecond}, {"us", time.Microsecond},
	{"msec", time.Millisecond}, {"ms", time.Millisecond},
	{"min", time.Minute}, {"sec", time.Second}, {"s", time.Second},
	{"h", time.Hour}, {"d", 24 * time.Hour}, {"w", 7 * 24 * time.Hour},
}

// parseSystemdTimespan reads a systemd timespan ("1min 30s", "24h 15min",
// "infinity"); a bare number is seconds.
func parseSystemdTimespan(text string) (span time.Duration, infinite bool, err error) {
	text = strings.TrimSpace(text)
	if text == "infinity" {
		return 0, true, nil
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 0, false, fmt.Errorf("an empty timespan")
	}
	for _, field := range fields {
		n, unit, err := splitTimespanField(field)
		if err != nil {
			return 0, false, err
		}
		span += time.Duration(n * float64(unit))
	}
	return span, false, nil
}

func splitTimespanField(field string) (float64, time.Duration, error) {
	end := strings.IndexFunc(field, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	if end == 0 {
		return 0, 0, fmt.Errorf("timespan %q has no number", field)
	}
	number, suffix := field, ""
	if end > 0 {
		number, suffix = field[:end], field[end:]
	}
	n, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("timespan %q: %w", field, err)
	}
	if suffix == "" {
		return n, time.Second, nil
	}
	for _, u := range systemdUnits {
		if suffix == u.suffix {
			return n, u.unit, nil
		}
	}
	return 0, 0, fmt.Errorf("timespan %q has an unknown unit", field)
}

// ecsStopWarnings warns, on ECS (its task metadata URI set), when the grace plus
// the stop's floor exceeds Fargate's 120 s cap: the stop timeout itself is not
// readable there.
func ecsStopWarnings(metadataURI string, grace time.Duration) []string {
	if metadataURI == "" || grace+StopTimeoutFloor <= fargateStopCap {
		return nil
	}
	return []string{fmt.Sprintf(
		"this Orker runs on ECS, where its stop timeout cannot be read: its stop grace %s plus the stop's floor %s "+
			"is above Fargate's %.0fs cap, so on Fargate running tasks are lost rather than handed back; an EC2 task "+
			"may set a longer stopTimeout, at least %s",
		grace, StopTimeoutFloor, fargateStopCap.Seconds(), grace+StopTimeoutTail)}
}

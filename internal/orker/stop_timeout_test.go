package orker

import (
	"strings"
	"testing"
	"time"
)

func TestSystemdUnitFromCgroup(t *testing.T) {
	cases := []struct {
		name, cgroup, want string
	}{
		{name: "cgroup v2", cgroup: "0::/system.slice/orker.service\n", want: "orker.service"},
		{name: "cgroup v1", cgroup: "12:cpu:/x\n1:name=systemd:/system.slice/orker@a.service\n", want: "orker@a.service"},
		{name: "a user session scope is no unit we stop", cgroup: "0::/user.slice/user-1000.slice/session-3.scope\n"},
		{name: "a container's cgroup", cgroup: "0::/\n"},
		{name: "a nested cgroup under a service", cgroup: "0::/system.slice/orker.service/payload\n", want: "orker.service"},
		{name: "a user service names its own unit, not the user manager's", cgroup: "0::/user.slice/user-1000.slice/user@1000.service/app.slice/orker.service\n", want: "orker.service"},
		{name: "empty", cgroup: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := systemdUnitFromCgroup(tc.cgroup); got != tc.want {
				t.Fatalf("systemdUnitFromCgroup = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestParseSystemdTimespan(t *testing.T) {
	cases := []struct {
		in       string
		want     time.Duration
		infinite bool
		bad      bool
	}{
		{in: "1min 30s", want: 90 * time.Second},
		{in: "24h 15min", want: 24*time.Hour + 15*time.Minute},
		{in: "1d 15min", want: 24*time.Hour + 15*time.Minute},
		{in: "500ms", want: 500 * time.Millisecond},
		{in: "90", want: 90 * time.Second},
		{in: "infinity", infinite: true},
		{in: "", bad: true},
		{in: "soon", bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, infinite, err := parseSystemdTimespan(tc.in)
			if tc.bad {
				if err == nil {
					t.Fatalf("parseSystemdTimespan(%q) = %v; want an error", tc.in, got)
				}
				return
			}
			if err != nil || got != tc.want || infinite != tc.infinite {
				t.Fatalf("parseSystemdTimespan(%q) = (%v, %v, %v); want (%v, %v)", tc.in, got, infinite, err, tc.want, tc.infinite)
			}
		})
	}
}

// TestStopTimeout_SystemdWarning: a unit whose stop timeout is below the grace
// plus the tail, or whose KillMode is not mixed, is warned about, naming both
// figures; an unreadable one is not an error.
func TestStopTimeout_SystemdWarning(t *testing.T) {
	grace := time.Hour
	need := grace + StopTimeoutTail
	cases := []struct {
		name   string
		show   string
		warns  []string // substrings, each in one warning
		silent bool
	}{
		{name: "below", show: "TimeoutStopUSec=1min 30s\nKillMode=mixed\n", warns: []string{"1m30s", need.String()}},
		{name: "equal", show: "TimeoutStopUSec=" + systemdSpan(need) + "\nKillMode=mixed\n", silent: true},
		{name: "infinity", show: "TimeoutStopUSec=infinity\nKillMode=mixed\n", silent: true},
		{name: "kill mode", show: "TimeoutStopUSec=infinity\nKillMode=control-group\n", warns: []string{"KillMode=mixed", "control-group"}},
		{name: "unreadable", show: "TimeoutStopUSec=soon\nKillMode=mixed\n", silent: true},
		{name: "empty", show: "", silent: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := systemdStopWarnings("orker.service", tc.show, grace)
			if tc.silent {
				if len(got) != 0 {
					t.Fatalf("warnings = %q; want none", got)
				}
				return
			}
			joined := strings.Join(got, "\n")
			for _, want := range tc.warns {
				if !strings.Contains(joined, want) {
					t.Fatalf("warnings = %q; want one naming %q", got, want)
				}
			}
		})
	}
}

// systemdSpan writes d as systemctl show does, in whole seconds.
func systemdSpan(d time.Duration) string {
	return strings.TrimSpace(strings.NewReplacer("h", "h ", "m", "min ", "s", "s").Replace(d.Truncate(time.Second).String()))
}

// TestStopTimeout_ECSWarning: on ECS the stop timeout is not readable, so a
// grace plus tail above Fargate's 120 s cap is warned about.
func TestStopTimeout_ECSWarning(t *testing.T) {
	cases := []struct {
		name  string
		uri   string
		grace time.Duration
		warns bool
	}{
		{name: "unset", grace: time.Hour},
		{name: "set and within 120 s", uri: "http://169.254.170.2/v4/x", grace: 0},
		{name: "set and just above", uri: "http://169.254.170.2/v4/x", grace: 120*time.Second - StopTimeoutFloor + time.Second, warns: true},
		{name: "set and above", uri: "http://169.254.170.2/v4/x", grace: time.Hour, warns: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ecsStopWarnings(tc.uri, tc.grace)
			if (len(got) > 0) != tc.warns {
				t.Fatalf("warnings = %q; want warns=%v", got, tc.warns)
			}
			if tc.warns && (!strings.Contains(got[0], "120s") || !strings.Contains(got[0], "stopTimeout")) {
				t.Fatalf("warning %q names neither Fargate's cap nor stopTimeout", got[0])
			}
		})
	}
}

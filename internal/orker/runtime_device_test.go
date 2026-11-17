package orker

import (
	"strings"
	"testing"
)

// TestDockerCreateArgs_DevicesAreRenderedExplicitly: a device a profile
// passes is one --device host:container:perms with its permissions spelled
// out, and nothing else; a mapping that is not a /dev node to a /dev path
// with r, w or rw is refused before anything reaches the daemon.
func TestDockerCreateArgs_DevicesAreRenderedExplicitly(t *testing.T) {
	spec := validSpec()
	spec.Devices = []DeviceMapping{
		{HostPath: "/dev/ttyUSB199", ContainerPath: "/dev/sensor", Permissions: "rw"},
		{HostPath: "/dev/ttyACM0", ContainerPath: "/dev/modem", Permissions: "r"},
	}
	got := argsString(t, spec)
	for _, want := range []string{"--device /dev/ttyUSB199:/dev/sensor:rw ", "--device /dev/ttyACM0:/dev/modem:r "} {
		if !strings.Contains(got, want) {
			t.Errorf("args lack %q: %s", want, got)
		}
	}
	for _, forbidden := range []string{"--privileged", "--cap-add", "--device-cgroup-rule"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("a device added %s: %s", forbidden, got)
		}
	}
	if plain := argsString(t, validSpec()); strings.Contains(plain, "--device") {
		t.Errorf("a spec with no device renders one: %s", plain)
	}
	for name, d := range map[string]DeviceMapping{
		"host outside /dev":     {HostPath: "/etc/shadow", ContainerPath: "/dev/s", Permissions: "r"},
		"host with a colon":     {HostPath: "/dev/tty:USB0", ContainerPath: "/dev/s", Permissions: "r"},
		"host is a flag":        {HostPath: "--privileged", ContainerPath: "/dev/s", Permissions: "r"},
		"host not clean":        {HostPath: "/dev/../etc/shadow", ContainerPath: "/dev/s", Permissions: "r"},
		"container not in /dev": {HostPath: "/dev/ttyUSB0", ContainerPath: "/orkstack/s", Permissions: "r"},
		"container reserved":    {HostPath: "/dev/ttyUSB0", ContainerPath: "/dev/null", Permissions: "r"},
		"mknod permission":      {HostPath: "/dev/ttyUSB0", ContainerPath: "/dev/s", Permissions: "rwm"},
		"no permissions":        {HostPath: "/dev/ttyUSB0", ContainerPath: "/dev/s"},
	} {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			s.Devices = []DeviceMapping{d}
			if args, err := dockerCreateArgs(s); err == nil {
				t.Fatalf("accepted: %v", args)
			}
		})
	}
}

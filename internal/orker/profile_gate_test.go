package orker

import (
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const gateIdentity = "1d1d000000000000000000000000000000000000000000000000000000000000"

func gateState(t *testing.T) (*profileState, *Cordon) {
	t.Helper()
	s := newProfileState(Profiles{}, "")
	c := NewCordon()
	s.attach(c)
	s.setAudited(map[string]profileAudit{
		"sensor": {identity: gateIdentity, devices: []auditedDevice{{
			ProfileDevice: ProfileDevice{Path: "/dev/serial/by-id/usb-x", ContainerPath: "/dev/sensor", Permissions: "rw"},
			Host:          hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 1000},
		}}},
	})
	return s, c
}

func sensorRef(digest string) []*orkstackv1.ProfileRef {
	return []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: digest}}
}

func sameNode(p string) (hostDevice, error) {
	return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 1000}, nil
}

// TestProfileGate_Step3c holds step 3c (design §4.4) to what this Orker
// audited and what the run pinned: each failure names the profile, is an
// InfrastructureError retried under policy, and a drift latches and cordons.
func TestProfileGate_Step3c(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)

	t.Run("an audited, pinned, unchanged profile passes its devices", func(t *testing.T) {
		s, _ := gateState(t)
		got, failure := s.gate(sensorRef(gateIdentity), sameNode, now)
		if failure != nil {
			t.Fatalf("gate: %v", failure)
		}
		want := DeviceMapping{HostPath: "/dev/ttyUSB199", ContainerPath: "/dev/sensor", Permissions: "rw"}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("devices = %+v", got)
		}
	})
	t.Run("no profile, no devices", func(t *testing.T) {
		s, _ := gateState(t)
		if got, failure := s.gate(nil, sameNode, now); got != nil || failure != nil {
			t.Fatalf("gate = %v, %v", got, failure)
		}
	})
	for _, tc := range []struct {
		name    string
		refs    []*orkstackv1.ProfileRef
		inspect func(string) (hostDevice, error)
		reason  string
		drifts  bool
	}{
		{"not offered here", []*orkstackv1.ProfileRef{{Name: "modem", IdentityDigest: gateIdentity}}, sameNode, ReasonProfileNotOffered, false},
		{"another identity", sensorRef(strings.Repeat("e", 64)), sameNode, ReasonProfileChanged, false},
		{"no identity", sensorRef(""), sameNode, ReasonProfileChanged, false},
		{"the node renumbered", sensorRef(gateIdentity), func(string) (hostDevice, error) {
			return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 0, GID: 1000}, nil
		}, ReasonProfileDrifted, true},
		{"the link moved", sensorRef(gateIdentity), func(string) (hostDevice, error) {
			return hostDevice{Resolved: "/dev/ttyUSB0", Char: true, Major: 188, Minor: 199, GID: 1000}, nil
		}, ReasonProfileDrifted, true},
		{"another group", sensorRef(gateIdentity), func(string) (hostDevice, error) {
			return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 5}, nil
		}, ReasonProfileDrifted, true},
		{"the node is gone", sensorRef(gateIdentity), func(string) (hostDevice, error) {
			return hostDevice{}, errors.New("no such file or directory")
		}, ReasonProfileDrifted, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := gateState(t)
			got, failure := s.gate(tc.refs, tc.inspect, now)
			if failure == nil || failure.Reason != tc.reason {
				t.Fatalf("gate = %v, %v; want %s", got, failure, tc.reason)
			}
			if failure.Category() != CategoryInfrastructureError || !RetryableReason(failure.Reason) {
				t.Errorf("%s is %s, retryable %v", failure.Reason, failure.Category(), RetryableReason(failure.Reason))
			}
			if !strings.Contains(failure.Error(), `"sensor"`) && !strings.Contains(failure.Error(), `"modem"`) {
				t.Errorf("the failure does not name its profile: %v", failure)
			}
			if strings.Contains(failure.Error(), gateIdentity) {
				t.Errorf("the failure names an identity: %v", failure)
			}
			cordoned, _, _ := c.State()
			if cordoned != tc.drifts || s.drifted("sensor") != tc.drifts {
				t.Errorf("cordoned %v, drifted %v; want %v", cordoned, s.drifted("sensor"), tc.drifts)
			}
			if tc.drifts {
				// Latched: the next attempt meets the drift even if the node
				// is back.
				if _, again := s.gate(sensorRef(gateIdentity), sameNode, now); again == nil || again.Reason != ReasonProfileDrifted {
					t.Fatalf("a drift did not latch: %v", again)
				}
			}
		})
	}
}

// TestProfileGate_RecheckBeforeCreate: the recheck just before create is
// the same device comparison, and a change there drifts too.
func TestProfileGate_RecheckBeforeCreate(t *testing.T) {
	s, c := gateState(t)
	now := time.Unix(1_800_000_000, 0)
	if failure := s.recheck(sensorRef(gateIdentity), sameNode, now); failure != nil {
		t.Fatalf("recheck: %v", failure)
	}
	gone := func(string) (hostDevice, error) { return hostDevice{}, errors.New("gone") }
	if failure := s.recheck(sensorRef(gateIdentity), gone, now); failure == nil || failure.Reason != ReasonProfileDrifted {
		t.Fatalf("recheck = %v", failure)
	}
	if cordoned, _, _ := c.State(); !cordoned {
		t.Fatal("a drift at the recheck left the Orker uncordoned")
	}
}

package engine

import (
	"errors"
	"maps"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	digestA = "aaaa000000000000000000000000000000000000000000000000000000000000"
	digestB = "bbbb000000000000000000000000000000000000000000000000000000000000"
)

func deviceOrker(id string, kinds []orkstackv1.ProfileKind, profiles ...*orkstackv1.OrkerProfile) *orkstackv1.DescribeOrkerResult {
	return &orkstackv1.DescribeOrkerResult{OrkerId: id, BuildId: "b-" + id, ProfileKindsSupported: kinds, Profiles: profiles}
}

// offered is a profile in the short form: one contract, "1", its default.
func offered(name string, kind orkstackv1.ProfileKind, digest string) *orkstackv1.OrkerProfile {
	return &orkstackv1.OrkerProfile{Name: name, Kind: kind, IdentityDigest: digest, DefaultContract: "1",
		Contracts: []*orkstackv1.OrkerProfileContract{{Label: "1", IdentityDigest: digest}}}
}

var devicesKind = []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}

// TestJudgeTaskProfiles holds submit's profile refusals (design §4.3) to the
// Orkers that answered: every one must implement the kind and offer the
// profile with one structural identity, which the run then pins. No refusal
// names a digest.
func TestJudgeTaskProfiles(t *testing.T) {
	const dev = orkstackv1.ProfileKind_PROFILE_KIND_DEVICES
	tasks := []*orkstackv1.Task{
		{Id: "read", Devices: []string{"sensor"}},
		{Id: "plain"},
		{Id: "write", Devices: []string{"sensor", "modem"}},
	}
	both := func(id, sensorDigest string) *orkstackv1.DescribeOrkerResult {
		return deviceOrker(id, devicesKind, offered("sensor", dev, sensorDigest), offered("modem", dev, digestB))
	}

	t.Run("one identity pinned per profile", func(t *testing.T) {
		got, err := judgeTaskProfiles("ns", "edge", tasks, []*orkstackv1.DescribeOrkerResult{both("o1", digestA), both("o2", digestA)}, nil)
		if err != nil {
			t.Fatalf("judge: %v", err)
		}
		if want := map[string]string{"sensor": digestA, "modem": digestB}; !maps.Equal(got.identities, want) {
			t.Fatalf("identities = %v, want %v", got, want)
		}
	})
	t.Run("no profiled task asks nothing", func(t *testing.T) {
		got, err := judgeTaskProfiles("ns", "edge", []*orkstackv1.Task{{Id: "plain"}}, nil, nil)
		if err != nil || got.identities != nil || got.contracts != nil {
			t.Fatalf("judge = %v, %v", got, err)
		}
	})

	for _, tc := range []struct {
		name   string
		orkers []*orkstackv1.DescribeOrkerResult
		check  func(t *testing.T, err error)
	}{
		{"no Orker answered", nil, func(t *testing.T, err error) {
			var u *ProfileUnavailableError
			if !errors.As(err, &u) || u.OrkerID != "" || !strings.Contains(err.Error(), "no Orker") {
				t.Fatalf("err = %v", err)
			}
		}},
		{"no Orker offers it", []*orkstackv1.DescribeOrkerResult{deviceOrker("o1", devicesKind, offered("modem", dev, digestB))}, func(t *testing.T, err error) {
			var u *ProfileUnavailableError
			if !errors.As(err, &u) || u.Profile != "sensor" || !strings.Contains(err.Error(), ReasonProfileUnavailable) {
				t.Fatalf("err = %v", err)
			}
		}},
		{"one Orker lacks it", []*orkstackv1.DescribeOrkerResult{both("o1", digestA), deviceOrker("o2", devicesKind, offered("modem", dev, digestB))}, func(t *testing.T, err error) {
			var u *ProfileUnavailableError
			if !errors.As(err, &u) || u.OrkerID != "o2" || u.Profile != "sensor" {
				t.Fatalf("err = %v", err)
			}
		}},
		{"identities differ", []*orkstackv1.DescribeOrkerResult{both("o1", digestA), both("o2", digestB)}, func(t *testing.T, err error) {
			var m *ProfileMismatchError
			if !errors.As(err, &m) || m.Profile != "sensor" || m.OrkerID != "o1" || m.OtherOrkerID != "o2" ||
				!strings.Contains(err.Error(), ReasonProfileMismatch) {
				t.Fatalf("err = %v", err)
			}
		}},
		{"kinds differ", []*orkstackv1.DescribeOrkerResult{
			both("o1", digestA),
			deviceOrker("o2", []orkstackv1.ProfileKind{dev, orkstackv1.ProfileKind_PROFILE_KIND_GPU},
				offered("sensor", orkstackv1.ProfileKind_PROFILE_KIND_GPU, digestA), offered("modem", dev, digestB)),
		}, func(t *testing.T, err error) {
			var m *ProfileMismatchError
			if !errors.As(err, &m) || m.Profile != "sensor" {
				t.Fatalf("err = %v", err)
			}
		}},
		{"an Orker without the kind", []*orkstackv1.DescribeOrkerResult{both("o1", digestA), deviceOrker("old", nil)}, func(t *testing.T, err error) {
			var k *ProfileKindUnsupportedError
			if !errors.As(err, &k) || k.OrkerID != "old" || k.BuildID != "b-old" || k.Kind != "devices" {
				t.Fatalf("err = %v", err)
			}
		}},
		{"an Orker reporting no identity", []*orkstackv1.DescribeOrkerResult{both("o1", "")}, func(t *testing.T, err error) {
			var u *ProfileUnavailableError
			if !errors.As(err, &u) || u.OrkerID != "o1" || !strings.Contains(err.Error(), "identity") {
				t.Fatalf("err = %v", err)
			}
		}},
		{"not a device or gpu profile", []*orkstackv1.DescribeOrkerResult{
			deviceOrker("o1", devicesKind, offered("sensor", orkstackv1.ProfileKind_PROFILE_KIND_UNSPECIFIED, digestA), offered("modem", dev, digestB)),
		}, func(t *testing.T, err error) {
			var n *DevicesNotAProfileError
			if !errors.As(err, &n) || n.Task != "read" || n.Profile != "sensor" {
				t.Fatalf("err = %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeTaskProfiles("ns", "edge", tasks, tc.orkers, nil)
			if err == nil {
				t.Fatalf("judge = %v, want a refusal", got)
			}
			if strings.Contains(err.Error(), digestA) || strings.Contains(err.Error(), digestB) {
				t.Errorf("a refusal names a digest: %v", err)
			}
			if !strings.Contains(err.Error(), `"edge"`) {
				t.Errorf("a refusal does not name its pool: %v", err)
			}
			tc.check(t, err)
		})
	}
}

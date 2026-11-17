package engine

import (
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Submit's profile judgement (design §4.3): a task that names profiles in
// `devices:` runs only on a pool every answering Orker of which implements
// each profile's kind and offers it with one structural identity, which the
// run pins (RunPlan.ProfileIdentities) and every attempt is held to. Nothing
// here names a digest: an identity is compared, never shown.

// Reasons of a refused profiled submit.
const (
	// ReasonProfileUnavailable: an answering Orker of the pool does not offer
	// a profile a task names, or no Orker answered at all.
	ReasonProfileUnavailable = "PROFILE_UNAVAILABLE"
	// ReasonProfileMismatch: the pool's Orkers offer a profile of one name
	// with different kinds or structural identities.
	ReasonProfileMismatch = "PROFILE_MISMATCH"
)

// ProfileUnavailableError refuses a submit whose task names a profile an
// answering Orker of its pool does not offer, or whose pool no Orker
// answered for (OrkerID empty). Any Orker of a pool may take the task, so
// every one must offer it. allow_empty_pool never downgrades it: a device
// cannot be assumed present.
type ProfileUnavailableError struct {
	Namespace string
	Pool      string
	Profile   string
	Tasks     []string
	OrkerID   string
	// Detail says what the Orker reported instead, when it reported the
	// profile at all.
	Detail string
}

func (e *ProfileUnavailableError) Error() string {
	if e.OrkerID == "" {
		return fmt.Sprintf("%s: profile %q (tasks: %s) is named on pool %q (namespace %q), and no Orker of "+
			"the pool answered, so no Orker is known to offer it. Start an Orker that defines it, or try again",
			ReasonProfileUnavailable, e.Profile, strings.Join(e.Tasks, ", "), e.Pool, e.Namespace)
	}
	what := "does not offer it"
	if e.Detail != "" {
		what = e.Detail
	}
	return fmt.Sprintf("%s: profile %q (tasks: %s) is named on pool %q (namespace %q), and its Orker %q %s. "+
		"Every Orker of a pool must define the profile (docs/DEPLOY.md, \"Running a device Orker\")",
		ReasonProfileUnavailable, e.Profile, strings.Join(e.Tasks, ", "), e.Pool, e.Namespace, e.OrkerID, what)
}

// ProfileMismatchError refuses a submit whose task names a profile that two
// Orkers of its pool define differently: another kind, or another
// structural identity. It names the Orkers, never the identities.
type ProfileMismatchError struct {
	Namespace    string
	Pool         string
	Profile      string
	Tasks        []string
	OrkerID      string
	OtherOrkerID string
}

func (e *ProfileMismatchError) Error() string {
	return fmt.Sprintf("%s: profile %q (tasks: %s) is defined differently by Orkers %q and %q of pool %q "+
		"(namespace %q). Every Orker of a pool must define it the same way (docs/DEPLOY.md, \"Running a device Orker\")",
		ReasonProfileMismatch, e.Profile, strings.Join(e.Tasks, ", "), e.OrkerID, e.OtherOrkerID, e.Pool, e.Namespace)
}

// ProfileKindUnsupportedError refuses a submit whose task names a profile of
// a kind one of its pool's Orkers does not implement, an Orker that predates
// it: such an Orker could not hold the task's input. It names the Orker and
// its build, as the other capability refusals do.
type ProfileKindUnsupportedError struct {
	Namespace string
	Pool      string
	Profile   string
	Kind      string
	Tasks     []string
	OrkerID   string
	BuildID   string
}

func (e *ProfileKindUnsupportedError) Error() string {
	return fmt.Sprintf("pool %q (namespace %q, tasks: %s) is served by Orker %q (build %q), which does not "+
		"implement %s profiles, and profile %q is one. Upgrade the pool's Orkers before submitting this Orkflow.",
		e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID, e.BuildID, e.Kind, e.Profile)
}

// DevicesNotAProfileError refuses a task whose `devices:` names a profile
// that is neither a devices nor a gpu profile: the request is malformed.
type DevicesNotAProfileError struct {
	Namespace string
	Pool      string
	Task      string
	Profile   string
}

func (e *DevicesNotAProfileError) Error() string {
	return fmt.Sprintf("task %q names %q in devices:, which pool %q (namespace %q) defines as neither a "+
		"devices nor a gpu profile", e.Task, e.Profile, e.Pool, e.Namespace)
}

// profileKindName is the word a refusal uses for a kind.
func profileKindName(k orkstackv1.ProfileKind) string {
	switch k {
	case orkstackv1.ProfileKind_PROFILE_KIND_DEVICES:
		return "devices"
	case orkstackv1.ProfileKind_PROFILE_KIND_GPU:
		return "gpu"
	default:
		return k.String()
	}
}

// findProfile is the profile of that name an Orker offers, or nil.
func findProfile(o *orkstackv1.DescribeOrkerResult, name string) *orkstackv1.OrkerProfile {
	for _, p := range o.GetProfiles() {
		if p.GetName() == name {
			return p
		}
	}
	return nil
}

// judgeTaskProfiles is the identity every profile the pool's tasks name
// must be run with, by name, or the refusal of the first profile that cannot
// run there. orkers are the pool's answering Orkers, in identity order. Nil
// when no task names a profile.
func judgeTaskProfiles(namespace, pool string, tasks []*orkstackv1.Task, orkers []*orkstackv1.DescribeOrkerResult) (map[string]string, error) {
	var names []string
	naming := map[string][]string{}
	for _, t := range tasks {
		for _, name := range t.GetDevices() {
			if _, seen := naming[name]; !seen {
				names = append(names, name)
			}
			naming[name] = append(naming[name], t.GetId())
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	identities := make(map[string]string, len(names))
	for _, name := range names {
		if len(orkers) == 0 {
			return nil, &ProfileUnavailableError{Namespace: namespace, Pool: pool, Profile: name, Tasks: naming[name]}
		}
		ref, refOrker := firstOffer(orkers, name)
		if ref == nil {
			return nil, &ProfileUnavailableError{Namespace: namespace, Pool: pool, Profile: name, Tasks: naming[name],
				OrkerID: orkers[0].GetOrkerId()}
		}
		kind := ref.GetKind()
		if kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES && kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU {
			return nil, &DevicesNotAProfileError{Namespace: namespace, Pool: pool, Task: naming[name][0], Profile: name}
		}
		if old := lacking(orkers, func(o *orkstackv1.DescribeOrkerResult) bool {
			return slices.Contains(o.GetProfileKindsSupported(), kind)
		}); old != nil {
			return nil, &ProfileKindUnsupportedError{Namespace: namespace, Pool: pool, Profile: name,
				Kind: profileKindName(kind), Tasks: naming[name], OrkerID: old.GetOrkerId(), BuildID: old.GetBuildId()}
		}
		for _, o := range orkers {
			p := findProfile(o, name)
			switch {
			case p == nil:
				return nil, &ProfileUnavailableError{Namespace: namespace, Pool: pool, Profile: name, Tasks: naming[name],
					OrkerID: o.GetOrkerId()}
			case p.GetIdentityDigest() == "":
				return nil, &ProfileUnavailableError{Namespace: namespace, Pool: pool, Profile: name, Tasks: naming[name],
					OrkerID: o.GetOrkerId(), Detail: "reports it with no structural identity"}
			case p.GetKind() != kind || p.GetIdentityDigest() != ref.GetIdentityDigest():
				return nil, &ProfileMismatchError{Namespace: namespace, Pool: pool, Profile: name, Tasks: naming[name],
					OrkerID: refOrker, OtherOrkerID: o.GetOrkerId()}
			}
		}
		identities[name] = ref.GetIdentityDigest()
	}
	return identities, nil
}

// firstOffer is the first Orker's offer of a profile with an identity, and
// that Orker's id; the first offer at all when none reports one.
func firstOffer(orkers []*orkstackv1.DescribeOrkerResult, name string) (*orkstackv1.OrkerProfile, string) {
	var first *orkstackv1.OrkerProfile
	var firstID string
	for _, o := range orkers {
		p := findProfile(o, name)
		if p == nil {
			continue
		}
		if p.GetIdentityDigest() != "" {
			return p, o.GetOrkerId()
		}
		if first == nil {
			first, firstID = p, o.GetOrkerId()
		}
	}
	return first, firstID
}

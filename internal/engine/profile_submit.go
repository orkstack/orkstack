package engine

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Submit's profile judgement (design §4.3; docs/DEPLOY.md, "Changing a
// profile"): a task that names profiles in `devices:` runs only on a pool
// every serving Orker of which implements each profile's kind and serves the
// contract the task's image declared, or, for an image that declares none,
// one its default contract serves. The run pins those contracts
// (RunPlan.ProfileContracts) and one identity beside them
// (RunPlan.ProfileIdentities), and every attempt is held to them. Nothing
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
	// Nowhere: Orkers answered, and not one of them defines the profile.
	Nowhere bool
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
// Orkers of its pool define differently: another kind, or, with Contract
// set, one contract label with two definitions. With Defaults set, the
// pool's Orkers' default contracts serve no one label, so a task whose image
// declares no contract has none it could run under on every Orker. It names
// the Orkers and labels, never the identities.
type ProfileMismatchError struct {
	Namespace    string
	Pool         string
	Profile      string
	Tasks        []string
	OrkerID      string
	OtherOrkerID string
	Contract     string
	// Defaults lists each Orker's default contract ("o1: 2, o2: 3").
	Defaults string
}

func (e *ProfileMismatchError) Error() string {
	switch {
	case e.Defaults != "":
		return fmt.Sprintf("%s: profile %q (tasks: %s) on pool %q (namespace %q): the pool's Orkers serve tasks whose "+
			"image declares no contract under default contracts no one label serves (%s). Add the old contract to "+
			"the new default's accepts:, or finish the roll (docs/DEPLOY.md, \"Changing a profile\")",
			ReasonProfileMismatch, e.Profile, strings.Join(e.Tasks, ", "), e.Pool, e.Namespace, e.Defaults)
	case e.Contract != "":
		return fmt.Sprintf("%s: profile %q (tasks: %s) on pool %q (namespace %q): Orkers %q and %q both define its "+
			"contract %q, differently. Give them one definition, or give the new one another contract label "+
			"(docs/DEPLOY.md, \"Changing a profile\")",
			ReasonProfileMismatch, e.Profile, strings.Join(e.Tasks, ", "), e.Pool, e.Namespace, e.OrkerID, e.OtherOrkerID, e.Contract)
	}
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

// GPUWithoutGPUProfileError refuses a task that asks for gpu units and names
// no profile its pool defines as a gpu profile: an Orker would hold the count
// and give the task no unit.
type GPUWithoutGPUProfileError struct {
	Namespace string
	Pool      string
	Task      string
	GPU       int32
}

func (e *GPUWithoutGPUProfileError) Error() string {
	return fmt.Sprintf("task %q asks for %d gpu units (resources.gpu), but none of the profiles it names in "+
		"devices: is a gpu profile on pool %q (namespace %q); name the pool's gpu profile in devices:",
		e.Task, e.GPU, e.Pool, e.Namespace)
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

// profilePins is what submit pins of the profiles one pool's tasks name:
// the identity each ref's field 2 carries (RunPlan.ProfileIdentities) and the
// contracts it may run under (RunPlan.ProfileContracts), by profile name.
// unpinnedDigest says a pinned label has no definition from a serving or
// cordoned Orker, so the pool's silent Orkers are worth asking once more;
// warnings are what the caller logs (cordoned owners that disagree).
type profilePins struct {
	identities     map[string]string
	contracts      map[string]ProfileContracts
	unpinnedDigest bool
	warnings       []profileWarning
}

// profileWarning is one logged, never refused, oddity of a judgement:
// operator-cordoned Orkers that give one pinned label different definitions.
type profileWarning struct {
	Profile, Contract string
	Orkers            []string
}

// judgeTaskProfiles is what the pool's profiled tasks pin, or the refusal of
// the first profile that cannot run there (docs/DEPLOY.md, "Changing a
// profile"). serving are the pool's answering Orkers that take work or keep
// the pool live; cordoned are its answering operator-cordoned Orkers, read
// only for definitions. Both in identity order. The zero value when no task
// names a profile.
//
// For each profile: every serving Orker implements its kind and offers it
// with at least one contract; serving Orkers that own one label give it one
// definition; every label an image declared is served (owned or accepted) by
// every serving Orker; and, when a task's image declares none, the labels
// every serving Orker's default contract serves meet in at least one.
func judgeTaskProfiles(namespace, pool string, tasks []*orkstackv1.Task, serving, cordoned []*orkstackv1.DescribeOrkerResult) (profilePins, error) {
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
		return profilePins{}, nil
	}
	pins := profilePins{identities: make(map[string]string, len(names)), contracts: make(map[string]ProfileContracts, len(names))}
	kinds := make(map[string]orkstackv1.ProfileKind, len(names))
	for _, name := range names {
		j := profileJudge{namespace: namespace, pool: pool, name: name, tasks: naming[name], serving: serving}
		kind, err := j.offeredAndKind()
		if err != nil {
			return profilePins{}, err
		}
		owners, err := j.oneDefinitionPerLabel()
		if err != nil {
			return profilePins{}, err
		}
		declared, undeclared := declaredLabels(tasks, name)
		pinned := ProfileContracts{}
		for _, label := range declared {
			if err := j.everyOrkerServes(label, tasks); err != nil {
				return profilePins{}, err
			}
			if pinned.Declared == nil {
				pinned.Declared = map[string][]string{}
			}
			pinned.Declared[label] = j.digestsOf(label, owners, cordoned, &pins)
		}
		// The set is pinned only for a task whose image declares no
		// contract: a profile only declaring tasks name pins none, and asks
		// no second look for labels nothing runs under.
		if undeclared {
			set := j.defaultsMeet()
			if len(set) == 0 {
				return profilePins{}, j.defaultsDisjoint()
			}
			for _, label := range set {
				pinned.Set = append(pinned.Set, ContractPin{Label: label, Digests: j.digestsOf(label, owners, cordoned, &pins)})
			}
		}
		pins.contracts[name] = pinned
		pins.identities[name] = fieldTwo(pinned, serving, name)
		kinds[name] = kind
	}
	for _, t := range tasks {
		if gpu := t.GetResources().GetGpu(); gpu != 0 && !slices.ContainsFunc(t.GetDevices(), func(name string) bool {
			return kinds[name] == orkstackv1.ProfileKind_PROFILE_KIND_GPU
		}) {
			return profilePins{}, &GPUWithoutGPUProfileError{Namespace: namespace, Pool: pool, Task: t.GetId(), GPU: gpu}
		}
	}
	return pins, nil
}

// profileJudge is one profile's judgement on one pool.
type profileJudge struct {
	namespace, pool, name string
	tasks                 []string
	serving               []*orkstackv1.DescribeOrkerResult
}

// offeredAndKind refuses a profile a serving Orker does not offer, offers
// with no contract or with a contract of no identity, of a kind it is not,
// or of a kind an Orker does not implement; else the kind.
func (j profileJudge) offeredAndKind() (orkstackv1.ProfileKind, error) {
	if len(j.serving) == 0 {
		return 0, &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks}
	}
	var first *orkstackv1.OrkerProfile
	var firstID string
	for _, o := range j.serving {
		if p := findProfile(o, j.name); p != nil {
			first, firstID = p, o.GetOrkerId()
			break
		}
	}
	if first == nil {
		return 0, &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
			OrkerID: j.serving[0].GetOrkerId(), Nowhere: true}
	}
	kind := first.GetKind()
	if kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES && kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU {
		return 0, &DevicesNotAProfileError{Namespace: j.namespace, Pool: j.pool, Task: j.tasks[0], Profile: j.name}
	}
	if old := lacking(j.serving, func(o *orkstackv1.DescribeOrkerResult) bool {
		return slices.Contains(o.GetProfileKindsSupported(), kind)
	}); old != nil {
		return 0, &ProfileKindUnsupportedError{Namespace: j.namespace, Pool: j.pool, Profile: j.name,
			Kind: profileKindName(kind), Tasks: j.tasks, OrkerID: old.GetOrkerId(), BuildID: old.GetBuildId()}
	}
	for _, o := range j.serving {
		p := findProfile(o, j.name)
		switch {
		case p == nil:
			return 0, &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
				OrkerID: o.GetOrkerId()}
		case p.GetKind() != kind:
			return 0, &ProfileMismatchError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
				OrkerID: firstID, OtherOrkerID: o.GetOrkerId()}
		case defaultContract(p) == nil:
			return 0, &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
				OrkerID: o.GetOrkerId(), Detail: "reports it with no default contract"}
		}
		for _, c := range p.GetContracts() {
			if c.GetIdentityDigest() == "" {
				return 0, &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
					OrkerID: o.GetOrkerId(), Detail: fmt.Sprintf("reports its contract %q with no structural identity", c.GetLabel())}
			}
		}
	}
	return kind, nil
}

// labelOwner is the definition the serving Orkers give one label, and the
// first of them to give it.
type labelOwner struct{ digest, orker string }

// oneDefinitionPerLabel refuses two serving Orkers that own one label with
// different definitions; else each owned label's one definition.
func (j profileJudge) oneDefinitionPerLabel() (map[string]labelOwner, error) {
	owners := map[string]labelOwner{}
	for _, o := range j.serving {
		for _, c := range findProfile(o, j.name).GetContracts() {
			was, ok := owners[c.GetLabel()]
			switch {
			case !ok:
				owners[c.GetLabel()] = labelOwner{digest: c.GetIdentityDigest(), orker: o.GetOrkerId()}
			case was.digest != c.GetIdentityDigest():
				return nil, &ProfileMismatchError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
					OrkerID: was.orker, OtherOrkerID: o.GetOrkerId(), Contract: c.GetLabel()}
			}
		}
	}
	return owners, nil
}

// everyOrkerServes refuses a declared label a serving Orker neither owns nor
// accepts under any contract of the profile: an image moved ahead of the roll.
func (j profileJudge) everyOrkerServes(label string, tasks []*orkstackv1.Task) error {
	for _, o := range j.serving {
		if !slices.ContainsFunc(findProfile(o, j.name).GetContracts(), func(c *orkstackv1.OrkerProfileContract) bool {
			return c.GetLabel() == label || slices.Contains(c.GetAccepts(), label)
		}) {
			return &ProfileUnavailableError{Namespace: j.namespace, Pool: j.pool, Profile: j.name,
				Tasks: declaringTasks(tasks, j.name, label), OrkerID: o.GetOrkerId(),
				Detail: fmt.Sprintf("does not offer its contract %q, which the tasks' image needs. Roll the contract "+
					"in on every Orker of the pool first", label)}
		}
	}
	return nil
}

// defaultsMeet is the labels every serving Orker's default contract serves
// (its own label and its accepts), sorted.
func (j profileJudge) defaultsMeet() []string {
	var meet []string
	for i, o := range j.serving {
		def := defaultContract(findProfile(o, j.name))
		served := append([]string{def.GetLabel()}, def.GetAccepts()...)
		if i == 0 {
			meet = served
			continue
		}
		meet = slices.DeleteFunc(meet, func(l string) bool { return !slices.Contains(served, l) })
	}
	slices.Sort(meet)
	return slices.Compact(meet)
}

// defaultsDisjoint is the refusal of a pool whose Orkers' default contracts
// no one label serves, for a task whose image declares no contract.
func (j profileJudge) defaultsDisjoint() error {
	parts := make([]string, len(j.serving))
	for i, o := range j.serving {
		parts[i] = fmt.Sprintf("%s: %s", o.GetOrkerId(), defaultContract(findProfile(o, j.name)).GetLabel())
	}
	return &ProfileMismatchError{Namespace: j.namespace, Pool: j.pool, Profile: j.name, Tasks: j.tasks,
		Defaults: strings.Join(parts, ", ")}
}

// digestsOf is the definitions a pinned label runs with: the serving owners'
// one, else every one an operator-cordoned owner gives (a disagreement is a
// warning), else none, which only an Orker accepting the label serves.
func (j profileJudge) digestsOf(label string, owners map[string]labelOwner, cordoned []*orkstackv1.DescribeOrkerResult, pins *profilePins) []string {
	if owner, ok := owners[label]; ok {
		return []string{owner.digest}
	}
	var digests, by []string
	for _, o := range cordoned {
		for _, c := range findProfile(o, j.name).GetContracts() {
			if c.GetLabel() == label && c.GetIdentityDigest() != "" {
				by = append(by, o.GetOrkerId())
				digests = append(digests, c.GetIdentityDigest())
			}
		}
	}
	slices.Sort(digests)
	digests = slices.Compact(digests)
	if len(digests) > 1 {
		pins.warnings = append(pins.warnings, profileWarning{Profile: j.name, Contract: label, Orkers: by})
	}
	if len(digests) == 0 {
		pins.unpinnedDigest = true
		return nil
	}
	return digests
}

// fieldTwo is the one identity a ref carries beside its contracts, never
// empty for a pinned profile: label "1"'s single definition, else the first
// pinned definition by label, else the first serving Orker's own default.
func fieldTwo(pinned ProfileContracts, serving []*orkstackv1.DescribeOrkerResult, name string) string {
	byLabel := map[string][]string{}
	for _, pin := range pinned.Set {
		byLabel[pin.Label] = pin.Digests
	}
	maps.Copy(byLabel, pinned.Declared)
	if d := byLabel["1"]; len(d) == 1 {
		return d[0]
	}
	for _, label := range slices.Sorted(maps.Keys(byLabel)) {
		if d := byLabel[label]; len(d) > 0 {
			return d[0]
		}
	}
	return defaultContract(findProfile(serving[0], name)).GetIdentityDigest()
}

// defaultContract is the contract an Orker's profile names its default, or
// nil when it reports none.
func defaultContract(p *orkstackv1.OrkerProfile) *orkstackv1.OrkerProfileContract {
	for _, c := range p.GetContracts() {
		if c.GetLabel() == p.GetDefaultContract() {
			return c
		}
	}
	return nil
}

// declaredLabels is the distinct labels the tasks naming a profile declared
// for it, sorted, and whether any task naming it declared none.
func declaredLabels(tasks []*orkstackv1.Task, name string) (labels []string, undeclared bool) {
	for _, t := range tasks {
		if !slices.Contains(t.GetDevices(), name) {
			continue
		}
		label, ok := t.GetDeviceContracts()[name]
		if !ok {
			undeclared = true
			continue
		}
		labels = append(labels, label)
	}
	slices.Sort(labels)
	return slices.Compact(labels), undeclared
}

// declaringTasks is the ids of the tasks whose image declared label for name.
func declaringTasks(tasks []*orkstackv1.Task, name, label string) []string {
	var ids []string
	for _, t := range tasks {
		if l, ok := t.GetDeviceContracts()[name]; ok && l == label && slices.Contains(t.GetDevices(), name) {
			ids = append(ids, t.GetId())
		}
	}
	return ids
}

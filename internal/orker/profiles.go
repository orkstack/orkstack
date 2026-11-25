package orker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// Profiles are the operator-defined settings a task selects by name
// (orker.profiles, ORK_ORKER_PROFILES; docs/DEPLOY.md, "Profiles"). Their kinds
// are a closed set compiled into the Orker (AGENTS.md §6): a profile is data an
// operator writes, never code.
//
// ParseProfiles checks only what the document itself says. What the host says
// — that a device is of an allowed class, that a CDI spec is trusted — is the
// boot audit's, which each kind brings with it.

// ProfilesVar is the variable orker.profiles renders to.
const ProfilesVar = "ORK_ORKER_PROFILES"

// The profile intervals' defaults.
const (
	DefaultProfileAuditInterval    = 60 * time.Second
	DefaultProfileRecoveryInterval = 5 * time.Second
)

// Bounds the document is held to.
const (
	maxProfileDevices    = 16
	maxProfileGPUUnits   = 64
	maxProfileInterval   = 24 * time.Hour
	minProfileInterval   = time.Second
	maxProfileNameBytes  = 200
	maxProfileValueBytes = 253
	// maxProfileContracts bounds a profile's contracts, and
	// maxContractAccepts the labels one contract also accepts.
	maxProfileContracts = 8
	maxContractAccepts  = 8
	// defaultContractLabel labels a short-form profile written without
	// `contract:`.
	defaultContractLabel = "1"
)

// Profiles is a parsed orker.profiles document.
type Profiles struct {
	// ProbeImage is the orker image, pinned by digest, that runs the profile
	// probes, used as given. Empty on a release build means this release's
	// own image (probe_image.go); a build with no release image needs it.
	ProbeImage       string
	AuditInterval    time.Duration
	RecoveryInterval time.Duration
	// List is every declared profile, sorted by name.
	List []Profile
}

// Profile is one declared profile. Exactly one of GPU or Devices is
// set, according to Kind.
type Profile struct {
	Name string
	Kind orkstackv1.ProfileKind
	// Revision is the operator's label; it changes memo keys only.
	Revision string
	GPU      *GPUProfile
	// Devices are the default contract's devices: what a devices profile
	// passes a task whose image declares no contract (docs/DEPLOY.md,
	// "Changing a profile").
	Devices []ProfileDevice
	// Contracts are the profile's contracts, sorted by label: one for the
	// short form and every gpu profile, up to maxProfileContracts for the
	// long form. Each carries its own devices; a gpu profile's carry none.
	Contracts []ProfileContract
	// Default is the label of the contract an image that declares none runs
	// under.
	Default string
}

// ProfileContract is one contract of a profile: a label for one definition,
// and the other labels it also serves.
type ProfileContract struct {
	Label string
	// Accepts are labels of contracts this profile does not define that a run
	// pinned to them may run under this one: the operator's assertion that the
	// definitions are compatible.
	Accepts []string
	Devices []ProfileDevice
}

// DefaultContract is the profile's default contract, and whether it has one;
// every parsed profile does.
func (p Profile) DefaultContract() (ProfileContract, bool) {
	contracts, def := p.contractList()
	for _, c := range contracts {
		if c.Label == def {
			return c, true
		}
	}
	return ProfileContract{}, false
}

// contractList is the profile's contracts and its default's label. A profile
// built without contracts (as code before them wrote one) is one contract of
// its Devices, labelled defaultContractLabel.
func (p Profile) contractList() ([]ProfileContract, string) {
	if len(p.Contracts) == 0 {
		return []ProfileContract{{Label: defaultContractLabel, Devices: p.Devices}}, defaultContractLabel
	}
	return p.Contracts, p.Default
}

// GPUProfile is a `gpu` profile: counted, exclusive units, each one fully
// qualified CDI device of one CDI kind.
type GPUProfile struct {
	CDIKind string
	// Units are the CDI device names, in the order whose 0-based positions
	// tasks see.
	Units []string
}

// ProfileDevice is one character device of a `devices` profile.
type ProfileDevice struct {
	// Path is the host path; the boot audit resolves it under /dev.
	Path          string
	ContainerPath string
	// Permissions is r, w or rw, always rendered explicitly.
	Permissions string
}

// The settings that share orker.profiles with the profiles themselves. None
// is a slug, so none can collide with a profile's name.
const (
	profilesKeyProbeImage       = "probe_image"
	profilesKeyAuditInterval    = "audit_interval"
	profilesKeyRecoveryInterval = "recovery_interval"
)

var (
	cdiKindPattern       = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?/[a-z0-9_.-]+$`)
	cdiUnitPattern       = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	revisionPattern      = regexp.MustCompile(`^[A-Za-z0-9_.:+-]{1,64}$`)
	containerPathPattern = regexp.MustCompile(`^/dev/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)?$`)
)

// reservedContainerPaths are device paths every container already has, or
// whose replacement would change what the runtime itself provides.
var reservedContainerPaths = map[string]bool{
	"/dev/null": true, "/dev/zero": true, "/dev/full": true, "/dev/random": true,
	"/dev/urandom": true, "/dev/tty": true, "/dev/console": true, "/dev/ptmx": true,
	"/dev/fd": true, "/dev/stdin": true, "/dev/stdout": true, "/dev/stderr": true,
	"/dev/core": true, "/dev/shm": true, "/dev/pts": true, "/dev/mqueue": true,
}

// profilesDocument is the variable's text: the subtree under its own key.
type profilesDocument struct {
	Profiles yaml.Node `yaml:"profiles"`
}

type profileFields struct {
	Kind     string          `yaml:"kind"`
	Revision string          `yaml:"revision"`
	CDI      *cdiFields      `yaml:"cdi"`
	Devices  *[]deviceFields `yaml:"devices"`
	// The contract keys are read as nodes, so a label is the text written,
	// never a number YAML resolved it to.
	Contract  yaml.Node `yaml:"contract"`
	Accepts   yaml.Node `yaml:"accepts"`
	Default   yaml.Node `yaml:"default"`
	Contracts yaml.Node `yaml:"contracts"`
}

// contractFields is one contract of the long form.
type contractFields struct {
	Accepts yaml.Node       `yaml:"accepts"`
	Devices *[]deviceFields `yaml:"devices"`
}

type cdiFields struct {
	Kind  string   `yaml:"kind"`
	Units []string `yaml:"units"`
}

type deviceFields struct {
	Path          string `yaml:"path"`
	ContainerPath string `yaml:"container_path"`
	Permissions   string `yaml:"permissions"`
}

// ParseProfiles decodes an orker.profiles document strictly: one document,
// no unknown key, no anchor, alias, merge key or tag, every value checked. An
// empty document declares no profile.
func ParseProfiles(raw []byte) (Profiles, error) {
	out := Profiles{
		AuditInterval:    DefaultProfileAuditInterval,
		RecoveryInterval: DefaultProfileRecoveryInterval,
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return out, nil
	}
	if line, found := dsl.SecondDocumentLine(raw); found {
		return Profiles{}, fmt.Errorf("line %d: a second YAML document begins here; the setting holds one", line)
	}
	var root yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(raw)).Decode(&root); err != nil && !errors.Is(err, io.EOF) {
		return Profiles{}, fmt.Errorf("parse yaml: %w", err)
	}
	if err := refusePlainOnly(&root); err != nil {
		return Profiles{}, err
	}
	var doc profilesDocument
	if err := strictDecode(raw, &doc); err != nil {
		return Profiles{}, err
	}
	body := &doc.Profiles
	if body.Kind == 0 || (body.Kind == yaml.ScalarNode && body.Tag == "!!null") {
		// A setting that is written but declares nothing is a mistake, not
		// "no profiles": an operator who meant none leaves it unset, or
		// writes `profiles: {}`.
		return Profiles{}, errors.New("profiles: the setting is written but declares nothing; leave it unset or write profiles: {}")
	}
	if body.Kind != yaml.MappingNode {
		return Profiles{}, errors.New("profiles: must be a mapping")
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(body.Content); i += 2 {
		key, value := body.Content[i], body.Content[i+1]
		name := key.Value
		if seen[name] {
			return Profiles{}, fmt.Errorf("profiles.%s: declared twice", name)
		}
		seen[name] = true
		switch name {
		case profilesKeyProbeImage:
			image, err := scalar(value, "profiles."+name)
			if err != nil {
				return Profiles{}, err
			}
			if err := digestPinned(image); err != nil {
				return Profiles{}, fmt.Errorf("profiles.probe_image: %w", err)
			}
			out.ProbeImage = image
		case profilesKeyAuditInterval, profilesKeyRecoveryInterval:
			d, err := profileInterval(value, "profiles."+name)
			if err != nil {
				return Profiles{}, err
			}
			if name == profilesKeyAuditInterval {
				out.AuditInterval = d
			} else {
				out.RecoveryInterval = d
			}
		default:
			p, err := parseProfile(name, value)
			if err != nil {
				return Profiles{}, err
			}
			out.List = append(out.List, p)
		}
	}
	sort.Slice(out.List, func(i, j int) bool { return out.List[i].Name < out.List[j].Name })
	if err := checkOnePerKind(out.List); err != nil {
		return Profiles{}, err
	}
	return out, nil
}

// refusePlainOnly refuses an anchor, alias, merge key or explicit tag
// anywhere in the document: what the operator wrote is what is read.
func refusePlainOnly(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode {
		return fmt.Errorf("line %d: an alias is not allowed in orker.profiles", n.Line)
	}
	if n.Anchor != "" {
		return fmt.Errorf("line %d: an anchor is not allowed in orker.profiles (no alias may refer to it)", n.Line)
	}
	if n.Style&yaml.TaggedStyle != 0 {
		return fmt.Errorf("line %d: an explicit tag is not allowed in orker.profiles", n.Line)
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == "<<" && n.Content[i].Style == 0 {
				return fmt.Errorf("line %d: a merge key is not allowed in orker.profiles", n.Content[i].Line)
			}
		}
	}
	for _, c := range n.Content {
		if err := refusePlainOnly(c); err != nil {
			return err
		}
	}
	return nil
}

func strictDecode(raw []byte, into any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(into); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parse yaml: %w", err)
	}
	return nil
}

func scalar(n *yaml.Node, at string) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" {
		return "", fmt.Errorf("%s: must be a string", at)
	}
	return n.Value, nil
}

func profileInterval(n *yaml.Node, at string) (time.Duration, error) {
	text, err := scalar(n, at)
	if err != nil {
		return 0, err
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration such as 60s", at, text)
	}
	if d < minProfileInterval || d > maxProfileInterval {
		return 0, fmt.Errorf("%s: %s is outside %s to %s", at, d, minProfileInterval, maxProfileInterval)
	}
	return d, nil
}

func digestPinned(image string) error {
	if strings.TrimSpace(image) != image {
		return fmt.Errorf("%q has leading or trailing space", image)
	}
	ref, err := dsl.ParseReference(image)
	if err != nil {
		return err
	}
	if ref.Digest == "" {
		return fmt.Errorf("%q is not pinned by digest (name@sha256:…)", image)
	}
	return nil
}

func parseProfile(name string, value *yaml.Node) (Profile, error) {
	at := "profiles." + name
	if len(name) > maxProfileNameBytes {
		return Profile{}, fmt.Errorf("%s: a profile name is at most %d bytes", at, maxProfileNameBytes)
	}
	if err := dsl.ValidSlug("profile", name); err != nil {
		return Profile{}, fmt.Errorf("%s: %w", at, err)
	}
	if value.Kind != yaml.MappingNode {
		return Profile{}, fmt.Errorf("%s: must be a mapping", at)
	}
	text, err := yaml.Marshal(value)
	if err != nil {
		return Profile{}, fmt.Errorf("%s (line %d): %w", at, value.Line, err)
	}
	var f profileFields
	if err := strictDecode(text, &f); err != nil {
		// The decoded text is this profile alone, so its line numbers count
		// from the profile's own; say where the profile is.
		return Profile{}, fmt.Errorf("%s (line %d): %w", at, value.Line, err)
	}
	// Which keys were written, null or empty included: a key of another kind
	// is refused even when its value is empty.
	keys := map[string]bool{}
	for i := 0; i+1 < len(value.Content); i += 2 {
		keys[value.Content[i].Value] = true
	}
	p := Profile{Name: name}
	if f.Revision != "" {
		if !revisionPattern.MatchString(f.Revision) {
			return Profile{}, fmt.Errorf("%s.revision: %q is not 1-64 letters, digits or ._:+-", at, f.Revision)
		}
		p.Revision = f.Revision
	}
	switch f.Kind {
	case "gpu":
		p.Kind = orkstackv1.ProfileKind_PROFILE_KIND_GPU
		if err := foreignKeys(at, "gpu", keys, "devices"); err != nil {
			return Profile{}, err
		}
		if keys["contracts"] || keys["default"] {
			return Profile{}, fmt.Errorf("%s: a gpu profile has one contract: write `contract` and `accepts`, never `contracts` or `default`", at)
		}
		if p.GPU, err = parseGPU(at, f.CDI); err != nil {
			return Profile{}, err
		}
		err = p.shortContract(at, keys, f, nil)
	case "devices":
		p.Kind = orkstackv1.ProfileKind_PROFILE_KIND_DEVICES
		if err := foreignKeys(at, "devices", keys, "cdi"); err != nil {
			return Profile{}, err
		}
		if keys["contracts"] {
			err = p.longContracts(at, keys, f)
			break
		}
		devices, derr := parseDevices(at, f.Devices)
		if derr != nil {
			return Profile{}, derr
		}
		err = p.shortContract(at, keys, f, devices)
	case "":
		return Profile{}, fmt.Errorf("%s.kind is required: gpu or devices", at)
	default:
		return Profile{}, fmt.Errorf("%s.kind: %q is not a profile kind (gpu or devices)", at, f.Kind)
	}
	if err != nil {
		return Profile{}, err
	}
	return p, nil
}

// shortContract is the short form: one contract of the profile's own keys,
// labelled by `contract:` (defaultContractLabel when absent).
func (p *Profile) shortContract(at string, keys map[string]bool, f profileFields, devices []ProfileDevice) error {
	if keys["default"] {
		return fmt.Errorf("%s.default is for contracts:; the short form's contract is its default", at)
	}
	label := defaultContractLabel
	if keys["contract"] {
		var err error
		if label, err = contractLabel(&f.Contract, at+".contract"); err != nil {
			return err
		}
	}
	accepts, err := contractAccepts(&f.Accepts, at+".accepts", []string{label})
	if err != nil {
		return err
	}
	p.Contracts = []ProfileContract{{Label: label, Accepts: accepts, Devices: devices}}
	p.Default, p.Devices = label, devices
	return nil
}

// longContracts is the long form: `contracts:`, a mapping of label to
// contract, and `default:`, required with two or more.
func (p *Profile) longContracts(at string, keys map[string]bool, f profileFields) error {
	switch {
	case keys["devices"]:
		return fmt.Errorf("%s: write devices under `contracts`, or use the short form without `contracts`", at)
	case keys["contract"]:
		return fmt.Errorf("%s.contract is for the short form; under contracts: each key is a label", at)
	case keys["accepts"]:
		return fmt.Errorf("%s.accepts: write `accepts` under each contract of `contracts`", at)
	}
	node := &f.Contracts
	if node.Kind != yaml.MappingNode || len(node.Content) == 0 || len(node.Content)/2 > maxProfileContracts {
		return fmt.Errorf("%s.contracts: declare 1 to %d contracts", at, maxProfileContracts)
	}
	var labels []string
	for i := 0; i+1 < len(node.Content); i += 2 {
		label, err := contractLabel(node.Content[i], at+".contracts")
		if err != nil {
			return err
		}
		if slices.Contains(labels, label) {
			return fmt.Errorf("%s.contracts: %q is declared twice", at, label)
		}
		labels = append(labels, label)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		label, value := labels[i/2], node.Content[i+1]
		cat := at + ".contracts." + label
		if value.Kind != yaml.MappingNode {
			return fmt.Errorf("%s: must be a mapping", cat)
		}
		text, err := yaml.Marshal(value)
		if err != nil {
			return fmt.Errorf("%s (line %d): %w", cat, value.Line, err)
		}
		var cf contractFields
		if err := strictDecode(text, &cf); err != nil {
			return fmt.Errorf("%s (line %d): %w", cat, value.Line, err)
		}
		accepts, err := contractAccepts(&cf.Accepts, cat+".accepts", labels)
		if err != nil {
			return err
		}
		devices, err := parseDevices(cat, cf.Devices)
		if err != nil {
			return err
		}
		p.Contracts = append(p.Contracts, ProfileContract{Label: label, Accepts: accepts, Devices: devices})
	}
	slices.SortFunc(p.Contracts, func(a, b ProfileContract) int { return strings.Compare(a.Label, b.Label) })
	switch {
	case keys["default"]:
		def, err := contractLabel(&f.Default, at+".default")
		if err != nil {
			return err
		}
		if !slices.Contains(labels, def) {
			return fmt.Errorf("%s.default: %q is not one of its contracts (%s)", at, def, strings.Join(slices.Sorted(slices.Values(labels)), ", "))
		}
		p.Default = def
	case len(labels) == 1:
		p.Default = labels[0]
	default:
		return fmt.Errorf("%s.default is required with %d contracts: name the one an image that declares none runs under", at, len(labels))
	}
	d, _ := p.DefaultContract()
	p.Devices = d.Devices
	return nil
}

// contractLabel is a label written at n: a plain scalar of the revision
// grammar, read as the text written.
func contractLabel(n *yaml.Node, at string) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" {
		return "", fmt.Errorf("%s: a contract label must be a string", at)
	}
	if !revisionPattern.MatchString(n.Value) {
		return "", fmt.Errorf("%s: %q is not a contract label of 1-64 letters, digits or ._:+-", at, n.Value)
	}
	return n.Value, nil
}

// contractAccepts is an `accepts:` list at n, sorted: at most
// maxContractAccepts labels, none listed twice and none of own, the profile's
// own contract labels. An absent list accepts nothing.
func contractAccepts(n *yaml.Node, at string, own []string) ([]string, error) {
	if n.Kind == 0 || (n.Kind == yaml.ScalarNode && n.Tag == "!!null") {
		return nil, nil
	}
	if n.Kind != yaml.SequenceNode || len(n.Content) > maxContractAccepts {
		return nil, fmt.Errorf("%s: a list of at most %d contract labels", at, maxContractAccepts)
	}
	var out []string
	for _, item := range n.Content {
		label, err := contractLabel(item, at)
		if err != nil {
			return nil, err
		}
		switch {
		case slices.Contains(own, label):
			return nil, fmt.Errorf("%s: %q is a contract of this profile; accepts names only other labels", at, label)
		case slices.Contains(out, label):
			return nil, fmt.Errorf("%s: %q is listed twice", at, label)
		}
		out = append(out, label)
	}
	slices.Sort(out)
	return out, nil
}

// foreignKeys refuses a written key that belongs to another kind, checking
// them in the order given so the message is stable.
func foreignKeys(at, kind string, written map[string]bool, foreign ...string) error {
	for _, key := range foreign {
		if written[key] {
			return fmt.Errorf("%s.%s: not a key of a %s profile", at, key, kind)
		}
	}
	return nil
}

func parseGPU(at string, f *cdiFields) (*GPUProfile, error) {
	if f == nil {
		return nil, fmt.Errorf("%s.cdi is required for a gpu profile", at)
	}
	if !cdiKindPattern.MatchString(f.Kind) {
		return nil, fmt.Errorf("%s.cdi.kind: %q is not a CDI kind such as amd.com/gpu", at, f.Kind)
	}
	if len(f.Units) == 0 || len(f.Units) > maxProfileGPUUnits {
		return nil, fmt.Errorf("%s.cdi.units: declare 1 to %d units", at, maxProfileGPUUnits)
	}
	seen := map[string]bool{}
	for i, unit := range f.Units {
		if unit == "all" {
			return nil, fmt.Errorf("%s.cdi.units[%d]: `all` is refused; name each unit", at, i)
		}
		if !cdiUnitPattern.MatchString(unit) || len(unit) > maxProfileValueBytes {
			return nil, fmt.Errorf("%s.cdi.units[%d]: %q is not a CDI device name", at, i, unit)
		}
		if seen[unit] {
			return nil, fmt.Errorf("%s.cdi.units[%d]: %q is a duplicate", at, i, unit)
		}
		seen[unit] = true
	}
	return &GPUProfile{CDIKind: f.Kind, Units: slices.Clone(f.Units)}, nil
}

func parseDevices(at string, list *[]deviceFields) ([]ProfileDevice, error) {
	if list == nil || len(*list) == 0 || len(*list) > maxProfileDevices {
		return nil, fmt.Errorf("%s.devices: declare 1 to %d devices", at, maxProfileDevices)
	}
	out := make([]ProfileDevice, 0, len(*list))
	seen, seenHost := map[string]bool{}, map[string]bool{}
	for i, d := range *list {
		item := fmt.Sprintf("%s.devices[%d]", at, i)
		if err := hostDevicePath(d.Path); err != nil {
			return nil, fmt.Errorf("%s.path: %w", item, err)
		}
		if err := checkContainerPath(d.ContainerPath); err != nil {
			return nil, fmt.Errorf("%s.container_path: %w", item, err)
		}
		switch d.Permissions {
		case "r", "w", "rw":
		default:
			return nil, fmt.Errorf("%s.permissions: %q is not r, w or rw", item, d.Permissions)
		}
		if seen[d.ContainerPath] {
			return nil, fmt.Errorf("%s.container_path: %q is a duplicate", item, d.ContainerPath)
		}
		seen[d.ContainerPath] = true
		if seenHost[d.Path] {
			return nil, fmt.Errorf("%s.path: %q is declared twice", item, d.Path)
		}
		seenHost[d.Path] = true
		out = append(out, ProfileDevice(d))
	}
	for _, a := range out {
		for _, b := range out {
			if strings.HasPrefix(b.ContainerPath, a.ContainerPath+"/") {
				return nil, fmt.Errorf("%s.devices: container_path %q would nest %q", at, a.ContainerPath, b.ContainerPath)
			}
		}
	}
	return out, nil
}

// hostDevicePath holds a host path to a clean, absolute path under /dev that
// a runtime's --device argument cannot misread. The class check is the
// audit's: this only refuses what no class could ever accept.
func hostDevicePath(p string) error {
	if !strings.HasPrefix(p, "/dev/") || len(p) > 4096 {
		return fmt.Errorf("%q is not a path under /dev", p)
	}
	if path.Clean(p) != p || strings.ContainsAny(p, ":,") || !plainMountPath(p) {
		return fmt.Errorf("%q is not a plain path (clean, without : or , or control characters)", p)
	}
	// What no device class could ever be: memory, ports, block-device
	// trees and the container's own mounts. The boot audit still resolves
	// the path and checks it is a character device of an allowed class.
	for _, refused := range refusedHostDevicePaths {
		if p == refused || strings.HasPrefix(p, refused+"/") {
			return fmt.Errorf("%q is under %s, which no device profile may pass", p, refused)
		}
	}
	return nil
}

// refusedHostDevicePaths are host paths no device profile may name, however
// the boot audit would classify them.
var refusedHostDevicePaths = []string{
	"/dev/mem", "/dev/kmem", "/dev/port", "/dev/shm", "/dev/pts", "/dev/mqueue",
	"/dev/disk", "/dev/block", "/dev/mapper", "/dev/fd", "/dev/core",
}

func checkContainerPath(p string) error {
	if !containerPathPattern.MatchString(p) {
		return fmt.Errorf("%q is not /dev/<name> or /dev/<dir>/<name>", p)
	}
	for _, part := range strings.Split(p, "/")[2:] {
		if part == "." || part == ".." {
			return fmt.Errorf("%q has a . or .. component", p)
		}
	}
	if reservedContainerPaths[p] {
		return fmt.Errorf("%q is a path every container already has", p)
	}
	// Nothing goes under one either: /dev/shm/x, /dev/null/x.
	if first := "/dev/" + strings.Split(p, "/")[2]; first != p && reservedContainerPaths[first] {
		return fmt.Errorf("%q is under %s, a path every container already has", p, first)
	}
	return nil
}

// checkOnePerKind holds the per-Orker limit: one gpu profile (its units come
// from one CDI kind).
func checkOnePerKind(list []Profile) error {
	var gpu []string
	for _, p := range list {
		if p.Kind == orkstackv1.ProfileKind_PROFILE_KIND_GPU {
			gpu = append(gpu, p.Name)
		}
	}
	if len(gpu) > 1 {
		return fmt.Errorf("profiles: an Orker offers at most one gpu profile; %s are declared", strings.Join(gpu, ", "))
	}
	return nil
}

// SupportedProfileKinds are the kinds this build implements, which it
// advertises on DescribeOrker. A kind joins this list only in the change that
// implements it end to end.
func SupportedProfileKinds() []orkstackv1.ProfileKind {
	return []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, orkstackv1.ProfileKind_PROFILE_KIND_GPU}
}

// CheckSupported refuses a profile whose kind this build does not implement:
// a declared profile is never silently ignored.
func (p Profiles) CheckSupported(supported []orkstackv1.ProfileKind) error {
	var refused []string
	for _, profile := range p.List {
		if !slices.Contains(supported, profile.Kind) {
			refused = append(refused, fmt.Sprintf("%s (kind %s)", profile.Name, profileKindWord(profile.Kind)))
		}
	}
	if len(refused) > 0 {
		return fmt.Errorf("%s: this Orker's build does not implement the profiles %s", ProfilesVar, strings.Join(refused, ", "))
	}
	return nil
}

// profileKindWord is the kind as the document spells it.
func profileKindWord(k orkstackv1.ProfileKind) string {
	switch k {
	case orkstackv1.ProfileKind_PROFILE_KIND_GPU:
		return "gpu"
	case orkstackv1.ProfileKind_PROFILE_KIND_DEVICES:
		return "devices"
	}
	return fmt.Sprintf("unknown (%d)", int32(k))
}

// DeviceClassifier names the class of a host device path. A devices
// profile's identity needs it, because the class, not the host path, is what
// every Orker of a pool must share.
type DeviceClassifier func(path string) (string, error)

// The canonical identity objects. Their fields are declared in sorted key
// order, so the stable encoding is the canonical one.
type gpuIdentity struct {
	CDIKind string `json:"cdi_kind"`
	Kind    string `json:"kind"`
	Name    string `json:"name"`
}

type deviceIdentity struct {
	Class         string `json:"class"`
	ContainerPath string `json:"container_path"`
	Permissions   string `json:"permissions"`
}

type devicesIdentity struct {
	Devices []deviceIdentity `json:"devices"`
	Kind    string           `json:"kind"`
	Name    string           `json:"name"`
}

// StructuralIdentity is the SHA-256 (hex) of what a run pins and what every
// Orker of a pool must share, for the profile's default contract: for gpu its
// name, kind and CDI kind; for devices its name, kind and each device's
// class, container path and permissions. Units and host paths are local to
// one Orker and are not in it; the revision changes memo keys only, and a
// contract's label is not in it, so equal definitions under two labels have
// one identity.
func (p Profile) StructuralIdentity(classify DeviceClassifier) (string, error) {
	c, _ := p.DefaultContract()
	return p.ContractIdentity(c, classify)
}

// ContractIdentity is StructuralIdentity for one contract of p.
func (p Profile) ContractIdentity(c ProfileContract, classify DeviceClassifier) (string, error) {
	var v any
	switch {
	case p.Kind == orkstackv1.ProfileKind_PROFILE_KIND_GPU && p.GPU != nil:
		v = gpuIdentity{CDIKind: p.GPU.CDIKind, Kind: "gpu", Name: p.Name}
	case p.Kind == orkstackv1.ProfileKind_PROFILE_KIND_DEVICES:
		if classify == nil {
			return "", fmt.Errorf("profile %s: its devices have not been classified", p.Name)
		}
		devices := make([]deviceIdentity, 0, len(c.Devices))
		for _, d := range c.Devices {
			class, err := classify(d.Path)
			if err != nil {
				return "", fmt.Errorf("profile %s: %w", p.Name, err)
			}
			devices = append(devices, deviceIdentity{Class: class, ContainerPath: d.ContainerPath, Permissions: d.Permissions})
		}
		sort.Slice(devices, func(i, j int) bool {
			a, b := devices[i], devices[j]
			if a.Class != b.Class {
				return a.Class < b.Class
			}
			if a.ContainerPath != b.ContainerPath {
				return a.ContainerPath < b.ContainerPath
			}
			return a.Permissions < b.Permissions
		})
		v = devicesIdentity{Devices: devices, Kind: "devices", Name: p.Name}
	default:
		return "", fmt.Errorf("profile %s: kind %s has no identity", p.Name, profileKindWord(p.Kind))
	}
	encoded, err := stablejson.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("profile %s: encode identity: %w", p.Name, err)
	}
	return sha256Hex(encoded), nil
}

// ProfileMemoEntry is one profile a cached task names, for its memo key.
type ProfileMemoEntry struct {
	Identity string `json:"identity"`
	Name     string `json:"name"`
	Revision string `json:"revision"`
}

type profileMemo struct {
	GPU      int32              `json:"gpu"`
	Profiles []ProfileMemoEntry `json:"profiles"`
}

// ProfileMemoComponent is the profiles' part of a cached task's memo key: each
// named profile's name, structural identity and revision, sorted by name, and
// the GPU unit count. It is nil for a task that names no profile, so such a
// task's memo key is unchanged. A profile named twice is refused.
func ProfileMemoComponent(entries []ProfileMemoEntry, gpu int32) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b ProfileMemoEntry) int { return strings.Compare(a.Name, b.Name) })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Name == sorted[i-1].Name {
			return nil, fmt.Errorf("profile memo component: profile %s is named twice", sorted[i].Name)
		}
	}
	encoded, err := stablejson.Marshal(profileMemo{GPU: gpu, Profiles: sorted})
	if err != nil {
		return nil, fmt.Errorf("profile memo component: %w", err)
	}
	return encoded, nil
}

// Describe is the profiles as this Orker reports them on its own channel:
// name, kind, units, revision, each contract with its identity and accepted
// labels, the default contract, and the default's identity as the profile's
// own (what an engine built before contracts pins). Health and binding counts
// are the running Orker's to fill.
func (p Profiles) Describe(classify DeviceClassifier) ([]*orkstackv1.OrkerProfile, error) {
	out := make([]*orkstackv1.OrkerProfile, 0, len(p.List))
	for _, profile := range p.List {
		contracts, def := profile.contractList()
		described := &orkstackv1.OrkerProfile{
			Name:            profile.Name,
			Kind:            profile.Kind,
			Revision:        profile.Revision,
			DefaultContract: def,
		}
		for _, c := range contracts {
			identity, err := profile.ContractIdentity(c, classify)
			if err != nil {
				return nil, err
			}
			described.Contracts = append(described.Contracts, &orkstackv1.OrkerProfileContract{
				Label: c.Label, IdentityDigest: identity, Accepts: slices.Clone(c.Accepts),
			})
			if c.Label == def {
				described.IdentityDigest = identity
			}
		}
		if profile.GPU != nil {
			described.Units = int32(len(profile.GPU.Units))
		}
		out = append(out, described)
	}
	return out, nil
}

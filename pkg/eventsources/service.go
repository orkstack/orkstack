package eventsources

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The labels a service image carries (docs/ORKER_SERVICE_CONTRACT.md, "A
// service image").
const (
	ContractLabel  = "org.orkstack.service.contract"
	ProfileLabel   = "org.orkstack.service.profile"
	InterfaceLabel = "org.orkstack.service.interface"
	// ProfileEventSource is the profile a source image implements.
	ProfileEventSource = "event-source"
	// MaxInterfaceLabelBytes bounds the packed interface.
	MaxInterfaceLabelBytes = 64 << 10
)

// The contract and profile versions this release hosts.
var (
	supportedContracts = []int32{1}
	supportedProfiles  = []int32{1}
)

// firstPartyNames are the names `uses:` reserves for the source images
// OrkStack ships. None ships in this release: a source naming one is
// refused, and a name is never resolved as a registry reference, so it
// cannot change meaning when its image ships.
var firstPartyNames = map[string]bool{"nats-jetstream": true, "nats": true, "sqs": true}

// IsFirstPartyName reports whether uses is a first-party name rather than a
// registry reference.
func IsFirstPartyName(uses string) bool { return firstPartyNames[uses] }

// FirstPartyNames lists the reserved names, sorted.
func FirstPartyNames() []string {
	out := make([]string, 0, len(firstPartyNames))
	for n := range firstPartyNames {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Image is what a source image declares, as its labels say.
type Image struct {
	// Contracts and Profiles are the versions it implements that this
	// release hosts too, highest first.
	Contracts []int32
	Profiles  []int32
	// Interface is its declared interface, or nil when it declares none.
	Interface *orkstackv1.ServiceInterface
}

func imageError(format string, args ...any) *Error {
	return &Error{Reason: ReasonImageUnsupported, Field: "uses", Message: fmt.Sprintf(format, args...)}
}

// ReadServiceLabels reads a source image's labels: the contract and profile
// versions it implements, of which this release must host one each, and
// its interface, which must parse and declare what an image of its class
// must.
func ReadServiceLabels(labels map[string]string) (Image, error) {
	var img Image
	contracts, ok := versions(labels[ContractLabel])
	if !ok {
		return Image{}, imageError("%s: the image's %s label is missing or unreadable", ReasonServiceContractUnknown, ContractLabel)
	}
	img.Contracts = common(contracts, supportedContracts)
	if len(img.Contracts) == 0 {
		return Image{}, imageError("%s: the image implements Orker Service Contract %s; this release hosts %s",
			ReasonServiceContractUnknown, joinVersions(contracts), joinVersions(supportedContracts))
	}
	profile, rest, found := strings.Cut(labels[ProfileLabel], "/")
	profiles, ok := versions(rest)
	if !found || strings.TrimSpace(profile) != ProfileEventSource || !ok {
		return Image{}, imageError("%s: the image's %s label says %q; a source image implements %s/1",
			ReasonServiceProfileUnknown, ProfileLabel, SafeText(labels[ProfileLabel], 64), ProfileEventSource)
	}
	img.Profiles = common(profiles, supportedProfiles)
	if len(img.Profiles) == 0 {
		return Image{}, imageError("%s: the image implements %s %s; this release hosts %s",
			ReasonServiceProfileUnknown, ProfileEventSource, joinVersions(profiles), joinVersions(supportedProfiles))
	}
	if raw, present := labels[InterfaceLabel]; present {
		iface, err := ParseInterfaceLabel(raw)
		if err != nil {
			return Image{}, imageError("its declared interface: %s", err)
		}
		img.Interface = iface
	}
	return img, nil
}

// ParseInterfaceLabel reads a packed ServiceInterface and holds it to what
// a source image must declare.
func ParseInterfaceLabel(raw string) (*orkstackv1.ServiceInterface, error) {
	if len(raw) > MaxInterfaceLabelBytes {
		return nil, fmt.Errorf("the interface is %d bytes; at most %d", len(raw), MaxInterfaceLabelBytes)
	}
	iface := &orkstackv1.ServiceInterface{}
	if err := protojson.Unmarshal([]byte(raw), iface); err != nil {
		return nil, fmt.Errorf("the interface does not parse")
	}
	if err := ValidateInterface(iface); err != nil {
		return nil, err
	}
	return iface, nil
}

// ValidateInterface holds a declared interface to the profile's rules.
func ValidateInterface(iface *orkstackv1.ServiceInterface) error {
	if iface.GetSchemaVersion() != 1 {
		return fmt.Errorf("schemaVersion %d; this release reads 1", iface.GetSchemaVersion())
	}
	if iface.GetProfile() != ProfileEventSource {
		return fmt.Errorf("the interface's profile is %q; a source image's is %s", SafeText(iface.GetProfile(), 64), ProfileEventSource)
	}
	for _, key := range sortedKeys(iface.GetWith()) {
		f := iface.GetWith()[key]
		for _, t := range strings.Split(f.GetType(), "|") {
			switch t {
			case "string", "number", "boolean", "object", "array":
			default:
				return fmt.Errorf("with.%s: type is string, number, boolean, object, array or several joined by |", SafeText(key, 64))
			}
		}
		if f.GetRequired() && f.GetDefault() != nil {
			return fmt.Errorf("with.%s: a key with a default is not required", SafeText(key, 64))
		}
	}
	decl := iface.GetEventSource()
	if decl.GetMaxConcurrency() < 0 || decl.GetMaxConcurrency() > MaxConcurrency {
		return fmt.Errorf("event_source.max_concurrency is 1 to %d", MaxConcurrency)
	}
	if delivery(decl) == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE {
		if decl != nil && decl.DeadLetter != nil && !decl.GetDeadLetter() {
			return fmt.Errorf("an at-least-once image declares dead_letter: its broker stops delivering after a maximum")
		}
		if decl != nil && decl.ExtendsLeases != nil && !decl.GetExtendsLeases() {
			return fmt.Errorf("an at-least-once image declares extends_leases: it keeps a message leased until its verdict")
		}
	}
	return nil
}

func delivery(decl *orkstackv1.EventSourceDeclaration) orkstackv1.DeliveryClass {
	if d := decl.GetDelivery(); d != orkstackv1.DeliveryClass_DELIVERY_CLASS_UNSPECIFIED {
		return d
	}
	return orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE
}

// CheckImage holds a source to its image: its delivery class must be the
// image's, its lanes at most the image's maximum, and its with: what the
// image declares, when it declares an interface.
func CheckImage(img Image, src *orkstackv1.EventSource) error {
	e := Effective(src, Limits{})
	decl := img.Interface.GetEventSource()
	if want := delivery(decl); e.GetDelivery() != want {
		return &Error{Reason: ReasonDeliveryMismatch, Field: "delivery", Message: fmt.Sprintf(
			"the source says %s; its image is %s", deliveryWord(e.GetDelivery()), deliveryWord(want))}
	}
	maxLanes := decl.GetMaxConcurrency()
	if maxLanes == 0 {
		maxLanes = 1
	}
	if e.GetConcurrency() > maxLanes {
		return &Error{Reason: ReasonConfigInvalid, Field: "concurrency", Message: fmt.Sprintf(
			"%d; the image serves at most %d at once", e.GetConcurrency(), maxLanes)}
	}
	if img.Interface == nil {
		return nil
	}
	declared := img.Interface.GetWith()
	with := src.GetWith().GetFields()
	for _, key := range sortedKeys(declared) {
		if _, ok := with[key]; !ok && declared[key].GetRequired() {
			return &Error{Reason: ReasonConfigInvalid, Field: "with." + SafeText(key, 64), Message: "the image requires it"}
		}
	}
	for _, key := range sortedKeys(with) {
		f, ok := declared[key]
		if !ok {
			names := sortedKeys(declared)
			for i, n := range names {
				names[i] = SafeText(n, 64)
			}
			return &Error{Reason: ReasonConfigInvalid, Field: "with." + SafeText(key, 64), Message: fmt.Sprintf(
				"the image declares no such key (it declares %s)", SafeText(strings.Join(names, ", "), 1024))}
		}
		if !hasAnyType(with[key], f.GetType()) {
			return &Error{Reason: ReasonConfigInvalid, Field: "with." + SafeText(key, 64), Message: "the image declares it " + SafeText(f.GetType(), 64)}
		}
	}
	return nil
}

func hasAnyType(v *structpb.Value, types string) bool {
	if types == "" {
		return true
	}
	for _, t := range strings.Split(types, "|") {
		if valueHasType(v, t) {
			return true
		}
	}
	return false
}

// DeliveryWord is a delivery class as a document spells it.
func deliveryWord(d orkstackv1.DeliveryClass) string {
	switch d {
	case orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE:
		return "at-most-once"
	default:
		return "at-least-once"
	}
}

func versions(s string) ([]int32, bool) {
	if strings.TrimSpace(s) == "" {
		return nil, false
	}
	var out []int32
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(part), 10, 32)
		if err != nil || n <= 0 {
			return nil, false
		}
		out = append(out, int32(n))
	}
	return out, true
}

// common is the versions in both lists, highest first.
func common(have, host []int32) []int32 {
	var out []int32
	for _, h := range host {
		for _, v := range have {
			if v == h {
				out = append(out, v)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] > out[j] })
	return out
}

func joinVersions(vs []int32) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strconv.Itoa(int(v))
	}
	return strings.Join(parts, ",")
}

// SafeText renders text an image or a publisher wrote for one of
// OrkStack's sentences: valid UTF-8, no control character, at most limit
// bytes.
func SafeText(s string, limit int) string {
	var b strings.Builder
	for _, r := range strings.ToValidUTF8(s, "�") {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			r = '�'
		}
		if b.Len()+len(string(r)) > limit {
			b.WriteString("…")
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

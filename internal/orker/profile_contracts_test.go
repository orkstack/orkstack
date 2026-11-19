package orker

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// contractsDoc is a profiles document with the given profile body under
// `sensor:`, indented four spaces.
func contractsDoc(body string) string {
	return "profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n  sensor:\n" + body
}

const contractDevice = `
          - path: /dev/serial/by-id/usb-FTDI_FT232R-if00-port0
            container_path: /dev/sensor
            permissions: r`

// TestParseOrkerProfiles_Contracts: the short form is one contract, labelled
// "1" unless `contract:` says otherwise; the long form names its contracts
// and, with two or more, its default; every rule a contract is held to
// refuses the boot naming the key.
func TestParseOrkerProfiles_Contracts(t *testing.T) {
	type contract struct {
		label   string
		accepts []string
		paths   []string
	}
	for name, tc := range map[string]struct {
		body      string
		def       string
		contracts []contract
		refused   string
	}{
		"short form is contract 1": {
			body: "    kind: devices\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "),
			def:  "1", contracts: []contract{{label: "1", paths: []string{"/dev/sensor"}}},
		},
		"short form with a label and accepts": {
			body: "    kind: devices\n    contract: \"2\"\n    accepts:\n      - \"1\"\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "),
			def:  "2", contracts: []contract{{label: "2", accepts: []string{"1"}, paths: []string{"/dev/sensor"}}},
		},
		"an unquoted label is its text": {
			body: "    kind: devices\n    contract: 2.10\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "),
			def:  "2.10", contracts: []contract{{label: "2.10", paths: []string{"/dev/sensor"}}},
		},
		"long form with a default": {
			body: "    kind: devices\n    default: \"1\"\n    contracts:\n      \"1\":\n        devices:" + contractDevice +
				"\n      \"2\":\n        accepts:\n          - \"0\"\n        devices:\n          - path: /dev/serial/by-id/usb-FTDI_FT232R-if00-port0\n            container_path: /dev/ttySensor\n            permissions: rw",
			def: "1", contracts: []contract{{label: "1", paths: []string{"/dev/sensor"}}, {label: "2", accepts: []string{"0"}, paths: []string{"/dev/ttySensor"}}},
		},
		"a contract accepting a sibling": {body: "    kind: devices\n    default: \"1\"\n    contracts:\n      \"1\":\n        devices:" + contractDevice + "\n      \"2\":\n        accepts:\n          - \"1\"\n        devices:" + contractDevice, refused: "accepts: \"1\" is a contract of this profile"},
		"long form with one contract needs no default": {
			body: "    kind: devices\n    contracts:\n      a:\n        devices:" + contractDevice,
			def:  "a", contracts: []contract{{label: "a", paths: []string{"/dev/sensor"}}},
		},
		"two contracts and no default":     {body: "    kind: devices\n    contracts:\n      \"1\":\n        devices:" + contractDevice + "\n      \"2\":\n        devices:" + contractDevice, refused: "sensor.default is required"},
		"a default that names no contract": {body: "    kind: devices\n    default: \"3\"\n    contracts:\n      \"1\":\n        devices:" + contractDevice, refused: "sensor.default: \"3\" is not one of"},
		"a label outside the grammar":      {body: "    kind: devices\n    contract: \"two words\"\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "), refused: "sensor.contract: \"two words\" is not"},
		"accepts its own label":            {body: "    kind: devices\n    accepts:\n      - \"1\"\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "), refused: "accepts: \"1\" is a contract of this profile"},
		"accepts a label twice":            {body: "    kind: devices\n    contract: \"2\"\n    accepts:\n      - \"1\"\n      - \"1\"\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "), refused: "accepts: \"1\" is listed twice"},
		"both forms at once":               {body: "    kind: devices\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      ") + "\n    contracts:\n      \"1\":\n        devices:" + contractDevice, refused: "write devices under `contracts`"},
		"a default with the short form":    {body: "    kind: devices\n    default: \"1\"\n    devices:" + strings.ReplaceAll(contractDevice, "          ", "      "), refused: "sensor.default is for contracts"},
		"contract with the long form":      {body: "    kind: devices\n    contract: \"1\"\n    contracts:\n      \"1\":\n        devices:" + contractDevice, refused: "sensor.contract is for the short form"},
		"an empty contracts mapping":       {body: "    kind: devices\n    contracts: {}", refused: "declare 1 to 8 contracts"},
		"an unknown key in a contract":     {body: "    kind: devices\n    contracts:\n      \"1\":\n        devices:" + contractDevice + "\n        revision: x", refused: "contracts.1"},
		"a contract with no devices":       {body: "    kind: devices\n    contracts:\n      \"1\":\n        accepts:\n          - \"0\"", refused: "sensor.contracts.1.devices: declare 1 to 16 devices"},
		"one path twice within a contract": {body: "    kind: devices\n    contracts:\n      \"1\":\n        devices:" + contractDevice + contractDevice, refused: "is a duplicate"},
		"nine contracts":                   {body: "    kind: devices\n    default: c1\n    contracts:" + nineContracts(), refused: "declare 1 to 8 contracts"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ParseProfiles([]byte(contractsDoc(tc.body)))
			if tc.refused != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refused) {
					t.Fatalf("ParseProfiles = %v; want it refused with %q", err, tc.refused)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseProfiles: %v", err)
			}
			p := got.List[0]
			if p.Default != tc.def {
				t.Errorf("default = %q, want %q", p.Default, tc.def)
			}
			var have []contract
			for _, c := range p.Contracts {
				var paths []string
				for _, d := range c.Devices {
					paths = append(paths, d.ContainerPath)
				}
				have = append(have, contract{label: c.Label, accepts: c.Accepts, paths: paths})
			}
			if len(have) != len(tc.contracts) {
				t.Fatalf("contracts = %+v, want %+v", have, tc.contracts)
			}
			for i := range have {
				if have[i].label != tc.contracts[i].label || !slices.Equal(have[i].accepts, tc.contracts[i].accepts) ||
					!slices.Equal(have[i].paths, tc.contracts[i].paths) {
					t.Errorf("contract %d = %+v, want %+v", i, have[i], tc.contracts[i])
				}
			}
			if d, ok := p.DefaultContract(); !ok || d.Label != tc.def || !slices.Equal(p.Devices, d.Devices) {
				t.Errorf("Devices are not the default contract's: %+v vs %+v", p.Devices, d)
			}
		})
	}
}

// TestParseOrkerProfiles_GPUHasOneContract: a gpu profile takes `contract:`
// and `accepts:`, never `contracts:` or `default:`.
func TestParseOrkerProfiles_GPUHasOneContract(t *testing.T) {
	gpu := "profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n  amd:\n    kind: gpu\n%s    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n"
	got, err := ParseProfiles([]byte(strings.Replace(gpu, "%s", "    contract: v2\n    accepts:\n      - \"1\"\n", 1)))
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	if p := got.List[0]; p.Default != "v2" || len(p.Contracts) != 1 || !slices.Equal(p.Contracts[0].Accepts, []string{"1"}) {
		t.Fatalf("gpu contracts = %+v, default %q", p.Contracts, p.Default)
	}
	for _, key := range []string{"    default: \"1\"\n", "    contracts: {}\n"} {
		if _, err := ParseProfiles([]byte(strings.Replace(gpu, "%s", key, 1))); err == nil || !strings.Contains(err.Error(), "a gpu profile has one contract") {
			t.Errorf("a gpu profile with %q = %v; want it refused", strings.TrimSpace(key), err)
		}
	}
}

func nineContracts() string {
	var b strings.Builder
	for i := 1; i <= 9; i++ {
		b.WriteString("\n      c" + string(rune('0'+i)) + ":\n        devices:" + contractDevice)
	}
	return b.String()
}

// usbNodes is a host where every /dev/ttyUSB<n> is a usb-serial node of
// minor n.
func usbNodes(path string) (hostDevice, error) {
	var minor uint32
	fmt.Sscanf(strings.TrimPrefix(path, "/dev/ttyUSB"), "%d", &minor)
	return hostDevice{Resolved: path, Char: true, Major: 188, Minor: minor, GID: 20}, nil
}

const usbProcDevices = "Character devices:\n188 ttyUSB\n"

func twoContracts(name, path1, path2 string) Profile {
	return Profile{
		Name: name, Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, Default: "1",
		Devices: []ProfileDevice{{Path: "/dev/ttyUSB0", ContainerPath: path1, Permissions: "r"}},
		Contracts: []ProfileContract{
			{Label: "1", Devices: []ProfileDevice{{Path: "/dev/ttyUSB0", ContainerPath: path1, Permissions: "r"}}},
			{Label: "2", Devices: []ProfileDevice{{Path: "/dev/ttyUSB1", ContainerPath: path2, Permissions: "rw"}}},
		},
	}
}

// TestAudit_ContractsOfOneNameMayShareAContainerPath: a task holds one
// contract of a name, so two of its contracts may give the same container
// path; each contract is audited with its own identity, the profile's own is
// its default's, and Describe reports every contract.
func TestAudit_ContractsOfOneNameMayShareAContainerPath(t *testing.T) {
	declared := Profiles{List: []Profile{twoContracts("sensor", "/dev/sensor", "/dev/sensor")}}
	audits, described, err := auditProfiles(declared, usbProcDevices, usbNodes)
	if err != nil {
		t.Fatalf("auditProfiles: %v", err)
	}
	a := audits["sensor"]
	one, two := a.contracts["1"], a.contracts["2"]
	if one.identity == "" || two.identity == "" || one.identity == two.identity || a.identity != one.identity {
		t.Fatalf("identities: profile %q, 1 %q, 2 %q; want two distinct, the profile's the default's", a.identity, one.identity, two.identity)
	}
	if len(one.devices) != 1 || one.devices[0].Host.Resolved != "/dev/ttyUSB0" || two.devices[0].Host.Resolved != "/dev/ttyUSB1" {
		t.Fatalf("contract devices = %+v / %+v", one.devices, two.devices)
	}
	if len(a.devices) != 2 {
		t.Fatalf("the drift set = %+v; want both host devices once", a.devices)
	}
	if a.devices[0].ContainerPath == a.devices[1].ContainerPath {
		t.Fatalf("the probe would be given %s twice", a.devices[0].ContainerPath)
	}
	d := described[0]
	if d.GetDefaultContract() != "1" || d.GetIdentityDigest() != one.identity || len(d.GetContracts()) != 2 ||
		d.GetContracts()[1].GetLabel() != "2" || d.GetContracts()[1].GetIdentityDigest() != two.identity {
		t.Fatalf("described = %v", d)
	}
}

// TestAudit_ContractsOfOtherNamesMayNotShareAContainerPath: a task may hold
// both names, so any contract of one may not give a container path a
// contract of another gives.
func TestAudit_ContractsOfOtherNamesMayNotShareAContainerPath(t *testing.T) {
	declared := Profiles{List: []Profile{twoContracts("a", "/dev/x", "/dev/y"), twoContracts("b", "/dev/z", "/dev/y")}}
	if _, _, err := auditProfiles(declared, usbProcDevices, usbNodes); err == nil || !strings.Contains(err.Error(), "both pass a device at /dev/y") {
		t.Fatalf("auditProfiles = %v; want the shared container path refused", err)
	}
}

// TestHostViewProbe_CountsDistinctPaths: the host-view probe asks about each
// host device once, so its bound counts distinct host paths, not contracts.
func TestHostViewProbe_CountsDistinctPaths(t *testing.T) {
	var contracts []ProfileContract
	for c := 0; c < maxProfileContracts; c++ {
		var devices []ProfileDevice
		for j := 0; j < maxProfileDevices; j++ {
			devices = append(devices, ProfileDevice{Path: fmt.Sprintf("/dev/ttyUSB%d", j), ContainerPath: fmt.Sprintf("/dev/s%d", j), Permissions: "r"})
		}
		contracts = append(contracts, ProfileContract{Label: fmt.Sprint(c), Devices: devices})
	}
	p := Profile{Name: "many", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, Default: "0", Contracts: contracts, Devices: contracts[0].Devices}
	if _, _, err := auditProfiles(Profiles{List: []Profile{p}}, usbProcDevices, usbNodes); err != nil {
		t.Fatalf("%d contracts of the same %d devices = %v; want them counted once", maxProfileContracts, maxProfileDevices, err)
	}
}

// contractState is an Orker whose profile sensor has contract 1 (digest d1,
// at /dev/sensor, the default) and contract 2 (digest d2, at /dev/ttySensor,
// accepting 0 and 9).
func contractState(t *testing.T) *profileState {
	t.Helper()
	s := newProfileState(Profiles{}, "")
	s.attach(NewCordon())
	dev := func(host, at string, minor uint32) auditedDevice {
		return auditedDevice{ProfileDevice: ProfileDevice{Path: host, ContainerPath: at, Permissions: "rw"},
			Host: hostDevice{Resolved: host, Char: true, Major: 188, Minor: minor, GID: 20}}
	}
	one, two := dev("/dev/ttyUSB0", "/dev/sensor", 0), dev("/dev/ttyUSB1", "/dev/ttySensor", 1)
	s.setAudited(map[string]profileAudit{"sensor": {
		identity: "d1", def: "1", devices: []auditedDevice{one, two},
		contracts: map[string]contractAudit{
			"1": {identity: "d1", devices: []auditedDevice{one}},
			"2": {identity: "d2", accepts: []string{"0", "9"}, devices: []auditedDevice{two}},
		},
	}})
	return s
}

func pin(label string, digests ...string) *orkstackv1.ProfileContractPin {
	return &orkstackv1.ProfileContractPin{Label: label, IdentityDigests: digests}
}

// TestProfileGate_ContractMatrix holds step 3c's selection (design §5) to its
// three tables: a ref without contracts is judged by its digest alone; a
// declared ref runs under the contract owning its label with a pinned
// digest, or one accepting it; an undeclared ref runs under the default
// contract only. Every contract fault is marked never-started.
func TestProfileGate_ContractMatrix(t *testing.T) {
	for name, tc := range map[string]struct {
		ref          *orkstackv1.ProfileRef
		wantPath     string
		wantLabel    string
		wantAccepted bool
		wantReason   string
		neverStarted bool
	}{
		"no contracts, the default's digest":      {ref: &orkstackv1.ProfileRef{IdentityDigest: "d1"}, wantPath: "/dev/sensor", wantLabel: "1"},
		"no contracts, another contract's digest": {ref: &orkstackv1.ProfileRef{IdentityDigest: "d2"}, wantPath: "/dev/ttySensor", wantLabel: "2"},
		"no contracts, no digest of this Orker":   {ref: &orkstackv1.ProfileRef{IdentityDigest: "dx"}, wantReason: ReasonProfileChanged},
		"no contracts and no digest":              {ref: &orkstackv1.ProfileRef{}, wantReason: ReasonProfileChanged},

		"declared, owned with its digest":       {ref: &orkstackv1.ProfileRef{Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("2", "d2")}}, wantPath: "/dev/ttySensor", wantLabel: "2"},
		"declared, owned with another digest":   {ref: &orkstackv1.ProfileRef{Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("2", "dx")}}, wantReason: ReasonProfileChanged, neverStarted: true},
		"declared, accepted by a non-default":   {ref: &orkstackv1.ProfileRef{Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("9", "d9")}}, wantPath: "/dev/ttySensor", wantLabel: "2", wantAccepted: true},
		"declared, owned with no digest pinned": {ref: &orkstackv1.ProfileRef{Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("1")}}, wantReason: ReasonProfileContractNotServed, neverStarted: true},
		"declared, a label nobody serves":       {ref: &orkstackv1.ProfileRef{Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("7", "d7")}}, wantReason: ReasonProfileContractNotServed, neverStarted: true},
		"declared with no pin":                  {ref: &orkstackv1.ProfileRef{Declared: true}, wantReason: ReasonProfileContractNotServed, neverStarted: true},

		"undeclared, the default in the set":    {ref: &orkstackv1.ProfileRef{IdentityDigest: "d1", Contracts: []*orkstackv1.ProfileContractPin{pin("1", "d1", "d1b")}}, wantPath: "/dev/sensor", wantLabel: "1"},
		"undeclared, the default edited":        {ref: &orkstackv1.ProfileRef{IdentityDigest: "d1", Contracts: []*orkstackv1.ProfileContractPin{pin("1", "dold")}}, wantReason: ReasonProfileChanged, neverStarted: true},
		"undeclared, only another contract":     {ref: &orkstackv1.ProfileRef{IdentityDigest: "d2", Contracts: []*orkstackv1.ProfileContractPin{pin("2", "d2")}}, wantReason: ReasonProfileContractNotServed, neverStarted: true},
		"undeclared, the default's label alone": {ref: &orkstackv1.ProfileRef{IdentityDigest: "d1", Contracts: []*orkstackv1.ProfileContractPin{pin("1")}}, wantReason: ReasonProfileContractNotServed, neverStarted: true},
	} {
		t.Run(name, func(t *testing.T) {
			tc.ref.Name = "sensor"
			s := contractState(t)
			devices, picked, failure := s.gateSelect([]*orkstackv1.ProfileRef{tc.ref}, usbNodes, time.Unix(1_800_000_000, 0))
			if tc.wantReason != "" {
				if failure == nil || failure.Reason != tc.wantReason || failure.NeverStarted != tc.neverStarted {
					t.Fatalf("failure = %+v; want %s, never started %v", failure, tc.wantReason, tc.neverStarted)
				}
				if !strings.Contains(failure.Message, `"sensor"`) {
					t.Errorf("the failure %q does not name the profile", failure.Message)
				}
				return
			}
			if failure != nil {
				t.Fatalf("failure = %+v", failure)
			}
			if len(devices) != 1 || devices[0].ContainerPath != tc.wantPath {
				t.Fatalf("devices = %+v; want the contract at %s", devices, tc.wantPath)
			}
			if len(picked) != 1 || picked[0].Label != tc.wantLabel || picked[0].Accepted != tc.wantAccepted || picked[0].Declared != tc.ref.GetDeclared() {
				t.Fatalf("selection = %+v; want %s, accepted %v", picked, tc.wantLabel, tc.wantAccepted)
			}
		})
	}
}

// TestProfileGate_UndeclaredUsesOnlyTheDefault: an image that declares
// nothing never meets a contract added for images that need it, even one the
// run pinned.
func TestProfileGate_UndeclaredUsesOnlyTheDefault(t *testing.T) {
	s := contractState(t)
	ref := &orkstackv1.ProfileRef{Name: "sensor", IdentityDigest: "d1",
		Contracts: []*orkstackv1.ProfileContractPin{pin("1", "d1"), pin("2", "d2")}}
	devices, picked, failure := s.gateSelect([]*orkstackv1.ProfileRef{ref}, usbNodes, time.Now())
	if failure != nil || picked[0].Label != "1" || devices[0].ContainerPath != "/dev/sensor" {
		t.Fatalf("selection = %+v, %+v, %v; want the default contract 1", picked, devices, failure)
	}
}

// TestProfileGate_DeclaredPicksTheNamedContract: a declared ref runs under
// the contract its image named, not the default.
func TestProfileGate_DeclaredPicksTheNamedContract(t *testing.T) {
	s := contractState(t)
	ref := &orkstackv1.ProfileRef{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("2", "d2")}}
	devices, picked, failure := s.gateSelect([]*orkstackv1.ProfileRef{ref}, usbNodes, time.Now())
	if failure != nil || picked[0].Label != "2" || !picked[0].Declared || devices[0].ContainerPath != "/dev/ttySensor" {
		t.Fatalf("selection = %+v, %+v, %v; want contract 2, declared", picked, devices, failure)
	}
}

// TestProfileGate_LateOwnerWithWrongDefinition: an Orker that owns the pinned
// label with another definition fails PROFILE_CHANGED before any accepts:
// could mask the edit.
func TestProfileGate_LateOwnerWithWrongDefinition(t *testing.T) {
	s := contractState(t)
	ref := &orkstackv1.ProfileRef{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("2", "d2-old")}}
	if _, _, failure := s.gateSelect([]*orkstackv1.ProfileRef{ref}, usbNodes, time.Now()); failure == nil ||
		failure.Reason != ReasonProfileChanged || !failure.NeverStarted {
		t.Fatalf("failure = %+v; want PROFILE_CHANGED, never started", failure)
	}
}

// TestProfileGate_DriftCheckedBeforeServed: a drifted device of any contract
// of the name fails the attempt PROFILE_DRIFTED, even for a run this Orker
// would not have served.
func TestProfileGate_DriftCheckedBeforeServed(t *testing.T) {
	s := contractState(t)
	moved := func(p string) (hostDevice, error) {
		d, err := usbNodes(p)
		if p == "/dev/ttyUSB1" {
			d.Minor = 99
		}
		return d, err
	}
	ref := &orkstackv1.ProfileRef{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("7", "d7")}}
	if _, _, failure := s.gateSelect([]*orkstackv1.ProfileRef{ref}, moved, time.Now()); failure == nil || failure.Reason != ReasonProfileDrifted {
		t.Fatalf("failure = %+v; want PROFILE_DRIFTED from contract 2's device", failure)
	}
}

// TestProfileMemoComponent_UsesOwnDigest: a cached task's memo key carries
// the identity of the contract that runs it here, not the run's pin, so a
// structural change misses and equal definitions under two labels hit.
func TestProfileMemoComponent_UsesOwnDigest(t *testing.T) {
	s := contractState(t)
	for name, tc := range map[string]struct {
		ref  *orkstackv1.ProfileRef
		want string
	}{
		"declared contract 2":     {&orkstackv1.ProfileRef{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("2", "d2")}}, "d2"},
		"accepted label 9":        {&orkstackv1.ProfileRef{Name: "sensor", Declared: true, Contracts: []*orkstackv1.ProfileContractPin{pin("9", "d9")}}, "d2"},
		"a ref without contracts": {&orkstackv1.ProfileRef{Name: "sensor", IdentityDigest: "d1"}, "d1"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := s.memoIdentity(tc.ref); got != tc.want {
				t.Fatalf("memo identity = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNewOrker_RefWithoutContractsJudgesDigest: a ref from an engine built
// before contracts runs only under a contract whose definition is the one it
// pinned, never on a label.
func TestNewOrker_RefWithoutContractsJudgesDigest(t *testing.T) {
	s := contractState(t)
	if _, _, failure := s.gateSelect([]*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: "d3"}}, usbNodes, time.Now()); failure == nil ||
		failure.Reason != ReasonProfileChanged || failure.NeverStarted {
		t.Fatalf("failure = %+v; want PROFILE_CHANGED under the task's policy, as before contracts", failure)
	}
}

// FuzzParseProfiles states what ParseProfiles claims of any document it
// accepts: every profile has 1 to maxProfileContracts contracts with labels
// of the revision grammar, sorted and distinct; its default names one of
// them and its Devices are that one's; no contract accepts a label of its
// own profile or more than maxContractAccepts; and parsing is deterministic.
// Seeds are the table tests' documents.
func FuzzParseProfiles(f *testing.F) {
	f.Add(validProfilesDoc)
	device := strings.ReplaceAll(contractDevice, "          ", "      ")
	for _, body := range []string{
		"    kind: devices\n    devices:" + device,
		"    kind: devices\n    contract: \"2\"\n    accepts:\n      - \"1\"\n    devices:" + device,
		"    kind: devices\n    contract: 2.10\n    devices:" + device,
		"    kind: devices\n    default: \"1\"\n    contracts:\n      \"1\":\n        devices:" + contractDevice + "\n      \"2\":\n        accepts:\n          - \"0\"\n        devices:" + contractDevice,
		"    kind: devices\n    contracts:\n      \"1\":\n        devices:" + contractDevice + "\n      \"2\":\n        devices:" + contractDevice,
		"    kind: devices\n    accepts:\n      - \"1\"\n    devices:" + device,
		"    kind: devices\n    contracts: {}",
		"    kind: devices\n    default: c1\n    contracts:" + nineContracts(),
	} {
		f.Add(contractsDoc(body))
	}
	f.Fuzz(func(t *testing.T, doc string) {
		got, err := ParseProfiles([]byte(doc))
		if err != nil {
			return
		}
		again, err := ParseProfiles([]byte(doc))
		if err != nil || !reflect.DeepEqual(again, got) {
			t.Fatalf("parsing twice differs: %v\n%+v\n%+v", err, got, again)
		}
		for _, p := range got.List {
			if len(p.Contracts) == 0 || len(p.Contracts) > maxProfileContracts {
				t.Fatalf("profile %s has %d contracts", p.Name, len(p.Contracts))
			}
			var labels []string
			for _, c := range p.Contracts {
				if !revisionPattern.MatchString(c.Label) {
					t.Fatalf("profile %s: label %q is outside the grammar", p.Name, c.Label)
				}
				labels = append(labels, c.Label)
			}
			if !slices.IsSorted(labels) || len(slices.Compact(slices.Clone(labels))) != len(labels) {
				t.Fatalf("profile %s: labels %v are not sorted and distinct", p.Name, labels)
			}
			d, ok := p.DefaultContract()
			if !ok || p.Default != d.Label || !slices.Equal(p.Devices, d.Devices) {
				t.Fatalf("profile %s: default %q is not a contract whose devices Devices are", p.Name, p.Default)
			}
			for _, c := range p.Contracts {
				if len(c.Accepts) > maxContractAccepts {
					t.Fatalf("profile %s: contract %s accepts %d labels", p.Name, c.Label, len(c.Accepts))
				}
				for _, a := range c.Accepts {
					if slices.Contains(labels, a) {
						t.Fatalf("profile %s: contract %s accepts its own profile's label %s", p.Name, c.Label, a)
					}
				}
			}
		}
	})
}

// TestAudit_ProbePathsAreDistinctAcrossNames: the host-view probe sees every
// name's devices in one container, so a path given for the probe alone is
// unique across every name, and never one a profile itself gives.
func TestAudit_ProbePathsAreDistinctAcrossNames(t *testing.T) {
	a := twoContracts("a", "/dev/x", "/dev/x")
	b := twoContracts("b", "/dev/y", "/dev/y")
	b.Contracts[0].Devices[0].Path, b.Contracts[1].Devices[0].Path = "/dev/ttyUSB2", "/dev/ttyUSB3"
	b.Devices = b.Contracts[0].Devices
	c := Profile{Name: "c", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES,
		Devices: []ProfileDevice{{Path: "/dev/ttyUSB4", ContainerPath: "/dev/ork-probe-1", Permissions: "r"}}}
	audits, _, err := auditProfiles(Profiles{List: []Profile{a, b, c}}, usbProcDevices, usbNodes)
	if err != nil {
		t.Fatalf("auditProfiles: %v", err)
	}
	seen := map[string]string{}
	for name, audit := range audits {
		for _, d := range audit.devices {
			if other, dup := seen[d.ContainerPath]; dup {
				t.Fatalf("the probe would be given %s for %s and %s", d.ContainerPath, other, name)
			}
			seen[d.ContainerPath] = name
		}
	}
	if len(seen) != 5 {
		t.Fatalf("probe paths = %v; want each of the 5 host devices once", seen)
	}
}

// TestAudit_ProbeBoundCountsWhatTheProbeIsAsked: a host device two names
// pass is asked about once for each, so the bound counts it for each.
func TestAudit_ProbeBoundCountsWhatTheProbeIsAsked(t *testing.T) {
	var list []Profile
	for i := 0; i < 5; i++ {
		p := Profile{Name: fmt.Sprintf("p%d", i), Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
		for j := 0; j < 16; j++ {
			p.Devices = append(p.Devices, ProfileDevice{Path: fmt.Sprintf("/dev/ttyUSB%d", j), ContainerPath: fmt.Sprintf("/dev/s%d-%d", i, j), Permissions: "r"})
		}
		list = append(list, p)
	}
	if _, _, err := auditProfiles(Profiles{List: list}, usbProcDevices, usbNodes); err == nil || !strings.Contains(err.Error(), "more than") {
		t.Fatalf("5 names of the same 16 devices (80 probe paths) = %v; want the bound refused", err)
	}
}

package dsl

import (
	"bytes"
	"maps"
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestParseInterfaceLabel_Devices: an image's interface may say which
// contract of a profile it needs, by profile name; a name and a label are
// each held to their grammar, and the map to the task's own bound.
func TestParseInterfaceLabel_Devices(t *testing.T) {
	iface, err := ParseInterfaceLabel(`{"schemaVersion":1,"devices":{"sensor":"2","cam":"v1.2:rc+1"}}`)
	if err != nil {
		t.Fatalf("a devices map was refused: %v", err)
	}
	if want := map[string]string{"sensor": "2", "cam": "v1.2:rc+1"}; !maps.Equal(iface.GetDevices(), want) {
		t.Fatalf("devices = %v, want %v", iface.GetDevices(), want)
	}
	many := make([]string, 0, MaxTaskDevices+1)
	for i := range MaxTaskDevices + 1 {
		many = append(many, `"d`+string(rune('a'+i))+`":"1"`)
	}
	for _, tc := range []struct{ name, label, want string }{
		{"a name not a slug", `{"schemaVersion":1,"devices":{"Sensor":"1"}}`, `devices: "Sensor" is not a profile name`},
		{"an empty label", `{"schemaVersion":1,"devices":{"sensor":""}}`, `devices.sensor: contract "" is not a contract label`},
		{"a label with a space", `{"schemaVersion":1,"devices":{"sensor":"a b"}}`, `devices.sensor: contract "a b" is not a contract label`},
		{"a label of 65 bytes", `{"schemaVersion":1,"devices":{"sensor":"` + strings.Repeat("1", 65) + `"}}`, `is not a contract label`},
		{"too many", `{"schemaVersion":1,"devices":{` + strings.Join(many, ",") + `}}`, "devices names 9 profiles, limit is 8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInterfaceLabel(tc.label)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseInterfaceLabel = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestCompile_DeviceOverrideSuffix: a devices entry written name@label pins
// that contract without reading the image: compile, which is pure, records it
// in device_contracts and keeps the bare name in devices.
func TestCompile_DeviceOverrideSuffix(t *testing.T) {
	wf, err := Compile([]byte(devicesDoc("sensor@2", "cam")))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	task := wf.GetTasks()[0]
	if got := task.GetDevices(); !slices.Equal(got, []string{"sensor", "cam"}) {
		t.Errorf("devices = %v, want the bare names", got)
	}
	if want := map[string]string{"sensor": "2"}; !maps.Equal(task.GetDeviceContracts(), want) {
		t.Errorf("device_contracts = %v, want %v", task.GetDeviceContracts(), want)
	}
	for _, tc := range []struct{ name, doc, want string }{
		{"an empty label", devicesDoc("sensor@"), `"sensor@"`},
		{"a label with a slash", devicesDoc("sensor@a/b"), `"sensor@a/b"`},
		{"no name", devicesDoc(`"@2"`), `"@2"`},
		{"one profile twice", devicesDoc("sensor@2", "sensor"), "twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), `task "a"`) {
				t.Fatalf("Compile = %v, want an error naming task a and %q", err, tc.want)
			}
		})
	}
}

// TestCompile_DeviceContractsInTheDigestOnlyWhenSet: a task's contracts are
// part of its definition, so of the digest; a task with none emits nothing,
// so every definition without them keeps its digest.
func TestCompile_DeviceContractsInTheDigestOnlyWhenSet(t *testing.T) {
	compile := func(doc string) *orkstackv1.Orkflow {
		t.Helper()
		wf, err := Compile([]byte(doc))
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return wf
	}
	digest := func(wf *orkstackv1.Orkflow) string {
		t.Helper()
		d, err := DefinitionDigest(wf)
		if err != nil {
			t.Fatalf("DefinitionDigest: %v", err)
		}
		return d
	}
	plain := compile(devicesDoc("sensor"))
	b, err := canonicalOrkflow(plain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "deviceContracts") {
		t.Errorf("a task with no contracts emits them: %s", b)
	}
	empty := compile(devicesDoc("sensor"))
	empty.GetTasks()[0].DeviceContracts = map[string]string{}
	if digest(empty) != digest(plain) {
		t.Error("an empty contract map changed the digest")
	}
	two := compile(devicesDoc("sensor@2"))
	three := compile(devicesDoc("sensor@3"))
	if digest(two) == digest(plain) || digest(two) == digest(three) {
		t.Error("a task's contract is not in its digest")
	}
}

// profiledTask is a task naming profiles, its image resolved as uses.
func profiledTask(uses string, devices []string, contracts map[string]string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: "t", Uses: uses, Devices: devices, DeviceContracts: contracts,
		Image: &orkstackv1.ImageRef{Pinned: testPinned}}
}

func declaring(devices map[string]string) *Resolution {
	return &Resolution{Interface: &orkstackv1.ActivityInterface{SchemaVersion: 1, Devices: devices}}
}

// TestCheckOrkflowInterfaces_DeviceContracts: the image's declaration is a
// requirement of the task: a profile it needs the task does not name is an
// error, a label the task overrides differently is an error, and a profile the
// task names but the image does not mention is fine (it is undeclared).
func TestCheckOrkflowInterfaces_DeviceContracts(t *testing.T) {
	for _, tc := range []struct {
		name string
		task *orkstackv1.Task
		res  *Resolution
		want []wantIssue
	}{
		{"declared and named", profiledTask("img", []string{"sensor"}, nil), declaring(map[string]string{"sensor": "2"}), nil},
		{"declared, overridden alike", profiledTask("img", []string{"sensor"}, map[string]string{"sensor": "2"}),
			declaring(map[string]string{"sensor": "2"}), nil},
		{"named, not declared", profiledTask("img", []string{"sensor"}, nil), declaring(nil), nil},
		{"declared, not named", profiledTask("img", nil, nil), declaring(map[string]string{"sensor": "2"}),
			[]wantIssue{{sevError, "tasks[0].devices", "image " + testPinned + " needs profile sensor (contract 2); add it to devices:"}}},
		{"overridden differently", profiledTask("img", []string{"sensor"}, map[string]string{"sensor": "3"}),
			declaring(map[string]string{"sensor": "2"}),
			[]wantIssue{{sevError, "tasks[0].devices", "devices names sensor@3, but image " + testPinned + " declares contract 2 of sensor; write sensor or sensor@2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{tc.task}}
			assertIssues(t, CheckOrkflowInterfaces(wf, Resolutions{"img": tc.res}), tc.want)
		})
	}
}

// TestCheckOrkflowInterfaces_UnknowableDeclarationIsAnErrorForProfiledTasks:
// for a task naming a profile with no @label, an image whose declaration
// cannot be known (its label unreadable, or no resolution made) is an error;
// with every profile overridden, or with no profile at all, it stays a
// warning or nothing.
func TestCheckOrkflowInterfaces_UnknowableDeclarationIsAnErrorForProfiledTasks(t *testing.T) {
	broken := &Resolution{InterfaceError: "schemaVersion 2 is not supported (this engine reads 1)"}
	unreadable := testPinned + " carries an org.orkstack.activity.interface label that could not be read " +
		"(schemaVersion 2 is not supported (this engine reads 1)); with: was not checked against it"
	for _, tc := range []struct {
		name string
		task *orkstackv1.Task
		res  Resolutions
		want []wantIssue
	}{
		{"unreadable, profiled", profiledTask("img", []string{"sensor"}, nil), Resolutions{"img": broken},
			[]wantIssue{{sevError, "tasks[0].devices", "the image's device contracts cannot be read (" +
				"schemaVersion 2 is not supported (this engine reads 1)); write the contract explicitly, as sensor@<contract>"}}},
		{"unreadable, overridden", profiledTask("img", []string{"sensor"}, map[string]string{"sensor": "1"}), Resolutions{"img": broken},
			[]wantIssue{
				{sevWarning, "tasks[0].devices", "the image's device contracts cannot be read (schemaVersion 2 is not supported (this engine reads 1)); sensor@1 is not checked against the image"},
				{sevWarning, "tasks[0].uses", unreadable},
			}},
		{"unreadable, no profile", profiledTask("img", nil, nil), Resolutions{"img": broken},
			[]wantIssue{{sevWarning, "tasks[0].uses", unreadable}}},
		{"no resolution, profiled", profiledTask("img", []string{"cam", "sensor"}, map[string]string{"cam": "1"}), nil,
			[]wantIssue{{sevError, "tasks[0].devices", "the image's device contracts cannot be read (no image resolution was made); " +
				"write the contract explicitly, as sensor@<contract>"}}},
		{"no resolution, every profile overridden", profiledTask("img", []string{"sensor"}, map[string]string{"sensor": "1"}), nil,
			[]wantIssue{{sevWarning, "tasks[0].devices", "the image's device contracts cannot be read (no image resolution was made); sensor@1 is not checked against the image"}}},
		{"no resolution, no profile", profiledTask("img", nil, nil), nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{tc.task}}
			assertIssues(t, CheckOrkflowInterfaces(wf, tc.res), tc.want)
		})
	}
}

// TestCheckOrkflowInterfaces_AllPlatformsMustAgreeOnDevices: for a profiled
// task, every platform of a multi-platform image must declare the same
// contract of each profile the task names, so the outcome never depends on
// which Orker takes the task; a platform whose label cannot be read leaves the
// declaration unknowable.
func TestCheckOrkflowInterfaces_AllPlatformsMustAgreeOnDevices(t *testing.T) {
	multi := func(others ...PlatformDevices) *Resolution {
		res := declaring(map[string]string{"sensor": "2"})
		res.Platform = "linux/amd64"
		res.OtherPlatforms = others
		return res
	}
	for _, tc := range []struct {
		name string
		task *orkstackv1.Task
		res  *Resolution
		want []wantIssue
	}{
		{"agree", profiledTask("img", []string{"sensor"}, nil),
			multi(PlatformDevices{Platform: "linux/arm64", Devices: map[string]string{"sensor": "2"}}), nil},
		{"differ", profiledTask("img", []string{"sensor"}, nil),
			multi(PlatformDevices{Platform: "linux/arm64", Devices: map[string]string{"sensor": "3"}}),
			[]wantIssue{{sevError, "tasks[0].devices", "image " + testPinned + " declares contract 2 of sensor on linux/amd64 and contract 3 on linux/arm64; " +
				"every platform must declare the same device contracts"}}},
		{"silent on one", profiledTask("img", []string{"sensor"}, nil),
			multi(PlatformDevices{Platform: "linux/arm64"}),
			[]wantIssue{{sevError, "tasks[0].devices", "image " + testPinned + " declares contract 2 of sensor on linux/amd64 and no contract on linux/arm64; " +
				"every platform must declare the same device contracts"}}},
		{"another platform needs a profile the first does not", profiledTask("img", []string{"sensor"}, nil),
			multi(PlatformDevices{Platform: "linux/arm64", Devices: map[string]string{"sensor": "2", "cam": "9"}}),
			[]wantIssue{{sevError, "tasks[0].devices", "image " + testPinned + " declares no contract of cam on linux/amd64 and contract 9 on linux/arm64; " +
				"every platform must declare the same device contracts"}}},
		{"unreadable on one, pinned", profiledTask("img", []string{"sensor"}, map[string]string{"sensor": "2"}),
			multi(PlatformDevices{Platform: "linux/arm64", Err: "a"}, PlatformDevices{Platform: "linux/s390x", Err: "b"}),
			[]wantIssue{{sevWarning, "tasks[0].devices", "the image's device contracts cannot be read " +
				"(linux/arm64: a; linux/s390x: b); sensor@2 is not checked against the image"}}},
		{"unreadable on one", profiledTask("img", []string{"sensor"}, nil),
			multi(PlatformDevices{Platform: "linux/arm64", Err: "the config blob could not be fetched"}),
			[]wantIssue{{sevError, "tasks[0].devices", "the image's device contracts cannot be read " +
				"(linux/arm64: the config blob could not be fetched); write the contract explicitly, as sensor@<contract>"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{tc.task}}
			assertIssues(t, CheckOrkflowInterfaces(wf, Resolutions{"img": tc.res}), tc.want)
		})
	}
}

// TestApplyDeviceContracts_CopiesTheImagesDeclaration: create copies the
// image's declaration into the task for each profile it names, beside the
// author's overrides, and writes nothing for a task with none.
func TestApplyDeviceContracts_CopiesTheImagesDeclaration(t *testing.T) {
	declared := profiledTask("img", []string{"sensor", "cam", "gpu"}, map[string]string{"cam": "7"})
	plain := profiledTask("plain", []string{"sensor"}, nil)
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{declared, plain}}
	ApplyDeviceContracts(wf, Resolutions{
		"img":   declaring(map[string]string{"sensor": "2", "cam": "7", "other": "1"}),
		"plain": declaring(nil),
	})
	if want := map[string]string{"sensor": "2", "cam": "7"}; !maps.Equal(declared.GetDeviceContracts(), want) {
		t.Errorf("device_contracts = %v, want %v", declared.GetDeviceContracts(), want)
	}
	if plain.GetDeviceContracts() != nil {
		t.Errorf("a task its image declares nothing for got %v", plain.GetDeviceContracts())
	}
}

// TestParseActivityDocument_Devices: an authored interface's devices: reaches
// the label as written, so `orkctl activity interface pack` carries it; a
// label written as a bare number is the same label, and one the grammar
// refuses is refused with the label's own message.
func TestParseActivityDocument_Devices(t *testing.T) {
	const header = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	for _, doc := range []string{header + "devices:\n  sensor: \"2\"\n", header + "devices:\n  sensor: 2\n"} {
		iface, err := ParseActivityDocument([]byte(doc))
		if err != nil {
			t.Fatalf("ParseActivityDocument(%q): %v", doc, err)
		}
		if want := map[string]string{"sensor": "2"}; !maps.Equal(iface.Interface.GetDevices(), want) {
			t.Fatalf("devices = %v, want %v", iface.Interface.GetDevices(), want)
		}
		label, err := PackInterfaceLabel(iface.Interface)
		if err != nil {
			t.Fatal(err)
		}
		back, err := ParseInterfaceLabel(label)
		if err != nil || !maps.Equal(back.GetDevices(), iface.Interface.GetDevices()) {
			t.Fatalf("the packed label %s reads back as %v, %v", label, back.GetDevices(), err)
		}
	}
	if _, err := ParseActivityDocument([]byte(header + "devices:\n  sensor: \"a b\"\n")); err == nil ||
		!strings.Contains(err.Error(), `devices.sensor: contract "a b" is not a contract label`) {
		t.Fatalf("a bad label = %v", err)
	}
	iface, err := ParseActivityDocument([]byte(header + "fields:\n  a:\n    type: string\n"))
	if err != nil || iface.Interface.GetDevices() != nil {
		t.Fatalf("a document with no devices gave %v, %v", iface.Interface.GetDevices(), err)
	}
}

// twoPlatformStub serves an index over two images, amd64 then arm64, each
// with its own labels.
func twoPlatformStub(t *testing.T, amd, arm map[string]string) *registryStub {
	t.Helper()
	first, second := newTestImage(t, amd), newTestImage(t, arm)
	body, err := stablejson.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIIndex,
		"manifests": []any{
			map[string]any{"mediaType": MediaTypeOCIManifest, "digest": digestOf(first.manifest),
				"platform": map[string]any{"os": "linux", "architecture": "amd64"}},
			map[string]any{"mediaType": MediaTypeOCIManifest, "digest": digestOf(first.attestation),
				"platform": map[string]any{"os": "unknown", "architecture": "unknown"}},
			map[string]any{"mediaType": MediaTypeOCIManifest, "digest": digestOf(second.manifest),
				"platform": map[string]any{"os": "linux", "architecture": "arm64", "variant": "v8"}},
		},
	})
	must.OKf(t, err, "index")
	objects := first.objects()
	maps.Copy(objects, second.objects())
	return &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body), objects: objects}
}

// TestResolveOrkflow_ReadsEveryPlatformsDevicesForAProfiledTask: an image a
// task naming a profile uses has every runnable platform's declaration read,
// one config each, so create can hold them to one; an image no profiled task
// uses is read as before, its first platform only.
func TestResolveOrkflow_ReadsEveryPlatformsDevicesForAProfiledTask(t *testing.T) {
	label := func(contract string) map[string]string {
		return map[string]string{InterfaceLabel: `{"schemaVersion":1,"devices":{"sensor":"` + contract + `"}}`}
	}
	stub := twoPlatformStub(t, label("2"), label("3"))
	_, reference := newStubResolver(t, stub)

	profiled := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a", Uses: "img"}, {Id: "b", Uses: "img", Devices: []string{"sensor"}}}}
	resolutions, err := NewResolver("").ResolveOrkflow(t.Context(), Catalog{"img": reference}, profiled)
	must.OKf(t, err, "resolve")
	res := resolutions["img"]
	if res.Platform != "linux/amd64" || res.Interface.GetDevices()["sensor"] != "2" {
		t.Fatalf("first platform = %q, devices %v", res.Platform, res.Interface.GetDevices())
	}
	if len(res.OtherPlatforms) != 1 || res.OtherPlatforms[0].Platform != "linux/arm64/v8" ||
		res.OtherPlatforms[0].Devices["sensor"] != "3" || res.OtherPlatforms[0].Err != "" {
		t.Fatalf("other platforms = %+v, want linux/arm64/v8 declaring 3", res.OtherPlatforms)
	}
	pinned := profiled.GetTasks()[1].GetImage().GetPinned()
	assertIssues(t, CheckOrkflowInterfaces(profiled, resolutions), []wantIssue{
		{sevError, "tasks[0].devices", "image " + pinned + " needs profile sensor (contract 2); add it to devices:"},
		{sevError, "tasks[1].devices", "image " + pinned + " declares contract 2 of sensor on linux/amd64 and contract 3 on linux/arm64/v8; " +
			"every platform must declare the same device contracts"},
	})

	stub.paths = nil
	plain := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a", Uses: "img"}}}
	resolutions, err = NewResolver("").ResolveOrkflow(t.Context(), Catalog{"img": reference}, plain)
	must.OKf(t, err, "resolve")
	if got := resolutions["img"].OtherPlatforms; got != nil {
		t.Fatalf("an image no profiled task uses read other platforms: %+v", got)
	}
	if len(stub.paths) != 3 {
		t.Fatalf("GETs = %v, want the index, one platform manifest and one config", stub.paths)
	}
}

// TestResolveOrkflow_AnUnreadableOtherPlatformIsNamed: a platform whose label
// cannot be parsed is named with why, never read as declaring nothing.
func TestResolveOrkflow_AnUnreadableOtherPlatformIsNamed(t *testing.T) {
	stub := twoPlatformStub(t,
		map[string]string{InterfaceLabel: `{"schemaVersion":1,"devices":{"sensor":"2"}}`},
		map[string]string{InterfaceLabel: `{"schemaVersion":2}`})
	_, reference := newStubResolver(t, stub)
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a", Uses: "img", Devices: []string{"sensor"}}}}
	resolutions, err := NewResolver("").ResolveOrkflow(t.Context(), Catalog{"img": reference}, wf)
	must.OKf(t, err, "resolve")
	others := resolutions["img"].OtherPlatforms
	if len(others) != 1 || others[0].Err != "schemaVersion 2 is not supported (this engine reads 1)" {
		t.Fatalf("other platforms = %+v, want arm64 named with its parse error", others)
	}
}

// TestRunnablePlatforms_NamesOnly: an entry whose os, architecture or variant is not a
// name is no runnable platform, and the first runnable one is the one whose config is
// read.
func TestRunnablePlatforms_NamesOnly(t *testing.T) {
	a, b := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	entry := func(digest, platform string) string {
		return `{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"` + digest + `","platform":` + platform + `}`
	}
	for name, tc := range map[string]struct {
		first string
		want  []string
	}{
		"an os that is a separator": {`{"os":"/"}`, []string{"linux/amd64"}},
		"an os with a separator":    {`{"os":"linux/x","architecture":"amd64"}`, []string{"linux/amd64"}},
		"an architecture with one":  {`{"os":"linux","architecture":"/"}`, []string{"linux/amd64"}},
		"a variant with one":        {`{"os":"linux","architecture":"arm64","variant":"v8/"}`, []string{"linux/amd64"}},
		"a space":                   {`{"os":"linux","architecture":"amd 64"}`, []string{"linux/amd64"}},
		"a variant of nothing":      {`{"os":"linux","variant":"v8"}`, []string{"linux/amd64"}},
		"an attestation":            {`{"os":"unknown","architecture":"unknown"}`, []string{"linux/amd64"}},
		"one that is":               {`{"os":"linux","architecture":"arm64","variant":"v8"}`, []string{"linux/arm64/v8", "linux/amd64"}},
		"one with no architecture":  {`{"os":"windows"}`, []string{"windows/", "linux/amd64"}},
		"names of dots and dashes":  {`{"os":"linux","architecture":"x86_64-v2.1"}`, []string{"linux/x86_64-v2.1", "linux/amd64"}},
	} {
		index := []byte(`{"manifests":[` + entry(a, tc.first) + `,` + entry(b, `{"os":"linux","architecture":"amd64"}`) + `]}`)
		var got []string
		for _, e := range runnablePlatforms(index) {
			got = append(got, e.platform)
		}
		want := b
		if len(tc.want) == 2 {
			want = a
		}
		if digest, ok := selectPlatformManifest(index); !slices.Equal(got, tc.want) || !ok || digest != want {
			t.Errorf("%s: %q, and the config of %s, %v", name, got, digest, ok)
		}
	}
}

// FuzzRunnablePlatforms: an index a registry serves is untrusted; the list of
// its runnable platforms never panics, never holds an attestation entry, one
// with no os or one whose parts are not names, never holds more entries than
// the index lists, and begins with the entry whose config is read.
func FuzzRunnablePlatforms(f *testing.F) {
	for _, seed := range []string{
		`{"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:` + strings.Repeat("a", 64) +
			`","platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"unknown","architecture":"unknown"}},` +
			`{"digest":"x","platform":{"os":"linux","architecture":"arm64","variant":"v8"}}]}`,
		`{"manifests":[{"mediaType":"application/vnd.oci.image.index.v1+json","platform":{"os":"linux"}}]}`,
		`{"manifests":[{}]}`, `{"manifests":null}`, `{}`, ``, `[]`, `{"manifests":[{"platform":{"os":"\u001b[31m"}}]}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, index []byte) {
		entries := runnablePlatforms(index)
		if len(entries) > bytes.Count(index, []byte("{")) {
			t.Fatalf("runnablePlatforms(%q) listed %d platforms", index, len(entries))
		}
		// The config read is the first runnable platform's, where it is one to read.
		if digest, ok := selectPlatformManifest(index); ok && (len(entries) == 0 || entries[0].digest != digest) {
			t.Fatalf("selectPlatformManifest(%q) read %s, the first runnable platform is %+v", index, digest, entries)
		}
		for _, entry := range entries {
			if entry.platform == "" || strings.HasPrefix(entry.platform, "/") || strings.HasPrefix(entry.platform, "unknown/") ||
				strings.Count(entry.platform, "/") > 2 || strings.Contains(entry.platform, "//") {
				t.Fatalf("runnablePlatforms(%q) listed the platform %q", index, entry.platform)
			}
			if entry.platform != SafeReason(entry.platform) {
				t.Fatalf("runnablePlatforms(%q) listed the unsafe platform %q", index, entry.platform)
			}
		}
	})
}

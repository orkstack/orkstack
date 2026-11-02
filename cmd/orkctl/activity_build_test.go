package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestActivityBuild_EachLabelIsOneArgument: --interface reaches the
// toolchain as one --label key=value argument per activity label, an
// undeclared icon or docs with the empty value that clears a base image's,
// before the context, with no shell between them, so the labels' quotes and
// braces arrive as they are.
func TestActivityBuild_EachLabelIsOneArgument(t *testing.T) {
	iface := `{"schemaVersion":1,"fields":{"greeting":{"type":"string","default":"a b {c}"}}}`
	icon := `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 7h8" stroke="currentColor"/></svg>`
	labels := []packedLabel{
		{"interface", "org.orkstack.activity.interface", iface},
		{"icon", "org.orkstack.activity.icon", icon},
		{"docs", "org.orkstack.activity.docs", ""},
	}
	declared := []string{"--label", "org.orkstack.activity.interface=" + iface, "--label", "org.orkstack.activity.icon=" + icon,
		"--label", "org.orkstack.activity.docs="}
	for _, tc := range []struct {
		name           string
		platform, file string
		labels         []packedLabel
		want           []string
	}{
		{"no interface", "", "", nil, []string{"build", "--tag", "reg/x:1", "ctx"}},
		{"an interface", "", "", labels, append(append([]string{"build", "--tag", "reg/x:1"}, declared...), "ctx")},
		{"every flag", "linux/amd64,linux/arm64", "Dockerfile.alt", labels,
			append(append([]string{"build", "--tag", "reg/x:1", "--platform", "linux/amd64,linux/arm64", "--file", "Dockerfile.alt"},
				declared...), "ctx")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := activityBuildArgs("reg/x:1", tc.platform, tc.file, tc.labels, "ctx")
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("args\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// TestActivityBuild_RefusesABadInterfaceBeforeBuilding: the document is
// packed before the toolchain runs, so a document that cannot be packed
// builds nothing. PATH holds no docker: had the build started, the error
// would name docker instead.
func TestActivityBuild_RefusesABadInterfaceBeforeBuilding(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	bad := writeActivityInterface(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\n")
	_, err := executeAs(t, "linux", "activity", "build", t.TempDir(), "--tag", "localhost:5001/x:1", "--interface", bad)
	if err == nil || !strings.Contains(err.Error(), bad+": ") || !strings.Contains(err.Error(), "not an activity interface") {
		t.Fatalf("build with a bad --interface: %v", err)
	}
	if strings.Contains(err.Error(), "docker") {
		t.Fatalf("the toolchain ran before the interface was packed: %v", err)
	}
}

// TestActivityBuild_AFailedBuildNamesTheLabelsBySize: a toolchain error
// quotes the build's argv, and a packed label — up to 64 KiB of JSON — is
// named by its key and size there, never echoed.
func TestActivityBuild_AFailedBuildNamesTheLabelsBySize(t *testing.T) {
	labels := []packedLabel{
		{"interface", "org.orkstack.activity.interface", `{"schemaVersion":1,"fields":{"secret-looking":{"type":"string"}}}`},
		{"icon", "org.orkstack.activity.icon", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 7h8"/></svg>`},
		{"docs", "org.orkstack.activity.docs", `{"schemaVersion":1,"description":"a long text"}`},
	}
	got := describeDockerArgs(activityBuildArgs("reg/x:1", "", "", labels, "ctx"))
	for _, label := range labels {
		if strings.Contains(got, label.value) {
			t.Errorf("the described argv echoes the %s label's value: %s", label.short, got)
		}
		if !strings.Contains(got, fmt.Sprintf("%s=<packed %s, %d bytes>", label.key, label.short, len(label.value))) {
			t.Errorf("the described argv does not name the %s label by its size: %s", label.short, got)
		}
	}
	if !strings.HasPrefix(got, "build --tag reg/x:1 ") || !strings.HasSuffix(got, " ctx") {
		t.Errorf("the described argv lost the build's own arguments: %s", got)
	}
}

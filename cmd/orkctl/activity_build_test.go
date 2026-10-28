package main

import (
	"reflect"
	"strings"
	"testing"
)

// TestActivityBuild_TheInterfaceIsOneLabelArgument: --interface reaches the
// toolchain as exactly one --label key=value argument, before the context,
// with no shell between them, so the label's quotes and braces arrive as
// they are.
func TestActivityBuild_TheInterfaceIsOneLabelArgument(t *testing.T) {
	label := `{"schemaVersion":1,"fields":{"greeting":{"type":"string","default":"a b {c}"}}}`
	for _, tc := range []struct {
		name                   string
		platform, file, packed string
		want                   []string
	}{
		{"no interface", "", "", "", []string{"build", "--tag", "reg/x:1", "ctx"}},
		{"an interface", "", "", label,
			[]string{"build", "--tag", "reg/x:1", "--label", "org.orkstack.activity.interface=" + label, "ctx"}},
		{"every flag", "linux/amd64,linux/arm64", "Dockerfile.alt", label,
			[]string{"build", "--tag", "reg/x:1", "--platform", "linux/amd64,linux/arm64", "--file", "Dockerfile.alt",
				"--label", "org.orkstack.activity.interface=" + label, "ctx"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := activityBuildArgs("reg/x:1", tc.platform, tc.file, tc.packed, "ctx")
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

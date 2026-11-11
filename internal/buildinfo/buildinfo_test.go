package buildinfo

import (
	"bytes"
	"regexp"
	"runtime/debug"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	vcs := &debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs.modified", Value: "true"},
		{Key: "vcs.revision", Value: "from-vcs"},
	}}
	emptyVCS := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: ""}}}
	noVCS := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "-buildvcs", Value: "false"}}}
	for _, c := range []struct {
		name    string
		stamped string
		build   *debug.BuildInfo
		want    string
	}{
		{"a stamp wins over version control", "stamped", vcs, "stamped"},
		{"a stamp needs no version control", "stamped", noVCS, "stamped"},
		{"a stamp needs no build information", "stamped", nil, "stamped"},
		{"a development build reads version control", "", vcs, "from-vcs"},
		{"an empty revision says nothing", "", emptyVCS, Unknown},
		{"nothing stamped, no version control", "", noVCS, Unknown},
		{"nothing at all", "", nil, Unknown},
	} {
		if got := resolve(c.stamped, c.build); got != c.want {
			t.Errorf("%s: resolve = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestWrite_NamesTheBinaryAndItsCommit(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	must.OK(t, Write(&out, "ork", "v1.2.3", "abc123"))
	for _, line := range []string{`(?m)^ork:\s+v1\.2\.3$`, `(?m)^commit:\s+abc123$`, `(?m)^go:\s+go`, `(?m)^platform:\s+\S+/\S+$`} {
		if !regexp.MustCompile(line).MatchString(out.String()) {
			t.Errorf("Write printed\n%s\nwith no line matching %s", out.String(), line)
		}
	}
}

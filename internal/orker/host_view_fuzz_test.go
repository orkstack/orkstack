package orker

import (
	"bytes"
	"path"
	"slices"
	"testing"
)

// FuzzParseProbeView holds the probe's report, a container's output, to
// what parseProbeView claims: it never panics, never accepts more than its
// bound, and whatever it accepts is absolute clean paths each with exactly a
// known kind or an error, and writes back to a report it reads the same.
func FuzzParseProbeView(f *testing.F) {
	for _, seed := range []string{
		`{"view":[]}`,
		`{"view":[{"path":"/dev/ttyUSB199","kind":"char","major":188,"minor":199,"gid":1000}]}`,
		`{"view":[{"path":"/dev/gone","error":"no such file"}]}`,
		`{"view":[{"path":"/etc/cdi/x.yaml","kind":"file","dev":2049,"ino":77}]}`,
		`{"view":[{"path":"dev/x","kind":"char"}]}`,
		`{"view":[{"path":"/dev/x","kind":"socket"}]}`,
		`{"view":[{"path":"/dev/x","kind":"char","extra":1}]}`,
		`{"view":[]} {}`,
		``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, err := parseProbeView(raw)
		if err != nil {
			return
		}
		if len(raw) > maxProbeViewBytes {
			t.Fatalf("accepted %d bytes", len(raw))
		}
		for _, v := range got {
			if !path.IsAbs(v.Path) || path.Clean(v.Path) != v.Path {
				t.Fatalf("accepted path %q", v.Path)
			}
			known := v.Kind == viewChar || v.Kind == viewDir || v.Kind == viewFile
			if (v.Error == "") != known {
				t.Fatalf("accepted entry %+v", v)
			}
		}
		var buf bytes.Buffer
		if err := writeProbeView(&buf, got); err != nil {
			t.Fatalf("write: %v", err)
		}
		again, err := parseProbeView(buf.Bytes())
		if err != nil || !slices.Equal(again, got) {
			t.Fatalf("round trip = %+v, %v; want %+v", again, err, got)
		}
	})
}

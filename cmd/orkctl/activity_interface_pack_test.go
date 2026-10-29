package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const packDocument = `# The with: contract of a small image.
apiVersion: orkstack/v1
kind: ActivityInterface
fields:
  url:
    type: string
    required: true
ops:
  get: {}
defaultOp: get
`

const packLabel = `{"schemaVersion":1,"fields":{"url":{"type":"string","required":true}},"ops":{"get":{}},"defaultOp":"get"}`

func writeActivityInterface(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "interface.ork.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestActivityInterfacePack_PrintsTheLabel: pack prints the label value and
// nothing else, so $(orkctl activity interface pack FILE) is the value a
// --label takes.
func TestActivityInterfacePack_PrintsTheLabel(t *testing.T) {
	out, err := executeAs(t, "linux", "activity", "interface", "pack", writeActivityInterface(t, packDocument))
	if err != nil {
		t.Fatalf("pack: %v\n%s", err, out)
	}
	if out != packLabel+"\n" {
		t.Fatalf("pack printed\n%q\nwant\n%q", out, packLabel+"\n")
	}
}

// TestActivityInterfacePack_NamesTheFileItRefuses: a document pack cannot
// read is refused with its path and the parser's reason, and prints nothing
// a shell substitution could mistake for a label.
func TestActivityInterfacePack_NamesTheFileItRefuses(t *testing.T) {
	for _, tc := range []struct{ name, content, want string }{
		{"an Orkflow", "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\n", "kind: Orkflow is an Orkflow, not an activity interface"},
		{"a bad type", "apiVersion: orkstack/v1\nkind: ActivityInterface\nfields:\n  a:\n    type: text\n", `fields.a: type "text" is not one of`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeActivityInterface(t, tc.content)
			out, err := executeAs(t, "linux", "activity", "interface", "pack", path)
			if err == nil || !strings.HasPrefix(err.Error(), path+": ") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("pack = %q, %v; want an error naming %s and %q", out, err, path, tc.want)
			}
			if strings.Contains(out, "{") {
				t.Fatalf("a refused pack printed a value: %q", out)
			}
		})
	}
}

func TestActivityInterfacePack_AMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.ork.yaml")
	_, err := executeAs(t, "linux", "activity", "interface", "pack", path)
	if err == nil || !strings.Contains(err.Error(), "read "+path) {
		t.Fatalf("pack of a missing file: %v", err)
	}
}

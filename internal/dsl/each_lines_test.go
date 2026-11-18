package dsl

import (
	"strings"
	"testing"
	"unicode/utf8"
)

var overArtifactCases = []struct {
	name, in, producer, artifact, refusal string
}{
	{name: "task and name", in: "list/files.jsonl", producer: "list", artifact: "files.jsonl"},
	{name: "first slash splits", in: "list/a/b", refusal: "holds a \"/\""},
	{name: "dashed task", in: "list-files/out.txt", producer: "list-files", artifact: "out.txt"},
	{name: "dotfile name", in: "list/.hidden", producer: "list", artifact: ".hidden"},
	{name: "utf8 name", in: "list/données.jsonl", producer: "list", artifact: "données.jsonl"},
	{name: "empty", in: "", refusal: "it is empty"},
	{name: "no slash", in: "list", refusal: "no \"/\""},
	{name: "no task", in: "/files.jsonl", refusal: "names no task"},
	{name: "no name", in: "list/", refusal: "names no artifact"},
	{name: "inputs refused", in: "_inputs/rows.jsonl", refusal: "run input artifact is not supported"},
	{name: "reserved prefix", in: "_other/rows.jsonl", refusal: "reserved"},
	{name: "uppercase task", in: "List/rows.jsonl", refusal: "not a task id"},
	{name: "task too long", in: strings.Repeat("a", MaxSlug+1) + "/x", refusal: "not a task id"},
	{name: "dot name", in: "list/.", refusal: "not a file name"},
	{name: "dotdot name", in: "list/..", refusal: "not a file name"},
	{name: "nul name", in: "list/a\x00b", refusal: "NUL"},
	{name: "invalid utf8", in: "list/\xff", refusal: "not valid UTF-8"},
}

func TestParseOverArtifact(t *testing.T) {
	for _, tc := range overArtifactCases {
		t.Run(tc.name, func(t *testing.T) {
			producer, name, err := ParseOverArtifact(tc.in)
			if tc.refusal != "" {
				if err == nil {
					t.Fatalf("ParseOverArtifact(%q) = %q, %q; want a refusal containing %q", tc.in, producer, name, tc.refusal)
				}
				if !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("ParseOverArtifact(%q) refusal %q; want it to contain %q", tc.in, err, tc.refusal)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOverArtifact(%q): %v", tc.in, err)
			}
			if producer != tc.producer || name != tc.artifact {
				t.Fatalf("ParseOverArtifact(%q) = %q, %q; want %q, %q", tc.in, producer, name, tc.producer, tc.artifact)
			}
		})
	}
}

// FuzzParseOverArtifact states the parser's properties: it never panics; an
// accepted value is exactly producer + "/" + name; the producer is a task id
// and the name a single valid file name; a refusal names the value.
func FuzzParseOverArtifact(f *testing.F) {
	for _, tc := range overArtifactCases {
		f.Add(tc.in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		producer, name, err := ParseOverArtifact(in)
		if err != nil {
			if producer != "" || name != "" {
				t.Fatalf("refused %q but returned %q, %q", in, producer, name)
			}
			return
		}
		if producer+"/"+name != in {
			t.Fatalf("accepted %q as %q + %q, which do not rebuild it", in, producer, name)
		}
		if !slugPattern.MatchString(producer) || len(producer) > MaxSlug {
			t.Fatalf("accepted producer %q, which is not a task id", producer)
		}
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || !utf8.ValidString(name) {
			t.Fatalf("accepted name %q, which is not one file name", name)
		}
	})
}

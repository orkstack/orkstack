package evidence

import (
	"strings"
	"testing"
)

// E1
func TestMemberName_CleanNamesAreVerbatimAndOthersAreEncoded(t *testing.T) {
	clean201 := strings.Repeat("a", 201)
	cases := []struct {
		name string
		want string
	}{
		{"report.txt", "report.txt"},
		{"rates.parquet", "rates.parquet"},
		{"A-b_c.1", "A-b_c.1"},
		{strings.Repeat("x", 200), strings.Repeat("x", 200)},
		{"a/b.txt", "~YS9iLnR4dA"},
		{"..", "~Li4"},
		{".", "~Lg"},
		{"sub/dir.txt", "~c3ViL2Rpci50eHQ"},
		{"~tilde", "~fnRpbGRl"},
		{"with space", "~d2l0aCBzcGFjZQ"},
	}
	for _, tc := range cases {
		if got := MemberName(tc.name); got != tc.want {
			t.Errorf("MemberName(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}

	long := MemberName(clean201)
	if !strings.HasPrefix(long, "~") {
		t.Fatalf("a 201-byte name must be encoded, got %q", long)
	}
	chunks := strings.Split(strings.TrimPrefix(long, "~"), "~")
	if len(chunks) != 2 || len(chunks[0]) != 200 {
		t.Fatalf("a 201-byte name encodes to %d chunks (first %d chars), want 2 with the first 200", len(chunks), len(chunks[0]))
	}
	for _, name := range []string{"~x", "~", "~report.txt"} {
		if got := MemberName(name); got == name {
			t.Errorf("a name starting with ~ must never be verbatim: %q", name)
		}
	}

	// Two distinct names never map to one path.
	corpus := []string{
		"report.txt", "a/b.txt", "a_b.txt", "..", ".", "...", "~", "~~", "a b", "a  b",
		"ünïcødé", "日本語.txt", "../etc/passwd", "/abs", "a/", "/a", "x", "X", "x.", ".x",
		"-", "_", "commit.json", "values.json", "stdout.log", "stderr.log", "a\x00b", "a\nb",
		"~YS9iLnR4dA", "YS9iLnR4dA", "~Li4", "Li4", strings.Repeat("z", 200), strings.Repeat("z", 201),
		strings.Repeat("z", 202), strings.Repeat("é", 150), "tab\tname", "q?x", "a#b", "%2F", "a%2Fb",
		"CON", "nul", "a..b", "a/../b", "./a", "a/.", "rates.parquet", "Rates.parquet", "rates.Parquet",
	}
	if len(corpus) != 50 {
		t.Fatalf("corpus has %d names, want 50", len(corpus))
	}
	paths := map[string]string{}
	for _, name := range corpus {
		got := MemberName(name)
		if prior, dup := paths[got]; dup {
			t.Errorf("%q and %q both map to %q", prior, name, got)
		}
		paths[got] = name
		if strings.ContainsAny(got, "/\x00") || got == "." || got == ".." {
			t.Errorf("MemberName(%q) = %q is not one safe segment", name, got)
		}
	}
}

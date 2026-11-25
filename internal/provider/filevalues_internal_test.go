package provider

import (
	"strconv"
	"strings"
	"testing"
)

func TestFileValues(t *testing.T) {
	for name, tc := range map[string]struct {
		content string
		want    []string
	}{
		"an AWS process document": {`{"Version":1,"AccessKeyId":"AKIA","SecretAccessKey":"a/b\/c","SessionToken":"t"}` + "\n", []string{"AKIA", "a/b/c", "t"}},
		"nested":                  {`{"a":{"b":["x",{"c":"y"}],"d":true},"e":null}`, []string{"x", "y"}},
		"an array":                {`["x","y"]`, []string{"x", "y"}},
		"a key written twice":     {`{"a":"x","a":"y"}`, []string{"x", "y"}},
		"a bare string is whole":  {`"x"`, nil},
		"two documents":           {`{"a":"x"}{"a":"y"}`, nil},
		"cut short":               {`{"a":"x"`, nil},
		"an INI file":             {"[default]\n# a comment\naws_secret_access_key = k=ey\n\naws_session_token=t\n", []string{"k=ey", "t"}},
		"quotes and comments":     {"[default]\na = \"q u\"\nb = v # c\n", []string{`"q u"`, "q u", "v # c", "v"}},
		"an empty value":          {`{"a":"","b":"x","c":"x"}`, []string{"x"}},
		"a .env file":             {"# c\nA=b\nC=\"d e\"\n", []string{"b", `"d e"`, "d e"}},
		"key=value and prose":     {"a=b\nhello there\n", nil},
		"a byte order mark":       {"\xef\xbb\xbf{\"a\":\"x\"}", []string{"x"}},
		"a section and a comment": {"[default] # c\na = b\n  continued\n", []string{"b"}},
		"a PEM file":              {"-----BEGIN KEY-----\nabc=\n-----END KEY-----\n", nil},
		"a section and prose":     {"[default]\nhello\n", nil},
		"empty":                   {"", nil},
	} {
		t.Run(name, func(t *testing.T) {
			var got []string
			values, whole := fileValues([]byte(tc.content))
			if !whole {
				t.Error("a small file is said to hold more than the bound")
			}
			for _, v := range values {
				got = append(got, string(v))
			}
			if strings.Join(got, "|") != strings.Join(tc.want, "|") || len(got) != len(tc.want) {
				t.Fatalf("fileValues(%q) = %q, want %q", tc.content, got, tc.want)
			}
		})
	}
}

// FuzzFileValues: whatever a provider put in a file, a value is never longer
// than the file, and reading it ends.
func FuzzFileValues(f *testing.F) {
	for _, s := range []string{`{"a":"b"}`, "[x]\na=b\n", "", "[", `["\ud800"]`, "{\"a\":\"\xff\"}", strings.Repeat("[", 20000)} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, content []byte) {
		values, _ := fileValues(content)
		if len(values) > maxFileValues {
			t.Fatalf("%d values", len(values))
		}
		for _, v := range values {
			if len(v) == 0 {
				t.Fatal("an empty value")
			}
			// A U+FFFD written for one byte of invalid UTF-8 takes three.
			if len(v) > 3*len(content) {
				t.Fatalf("a value of %d bytes from a file of %d", len(v), len(content))
			}
		}
	})
}

// A file of more values than the bound keeps the first of them and says so.
func TestFileValues_AreBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString("[")
	for i := range maxFileValues + 1 {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"value-` + strconv.Itoa(i) + `"`)
	}
	b.WriteString("]")
	values, whole := fileValues([]byte(b.String()))
	if len(values) != maxFileValues || whole || string(values[0]) != "value-0" {
		t.Fatalf("%d values, whole %v", len(values), whole)
	}
}

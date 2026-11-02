package dsl

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	testIconLabel = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 7h8" stroke="currentColor"/></svg>`
	// testDocsFor describes testLabel's one op and key.
	testDocsFor = `{"schemaVersion":1,"description":"Copies.","ops":{"copy":{"description":"Copies one.","fields":{"src":"The file."}}}}`
)

func inspectLabels(t *testing.T, labels map[string]string) *Resolution {
	t.Helper()
	resolver, reference := newStubResolver(t, singleManifestStub(t, newTestImage(t, labels)))
	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v — no label may fail resolution", err)
	}
	return res
}

func TestInspect_ReadsTheIconAndDocsFromTheSameConfig(t *testing.T) {
	stub := singleManifestStub(t, newTestImage(t, map[string]string{InterfaceLabel: testLabel, IconLabel: testIconLabel, DocsLabel: testDocsFor}))
	resolver, reference := newStubResolver(t, stub)
	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatal(err)
	}
	blobs := 0
	for _, path := range stub.paths {
		if strings.Contains(path, "/blobs/") {
			blobs++
		}
	}
	if blobs != 1 {
		t.Fatalf("read %d config blobs for three labels, want one: %v", blobs, stub.paths)
	}
	if res.InterfaceError != "" || res.IconError != "" || res.DocsError != "" {
		t.Fatalf("errors = %q / %q / %q", res.InterfaceError, res.IconError, res.DocsError)
	}
	if got := res.Icon.GetElements(); len(got) != 1 || got[0].GetName() != "path" {
		t.Fatalf("icon = %v", res.Icon)
	}
	if got := res.Docs.GetOps()["copy"].GetFields()["src"]; got != "The file." {
		t.Fatalf("docs = %v", res.Docs)
	}
}

// TestInspect_TheInterfaceIsReadAsBeforeWhateverTheOtherLabelsHold: the
// interface label is read exactly as an engine that predates the icon and
// docs labels reads it, whatever those two hold.
func TestInspect_TheInterfaceIsReadAsBeforeWhateverTheOtherLabelsHold(t *testing.T) {
	alone := inspectLabels(t, map[string]string{InterfaceLabel: testLabel})
	for _, others := range []map[string]string{
		{IconLabel: testIconLabel, DocsLabel: testDocsFor},
		{IconLabel: `<svg onload="x"/>`, DocsLabel: `{"schemaVersion":9}`},
		{IconLabel: `{"schemaVersion":1,"paths":[]}`, DocsLabel: `{"schemaVersion":1,"fields":{"nope":"x"}}`},
		{IconLabel: "", DocsLabel: ""},
	} {
		labels := map[string]string{InterfaceLabel: testLabel}
		for k, v := range others {
			labels[k] = v
		}
		res := inspectLabels(t, labels)
		if !proto.Equal(res.Interface, alone.Interface) || res.InterfaceError != alone.InterfaceError {
			t.Fatalf("with %v the interface reads %v / %q, want %v / %q",
				others, res.Interface, res.InterfaceError, alone.Interface, alone.InterfaceError)
		}
	}
}

// TestInspect_EachLabelFailsAlone: one label the engine cannot read costs
// that label alone; an empty one declares none.
func TestInspect_EachLabelFailsAlone(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		labels                         map[string]string
		iface, icon, docs              bool
		interfaceErr, iconErr, docsErr string
	}{
		{name: "a malformed icon", labels: map[string]string{InterfaceLabel: testLabel, IconLabel: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><script/></svg>`, DocsLabel: testDocsFor},
			iface: true, docs: true, iconErr: `holds the element "script"`},
		{name: "malformed docs", labels: map[string]string{InterfaceLabel: testLabel, IconLabel: testIconLabel, DocsLabel: `{"schemaVersion":1,"description":"\u202e"}`},
			iface: true, icon: true, docsErr: "format character U+202E"},
		{name: "docs of an undeclared key", labels: map[string]string{InterfaceLabel: testLabel, DocsLabel: `{"schemaVersion":1,"fields":{"dest":"x"}}`},
			iface: true, docsErr: `describes the shared with: key "dest", which the interface does not declare`},
		{name: "a malformed interface", labels: map[string]string{InterfaceLabel: `{"schemaVersion":2}`, IconLabel: testIconLabel, DocsLabel: testDocsFor},
			icon: true, docs: true, interfaceErr: "schemaVersion 2 is not supported"},
		{name: "no interface", labels: map[string]string{IconLabel: testIconLabel, DocsLabel: testDocsFor}, icon: true, docs: true},
		{name: "empty icon and docs", labels: map[string]string{InterfaceLabel: testLabel, IconLabel: "", DocsLabel: ""}, iface: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := inspectLabels(t, tc.labels)
			for _, check := range []struct {
				what    string
				present bool
				want    bool
				err     string
				wantErr string
			}{
				{"interface", res.Interface != nil, tc.iface, res.InterfaceError, tc.interfaceErr},
				{"icon", res.Icon != nil, tc.icon, res.IconError, tc.iconErr},
				{"docs", res.Docs != nil, tc.docs, res.DocsError, tc.docsErr},
			} {
				if check.present != check.want {
					t.Errorf("%s read = %v, want %v (error %q)", check.what, check.present, check.want, check.err)
				}
				if (check.wantErr == "") != (check.err == "") || !strings.Contains(check.err, check.wantErr) {
					t.Errorf("%s error = %q, want %q", check.what, check.err, check.wantErr)
				}
			}
		})
	}
}

// TestReadLabels_AReasonQuotesNothingATerminalWouldObey: whatever a parser
// says about a label an image wrote — here a stand-in parser that quotes it
// raw, since today's parsers already quote what they echo — the reason every
// client is given holds no control or format character and is bounded.
func TestReadLabels_AReasonQuotesNothingATerminalWouldObey(t *testing.T) {
	hostile := "\x1b]0;pwn\x07 \u009b31m \u202eevil \xff " + strings.Repeat("x", 2*maxReasonBytes)
	fail := func(string) error { return errors.New("cannot read " + hostile) }
	readers := labelReaders{
		iface: func(s string) (*orkstackv1.ActivityInterface, error) { return nil, fail(s) },
		icon:  func(s string) (*orkstackv1.ActivityIcon, error) { return nil, fail(s) },
		docs:  func(s string) (*orkstackv1.ActivityDocs, error) { return nil, fail(s) },
	}
	res := &Resolution{}
	readers.read(map[string]string{InterfaceLabel: "x", IconLabel: "x", DocsLabel: "x"}, res)
	for what, reason := range map[string]string{"interface": res.InterfaceError, "icon": res.IconError, "docs": res.DocsError} {
		if !strings.HasPrefix(reason, "cannot read ") || !utf8.ValidString(reason) || len(reason) > 512 {
			t.Fatalf("%s reason %q is not the parser's, bounded and valid UTF-8", what, reason)
		}
		for _, r := range reason {
			if r < 0x20 || (r >= 0x7f && r < 0xa0) || unicode.Is(unicode.Cf, r) {
				t.Fatalf("%s reason %q holds %U", what, reason, r)
			}
		}
	}
}

func TestSafeReason(t *testing.T) {
	for in, want := range map[string]string{
		"a\u2028b\u2029c":           `a\u2028b\u2029c`,
		"plain words, quoted \"x\"": "plain words, quoted \"x\"",
		"a\x1b[2Jb":                 `a\u001b[2Jb`,
		"a\u202eb\u200bc":           `a\u202eb\u200bc`,
		"a\xffb":                    `a\xffb`,
		"a\uFFFDb":                  "a\uFFFDb", // a replacement character the text holds is text
		"ünïcödé 日本":                "ünïcödé 日本",
	} {
		if got := SafeReason(in); got != want {
			t.Errorf("SafeReason(%q) = %q, want %q", in, got, want)
		}
	}
	// A tag character is past the Basic Multilingual Plane: its escape is
	// the long form, so no digit after it reads as its own.
	if got, want := SafeReason("a"+string(rune(0xe0041))+"1"), `a\U000e00411`; got != want {
		t.Errorf("SafeReason of a tag character = %q, want %q", got, want)
	}
	// A cut falls between escapes, never inside one.
	if got := SafeReason(strings.Repeat("\x1b", 200)); len(got) > 512 || !strings.HasSuffix(got, `\u001b…`) {
		t.Errorf("a reason of escapes was cut to %d bytes ending %q", len(got), got[len(got)-12:])
	}
	// A reason up to the bound passes whole; one byte over it is cut to 512
	// bytes, the ellipsis included.
	for _, n := range []int{400, 511, 512} {
		if plain := strings.Repeat("a", n); SafeReason(plain) != plain {
			t.Errorf("a %d-byte reason was cut", n)
		}
	}
	if got := SafeReason(strings.Repeat("a", 513)); got != strings.Repeat("a", 509)+"…" {
		t.Errorf("a 513-byte reason became %d bytes ending %q", len(got), got[len(got)-6:])
	}
	if got := SafeReason(strings.Repeat("日", 1000)); len(got) > 512 || len(got) < 500 || !strings.HasSuffix(got, "…") {
		t.Errorf("a long reason is %d bytes: %q", len(got), got[len(got)-8:])
	}
}

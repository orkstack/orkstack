package logstream

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"net/url"
	"strings"
	"testing"
)

// referenceMask is masking written without the automaton or the stream:
// every byte inside any occurrence of any form is covered; at the end, the
// longest tail that begins a form is covered too; each maximal run of
// covered bytes becomes one MaskReplacement.
func referenceMask(forms [][]byte, input []byte) []byte {
	return referenceMaskTail(forms, input, true)
}

// referenceMaskTail is referenceMask; withTail false leaves the tail that
// only begins a form as it is, as a stream does before it ends.
func referenceMaskTail(forms [][]byte, input []byte, withTail bool) []byte {
	covered := make([]bool, len(input))
	for _, f := range forms {
		for i := 0; i+len(f) <= len(input); i++ {
			if bytes.Equal(input[i:i+len(f)], f) {
				for j := i; j < i+len(f); j++ {
					covered[j] = true
				}
			}
		}
	}
	tail := 0
	for n := len(input); withTail && n > 0; n-- {
		suffix := input[len(input)-n:]
		for _, f := range forms {
			if bytes.HasPrefix(f, suffix) {
				tail = n
				break
			}
		}
		if tail > 0 {
			break
		}
	}
	for j := len(input) - tail; j < len(input); j++ {
		covered[j] = true
	}
	var out []byte
	for i := 0; i < len(input); i++ {
		if !covered[i] {
			out = append(out, input[i])
			continue
		}
		out = append(out, MaskReplacement...)
		for i+1 < len(input) && covered[i+1] {
			i++
		}
	}
	return out
}

// fuzzForms splits raw into forms at '|', dropping empty ones and any that
// holds a line terminator, as every registered form does.
func fuzzForms(raw string) [][]byte {
	var forms [][]byte
	for _, f := range strings.Split(raw, "|") {
		if f != "" && !strings.ContainsAny(f, "\r\n") && len(forms) < 16 {
			forms = append(forms, []byte(f))
		}
	}
	return forms
}

// FuzzMasker: for any forms, any stream and any split points, the stream's
// output equals the reference's; bytes outside occurrences keep their
// order; and, when no form holds a '*', the output holds no form.
func FuzzMasker(f *testing.F) {
	for _, seed := range []struct {
		forms, input string
		a, b         uint16
	}{
		{"hunter2-correct-horse", "token=hunter2-correct-horse\n", 3, 9},
		{"abcdefgh12|gh12345678", "xxabcdefgh12345678yy\n", 5, 11},
		{"abcdefgh|ijklmnop", "abcdefghijklmnop-\n", 8, 8},
		{"hunter2-correct-horse", "killed: hunter2-correct-hors", 10, 20},
		{"aaaa|aab", "aaaaaab\naaa", 1, 2},
		{"ab|b|abc", "abcabc\rab", 0, 4},
		{"", "nothing to mask\n", 2, 3},
	} {
		f.Add(seed.forms, seed.input, seed.a, seed.b)
	}
	f.Fuzz(func(t *testing.T, rawForms, input string, a, b uint16) {
		forms := fuzzForms(rawForms)
		want := referenceMask(forms, []byte(input))
		cut1 := int(a) % (len(input) + 1)
		cut2 := cut1 + int(b)%(len(input)-cut1+1)
		var out bytes.Buffer
		st := maskStream{match: newMatcher(forms)}
		for _, piece := range [][]byte{[]byte(input[:cut1]), []byte(input[cut1:cut2]), []byte(input[cut2:])} {
			st.feed(piece)
			out.Write(st.appendEmit(nil, false))
			// Nothing held spans a line terminator.
			if bytes.ContainsAny(st.buf, "\r\n") {
				t.Fatalf("held %q across a terminator", st.buf)
			}
		}
		st.finish(0)
		out.Write(st.appendEmit(nil, true))
		if got := out.Bytes(); !bytes.Equal(got, want) {
			t.Fatalf("forms %q, input %q, cuts %d,%d: got %q, want %q", forms, input, cut1, cut2, got, want)
		}
		hasStar := false
		for _, form := range forms {
			hasStar = hasStar || bytes.Contains(form, []byte("*"))
		}
		if !hasStar {
			for _, form := range forms {
				if bytes.Contains(out.Bytes(), form) {
					t.Fatalf("output %q holds form %q", out.Bytes(), form)
				}
			}
		}
	})
}

// FuzzMaskForms: every form of a value spells the value, or a line of it,
// by its own encoding; a core is in the value's encoding at its alignment;
// none is short, holds a terminator or repeats.
func FuzzMaskForms(f *testing.F) {
	for _, seed := range []string{
		"hunter2-correct-horse", "pass word/with&odd\"chars", "1234", "12345678",
		"line one secret\nline two secret", "<tag>&amp; ", "\xff\xfe binary bytes",
		strings.Repeat("~~~?", 80),
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		lines := map[string]bool{}
		for _, line := range splitLines(value) {
			lines[string(line)] = true
			lines[string(bytes.TrimSpace(line))] = true
		}
		seen := map[string]bool{}
		for _, fm := range maskForms(value) {
			s := string(fm.bytes)
			if len(fm.bytes) < MaskMinFormBytes || strings.ContainsAny(s, "\r\n") || seen[s] {
				t.Fatalf("form %q (kind %d) is short, holds a terminator or repeats", s, fm.kind)
			}
			seen[s] = true
			var back []byte
			var err error
			switch fm.kind {
			case formRaw:
				back = fm.bytes
			case formLine:
				if !lines[s] {
					t.Fatalf("line form %q is no line of the value", s)
				}
				continue
			case formPEMBody:
				continue // FuzzPEMForms
			case formBase64Std:
				back, err = base64.StdEncoding.DecodeString(s)
			case formBase64StdRaw:
				back, err = base64.RawStdEncoding.DecodeString(s)
			case formBase64URL:
				back, err = base64.URLEncoding.DecodeString(s)
			case formBase64URLRaw:
				back, err = base64.RawURLEncoding.DecodeString(s)
			case formBase64Core:
				enc := base64.RawStdEncoding
				if fm.url {
					enc = base64.RawURLEncoding
				}
				whole := enc.EncodeToString(append(make([]byte, fm.align), value...))
				if !strings.Contains(whole, s) {
					t.Fatalf("core %q is not in the value's encoding at alignment %d", s, fm.align)
				}
				continue
			case formPercentPlus, formPercent20:
				var text string
				text, err = url.QueryUnescape(s)
				back = []byte(text)
			case formJSON, formJSONHTML:
				var text string
				err = json.Unmarshal([]byte(`"`+s+`"`), &text)
				back = []byte(text)
			case formHexLower, formHexUpper:
				back, err = hex.DecodeString(s)
			}
			if err != nil || !bytes.Equal(back, value) {
				t.Fatalf("form %q (kind %d) decodes to %q, %v; want the value %q", s, fm.kind, back, err, value)
			}
		}
	})
}

// FuzzPEMForms: no form of a PEM block is an armour line, and every body
// line of 8 bytes or more and the joined body are forms.
func FuzzPEMForms(f *testing.F) {
	f.Add("PRIVATE KEY", "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC\nBKcwggSjAgEAAoIBAQC7")
	f.Add("CERTIFICATE", "short\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A")
	f.Fuzz(func(t *testing.T, label, body string) {
		if strings.ContainsAny(label, "\r\n-") || strings.TrimSpace(label) != label || label == "" {
			return
		}
		begin, end := "-----BEGIN "+label+"-----", "-----END "+label+"-----"
		value := []byte(begin + "\n" + body + "\n" + end + "\n")
		forms := map[string]bool{}
		for _, fm := range MaskForms(value) {
			forms[string(fm)] = true
		}
		if forms[begin] || forms[end] {
			t.Fatalf("an armour line is a form: %v", forms)
		}
		var joined []byte
		for _, line := range splitLines([]byte(body)) {
			if isArmour(line) {
				return // a body that holds armour is another document
			}
			joined = append(joined, bytes.TrimSpace(line)...)
			if len(line) >= MaskMinFormBytes && !forms[string(line)] {
				t.Fatalf("body line %q is no form", line)
			}
		}
		if len(joined) >= MaskMinFormBytes && !forms[string(joined)] {
			t.Fatalf("the joined body %q is no form", joined)
		}
	})
}

// FuzzMaskerSwap: forms are replaced mid-stream (a refresh adds a value, an
// expiry drops one). What was passed on before the swap is the old forms'
// masking of those bytes, and what follows is the new forms' masking of
// every byte the stream still held, read afresh.
func FuzzMaskerSwap(f *testing.F) {
	f.Add("abcdefgh|ijklmnop|abcdefgh!!ijklmnop!!!!", "abcdefgh|ijklmnop", "abcdefgh!!ijklmnop!!", uint16(20))
	f.Add("first-secret", "first-secret|refreshed-token", "x first-secret refreshed-tok y\n", uint16(20))
	f.Add("ab|b", "abc", "0bab\nab", uint16(3))
	f.Fuzz(func(t *testing.T, rawA, rawB, input string, cut uint16) {
		a, b := fuzzForms(rawA), fuzzForms(rawB)
		at := int(cut) % (len(input) + 1)
		st := maskStream{match: newMatcher(a)}
		st.feed([]byte(input[:at]))
		before := st.appendEmit(nil, false)
		base := int(st.base)
		if want := referenceMaskTail(a, []byte(input[:base]), false); !bytes.Equal(before, want) {
			t.Fatalf("before the swap at %d (base %d): got %q, want %q", at, base, before, want)
		}
		st.rescan(newMatcher(b))
		var after []byte
		after = st.appendEmit(after, false)
		st.feed([]byte(input[at:]))
		after = st.appendEmit(after, false)
		st.finish(0)
		after = st.appendEmit(after, true)
		if want := referenceMask(b, []byte(input[base:])); !bytes.Equal(after, want) {
			t.Fatalf("after the swap at %d (base %d): got %q, want %q", at, base, after, want)
		}
	})
}

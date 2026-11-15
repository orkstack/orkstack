package logstream_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/pkg/logstream"
)

// maskAll streams input through a masker holding values, cut at each of
// cuts, and returns what reached the writer once the stream closed.
func maskAll(t *testing.T, values []string, input string, cuts ...int) string {
	t.Helper()
	m := logstream.NewMasker()
	for i, v := range values {
		if _, err := m.Add("holder", []byte(v)); err != nil {
			t.Fatalf("Add(value %d): %v", i, err)
		}
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	last := 0
	for _, cut := range append(cuts, len(input)) {
		if _, err := w.Write([]byte(input[last:cut])); err != nil {
			t.Fatal(err)
		}
		last = cut
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestMasker_MasksEveryFormOfAValue(t *testing.T) {
	secret := "hunter2-correct-horse"
	b64 := base64.StdEncoding.EncodeToString([]byte(secret))
	for _, tc := range []struct {
		name, input, want string
	}{
		{"the value", "token=" + secret + "\n", "token=***\n"},
		{"standard base64", "b " + b64 + "\n", "b ***\n"},
		{"unpadded url base64", "b " + base64.RawURLEncoding.EncodeToString([]byte(secret)) + "\n", "b ***\n"},
		{"lowercase hex", hex.EncodeToString([]byte(secret)) + "\n", "***\n"},
		{"uppercase hex", strings.ToUpper(hex.EncodeToString([]byte(secret))) + "\n", "***\n"},
		{"twice on one line", secret + " and " + secret, "*** and ***"},
		{"nothing to mask", "all is well\n", "all is well\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskAll(t, []string{secret}, tc.input); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A value inside a longer string that was base64-encoded whole is caught at
// each of the three alignments, all but the edges of its encoding.
func TestMasker_CatchesAValueEncodedInsideALongerString(t *testing.T) {
	secret := "s3cr3t-value-of-32-bytes-or-more"
	for _, prefix := range []string{"user:", "us:", "u:"} {
		encoded := base64.StdEncoding.EncodeToString([]byte(prefix + secret + "@host"))
		got := maskAll(t, []string{secret}, encoded+"\n")
		if !strings.Contains(got, "***") {
			t.Errorf("prefix %q: %q is not masked in %q", prefix, encoded, got)
		}
		if len(got) > len("***\n")+16 {
			t.Errorf("prefix %q: %q leaves more than the encoding's edges", prefix, got)
		}
	}
}

func TestMasker_PercentAndJSONForms(t *testing.T) {
	secret := "pass word/with&odd\"chars"
	for _, tc := range []struct{ name, input string }{
		{"percent with +", "q=pass+word%2Fwith%26odd%22chars\n"},
		{"percent with %20", "q=pass%20word%2Fwith%26odd%22chars\n"},
		{"json string", `{"p":"pass word/with&odd\"chars"}` + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskAll(t, []string{secret}, tc.input); strings.Contains(got, "word") {
				t.Errorf("got %q, want the form masked", got)
			}
		})
	}
}

// Overlapping occurrences of different forms are masked as their union: no
// tail of one is left visible, and touching runs become one ***.
func TestMasker_MasksTheUnionOfOccurrences(t *testing.T) {
	got := maskAll(t, []string{"abcdefgh12", "gh12345678"}, "xxabcdefgh12345678yy\n")
	if got != "xx***yy\n" {
		t.Errorf("got %q, want xx***yy", got)
	}
	got = maskAll(t, []string{"abcdefgh", "ijklmnop"}, "abcdefghijklmnop-\n")
	if got != "***-\n" {
		t.Errorf("touching occurrences: got %q, want one ***", got)
	}
}

// Splitting the stream at any byte gives the same output.
func TestMasker_SplitsAnywhereGiveTheSameOutput(t *testing.T) {
	values := []string{"hunter2-correct-horse", "another-secret-value"}
	input := "a hunter2-correct-horse b another-secret-value c hunter2-corr\nend"
	whole := maskAll(t, values, input)
	for cut := 0; cut <= len(input); cut++ {
		for cut2 := cut; cut2 <= len(input); cut2 += 7 {
			if got := maskAll(t, values, input, cut, cut2); got != whole {
				t.Fatalf("cuts %d,%d: got %q, want %q", cut, cut2, got, whole)
			}
		}
	}
}

// Nothing is held across a line terminator: a complete line reaches the
// writer at once, even when the line before the terminator began a secret.
func TestMasker_HoldsNothingAcrossALineEnd(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("h", []byte("hunter2-correct-horse")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	if _, err := w.Write([]byte("prompt: hunter2-")); err != nil {
		t.Fatal(err)
	}
	if out.String() != "prompt: " {
		t.Errorf("after a prefix of the secret: %q reached the writer, want the prefix held", out.String())
	}
	if _, err := w.Write([]byte("\nnext line\n")); err != nil {
		t.Fatal(err)
	}
	if out.String() != "prompt: hunter2-\nnext line\n" {
		t.Errorf("after the terminator: %q, want every complete line released", out.String())
	}
}

// What is still held when the stream ends is masked, not released.
func TestMasker_MasksWhatIsHeldAtClose(t *testing.T) {
	if got := maskAll(t, []string{"hunter2-correct-horse"}, "killed: hunter2-correct-hors"); got != "killed: ***" {
		t.Errorf("got %q, want the held prefix masked", got)
	}
}

// Forms shorter than eight bytes are not registered; Add says when a value
// is left unmasked entirely.
func TestMasker_ShortValuesAreNotMasked(t *testing.T) {
	m := logstream.NewMasker()
	masked, err := m.Add("pin", []byte("1234"))
	if err != nil || masked {
		t.Errorf("Add(1234) = %v, %v; want unmasked, no error", masked, err)
	}
	masked, err = m.Add("pw", []byte("12345678"))
	if err != nil || !masked {
		t.Errorf("Add(12345678) = %v, %v; want masked", masked, err)
	}
}

// A form added while the stream runs masks what follows, and the held bytes
// are read again under it.
func TestMasker_AddMidStream(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("a", []byte("first-secret-value")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	if _, err := w.Write([]byte("x first-secret-value ")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add("b", []byte("refreshed-token-value")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("y refreshed-token-value\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "x *** y ***\n" {
		t.Errorf("got %q, want both values masked", got)
	}
}

// A form leaves only with its last holder.
func TestMasker_AFormLeavesWithItsLastHolder(t *testing.T) {
	m := logstream.NewMasker()
	for _, holder := range []string{"lease-1", "lease-2"} {
		if _, err := m.Add(holder, []byte("same-password-123")); err != nil {
			t.Fatal(err)
		}
	}
	forms, _ := m.Size()
	m.Drop("lease-1")
	if after, _ := m.Size(); after != forms {
		t.Errorf("dropping one of two holders changed the form count from %d to %d", forms, after)
	}
	if got := m.Mask("pw=same-password-123"); got != "pw=***" {
		t.Errorf("after dropping one holder: %q, want still masked", got)
	}
	m.Drop("lease-2")
	if after, _ := m.Size(); after != 0 {
		t.Errorf("after the last holder left: %d forms, want 0", after)
	}
	if got := m.Mask("pw=same-password-123"); got != "pw=same-password-123" {
		t.Errorf("after the last holder left: %q, want it unmasked", got)
	}
}

// The bounds refuse a value whole, leaving the set as it was.
func TestMasker_RefusesAValuePastTheBounds(t *testing.T) {
	m := logstream.NewMasker()
	big := bytes.Repeat([]byte("x"), logstream.MaskMaxFormBytes+1)
	_, err := m.Add("big", big)
	var capacity *logstream.MaskCapacityError
	if !errors.As(err, &capacity) || capacity.Bytes <= logstream.MaskMaxFormBytes {
		t.Fatalf("Add(512 KiB) = %v, want a capacity error naming the bytes", err)
	}
	if forms, n := m.Size(); forms != 0 || n != 0 {
		t.Errorf("a refused Add left %d forms, %d bytes", forms, n)
	}
}

// Long values take fewer forms; a PEM block's armour lines are never forms.
func TestMaskForms_LongValuesAndPEM(t *testing.T) {
	long := strings.Repeat("~~~?", 80) // 320 bytes; its two alphabets differ ("+" and "-")
	if forms := logstream.MaskForms([]byte(long)); len(forms) != 3 {
		t.Errorf("a 320-byte value has %d forms, want 3 (raw and two unpadded base64)", len(forms))
	}
	pem := "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\nBKcwggSjAgEAAoIBAQC7\n-----END PRIVATE KEY-----\n"
	forms := logstream.MaskForms([]byte(pem))
	var joined bool
	for _, f := range forms {
		if s := string(f); s == "-----BEGIN PRIVATE KEY-----" || s == "-----END PRIVATE KEY-----" || bytes.ContainsAny(f, "\r\n") {
			t.Errorf("form %q is an armour line or holds a terminator", f)
		}
		if string(f) == "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7" {
			joined = true
		}
	}
	if !joined {
		t.Errorf("forms %q lack the joined body", forms)
	}
	if got := maskAll(t, []string{pem}, "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\n"); got != "-----BEGIN PRIVATE KEY-----\n***\n" {
		t.Errorf("printing the key: %q, want its header kept and its body masked", got)
	}
}

// The two streams of one masker keep their own state: half a secret on
// stdout and half on stderr is two halves, not a secret.
func TestMasker_StreamsKeepTheirOwnState(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("h", []byte("hunter2-correct-horse")); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	o, e := m.Writer(&stdout), m.Writer(&stderr)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = o.Write([]byte("hunter2-\n")) }()
	go func() { defer wg.Done(); _, _ = e.Write([]byte("correct-horse\n")) }()
	wg.Wait()
	_ = o.Close()
	_ = e.Close()
	if stdout.String() != "hunter2-\n" || stderr.String() != "correct-horse\n" {
		t.Errorf("stdout %q, stderr %q; want each half kept", stdout.String(), stderr.String())
	}
}

// A swap re-reads what is held from scratch: two runs held apart stay apart
// when a form that joined them is dropped.
func TestMasker_ADropKeepsHeldRunsApart(t *testing.T) {
	m := logstream.NewMasker()
	for holder, v := range map[string]string{"a": "abcdefgh", "b": "ijklmnop", "c": "abcdefgh!!ijklmnop!!!!"} {
		if _, err := m.Add(holder, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	if _, err := w.Write([]byte("abcdefgh!!ijklmnop!!")); err != nil {
		t.Fatal(err)
	}
	m.Drop("c")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "***!!***!!" {
		t.Errorf("got %q, want ***!!***!!", got)
	}
}

// A secret repeated without a line end is not held without bound: past the
// bound it is passed on masked, in pieces.
func TestMasker_ARepeatedSecretIsNotHeldWithoutBound(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("h", []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	chunk := bytes.Repeat([]byte("abcdefgh"), 1<<10)
	for range 100 { // 800 KiB
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if out.Len() == 0 {
		t.Fatal("800 KiB of a repeated secret are all held")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Trim(out.String(), "*"); got != "" {
		t.Errorf("unmasked bytes passed on: %q", got)
	}
}

// The rest of a run cut at the held bound stays masked when a swap reads the
// held bytes again, though the swap cannot find its occurrence any more.
func TestMasker_ACutRunStaysMaskedAcrossASwap(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("a", []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add("g", []byte("fghXXXXXXXX")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	// 72 KiB of the secret, then a byte that leaves "fghX" held: it begins
	// the other form, so the cut falls inside the last occurrence.
	if _, err := w.Write(append(bytes.Repeat([]byte("abcdefgh"), 9<<10), 'X')); err != nil {
		t.Fatal(err)
	}
	m.Drop("g")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(strings.Trim(out.String(), "*"), "X"); got != "" {
		t.Errorf("unmasked bytes of the secret passed on after the swap: %q", got)
	}
}

// failingWriter fails every write.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("the sink is gone") }

// A sink failure on bytes a swap released is reported: by Err at once, and
// by the next Write and by Close.
func TestMasker_ASwapsSinkFailureIsReported(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("a", []byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	w := m.Writer(failingWriter{})
	if _, err := w.Write([]byte("abcdefg")); err != nil { // held: it begins the form
		t.Fatal(err)
	}
	m.Drop("a") // releases what was held, into a sink that fails
	if w.Err() == nil {
		t.Fatal("Err is nil after the swap's write failed")
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Error("the next Write did not report the failure")
	}
	if err := w.Close(); err == nil {
		t.Error("Close did not report the failure")
	}
}

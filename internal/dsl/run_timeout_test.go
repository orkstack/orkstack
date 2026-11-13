package dsl

import (
	"errors"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const runTimeoutHeader = "apiVersion: orkstack/v1\nkind: Orkflow\nname: nightly\n"
const runTimeoutTasks = "tasks:\n  - id: a\n    uses: busybox:1\n"

func runTimeoutDoc(timeoutLine string) []byte {
	if timeoutLine != "" {
		timeoutLine += "\n"
	}
	return []byte(runTimeoutHeader + timeoutLine + runTimeoutTasks)
}

func TestRunTimeout_Grammar(t *testing.T) {
	accept := map[string]int64{
		"timeout: 10s":    10,
		"timeout: 6h":     21600,
		"timeout: 1h30m":  5400,
		"timeout: 8760h":  31536000,
		"timeout: '360m'": 21600,
		"timeout: 90s":    90,
	}
	for line, want := range accept {
		wf, err := CompileWithOptions(runTimeoutDoc(line), Options{})
		seconds := wf.GetTimeoutSeconds()
		if err != nil || seconds != want {
			t.Errorf("%s: seconds %d, err %v; want %d", line, seconds, err, want)
		}
	}

	grammar := "is not a duration such as 30m or 6h"
	whole := "must be a whole number of seconds"
	bounds := "is outside 10s … 8760h; omit timeout: for a run with no time limit"
	refuse := map[string]string{
		"timeout: 9s":       bounds,
		"timeout: 8761h":    bounds,
		"timeout: 1.5s":     whole,
		"timeout: 500ms":    whole,
		"timeout: 0s":       bounds,
		"timeout: -1h":      bounds,
		"timeout: ''":       grammar,
		"timeout: 1d":       grammar,
		"timeout: yes":      grammar,
		"timeout: null":     "omit timeout:",
		"timeout:":          "omit timeout:",
		"timeout: 3600":     "the run's timeout is a duration such as 3600s or 1h; tasks use timeout_seconds",
		"timeout: [6h]":     grammar,
		"timeout: {a: 1}":   grammar,
		"timeout: !!str 6h": "tag",
		"timeout: '3600'":   grammar,
	}
	for line, want := range refuse {
		_, err := CompileWithOptions(runTimeoutDoc(line), Options{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err %v; want it to contain %q", line, err, want)
			continue
		}
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != "timeout" {
			t.Errorf("%s: err %v is not a ValidationError for field timeout", line, err)
		}
	}
}

func TestRunTimeout_AbsentIsZero(t *testing.T) {
	wf, err := CompileWithOptions(runTimeoutDoc(""), Options{})
	if err != nil || wf.GetTimeoutSeconds() != 0 {
		t.Fatalf("absent: %d, %v", wf.GetTimeoutSeconds(), err)
	}
}

func TestRunTimeout_CompileKeepsPublicSignature(t *testing.T) {
	if _, err := Compile(runTimeoutDoc("timeout: 6h")); err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(runTimeoutDoc("timeout: 3600")); err == nil {
		t.Fatal("Compile accepted an integer timeout")
	}
}

func TestValidateRunTimeout_Bounds(t *testing.T) {
	for _, ok := range []int64{0, 10, 21600, 31536000} {
		if err := ValidateRunTimeout(ok); err != nil {
			t.Errorf("ValidateRunTimeout(%d) = %v", ok, err)
		}
	}
	for _, bad := range []int64{-1, 1, 9, 31536001} {
		err := ValidateRunTimeout(bad)
		if err == nil || !strings.Contains(err.Error(), "is outside 10s … 8760h") {
			t.Errorf("ValidateRunTimeout(%d) = %v", bad, err)
		}
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != "timeout" {
			t.Errorf("ValidateRunTimeout(%d): %v is not a ValidationError for timeout", bad, err)
		}
	}
}

func TestCanonical_NoTimeoutUnchangedBytes(t *testing.T) {
	wf := goldenOrkflow(t)
	plain, err := CanonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := canonicalWithTimeout(wf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != string(zero) || strings.Contains(string(zero), `"timeout":`) {
		t.Fatalf("a definition without timeout changed: %s", zero)
	}
}

func TestCanonical_TimeoutShortForm(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "x"}
	for seconds, want := range map[int64]string{
		21600: `"timeout":"6h"`, 5400: `"timeout":"1h30m"`, 90: `"timeout":"1m30s"`,
		10: `"timeout":"10s"`, 60: `"timeout":"1m"`, 31536000: `"timeout":"8760h"`,
	} {
		got, err := canonicalWithTimeout(wf, seconds)
		if err != nil || !strings.Contains(string(got), want) {
			t.Errorf("%d: %s, %v; want %s", seconds, got, err, want)
		}
	}
	a, _ := canonicalWithTimeout(wf, 21600)
	b, _ := canonicalWithTimeout(wf, int64(360*time.Minute/time.Second))
	base, _ := canonicalWithTimeout(wf, 0)
	if string(a) != string(b) || string(a) == string(base) {
		t.Fatal("6h and 360m must share bytes and differ from no timeout")
	}
}

// canonicalWithTimeout is wf's canonical form with timeout_seconds set to
// seconds, on a copy, so one Orkflow gives every case.
func canonicalWithTimeout(wf *orkstackv1.Orkflow, seconds int64) ([]byte, error) {
	c := proto.Clone(wf).(*orkstackv1.Orkflow)
	c.TimeoutSeconds = seconds
	return CanonicalOrkflow(c)
}

// FuzzRunTimeoutDuration: what ParseRunTimeout accepts is a whole number of
// seconds inside the bounds that spells itself back to the same value.
func FuzzRunTimeoutDuration(f *testing.F) {
	for _, seed := range []string{"10s", "6h", "1h30m", "8760h", "9s", "8761h", "1.5s", "500ms", "0s", "-1h",
		"3600", "", "1d", "yes", "null", "360m", "9223372036854775807ns", "2562048h", "\x00", "1h 30m", "+6h", ".5h"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		seconds, err := ParseRunTimeout(text)
		if err != nil {
			if seconds != 0 {
				t.Fatalf("ParseRunTimeout(%q) = %d with %v", text, seconds, err)
			}
			return
		}
		if seconds < MinRunTimeoutSeconds || seconds > MaxRunTimeoutSeconds {
			t.Fatalf("ParseRunTimeout(%q) = %d, outside the bounds", text, seconds)
		}
		d := time.Duration(seconds) * time.Second
		if parsed, perr := time.ParseDuration(text); perr != nil || parsed != d {
			t.Fatalf("ParseRunTimeout(%q) = %d but time.ParseDuration says %v, %v", text, seconds, parsed, perr)
		}
		back, err := ParseRunTimeout(shortDuration(d))
		if err != nil || back != seconds {
			t.Fatalf("ParseRunTimeout(%q) = %d; its short form %q reads %d, %v", text, seconds, shortDuration(d), back, err)
		}
		if err := ValidateRunTimeout(seconds); err != nil {
			t.Fatalf("ParseRunTimeout(%q) = %d, which ValidateRunTimeout refuses: %v", text, seconds, err)
		}
	})
}

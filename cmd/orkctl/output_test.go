package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/evidence"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskStatusLabel_FlagsACanceledTaskThatNeverStarted(t *testing.T) {
	taskRun := &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_CANCELED}
	if got := taskStatusLabel(taskRun); got != "CANCELED (never started)" {
		t.Fatalf("taskStatusLabel(never-started CANCELED) = %q, want %q", got, "CANCELED (never started)")
	}
}

func TestTaskStatusLabel_LeavesACanceledTaskThatDidStartAlone(t *testing.T) {
	taskRun := &orkstackv1.TaskRun{
		Status:    orkstackv1.Status_STATUS_CANCELED,
		StartedAt: timestamppb.New(time.Now()),
	}
	if got := taskStatusLabel(taskRun); got != "CANCELED" {
		t.Fatalf("taskStatusLabel(started-then-canceled) = %q, want %q", got, "CANCELED")
	}
}

func TestTaskStatusLabel_LeavesEveryOtherStatusAlone(t *testing.T) {
	cases := []orkstackv1.Status{
		orkstackv1.Status_STATUS_PENDING,
		orkstackv1.Status_STATUS_RUNNING,
		orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED,
	}
	for _, status := range cases {
		taskRun := &orkstackv1.TaskRun{Status: status}
		want := statusLabel(status)
		if got := taskStatusLabel(taskRun); got != want {
			t.Fatalf("taskStatusLabel(%s) = %q, want %q", status, got, want)
		}
	}
}

// TestPayloadSummary_IsBoundedAndPointsAtTheWholeDocument: run get prints a
// payload on one line, so it is cut at payloadSummaryMax bytes and the cut
// names its size and the command that prints it whole; up to the bound it
// prints as it is.
func TestPayloadSummary_IsBoundedAndPointsAtTheWholeDocument(t *testing.T) {
	payload := func(n int) *orkstackv1.Payload {
		data, err := structpb.NewStruct(map[string]any{"k": strings.Repeat("a", n)})
		if err != nil {
			t.Fatalf("struct: %v", err)
		}
		return &orkstackv1.Payload{Data: data}
	}
	// `{"k":"` + n bytes + `"}` is n+8 bytes of JSON.
	under := `{"k":"` + strings.Repeat("a", 10) + `"}`
	at := `{"k":"` + strings.Repeat("a", 504) + `"}`
	over := (`{"k":"` + strings.Repeat("a", 1000) + `"}`)[:512]
	for _, tc := range []struct {
		name      string
		payload   *orkstackv1.Payload
		namespace string
		want      string
	}{
		{"empty", nil, "", "-"},
		{"under the bound", payload(10), "", under},
		{"at the bound", payload(504), "", at},
		{"over the bound", payload(1000), "",
			over + "… (1008 bytes; orkctl run get ork-1 --json prints it whole)"},
		{"over the bound, in a namespace", payload(1000), "payments",
			over + "… (1008 bytes; orkctl -n payments run get ork-1 --json prints it whole)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := payloadSummary(tc.payload, "ork-1", tc.namespace); got != tc.want {
				t.Fatalf("payloadSummary =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// TestPayloadSummary_NeverSplitsACharacter: the cut lands on a UTF-8
// boundary, so the line stays valid text.
func TestPayloadSummary_NeverSplitsACharacter(t *testing.T) {
	data, err := structpb.NewStruct(map[string]any{"k": strings.Repeat("é", 400)})
	if err != nil {
		t.Fatalf("struct: %v", err)
	}
	got := payloadSummary(&orkstackv1.Payload{Data: data}, "ork-1", "")
	head, _, ok := strings.Cut(got, "… (")
	if !ok {
		t.Fatalf("an 800-byte payload was not cut: %q", got)
	}
	if !utf8.ValidString(head) || len(head) > payloadSummaryMax {
		t.Fatalf("cut = %q (%d bytes), want valid UTF-8 within %d bytes", head, len(head), payloadSummaryMax)
	}
}

// TestOneLinePrinters_EscapeWhatATerminalObeys: a run's or a task's error, a
// schedule's input and a task run's values each reach a human table or report
// as one line, and none of them may print a character a terminal would obey.
func TestOneLinePrinters_EscapeWhatATerminalObeys(t *testing.T) {
	hostile := "a\x1b]0;x\x07\u009b31m\u202e\x7f\nb"
	data, err := structpb.NewStruct(map[string]any{"k": hostile})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"errorLine, code and message": errorLine(&orkstackv1.Error{Code: "X\x1b[2J", Message: hostile}),
		"errorLine, message alone":    errorLine(&orkstackv1.Error{Message: hostile}),
		"inputLabel":                  inputLabel(&orkstackv1.Payload{Data: data}),
		"canonicalValues, JSON":       canonicalValues([]byte(`{"k": "a\u001b]0;x\u0007` + "\u009b\u202e\x7f" + `"}`)),
		"canonicalValues, not JSON":   canonicalValues([]byte(hostile)),
		"canonicalValues, cut":        canonicalValues([]byte(strings.Repeat("\u009b", taskRunValuesPrintLimit))),
	}
	for name, got := range cases {
		t.Run(name, func(t *testing.T) {
			for _, r := range got {
				if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
					t.Fatalf("%q holds %U raw", got, r)
				}
			}
		})
	}
	if got, want := inputLabel(&orkstackv1.Payload{Data: data}), `{"k":"a\u001b]0;x\u0007\u009b31m\u202e\u007f\nb"}`; got != want {
		t.Fatalf("inputLabel = %s, want %s: still JSON, escaped inside its string", got, want)
	}
}

// jsonTerminalSafeCases are text and its form inside a JSON string once the
// encoder and jsonTerminalSafe have both had it.
var jsonTerminalSafeCases = []struct {
	name, text, want string
}{
	{"plain text", "plain", `plain`},
	{"other scripts", "日本語 é", `日本語 é`},
	{"an emoji", "\U0001F600", "\U0001F600"},
	{"DEL", "a\x7fb", `a\u007fb`},
	{"a C1 control", "\u009b31m", `\u009b31m`},
	{"a bidirectional override", "a\u202eb", `a\u202eb`},
	{"a zero-width joiner", "a\u200db", `a\u200db`},
	{"a byte order mark", "\ufeffa", `\ufeffa`},
	{"a tag character", "a\U000e0041b", `a\udb40\udc41b`},
	{"what the encoder escapes itself", "\x1b]0;x\x07\n", `\u001b]0;x\u0007\n`},
	{"a separator the encoder escapes", "a\u2028b", `a\u2028b`},
	{"a quote and a backslash", `"\`, `\"\\`},
}

func TestJSONTerminalSafe(t *testing.T) {
	for _, tc := range jsonTerminalSafeCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := string(jsonTerminalSafe(encoded)), `"`+tc.want+`"`; got != want {
				t.Fatalf("jsonTerminalSafe(%s) = %s, want %s", encoded, got, want)
			}
		})
	}
}

// requireJSONTerminalSafe holds what jsonTerminalSafe claims of an encoder's
// output: no raw character a terminal could obey, and the same JSON.
func requireJSONTerminalSafe(t *testing.T, encoded, safe []byte) {
	t.Helper()
	for _, r := range string(safe) {
		if r == 0x7f || (r >= 0x80 && r <= 0x9f) || unicode.Is(unicode.Cf, r) {
			t.Fatalf("%q holds %U raw", safe, r)
		}
	}
	var before, after any
	if err := json.Unmarshal(encoded, &before); err != nil {
		t.Fatalf("the encoder's own output %q: %v", encoded, err)
	}
	if err := json.Unmarshal(safe, &after); err != nil {
		t.Fatalf("%q from %q no longer parses: %v", safe, encoded, err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("%q from %q decodes to %#v, was %#v", safe, encoded, after, before)
	}
}

// FuzzJSONTerminalSafe: whatever a value holds, in a key or a string, what
// --json prints has no raw character a terminal could obey and is the same
// JSON. Of any bytes at all it is idempotent, leaves what it does not escape
// alone, and keeps valid JSON valid and equal.
func FuzzJSONTerminalSafe(f *testing.F) {
	for _, tc := range jsonTerminalSafeCases {
		f.Add(tc.text, []byte(`{"`+tc.text+`": 1}`))
	}
	f.Add("\xff\xfe", []byte{0xff, 0x9b, 0x7f, 'a'})
	f.Add("", []byte(`\u202e not a string`))
	f.Fuzz(func(t *testing.T, text string, raw []byte) {
		for _, encode := range []func(any) ([]byte, error){
			json.Marshal,
			func(value any) ([]byte, error) { return json.MarshalIndent(value, "", "  ") },
		} {
			encoded, err := encode(map[string]any{text: []string{text}})
			if err != nil {
				t.Fatal(err)
			}
			requireJSONTerminalSafe(t, encoded, jsonTerminalSafe(encoded))
		}
		safe := jsonTerminalSafe(raw)
		if again := jsonTerminalSafe(safe); !bytes.Equal(again, safe) {
			t.Fatalf("not idempotent on %q: %q then %q", raw, safe, again)
		}
		if json.Valid(raw) {
			requireJSONTerminalSafe(t, raw, safe)
		}
	})
}

// TestJSONWriters_EscapeWhatTheyPrint: each writer behind a --json that does
// not print a protobuf message, given a value holding what a terminal obeys.
func TestJSONWriters_EscapeWhatTheyPrint(t *testing.T) {
	const hostile = "x\x7f\u009b31m\u202e\U000e0041"
	const escaped = `x\u007f\u009b31m\u202e\udb40\udc41`
	for _, tc := range []struct {
		name  string
		write func(t *testing.T) []byte
	}{
		{"plain Go values: webhook, version, activity test, task run", func(t *testing.T) []byte {
			var out bytes.Buffer
			if err := writeGoJSON(&out, map[string]string{"name": hostile}); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"a raw label inside a report", func(t *testing.T) []byte {
			var out bytes.Buffer
			if err := writeGoJSON(&out, map[string]json.RawMessage{"label": json.RawMessage(`"` + hostile + `"`)}); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"run timing", func(t *testing.T) []byte {
			var out bytes.Buffer
			if err := writeTimingJSON(&out, &runTiming{RunID: hostile}); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"evidence verify", func(t *testing.T) []byte {
			var out bytes.Buffer
			if err := writeVerifyJSON(&out, &evidence.Report{Failures: []evidence.Failure{{Path: hostile}}}, nil); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"the log stream", func(t *testing.T) []byte {
			var out bytes.Buffer
			writer := &logWriter{out: &out, json: true}
			if err := writer.chunk(&orkstackv1.TaskLogChunk{TaskId: hostile}, false); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"the inspect stream", func(t *testing.T) []byte {
			encoded, err := marshalCompactJSON(&orkstackv1.InspectRunResponse{Attempts: []*orkstackv1.LiveAttempt{{TaskId: hostile}}})
			if err != nil {
				t.Fatal(err)
			}
			return encoded
		}},
		{"the watch stream", func(t *testing.T) []byte {
			var out bytes.Buffer
			sink := &jsonEventSink{out: &out}
			if err := sink.event(&orkstackv1.Event{Message: hostile}); err != nil {
				t.Fatal(err)
			}
			return out.Bytes()
		}},
		{"the config file", func(t *testing.T) []byte {
			t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
			path, err := saveConfig(cliConfig{Namespace: hostile})
			if err != nil {
				t.Fatal(err)
			}
			got, err := loadConfig()
			if err != nil || got.Namespace != hostile {
				t.Fatalf("the config read back as %q, %v", got.Namespace, err)
			}
			written, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			return written
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.write(t)
			if !bytes.Contains(got, []byte(escaped)) {
				t.Fatalf("wrote %q, want it to hold %s", got, escaped)
			}
			requireTerminalSafeOutput(t, tc.name, string(got))
		})
	}
}

// TestJSONWriters_NoOtherEncoderPrints: json.MarshalIndent, json.NewEncoder and
// api.MarshalJSON stand only in output.go, so no writer can print past
// jsonTerminalSafe. json.Marshal stays free: a human table encodes a cell with
// it, and the document a --json writer builds from it passes through output.go.
func TestJSONWriters_NoOtherEncoderPrints(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	banned := map[string]bool{"json.MarshalIndent": true, "json.NewEncoder": true, "api.MarshalJSON": true}
	fset := token.NewFileSet()
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") || source == "output.go" {
			continue
		}
		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.SelectorExpr); ok {
				if pkg, ok := call.X.(*ast.Ident); ok && banned[pkg.Name+"."+call.Sel.Name] {
					t.Errorf("%s: %s.%s prints past jsonTerminalSafe; use writeJSON, writeGoJSON, marshalJSON or marshalCompactJSON",
						fset.Position(call.Pos()), pkg.Name, call.Sel.Name)
				}
			}
			return true
		})
	}
}

// TestPayloadSummary_IsTerminalSafe: run get prints a payload a task wrote, so
// what a terminal could obey reaches it as text, cut or not. It stays the same
// JSON, and --json, which has its own writer, prints what it printed.
func TestPayloadSummary_IsTerminalSafe(t *testing.T) {
	const hostile = "x\x7f\u009b31m\u202e\U000e0041\x1b]0;x\x07\u2028"
	const escaped = `x\u007f\u009b31m\u202e\udb40\udc41\u001b]0;x\u0007\u2028`
	payload := func(value string) *orkstackv1.Payload {
		data, err := structpb.NewStruct(map[string]any{"k": value})
		if err != nil {
			t.Fatalf("struct: %v", err)
		}
		return &orkstackv1.Payload{Data: data}
	}
	short := payloadSummary(payload(hostile), "ork-1", "")
	requireTerminalSafe(t, "a short summary", short)
	if want := `{"k":"` + escaped + `"}`; short != want {
		t.Fatalf("summary = %s, want %s", short, want)
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(short), &decoded); err != nil || decoded["k"] != hostile {
		t.Fatalf("the summary decodes to %q, %v", decoded["k"], err)
	}
	long := payloadSummary(payload(strings.Repeat(hostile, 100)), "ork-1", "")
	requireTerminalSafe(t, "a cut summary", long)
	if !strings.Contains(long, "… (") {
		t.Fatalf("the summary was not cut: %s", long)
	}
	whole, err := marshalJSON(payload(hostile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(whole), escaped) {
		t.Fatalf("--json changed: %s", whole)
	}
}

// TestTaskRun_PrintsConfigTerminalSafe: the transcript shows a rendered
// config.json, which holds whatever an input held. What a terminal could obey
// prints as text, and the report behind --json keeps the file's own bytes.
func TestTaskRun_PrintsConfigTerminalSafe(t *testing.T) {
	// As an encoder writes it: the C0 controls are escaped, and these are not.
	raw := []byte("{\n  \"k\": \"x\x7f\u009b31m\u202e\U000e0041\",\n  \"plain\": \"日本語\"\n}\n")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	r := &taskRunner{t: &transcript{out: &out}, report: &taskRunReport{}, opts: &taskRunOptions{}}
	r.onSealed(dir)
	requireTerminalSafeOutput(t, "the transcript", out.String())
	for _, want := range []string{`"k": "x\u007f\u009b31m\u202e\U000e0041",`, `"plain": "日本語"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("transcript = %q, want it to hold %s", out.String(), want)
		}
	}
	if !bytes.Equal(r.report.Config, raw) {
		t.Fatalf("the report's config = %q, want the file's bytes", r.report.Config)
	}
}

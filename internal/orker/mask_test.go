package orker

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/logstream"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// scriptedRuntime is a container that prints what it is told, writes the
// values.json it is told, and exits as it is told: enough to drive one
// attempt through masking with no daemon.
type scriptedRuntime struct {
	stdout, stderr []string // each element is one Write
	values         string   // "" writes none
	exit           int32
	startErr       error

	mu      sync.Mutex
	creates int
}

func (r *scriptedRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", PlatformDigest: "sha256:" + repeat64('b'), Delivery: DeliveryRegistry}, nil
}

func (r *scriptedRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (r *scriptedRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.creates++
	r.mu.Unlock()
	if r.values == "" {
		return "scripted", nil
	}
	for _, m := range spec.Mounts {
		if m.Destination == ContainerOutput {
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(r.values), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "scripted", nil
}

func (r *scriptedRuntime) Start(_ context.Context, _ ContainerID, stdout, stderr io.Writer) error {
	for _, p := range r.stdout {
		_, _ = stdout.Write([]byte(p))
	}
	for _, p := range r.stderr {
		_, _ = stderr.Write([]byte(p))
	}
	return r.startErr
}

func (r *scriptedRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }
func (r *scriptedRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: r.exit}, nil
}
func (r *scriptedRuntime) Remove(context.Context, ContainerID) error     { return nil }
func (r *scriptedRuntime) Reap(context.Context, string) (int, error)     { return 0, nil }
func (r *scriptedRuntime) Describe(context.Context) (RuntimeInfo, error) { return RuntimeInfo{}, nil }

func (r *scriptedRuntime) created() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.creates
}

// maskedRun is everything one attempt let out: the live chunks, the export,
// the in-process observer, the stored streams, the failure, and the store.
type maskedRun struct {
	chunks, exported, observed string
	stdoutLog, stderrLog       string
	err                        error
	detail                     *orkstackv1.ActivityFailureDetail
	attempts                   *artifacts.MemoryAttemptStore
	in                         *orkstackv1.ExecuteTaskInput
	storeDir                   string
}

func (m maskedRun) everything() string {
	message := ""
	if m.err != nil {
		message = m.err.Error()
	}
	return strings.Join([]string{m.chunks, m.exported, m.observed, m.stdoutLog, m.stderrLog, message,
		strings.Join(m.detail.GetScanNotes(), "\n")}, "\n")
}

// stored reports whether any object in the artifact store holds s.
func (m maskedRun) stored(t *testing.T, s string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(m.storeDir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		found = found || strings.Contains(string(raw), s)
		return nil
	})
	must.OKf(t, err, "walk the store")
	return found
}

func runMasked(t *testing.T, runtime *scriptedRuntime, configure func(*OCIDispatcher), secrets ...*orkstackv1.SecretRef) maskedRun {
	t.Helper()
	attempts := artifacts.NewMemoryAttemptStore()
	d := newAttemptDispatcher(t, runtime, attempts)
	storeDir := t.TempDir()
	store, err := artifacts.NewLocalStore(storeDir)
	must.OKf(t, err, "local artifact store")
	d.Store = store
	logs := &recordingLogStore{}
	d.Logs = logs
	exporter := &recordingTaskLogExporter{}
	d.TaskLogs = exporter
	var observed strings.Builder
	var mu sync.Mutex
	d.LogSinkObserver = func(_ *orkstackv1.ExecuteTaskInput, line string) {
		mu.Lock()
		defer mu.Unlock()
		observed.WriteString(line + "\n")
	}
	if configure != nil {
		configure(d)
	}
	in := baseExportInput("masked")
	in.Secrets = secrets
	_, err = d.Execute(context.Background(), in)
	run := maskedRun{err: err, attempts: attempts, in: in, storeDir: storeDir}
	run.chunks = strings.Join(linesOf(t, logs), "\n")
	_, events := exporter.recorded()
	for _, e := range events {
		run.exported += e.write + "\n"
	}
	mu.Lock()
	run.observed = observed.String()
	mu.Unlock()
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		must.OKf(t, appErr.Details(&run.detail), "failure detail")
		for _, ref := range run.detail.GetSystemArtifacts() {
			body, err := d.Store.Get(context.Background(), ref.GetUri())
			must.OKf(t, err, "read %s", ref.GetName())
			raw, err := io.ReadAll(body)
			_ = body.Close()
			must.OKf(t, err, "read %s", ref.GetName())
			switch ref.GetName() {
			case StdoutLogName:
				run.stdoutLog = string(raw)
			case StderrLogName:
				run.stderrLog = string(raw)
			}
		}
	}
	return run
}

func envSecret(t *testing.T, name, value string) *orkstackv1.SecretRef {
	t.Helper()
	variable := "ORKSTACK_TEST_MASK_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	t.Setenv(variable, value)
	return &orkstackv1.SecretRef{Name: name, Source: "env:" + variable}
}

func splitBytes(s string) []string {
	out := make([]string, len(s))
	for i := range s {
		out[i] = s[i : i+1]
	}
	return out
}

func isNonRetryable(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.NonRetryable()
}

// TestDispatcher_MasksASecretEverywhereTheTasksOutputGoes: a task that prints
// its secret, its base64 and its hex on stdout, and the secret one byte per
// write on stderr, leaves none of them in the live chunks, the export, the
// observer, stdout.log, stderr.log or the failure, and the attempt's record
// counts what was replaced.
func TestDispatcher_MasksASecretEverywhereTheTasksOutputGoes(t *testing.T) {
	value := mintSecret(t)
	runtime := &scriptedRuntime{
		stdout: []string{
			"token " + value + "\n",
			"b64 " + base64.StdEncoding.EncodeToString([]byte(value)) + "\n",
			"hex " + strings.ToUpper(hex.EncodeToString([]byte(value))) + "\n",
		},
		stderr: append(splitBytes("again "+value), "\n", "tail "+value[:20]),
		exit:   1,
	}
	run := runMasked(t, runtime, nil, envSecret(t, "token", value))
	everything := run.everything()
	if strings.Contains(everything, value[:12]) {
		t.Fatalf("the secret, or most of it, left the attempt:\n%s", everything)
	}
	for _, form := range logstream.MaskForms([]byte(value)) {
		if strings.Contains(everything, string(form)) {
			t.Fatalf("form %q left the attempt", form)
		}
	}
	for _, want := range []string{"token ***", "b64 ***", "hex ***"} {
		if !strings.Contains(run.stdoutLog, want) || !strings.Contains(run.chunks, want) ||
			!strings.Contains(run.exported, want) || !strings.Contains(run.observed, want) {
			t.Fatalf("want %q in every place:\nstdout.log %q\nchunks %q", want, run.stdoutLog, run.chunks)
		}
	}
	if run.stderrLog != "again ***\ntail ***" {
		t.Fatalf("stderr.log = %q; the held tail is masked at the end", run.stderrLog)
	}
	if run.stored(t, value) {
		t.Fatal("an object in the store holds the value")
	}
	records, err := run.attempts.ListAttempts(context.Background(), run.in.GetRunId(), run.in.GetTaskId(), artifacts.NoItem)
	must.OKf(t, err, "list attempts")
	if len(records) != 1 || records[0].GetMaskedOccurrences() != 5 {
		t.Fatalf("records = %v; want one with masked_occurrences 5", records)
	}
}

// TestDispatcher_WhatIsNotMaskedIsCapturedByteExact: a task with no secret, a
// secret that says mask: false, and an Orker that does not mask.
func TestDispatcher_WhatIsNotMaskedIsCapturedByteExact(t *testing.T) {
	value := mintSecret(t)
	output := "plain " + value + "\nno newline at the end " + value[:10]
	tests := []struct {
		name      string
		configure func(*OCIDispatcher)
		secrets   func(t *testing.T) []*orkstackv1.SecretRef
	}{
		{"no secret", nil, func(*testing.T) []*orkstackv1.SecretRef { return nil }},
		{"mask false", nil, func(t *testing.T) []*orkstackv1.SecretRef {
			s := envSecret(t, "token", value)
			s.Unmasked = true
			return []*orkstackv1.SecretRef{s}
		}},
		{"masking off", func(d *OCIDispatcher) { d.LogUnmasked = true },
			func(t *testing.T) []*orkstackv1.SecretRef {
				return []*orkstackv1.SecretRef{envSecret(t, "token", value)}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &scriptedRuntime{stdout: []string{output}, exit: 1}
			run := runMasked(t, runtime, tt.configure, tt.secrets(t)...)
			if run.stdoutLog != output {
				t.Fatalf("stdout.log = %q; want the bytes the task wrote", run.stdoutLog)
			}
		})
	}
}

// TestDispatcher_TwoSecretsOfOneValueAreMaskedUnlessBothSayNot: a value is
// unmasked only when every secret holding it says mask: false.
func TestDispatcher_TwoSecretsOfOneValueAreMaskedUnlessBothSayNot(t *testing.T) {
	value := mintSecret(t)
	plain := envSecret(t, "plain", value)
	plain.Unmasked = true
	run := runMasked(t, &scriptedRuntime{stdout: []string{value + "\n"}, exit: 1}, nil, plain, envSecret(t, "hidden", value))
	if strings.Contains(run.everything(), value) {
		t.Fatal("a value one of its secrets masks was left unmasked")
	}
}

// TestDispatcher_AShortSecretIsNotMaskedAndTheLogSaysSo names the secret and
// never its length or value.
func TestDispatcher_AShortSecretIsNotMaskedAndTheLogSaysSo(t *testing.T) {
	run := runMasked(t, &scriptedRuntime{stdout: []string{"pin 4321\n"}, exit: 1}, nil, envSecret(t, "pin", "4321"))
	if !strings.Contains(run.chunks, `[ork] secret "pin" is shorter than 8 bytes and is not masked in logs`) {
		t.Fatalf("chunks = %q; want the [ork] line", run.chunks)
	}
	if !strings.Contains(run.chunks, `[ork] secret "pin" is shorter than 8 bytes and is not looked for in values.json`) {
		t.Fatalf("chunks = %q; want the values check's [ork] line too", run.chunks)
	}
	if !strings.Contains(run.stdoutLog, "pin 4321") {
		t.Fatalf("stdout.log = %q; a short value is not masked", run.stdoutLog)
	}
}

// TestDispatcher_AFailureQuotingTheTasksStderrIsMasked: the runtime's own
// words, which can carry a task's stderr, leave masked.
func TestDispatcher_AFailureQuotingTheTasksStderrIsMasked(t *testing.T) {
	value := mintSecret(t)
	runtime := &scriptedRuntime{startErr: fmt.Errorf("exec failed: stderr: %s", value)}
	run := runMasked(t, runtime, nil, envSecret(t, "token", value))
	if run.err == nil {
		t.Fatal("want the start to fail")
	}
	if strings.Contains(run.everything(), value) || !strings.Contains(run.err.Error(), "stderr: ***") {
		t.Fatalf("failure = %v; want the value masked", run.err)
	}
}

// TestDispatcher_SecretsPastTheMaskersBoundsStartNoContainer: refused
// MASK_CAPACITY_EXCEEDED before the container exists, naming the largest
// secrets by name and size, never a value.
func TestDispatcher_SecretsPastTheMaskersBoundsStartNoContainer(t *testing.T) {
	var v strings.Builder
	for j := 0; v.Len() < 100<<10; j++ {
		// Distinct lines of a value over 64 KiB, which is registered by its
		// lines only: more of them than one masker holds.
		fmt.Fprintf(&v, "secret-line-%06d-%s\n", j, strings.Repeat("x", 40))
	}
	runtime := &scriptedRuntime{values: `{}`}
	// The small one first: the refusal names the largest whatever the order.
	run := runMasked(t, runtime, nil, envSecret(t, "small", mintSecret(t)), envSecret(t, "big", v.String()))
	if run.err == nil || !strings.Contains(run.err.Error(), ReasonMaskCapacityExceeded) {
		t.Fatalf("err = %v; want %s", run.err, ReasonMaskCapacityExceeded)
	}
	if runtime.created() != 0 {
		t.Fatal("a container was created for an attempt that cannot be masked")
	}
	if !strings.Contains(run.err.Error(), `secret "big" (`) || strings.Contains(run.err.Error(), "line-000001") {
		t.Fatalf("err = %v; want the largest named and no value", run.err)
	}
	if !isNonRetryable(run.err) {
		t.Fatalf("err = %v; want a non-retryable failure", run.err)
	}
}

// TestDispatcher_AResultHoldingASecretFails: VALUES_CONTAIN_SECRET, not
// retried, before anything is uploaded; it names the secret and where, by
// position, and never the value.
func TestDispatcher_AResultHoldingASecretFails(t *testing.T) {
	value := "p\"a/s<s\\wörd-" + mintSecret(t)[:16]
	escaped := strings.NewReplacer(`"`, `\"`, `\`, `\\`, `/`, `\/`, `<`, `<`, `ö`, `ö`).Replace(value)
	multi := "first-line-of-it\nsecond-line-of-it"
	tests := []struct {
		name, values, path string
	}{
		{"the value, escaped as no encoder would", `{"a":1,"b":"` + escaped + `"}`, "$.#2"},
		{"its base64", `{"a":["x","` + base64.StdEncoding.EncodeToString([]byte(value)) + `"]}`, "$.#1[1]"},
		{"a key", `{"ok":true,"` + escaped + `":1}`, "$.#2"},
		{"one line of a multi-line secret", `{"x":{"y":"second-line-of-it"}}`, "$.#1.#1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &scriptedRuntime{values: tt.values}
			run := runMasked(t, runtime, nil, envSecret(t, "db", value), envSecret(t, "cert", multi))
			if run.err == nil || !strings.Contains(run.err.Error(), ReasonValuesContainSecret) {
				t.Fatalf("err = %v; want %s", run.err, ReasonValuesContainSecret)
			}
			if !isNonRetryable(run.err) {
				t.Fatal("VALUES_CONTAIN_SECRET was left retryable")
			}
			if !strings.Contains(run.err.Error(), "(at "+tt.path+")") {
				t.Fatalf("err = %v; want it found at %s", run.err, tt.path)
			}
			if strings.Contains(run.everything(), value) || strings.Contains(run.err.Error(), "second-line") {
				t.Fatalf("the value left the attempt: %v", run.err)
			}
			if run.stored(t, "second-line-of-it") || run.stored(t, base64.StdEncoding.EncodeToString([]byte(value))) ||
				run.stored(t, `"b":"p`) {
				t.Fatal("an object in the store holds the result")
			}
		})
	}
}

// TestDispatcher_AResultThatIsNoSecretPasses: the secret's name, a secret that
// says mask: false, and an Orker that does not refuse; and the check still
// runs where logs are not masked.
func TestDispatcher_AResultThatIsNoSecretPasses(t *testing.T) {
	value := mintSecret(t)
	inValues := `{"secret":"token","v":"` + value + `"}`
	tests := []struct {
		name      string
		values    string
		configure func(*OCIDispatcher)
		unmasked  bool
		wantFail  bool
	}{
		{"the name only", `{"secret":"token"}`, nil, false, false},
		{"mask false", inValues, nil, true, false},
		{"the check off", inValues, func(d *OCIDispatcher) { d.AllowSecretValues = true }, false, false},
		{"masking off, the check on", inValues, func(d *OCIDispatcher) { d.LogUnmasked = true }, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := envSecret(t, "token", value)
			secret.Unmasked = tt.unmasked
			run := runMasked(t, &scriptedRuntime{values: tt.values}, tt.configure, secret)
			failed := run.err != nil && strings.Contains(run.err.Error(), ReasonValuesContainSecret)
			if failed != tt.wantFail {
				t.Fatalf("err = %v; want refused %v", run.err, tt.wantFail)
			}
			if !tt.wantFail {
				must.OKf(t, run.err, "the attempt")
			}
		})
	}
}

// TestDispatcher_ACheckThatCannotBeBuiltFailsTheAttempt: the check fails
// closed, never passed by being skipped.
func TestDispatcher_ACheckThatCannotBeBuiltFailsTheAttempt(t *testing.T) {
	calls := 0
	failSecond := func(d *OCIDispatcher) {
		d.valuesFinder = func(values []logstream.NamedValue) (*logstream.SecretFinder, error) {
			calls++
			if calls > 1 {
				return nil, &logstream.MaskCapacityError{Forms: 2000, Bytes: 1, MaxForms: 1024, MaxBytes: 1}
			}
			return logstream.NewSecretFinder(values)
		}
	}
	run := runMasked(t, &scriptedRuntime{values: `{"ok":true}`}, failSecond, envSecret(t, "token", mintSecret(t)))
	if calls != 2 {
		t.Fatalf("the check's automaton was built %d times; want at setup and at the check", calls)
	}
	if run.err == nil || !strings.Contains(run.err.Error(), ReasonMaskCapacityExceeded) {
		t.Fatalf("err = %v; want the attempt failed %s", run.err, ReasonMaskCapacityExceeded)
	}
}

// TestFindSecretInValues_Positions: the path is array indexes and member
// ordinals, never a key's text.
func TestFindSecretInValues_Positions(t *testing.T) {
	finder, err := logstream.NewSecretFinder([]logstream.NamedValue{{Name: "s", Value: []byte("needle-in-json")}})
	must.OKf(t, err, "finder")
	tests := []struct {
		raw, path string
	}{
		{`{"needle-in-json":1}`, "$.#1"},
		{`{"a":1,"b":[0,{"c":"needle-in-json"}]}`, "$.#2[1].#1"},
		{`{"a":{"b":{}},"c":["x","y","needle-in-json"]}`, "$.#2[2]"},
		{`{"a":"nothing here"}`, ""},
	}
	for _, tt := range tests {
		name, path, failure := findSecretInValues(finder, []byte(tt.raw))
		if failure != nil {
			t.Fatalf("%s: %v", tt.raw, failure)
		}
		if path != tt.path || (tt.path != "") != (name == "s") {
			t.Fatalf("%s: found %q at %q; want %q", tt.raw, name, path, tt.path)
		}
	}
}

// FuzzFindSecretInValues: the check reads any document without panicking; a
// path is made only of $, indexes and ordinals; a secret written as a whole
// string value is always found.
func FuzzFindSecretInValues(f *testing.F) {
	f.Add(`{"a":1}`, "secret-value-1")
	f.Add(`{"a":["x",{"b":null}],"c":true}`, "another-secret")
	f.Add(`{"A":"é "}`, "é-and-more")
	f.Add(`{"needle-in-json":1}`, "needle-in-json")
	f.Fuzz(func(t *testing.T, doc, secret string) {
		finder, err := logstream.NewSecretFinder([]logstream.NamedValue{{Name: "s", Value: []byte(secret)}})
		if err != nil {
			return
		}
		check := func(raw []byte) string {
			name, path, failure := findSecretInValues(finder, raw)
			if failure != nil {
				return ""
			}
			for _, r := range strings.TrimPrefix(path, "$") {
				if !strings.ContainsRune("0123456789[].#", r) {
					t.Fatalf("path %q holds more than positions", path)
				}
			}
			return name
		}
		check([]byte(doc))
		if len(logstream.MaskForms([]byte(secret))) == 0 {
			return
		}
		quoted, err := jsontext.AppendQuote(nil, secret)
		if err != nil {
			return
		}
		if name := check([]byte(`{"k":` + string(quoted) + `}`)); name != "s" {
			t.Fatalf("a secret written as a string value was not found: %s", quoted)
		}
	})
}

// TestAttemptMask_AFailuresCauseIsMaskedAndStillMatches: the cause's text is
// masked, and errors.Is still reaches what it wraps.
func TestAttemptMask_AFailuresCauseIsMaskedAndStillMatches(t *testing.T) {
	value := mintSecret(t)
	m := (&OCIDispatcher{}).newAttemptMask(nil)
	must.OKf(t, nilFailure(m.register([]secretValue{{name: "s", value: []byte(value)}})), "register")
	sentinel := errors.New("the daemon refused")
	f := m.failure(FailCause(ReasonRuntimeRefused, fmt.Errorf("%w: stderr %s", sentinel, value), "start: %s", value))
	if strings.Contains(f.Message, value) || strings.Contains(f.Cause.Error(), value) {
		t.Fatalf("failure %q, cause %q: the value left unmasked", f.Message, f.Cause)
	}
	if !errors.Is(f, sentinel) {
		t.Fatal("errors.Is no longer reaches the cause")
	}
	if f := (*attemptMask)(nil).register([]secretValue{{name: "s", value: []byte(value)}}); f == nil {
		t.Fatal("an attempt with secrets and no masker was not refused")
	}
}

func nilFailure(f *Failure) error {
	if f == nil {
		return nil
	}
	return f
}

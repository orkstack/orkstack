package main

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// The six §10.3 assertion names, exactly as activitySuite reports them.
//
// They are duplicated here on purpose. A test that read the names out of the
// implementation would still pass if every name were replaced by the empty
// string, and "the report NAMES the assertion that failed" is precisely the
// property Phase 3 acceptance criterion (6) turns on.
//
// TestAssertionNamesMatchTheImplementation keeps the duplication honest.
const (
	nameStartsAndWritesValues = "starts, writes values.json, and its stdout reaches the stream"
	nameInputIsReadOnly       = "/orkstack/input is read-only to the workload"
	nameTaskJSONIsValid       = "task.json is present and complete against the §3.2 table"
	nameArtifactNames         = "artifact names round-trip, and a directory is refused at the scan"
	nameExitCodes             = "every non-zero exit fails the activity, and none is magic"
	nameCancellation          = "SIGTERM reaches the workload within the grace period"
)

func sixAssertions() []assertion {
	return []assertion{
		{name: nameStartsAndWritesValues, detail: "values.json committed"},
		{name: nameInputIsReadOnly, detail: "a write into /orkstack/input was refused"},
		{name: nameTaskJSONIsValid, detail: "every §3.2 row present"},
		{name: nameArtifactNames, detail: "an invalid name was rejected at the scan"},
		{name: nameExitCodes, detail: "0/1/42/125/126/127 reported verbatim"},
		{name: nameCancellation, detail: "the workload's TERM trap fired"},
	}
}

// TestAssertionNamesMatchTheImplementation stops the constants above drifting
// from the names activitySuite actually reports.
//
// The six assertion methods each need a container runtime to run, so the names
// cannot be obtained by calling them. Reading the source is the cheap honest
// alternative: if somebody rewords an assertion and not this file, the tests
// that check "the FAIL row names the right assertion" would keep passing while
// checking a name no user ever sees.
func TestAssertionNamesMatchTheImplementation(t *testing.T) {
	t.Parallel()
	source, err := os.ReadFile("activity_test_cmd.go")
	if err != nil {
		t.Fatalf("read the implementation: %v", err)
	}
	for _, name := range []string{
		nameStartsAndWritesValues,
		nameInputIsReadOnly,
		nameTaskJSONIsValid,
		nameArtifactNames,
		nameExitCodes,
		nameCancellation,
	} {
		if !bytes.Contains(source, []byte(`assertion{name: "`+name+`"}`)) {
			t.Errorf("activity_test_cmd.go declares no assertion named %q;\n"+
				"the constant in this file has drifted from what the command reports", name)
		}
	}
}
func TestAssertionStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a    assertion
		want string
	}{
		{"a clean assertion passes", assertion{name: "x"}, "PASS"},
		{"an error is a FAIL", assertion{name: "x", err: errors.New("boom")}, "FAIL"},
		{"a skip is a SKIP, never a PASS", assertion{name: "x", skipped: true}, "SKIP"},
		{
			"a skip wins over an error, so an unexpressible property is never an accusation",
			assertion{name: "x", skipped: true, err: errors.New("boom")},
			"SKIP",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.a.status(); got != tt.want {
				t.Errorf("status() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBuildActivityTestReport_NamesTheAssertionThatFailed is the fail half of
// Phase 3 acceptance criterion (6): `orkctl activity test` must report
// pass/fail, and a FAIL must say WHICH of the six properties broke.
//
// One case per assertion, because a report that named only the first failure,
// or that reported a generic "contract failed", would satisfy a weaker test
// and would be useless to somebody holding a broken image.
func TestBuildActivityTestReport_NamesTheAssertionThatFailed(t *testing.T) {
	t.Parallel()
	for i, want := range []string{
		nameStartsAndWritesValues,
		nameInputIsReadOnly,
		nameTaskJSONIsValid,
		nameArtifactNames,
		nameExitCodes,
		nameCancellation,
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()
			assertions := sixAssertions()
			assertions[i].err = errors.New("the image did not satisfy this property")
			assertions[i].detail = ""

			report := buildActivityTestReport("example.test/img@sha256:abc", assertions)

			if report.OK {
				t.Error("OK = true; one failing assertion must make the whole report fail")
			}
			if report.Failed != 1 {
				t.Errorf("Failed = %d, want 1", report.Failed)
			}
			if report.Passed != 5 {
				t.Errorf("Passed = %d, want 5", report.Passed)
			}
			if report.Skipped != 0 {
				t.Errorf("Skipped = %d, want 0", report.Skipped)
			}
			if len(report.Assertions) != 6 {
				t.Fatalf("reported %d assertions, want 6", len(report.Assertions))
			}

			var failed []activityTestOutcome
			for _, a := range report.Assertions {
				if a.Status == "FAIL" {
					failed = append(failed, a)
				}
			}
			if len(failed) != 1 {
				t.Fatalf("%d FAIL rows, want exactly 1", len(failed))
			}
			if failed[0].Name != want {
				t.Errorf("the FAIL row names %q, want %q", failed[0].Name, want)
			}
			if failed[0].Error == "" {
				t.Error("the FAIL row carries no error; a FAIL that says nothing explains nothing")
			}
		})
	}
}

func TestBuildActivityTestReport_Counts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                             string
		mutate                           func([]assertion)
		wantPassed, wantFailed, wantSkip int
		wantOK                           bool
	}{
		{
			name:       "all six pass",
			mutate:     func([]assertion) {},
			wantPassed: 6, wantFailed: 0, wantSkip: 0, wantOK: true,
		},
		{
			name: "two fail",
			mutate: func(a []assertion) {
				a[1].err = errors.New("no")
				a[4].err = errors.New("no")
			},
			wantPassed: 4, wantFailed: 2, wantSkip: 0, wantOK: false,
		},
		{
			name: "a skip is not a pass and is not a failure",
			mutate: func(a []assertion) {
				a[3].skipped = true
			},
			wantPassed: 5, wantFailed: 0, wantSkip: 1, wantOK: true,
		},
		{
			name: "a skip beside a failure still fails the report",
			mutate: func(a []assertion) {
				a[0].skipped = true
				a[2].err = errors.New("no")
			},
			wantPassed: 4, wantFailed: 1, wantSkip: 1, wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertions := sixAssertions()
			tt.mutate(assertions)
			report := buildActivityTestReport("img", assertions)
			if report.Passed != tt.wantPassed {
				t.Errorf("Passed = %d, want %d", report.Passed, tt.wantPassed)
			}
			if report.Failed != tt.wantFailed {
				t.Errorf("Failed = %d, want %d", report.Failed, tt.wantFailed)
			}
			if report.Skipped != tt.wantSkip {
				t.Errorf("Skipped = %d, want %d", report.Skipped, tt.wantSkip)
			}
			if report.OK != tt.wantOK {
				t.Errorf("OK = %v, want %v", report.OK, tt.wantOK)
			}
		})
	}
}

// TestRenderActivityTestReport_ShowsTheFailingAssertion checks the surface a
// human actually reads. The JSON form is covered above; this is the table.
func TestRenderActivityTestReport_ShowsTheFailingAssertion(t *testing.T) {
	t.Parallel()
	assertions := sixAssertions()
	assertions[0].err = errors.New("the attempt succeeded but committed no values.json")
	assertions[0].detail = ""
	report := buildActivityTestReport("localhost:5001/orkstack/bad:dev", assertions)

	var out bytes.Buffer
	renderActivityTestReport(&out, report)
	got := out.String()

	for _, want := range []string{
		"localhost:5001/orkstack/bad:dev",
		"FAIL",
		nameStartsAndWritesValues,
		"the attempt succeeded but committed no values.json",
		"5 passed, 1 failed, 0 skipped",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered report does not contain %q\n---\n%s", want, got)
		}
	}
	if strings.Contains(got, "satisfies the OCI Activity Contract") {
		t.Errorf("a failing report claims the image satisfies the contract:\n%s", got)
	}
}

func TestRenderActivityTestReport_PassingReportSaysSo(t *testing.T) {
	t.Parallel()
	report := buildActivityTestReport("good:dev", sixAssertions())

	var out bytes.Buffer
	renderActivityTestReport(&out, report)
	got := out.String()

	if !strings.Contains(got, "6 passed, 0 failed, 0 skipped") {
		t.Errorf("missing the tally:\n%s", got)
	}
	if !strings.Contains(got, "good:dev satisfies the OCI Activity Contract.") {
		t.Errorf("a clean report does not say the image satisfies the contract:\n%s", got)
	}
	if strings.Contains(got, "FAIL") {
		t.Errorf("a clean report contains a FAIL row:\n%s", got)
	}
}

// TestPreflightIsContractViolation guards the fail half of this command from
// becoming unreachable.
//
// preflight exists to forgive an image that speaks a different config contract
// (activity-http takes a url, activity-file takes a src) rather than accuse it
// of six violations. It must NOT forgive every failed preflight: an image that
// accepted the config, exited 0, and wrote no values.json is a contract
// violation, and forgiving it would make `orkctl activity test` on the
// canonical broken image return a bare error with no report and no FAIL row
// at all.
//
// The exit code is what separates the two: a workload that exits 0 has stated
// the work succeeded, and §3.4 then requires values.json unconditionally.
func TestPreflightIsContractViolation(t *testing.T) {
	t.Parallel()

	// The shape OCIDispatcher.Execute actually returns: a Temporal
	// ApplicationError built by Failure.TemporalError(). Constructing it
	// through the real constructor is the point — an earlier version of this
	// decision matched on *orker.Failure, which errors.As never finds in this
	// chain, so the fix silently did nothing against a live daemon.
	viaTemporal := func(reason string, exit int32) error {
		return orker.Fail(reason, "constructed for the test").
			WithDetail(&orkstackv1.ActivityFailureDetail{ExitCode: exit}).
			TemporalError()
	}
	raw := func(reason string, exit int32) error {
		return orker.Fail(reason, "constructed for the test").
			WithDetail(&orkstackv1.ActivityFailureDetail{ExitCode: exit})
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "exit 0 with no values.json is the image breaking the ABI floor",
			err:  viaTemporal(orker.ReasonValuesMissing, 0),
			want: true,
		},
		{
			name: "exit 0 with malformed values.json is likewise the image's fault",
			err:  viaTemporal(orker.ReasonValuesMalformed, 0),
			want: true,
		},
		{
			name: "activity-http refusing an unknown op exits 2 — not a violation",
			err:  viaTemporal(orker.ReasonContainerExitNonzero, 2),
			want: false,
		},
		{
			name: "activity-file's fail() exits 1 — not a violation",
			err:  viaTemporal(orker.ReasonContainerExitNonzero, 1),
			want: false,
		},
		{
			name: "a contract violation AFTER a non-zero exit is still a refusal",
			err:  viaTemporal(orker.ReasonValuesMissing, 1),
			want: false,
		},
		{
			name: "an infrastructure failure is the host's problem, not the image's contract",
			err:  viaTemporal(orker.ReasonImagePullFailed, 0),
			want: false,
		},
		{
			name: "a raw Failure — what the pre-runtime refusals return — is read too",
			err:  raw(orker.ReasonValuesMissing, 0),
			want: true,
		},
		{
			name: "a raw Failure after a non-zero exit is still a refusal",
			err:  raw(orker.ReasonContainerExitNonzero, 3),
			want: false,
		},
		{
			name: "a plain error carries no reason code and cannot be judged",
			err:  errors.New("something else went wrong"),
			want: false,
		},
		{
			name: "no error at all is not a violation",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := preflightIsContractViolation(tt.err); got != tt.want {
				t.Errorf("preflightIsContractViolation() = %v, want %v", got, tt.want)
			}
		})
	}
}

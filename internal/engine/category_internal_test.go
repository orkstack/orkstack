package engine

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// temporalErrorReader is a selector of the SDK's temporal package that reads
// an error: one of its error types, or an Is…Error predicate. Building one
// (New…Error) is not reading one.
var temporalErrorReader = regexp.MustCompile(`^(Is[A-Z]\w*Error|[A-Z]\w*Error)$`)

// TestCategoryOf_NoOtherReader: categoryOf is the one reader of a failure the
// engine's Temporal calls return (E92 §4.7). No other file of the engine's
// code names an SDK error type or an Is…Error predicate, or imports the raw
// failure proto, so a new kind of failure is classified in one place and every
// reader sees it the same way.
func TestCategoryOf_NoOtherReader(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob the engine's files: %v (%d found)", err, len(paths))
	}
	fset := token.NewFileSet()
	var readers []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") || path == "category.go" {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		temporalName := ""
		for _, spec := range file.Imports {
			importPath, _ := strconv.Unquote(spec.Path.Value)
			switch importPath {
			case "go.temporal.io/sdk/temporal":
				temporalName = "temporal"
				if spec.Name != nil {
					temporalName = spec.Name.Name
				}
				if temporalName == "." {
					// A dot-import would hide every reader from the check below.
					readers = append(readers, fset.Position(spec.Pos()).String()+": dot-imports the temporal package")
				}
			case "go.temporal.io/api/failure/v1":
				readers = append(readers, fset.Position(spec.Pos()).String()+": imports the raw failure proto")
			}
		}
		if temporalName == "" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == temporalName &&
				temporalErrorReader.MatchString(sel.Sel.Name) && !strings.HasPrefix(sel.Sel.Name, "New") {
				readers = append(readers, fset.Position(sel.Pos()).String()+": "+temporalName+"."+sel.Sel.Name)
			}
			return true
		})
	}
	sort.Strings(readers)
	if len(readers) > 0 {
		t.Fatalf("read a failure through categoryOf (category.go), not here:\n  %s", strings.Join(readers, "\n  "))
	}
}

// TestCategoryOf_ReadsEachKindOfFailure: what categoryOf reads off each shape
// of error a workflow receives, the wire forms built through the SDK's own
// failure converter, as retrystate_internal_test.go does.
func TestCategoryOf_ReadsEachKindOfFailure(t *testing.T) {
	detail := &orkstackv1.ActivityFailureDetail{ReasonCode: "CONTAINER_EXIT_NONZERO", OrkerId: "orker-a"}
	detailPayloads, err := converter.GetDefaultDataConverter().ToPayloads(detail)
	if err != nil {
		t.Fatal(err)
	}
	wire := func(f *failurepb.Failure) error { return temporal.GetDefaultFailureConverter().FailureToError(f) }
	appFailure := func(typ string, nonRetryable bool, details *commonpb.Payloads) *failurepb.Failure {
		return &failurepb.Failure{Message: "workload exited 1", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: typ, NonRetryable: nonRetryable, Details: details},
		}}
	}
	activity := func(cause *failurepb.Failure) error {
		return wire(&failurepb.Failure{Message: "activity error", Cause: cause, FailureInfo: &failurepb.Failure_ActivityFailureInfo{
			ActivityFailureInfo: &failurepb.ActivityFailureInfo{ActivityId: "1", ActivityType: &commonpb.ActivityType{Name: "ExecuteTask"},
				RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED},
		}})
	}
	type want struct {
		appType, message, reason, childInner string
		nonRetryable, viaActivity, viaChild  bool
		canceled, terminated                 bool
		timeout                              enumspb.TimeoutType
		retryState                           orkstackv1.RetryState
	}
	cases := []struct {
		name string
		err  error
		want want
	}{
		{"nil", nil, want{}},
		{"a plain error", errors.New("boom"), want{}},
		{"an activity's failure with its detail", activity(appFailure(task.CategoryActivityFailed, true, detailPayloads)), want{
			appType: task.CategoryActivityFailed, message: "workload exited 1", reason: "CONTAINER_EXIT_NONZERO",
			nonRetryable: true, viaActivity: true, retryState: orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		}},
		{"an activity's heartbeat timeout", activity(&failurepb.Failure{Message: "heartbeat", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT},
		}}), want{viaActivity: true, timeout: enumspb.TIMEOUT_TYPE_HEARTBEAT, retryState: orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED}},
		{"a cancellation", temporal.NewCanceledError(), want{canceled: true}},
		{"a child run's failure", wire(&failurepb.Failure{Message: "child workflow execution error", Cause: appFailure(RunHistoryLimitCode, true, nil),
			FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}}}),
			// childInner is the inner error's own text, the SDK's decoration
			// included, as childInnerMessage has always quoted it.
			want{appType: RunHistoryLimitCode, message: "workload exited 1", nonRetryable: true, viaChild: true,
				childInner: "workload exited 1 (type: RUN_HISTORY_LIMIT, retryable: false)"}},
		{"a terminated child run", wire(&failurepb.Failure{Message: "child workflow execution error",
			Cause:       &failurepb.Failure{Message: "terminated", FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}},
			FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}}}),
			want{terminated: true, viaChild: true, childInner: "terminated"}},
		// The run's own error, in-process, whose details are a Go string: read
		// without decoding them as an activity's detail.
		{"the history guard's own error", temporal.NewNonRetryableApplicationError("history full", RunHistoryLimitCode, nil, "blockers"),
			want{appType: RunHistoryLimitCode, message: "history full", nonRetryable: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := categoryOf(tc.err)
			got := want{
				appType: r.appType(), message: r.appMessage(), childInner: r.childInner,
				nonRetryable: r.nonRetryable(), viaActivity: r.viaActivity, viaChild: r.viaChild,
				canceled: r.canceled, terminated: r.terminated, timeout: r.timeoutType(), retryState: r.retryState,
			}
			if tc.want.reason != "" {
				got.reason = r.reasonCode()
			}
			if got != tc.want {
				t.Fatalf("categoryOf = %+v\nwant          %+v", got, tc.want)
			}
		})
	}
	var blockers string
	if !categoryOf(temporal.NewNonRetryableApplicationError("history full", RunHistoryLimitCode, nil, "blockers")).appDetails(&blockers) || blockers != "blockers" {
		t.Fatalf("the guard's details = %q, want blockers", blockers)
	}
	if detail, ok := categoryOf(activity(appFailure(task.CategoryActivityFailed, false, detailPayloads))).activityDetail(); !ok || detail.GetOrkerId() != "orker-a" {
		t.Fatalf("activityDetail = %v, %v; want orker-a's detail", detail, ok)
	}
}

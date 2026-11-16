package engine

import (
	"testing"

	"google.golang.org/protobuf/types/dynamicpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestMessageName_NamesEveryTargetTheSDKDecodesIntoWithoutPanicking: the
// shapes the SDK hands a payload converter are named, and a target with no
// name to give (a nil dynamicpb.Message, a non-pointer, nil) is not an
// answer — never a panic in the engine's own code.
func TestMessageName_NamesEveryTargetTheSDKDecodesIntoWithoutPanicking(t *testing.T) {
	var nilDetail *orkstackv1.ActivityFailureDetail
	var nilDynamic *dynamicpb.Message
	for _, tc := range []struct {
		name   string
		target any
		want   string
	}{
		{"pointer to a message value", &orkstackv1.ExecuteTaskResult{}, "orkstack.v1.ExecuteTaskResult"},
		{"pointer to a nil message pointer", &nilDetail, "orkstack.v1.ActivityFailureDetail"},
		{"pointer to a set message pointer", func() any { m := &orkstackv1.DescribeOrkerResult{}; return &m }(), "orkstack.v1.DescribeOrkerResult"},
		{"pointer to a nil dynamic message", &nilDynamic, ""},
		{"a non-pointer", orkstackv1.ExecuteTaskResult{}, ""},
		{"nil", nil, ""},
		{"a pointer to a string", new(string), ""},
	} {
		got, ok := messageName(tc.target)
		if string(got) != tc.want || ok != (tc.want != "") {
			t.Errorf("%s: messageName = %q, %v; want %q", tc.name, got, ok, tc.want)
		}
	}
}

package engine

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
)

func TestClassifyWorkflowExists(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		err     error
		exists  bool
		wantErr bool
	}{
		{name: "visible", exists: true},
		{name: "execution gone", err: serviceerror.NewNotFound("gone")},
		{name: "namespace gone is not execution absence", err: serviceerror.NewNamespaceNotFound("gone"), wantErr: true},
		{name: "deadline is not absence", err: context.DeadlineExceeded, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exists, err := classifyWorkflowExists(testCase.err)
			if exists != testCase.exists || (err != nil) != testCase.wantErr {
				t.Fatalf("classifyWorkflowExists(%v) = (%v, %v)", testCase.err, exists, err)
			}
			if testCase.wantErr && !errors.Is(err, testCase.err) {
				t.Fatalf("error = %v, want %v", err, testCase.err)
			}
		})
	}
}

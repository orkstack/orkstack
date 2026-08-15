package engine_test

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

func TestApplyInputDefaults_MergesDefaultsWithoutChangingCallerValues(t *testing.T) {
	declarations, err := structpb.NewStruct(map[string]any{
		"url":      map[string]any{"type": "string", "default": "https://default.test"},
		"count":    map[string]any{"type": "number", "default": float64(3)},
		"optional": map[string]any{"type": "string"},
	})
	if err != nil {
		t.Fatalf("declarations: %v", err)
	}
	wf := &orkstackv1.Orkflow{Inputs: declarations}

	t.Run("omitted values receive defaults", func(t *testing.T) {
		got := engine.ApplyInputDefaults(wf, &orkstackv1.Payload{})
		if got.GetData().AsMap()["url"] != "https://default.test" {
			t.Errorf("url = %v, want declared default", got.GetData().AsMap()["url"])
		}
		if got.GetData().AsMap()["count"] != float64(3) {
			t.Errorf("count = %v, want declared default", got.GetData().AsMap()["count"])
		}
		if _, exists := got.GetData().AsMap()["optional"]; exists {
			t.Error("input without a default was added")
		}
	})

	t.Run("caller values including null win", func(t *testing.T) {
		data, err := structpb.NewStruct(map[string]any{
			"url":   nil,
			"count": float64(9),
			"extra": "preserve me",
		})
		if err != nil {
			t.Fatalf("input: %v", err)
		}
		input := &orkstackv1.Payload{Data: data}
		got := engine.ApplyInputDefaults(wf, input)
		fields := got.GetData().GetFields()
		if fields["url"].GetNullValue() != structpb.NullValue_NULL_VALUE {
			t.Errorf("explicit null url was replaced: %v", fields["url"])
		}
		if fields["count"].GetNumberValue() != 9 {
			t.Errorf("count = %v, want caller value 9", fields["count"])
		}
		if fields["extra"].GetStringValue() != "preserve me" {
			t.Errorf("undeclared field changed: %v", fields["extra"])
		}
		if input.GetData().GetFields()["count"].GetNumberValue() != 9 {
			t.Error("input payload was mutated")
		}
	})
}

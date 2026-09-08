package main

import (
	"os"
	"path/filepath"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAgainInputPreservesSourceAndAppliesTypedOverrides(t *testing.T) {
	data, err := structpb.NewStruct(map[string]any{"kept": "value", "count": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	got, err := againInput(&orkstackv1.Payload{Data: data}, []string{"count=2", "flag=true"}, `{"json":"yes"}`)
	if err != nil {
		t.Fatal(err)
	}
	values := got.GetData().AsMap()
	if values["kept"] != "value" || values["count"] != float64(2) || values["flag"] != true || values["json"] != "yes" {
		t.Fatalf("againInput values = %#v", values)
	}
}

func TestSafeOutputPathOnlyValidatesPathNotExistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := safeOutputPath(path, false); err != nil {
		t.Fatalf("safeOutputPath = %v; existence is committed atomically later", err)
	}
}

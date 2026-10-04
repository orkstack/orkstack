package task

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestItemIdempotencyKeyFor(t *testing.T) {
	if got := ItemIdempotencyKeyFor("", "ork-1", "convert", 97); got != "default/ork-1/convert/97" {
		t.Fatalf("ItemIdempotencyKeyFor = %q", got)
	}
	if got := ItemIdempotencyKeyFor("data", "ork-1", "convert", 0); got != "data/ork-1/convert/0" {
		t.Fatalf("ItemIdempotencyKeyFor(item 0) = %q", got)
	}
	// A task key has one segment fewer, so the two shapes never collide.
	task := IdempotencyKeyFor("data", "ork-1", "convert")
	item := ItemIdempotencyKeyFor("data", "ork-1", "convert", 0)
	if task == item || strings.Count(item, "/") != strings.Count(task, "/")+1 {
		t.Fatalf("task key %q and item key %q", task, item)
	}
}

func TestInputSize_EqualsTheCheckersMeasure(t *testing.T) {
	in := &orkstackv1.ExecuteTaskInput{RunId: "ork-1", TaskId: "convert", Command: []string{"sh", "-c", "echo \"quoted\"\n"}}
	size, err := InputSize(in)
	if err != nil {
		t.Fatalf("InputSize: %v", err)
	}
	if err := CheckInputSize(in, size); err != nil {
		t.Fatalf("CheckInputSize at exactly InputSize refused: %v", err)
	}
	if err := CheckInputSize(in, size-1); err == nil || !strings.Contains(err.Error(), ReasonInputTooLarge) {
		t.Fatalf("CheckInputSize one byte under InputSize = %v, want INPUT_TOO_LARGE", err)
	}
}

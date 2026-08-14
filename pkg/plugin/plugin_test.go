package plugin

import (
	"context"
	"errors"
	"testing"
)

type fakePlugin struct{ name string }

func (f fakePlugin) Name() string          { return f.name }
func (f fakePlugin) Describe() Descriptor   { return Descriptor{Name: f.name, Version: "0"} }
func (f fakePlugin) Execute(context.Context, *ExecContext) (*Result, error) {
	return &Result{Output: Payload{"ok": true}}, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	r.Register(fakePlugin{name: "http"})

	got, ok := r.Get("http")
	if !ok {
		t.Fatalf("expected plugin 'http' to be registered")
	}
	if got.Name() != "http" {
		t.Fatalf("got name %q, want http", got.Name())
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatalf("did not expect 'missing' plugin")
	}
	if n := len(r.List()); n != 1 {
		t.Fatalf("List len = %d, want 1", n)
	}
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	r.Register(fakePlugin{name: "dup"})
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on duplicate registration")
		}
	}()
	r.Register(fakePlugin{name: "dup"})
}

func TestRetryableClassification(t *testing.T) {
	base := errors.New("boom")
	var re *RetryableError
	if !errors.As(Retryable(base), &re) || !re.Retryable {
		t.Fatalf("Retryable should mark error retryable")
	}
	if !errors.As(Permanent(base), &re) || re.Retryable {
		t.Fatalf("Permanent should mark error non-retryable")
	}
	if !errors.Is(Retryable(base), base) {
		t.Fatalf("wrapped error should unwrap to base")
	}
}

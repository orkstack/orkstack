package dsl

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const concurrencyHeader = "apiVersion: orkstack/v1\nkind: Orkflow\nname: migrate\nversion: \"1\"\ninputs:\n  env:\n    type: string\n"
const concurrencyTasks = "tasks:\n  - id: a\n    uses: busybox:1\n"

func concurrencyDoc(block string) []byte {
	return []byte(concurrencyHeader + block + concurrencyTasks)
}

// A run-level concurrency: block compiles to its spec, on_limit defaulting
// to queue and the queue timeout read as whole seconds.
func TestConcurrency_Compiles(t *testing.T) {
	for name, tc := range map[string]struct {
		block string
		want  *orkstackv1.ConcurrencySpec
	}{
		"limit only": {"concurrency:\n  limit: 1\n",
			&orkstackv1.ConcurrencySpec{Limit: 1, OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}},
		"every key": {"concurrency:\n  key: \"db-migrate:{{ .inputs.env }}\"\n  limit: 2\n  on_limit: queue\n  queue_timeout: 2h\n  max_queued: 50\n",
			&orkstackv1.ConcurrencySpec{Limit: 2, Key: "db-migrate:{{ .inputs.env }}", OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE,
				QueueTimeoutSeconds: 7200, MaxQueued: 50}},
		"a literal key": {"concurrency:\n  key: registry-push\n  limit: 3\n",
			&orkstackv1.ConcurrencySpec{Limit: 3, Key: "registry-push", OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}},
		"run metadata roots": {"concurrency:\n  key: \"{{ .run.orkflow }}/{{ .run.version }}/{{ .run.origin.kind }}/{{ .run.origin.id }}\"\n  limit: 1\n",
			&orkstackv1.ConcurrencySpec{Limit: 1, Key: "{{ .run.orkflow }}/{{ .run.version }}/{{ .run.origin.kind }}/{{ .run.origin.id }}",
				OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}},
	} {
		t.Run(name, func(t *testing.T) {
			wf, err := CompileWithOptions(concurrencyDoc(tc.block), Options{})
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if !proto.Equal(wf.GetConcurrency(), tc.want) {
				t.Fatalf("concurrency = %v, want %v", wf.GetConcurrency(), tc.want)
			}
			if err := ValidateConcurrency(wf.GetConcurrency()); err != nil {
				t.Fatalf("the compiled spec does not validate again: %v", err)
			}
		})
	}
	wf, err := CompileWithOptions(concurrencyDoc(""), Options{})
	if err != nil || wf.GetConcurrency() != nil {
		t.Fatalf("no block = %v, %v; want no limit", wf.GetConcurrency(), err)
	}
}

// Every refusal names the field, and is a validation error of the
// concurrency block.
func TestConcurrency_Refusals(t *testing.T) {
	for name, tc := range map[string]struct{ block, want string }{
		"no limit":                {"concurrency:\n  key: k\n", "concurrency.limit"},
		"limit zero":              {"concurrency:\n  limit: 0\n", "concurrency.limit"},
		"limit negative":          {"concurrency:\n  limit: -1\n", "concurrency.limit"},
		"an unknown key":          {"concurrency:\n  limit: 1\n  burst: 2\n", "burst"},
		"not a mapping":           {"concurrency: 1\n", "concurrency"},
		"an unknown strategy":     {"concurrency:\n  limit: 1\n  on_limit: wait\n", "on_limit"},
		"a strategy not yet here": {"concurrency:\n  limit: 1\n  on_limit: refuse\n", "not available yet"},
		"replace not yet here":    {"concurrency:\n  limit: 1\n  on_limit: replace\n", "not available yet"},
		"a timeout as a number":   {"concurrency:\n  limit: 1\n  queue_timeout: 60\n", "queue_timeout"},
		"a timeout not whole":     {"concurrency:\n  limit: 1\n  queue_timeout: 1500ms\n", "whole number of seconds"},
		"a zero timeout":          {"concurrency:\n  limit: 1\n  queue_timeout: 0s\n", "queue_timeout"},
		"a negative max_queued":   {"concurrency:\n  limit: 1\n  max_queued: -1\n", "max_queued"},
		"a reserved literal key":  {"concurrency:\n  key: \"ork:mine\"\n  limit: 1\n", "ork:"},
		"a literal key with a space": {"concurrency:\n  key: \"db migrate\"\n  limit: 1\n", "offset 2"},
		"a literal key too long":  {"concurrency:\n  key: \"" + strings.Repeat("k", 129) + "\"\n  limit: 1\n", "CONCURRENCY_KEY_INVALID"},
		"a template too long":     {"concurrency:\n  key: \"{{ .inputs.env }}" + strings.Repeat("k", 512) + "\"\n  limit: 1\n", "512 bytes"},
		"the run id":              {"concurrency:\n  key: \"{{ .run.id }}\"\n  limit: 1\n", ".run.id"},
		"a principal":             {"concurrency:\n  key: \"{{ .run.origin.principal }}\"\n  limit: 1\n", ".run.origin.principal"},
		"a task output":           {"concurrency:\n  key: \"{{ .tasks.a.output.x }}\"\n  limit: 1\n", ".tasks"},
		"a function":              {"concurrency:\n  key: \"{{ printf \\\"%s\\\" .inputs.env }}\"\n  limit: 1\n", "only a field"},
		"a pipeline":              {"concurrency:\n  key: \"{{ .inputs.env | len }}\"\n  limit: 1\n", "only a field"},
		"a condition":             {"concurrency:\n  key: \"{{ if .inputs.env }}x{{ end }}\"\n  limit: 1\n", "only a field"},
		"a parse error":           {"concurrency:\n  key: \"{{ .inputs.env \"\n  limit: 1\n", "template"},
		"an empty key":            {"concurrency:\n  key: \"\"\n  limit: 1\n", "concurrency.key"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := CompileWithOptions(concurrencyDoc(tc.block), Options{})
			if err == nil {
				t.Fatalf("accepted %q", tc.block)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused as %q; want it to name %q", err, tc.want)
			}
		})
	}
}

// A refusal of a field's value is the concurrency block's validation
// error, as every other validation refusal is.
func TestConcurrency_RefusalIsAValidationError(t *testing.T) {
	_, err := CompileWithOptions(concurrencyDoc("concurrency:\n  limit: 0\n"), Options{})
	var v *ValidationError
	if !errors.As(err, &v) || v.Field != "concurrency" {
		t.Fatalf("err = %v; want a ValidationError of field concurrency", err)
	}
}

// The block is in the definition's digest only when it is there, so a
// definition that declares none keeps its digest; two spellings of one
// timeout are one definition.
func TestConcurrency_Digest(t *testing.T) {
	digest := func(block string) string {
		t.Helper()
		wf, err := CompileWithOptions(concurrencyDoc(block), Options{})
		if err != nil {
			t.Fatal(err)
		}
		d, err := DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	none, one := digest(""), digest("concurrency:\n  limit: 1\n")
	if none == one {
		t.Fatal("declaring a limit left the digest unchanged")
	}
	if digest("concurrency:\n  limit: 1\n  queue_timeout: 2h\n") != digest("concurrency:\n  limit: 1\n  queue_timeout: 120m\n") {
		t.Fatal("2h and 120m are two definitions")
	}
	if digest("concurrency:\n  limit: 1\n") != digest("concurrency:\n  limit: 1\n  on_limit: queue\n") {
		t.Fatal("the default on_limit, written out, is another definition")
	}
	if digest("concurrency:\n  limit: 1\n") == digest("concurrency:\n  limit: 2\n") {
		t.Fatal("two limits are one definition")
	}
}

// CheckConcurrencyKey is §3.3's rule for a rendered key, refused
// CONCURRENCY_KEY_INVALID naming the offset of the first bad byte.
func TestCheckConcurrencyKey(t *testing.T) {
	for key, want := range map[string]string{
		"db-migrate:prod":                    "",
		"a":                                  "",
		"A.b_c:d/e@f+g=h-9":                  "",
		strings.Repeat("k", 128):             "",
		"":                                   "empty",
		strings.Repeat("k", 129):             "at most 128",
		"ork:taken":                          "ork:",
		"-leading":                           "offset 0",
		"has space":                          "offset 3",
		"tab\tin":                            "offset 3",
	} {
		err := CheckConcurrencyKey(key)
		switch {
		case want == "" && err != nil:
			t.Errorf("%q refused: %v", key, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "CONCURRENCY_KEY_INVALID")):
			t.Errorf("%q = %v; want CONCURRENCY_KEY_INVALID naming %q", key, err, want)
		}
	}
}

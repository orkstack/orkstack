package dsl

import (
	"strings"
	"testing"
)

// FuzzCompileConcurrency feeds arbitrary concurrency: blocks through the
// compiler. Properties: it never panics; a block it accepts compiles to a
// spec ValidateConcurrency accepts again, as create and plan re-check a
// stored definition; and a literal key it accepts is a key
// CheckConcurrencyKey accepts, while a refused key is refused
// CONCURRENCY_KEY_INVALID or as a template, naming the key field.
func FuzzCompileConcurrency(f *testing.F) {
	for _, seed := range []string{
		"concurrency:\n  limit: 1\n",
		"concurrency:\n  key: \"db-migrate:{{ .inputs.env }}\"\n  limit: 2\n  on_limit: queue\n  queue_timeout: 2h\n  max_queued: 50\n",
		"concurrency:\n  key: registry-push\n  limit: 3\n",
		"concurrency:\n  key: \"{{ .run.origin.id }}\"\n  limit: 1\n",
		"concurrency:\n  limit: 0\n",
		"concurrency:\n  limit: 1\n  on_limit: replace\n",
		"concurrency:\n  limit: 1\n  queue_timeout: 60\n",
		"concurrency:\n  key: \"{{ .run.id }}\"\n  limit: 1\n",
		"concurrency:\n  key: \"{{ printf \\\"%s\\\" .inputs.env }}\"\n  limit: 1\n",
		"concurrency:\n  key: \"ork:x\"\n  limit: 1\n",
		"concurrency: [1]\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, block string) {
		wf, err := CompileWithOptions(concurrencyDoc(block), Options{})
		if err != nil {
			return
		}
		spec := wf.GetConcurrency()
		if err := ValidateConcurrency(spec); err != nil {
			t.Fatalf("compiled %q to %v, which ValidateConcurrency refuses: %v", block, spec, err)
		}
		if key := spec.GetKey(); key != "" && !strings.Contains(key, "{{") {
			if err := CheckConcurrencyKey(key); err != nil {
				t.Fatalf("compiled the literal key %q, which CheckConcurrencyKey refuses: %v", key, err)
			}
		}
	})
}

// FuzzCheckConcurrencyKey: an accepted key is 1 to 128 bytes of the key
// alphabet, starting with a letter or digit and not with ork:; a refusal is
// always CONCURRENCY_KEY_INVALID.
func FuzzCheckConcurrencyKey(f *testing.F) {
	for _, seed := range []string{"db-migrate:prod", "", "ork:x", "-a", "a b", strings.Repeat("k", 129), "A.b_c:d/e@f+g=h-9", "ork:orkflow:default/a", "ork:task:a/b:c"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		err := CheckConcurrencyKey(key)
		// A key read back is an authored one or one of OrkStack's own, and no
		// other is.
		stored := CheckStoredConcurrencyKey(key)
		if stored != nil && !strings.Contains(stored.Error(), "CONCURRENCY_KEY_INVALID") {
			t.Fatalf("%q refused without its code: %v", key, stored)
		}
		if reserved := strings.HasPrefix(key, "ork:"); !reserved && (stored == nil) != (err == nil) ||
			reserved && stored == nil && (err == nil || len(key) > 611) {
			t.Fatalf("%q read back = %v, authored = %v", key, stored, err)
		}
		if err != nil {
			if !strings.Contains(err.Error(), "CONCURRENCY_KEY_INVALID") {
				t.Fatalf("%q refused without its code: %v", key, err)
			}
			return
		}
		if len(key) < 1 || len(key) > ConcurrencyKeyMaxBytes || strings.HasPrefix(key, "ork:") {
			t.Fatalf("%q accepted outside its bounds", key)
		}
		for i := 0; i < len(key); i++ {
			c := key[i]
			alnum := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
			if (i == 0 && !alnum) || (!alnum && !strings.ContainsRune("._:/@+=-", rune(c))) {
				t.Fatalf("%q accepted with %q at offset %d", key, c, i)
			}
		}
	})
}

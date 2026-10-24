package dsl

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func cacheFieldOf(err error) string {
	var verr *ValidationError
	if errors.As(err, &verr) {
		return verr.Field
	}
	return ""
}

func TestValidate_HandBuiltCacheRules(t *testing.T) {
	base := func() *orkstackv1.Orkflow {
		return &orkstackv1.Orkflow{
			Name: "hand",
			Pins: []*orkstackv1.Pin{{Name: "build"}},
			Tasks: []*orkstackv1.Task{{
				Id: "t", Uses: "shell", TimeoutSeconds: 60,
				Cache: &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW},
			}, {Id: "member", Uses: "shell", TimeoutSeconds: 60, Pin: "build"}},
		}
	}
	if err := validate(base(), Options{}); err != nil {
		t.Fatalf("a valid hand-built cached task: %v", err)
	}
	cases := map[string]struct {
		mutate func(*orkstackv1.Task)
		field  string
		text   string
	}{
		"unspecified scope": {func(t *orkstackv1.Task) { t.Cache.Scope = orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED },
			"tasks[0].cache.scope", "a stored definition never carries UNSPECIFIED"},
		"out-of-range scope": {func(t *orkstackv1.Task) { t.Cache.Scope = orkstackv1.CacheScope(7) },
			"tasks[0].cache.scope", "is not orkflow or namespace"},
		"negative ttl": {func(t *orkstackv1.Task) { t.Cache.TtlSeconds = -5 },
			"tasks[0].cache.ttl_seconds", "cache.ttl_seconds -5 is negative"},
		"pinned": {func(t *orkstackv1.Task) { t.Pin = "build" },
			"tasks[0].cache", "a cached task cannot join a pin"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wf := base()
			tc.mutate(wf.GetTasks()[0])
			err := validate(wf, Options{})
			if err == nil || cacheFieldOf(err) != tc.field || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err = %v (field %q), want %q at %s", err, cacheFieldOf(err), tc.text, tc.field)
			}
		})
	}
	// Rule 15 and rule 27 on compiled messages.
	await := &orkstackv1.Orkflow{Name: "hand", Tasks: []*orkstackv1.Task{{
		Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60},
		Cache: &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW},
	}}}
	if err := validate(await, Options{}); err == nil || cacheFieldOf(err) != "tasks[0].cache" {
		t.Fatalf("hand-built cached await = %v", err)
	}
}

// cache is emitted only when set: every existing canonical golden is
// unchanged, adding cache changes the digest, and so does changing its TTL.
func TestCanonicalOrkflow_CacheEmittedOnlyWhenSet(t *testing.T) {
	base := goldenOrkflow(t)
	canonical, err := CanonicalOrkflow(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), `"cache"`) {
		t.Fatalf("an uncached definition emits cache: %s", canonical)
	}
	if got, _ := DefinitionDigest(base); got != GoldenDefinitionDigest {
		t.Fatalf("the golden digest moved: %s", got)
	}
	cached := goldenOrkflow(t)
	cached.GetTasks()[0].Cache = &orkstackv1.CacheSpec{TtlSeconds: 60, Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}
	withCache, _ := DefinitionDigest(cached)
	if withCache == GoldenDefinitionDigest {
		t.Fatal("adding cache did not change the digest")
	}
	cached.GetTasks()[0].Cache.TtlSeconds = 61
	if again, _ := DefinitionDigest(cached); again == withCache {
		t.Fatal("changing ttl_seconds did not change the digest")
	}
	cached.GetTasks()[0].Cache.Scope = orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE
	if again, _ := DefinitionDigest(cached); again == withCache {
		t.Fatal("changing scope did not change the digest")
	}
	doc, _ := CanonicalOrkflow(cached)
	if !strings.Contains(string(doc), `"cache":{"scope":2,"ttlSeconds":61}`) {
		t.Fatalf("canonical cache = %s", doc)
	}
}

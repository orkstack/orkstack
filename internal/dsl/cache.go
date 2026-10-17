package dsl

import (
	"fmt"
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// cacheDocument is one task's cache declaration (docs/DSL.md "Caching").
// KnownFields(true) applies inside it, so a misspelled key is refused.
type cacheDocument struct {
	// Enabled is required: a pointer so an absent key is refused by name.
	// true is the opt-in AND the author's assertion that the task is a pure
	// function of what it is given; false compiles to no cache at all.
	Enabled    *bool  `yaml:"enabled"`
	TTLSeconds *int32 `yaml:"ttl_seconds"`
	Scope      string `yaml:"scope"`
}

// cacheScopeWords are the DSL's cache scopes, the default first.
var cacheScopeWords = []string{"orkflow", "namespace"}

// cacheScopeOf maps the DSL's word to the enum. "" and "orkflow" are one
// value: normalising here means a document that writes `scope: orkflow` and
// one that omits it compile to one message and one digest. An unknown word
// is UNSPECIFIED, which the caller has already refused.
func cacheScopeOf(raw string) orkstackv1.CacheScope {
	switch raw {
	case "", cacheScopeWords[0]:
		return orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	case cacheScopeWords[1]:
		return orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE
	default:
		return orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED
	}
}

// compileCache builds a task's CacheSpec, or nil when the document declares
// no `cache:` or declares it with `enabled: false`. Rules 33–35 are checked
// at this layer whatever `enabled` says, so flipping `enabled: false` back to
// true can never surface a latent error.
func compileCache(taskIndex int, td taskDocument) (*orkstackv1.CacheSpec, error) {
	if td.Cache == nil {
		return nil, nil
	}
	at := func(field string) string { return fmt.Sprintf("tasks[%d].cache.%s", taskIndex, field) }
	// Rule 33.
	if td.Cache.Enabled == nil {
		return nil, validationError(at("enabled"), fmt.Errorf(
			"task %q: cache.enabled is required — write enabled: true to opt this task in; it asserts "+
				"the task is a pure function of its image, command, env, config and inputs, which "+
				"OrkStack cannot verify", td.ID))
	}
	// Rule 35, YAML layer.
	if td.Cache.Scope != "" && !slices.Contains(cacheScopeWords, td.Cache.Scope) {
		return nil, validationError(at("scope"), fmt.Errorf(
			"task %q: cache.scope %q is not orkflow or namespace", td.ID, td.Cache.Scope))
	}
	// Rule 34, YAML layer.
	var ttl int32
	if td.Cache.TTLSeconds != nil {
		ttl = *td.Cache.TTLSeconds
		if ttl < 0 {
			return nil, validationError(at("ttl_seconds"), cacheTTLRefusal(td.ID, ttl))
		}
	}
	if !*td.Cache.Enabled {
		return nil, nil
	}
	return &orkstackv1.CacheSpec{TtlSeconds: ttl, Scope: cacheScopeOf(td.Cache.Scope)}, nil
}

// cacheTTLRefusal is rule 34's sentence.
func cacheTTLRefusal(taskID string, ttl int32) error {
	return fmt.Errorf("task %q: cache.ttl_seconds %d is negative — omit it (or 0) for the deployment's cap",
		taskID, ttl)
}

// validateCache enforces rules 34–36 on one compiled container task's
// CacheSpec, so a hand-built request meets them too. A task with no cache
// passes.
func validateCache(taskIndex int, task *orkstackv1.Task) error {
	spec := task.GetCache()
	if spec == nil {
		return nil
	}
	id := task.GetId()
	at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
	// Rule 34.
	if spec.GetTtlSeconds() < 0 {
		return validationError(at("cache.ttl_seconds"), cacheTTLRefusal(id, spec.GetTtlSeconds()))
	}
	// Rule 35.
	switch spec.GetScope() {
	case orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE:
	default:
		return validationError(at("cache.scope"), fmt.Errorf(
			"task %q: cache.scope %s is not orkflow or namespace — the compiler writes orkflow when "+
				"scope is absent; a stored definition never carries UNSPECIFIED", id, spec.GetScope()))
	}
	// Rule 36.
	if task.GetPin() != "" {
		return validationError(at("cache"), fmt.Errorf(
			"task %q: a cached task cannot join a pin — its pin's /orkstack/shared is an input no memo "+
				"key can cover, and a reused result would skip writes the pin's other tasks may read", id))
	}
	return nil
}

// DeclaresCache reports whether any task of wf declares cache. Pure.
func DeclaresCache(wf *orkstackv1.Orkflow) bool {
	for _, task := range wf.GetTasks() {
		if task.GetCache() != nil {
			return true
		}
	}
	return false
}

// CacheScopeName renders a scope for human surfaces: "orkflow", "namespace",
// or "" for UNSPECIFIED. Pure.
func CacheScopeName(scope orkstackv1.CacheScope) string {
	switch scope {
	case orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW:
		return "orkflow"
	case orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE:
		return "namespace"
	default:
		return ""
	}
}

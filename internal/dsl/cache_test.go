package dsl_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func cacheDoc(cache string) []byte {
	return []byte(`
name: cached
tasks:
  - id: featurize
    uses: shell
    with: {op: exec, command: ["true"]}
` + cache)
}

func TestCompile_CacheCompiles(t *testing.T) {
	wf, err := dsl.CompileFile("../../examples/cached-pipeline.ork.yaml")
	if err != nil {
		t.Fatalf("compile the example: %v", err)
	}
	got := map[string]*orkstackv1.CacheSpec{}
	for _, task := range wf.GetTasks() {
		got[task.GetId()] = task.GetCache()
	}
	if want := (&orkstackv1.CacheSpec{TtlSeconds: 86400, Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}); !proto.Equal(got["featurize"], want) {
		t.Fatalf("featurize cache = %v, want %v", got["featurize"], want)
	}
	if want := (&orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}); !proto.Equal(got["train"], want) {
		t.Fatalf("train cache = %v, want %v", got["train"], want)
	}
	if got["stamp"] != nil {
		t.Fatalf("stamp cache = %v, want none", got["stamp"])
	}

	ns, err := dsl.Compile(cacheDoc("    cache: {enabled: true, scope: namespace, ttl_seconds: 60}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s := ns.GetTasks()[0].GetCache(); s.GetScope() != orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE || s.GetTtlSeconds() != 60 {
		t.Fatalf("namespace scope = %v", s)
	}

	// Absent scope and `scope: orkflow` are one message and one digest.
	absent, err := dsl.Compile(cacheDoc("    cache: {enabled: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := dsl.Compile(cacheDoc("    cache: {enabled: true, scope: orkflow}\n"))
	if err != nil {
		t.Fatal(err)
	}
	absent.Version = explicit.GetVersion()
	if !proto.Equal(absent.GetTasks()[0], explicit.GetTasks()[0]) {
		t.Fatalf("absent scope and scope: orkflow compile to two messages")
	}
	a, _ := dsl.DefinitionDigest(absent)
	b, _ := dsl.DefinitionDigest(explicit)
	if a != b {
		t.Fatalf("absent scope and scope: orkflow digest differently")
	}
}

func TestCompile_CacheEnabledIsRequired(t *testing.T) {
	_, err := dsl.Compile(cacheDoc("    cache: {ttl_seconds: 60}\n"))
	want := `task "featurize": cache.enabled is required — write enabled: true to opt this task in; ` +
		`it asserts the task is a pure function of its image, command, env, config and inputs, which OrkStack cannot verify`
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v\nwant %s", err, want)
	}
	if fieldOf(err) != "tasks[0].cache.enabled" {
		t.Fatalf("field = %q", fieldOf(err))
	}
	if _, err := dsl.Compile(cacheDoc("    cache: {enabled: true, ttl: 3}\n")); err == nil {
		t.Fatal("an unknown key inside cache: compiled")
	}
}

func TestCompile_CacheEnabledFalseCompilesToNoCache(t *testing.T) {
	off, err := dsl.Compile(cacheDoc("    cache: {enabled: false, ttl_seconds: 60, scope: namespace}\n"))
	if err != nil {
		t.Fatal(err)
	}
	none, err := dsl.Compile(cacheDoc(""))
	if err != nil {
		t.Fatal(err)
	}
	if off.GetTasks()[0].GetCache() != nil {
		t.Fatalf("enabled: false compiled a cache: %v", off.GetTasks()[0].GetCache())
	}
	off.Version = none.GetVersion()
	if !proto.Equal(off.GetTasks()[0], none.GetTasks()[0]) {
		t.Fatal("enabled: false compiles to a different message than no cache:")
	}
	a, _ := dsl.DefinitionDigest(off)
	b, _ := dsl.DefinitionDigest(none)
	if a != b {
		t.Fatal("enabled: false digests differently than no cache:")
	}
	// Latent errors are refused whatever enabled says.
	for _, bad := range []string{
		"    cache: {enabled: false, ttl_seconds: -1}\n",
		"    cache: {enabled: false, scope: global}\n",
	} {
		if _, err := dsl.Compile(cacheDoc(bad)); err == nil {
			t.Fatalf("%q compiled", bad)
		}
	}
}

func TestCompile_CacheTTLAndScopeRules(t *testing.T) {
	_, err := dsl.Compile(cacheDoc("    cache: {enabled: true, ttl_seconds: -1}\n"))
	if err == nil || fieldOf(err) != "tasks[0].cache.ttl_seconds" ||
		err.Error() != `task "featurize": cache.ttl_seconds -1 is negative — omit it (or 0) for the deployment's cap` {
		t.Fatalf("ttl -1 = %v (field %q)", err, fieldOf(err))
	}
	for _, ok := range []string{"0", "2147483647"} {
		if _, err := dsl.Compile(cacheDoc("    cache: {enabled: true, ttl_seconds: " + ok + "}\n")); err != nil {
			t.Fatalf("ttl %s refused: %v", ok, err)
		}
	}
	_, err = dsl.Compile(cacheDoc("    cache: {enabled: true, scope: global}\n"))
	if err == nil || fieldOf(err) != "tasks[0].cache.scope" ||
		err.Error() != `task "featurize": cache.scope "global" is not orkflow or namespace` {
		t.Fatalf("scope global = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_CacheRefusesPin(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: cached
pins: {build: {}}
tasks:
  - id: featurize
    uses: shell
    pin: build
    cache: {enabled: true}
    with: {op: exec, command: ["true"]}
`))
	if err == nil || fieldOf(err) != "tasks[0].cache" ||
		!strings.Contains(err.Error(), "a cached task cannot join a pin — its pin's /orkstack/shared is an input no memo key can cover") {
		t.Fatalf("cache with pin = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_AwaitRefusesCache(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: cached
tasks:
  - id: gate
    await: {timeout_seconds: 60}
    cache: {enabled: true}
`))
	want := `task "gate": an awaited task takes only id, needs, condition, join and await; "cache" does not apply ` +
		`(its value comes from whoever resolves it, not from a computation)`
	if err == nil || err.Error() != want || fieldOf(err) != "tasks[0].cache" {
		t.Fatalf("cache on an await = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_ComposedTaskRefusesCache(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: caller
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    cache: {enabled: true}
`))
	want := `task "release": a composed task takes only id, needs, condition, join, uses and with; "cache" does not apply ` +
		`(it runs orkflow release-block@2.1.0 as a child run, whose own tasks declare where and how they run)`
	if err == nil || err.Error() != want || fieldOf(err) != "tasks[0].cache" {
		t.Fatalf("cache on a composed task = %v (field %q)", err, fieldOf(err))
	}
}

func TestCompile_FanOutMayBeCached(t *testing.T) {
	wf, err := dsl.Compile(eachDoc(`
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5}
    cache: {enabled: true, ttl_seconds: 3600}
    with: {op: exec, x: "{{ .each.item }}"}
`))
	if err != nil {
		t.Fatalf("a cached fan-out: %v", err)
	}
	if wf.GetTasks()[1].GetCache().GetTtlSeconds() != 3600 || wf.GetTasks()[1].GetEach() == nil {
		t.Fatalf("convert = %v", wf.GetTasks()[1])
	}
}

func TestDeclaresCache(t *testing.T) {
	if dsl.DeclaresCache(&orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a"}}}) {
		t.Fatal("no cache declared, DeclaresCache true")
	}
	if !dsl.DeclaresCache(&orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "a"}, {Id: "b", Cache: &orkstackv1.CacheSpec{}}}}) {
		t.Fatal("a cached task, DeclaresCache false")
	}
	if dsl.DeclaresCache(nil) {
		t.Fatal("nil orkflow declares cache")
	}
}

func TestCacheScopeName(t *testing.T) {
	for scope, want := range map[orkstackv1.CacheScope]string{
		orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW:     "orkflow",
		orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE:   "namespace",
		orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED: "",
	} {
		if got := dsl.CacheScopeName(scope); got != want {
			t.Fatalf("CacheScopeName(%v) = %q, want %q", scope, got, want)
		}
	}
}

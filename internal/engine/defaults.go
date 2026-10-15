package engine

import (
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// DefaultEachMaxItems is ORK_EACH_MAX_ITEMS' default: the items one
// Orkflow's fan-outs may declare in total — a deployment default of 1000,
// which a deployment lowers, or raises up to the built-in 10000
// (dsl.MaxEachItems) with continuation on (docs/DSL.md "Fan-out",
// docs/DEPLOY.md §1).
const DefaultEachMaxItems int32 = 1000

// DefaultOrkflowMaxTasks is ORK_ORKFLOW_MAX_TASKS' default: the most tasks
// one Orkflow may declare (docs/DSL.md "Tasks", docs/DEPLOY.md §1). A
// deployment may lower or raise it. Every task is at least one activity and
// its lifecycle events in the run's history, which the cluster bounds. A run
// continues as new past its budget only at a moment with nothing in flight,
// and each history must still hold what its failure path would append for
// every task not yet started, so tasks that start together share ONE
// history; 1000 keeps such a stage of plain tasks well inside the cluster's
// default history bound beside the 1000 fan-out items DefaultEachMaxItems
// already allows. A larger default waits on the soak that measures it, and
// a deployment that raises it before then runs unmeasured.
const DefaultOrkflowMaxTasks int32 = 1000

// DefaultComposeMaxDepth is ORK_COMPOSE_MAX_DEPTH's default: the built-in
// ceiling on how many levels of child runs one run may start, which a
// deployment may lower and never raise (docs/DSL.md "Composition").
const DefaultComposeMaxDepth int32 = MaxComposeDepth

// DefaultCacheMaxTTL is ORK_CACHE_MAX_TTL's default: the oldest memoized
// result a cached task may reuse, measured from when it was committed
// (docs/DSL.md "Caching").
const DefaultCacheMaxTTL = 168 * time.Hour

// ApplyInputDefaults clones input and fills omitted fields from the orkflow's
// declared input defaults. Caller fields always win, including explicit null;
// undeclared fields are preserved unchanged.
//
// This helper is safe in workflow code: it has no side effects and sorts map
// keys before traversing them.
func ApplyInputDefaults(wf *orkstackv1.Orkflow, input *orkstackv1.Payload) *orkstackv1.Payload {
	var out *orkstackv1.Payload
	if input == nil {
		out = &orkstackv1.Payload{}
	} else {
		out = proto.Clone(input).(*orkstackv1.Payload)
	}

	declarations := wf.GetInputs().GetFields()
	if len(declarations) == 0 {
		return out
	}

	keys := make([]string, 0, len(declarations))
	for name := range declarations {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	for _, name := range keys {
		if out.GetData() != nil {
			if _, provided := out.GetData().GetFields()[name]; provided {
				continue
			}
		}
		declaration := declarations[name].GetStructValue()
		if declaration == nil {
			continue
		}
		defaultValue, hasDefault := declaration.GetFields()["default"]
		if !hasDefault {
			continue
		}
		if out.Data == nil {
			out.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
		}
		if out.Data.Fields == nil {
			out.Data.Fields = map[string]*structpb.Value{}
		}
		out.Data.Fields[name] = proto.Clone(defaultValue).(*structpb.Value)
	}
	return out
}

// DefaultDefinitionByReference is engine.definition_by_reference's built-in
// default: on, in a release after the one that first contained it — the
// order a rollback relies on (docs/DEPLOY.md §4, R3) — so a definition too
// large for a start payload travels by reference. Its effective value is
// this AND engine.continuation.enabled when neither the configuration
// document nor the environment sets the key, so the flip never refuses to
// boot a deployment that turned continuation off.
const DefaultDefinitionByReference = true

package api_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// The plugin inventory is one of the four things the Admin surface reports,
// and PluginService promises it "across connected Orkers" with each plugin's
// input and output schema on the wire. These tests hold it to both halves of
// that sentence.

// describedPlugins is a lister that answers for the deployment's default
// execution channel, with the schemas a real Orker declares.
type describedPlugins struct{ descriptors []plugin.Descriptor }

func (d describedPlugins) ListPlugins(context.Context) ([]plugin.Descriptor, error) {
	return d.descriptors, nil
}

// poolInventory stands in for the Orkers of one pool, answering with full
// descriptors — the same shape the Orker's own ListPlugins activity returns.
type poolInventory map[string][]plugin.Descriptor

func (p poolInventory) PluginsOnPool(
	_ context.Context,
	queue string,
) ([]plugin.Descriptor, error) {
	return p[queue], nil
}

// unreachablePool is a pool whose Orkers are live but cannot be asked.
type unreachablePool struct{ err error }

func (u unreachablePool) PluginsOnPool(
	context.Context,
	string,
) ([]plugin.Descriptor, error) {
	return nil, u.err
}

// TestListPlugins_CarriesTheInputAndOutputSchemasTheOrkersDeclare is the
// inventory's declared shape.
//
// PluginInfo has carried input_schema and output_schema since the contract was
// written, and the UI's plugin view is meant to disclose them. The mapping
// dropped both, so every client saw a name, a version and a description and
// had no way to tell what a plugin takes — a field on the wire that nothing
// ever fills is worse than no field, because a reader believes it.
func TestListPlugins_CarriesTheInputAndOutputSchemasTheOrkersDeclare(t *testing.T) {
	svc := api.NewService(api.Config{
		Lister: describedPlugins{descriptors: []plugin.Descriptor{{
			Name:        "http",
			Version:     "0.1.0",
			Description: "HTTP requests",
			InputSchema: plugin.SchemaDoc{
				"type": "object",
				"properties": map[string]any{
					"url": map[string]any{"type": "string"},
				},
			},
			OutputSchema: plugin.SchemaDoc{
				"type": "object",
				"properties": map[string]any{
					"status": map[string]any{"type": "number"},
				},
			},
		}}},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	if len(resp.Msg.GetPlugins()) != 1 {
		t.Fatalf("inventory = %v, want the one plugin the Orker declared", resp.Msg.GetPlugins())
	}
	info := resp.Msg.GetPlugins()[0]
	input := info.GetInputSchema().AsMap()
	if input["type"] != "object" {
		t.Errorf("input_schema = %v, want the declared JSON schema", input)
	}
	properties, ok := input["properties"].(map[string]any)
	if !ok || properties["url"] == nil {
		t.Errorf("input_schema properties = %v, want the declared url property", input["properties"])
	}
	output := info.GetOutputSchema().AsMap()
	if output["type"] != "object" {
		t.Errorf("output_schema = %v, want the declared JSON schema", output)
	}
}

// TestListPlugins_ReportsPluginsOnlyACustomPoolsOrkersRun is the "across
// connected Orkers" half.
//
// A deployment with a gpu pool runs a plugin the default pool's Orkers do not
// have. Asking only the default execution channel reports an inventory that is
// true of one pool and presented as the deployment's, so an operator looking
// for `container` concludes it is not installed while a run using it succeeds.
//
// Everything the answer is built from comes from the store (which pools exist)
// and the cluster (who is serving them and what they run), so any replica
// gives the same reply.
func TestListPlugins_ReportsPluginsOnlyACustomPoolsOrkersRun(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{
			"default": {Name: "default"},
			"vision":  {Name: "vision", AllowedPools: []string{"main", "gpu"}},
		},
		Lister: describedPlugins{descriptors: []plugin.Descriptor{
			{Name: "shell", Version: "0.1.0"},
		}},
		Pools: livePools{task.DefaultQueue: 2, gpu: 1},
		PoolPlugins: poolInventory{
			task.DefaultQueue: {{Name: "shell", Version: "0.1.0"}},
			gpu:               {{Name: "container", Version: "0.2.0", Description: "runs a container"}},
		},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	names := []string{}
	for _, info := range resp.Msg.GetPlugins() {
		names = append(names, info.GetName())
	}
	if fmt.Sprint(names) != fmt.Sprint([]string{"container", "shell"}) {
		t.Errorf("inventory = %v, want [container shell] — every connected Orker's plugins", names)
	}
	// One entry per plugin: the default pool and the deployment's default
	// channel are the same Orkers, and reporting them twice would make the
	// inventory a count of pools rather than of plugins.
	if len(resp.Msg.GetPlugins()) != len(names) {
		t.Errorf("inventory = %v, want no duplicates", resp.Msg.GetPlugins())
	}
	for _, info := range resp.Msg.GetPlugins() {
		if info.GetName() == "container" && info.GetDescription() != "runs a container" {
			t.Errorf("container description = %q, want the gpu Orkers' own",
				info.GetDescription())
		}
	}
}

// TestListPlugins_SkipsAPoolWithNoLiveOrkers keeps the aggregation honest in
// the other direction: a pool nobody serves has nobody to ask, and asking it
// would hold the request open for the activity's whole timeout to learn
// nothing. It is skipped, not reported empty and not waited on.
func TestListPlugins_SkipsAPoolWithNoLiveOrkers(t *testing.T) {
	cold := poolQueue(t, "cold")
	asked := map[string]int{}
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "cold"},
		}},
		Lister: describedPlugins{descriptors: []plugin.Descriptor{{Name: "shell"}}},
		Pools:  livePools{task.DefaultQueue: 1},
		PoolPlugins: countingInventory{
			counts: asked,
			byQueue: poolInventory{
				task.DefaultQueue: {{Name: "shell"}},
				cold:              {{Name: "never-asked"}},
			},
		},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	for _, info := range resp.Msg.GetPlugins() {
		if info.GetName() == "never-asked" {
			t.Fatal("a pool with no live Orkers answered the inventory")
		}
	}
	if asked[cold] != 0 {
		t.Errorf("the empty pool was asked %d times; nobody is there to answer", asked[cold])
	}
}

// countingInventory records which channels were asked.
type countingInventory struct {
	counts  map[string]int
	byQueue poolInventory
}

func (c countingInventory) PluginsOnPool(
	ctx context.Context,
	queue string,
) ([]plugin.Descriptor, error) {
	c.counts[queue]++
	return c.byQueue.PluginsOnPool(ctx, queue)
}

// TestListPlugins_SurfacesAPoolThatCannotBeAsked is the same rule as
// ListPools' below: an inventory silently missing a live pool is a report an
// operator acts on. Unavailable names the pool instead.
func TestListPlugins_SurfacesAPoolThatCannotBeAsked(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "gpu"},
		}},
		Lister:      describedPlugins{descriptors: []plugin.Descriptor{{Name: "shell"}}},
		Pools:       livePools{task.DefaultQueue: 1, gpu: 1},
		PoolPlugins: unreachablePool{err: errors.New("activity timed out")},
	})

	_, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err == nil {
		t.Fatal("the inventory answered while a live pool could not be asked")
	}
	if code := connect.CodeOf(err); code != connect.CodeUnavailable {
		t.Errorf("ListPlugins failed with %v, want Unavailable", code)
	}
	if !strings.Contains(err.Error(), "gpu") {
		t.Errorf("the failure does not name the pool that could not be asked: %v", err)
	}
}

// TestListPools_ReportsAFailedPluginReadInsteadOfAnEmptyPluginSet is the pool
// health contract's honesty rule.
//
// A pool with live Orkers whose plugin read failed used to be reported as a
// live pool with no plugins — the same bytes on the wire as "these Orkers run
// nothing", which is a fact an operator would act on. The two states must stay
// distinguishable: the failure is reported, and the caller retries.
func TestListPools_ReportsAFailedPluginReadInsteadOfAnEmptyPluginSet(t *testing.T) {
	svc := api.NewService(api.Config{
		Namespaces:  registry{"default": {Name: "default"}},
		Pools:       livePools{task.DefaultQueue: 2},
		PoolPlugins: unreachablePool{err: errors.New("no worker answered")},
	})

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err == nil {
		var reported []string
		for _, pool := range resp.Msg.GetPools() {
			reported = append(reported, fmt.Sprintf("%s: %d orkers, plugins %v",
				pool.GetName(), pool.GetLiveOrkerCount(), pool.GetPlugins()))
		}
		sort.Strings(reported)
		t.Fatalf("pool health answered while the plugin read failed: %s",
			strings.Join(reported, "; "))
	}
	if code := connect.CodeOf(err); code != connect.CodeUnavailable {
		t.Errorf("ListPools failed with %v, want Unavailable", code)
	}
	if !strings.Contains(err.Error(), task.DefaultPool) {
		t.Errorf("the failure does not name the pool it could not read: %v", err)
	}
}

package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// The plugin inventory is one of the four things the Admin surface reports,
// and PluginService promises it "across connected Orkers" with each plugin's
// input and output schema on the wire. These tests hold it to both halves of
// that sentence, and to the rule that governs both: this read ANSWERS. Every
// source beyond the deployment's own Orkers is best-effort, and a source that
// cannot be reached narrows the inventory and is logged — it never refuses the
// request.
//
// Every fixture below wires the Config fields `cmd/ork` wires: Pools (the
// Temporal probe), Lister (the deployment's ListPlugins activity) and
// PoolPlugins (the per-pool activity). A test that configured a seam no
// shipped binary sets would be describing a deployment that cannot exist.

// describedPlugins is a lister that answers for the deployment's default
// execution channel, with the schemas a real Orker declares.
type describedPlugins struct{ descriptors []plugin.Descriptor }

func (d describedPlugins) ListPlugins(context.Context) ([]plugin.Descriptor, error) {
	return d.descriptors, nil
}

// unreachableNames is a pool whose Orkers are live but do not answer.
type unreachableNames struct{ err error }

func (u unreachableNames) PluginsOnPool(context.Context, string) ([]string, error) {
	return nil, u.err
}

type downNamespaceRegistry struct{ err error }

func (d downNamespaceRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	// The registry is readable for the ONE document every request resolves
	// through (the built-in default) and unreadable for the listing this test
	// is about. A registry that failed both would be measuring the namespace
	// seam instead of the inventory's degradation.
	if artifacts.ResolveNamespace(name) == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, d.err
}

func (d downNamespaceRegistry) List(context.Context) ([]*orkstackv1.Namespace, error) {
	return nil, d.err
}

func (d downNamespaceRegistry) Put(context.Context, *orkstackv1.Namespace, artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	return nil, d.err
}

func (d downNamespaceRegistry) Delete(context.Context, string) error { return d.err }

func pluginNames(plugins []*orkstackv1.PluginInfo) []string {
	names := make([]string, 0, len(plugins))
	for _, info := range plugins {
		names = append(names, info.GetName())
	}
	return names
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
// connected Orkers" half, and the exact statement of how complete it is.
//
// A deployment with a gpu pool runs a plugin the default pool's Orkers do not
// have. Asking only the default execution channel reports an inventory that is
// true of one pool and presented as the deployment's, so an operator looking
// for `container` concludes it is not installed while a run using it succeeds.
//
// What the custom pool contributes is the NAME and nothing else: the per-pool
// seam this process is wired with (engine.PoolPluginLister, from
// TemporalStarter.PluginsOnPool) reports names. That limit is asserted here,
// not narrated, because it is what the wire promise has to match.
func TestListPlugins_ReportsPluginsOnlyACustomPoolsOrkersRun(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{
			"default": {Name: "default"},
			"vision":  {Name: "vision", AllowedPools: []string{"main", "gpu"}},
		},
		Lister: describedPlugins{descriptors: []plugin.Descriptor{
			{Name: "file", Version: "0.3.0"},
			{Name: "shell", Version: "0.1.0"},
		}},
		Pools:       livePools{task.DefaultQueue: 2, gpu: 1},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"file", "shell"}, gpu: {"container"}},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	names := pluginNames(resp.Msg.GetPlugins())
	if fmt.Sprint(names) != fmt.Sprint([]string{"container", "file", "shell"}) {
		t.Errorf("inventory = %v, want [container file shell] — the deployment's Orkers and the custom pool", names)
	}
	// One entry per plugin: the default pool and the deployment's default
	// channel are the same Orkers, and reporting them twice would make the
	// inventory a count of pools rather than of plugins.
	if len(resp.Msg.GetPlugins()) != len(names) {
		t.Errorf("inventory = %v, want no duplicates", resp.Msg.GetPlugins())
	}
	for _, info := range resp.Msg.GetPlugins() {
		switch info.GetName() {
		case "container":
			// The limit, pinned: a non-default pool answers with names.
			if info.GetVersion() != "" {
				t.Errorf("container version = %q, want empty: a non-default pool "+
					"reports names, and a version nothing measured would be invented",
					info.GetVersion())
			}
		case "file", "shell":
			if info.GetVersion() == "" {
				t.Errorf("%s has no version; the deployment's own Orkers answer with "+
					"whole descriptors", info.GetName())
			}
		}
	}
}

// TestListPlugins_ReportsAPoolWithSeveralOrkers is the case the previous round
// of this slice broke.
//
// A pool IS a set of Orkers, so two of them is the normal shape and the one an
// operator scales into. The read answers: the count is not a reason to refuse,
// and the plugins are what the pool's Orkers reported.
func TestListPlugins_ReportsAPoolWithSeveralOrkers(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "gpu"},
		}},
		Lister:      describedPlugins{descriptors: []plugin.Descriptor{{Name: "shell", Version: "0.1.0"}}},
		Pools:       livePools{task.DefaultQueue: 3, gpu: 2},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}, gpu: {"container"}},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins with several Orkers per pool: %v", err)
	}
	if got := fmt.Sprint(pluginNames(resp.Msg.GetPlugins())); got != "[container shell]" {
		t.Errorf("inventory = %s, want [container shell]", got)
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
		PoolPlugins: countingNames{
			counts: asked,
			byQueue: poolPlugins{
				task.DefaultQueue: {"shell"},
				cold:              {"never-asked"},
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

// countingNames records which channels were asked.
type countingNames struct {
	counts  map[string]int
	byQueue poolPlugins
}

func (c countingNames) PluginsOnPool(ctx context.Context, queue string) ([]string, error) {
	c.counts[queue]++
	return c.byQueue.PluginsOnPool(ctx, queue)
}

// TestListPlugins_StillAnswersWhenACustomPoolCannotBeAsked is the degradation
// rule, measured.
//
// A gpu pool that does not answer costs the inventory its gpu-only plugins. It
// does not cost the operator the plugins that ARE reachable: this view is
// opened when something is already wrong, and a refusal there hides the whole
// deployment because one pool is unwell. The loss is logged with the pool.
func TestListPlugins_StillAnswersWhenACustomPoolCannotBeAsked(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	logs := &bytes.Buffer{}
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "gpu"},
		}},
		Lister:      describedPlugins{descriptors: []plugin.Descriptor{{Name: "shell", Version: "0.1.0"}}},
		Pools:       livePools{task.DefaultQueue: 1, gpu: 1},
		PoolPlugins: unreachableNames{err: errors.New("activity timed out")},
		Logger:      slog.New(slog.NewTextHandler(logs, nil)),
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins refused because one pool did not answer: %v", err)
	}
	if got := fmt.Sprint(pluginNames(resp.Msg.GetPlugins())); got != "[shell]" {
		t.Errorf("inventory = %s, want [shell]: what the reachable Orkers reported", got)
	}
	for _, want := range []string{"pool=gpu", "activity timed out"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the degradation log does not carry %q", want)
		}
	}
	if t.Failed() {
		t.Logf("the log said: %s", logs.String())
	}
}

// TestListPlugins_StillAnswersWhenTheDurableSourcesCannotBeRead covers the
// other half of the same rule.
//
// The durable registry and the orkflow catalog are how this read LEARNS which
// non-default pools exist. Losing them narrows the inventory to the
// deployment's own Orkers — which is the whole answer this RPC gave before the
// Admin surface existed — and never refuses it.
func TestListPlugins_StillAnswersWhenTheDurableSourcesCannotBeRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  api.Config
		want string
	}{
		{
			name: "namespace registry",
			cfg: api.Config{
				Namespaces: downNamespaceRegistry{err: errors.New("bucket unavailable")},
				Pools:      livePools{task.DefaultQueue: 1},
				Lister: describedPlugins{descriptors: []plugin.Descriptor{
					{Name: "shell", Version: "0.1.0"},
				}},
				PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}},
			},
			want: "bucket unavailable",
		},
		{
			name: "orkflow catalog",
			cfg: api.Config{
				Orkflows:   downOrkflowStore{err: errors.New("catalog unavailable")},
				Namespaces: registry{"default": {Name: "default"}},
				Pools:      livePools{task.DefaultQueue: 1},
				Lister: describedPlugins{descriptors: []plugin.Descriptor{
					{Name: "shell", Version: "0.1.0"},
				}},
				PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}},
			},
			want: "catalog unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &bytes.Buffer{}
			cfg := tc.cfg
			cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
			svc := api.NewService(cfg)

			resp, err := svc.ListPlugins(context.Background(),
				connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
			if err != nil {
				t.Fatalf("ListPlugins refused after losing the %s: %v", tc.name, err)
			}
			if got := fmt.Sprint(pluginNames(resp.Msg.GetPlugins())); got != "[shell]" {
				t.Errorf("inventory = %s, want [shell]: the deployment's own Orkers", got)
			}
			if !strings.Contains(logs.String(), tc.want) {
				t.Errorf("the degradation log does not carry %q: %s", tc.want, logs.String())
			}
		})
	}
}

// TestListPlugins_KeepsCatalogPoolsOnBothSidesOfANamespaceCatalogFailure — F3.
//
// A namespace's allow-list is not the source this read is proving: these pool
// names exist only in the successful orkflow catalogs. The scan must retain
// the catalog-derived pool before a failing namespace, continue past that
// namespace, and retain the catalog-derived pool after it. The loss is logged
// so the narrowed inventory cannot look complete.
func TestListPlugins_KeepsCatalogPoolsOnBothSidesOfANamespaceCatalogFailure(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	fpga := poolQueue(t, "fpga")
	logs := &bytes.Buffer{}
	svc := api.NewService(api.Config{
		Namespaces: registry{
			"aaa": {Name: "aaa"},
			"mmm": {Name: "mmm"},
			"zzz": {Name: "zzz"},
		},
		Orkflows: catalogDownFor{
			namespace: "mmm",
			err:       errors.New("catalog unavailable"),
			heads: map[string][]*orkstackv1.Orkflow{
				"aaa": {{
					Name:    "aaa-flow",
					Version: "0.1.0",
					Tasks:   []*orkstackv1.Task{{Id: "gpu", Pool: "gpu"}},
				}},
				"zzz": {{
					Name:    "zzz-flow",
					Version: "0.1.0",
					Tasks:   []*orkstackv1.Task{{Id: "fpga", Pool: "fpga"}},
				}},
			},
		},
		Pools: livePools{task.DefaultQueue: 1, gpu: 1, fpga: 1},
		Lister: describedPlugins{descriptors: []plugin.Descriptor{
			{Name: "shell", Version: "0.1.0"},
		}},
		PoolPlugins: poolPlugins{
			task.DefaultQueue: {"shell"},
			gpu:               {"gpu-plugin"},
			fpga:              {"fpga-plugin"},
		},
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins refused after one namespace's catalog failed: %v", err)
	}
	if got := fmt.Sprint(pluginNames(resp.Msg.GetPlugins())); got !=
		"[fpga-plugin gpu-plugin shell]" {
		t.Fatalf("inventory = %s, want both catalog-derived pools and the default pool", got)
	}
	for _, want := range []string{"mmm", "catalog unavailable"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the degradation log does not carry %q: %s", want, logs.String())
		}
	}
}

// catalogDownFor is an orkflow store that has lost exactly one namespace,
// which is what a partial outage looks like from here: not "the bucket is
// gone", but "this prefix does not answer".
type catalogDownFor struct {
	namespace string
	err       error
	heads     map[string][]*orkstackv1.Orkflow
}

func (c catalogDownFor) Put(
	context.Context, *orkstackv1.Orkflow, artifacts.HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	return nil, false, c.err
}

func (c catalogDownFor) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, artifacts.ErrNotFound
}

func (c catalogDownFor) List(_ context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	if namespace == c.namespace {
		return nil, c.err
	}
	return append([]*orkstackv1.Orkflow(nil), c.heads[namespace]...), nil
}

func (c catalogDownFor) Versions(
	_ context.Context, namespace, name string,
) (artifacts.OrkflowVersions, error) {
	if namespace == c.namespace {
		return artifacts.OrkflowVersions{}, c.err
	}
	for _, head := range c.heads[namespace] {
		if head.GetName() == name {
			return artifacts.OrkflowVersions{All: []*orkstackv1.Orkflow{head}}, nil
		}
	}
	return artifacts.OrkflowVersions{}, artifacts.ErrNotFound
}

func (c catalogDownFor) SetCurrentVersion(context.Context, string, string, string) (string, error) {
	return "", c.err
}

// TestListPlugins_FailsWhenTheDeploymentsOwnOrkersDoNotAnswer is the one
// failure this RPC has ever had, kept.
//
// The deployment's own Orkers are asked through the same activity `orkctl
// plugin list` has always used. When THAT does not answer there is no
// inventory to render, and an empty list would read as "no plugins are
// installed".
func TestListPlugins_FailsWhenTheDeploymentsOwnOrkersDoNotAnswer(t *testing.T) {
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {Name: "default"}},
		Pools:      livePools{task.DefaultQueue: 1},
		Lister:     failingLister{},
	})

	_, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err == nil {
		t.Fatal("ListPlugins answered while the deployment's Orkers did not")
	}
	if code := connect.CodeOf(err); code != connect.CodeUnavailable {
		t.Errorf("ListPlugins failed with %v, want Unavailable", code)
	}
}

// TestListPlugins_ReportsKnownCustomPoolWhenTheDefaultPoolIsEmpty is the probe
// earning its place: a deployment whose Orkers all sit on a custom pool used
// to wait out the default channel's activity timeout and then answer nothing.
// The probe says the default route is dead, so it is not asked, and the pool
// that IS alive is.
func TestListPlugins_ReportsKnownCustomPoolWhenTheDefaultPoolIsEmpty(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"gpu"},
		}},
		Lister:      failingLister{},
		Pools:       livePools{gpu: 1},
		PoolPlugins: poolPlugins{gpu: {"container"}},
	})

	resp, err := svc.ListPlugins(context.Background(),
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins with only a custom pool alive: %v", err)
	}
	if len(resp.Msg.GetPlugins()) != 1 || resp.Msg.GetPlugins()[0].GetName() != "container" {
		t.Fatalf("plugins = %v, want the custom pool's container plugin", resp.Msg.GetPlugins())
	}
}

// TestListPools_StillReportsAPoolWhosePluginReadFailed is the same rule on the
// pool-health read, and it agrees with the dashboard by construction: both go
// through engine.PoolResolver.Pools.
//
// The pool's liveness is a separate fact from what its Orkers run, and the
// half that was measured is reported. `plugins` is empty here — which the wire
// contract says means "nobody was asked, or nobody answered", never "these
// Orkers run nothing".
//
// F2: AND IT IS LOGGED, which is what this test did not check before. A
// narrowed answer that says nothing is indistinguishable from a complete one,
// so the log is the only place the loss is recorded — and a claim no test
// exercises is not evidence. The line names both coordinates an operator
// needs: which pool went quiet, and what it said.
func TestListPools_StillReportsAPoolWhosePluginReadFailed(t *testing.T) {
	logs := &bytes.Buffer{}
	svc := api.NewService(api.Config{
		Namespaces:  registry{"default": {Name: "default"}},
		Pools:       livePools{task.DefaultQueue: 1},
		PoolPlugins: unreachableNames{err: errors.New("no worker answered")},
		Logger:      slog.New(slog.NewTextHandler(logs, nil)),
	})

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools refused because the plugin read failed: %v", err)
	}
	if len(resp.Msg.GetPools()) != 1 {
		t.Fatalf("pools = %v, want the one default pool", resp.Msg.GetPools())
	}
	pool := resp.Msg.GetPools()[0]
	if pool.GetLiveOrkerCount() != 1 {
		t.Errorf("live_orker_count = %d, want 1: liveness was measured", pool.GetLiveOrkerCount())
	}
	if len(pool.GetPlugins()) != 0 {
		t.Errorf("plugins = %v, want empty: nobody answered", pool.GetPlugins())
	}
	// Losing the plugin list is not losing the pool: the count stands, so the
	// row is not marked unavailable.
	if reason := pool.GetUnavailableReason(); reason != "" {
		t.Errorf("unavailable_reason = %q; liveness WAS measured, only the plugin "+
			"list was lost", reason)
	}
	for _, want := range []string{task.DefaultPool, "no worker answered"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the degradation log does not name %q: %s", want, logs.String())
		}
	}
}

// TestListPools_ReportsTheReachablePoolsWhenOneCannotBeProbed — F1.
//
// This is the rule proof/M11.3/README.md and docs/DEPLOY.md both state and
// the code did not have: one pool the cluster cannot answer for used to abort
// the whole read, and the RPC returned Unavailable with NO pools at all. A
// deployment with a healthy default pool and one unreachable `gpu` reported
// nothing — and an admin read is opened precisely when part of a deployment is
// already broken.
//
// The reachable part is reported. The rest is MARKED, not dropped and not
// silently zeroed: `unavailable_reason` says nothing was measured, so a reader
// cannot mistake it for "no Orkers are serving this pool" and go scale a pool
// that may be perfectly healthy. And the loss is logged, with the pool and the
// error.
func TestListPools_ReportsTheReachablePoolsWhenOneCannotBeProbed(t *testing.T) {
	gpuQueue := poolQueue(t, "gpu")
	logs := &bytes.Buffer{}
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{task.DefaultPool, "gpu"},
		}},
		Pools: partiallyReachablePools{
			live: livePools{task.DefaultQueue: 2},
			err:  map[string]error{gpuQueue: errors.New("dial tcp 127.0.0.1:7233: connection refused")},
		},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}},
		Logger:      slog.New(slog.NewTextHandler(logs, nil)),
	})

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools refused because ONE pool could not be probed: %v.\n"+
			"proof/M11.3/README.md and docs/DEPLOY.md both say an unreachable source "+
			"only narrows this answer", err)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, pool := range resp.Msg.GetPools() {
		byName[pool.GetName()] = pool
	}
	if len(byName) != 2 {
		t.Fatalf("pools = %v, want both %q and gpu", resp.Msg.GetPools(), task.DefaultPool)
	}

	healthy := byName[task.DefaultPool]
	if healthy.GetLiveOrkerCount() != 2 {
		t.Errorf("%s live_orker_count = %d, want 2: it was reachable and it was measured",
			task.DefaultPool, healthy.GetLiveOrkerCount())
	}
	if healthy.GetUnavailableReason() != "" {
		t.Errorf("%s carries unavailable_reason %q; it answered",
			task.DefaultPool, healthy.GetUnavailableReason())
	}
	if len(healthy.GetPlugins()) != 1 || healthy.GetPlugins()[0] != "shell" {
		t.Errorf("%s plugins = %v, want [shell]", task.DefaultPool, healthy.GetPlugins())
	}

	lost := byName["gpu"]
	if lost == nil {
		t.Fatalf("the unreachable pool is missing from %v; it is in the namespace's "+
			"allow-list and dropping it would read as 'this pool does not exist'",
			resp.Msg.GetPools())
	}
	if lost.GetUnavailableReason() == "" {
		t.Fatalf("gpu reports live_orker_count=%d with no unavailable_reason; a count "+
			"nothing measured reads as 'nobody is serving this pool', which is a "+
			"different fact needing the opposite fix", lost.GetLiveOrkerCount())
	}
	if !strings.Contains(lost.GetUnavailableReason(), "connection refused") {
		t.Errorf("gpu unavailable_reason = %q, want the cause", lost.GetUnavailableReason())
	}
	if lost.GetLiveOrkerCount() != 0 || lost.GetLastSeen() != nil {
		t.Errorf("gpu carries a count (%d) or a last_seen (%v) nothing measured",
			lost.GetLiveOrkerCount(), lost.GetLastSeen())
	}
	for _, want := range []string{"gpu", "connection refused"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the degradation log does not name %q: %s", want, logs.String())
		}
	}
}

// partiallyReachablePools answers for some channels and fails for others,
// which is what a real cluster partition looks like: not "the cluster is
// down", but "this question timed out".
type partiallyReachablePools struct {
	live livePools
	err  map[string]error
}

func (p partiallyReachablePools) ObservePool(
	ctx context.Context,
	queue string,
) (engine.PoolLiveness, error) {
	if err, ok := p.err[queue]; ok {
		return engine.PoolLiveness{}, err
	}
	return p.live.ObservePool(ctx, queue)
}

// TestListPools_ReportsAPoolWithSeveralOrkers is the regression this round
// exists for.
//
// A pool with two Orkers is a pool being scaled, which is exactly when an
// operator reads pool health. The count is reported and so are the plugins the
// pool answered with.
func TestListPools_ReportsAPoolWithSeveralOrkers(t *testing.T) {
	svc := api.NewService(api.Config{
		Namespaces:  registry{"default": {Name: "default"}},
		Pools:       livePools{task.DefaultQueue: 2},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}},
	})

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools with two Orkers on one pool: %v", err)
	}
	if len(resp.Msg.GetPools()) != 1 {
		t.Fatalf("pools = %v, want the one default pool", resp.Msg.GetPools())
	}
	pool := resp.Msg.GetPools()[0]
	if pool.GetLiveOrkerCount() != 2 {
		t.Errorf("live_orker_count = %d, want 2", pool.GetLiveOrkerCount())
	}
	if got := strings.Join(pool.GetPlugins(), ","); got != "shell" {
		t.Errorf("plugins = %q, want shell", got)
	}
}

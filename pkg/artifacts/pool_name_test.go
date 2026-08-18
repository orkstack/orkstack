package artifacts

import (
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The registry stores two routing targets — default_pool and allowed_pools —
// and M10.2 is the slice that reads them. A name this store accepts and
// dispatch refuses is a value nothing can ever honour, so the two rules must
// be the same rule. They cannot be the same CODE: pkg/ may not import
// internal/, or no external importer of pkg/artifacts could build. So they are
// the same rule by measurement, here, in a test — which no external importer
// builds either.
//
// The import below is the whole point of the file: it is the only place
// pkg/artifacts names internal/task, and it exists to make drift a test
// failure rather than a stored value M10.2 must reject.

// poolCorpus is spelled out rather than generated, so a reader can see exactly
// what is claimed. The four names in the first invalid group are the ones
// review measured slipping through cleanComponentPattern.
var poolCorpus = []string{
	// routable
	"main", "gpu", "gpu-2", "a", "x1", "ork-pool",
	// not routable, and once stored anyway
	"GPU", "Main", "gpu.1", "1gpu",
	// not routable, never stored
	"-gpu", "gpu_1", "ork pool", "gpu/1", "..", "gpu~1", "_gpu",
}

// TestNamespacePools_AreExactlyThePoolsDispatchCanReach is the pin: for every
// name, the registry and the only pool->queue translation in the tree agree.
func TestNamespacePools_AreExactlyThePoolsDispatchCanReach(t *testing.T) {
	for _, pool := range poolCorpus {
		stored := validNamespacePools(&orkstackv1.Namespace{
			Name:         "probe",
			DefaultPool:  pool,
			AllowedPools: []string{pool},
		}) == nil
		_, dispatchErr := task.QueueForPool(pool)
		routable := dispatchErr == nil

		switch {
		case stored && !routable:
			t.Errorf("the registry stores pool %q, dispatch refuses it (%v): a stored routing "+
				"target no dispatch can honour", pool, dispatchErr)
		case routable && !stored:
			t.Errorf("dispatch routes pool %q, the registry refuses to store it: a namespace "+
				"cannot name a pool its own tasks may use", pool)
		}
	}
}

// TestNamespacePools_UnsetDefaultIsTheBuiltInPool covers the one name the
// corpus above cannot: empty. In a document it means "unset", and it resolves
// to the built-in pool — which must be the same built-in pool dispatch uses,
// or an unset default_pool would route somewhere else entirely.
func TestNamespacePools_UnsetDefaultIsTheBuiltInPool(t *testing.T) {
	if DefaultPool != task.DefaultPool {
		t.Fatalf("artifacts.DefaultPool = %q, internal/task.DefaultPool = %q; the registry and "+
			"dispatch must mean the same pool", DefaultPool, task.DefaultPool)
	}
	if err := validNamespacePools(&orkstackv1.Namespace{
		Name:         "probe",
		AllowedPools: []string{DefaultPool},
	}); err != nil {
		t.Errorf("an unset default_pool with %q allowed was refused: %v", DefaultPool, err)
	}
	// An allow-list entry, unlike default_pool, is a name and not an absence.
	if err := validNamespacePools(&orkstackv1.Namespace{
		Name:         "probe",
		AllowedPools: []string{""},
	}); !errors.Is(err, ErrInvalidNamespace) {
		t.Errorf("allowed_pools [\"\"] = %v, want ErrInvalidNamespace: an empty entry names no pool", err)
	}
}

// TestNamespacePools_RejectTheCaseReviewMeasured names the four values
// explicitly, so a future loosening of the pattern fails on the exact
// regression rather than on an abstraction.
func TestNamespacePools_RejectTheCaseReviewMeasured(t *testing.T) {
	for _, pool := range []string{"GPU", "Main", "gpu.1", "1gpu"} {
		for _, ns := range []*orkstackv1.Namespace{
			{Name: "probe", DefaultPool: pool},
			{Name: "probe", AllowedPools: []string{DefaultPool, pool}},
		} {
			if err := validNamespacePools(ns); !errors.Is(err, ErrInvalidNamespace) {
				t.Errorf("validNamespacePools(%v) = %v, want ErrInvalidNamespace for pool %q",
					ns, err, pool)
			}
		}
	}
}

package types_test

import (
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/types"
)

// Every type is listed under its own name, the set is the caller's to keep,
// and a type this build has not is refused.
func TestAll_ListsEachTypeUnderItsName(t *testing.T) {
	for name, typ := range types.All() {
		if typ.Name() != name {
			t.Errorf("%q is listed as %q", typ.Name(), name)
		}
	}
	types.All()["mine"] = nil
	if _, ok := types.All()["mine"]; ok {
		t.Error("a caller changed the set")
	}
	if _, err := provider.ParseWith([]byte("providers:\n  cloud:\n    type: none-such\n    audience: x\n"), types.All()); err == nil {
		t.Error("a type this build has not was accepted")
	}
	if set, err := provider.ParseWith([]byte("providers:\n  cloud:\n    type: aws\n    region: eu-west-1\n"), types.All()); err != nil || set.Len() != 1 {
		t.Errorf("an aws declaration = %d providers, %v", set.Len(), err)
	}
	if set, err := provider.ParseWith([]byte("providers:\n  vault:\n    type: vault\n    address: https://vault.internal:8200\n    audience: vault.internal\n"), types.All()); err != nil || set.Len() != 1 {
		t.Errorf("a vault declaration = %d providers, %v", set.Len(), err)
	}
}

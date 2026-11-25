// Package types is the complete set of compiled-in provider types. It is a
// package of its own because a type's package imports internal/provider.
package types

import (
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/aws"
	"github.com/orkstack/orkstack/internal/provider/vault"
)

// All is the set, by the value of a declaration's `type:`. It is fixed when
// the binary is built: a value chosen by a setting, as Podman is to the
// runtime (AGENTS.md §5), never a plugin. To add a type, implement Type,
// Provider and Session in internal/provider/<type>/, add one line here, and
// pass providertest.Conformance (docs/PROVIDERS.md, "Adding a provider
// type"). "image" (P17c) lands in its own change.
func All() map[string]provider.Type {
	return map[string]provider.Type{
		aws.TypeName: aws.Type{},
		"vault":      vault.Type{},
	}
}

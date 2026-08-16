package orker

import (
	"github.com/orkstack/orkstack/pkg/plugin"
	containerplugin "github.com/orkstack/orkstack/pkg/plugins/container"
	fileplugin "github.com/orkstack/orkstack/pkg/plugins/file"
	httpplugin "github.com/orkstack/orkstack/pkg/plugins/http"
	shellplugin "github.com/orkstack/orkstack/pkg/plugins/shell"
)

// BuiltinRegistry returns a registry holding the builtin plugins an Orker
// serves out of the box: http, shell, file and container.
func BuiltinRegistry() *plugin.Registry {
	registry := plugin.NewRegistry()
	registry.Register(httpplugin.New())
	registry.Register(shellplugin.New())
	registry.Register(fileplugin.New())
	registry.Register(containerplugin.New())
	return registry
}

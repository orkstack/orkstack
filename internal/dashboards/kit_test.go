package dashboards

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The kit's Prometheus is pushed to and scrapes nothing: no scrape or
// remote-write path exists, and its container is given no route to the
// host (deploy/observability/README.md).
func TestKit_NoScrapeConfigs(t *testing.T) {
	b, err := os.ReadFile("../../deploy/observability/prometheus.yml")
	must.OK(t, err)
	var prom map[string]any
	must.OK(t, yaml.Unmarshal(b, &prom))
	for _, key := range []string{"scrape_configs", "scrape_config_files", "remote_write", "remote_read"} {
		if _, ok := prom[key]; ok {
			t.Errorf("prometheus.yml has %s; the kit's Prometheus is pushed to only", key)
		}
	}

	b, err = os.ReadFile("../../deploy/docker-compose.yml")
	must.OK(t, err)
	var compose struct {
		Services map[string]struct {
			Command    []string `yaml:"command"`
			ExtraHosts []string `yaml:"extra_hosts"`
		} `yaml:"services"`
	}
	must.OK(t, yaml.Unmarshal(b, &compose))
	p, ok := compose.Services["prometheus"]
	if !ok {
		t.Fatal("deploy/docker-compose.yml has no prometheus service")
	}
	if len(p.ExtraHosts) > 0 {
		t.Errorf("the prometheus service has extra_hosts %v; it needs no route to the host", p.ExtraHosts)
	}
	if !slices.Contains(p.Command, "--web.enable-otlp-receiver") {
		t.Errorf("the prometheus service command %v lacks --web.enable-otlp-receiver", p.Command)
	}
	if slices.ContainsFunc(p.Command, func(a string) bool { return strings.Contains(a, "remote-write-receiver") }) {
		t.Errorf("the prometheus service command %v receives remote writes", p.Command)
	}
}
